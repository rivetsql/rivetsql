"""Catalog Explorer data models and error class for rivet-core."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from rivet_core.errors import RivetError
from rivet_core.introspection import NodeSummary, ObjectMetadata, ObjectSchema

if TYPE_CHECKING:
    from rivet_core.introspection import CatalogNode
    from rivet_core.models import Catalog, ComputeEngine
    from rivet_core.plugins import CatalogPlugin, PluginRegistry

logger = logging.getLogger(__name__)

# Error code constants: RVT-870 through RVT-877
RVT_870 = "RVT-870"  # Catalog connection failed
RVT_871 = "RVT-871"  # Table not found
RVT_872 = "RVT-872"  # Schema introspection failed
RVT_873 = "RVT-873"  # Depth limit reached (informational)
RVT_874 = "RVT-874"  # Source generation failed
RVT_875 = "RVT-875"  # Preview failed
RVT_876 = "RVT-876"  # Search index build failed
RVT_877 = "RVT-877"  # Output file write failed


class CatalogExplorerError(Exception):
    """Raised when a catalog exploration operation fails."""

    def __init__(self, error: RivetError) -> None:
        self.error = error
        super().__init__(str(error))


@dataclass(frozen=True)
class CatalogInfo:
    """Top-level catalog summary returned by list_catalogs()."""

    name: str
    catalog_type: str
    connected: bool
    error: str | None
    options_summary: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ExplorerNode:
    """A node in the unified catalog tree."""

    name: str
    node_type: str  # "catalog", "database", "schema", "table", "view", "file", "column", "directory"
    path: list[str]  # full path from root
    is_expandable: bool
    depth: int
    summary: NodeSummary | None
    depth_limit_reached: bool


@dataclass(frozen=True)
class NodeDetail:
    """Detailed metadata for a specific node."""

    node: ExplorerNode
    schema: ObjectSchema | None
    metadata: ObjectMetadata | None
    children_count: int | None


@dataclass(frozen=True)
class SearchResult:
    """A fuzzy search hit."""

    kind: str  # "catalog", "schema", "table", "column"
    qualified_name: str
    short_name: str
    parent: str | None
    match_positions: list[int]
    score: float  # lower = better
    node_type: str


@dataclass(frozen=True)
class GeneratedSource:
    """A generated source joint declaration."""

    content: str
    format: str  # "yaml" or "sql"
    suggested_filename: str
    catalog_name: str
    table_name: str
    column_count: int




@dataclass(frozen=True)
class ConnectionResult:
    """Result of a connection test."""

    catalog_name: str
    connected: bool
    error: str | None
    elapsed_ms: float


def sanitize_name(name: str) -> str:
    """Sanitize a table name into a valid Rivet joint name.

    Lowercases, replaces non-alphanumeric characters with underscores,
    prefixes with 'raw_', and truncates to 128 characters.
    """
    lowered = name.lower()
    replaced = re.sub(r"[^a-z0-9]", "_", lowered)
    prefixed = "raw_" + replaced
    return prefixed[:128]


# ── Credential keys to exclude from options_summary ──────────────────────
_CREDENTIAL_KEYS = frozenset({
    "password", "secret", "token", "key", "credential", "credentials",
    "secret_key", "access_key", "api_key", "private_key",
})


def _safe_options_summary(options: dict[str, Any]) -> dict[str, str]:
    """Return a safe subset of catalog options, excluding credentials."""
    return {
        k: str(v)
        for k, v in options.items()
        if not any(ck in k.lower() for ck in _CREDENTIAL_KEYS)
    }


# ── Hierarchical normalization constants ──────────────────────────────────

# Recognized file extensions for S3/filesystem catalogs (Req 3.3)
_RECOGNIZED_EXTENSIONS = frozenset({".parquet", ".csv", ".json", ".ipc", ".orc"})

# Catalog types that use file-based hierarchies (Req 3.3, 3.4)
_FILE_CATALOG_TYPES = frozenset({"s3", "filesystem"})


def _has_recognized_extension(name: str) -> bool:
    """Return True if the filename has a recognized data file extension."""
    dot = name.rfind(".")
    return dot >= 0 and name[dot:].lower() in _RECOGNIZED_EXTENSIONS


# ── Node type ranking for search scoring ──────────────────────────────────
_NODE_TYPE_BONUS: dict[str, float] = {
    "catalog": -3.0,
    "database": -2.0,
    "schema": -2.0,
    "table": -1.0,
    "view": -1.0,
    "file": -1.0,
    "directory": -0.5,
    "column": 0.0,
}


def fuzzy_match(query: str, candidate: str) -> tuple[float, list[int]] | None:
    """Pure Python fuzzy matching. All query chars must appear in candidate in order.

    Returns (score, match_positions) or None if no match. Lower score = better.
    Scoring: exact prefix > word boundary > consecutive > scattered.
    Shorter candidates score better.
    """
    if not query:
        return (0.0, [])

    q = query.lower()
    c = candidate.lower()
    positions: list[int] = []
    ci = 0

    for ch in q:
        found = c.find(ch, ci)
        if found == -1:
            return None
        positions.append(found)
        ci = found + 1

    # Scoring components
    score = 0.0

    # Exact prefix match bonus (strong)
    if c.startswith(q):
        score -= 10.0

    # Word boundary bonus: count matches at word boundaries (after _, ., or start)
    for pos in positions:
        if pos == 0 or candidate[pos - 1] in ("_", ".", "-", "/"):
            score -= 2.0

    # Consecutive bonus: count consecutive position pairs
    for i in range(1, len(positions)):
        if positions[i] == positions[i - 1] + 1:
            score -= 1.5

    # Scatter penalty: total gap between matched positions
    if len(positions) > 1:
        score += (positions[-1] - positions[0] - len(positions) + 1) * 0.5

    # Length penalty: shorter candidates are better
    score += len(candidate) * 0.1

    return (score, positions)


class CatalogExplorer:
    """Headless catalog browsing service.

    Accepts instantiated catalogs, engines, and a PluginRegistry.
    All catalog browsing logic lives here — no compilation or assembly required.
    """

    def __init__(
        self,
        catalogs: dict[str, Catalog],
        engines: dict[str, ComputeEngine],
        registry: PluginRegistry,
        max_depth: int = 10,
        skip_probe: bool = False,
    ) -> None:
        self._catalogs = catalogs
        self._engines = engines
        self._registry = registry
        self._max_depth = max_depth
        self._tables_cache: dict[str, list[CatalogNode]] = {}
        self._children_cache: dict[tuple[str, tuple[str, ...]], list[CatalogNode]] = {}
        self._schema_cache: dict[tuple[str, str], ObjectSchema] = {}

        # Probe each catalog to determine connection status (lightweight)
        self._connection_status: dict[str, tuple[bool, str | None]] = {}
        if not skip_probe:
            for name, catalog in self._catalogs.items():
                plugin = self._registry.get_catalog_plugin(catalog.type)
                if plugin is None:
                    self._connection_status[name] = (False, f"No plugin for type '{catalog.type}'")
                    continue
                try:
                    check = getattr(plugin, "test_connection", None)
                    if check is not None:
                        check(catalog)
                    else:
                        plugin.list_tables(catalog)
                    self._connection_status[name] = (True, None)
                except Exception as exc:
                    logger.debug("Catalog '%s' connection probe failed: %s", name, exc)
                    self._connection_status[name] = (False, str(exc))

    def list_catalogs(self) -> list[CatalogInfo]:
        """Return CatalogInfo for each catalog without calling introspection methods."""
        result: list[CatalogInfo] = []
        for name, catalog in self._catalogs.items():
            connected, error = self._connection_status.get(name, (False, "Unknown"))
            result.append(
                CatalogInfo(
                    name=name,
                    catalog_type=catalog.type,
                    connected=connected,
                    error=error,
                    options_summary=_safe_options_summary(catalog.options),
                )
            )
        return result

    # ── Tree navigation (lazy) ────────────────────────────────────────

    _TABLE_NODE_TYPES = frozenset({"table", "view", "file"})

    def list_children(self, path: list[str]) -> list[ExplorerNode]:
        """List immediate children of a path with lazy per-level loading.

        Each expansion fetches only the immediate children via
        plugin.list_children(catalog, sub_path). Results are cached per-path
        so repeated expansions don't re-fetch.

        - Empty path: returns empty (use list_catalogs() instead).
        - [catalog_name]: fetches immediate children (e.g. schemas).
        - [catalog_name, ...table]: calls get_schema() for column nodes.
        - Disconnected catalogs: returns empty list.
        - Depth exceeding max_depth: returns empty list.
        """
        if not path:
            return []

        catalog_name = path[0]

        # Disconnected catalogs return empty
        connected, _ = self._connection_status.get(catalog_name, (False, None))
        if not connected:
            return []

        # Depth cap enforcement
        if len(path) >= self._max_depth:
            return []

        catalog = self._catalogs.get(catalog_name)
        if catalog is None:
            return []

        plugin = self._registry.get_catalog_plugin(catalog.type)
        if plugin is None:
            return []

        sub_path = path[1:]  # path within the catalog's namespace
        cache_key = (catalog_name, tuple(sub_path))

        # Check per-path cache first
        if cache_key not in self._children_cache:
            try:
                nodes = plugin.list_children(catalog, sub_path)
            except Exception:
                nodes = []
            self._children_cache[cache_key] = nodes

        cached_nodes = self._children_cache[cache_key]

        # No children returned — might be a table node, try column expansion
        if sub_path and not cached_nodes:
            return self._expand_columns(catalog_name, catalog, plugin, path)

        is_file_catalog = catalog.type in _FILE_CATALOG_TYPES
        child_depth = len(path)
        result: list[ExplorerNode] = []
        for node in cached_nodes:
            # For file-based catalogs, skip non-container nodes that aren't
            # recognized data files.  A node passes if it has a recognized
            # extension in its path OR the plugin already attached format
            # metadata (e.g. filesystem catalog uses logical names without
            # extensions but sets summary.format).
            if is_file_catalog and not node.is_container:
                raw_filename = node.path[-1] if node.path else node.name
                has_ext = _has_recognized_extension(raw_filename)
                has_format = node.summary is not None and getattr(node.summary, "format", None) is not None
                if not has_ext and not has_format:
                    continue
            is_table = node.node_type in self._TABLE_NODE_TYPES
            is_expandable = node.is_container or is_table
            depth_limit = child_depth + 1 >= self._max_depth
            result.append(
                ExplorerNode(
                    name=node.name,
                    node_type=node.node_type,
                    path=[catalog_name] + node.path,
                    is_expandable=is_expandable,
                    depth=child_depth,
                    summary=node.summary,
                    depth_limit_reached=depth_limit,
                )
            )
        return result



    def get_node_detail(self, path: list[str]) -> NodeDetail:
        """Return NodeDetail for the node at path, composing ExplorerNode + schema + metadata."""
        if not path:
            raise CatalogExplorerError(
                RivetError(code=RVT_871, message="Empty path provided")
            )
        catalog_name = path[0]
        catalog = self._catalogs.get(catalog_name)
        if catalog is None:
            raise CatalogExplorerError(
                RivetError(code=RVT_871, message=f"Catalog '{catalog_name}' not found")
            )

        # Build a minimal ExplorerNode for this path
        node = ExplorerNode(
            name=path[-1],
            node_type="table" if len(path) > 1 else "catalog",
            path=path,
            is_expandable=True,
            depth=len(path) - 1,
            summary=None,
            depth_limit_reached=False,
        )

        schema = self.get_table_schema(path)
        metadata = self.get_table_metadata(path)
        children_count = len(schema.columns) if schema is not None else None

        return NodeDetail(node=node, schema=schema, metadata=metadata, children_count=children_count)

    def get_table_schema(self, path: list[str]) -> ObjectSchema | None:
        """Return schema for the table at path, using cache (Req 18.2)."""
        if not path or len(path) < 2:
            return None
        catalog_name = path[0]
        catalog = self._catalogs.get(catalog_name)
        if catalog is None:
            return None
        plugin = self._registry.get_catalog_plugin(catalog.type)
        if plugin is None:
            return None

        table_key = ".".join(path[1:])
        cache_key = (catalog_name, table_key)
        if cache_key not in self._schema_cache:
            try:
                schema = plugin.get_schema(catalog, table_key)
                self._schema_cache[cache_key] = schema
            except Exception:
                return None
        return self._schema_cache.get(cache_key)

    def get_table_metadata(self, path: list[str]) -> ObjectMetadata | None:
        """Return metadata for the table at path. Always calls plugin — not cached (Req 18.4)."""
        if not path or len(path) < 2:
            return None
        catalog_name = path[0]
        catalog = self._catalogs.get(catalog_name)
        if catalog is None:
            return None
        plugin = self._registry.get_catalog_plugin(catalog.type)
        if plugin is None:
            return None

        table_key = ".".join(path[1:])
        try:
            return plugin.get_metadata(catalog, table_key)
        except Exception:
            return None

    def get_table_stats(self, path: list[str]) -> ObjectMetadata | None:
        """Return stats (metadata with column statistics) for the table at path.

        Always calls plugin — not cached (Req 18.4).
        """
        return self.get_table_metadata(path)

    def refresh_catalog(self, catalog_name: str) -> None:
        """Clear all cached data for the given catalog (Req 2.6, 18.3).

        The next call to list_children() will trigger fresh API calls.
        """
        self._tables_cache.pop(catalog_name, None)
        # Remove all children cache entries for this catalog
        children_keys = [k for k in self._children_cache if k[0] == catalog_name]
        for k in children_keys:
            del self._children_cache[k]
        # Remove all schema cache entries for this catalog
        keys_to_remove = [k for k in self._schema_cache if k[0] == catalog_name]
        for k in keys_to_remove:  # type: ignore[assignment]
            del self._schema_cache[k]  # type: ignore[arg-type]

    def test_connection(self, catalog_name: str) -> ConnectionResult:
        """Probe the catalog and return a ConnectionResult with status, error, elapsed time (Req 4.4)."""
        import time

        catalog = self._catalogs.get(catalog_name)
        if catalog is None:
            return ConnectionResult(
                catalog_name=catalog_name,
                connected=False,
                error=f"Catalog '{catalog_name}' not found",
                elapsed_ms=0.0,
            )

        plugin = self._registry.get_catalog_plugin(catalog.type)
        if plugin is None:
            return ConnectionResult(
                catalog_name=catalog_name,
                connected=False,
                error=f"No plugin for type '{catalog.type}'",
                elapsed_ms=0.0,
            )

        start = time.monotonic()
        try:
            check = getattr(plugin, "test_connection", None)
            if check is not None:
                check(catalog)
            else:
                plugin.list_tables(catalog)
            elapsed_ms = (time.monotonic() - start) * 1000.0
            self._connection_status[catalog_name] = (True, None)
            return ConnectionResult(
                catalog_name=catalog_name,
                connected=True,
                error=None,
                elapsed_ms=elapsed_ms,
            )
        except Exception as exc:
            elapsed_ms = (time.monotonic() - start) * 1000.0
            error_msg = str(exc)
            self._connection_status[catalog_name] = (False, error_msg)
            return ConnectionResult(
                catalog_name=catalog_name,
                connected=False,
                error=error_msg,
                elapsed_ms=elapsed_ms,
            )


    # ── Search ──────────────────────────────────────────────────────────

    def search(self, query: str, limit: int = 50) -> list[SearchResult]:
        """Fuzzy search across all cached nodes.

        Searches only what has already been loaded (lazy). Does not trigger
        new API calls — search improves as the user expands more of the tree.

        Returns SearchResult list sorted by score (ascending), capped at limit.
        Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6
        """
        if not query:
            return []

        hits: list[SearchResult] = []
        seen: set[str] = set()

        # Search children_cache (per-path lazy cache)
        for (catalog_name, _path_key), nodes in self._children_cache.items():
            for node in nodes:
                qualified = catalog_name + "." + ".".join(node.path)
                if qualified in seen:
                    continue
                seen.add(qualified)
                result = fuzzy_match(query, qualified)
                if result is None:
                    continue
                score, positions = result
                score += _NODE_TYPE_BONUS.get(node.node_type, 0.0)
                kind = node.node_type
                if kind in ("file", "directory"):
                    kind = "table" if not node.is_container else "schema"
                parent = catalog_name + "." + ".".join(node.path[:-1]) if len(node.path) > 1 else None
                hits.append(SearchResult(
                    kind=kind,
                    qualified_name=qualified,
                    short_name=node.name,
                    parent=parent,
                    match_positions=positions,
                    score=score,
                    node_type=node.node_type,
                ))

        # Also search legacy tables_cache (for plugins that don't override list_children)
        for catalog_name, nodes in self._tables_cache.items():
            for node in nodes:
                qualified = catalog_name + "." + ".".join(node.path)
                if qualified in seen:
                    continue
                seen.add(qualified)
                result = fuzzy_match(query, qualified)
                if result is None:
                    continue
                score, positions = result
                score += _NODE_TYPE_BONUS.get(node.node_type, 0.0)
                kind = node.node_type
                if kind in ("file", "directory"):
                    kind = "table" if not node.is_container else "schema"
                parent = catalog_name + "." + ".".join(node.path[:-1]) if len(node.path) > 1 else None
                hits.append(SearchResult(
                    kind=kind,
                    qualified_name=qualified,
                    short_name=node.name,
                    parent=parent,
                    match_positions=positions,
                    score=score,
                    node_type=node.node_type,
                ))

        hits.sort(key=lambda r: r.score)
        return hits[:limit]


    # ── Source generation ──────────────────────────────────────────────

    def generate_source(
        self,
        path: list[str],
        format: str = "yaml",
        columns: list[str] | None = None,
    ) -> GeneratedSource:
        """Generate a source joint declaration from a catalog table's introspected schema.

        Requirements: 7.1, 7.2, 7.3, 7.4, 7.5
        """
        schema = self.get_table_schema(path)
        if schema is None:
            raise CatalogExplorerError(
                RivetError(
                    code=RVT_874,
                    message=f"Cannot generate source: schema not available for '{'.'.join(path)}'",
                    context={"path": path},
                    remediation="Check that the table exists and the catalog is connected.",
                )
            )

        catalog_name = path[0]
        table_key = ".".join(path[1:])
        joint_name = sanitize_name(path[-1])

        # Filter columns if specified (Req 7.4, 7.5)
        selected = schema.columns
        if columns is not None:
            col_set = set(columns)
            known = {c.name for c in schema.columns}
            unrecognized = col_set - known
            if unrecognized:
                logger.warning(
                    "Unrecognized columns ignored in source generation: %s",
                    ", ".join(sorted(unrecognized)),
                )
            selected = [c for c in schema.columns if c.name in col_set]

        if format == "sql":
            content = self._generate_sql(joint_name, catalog_name, table_key, selected)
            ext = "sql"
        else:
            content = self._generate_yaml(joint_name, catalog_name, table_key, selected)
            ext = "yaml"

        return GeneratedSource(
            content=content,
            format=format,
            suggested_filename=f"{joint_name}.{ext}",
            catalog_name=catalog_name,
            table_name=table_key,
            column_count=len(selected),
        )

    @staticmethod
    def _generate_yaml(
        name: str, catalog: str, table: str, columns: list[Any],
    ) -> str:
        """Produce a YAML source joint declaration."""
        lines = [
            f"name: {name}",
            "type: source",
            f"catalog: {catalog}",
            f"table: {table}",
            "columns:",
        ]
        for col in columns:
            lines.append(f"  - name: {col.name}")
            lines.append(f"    type: {col.type}")
        lines.append("upstream: []")
        return "\n".join(lines) + "\n"

    @staticmethod
    def _generate_sql(
        name: str, catalog: str, table: str, columns: list[Any],
    ) -> str:
        """Produce a SQL source with Rivet annotation comments."""
        lines = [
            f"-- rivet:name: {name}",
            "-- rivet:type: source",
            f"-- rivet:catalog: {catalog}",
            f"-- rivet:table: {table}",
        ]
        return "\n".join(lines) + "\n"

    def _expand_columns(
        self,
        catalog_name: str,
        catalog: Catalog,
        plugin: CatalogPlugin,
        path: list[str],
    ) -> list[ExplorerNode]:
        """Expand a table node into column ExplorerNodes via get_schema()."""
        table_key = ".".join(path[1:])
        cache_key = (catalog_name, table_key)

        if cache_key not in self._schema_cache:
            try:
                schema = plugin.get_schema(catalog, table_key)
                self._schema_cache[cache_key] = schema
            except Exception:
                return []

        schema = self._schema_cache[cache_key]
        child_depth = len(path)
        return [
            ExplorerNode(
                name=col.name,
                node_type="column",
                path=path + [col.name],
                is_expandable=False,
                depth=child_depth,
                summary=None,
                depth_limit_reached=False,
            )
            for col in schema.columns
        ]
