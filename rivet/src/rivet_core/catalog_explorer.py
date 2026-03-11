"""Catalog Explorer data models and error class for rivet-core."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from rivet_core.errors import RivetError
from rivet_core.fuzzy import fuzzy_match
from rivet_core.introspection import (
    CatalogNode,
    ColumnDetail,
    NodeSummary,
    ObjectMetadata,
    ObjectSchema,
)
from rivet_core.smart_cache import CacheMode, CacheResult, SmartCache

if TYPE_CHECKING:
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
    node_type: (
        str  # "catalog", "database", "schema", "table", "view", "file", "column", "directory"
    )
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
_CREDENTIAL_KEYS = frozenset(
    {
        "password",
        "secret",
        "token",
        "key",
        "credential",
        "credentials",
        "secret_key",
        "access_key",
        "api_key",
        "private_key",
    }
)


def _safe_options_summary(options: dict[str, Any]) -> dict[str, str]:
    """Return a safe subset of catalog options, excluding credentials."""
    return {
        k: str(v) for k, v in options.items() if not any(ck in k.lower() for ck in _CREDENTIAL_KEYS)
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


def _rehydrate_nodes(data: Any) -> list[CatalogNode]:
    """Convert SmartCache data back to CatalogNode objects.

    After a JSON round-trip, CatalogNode objects become plain dicts.
    This reconstructs them. If the data is already CatalogNode objects
    (same-process cache hit), it's returned as-is.
    """
    if not isinstance(data, list):
        return []
    result: list[CatalogNode] = []
    for item in data:
        if isinstance(item, CatalogNode):
            result.append(item)
        elif isinstance(item, dict):
            summary_raw = item.get("summary")
            summary = NodeSummary(**summary_raw) if isinstance(summary_raw, dict) else None
            result.append(
                CatalogNode(
                    name=item["name"],
                    node_type=item["node_type"],
                    path=item["path"],
                    is_container=item["is_container"],
                    children_count=item.get("children_count"),
                    summary=summary,
                    metadata=item.get("metadata", {}),
                )
            )
    return result


def _rehydrate_schema(data: Any) -> ObjectSchema | None:
    """Convert SmartCache data back to an ObjectSchema object.

    After a JSON round-trip, ObjectSchema becomes a plain dict.
    If already an ObjectSchema, returns as-is.
    """
    if isinstance(data, ObjectSchema):
        return data
    if not isinstance(data, dict):
        return None
    columns = [ColumnDetail(**c) if isinstance(c, dict) else c for c in data.get("columns", [])]
    return ObjectSchema(
        path=data["path"],
        node_type=data["node_type"],
        columns=columns,
        primary_key=data.get("primary_key"),
        comment=data.get("comment"),
    )


class CatalogExplorer:
    """Headless catalog browsing service.

    Accepts instantiated catalogs, engines, and a PluginRegistry.
    All catalog browsing logic lives here — no compilation or assembly required.

    When an optional ``SmartCache`` is provided, catalog metadata is
    persisted on disk and reused across sessions.  The ``cache_mode``
    parameter controls behaviour:

    - ``CacheMode.READ_WRITE`` (interactive tools): reads from cache on
      startup for warm-start, performs staleness checks via TTL and
      plugin fingerprinting, and writes updates back.
    - ``CacheMode.WRITE_ONLY`` (non-interactive CLI): always fetches
      live data from plugins but writes results to cache so that future
      interactive sessions benefit.

    Without a ``SmartCache`` the explorer behaves identically to the
    pre-cache implementation (backward compatible).
    """

    def __init__(
        self,
        catalogs: dict[str, Catalog],
        engines: dict[str, ComputeEngine],
        registry: PluginRegistry,
        max_depth: int = 10,
        skip_probe: bool = False,
        smart_cache: SmartCache | None = None,
        cache_mode: CacheMode = CacheMode.READ_WRITE,
    ) -> None:
        self._catalogs = catalogs
        self._engines = engines
        self._registry = registry
        self._max_depth = max_depth
        self._smart_cache = smart_cache
        self._cache_mode = cache_mode
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

    def _connection_hash(self, catalog_name: str) -> str:
        """Compute a stable hash of catalog connection options."""
        catalog = self._catalogs.get(catalog_name)
        options = catalog.options if catalog is not None else {}
        serialized = json.dumps(options, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode()).hexdigest()[:16]

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

        When a SmartCache is provided:
        - READ_WRITE mode checks SmartCache before plugin call; expired
          entries trigger a staleness check via plugin.get_fingerprint().
        - WRITE_ONLY mode always calls the plugin but stores the result.
        - After any successful plugin call the result is stored in SmartCache.

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
        conn_hash = self._connection_hash(catalog_name)

        # Check per-path in-memory cache first
        if cache_key not in self._children_cache:
            # Try SmartCache in READ_WRITE mode
            smart_hit: CacheResult | None = None
            if self._smart_cache is not None and self._cache_mode == CacheMode.READ_WRITE:
                smart_hit = self._smart_cache.get(
                    "children",
                    catalog_name,
                    conn_hash,
                    tuple(sub_path),
                )
                if smart_hit is not None and not smart_hit.expired:
                    # Fresh hit — use cached data directly
                    self._children_cache[cache_key] = _rehydrate_nodes(smart_hit.data)
                elif smart_hit is not None and smart_hit.expired:
                    # Expired — perform staleness check
                    try:
                        fingerprint = plugin.get_fingerprint(catalog, sub_path)
                    except Exception:
                        # Network error — return stale data
                        logger.warning(
                            "Fingerprint check failed for '%s' — returning stale cached data",
                            ".".join(path),
                        )
                        self._children_cache[cache_key] = _rehydrate_nodes(smart_hit.data)
                        fingerprint = smart_hit.fingerprint  # keep existing

                    if cache_key not in self._children_cache:
                        # Fingerprint check succeeded — compare
                        if fingerprint is None:
                            # Plugin doesn't support fingerprinting — refetch
                            pass  # fall through to plugin call
                        elif fingerprint == smart_hit.fingerprint:
                            # Unchanged — reset TTL, use cached data
                            self._smart_cache.reset_ttl(
                                "children",
                                catalog_name,
                                conn_hash,
                                tuple(sub_path),
                            )
                            self._children_cache[cache_key] = _rehydrate_nodes(smart_hit.data)
                        else:
                            # Changed — invalidate and refetch
                            self._smart_cache.invalidate_entry(
                                "children",
                                catalog_name,
                                conn_hash,
                                tuple(sub_path),
                            )

            # If still not in in-memory cache, call plugin
            if cache_key not in self._children_cache:
                try:
                    nodes = plugin.list_children(catalog, sub_path)
                except Exception:
                    nodes = []
                self._children_cache[cache_key] = nodes

                # Store in SmartCache (all modes)
                if self._smart_cache is not None and nodes:
                    try:
                        fp = plugin.get_fingerprint(catalog, sub_path)
                    except Exception:
                        fp = None
                    self._smart_cache.put(
                        "children",
                        catalog_name,
                        conn_hash,
                        tuple(sub_path),
                        nodes,
                        fp,
                    )

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
                has_format = (
                    node.summary is not None and getattr(node.summary, "format", None) is not None
                )
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
            raise CatalogExplorerError(RivetError(code=RVT_871, message="Empty path provided"))
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

        return NodeDetail(
            node=node, schema=schema, metadata=metadata, children_count=children_count
        )

    def get_table_schema(self, path: list[str]) -> ObjectSchema | None:
        """Return schema for the table at path, using cache.

        When a SmartCache is provided the same staleness logic as
        ``list_children()`` applies: READ_WRITE checks SmartCache first,
        WRITE_ONLY always calls the plugin, and results are stored in
        SmartCache after every successful plugin call.
        """
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
        conn_hash = self._connection_hash(catalog_name)
        schema_path = tuple(path[1:])

        if cache_key not in self._schema_cache:
            # Try SmartCache in READ_WRITE mode
            smart_hit: CacheResult | None = None
            if self._smart_cache is not None and self._cache_mode == CacheMode.READ_WRITE:
                smart_hit = self._smart_cache.get(
                    "schema",
                    catalog_name,
                    conn_hash,
                    schema_path,
                )
                if smart_hit is not None and not smart_hit.expired:
                    rehydrated = _rehydrate_schema(smart_hit.data)
                    if rehydrated is not None:
                        self._schema_cache[cache_key] = rehydrated
                elif smart_hit is not None and smart_hit.expired:
                    # Staleness check
                    try:
                        fingerprint = plugin.get_fingerprint(catalog, list(path[1:]))
                    except Exception:
                        logger.warning(
                            "Fingerprint check failed for schema '%s' — returning stale cached data",
                            table_key,
                        )
                        rehydrated = _rehydrate_schema(smart_hit.data)
                        if rehydrated is not None:
                            self._schema_cache[cache_key] = rehydrated
                        fingerprint = smart_hit.fingerprint

                    if cache_key not in self._schema_cache:
                        if fingerprint is None:
                            pass  # fall through to plugin call
                        elif fingerprint == smart_hit.fingerprint:
                            self._smart_cache.reset_ttl(
                                "schema",
                                catalog_name,
                                conn_hash,
                                schema_path,
                            )
                            rehydrated = _rehydrate_schema(smart_hit.data)
                            if rehydrated is not None:
                                self._schema_cache[cache_key] = rehydrated
                        else:
                            self._smart_cache.invalidate_entry(
                                "schema",
                                catalog_name,
                                conn_hash,
                                schema_path,
                            )

            # If still not in in-memory cache, call plugin
            if cache_key not in self._schema_cache:
                try:
                    schema = plugin.get_schema(catalog, table_key)
                except Exception:
                    return None
                self._schema_cache[cache_key] = schema

                # Store in SmartCache (all modes)
                if self._smart_cache is not None:
                    try:
                        fp = plugin.get_fingerprint(catalog, list(path[1:]))
                    except Exception:
                        fp = None
                    self._smart_cache.put(
                        "schema",
                        catalog_name,
                        conn_hash,
                        schema_path,
                        schema,
                        fp,
                    )

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
        """Clear all cached data for the given catalog.

        Clears in-memory caches and, when a SmartCache is present,
        invalidates the catalog's persistent entries as well.
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
        # Invalidate SmartCache entries for this catalog
        if self._smart_cache is not None:
            conn_hash = self._connection_hash(catalog_name)
            self._smart_cache.invalidate_catalog(catalog_name, conn_hash)

    def close(self) -> None:
        """Flush the SmartCache to disk for durability on teardown."""
        if self._smart_cache is not None:
            self._smart_cache.flush()

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

    def search(
        self,
        query: str,
        limit: int = 50,
        expand: bool = True,
        expansion_budget_seconds: float = 2.0,
    ) -> list[SearchResult]:
        """Fuzzy search with progressive deepening and access-priority ranking.

        Phase 1 (always): seed ``_children_cache`` from SmartCache
        (READ_WRITE only, ordered by ``last_accessed`` desc), scan all
        in-memory nodes for fuzzy matches, apply rank-based score bonus
        for high-access entries.  This phase is always instant — no
        network calls.

        Phase 2 (only when ``expand=True`` AND READ_WRITE mode):
        progressively expand unexplored branches via
        ``list_children()``, prioritised by parent ``last_accessed``
        with depth as tiebreaker (breadth-first when priorities are
        equal).  Stops when tree exhausted or time budget expires.
        Results from both phases are merged, sorted by score, and
        truncated to ``limit``.

        In WRITE_ONLY mode only in-memory nodes are scanned, no expansion.
        """
        if not query:
            return []

        # Phase 1: Seed in-memory cache from SmartCache (READ_WRITE only)
        if self._smart_cache is not None and self._cache_mode == CacheMode.READ_WRITE:
            for key, nodes, _last_accessed in self._smart_cache.get_all_children(
                order_by_access=True,
            ):
                catalog_name_sc, path_tuple = key
                cache_key = (catalog_name_sc, path_tuple)
                if cache_key not in self._children_cache:
                    self._children_cache[cache_key] = _rehydrate_nodes(nodes)

        hits: list[SearchResult] = []
        seen: set[str] = set()

        # Scan children_cache (per-path lazy cache)
        for (catalog_name, path_key), nodes in self._children_cache.items():
            conn_hash = self._connection_hash(catalog_name)
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

                # Rank-based access bonus (READ_WRITE only)
                if self._smart_cache is not None and self._cache_mode == CacheMode.READ_WRITE:
                    rank = self._smart_cache.get_access_rank(
                        "children",
                        catalog_name,
                        conn_hash,
                        path_key,
                    )
                    if rank is not None:
                        bonus = max(0.0, 2.0 * (1.0 - rank / 20.0))
                        score -= bonus

                kind = node.node_type
                if kind in ("file", "directory"):
                    kind = "table" if not node.is_container else "schema"
                parent = (
                    catalog_name + "." + ".".join(node.path[:-1]) if len(node.path) > 1 else None
                )
                hits.append(
                    SearchResult(
                        kind=kind,
                        qualified_name=qualified,
                        short_name=node.name,
                        parent=parent,
                        match_positions=positions,
                        score=score,
                        node_type=node.node_type,
                    )
                )

        # Also search legacy tables_cache
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
                parent = (
                    catalog_name + "." + ".".join(node.path[:-1]) if len(node.path) > 1 else None
                )
                hits.append(
                    SearchResult(
                        kind=kind,
                        qualified_name=qualified,
                        short_name=node.name,
                        parent=parent,
                        match_positions=positions,
                        score=score,
                        node_type=node.node_type,
                    )
                )

        # Phase 2: Progressive expansion (READ_WRITE + expand=True only)
        if expand and self._cache_mode == CacheMode.READ_WRITE:
            import heapq
            import time as _time

            deadline = _time.monotonic() + expansion_budget_seconds
            frontier: list[tuple[float, int, list[str]]] = []

            # Build a set of path-segment names that produced good Phase 1
            # hits.  Used to prioritise structurally similar branches in
            # Phase 2 (e.g. if datalake_silver.data_factory_ingest had
            # hits, boost preprod_datalake_silver and its data_factory_ingest).
            _hit_segments: set[str] = set()
            for h in hits:
                for seg in h.qualified_name.split(".")[:-1]:  # parent segments only
                    _hit_segments.add(seg)

            # Track which (catalog, parent_path) combos already produced
            # Phase 1 hits so we skip re-expanding their siblings.
            _hit_parent_keys: set[tuple[str, tuple[str, ...]]] = set()
            for h in hits:
                parts = h.qualified_name.split(".")
                if len(parts) >= 3:
                    # e.g. ("unity", ("datalake_silver",)) for a hit under datalake_silver
                    _hit_parent_keys.add((parts[0], tuple(parts[1:-1])))

            def _segment_priority(name: str) -> float:
                """Priority boost if *name* overlaps with a hit-producing path segment."""
                # Exact match: this node name appeared in a hit path
                if name in _hit_segments:
                    return 20.0
                # Substring containment: e.g. "preprod_datalake_silver"
                # contains "datalake_silver" which is a hit segment.
                for seg in _hit_segments:
                    if seg in name or name in seg:
                        return 10.0
                return 0.0

            for cat_name in self._catalogs:
                cat_cache_key = (cat_name, ())
                conn_h = self._connection_hash(cat_name)
                if cat_cache_key not in self._children_cache:
                    priority: float = _segment_priority(cat_name)
                    if self._smart_cache is not None:
                        la = self._smart_cache.get_last_accessed(
                            "children",
                            cat_name,
                            conn_h,
                            (),
                        )
                        if la is not None:
                            priority = max(priority, la)
                    heapq.heappush(frontier, (-priority, 1, [cat_name]))

            # Also seed frontier from already-cached levels so Phase 2
            # can drill into branches that Phase 1 (SmartCache) seeded
            # but whose children haven't been explored yet.
            # Skip children with no segment relevance when their parent
            # already produced Phase 1 hits — avoids wasting budget on
            # dozens of irrelevant sibling schemas (e.g. datalake_silver
            # .greenforge, .genesys) when we already have hits from
            # datalake_silver.data_factory_ingest.
            for (cached_cat, _cached_sub), cached_nodes in list(self._children_cache.items()):
                if cached_cat not in self._catalogs:
                    continue
                parent_key = (cached_cat, _cached_sub)
                parent_had_hits = parent_key in _hit_parent_keys
                conn_h = self._connection_hash(cached_cat)
                for cnode in cached_nodes:
                    if not cnode.is_container:
                        continue
                    child_key = (cached_cat, tuple(cnode.path))
                    if child_key in self._children_cache:
                        continue  # already cached — will be scanned deeper
                    seg_prio = _segment_priority(cnode.name)
                    # If this parent already produced hits, only queue
                    # children that have segment relevance — skip the
                    # rest to preserve budget for unexplored catalogs.
                    if parent_had_hits and seg_prio == 0.0:
                        continue
                    la_val_seed: float = 0.0
                    if self._smart_cache is not None:
                        la_seed = self._smart_cache.get_last_accessed(
                            "children",
                            cached_cat,
                            conn_h,
                            tuple(cnode.path),
                        )
                        if la_seed is not None:
                            la_val_seed = la_seed
                    seed_priority = max(seg_prio, la_val_seed)
                    child_path = [cached_cat] + list(cnode.path)
                    heapq.heappush(
                        frontier,
                        (-seed_priority, len(child_path), child_path),
                    )

            while frontier and _time.monotonic() < deadline:
                _, _depth, exp_path = heapq.heappop(frontier)
                if len(exp_path) >= self._max_depth:
                    continue
                new_nodes = self.list_children(exp_path)
                catalog_name = exp_path[0]
                conn_hash = self._connection_hash(catalog_name)
                for enode in new_nodes:
                    qualified = ".".join(enode.path)
                    if qualified in seen:
                        # Only queue containers (schemas/databases) for
                        # deeper expansion — skip tables to avoid pulling
                        # in columns that pollute search results.
                        if (
                            enode.is_expandable
                            and enode.node_type not in self._TABLE_NODE_TYPES
                            and _time.monotonic() < deadline
                        ):
                            child_path = enode.path
                            child_cache_key = (catalog_name, tuple(child_path[1:]))
                            if child_cache_key not in self._children_cache:
                                seg_p: float = _segment_priority(enode.name)
                                la_val: float = 0.0
                                if self._smart_cache is not None:
                                    la_check = self._smart_cache.get_last_accessed(
                                        "children",
                                        catalog_name,
                                        conn_hash,
                                        tuple(child_path[1:]),
                                    )
                                    if la_check is not None:
                                        la_val = la_check
                                heapq.heappush(
                                    frontier, (-max(seg_p, la_val), len(child_path), child_path)
                                )
                        continue
                    seen.add(qualified)
                    result = fuzzy_match(query, qualified)
                    if result is not None:
                        score, positions = result
                        score += _NODE_TYPE_BONUS.get(enode.node_type, 0.0)
                        kind = enode.node_type
                        if kind in ("file", "directory"):
                            kind = "table" if not enode.is_expandable else "schema"
                        parent = ".".join(enode.path[:-1]) if len(enode.path) > 1 else None
                        hits.append(
                            SearchResult(
                                kind=kind,
                                qualified_name=qualified,
                                short_name=enode.name,
                                parent=parent,
                                match_positions=positions,
                                score=score,
                                node_type=enode.node_type,
                            )
                        )
                    # Queue container children for further exploration
                    # (skip tables — don't drill into columns during search)
                    if (
                        enode.is_expandable
                        and enode.node_type not in self._TABLE_NODE_TYPES
                        and _time.monotonic() < deadline
                    ):
                        child_path = enode.path
                        child_cache_key = (catalog_name, tuple(child_path[1:]))
                        if child_cache_key not in self._children_cache:
                            seg_p = _segment_priority(enode.name)
                            la_val = 0.0
                            if self._smart_cache is not None:
                                la_check = self._smart_cache.get_last_accessed(
                                    "children",
                                    catalog_name,
                                    conn_hash,
                                    tuple(child_path[1:]),
                                )
                                if la_check is not None:
                                    la_val = la_check
                            heapq.heappush(
                                frontier, (-max(seg_p, la_val), len(child_path), child_path)
                            )

        hits.sort(key=lambda r: r.score)

        # Drop low-quality scattered-subsequence matches.  With the
        # contiguous-substring bonus in fuzzy_match, genuine hits score
        # ~40+ points better than scattered noise.  A 20-point window
        # from the best hit keeps all real matches and discards garbage.
        if hits:
            best = hits[0].score
            hits = [h for h in hits if h.score <= best + 20.0]

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
        name: str,
        catalog: str,
        table: str,
        columns: list[Any],
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
        name: str,
        catalog: str,
        table: str,
        columns: list[Any],
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
