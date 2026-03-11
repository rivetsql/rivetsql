"""Property-based tests for SmartCache and CatalogExplorer + SmartCache integration.

Uses Hypothesis to generate random cache entries, catalog trees, and
connection parameters, then verifies correctness properties hold across
all generated inputs.

Uses a minimal concrete CatalogPlugin stub (not mocks) consistent with
the integration test strategy.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.catalog_explorer import CatalogExplorer
from rivet_core.introspection import CatalogNode, ColumnDetail, ObjectSchema
from rivet_core.models import Catalog
from rivet_core.plugins import CatalogPlugin, PluginRegistry
from rivet_core.smart_cache import (
    CacheEntry,
    CacheMode,
    SmartCache,
    _deserialize_entry,
    _serialize_entry,
)

# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------

# Safe text that avoids problematic characters for cache keys (no dots or
# colons since those are used as separators in entry keys).
_safe_name = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N"), whitelist_characters="_-"),
    min_size=1,
    max_size=20,
)

_path_segment = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N"), whitelist_characters="_"),
    min_size=1,
    max_size=10,
)

_fingerprint = st.one_of(st.none(), st.text(min_size=1, max_size=32))

_entry_type = st.sampled_from(["children", "schema", "metadata"])

_column_detail = st.builds(
    ColumnDetail,
    name=_safe_name,
    type=st.sampled_from(["int64", "utf8", "float64", "bool"]),
    native_type=st.sampled_from(["INTEGER", "VARCHAR", "DOUBLE", "BOOLEAN"]),
    nullable=st.booleans(),
    default=st.none(),
    comment=st.none(),
    is_primary_key=st.booleans(),
    is_partition_key=st.booleans(),
)

_catalog_node = st.builds(
    CatalogNode,
    name=_safe_name,
    node_type=st.sampled_from(["table", "view", "schema"]),
    path=st.lists(_path_segment, min_size=1, max_size=3),
    is_container=st.booleans(),
    children_count=st.one_of(st.none(), st.integers(min_value=0, max_value=100)),
    summary=st.none(),
)

_children_data = st.lists(_catalog_node, min_size=0, max_size=5)

_schema_data = st.builds(
    ObjectSchema,
    path=st.lists(_path_segment, min_size=1, max_size=3),
    node_type=st.just("table"),
    columns=st.lists(_column_detail, min_size=1, max_size=5),
    primary_key=st.one_of(st.none(), st.just([])),
    comment=st.none(),
)

# Strategy for cache entry data — either children list or schema dict
_cache_data = st.one_of(
    _children_data.map(lambda nodes: [_node_to_dict(n) for n in nodes]),
    _schema_data.map(lambda s: _schema_to_dict(s)),
)

_ttl = st.floats(min_value=1.0, max_value=3600.0, allow_nan=False, allow_infinity=False)

_timestamp = st.floats(
    min_value=1_000_000_000.0,
    max_value=2_000_000_000.0,
    allow_nan=False,
    allow_infinity=False,
)


def _node_to_dict(node: CatalogNode) -> dict[str, Any]:
    """Convert a CatalogNode to a JSON-serializable dict."""
    return {
        "name": node.name,
        "node_type": node.node_type,
        "path": node.path,
        "is_container": node.is_container,
        "children_count": node.children_count,
        "summary": None,
        "metadata": {},
    }


def _schema_to_dict(schema: ObjectSchema) -> dict[str, Any]:
    """Convert an ObjectSchema to a JSON-serializable dict."""
    return {
        "path": schema.path,
        "node_type": schema.node_type,
        "columns": [
            {
                "name": c.name,
                "type": c.type,
                "native_type": c.native_type,
                "nullable": c.nullable,
                "default": c.default,
                "comment": c.comment,
                "is_primary_key": c.is_primary_key,
                "is_partition_key": c.is_partition_key,
            }
            for c in schema.columns
        ],
        "primary_key": schema.primary_key,
        "comment": schema.comment,
    }


# ---------------------------------------------------------------------------
# Stub plugin (same pattern as integration tests)
# ---------------------------------------------------------------------------

CATALOG_NAME = "propcat"
CONN_OPTIONS: dict[str, Any] = {"host": "localhost"}


class StubCatalogPlugin(CatalogPlugin):
    """Minimal CatalogPlugin for property tests."""

    type = "stub"
    required_options: list[str] = []
    optional_options: dict[str, Any] = {}
    credential_options: list[str] = []

    def __init__(self) -> None:
        self.list_children_count = 0
        self.get_schema_count = 0
        self.get_fingerprint_count = 0
        self._children: dict[str, list[CatalogNode]] = {}
        self._schemas: dict[str, ObjectSchema] = {}
        self._fingerprints: dict[str, str | None] = {}

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog:
        return Catalog(name=name, type="stub", options=options)

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        return logical_name

    def list_tables(self, catalog: Catalog) -> list[CatalogNode]:
        return []

    def test_connection(self, catalog: Catalog) -> None:
        pass

    def list_children(self, catalog: Catalog, path: list[str]) -> list[CatalogNode]:
        self.list_children_count += 1
        key = ".".join(path)
        return self._children.get(key, [])

    def get_schema(self, catalog: Catalog, table: str) -> ObjectSchema:
        self.get_schema_count += 1
        return self._schemas[table]

    def get_fingerprint(self, catalog: Catalog, path: list[str]) -> str | None:
        self.get_fingerprint_count += 1
        key = ".".join(path)
        return self._fingerprints.get(key)

    def set_children(self, path_key: str, nodes: list[CatalogNode]) -> None:
        self._children[path_key] = nodes

    def set_schema(self, table_key: str, schema: ObjectSchema) -> None:
        self._schemas[table_key] = schema

    def set_fingerprint(self, path_key: str, fp: str | None) -> None:
        self._fingerprints[path_key] = fp


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _conn_hash(options: dict[str, Any] | None = None) -> str:
    opts = options if options is not None else CONN_OPTIONS
    serialized = json.dumps(opts, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def _make_catalog(name: str = CATALOG_NAME, options: dict[str, Any] | None = None) -> Catalog:
    return Catalog(name=name, type="stub", options=options or CONN_OPTIONS)


def _make_registry(plugin: StubCatalogPlugin) -> PluginRegistry:
    registry = PluginRegistry()
    registry.register_catalog_plugin(plugin)
    return registry


def _make_node(
    name: str,
    path: list[str],
    node_type: str = "table",
    is_container: bool = False,
) -> CatalogNode:
    return CatalogNode(
        name=name,
        node_type=node_type,
        path=path,
        is_container=is_container,
        children_count=None,
        summary=None,
    )


def _make_schema(path: list[str]) -> ObjectSchema:
    return ObjectSchema(
        path=path,
        node_type="table",
        columns=[
            ColumnDetail(
                name="id",
                type="int64",
                native_type="INTEGER",
                nullable=False,
                default=None,
                comment=None,
                is_primary_key=True,
                is_partition_key=False,
            ),
        ],
        primary_key=["id"],
        comment=None,
    )


def _make_explorer(
    plugin: StubCatalogPlugin,
    cache: SmartCache | None = None,
    cache_mode: CacheMode = CacheMode.READ_WRITE,
    catalogs: dict[str, Catalog] | None = None,
    max_depth: int = 10,
) -> CatalogExplorer:
    cats = catalogs or {CATALOG_NAME: _make_catalog()}
    registry = _make_registry(plugin)
    return CatalogExplorer(
        catalogs=cats,
        engines={},
        registry=registry,
        max_depth=max_depth,
        skip_probe=True,
        smart_cache=cache,
        cache_mode=cache_mode,
    )


def _fresh_cache_dir() -> Path:
    """Create a fresh temporary directory for each Hypothesis iteration."""
    return Path(tempfile.mkdtemp())


def _cleanup(cache_dir: Path) -> None:
    """Remove a temporary cache directory."""
    shutil.rmtree(cache_dir, ignore_errors=True)


# ===========================================================================
# Property 1: Cache entry serialization round-trip (Req 1.6)
# ===========================================================================


@given(
    data=_cache_data,
    fingerprint=_fingerprint,
    created_at=_timestamp,
    last_accessed=_timestamp,
    ttl=_ttl,
    entry_type=_entry_type,
)
@settings(max_examples=100)
def test_cache_entry_serialization_round_trip(
    data: Any,
    fingerprint: str | None,
    created_at: float,
    last_accessed: float,
    ttl: float,
    entry_type: str,
) -> None:
    """Serializing a CacheEntry to JSON and deserializing it back produces
    an equivalent CacheEntry with identical data, fingerprint, and metadata."""
    entry = CacheEntry(
        data=data,
        fingerprint=fingerprint,
        created_at=created_at,
        last_accessed=last_accessed,
        ttl=ttl,
        entry_type=entry_type,
    )
    serialized = _serialize_entry(entry)
    json_str = json.dumps(serialized, default=str)
    raw = json.loads(json_str)
    restored = _deserialize_entry(raw)

    assert restored.data == entry.data
    assert restored.fingerprint == entry.fingerprint
    assert restored.created_at == entry.created_at
    assert restored.last_accessed == entry.last_accessed
    assert restored.ttl == entry.ttl
    assert restored.entry_type == entry.entry_type


# ===========================================================================
# Property 2: Cache hit avoids plugin call in READ_WRITE mode (Req 1.2, 1.3, 4.2)
# ===========================================================================


@given(
    num_schemas=st.integers(min_value=1, max_value=5),
    num_tables_per_schema=st.integers(min_value=1, max_value=3),
)
@settings(max_examples=100)
def test_cache_hit_avoids_plugin_call_read_write(
    num_schemas: int,
    num_tables_per_schema: int,
) -> None:
    """In READ_WRITE mode, list_children and get_table_schema return cached
    data without invoking the CatalogPlugin when entries are valid."""
    cache_dir = _fresh_cache_dir()
    try:
        cache = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=300.0)
        plugin = StubCatalogPlugin()
        conn_hash = _conn_hash()

        for i in range(num_schemas):
            schema_name = f"s{i}"
            schema_nodes = [
                _make_node(f"t{j}", [schema_name, f"t{j}"], "table")
                for j in range(num_tables_per_schema)
            ]
            cache.put("children", CATALOG_NAME, conn_hash, (schema_name,), schema_nodes)

            for j in range(num_tables_per_schema):
                table_path = [schema_name, f"t{j}"]
                schema = _make_schema(table_path)
                cache.put("schema", CATALOG_NAME, conn_hash, tuple(table_path), schema)
                plugin.set_schema(f"{schema_name}.t{j}", schema)

        for i in range(num_schemas):
            plugin.set_children(
                f"s{i}",
                [_make_node("fresh", [f"s{i}", "fresh"], "table")],
            )

        explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.READ_WRITE)
        explorer._connection_status[CATALOG_NAME] = (True, None)

        for i in range(num_schemas):
            explorer.list_children([CATALOG_NAME, f"s{i}"])
        assert plugin.list_children_count == 0

        for i in range(num_schemas):
            for j in range(num_tables_per_schema):
                explorer.get_table_schema([CATALOG_NAME, f"s{i}", f"t{j}"])
        assert plugin.get_schema_count == 0
    finally:
        _cleanup(cache_dir)


# ===========================================================================
# Property 2b: Write-only mode always calls plugin but stores result (Req 4.3)
# ===========================================================================


@given(num_paths=st.integers(min_value=1, max_value=5))
@settings(max_examples=100)
def test_write_only_always_calls_plugin_and_stores(num_paths: int) -> None:
    """In WRITE_ONLY mode, the plugin is always called regardless of cache
    state, and the result is stored in SmartCache afterward."""
    cache_dir = _fresh_cache_dir()
    try:
        cache = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=300.0)
        plugin = StubCatalogPlugin()
        conn_hash = _conn_hash()

        for i in range(num_paths):
            cache.put(
                "children",
                CATALOG_NAME,
                conn_hash,
                (f"s{i}",),
                [_make_node(f"cached_{i}", [f"s{i}", f"cached_{i}"], "table")],
            )

        for i in range(num_paths):
            plugin.set_children(
                f"s{i}", [_make_node(f"fresh_{i}", [f"s{i}", f"fresh_{i}"], "table")]
            )

        explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.WRITE_ONLY)
        explorer._connection_status[CATALOG_NAME] = (True, None)

        for i in range(num_paths):
            result = explorer.list_children([CATALOG_NAME, f"s{i}"])
            assert len(result) >= 1
            assert result[0].name == f"fresh_{i}"

        assert plugin.list_children_count == num_paths

        for i in range(num_paths):
            cached = cache.get("children", CATALOG_NAME, conn_hash, (f"s{i}",))
            assert cached is not None
            assert cached.data[0].name == f"fresh_{i}"
    finally:
        _cleanup(cache_dir)


# ===========================================================================
# Property 3: Connection hash change invalidates entries (Req 1.5)
# ===========================================================================


@given(
    num_entries=st.integers(min_value=1, max_value=10),
    original_host=_safe_name,
    new_host=_safe_name,
)
@settings(max_examples=100)
def test_connection_hash_change_invalidates_entries(
    num_entries: int,
    original_host: str,
    new_host: str,
) -> None:
    """When connection options change (different hash), all previously cached
    entries become unreachable (cache misses)."""
    cache_dir = _fresh_cache_dir()
    try:
        cache = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=300.0)

        original_opts = {"host": original_host}
        new_opts = {"host": new_host + "_changed"}
        original_hash = _conn_hash(original_opts)
        new_hash = _conn_hash(new_opts)

        for i in range(num_entries):
            cache.put(
                "children",
                CATALOG_NAME,
                original_hash,
                (f"path{i}",),
                [{"name": f"node{i}"}],
            )

        for i in range(num_entries):
            result = cache.get("children", CATALOG_NAME, original_hash, (f"path{i}",))
            assert result is not None

        for i in range(num_entries):
            result = cache.get("children", CATALOG_NAME, new_hash, (f"path{i}",))
            assert result is None
    finally:
        _cleanup(cache_dir)


# ===========================================================================
# Property 5: Changed fingerprint triggers fresh fetch (Req 2.3, 2.4)
# ===========================================================================


@given(num_entries=st.integers(min_value=1, max_value=5))
@settings(max_examples=100)
def test_changed_fingerprint_triggers_fresh_fetch(num_entries: int) -> None:
    """When a cached entry is expired and the plugin returns a different
    fingerprint, the explorer invalidates the entry and fetches fresh data."""
    cache_dir = _fresh_cache_dir()
    try:
        cache = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=1.0)
        plugin = StubCatalogPlugin()
        conn_hash = _conn_hash()

        for i in range(num_entries):
            path_key = f"s{i}"
            old_nodes = [_make_node(f"old_{i}", [path_key, f"old_{i}"], "table")]
            cache.put(
                "children",
                CATALOG_NAME,
                conn_hash,
                (path_key,),
                old_nodes,
                fingerprint=f"fp_old_{i}",
            )
            plugin.set_children(path_key, [_make_node(f"new_{i}", [path_key, f"new_{i}"], "table")])
            plugin.set_fingerprint(path_key, f"fp_new_{i}")

        explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.READ_WRITE)
        explorer._connection_status[CATALOG_NAME] = (True, None)

        original_time = time.time()
        with patch("rivet_core.smart_cache.time") as mock_time:
            mock_time.time.return_value = original_time + 2.0
            mock_time.monotonic.return_value = time.monotonic()
            for i in range(num_entries):
                result = explorer.list_children([CATALOG_NAME, f"s{i}"])
                assert len(result) >= 1
                assert result[0].name == f"new_{i}"

        assert plugin.list_children_count == num_entries
    finally:
        _cleanup(cache_dir)


# ===========================================================================
# Property 6: Unchanged fingerprint resets TTL (Req 2.5)
# ===========================================================================


@given(num_entries=st.integers(min_value=1, max_value=5))
@settings(max_examples=100)
def test_unchanged_fingerprint_resets_ttl(num_entries: int) -> None:
    """When a cached entry is expired but the plugin returns the same
    fingerprint, the TTL is reset and cached data is returned without
    a data fetch."""
    cache_dir = _fresh_cache_dir()
    try:
        cache = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=1.0)
        plugin = StubCatalogPlugin()
        conn_hash = _conn_hash()

        for i in range(num_entries):
            path_key = f"s{i}"
            nodes = [_make_node(f"cached_{i}", [path_key, f"cached_{i}"], "table")]
            cache.put(
                "children", CATALOG_NAME, conn_hash, (path_key,), nodes, fingerprint=f"fp_{i}"
            )
            plugin.set_fingerprint(path_key, f"fp_{i}")
            plugin.set_children(
                path_key, [_make_node(f"fresh_{i}", [path_key, f"fresh_{i}"], "table")]
            )

        explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.READ_WRITE)
        explorer._connection_status[CATALOG_NAME] = (True, None)

        original_time = time.time()
        with patch("rivet_core.smart_cache.time") as mock_time:
            mock_time.time.return_value = original_time + 2.0
            mock_time.monotonic.return_value = time.monotonic()
            for i in range(num_entries):
                result = explorer.list_children([CATALOG_NAME, f"s{i}"])
                assert len(result) >= 1
                assert result[0].name == f"cached_{i}"

        assert plugin.list_children_count == 0
        assert plugin.get_fingerprint_count == num_entries

        for i in range(num_entries):
            cached = cache.get("children", CATALOG_NAME, conn_hash, (f"s{i}",))
            assert cached is not None
            assert cached.expired is False
    finally:
        _cleanup(cache_dir)


# ===========================================================================
# Property 7: Successful plugin call stores result in cache (Req 4.3)
# ===========================================================================


@given(
    num_paths=st.integers(min_value=1, max_value=5),
    use_read_write=st.booleans(),
)
@settings(max_examples=100)
def test_successful_plugin_call_stores_result(
    num_paths: int,
    use_read_write: bool,
) -> None:
    """After a CatalogPlugin method is called due to a cache miss, the
    returned data is present in the SmartCache immediately afterward,
    regardless of cache mode."""
    cache_dir = _fresh_cache_dir()
    try:
        cache = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=300.0)
        plugin = StubCatalogPlugin()
        conn_hash = _conn_hash()
        mode = CacheMode.READ_WRITE if use_read_write else CacheMode.WRITE_ONLY

        for i in range(num_paths):
            plugin.set_children(f"s{i}", [_make_node(f"t{i}", [f"s{i}", f"t{i}"], "table")])

        explorer = _make_explorer(plugin, cache=cache, cache_mode=mode)
        explorer._connection_status[CATALOG_NAME] = (True, None)

        for i in range(num_paths):
            explorer.list_children([CATALOG_NAME, f"s{i}"])

        for i in range(num_paths):
            cached = cache.get("children", CATALOG_NAME, conn_hash, (f"s{i}",))
            assert cached is not None
            assert len(cached.data) == 1
            assert cached.data[0].name == f"t{i}"
    finally:
        _cleanup(cache_dir)


# ===========================================================================
# Property 8: refresh_catalog invalidates all entries (Req 4.4, 5.3)
# ===========================================================================


@given(
    num_catalogs=st.integers(min_value=2, max_value=4),
    num_entries_per_catalog=st.integers(min_value=1, max_value=5),
    refresh_index=st.integers(min_value=0, max_value=3),
)
@settings(max_examples=100)
def test_refresh_catalog_invalidates_all_entries(
    num_catalogs: int,
    num_entries_per_catalog: int,
    refresh_index: int,
) -> None:
    """Calling refresh_catalog removes all SmartCache entries for that catalog
    only, leaving other catalogs' entries intact."""
    cache_dir = _fresh_cache_dir()
    try:
        cache = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=300.0)
        plugin = StubCatalogPlugin()

        catalog_names = [f"cat{i}" for i in range(num_catalogs)]
        catalogs = {name: _make_catalog(name) for name in catalog_names}

        for cat_name in catalog_names:
            cat_hash = _conn_hash(catalogs[cat_name].options)
            for j in range(num_entries_per_catalog):
                cache.put(
                    "children",
                    cat_name,
                    cat_hash,
                    (f"path{j}",),
                    [_make_node(f"n{j}", [f"path{j}", f"n{j}"], "table")],
                )
                plugin.set_children(
                    f"path{j}", [_make_node(f"n{j}", [f"path{j}", f"n{j}"], "table")]
                )

        explorer = _make_explorer(plugin, cache=cache, catalogs=catalogs)
        for cat_name in catalog_names:
            explorer._connection_status[cat_name] = (True, None)

        target_idx = refresh_index % num_catalogs
        target_name = catalog_names[target_idx]
        explorer.refresh_catalog(target_name)

        target_hash = _conn_hash(catalogs[target_name].options)
        for j in range(num_entries_per_catalog):
            assert cache.get("children", target_name, target_hash, (f"path{j}",)) is None

        for cat_name in catalog_names:
            if cat_name == target_name:
                continue
            cat_hash = _conn_hash(catalogs[cat_name].options)
            for j in range(num_entries_per_catalog):
                assert cache.get("children", cat_name, cat_hash, (f"path{j}",)) is not None
    finally:
        _cleanup(cache_dir)


# ===========================================================================
# Property 12: Search uses cached children and expands with access priority
# (Req 6.1, 6.3, 6.4, 6.5, 6.8, 6.9, 6.10, 6.11)
# ===========================================================================


@given(
    num_cached_schemas=st.integers(min_value=1, max_value=3),
    num_tables_per_schema=st.integers(min_value=1, max_value=3),
)
@settings(max_examples=100)
def test_search_uses_cache_and_expands_with_access_priority(
    num_cached_schemas: int,
    num_tables_per_schema: int,
) -> None:
    """Search seeds from SmartCache, applies access-priority ranking, and
    progressively expands unexplored branches ordered by access time."""
    cache_dir = _fresh_cache_dir()
    try:
        cache = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=300.0)
        plugin = StubCatalogPlugin()
        conn_hash = _conn_hash()

        # Use identical table names across schemas so fuzzy scores are
        # comparable — only the schema prefix differs, and the access-rank
        # bonus should break the tie.
        for i in range(num_cached_schemas):
            schema_name = f"sx{i}"  # same length prefix for all
            table_nodes = [
                _make_node(f"orders{j}", [schema_name, f"orders{j}"], "table")
                for j in range(num_tables_per_schema)
            ]
            cache.put("children", CATALOG_NAME, conn_hash, (schema_name,), table_nodes)

        # Access schemas in reverse order so sx0 is accessed LAST and has
        # the most recent last_accessed (rank 0 = hottest).
        for i in reversed(range(num_cached_schemas)):
            cache.get("children", CATALOG_NAME, conn_hash, (f"sx{i}",))

        # Set up an unexplored branch for expansion
        plugin.set_children(
            "", [_make_node("unexplored", ["unexplored"], "schema", is_container=True)]
        )
        plugin.set_children(
            "unexplored",
            [
                _make_node("orders_deep", ["unexplored", "orders_deep"], "table"),
            ],
        )

        explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.READ_WRITE)
        explorer._connection_status[CATALOG_NAME] = (True, None)

        results = explorer.search("orders", limit=50, expand=True, expansion_budget_seconds=5.0)
        result_names = {r.short_name for r in results}

        # (a) Cached nodes found
        for _i in range(num_cached_schemas):
            for j in range(num_tables_per_schema):
                assert f"orders{j}" in result_names

        # (b) Verify access-rank bonus is applied: entries from the hot
        # schema (sx0, most accessed) should have a score bonus compared
        # to the cold schema (last index, least accessed).  We check that
        # the hot schema's entry appears at or before the cold one in the
        # sorted results list.
        if num_cached_schemas >= 2 and num_tables_per_schema >= 1:
            hot_qn = f"{CATALOG_NAME}.sx0.orders0"
            last = num_cached_schemas - 1
            cold_qn = f"{CATALOG_NAME}.sx{last}.orders0"
            hot_idx = next(
                (idx for idx, r in enumerate(results) if r.qualified_name == hot_qn), None
            )
            cold_idx = next(
                (idx for idx, r in enumerate(results) if r.qualified_name == cold_qn), None
            )
            if hot_idx is not None and cold_idx is not None:
                assert hot_idx <= cold_idx

        # (c) Unexplored branch expanded
        assert "orders_deep" in result_names
    finally:
        _cleanup(cache_dir)


# ===========================================================================
# Property 12b: Write-only search does not expand (Req 6.7)
# ===========================================================================


@given(num_schemas=st.integers(min_value=1, max_value=3))
@settings(max_examples=100)
def test_write_only_search_no_expansion(num_schemas: int) -> None:
    """In WRITE_ONLY mode, search does not call list_children for unexplored
    branches regardless of the expand flag."""
    cache_dir = _fresh_cache_dir()
    try:
        cache = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=300.0)
        plugin = StubCatalogPlugin()

        for i in range(num_schemas):
            plugin.set_children(
                f"s{i}",
                [
                    _make_node(f"target_{i}", [f"s{i}", f"target_{i}"], "table"),
                ],
            )

        explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.WRITE_ONLY)
        explorer._connection_status[CATALOG_NAME] = (True, None)

        results = explorer.search("target", limit=50, expand=True)
        assert plugin.list_children_count == 0
        assert len(results) == 0
    finally:
        _cleanup(cache_dir)


# ===========================================================================
# Property 12c: Search with expand=False skips expansion (Req 6.12, 6.13)
# ===========================================================================


@given(num_cached=st.integers(min_value=1, max_value=3))
@settings(max_examples=100)
def test_search_expand_false_skips_expansion(num_cached: int) -> None:
    """In READ_WRITE mode with expand=False, only Phase 1 (cached scan)
    executes — no list_children calls for unexplored branches."""
    cache_dir = _fresh_cache_dir()
    try:
        cache = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=300.0)
        plugin = StubCatalogPlugin()
        conn_hash = _conn_hash()

        for i in range(num_cached):
            nodes = [_make_node(f"cached_{i}", [f"s{i}", f"cached_{i}"], "table")]
            cache.put("children", CATALOG_NAME, conn_hash, (f"s{i}",), nodes)

        plugin.set_children(
            "", [_make_node("unexplored", ["unexplored"], "schema", is_container=True)]
        )
        plugin.set_children(
            "unexplored", [_make_node("deep_table", ["unexplored", "deep_table"], "table")]
        )

        explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.READ_WRITE)
        explorer._connection_status[CATALOG_NAME] = (True, None)

        results = explorer.search("cached", limit=50, expand=False)
        result_names = {r.short_name for r in results}
        for i in range(num_cached):
            assert f"cached_{i}" in result_names
        assert plugin.list_children_count == 0
    finally:
        _cleanup(cache_dir)


# ===========================================================================
# Property 13: LRU eviction keeps cache within size bounds (Req 7.1, 7.2)
# ===========================================================================


@given(num_entries=st.integers(min_value=5, max_value=20))
@settings(max_examples=100)
def test_lru_eviction_keeps_cache_within_size_bounds(num_entries: int) -> None:
    """When entries exceed max_size_bytes, LRU eviction removes the least
    recently accessed entries and keeps total size within bounds."""
    cache_dir = _fresh_cache_dir()
    try:
        cache = SmartCache(
            profile="test",
            cache_dir=cache_dir,
            default_ttl=300.0,
            max_size_bytes=2048,
            flush_interval=999.0,
        )
        conn_hash = _conn_hash()

        for i in range(num_entries):
            data = [{"name": f"node_{i}", "payload": "x" * 50}]
            cache.put("children", CATALOG_NAME, conn_hash, (f"path{i}",), data)

        # Access the last few entries to make them "hot"
        hot_count = min(3, num_entries)
        for i in range(num_entries - hot_count, num_entries):
            cache.get("children", CATALOG_NAME, conn_hash, (f"path{i}",))

        assert cache.stats["total_size_bytes"] <= 2048

        if cache.stats["total_entries"] < num_entries:
            # Eviction occurred — hot entries should survive
            for i in range(num_entries - hot_count, num_entries):
                result = cache.get("children", CATALOG_NAME, conn_hash, (f"path{i}",))
                assert result is not None, f"Hot entry {i} was evicted"
    finally:
        _cleanup(cache_dir)


# ===========================================================================
# Property 14: Read access updates last_accessed time (Req 7.3)
# ===========================================================================


@given(num_entries=st.integers(min_value=1, max_value=10))
@settings(max_examples=100)
def test_read_access_updates_last_accessed(num_entries: int) -> None:
    """Reading a cache entry via get() updates its last_accessed timestamp."""
    cache_dir = _fresh_cache_dir()
    try:
        cache = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=300.0)
        conn_hash = _conn_hash()

        for i in range(num_entries):
            cache.put("children", CATALOG_NAME, conn_hash, (f"p{i}",), [{"name": f"n{i}"}])

        initial_times: dict[int, float | None] = {}
        for i in range(num_entries):
            initial_times[i] = cache.get_last_accessed(
                "children", CATALOG_NAME, conn_hash, (f"p{i}",)
            )

        time.sleep(0.01)

        for i in range(num_entries):
            result = cache.get("children", CATALOG_NAME, conn_hash, (f"p{i}",))
            assert result is not None

        for i in range(num_entries):
            new_time = cache.get_last_accessed("children", CATALOG_NAME, conn_hash, (f"p{i}",))
            assert new_time is not None
            assert initial_times[i] is not None
            assert new_time >= initial_times[i]  # type: ignore[operator]
    finally:
        _cleanup(cache_dir)


# ===========================================================================
# Property 15: Per-catalog file isolation (Req 1.1, 1.9)
# ===========================================================================


@given(
    num_catalogs=st.integers(min_value=2, max_value=5),
    num_entries=st.integers(min_value=1, max_value=3),
)
@settings(max_examples=100)
def test_per_catalog_file_isolation(num_catalogs: int, num_entries: int) -> None:
    """Each catalog has its own file on disk. Modifying one catalog's entries
    does not affect other catalogs' files."""
    cache_dir = _fresh_cache_dir()
    try:
        cache = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=300.0)

        catalog_names = [f"cat{i}" for i in range(num_catalogs)]
        conn_hashes = {name: _conn_hash({"host": name}) for name in catalog_names}

        for cat_name in catalog_names:
            for j in range(num_entries):
                cache.put(
                    "children",
                    cat_name,
                    conn_hashes[cat_name],
                    (f"p{j}",),
                    [{"name": f"n{j}"}],
                )
        cache.flush()

        profile_dir = cache_dir / "test"
        files = list(profile_dir.glob("*.json"))
        assert len(files) == num_catalogs

        target = catalog_names[0]
        cache.put(
            "children",
            target,
            conn_hashes[target],
            ("new_path",),
            [{"name": "new_node"}],
        )
        cache.flush()

        cache2 = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=300.0)
        for cat_name in catalog_names[1:]:
            for j in range(num_entries):
                result = cache2.get("children", cat_name, conn_hashes[cat_name], (f"p{j}",))
                assert result is not None
                assert result.data == [{"name": f"n{j}"}]
    finally:
        _cleanup(cache_dir)


# ===========================================================================
# Property 16: Flush durability (Req 9.2)
# ===========================================================================


@given(
    num_catalogs=st.integers(min_value=1, max_value=4),
    num_entries=st.integers(min_value=1, max_value=5),
)
@settings(max_examples=100)
def test_flush_durability(num_catalogs: int, num_entries: int) -> None:
    """After flush(), creating a new SmartCache instance from the same
    directory recovers all entries."""
    cache_dir = _fresh_cache_dir()
    try:
        cache = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=300.0)

        catalog_names = [f"cat{i}" for i in range(num_catalogs)]
        conn_hashes = {name: _conn_hash({"host": name}) for name in catalog_names}

        for cat_name in catalog_names:
            for j in range(num_entries):
                cache.put(
                    "children",
                    cat_name,
                    conn_hashes[cat_name],
                    (f"p{j}",),
                    [{"name": f"{cat_name}_n{j}"}],
                )

        cache.flush()

        cache2 = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=300.0)
        for cat_name in catalog_names:
            for j in range(num_entries):
                result = cache2.get("children", cat_name, conn_hashes[cat_name], (f"p{j}",))
                assert result is not None
                assert result.data == [{"name": f"{cat_name}_n{j}"}]
    finally:
        _cleanup(cache_dir)
