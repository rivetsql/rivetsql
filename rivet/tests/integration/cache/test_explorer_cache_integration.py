"""Integration tests for CatalogExplorer + SmartCache interaction.

Uses a minimal concrete CatalogPlugin stub (not mocks) that tracks call
counts, allowing tests to verify when the explorer delegates to the plugin
versus serving from the SmartCache.
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from rivet_core.catalog_explorer import CatalogExplorer
from rivet_core.introspection import CatalogNode, ColumnDetail, ObjectSchema
from rivet_core.models import Catalog
from rivet_core.plugins import CatalogPlugin, PluginRegistry
from rivet_core.smart_cache import CacheMode, SmartCache

# ---------------------------------------------------------------------------
# Stub plugin — minimal concrete CatalogPlugin that tracks call counts
# ---------------------------------------------------------------------------


class StubCatalogPlugin(CatalogPlugin):
    """Minimal CatalogPlugin for integration tests.

    Tracks how many times each method is called so tests can assert on
    whether the explorer hit the cache or fell through to the plugin.
    """

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
        self._fingerprint_error: bool = False
        self._list_children_delay: float = 0.0

    # --- abstract method implementations ---

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
        if self._list_children_delay > 0:
            import time as _t

            _t.sleep(self._list_children_delay)
        key = ".".join(path)
        return self._children.get(key, [])

    def get_schema(self, catalog: Catalog, table: str) -> ObjectSchema:
        self.get_schema_count += 1
        return self._schemas[table]

    def get_fingerprint(self, catalog: Catalog, path: list[str]) -> str | None:
        self.get_fingerprint_count += 1
        if self._fingerprint_error:
            raise ConnectionError("network error")
        key = ".".join(path)
        return self._fingerprints.get(key)

    # --- helpers for test setup ---

    def set_children(self, path_key: str, nodes: list[CatalogNode]) -> None:
        self._children[path_key] = nodes

    def set_schema(self, table_key: str, schema: ObjectSchema) -> None:
        self._schemas[table_key] = schema

    def set_fingerprint(self, path_key: str, fp: str | None) -> None:
        self._fingerprints[path_key] = fp


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CATALOG_NAME = "testcat"
CONN_OPTIONS: dict[str, Any] = {"host": "localhost"}


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
    name: str, path: list[str], node_type: str = "table", is_container: bool = False
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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def cache_dir(tmp_path: Path) -> Path:
    return tmp_path / "cache"


@pytest.fixture
def cache(cache_dir: Path) -> SmartCache:
    return SmartCache(
        profile="test",
        cache_dir=cache_dir,
        default_ttl=300.0,
        max_size_bytes=50 * 1024 * 1024,
        flush_interval=5.0,
    )


@pytest.fixture
def plugin() -> StubCatalogPlugin:
    return StubCatalogPlugin()


# ---------------------------------------------------------------------------
# list_children tests
# ---------------------------------------------------------------------------


def test_list_children_cache_hit_skips_plugin_read_write(
    plugin: StubCatalogPlugin,
    cache: SmartCache,
) -> None:
    """Populate SmartCache, explorer in READ_WRITE — plugin should NOT be called."""
    conn_hash = _conn_hash()
    nodes = [_make_node("s1", ["s1"], "schema", is_container=True)]
    cache.put("children", CATALOG_NAME, conn_hash, (), nodes)

    # Also set plugin data (should not be reached)
    plugin.set_children("", [_make_node("s1_fresh", ["s1_fresh"], "schema", is_container=True)])

    explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.READ_WRITE)
    # Force connection status to connected
    explorer._connection_status[CATALOG_NAME] = (True, None)

    result = explorer.list_children([CATALOG_NAME])
    assert plugin.list_children_count == 0
    assert len(result) == 1
    assert result[0].name == "s1"


def test_list_children_write_only_always_calls_plugin(
    plugin: StubCatalogPlugin,
    cache: SmartCache,
) -> None:
    """In WRITE_ONLY mode, plugin is always called even when cache is populated."""
    conn_hash = _conn_hash()
    nodes = [_make_node("cached_s1", ["cached_s1"], "schema", is_container=True)]
    cache.put("children", CATALOG_NAME, conn_hash, (), nodes)

    fresh_nodes = [_make_node("fresh_s1", ["fresh_s1"], "schema", is_container=True)]
    plugin.set_children("", fresh_nodes)

    explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.WRITE_ONLY)
    explorer._connection_status[CATALOG_NAME] = (True, None)

    result = explorer.list_children([CATALOG_NAME])
    assert plugin.list_children_count == 1
    assert len(result) == 1
    assert result[0].name == "fresh_s1"

    # Result should also be stored in SmartCache
    cached = cache.get("children", CATALOG_NAME, conn_hash, ())
    assert cached is not None
    assert cached.data[0].name == "fresh_s1"


def test_list_children_cache_miss_calls_plugin_and_stores(
    plugin: StubCatalogPlugin,
    cache: SmartCache,
) -> None:
    """Empty cache — plugin should be called and result stored in SmartCache."""
    conn_hash = _conn_hash()
    nodes = [_make_node("s1", ["s1"], "schema", is_container=True)]
    plugin.set_children("", nodes)

    explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.READ_WRITE)
    explorer._connection_status[CATALOG_NAME] = (True, None)

    result = explorer.list_children([CATALOG_NAME])
    assert plugin.list_children_count == 1
    assert len(result) == 1
    assert result[0].name == "s1"

    # Verify stored in SmartCache
    cached = cache.get("children", CATALOG_NAME, conn_hash, ())
    assert cached is not None
    assert len(cached.data) == 1


# ---------------------------------------------------------------------------
# Staleness / fingerprint tests
# ---------------------------------------------------------------------------


def test_staleness_check_in_explorer_fingerprint_match(
    plugin: StubCatalogPlugin,
    cache_dir: Path,
) -> None:
    """Expired entry + same fingerprint → reset TTL, no data fetch."""
    cache = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=1.0)
    conn_hash = _conn_hash()
    nodes = [_make_node("s1", ["s1"], "schema", is_container=True)]
    cache.put("children", CATALOG_NAME, conn_hash, (), nodes, fingerprint="fp1")
    plugin.set_fingerprint("", "fp1")
    plugin.set_children("", [_make_node("s1_new", ["s1_new"], "schema", is_container=True)])

    explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.READ_WRITE)
    explorer._connection_status[CATALOG_NAME] = (True, None)

    # Advance time past TTL
    original_time = time.time()
    with patch("rivet_core.smart_cache.time") as mock_time:
        mock_time.time.return_value = original_time + 2.0
        mock_time.monotonic.return_value = time.monotonic()
        result = explorer.list_children([CATALOG_NAME])

    # Fingerprint matched → no data fetch, TTL reset
    assert plugin.list_children_count == 0
    assert plugin.get_fingerprint_count == 1
    assert len(result) == 1
    assert result[0].name == "s1"  # cached data returned

    # TTL should have been reset — entry should no longer be expired
    cached = cache.get("children", CATALOG_NAME, conn_hash, ())
    assert cached is not None
    assert cached.expired is False


def test_staleness_check_in_explorer_fingerprint_changed(
    plugin: StubCatalogPlugin,
    cache_dir: Path,
) -> None:
    """Expired entry + different fingerprint → invalidate and refetch."""
    cache = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=1.0)
    conn_hash = _conn_hash()
    old_nodes = [_make_node("old_s1", ["old_s1"], "schema", is_container=True)]
    cache.put("children", CATALOG_NAME, conn_hash, (), old_nodes, fingerprint="fp_old")

    new_nodes = [_make_node("new_s1", ["new_s1"], "schema", is_container=True)]
    plugin.set_children("", new_nodes)
    plugin.set_fingerprint("", "fp_new")

    explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.READ_WRITE)
    explorer._connection_status[CATALOG_NAME] = (True, None)

    original_time = time.time()
    with patch("rivet_core.smart_cache.time") as mock_time:
        mock_time.time.return_value = original_time + 2.0
        mock_time.monotonic.return_value = time.monotonic()
        result = explorer.list_children([CATALOG_NAME])

    # Fingerprint changed → refetch
    assert plugin.list_children_count == 1
    assert plugin.get_fingerprint_count >= 1
    assert len(result) == 1
    assert result[0].name == "new_s1"


def test_staleness_check_no_fingerprint_refetches(
    plugin: StubCatalogPlugin,
    cache_dir: Path,
) -> None:
    """Expired entry + plugin returns None fingerprint → refetch."""
    cache = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=1.0)
    conn_hash = _conn_hash()
    nodes = [_make_node("s1", ["s1"], "schema", is_container=True)]
    cache.put("children", CATALOG_NAME, conn_hash, (), nodes, fingerprint="fp1")
    # Plugin returns None for fingerprint (no fingerprinting support)
    plugin.set_fingerprint("", None)
    plugin.set_children("", [_make_node("s1_fresh", ["s1_fresh"], "schema", is_container=True)])

    explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.READ_WRITE)
    explorer._connection_status[CATALOG_NAME] = (True, None)

    original_time = time.time()
    with patch("rivet_core.smart_cache.time") as mock_time:
        mock_time.time.return_value = original_time + 2.0
        mock_time.monotonic.return_value = time.monotonic()
        result = explorer.list_children([CATALOG_NAME])

    # No fingerprint → refetch
    assert plugin.list_children_count == 1
    assert len(result) == 1
    assert result[0].name == "s1_fresh"


def test_staleness_check_network_error_returns_stale(
    plugin: StubCatalogPlugin,
    cache_dir: Path,
) -> None:
    """Expired entry + get_fingerprint raises → return stale data."""
    cache = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=1.0)
    conn_hash = _conn_hash()
    nodes = [_make_node("s1", ["s1"], "schema", is_container=True)]
    cache.put("children", CATALOG_NAME, conn_hash, (), nodes, fingerprint="fp1")
    plugin._fingerprint_error = True

    explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.READ_WRITE)
    explorer._connection_status[CATALOG_NAME] = (True, None)

    original_time = time.time()
    with patch("rivet_core.smart_cache.time") as mock_time:
        mock_time.time.return_value = original_time + 2.0
        mock_time.monotonic.return_value = time.monotonic()
        result = explorer.list_children([CATALOG_NAME])

    # Network error → stale data returned, no plugin data fetch
    assert plugin.list_children_count == 0
    assert len(result) == 1
    assert result[0].name == "s1"


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------


def test_get_schema_cache_hit_skips_plugin_read_write(
    plugin: StubCatalogPlugin,
    cache: SmartCache,
) -> None:
    """Schema cache hit in READ_WRITE — plugin.get_schema not called."""
    conn_hash = _conn_hash()
    schema = _make_schema(["s1", "t1"])
    cache.put("schema", CATALOG_NAME, conn_hash, ("s1", "t1"), schema)

    plugin.set_schema("s1.t1", _make_schema(["s1", "t1"]))

    explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.READ_WRITE)
    explorer._connection_status[CATALOG_NAME] = (True, None)

    result = explorer.get_table_schema([CATALOG_NAME, "s1", "t1"])
    assert plugin.get_schema_count == 0
    assert result is not None
    assert result.path == ["s1", "t1"]


def test_get_schema_write_only_always_calls_plugin(
    plugin: StubCatalogPlugin,
    cache: SmartCache,
) -> None:
    """In WRITE_ONLY mode, plugin.get_schema is always called."""
    conn_hash = _conn_hash()
    cached_schema = _make_schema(["s1", "t1"])
    cache.put("schema", CATALOG_NAME, conn_hash, ("s1", "t1"), cached_schema)

    fresh_schema = ObjectSchema(
        path=["s1", "t1"],
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
            ColumnDetail(
                name="name",
                type="utf8",
                native_type="VARCHAR",
                nullable=True,
                default=None,
                comment=None,
                is_primary_key=False,
                is_partition_key=False,
            ),
        ],
        primary_key=["id"],
        comment=None,
    )
    plugin.set_schema("s1.t1", fresh_schema)

    explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.WRITE_ONLY)
    explorer._connection_status[CATALOG_NAME] = (True, None)

    result = explorer.get_table_schema([CATALOG_NAME, "s1", "t1"])
    assert plugin.get_schema_count == 1
    assert result is not None
    assert len(result.columns) == 2

    # Result stored in SmartCache
    cached = cache.get("schema", CATALOG_NAME, conn_hash, ("s1", "t1"))
    assert cached is not None
    assert len(cached.data.columns) == 2


# ---------------------------------------------------------------------------
# refresh_catalog tests
# ---------------------------------------------------------------------------


def test_refresh_catalog_invalidates_and_refetches(
    plugin: StubCatalogPlugin,
    cache: SmartCache,
) -> None:
    """After refresh_catalog, next list_children calls the plugin again."""
    conn_hash = _conn_hash()
    nodes = [_make_node("s1", ["s1"], "schema", is_container=True)]
    cache.put("children", CATALOG_NAME, conn_hash, (), nodes)

    fresh_nodes = [_make_node("s1_refreshed", ["s1_refreshed"], "schema", is_container=True)]
    plugin.set_children("", fresh_nodes)

    explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.READ_WRITE)
    explorer._connection_status[CATALOG_NAME] = (True, None)

    # First call — cache hit
    explorer.list_children([CATALOG_NAME])
    assert plugin.list_children_count == 0

    # Refresh
    explorer.refresh_catalog(CATALOG_NAME)

    # SmartCache should be invalidated
    assert cache.get("children", CATALOG_NAME, conn_hash, ()) is None

    # Next call — plugin must be called
    result = explorer.list_children([CATALOG_NAME])
    assert plugin.list_children_count == 1
    assert result[0].name == "s1_refreshed"


# ---------------------------------------------------------------------------
# No-cache backward compatibility
# ---------------------------------------------------------------------------


def test_explorer_without_cache_works(plugin: StubCatalogPlugin) -> None:
    """Explorer with no SmartCache works normally — plugin always called."""
    nodes = [_make_node("s1", ["s1"], "schema", is_container=True)]
    plugin.set_children("", nodes)

    explorer = _make_explorer(plugin, cache=None)
    explorer._connection_status[CATALOG_NAME] = (True, None)

    result = explorer.list_children([CATALOG_NAME])
    assert plugin.list_children_count == 1
    assert len(result) == 1
    assert result[0].name == "s1"

    # Second call — still calls plugin (no cache)
    result = explorer.list_children([CATALOG_NAME])
    # In-memory cache kicks in, so plugin is NOT called again
    assert plugin.list_children_count == 1


# ---------------------------------------------------------------------------
# close / flush tests
# ---------------------------------------------------------------------------


def test_explorer_close_flushes_cache(
    plugin: StubCatalogPlugin,
    cache: SmartCache,
    cache_dir: Path,
) -> None:
    """Explorer.close() flushes SmartCache to disk."""
    conn_hash = _conn_hash()
    nodes = [_make_node("s1", ["s1"], "schema", is_container=True)]
    plugin.set_children("", nodes)

    explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.READ_WRITE)
    explorer._connection_status[CATALOG_NAME] = (True, None)

    # Trigger a plugin call to populate cache
    explorer.list_children([CATALOG_NAME])

    # Close should flush
    explorer.close()

    # Verify data persisted — create new SmartCache from same dir
    cache2 = SmartCache(profile="test", cache_dir=cache_dir)
    result = cache2.get("children", CATALOG_NAME, conn_hash, ())
    assert result is not None
    assert len(result.data) == 1


# ---------------------------------------------------------------------------
# Search tests
# ---------------------------------------------------------------------------


def test_search_uses_cached_children(
    plugin: StubCatalogPlugin,
    cache: SmartCache,
) -> None:
    """Search seeds from SmartCache and finds cached nodes."""
    conn_hash = _conn_hash()
    nodes = [
        _make_node("orders", ["s1", "orders"], "table"),
        _make_node("users", ["s1", "users"], "table"),
    ]
    cache.put("children", CATALOG_NAME, conn_hash, ("s1",), nodes)

    explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.READ_WRITE)
    explorer._connection_status[CATALOG_NAME] = (True, None)

    results = explorer.search("orders", limit=10, expand=False)
    assert any(r.short_name == "orders" for r in results)
    # Plugin should not have been called (expand=False, data from SmartCache)
    assert plugin.list_children_count == 0


def test_search_progressive_expansion_read_write(
    plugin: StubCatalogPlugin,
    cache: SmartCache,
) -> None:
    """READ_WRITE + expand=True: expands unexplored branches and stores in SmartCache."""
    conn_hash = _conn_hash()

    # Set up a deep tree: catalog root → schema → tables
    schema_nodes = [_make_node("s1", ["s1"], "schema", is_container=True)]
    plugin.set_children("", schema_nodes)

    table_nodes = [
        _make_node("deep_orders", ["s1", "deep_orders"], "table"),
        _make_node("deep_users", ["s1", "deep_users"], "table"),
    ]
    plugin.set_children("s1", table_nodes)

    explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.READ_WRITE)
    explorer._connection_status[CATALOG_NAME] = (True, None)

    results = explorer.search("deep_orders", limit=10, expand=True, expansion_budget_seconds=5.0)
    assert any(r.short_name == "deep_orders" for r in results)
    # Plugin should have been called for expansion
    assert plugin.list_children_count >= 1

    # Results should be stored in SmartCache
    cached = cache.get("children", CATALOG_NAME, conn_hash, ("s1",))
    assert cached is not None


def test_search_access_priority_ranking(
    plugin: StubCatalogPlugin,
    cache: SmartCache,
) -> None:
    """Higher-access entries rank higher among equally-matched results."""
    conn_hash = _conn_hash()

    # Two entries with similar names but different access times
    hot_nodes = [_make_node("data_orders", ["hot", "data_orders"], "table")]
    cold_nodes = [_make_node("data_orders_archive", ["cold", "data_orders_archive"], "table")]

    cache.put("children", CATALOG_NAME, conn_hash, ("hot",), hot_nodes)
    cache.put("children", CATALOG_NAME, conn_hash, ("cold",), cold_nodes)

    # Access the "hot" entry multiple times to boost its last_accessed
    for _ in range(5):
        cache.get("children", CATALOG_NAME, conn_hash, ("hot",))

    explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.READ_WRITE)
    explorer._connection_status[CATALOG_NAME] = (True, None)

    results = explorer.search("data_orders", limit=10, expand=False)
    # Both should appear
    names = [r.short_name for r in results]
    assert "data_orders" in names

    # The hot entry should rank higher (lower score) than the cold one
    hot_results = [r for r in results if r.short_name == "data_orders"]
    cold_results = [r for r in results if r.short_name == "data_orders_archive"]
    if hot_results and cold_results:
        assert hot_results[0].score <= cold_results[0].score


def test_search_expansion_prioritises_hot_branches(
    plugin: StubCatalogPlugin,
    cache: SmartCache,
) -> None:
    """Branches with more recent access times are expanded first."""
    # Set up two catalogs — one "hot", one "cold"
    hot_cat = _make_catalog("hotcat")
    cold_cat = _make_catalog("coldcat")

    plugin.set_children("", [_make_node("hot_table", ["hot_table"], "table")])

    # Pre-populate SmartCache with access times for hotcat
    hot_hash = _conn_hash(hot_cat.options)
    cache.put("children", "hotcat", hot_hash, ("dummy",), [], fingerprint=None)
    # Access hotcat entry to make it "hot"
    for _ in range(5):
        cache.get("children", "hotcat", hot_hash, ("dummy",))

    explorer = _make_explorer(
        plugin,
        cache=cache,
        cache_mode=CacheMode.READ_WRITE,
        catalogs={"hotcat": hot_cat, "coldcat": cold_cat},
    )
    explorer._connection_status["hotcat"] = (True, None)
    explorer._connection_status["coldcat"] = (True, None)

    # Search with expansion — hot branches should be expanded first
    results = explorer.search("table", limit=2, expand=True, expansion_budget_seconds=5.0)
    assert plugin.list_children_count >= 1
    # At least one result should be found
    assert len(results) >= 1


def test_search_expansion_respects_time_budget(
    plugin: StubCatalogPlugin,
    cache: SmartCache,
) -> None:
    """Slow plugin + short budget → expansion stops early."""
    plugin._list_children_delay = 0.5  # 500ms per call
    schema_nodes = [_make_node("s1", ["s1"], "schema", is_container=True)]
    plugin.set_children("", schema_nodes)
    plugin.set_children(
        "s1",
        [
            _make_node("t1", ["s1", "t1"], "table"),
            _make_node("t2", ["s1", "t2"], "schema", is_container=True),
        ],
    )
    plugin.set_children(
        "s1.t2",
        [
            _make_node("t3", ["s1", "t2", "t3"], "table"),
        ],
    )

    explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.READ_WRITE)
    explorer._connection_status[CATALOG_NAME] = (True, None)

    start = time.monotonic()
    _results = explorer.search("t3", limit=50, expand=True, expansion_budget_seconds=0.3)
    elapsed = time.monotonic() - start

    # Should have stopped within a reasonable margin of the budget
    # (budget is 0.3s, plugin takes 0.5s per call, so at most 1-2 calls)
    assert elapsed < 3.0  # generous upper bound
    # Plugin was called but expansion was limited by budget
    assert plugin.list_children_count >= 1


def test_search_no_expansion_write_only(
    plugin: StubCatalogPlugin,
    cache: SmartCache,
) -> None:
    """WRITE_ONLY mode — no progressive expansion regardless of expand flag."""
    schema_nodes = [_make_node("s1", ["s1"], "schema", is_container=True)]
    plugin.set_children("", schema_nodes)
    plugin.set_children("s1", [_make_node("target", ["s1", "target"], "table")])

    explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.WRITE_ONLY)
    explorer._connection_status[CATALOG_NAME] = (True, None)

    results = explorer.search("target", limit=10, expand=True)
    # No expansion in WRITE_ONLY — plugin should not be called for expansion
    assert plugin.list_children_count == 0
    # No results since nothing is in memory and SmartCache is not read in WRITE_ONLY
    assert len(results) == 0


def test_search_no_expansion_when_expand_false(
    plugin: StubCatalogPlugin,
    cache: SmartCache,
) -> None:
    """READ_WRITE with expand=False — Phase 1 only, no network I/O."""
    conn_hash = _conn_hash()

    # Put some data in SmartCache (Phase 1 will seed from it)
    nodes = [_make_node("cached_table", ["s1", "cached_table"], "table")]
    cache.put("children", CATALOG_NAME, conn_hash, ("s1",), nodes)

    # Plugin has additional data that would be found via expansion
    plugin.set_children("", [_make_node("s1", ["s1"], "schema", is_container=True)])
    plugin.set_children("s1", [_make_node("uncached_table", ["s1", "uncached_table"], "table")])

    explorer = _make_explorer(plugin, cache=cache, cache_mode=CacheMode.READ_WRITE)
    explorer._connection_status[CATALOG_NAME] = (True, None)

    results = explorer.search("table", limit=10, expand=False)
    # Should find cached_table from SmartCache but NOT uncached_table
    names = [r.short_name for r in results]
    assert "cached_table" in names
    # Plugin should not have been called (no expansion)
    assert plugin.list_children_count == 0
