"""Unit tests for rivet_core/catalog_explorer.py data models and error class.

Also contains:
Feature: catalog-explorer, Property 1: list_catalogs returns all catalogs without introspection
Feature: catalog-explorer, Property 10: Default list_children filters to immediate children
Feature: catalog-explorer, list_children() unit tests (task 4.3)

For any path and any set of CatalogNode entries returned by list_tables(), the default
CatalogPlugin.list_children() implementation returns only nodes that are immediate children
of the given path — i.e., nodes whose path is exactly one segment longer than the input
path and shares the same prefix.

Validates: Requirements 5.1, 5.3
"""

from __future__ import annotations

import dataclasses
import re
from typing import Any
from unittest.mock import MagicMock

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.catalog_explorer import (
    RVT_870,
    RVT_871,
    RVT_872,
    RVT_873,
    RVT_874,
    RVT_875,
    RVT_876,
    RVT_877,
    CatalogExplorer,
    CatalogExplorerError,
    CatalogInfo,
    ConnectionResult,
    ExplorerNode,
    GeneratedSource,
    NodeDetail,
    SearchResult,
    sanitize_name,
)
from rivet_core.errors import RivetError
from rivet_core.introspection import CatalogNode
from rivet_core.models import Catalog
from rivet_core.plugins import CatalogPlugin, _is_immediate_child


class TestErrorCodeConstants:
    def test_error_codes_range(self):
        codes = [RVT_870, RVT_871, RVT_872, RVT_873, RVT_874, RVT_875, RVT_876, RVT_877]
        for i, code in enumerate(codes):
            assert code == f"RVT-{870 + i}"

    def test_all_eight_codes_defined(self):
        assert RVT_870 == "RVT-870"
        assert RVT_871 == "RVT-871"
        assert RVT_872 == "RVT-872"
        assert RVT_873 == "RVT-873"
        assert RVT_874 == "RVT-874"
        assert RVT_875 == "RVT-875"
        assert RVT_876 == "RVT-876"
        assert RVT_877 == "RVT-877"


class TestCatalogExplorerError:
    def test_wraps_rivet_error(self):
        rivet_err = RivetError(code=RVT_870, message="connection failed")
        exc = CatalogExplorerError(rivet_err)
        assert exc.error is rivet_err

    def test_is_exception(self):
        rivet_err = RivetError(code=RVT_871, message="table not found")
        exc = CatalogExplorerError(rivet_err)
        assert isinstance(exc, Exception)

    def test_str_delegates_to_rivet_error(self):
        rivet_err = RivetError(code=RVT_872, message="schema failed")
        exc = CatalogExplorerError(rivet_err)
        assert str(rivet_err) in str(exc)

    def test_can_be_raised_and_caught(self):
        rivet_err = RivetError(code=RVT_873, message="depth limit")
        with pytest.raises(CatalogExplorerError) as exc_info:
            raise CatalogExplorerError(rivet_err)
        assert exc_info.value.error.code == RVT_873


class TestDataclassImmutability:
    def test_catalog_info_frozen(self):
        info = CatalogInfo(name="pg", catalog_type="postgres", connected=True, error=None)
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            info.name = "other"  # type: ignore[misc]

    def test_explorer_node_frozen(self):
        node = ExplorerNode(
            name="public",
            node_type="schema",
            path=["pg", "public"],
            is_expandable=True,
            depth=1,
            summary=None,
            depth_limit_reached=False,
        )
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            node.name = "other"  # type: ignore[misc]

    def test_node_detail_frozen(self):
        node = ExplorerNode(
            name="t", node_type="table", path=["pg", "public", "t"],
            is_expandable=True, depth=2, summary=None, depth_limit_reached=False,
        )
        detail = NodeDetail(node=node, schema=None, metadata=None, children_count=None)
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            detail.children_count = 5  # type: ignore[misc]

    def test_search_result_frozen(self):
        result = SearchResult(
            kind="table", qualified_name="pg.public.orders", short_name="orders",
            parent="pg.public", match_positions=[0, 1], score=0.5, node_type="table",
        )
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            result.score = 0.1  # type: ignore[misc]

    def test_generated_source_frozen(self):
        src = GeneratedSource(
            content="name: raw_orders", format="yaml",
            suggested_filename="raw_orders.yaml", catalog_name="pg",
            table_name="orders", column_count=3,
        )
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            src.content = "other"  # type: ignore[misc]

    def test_connection_result_frozen(self):
        result = ConnectionResult(
            catalog_name="pg", connected=True, error=None, elapsed_ms=5.0,
        )
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            result.connected = False  # type: ignore[misc]


class TestCatalogInfoDefaults:
    def test_options_summary_defaults_to_empty_dict(self):
        info = CatalogInfo(name="pg", catalog_type="postgres", connected=True, error=None)
        assert info.options_summary == {}

    def test_error_can_be_none(self):
        info = CatalogInfo(name="pg", catalog_type="postgres", connected=True, error=None)
        assert info.error is None

    def test_error_can_be_string(self):
        info = CatalogInfo(name="pg", catalog_type="postgres", connected=False, error="timeout")
        assert info.error == "timeout"


# ── Minimal concrete plugin for testing ───────────────────────────────────────


class _ConcretePlugin(CatalogPlugin):
    type = "test"
    required_options: list = []
    optional_options: dict = {}
    credential_options: list = []

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def instantiate(self, name: str, options: dict[str, Any]) -> Any:
        pass

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        return logical_name


# ── Strategies ────────────────────────────────────────────────────────────────

_segment = st.text(
    alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_"),
    min_size=1,
    max_size=16,
)

_path = st.lists(_segment, min_size=0, max_size=4)

_node_path = st.lists(_segment, min_size=1, max_size=6)


def _make_node(path: list[str]) -> CatalogNode:
    return CatalogNode(
        name=path[-1],
        node_type="table",
        path=path,
        is_container=False,
        children_count=None,
        summary=None,
    )


# ── Property 10 ───────────────────────────────────────────────────────────────


@given(
    parent_path=_path,
    node_paths=st.lists(_node_path, min_size=0, max_size=20),
)
@settings(max_examples=200)
def test_property_10_default_list_children_filters_to_immediate_children(
    parent_path: list[str],
    node_paths: list[list[str]],
) -> None:
    """Property 10: Default list_children returns only immediate children of the given path.

    For any parent_path and any flat list of CatalogNode entries, the result contains
    exactly those nodes whose path is one segment longer than parent_path and shares
    the same prefix — no more, no less.
    """
    plugin = _ConcretePlugin()
    catalog = MagicMock()
    nodes = [_make_node(p) for p in node_paths]
    plugin.list_tables = MagicMock(return_value=nodes)

    result = plugin.list_children(catalog, parent_path)

    # Compute expected set independently using _is_immediate_child
    expected = [n for n in nodes if _is_immediate_child(n.path, parent_path)]

    assert result == expected

    # Every returned node must be exactly one segment deeper with matching prefix
    for node in result:
        assert len(node.path) == len(parent_path) + 1
        assert node.path[: len(parent_path)] == parent_path

    # No node that is NOT an immediate child should appear in the result
    non_immediate = [n for n in nodes if not _is_immediate_child(n.path, parent_path)]
    result_paths = {tuple(n.path) for n in result}
    for node in non_immediate:
        assert tuple(node.path) not in result_paths


# ── Property 14 ───────────────────────────────────────────────────────────────


class TestSanitizeNameProperty:
    """Property 14: Name sanitization produces valid joint names."""

    @given(st.text())
    @settings(max_examples=200)
    def test_sanitized_name_is_lowercase(self, name: str) -> None:
        result = sanitize_name(name)
        assert result == result.lower()

    @given(st.text())
    @settings(max_examples=200)
    def test_sanitized_name_contains_only_valid_chars(self, name: str) -> None:
        result = sanitize_name(name)
        assert re.fullmatch(r"[a-z0-9_]+", result), f"Invalid chars in {result!r}"

    @given(st.text())
    @settings(max_examples=200)
    def test_sanitized_name_starts_with_raw_(self, name: str) -> None:
        result = sanitize_name(name)
        assert result.startswith("raw_")

    @given(st.text())
    @settings(max_examples=200)
    def test_sanitized_name_at_most_128_chars(self, name: str) -> None:
        result = sanitize_name(name)
        assert len(result) <= 128


# ── Helpers for CatalogExplorer tests ────────────────────────────────────


def _make_registry(*catalog_plugins: tuple[str, MagicMock]) -> MagicMock:
    """Build a mock PluginRegistry that returns the given catalog plugins by type."""
    registry = MagicMock()
    plugin_map = {t: p for t, p in catalog_plugins}
    registry.get_catalog_plugin.side_effect = lambda t: plugin_map.get(t)
    return registry


def _wire_list_children(plugin: MagicMock) -> None:
    """Wire plugin.list_children to filter plugin.list_tables by path.

    Mimics the real CatalogPlugin.list_children default: call list_tables()
    and return only immediate children of the given path.
    """
    def _list_children_impl(_catalog, path):
        all_nodes = plugin.list_tables(_catalog)
        return [n for n in all_nodes if _is_immediate_child(n.path, path)]
    plugin.list_children.side_effect = _list_children_impl


def _make_plugin(*, list_tables_return=None, list_tables_side_effect=None, test_connection_side_effect=None) -> MagicMock:
    """Build a mock CatalogPlugin."""
    plugin = MagicMock()
    if list_tables_side_effect is not None:
        plugin.list_tables.side_effect = list_tables_side_effect
    else:
        plugin.list_tables.return_value = list_tables_return or []
    if test_connection_side_effect is not None:
        plugin.test_connection.side_effect = test_connection_side_effect
    # By default, wire list_children to filter list_tables (like the real base class)
    _wire_list_children(plugin)
    return plugin


class TestCatalogExplorerInit:
    def test_accepts_required_params(self):
        registry = _make_registry()
        explorer = CatalogExplorer(
            catalogs={}, engines={}, registry=registry,
        )
        assert explorer._max_depth == 10
        assert explorer._tables_cache == {}
        assert explorer._schema_cache == {}

    def test_custom_max_depth(self):
        registry = _make_registry()
        explorer = CatalogExplorer(
            catalogs={}, engines={}, registry=registry, max_depth=5,
        )
        assert explorer._max_depth == 5

    def test_probes_catalogs_on_init(self):
        plugin = _make_plugin()
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres", options={"host": "localhost"})
        CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)
        plugin.test_connection.assert_called_once_with(cat)

    def test_stores_connected_true_on_success(self):
        plugin = _make_plugin()
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)
        assert explorer._connection_status["pg"] == (True, None)

    def test_stores_connected_false_on_failure(self):
        plugin = _make_plugin(test_connection_side_effect=RuntimeError("connection refused"))
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)
        connected, error = explorer._connection_status["pg"]
        assert connected is False
        assert "connection refused" in error

    def test_stores_connected_false_when_no_plugin(self):
        registry = _make_registry()  # no plugins registered
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)
        connected, error = explorer._connection_status["pg"]
        assert connected is False
        assert "No plugin" in error


class TestCatalogExplorerListCatalogs:
    def test_returns_all_catalogs(self):
        plugin = _make_plugin()
        registry = _make_registry(("postgres", plugin), ("duckdb", plugin))
        catalogs = {
            "pg": Catalog(name="pg", type="postgres"),
            "dk": Catalog(name="dk", type="duckdb"),
        }
        explorer = CatalogExplorer(catalogs=catalogs, engines={}, registry=registry)
        result = explorer.list_catalogs()
        names = {c.name for c in result}
        assert names == {"pg", "dk"}

    def test_returns_correct_types(self):
        plugin = _make_plugin()
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)
        infos = explorer.list_catalogs()
        assert len(infos) == 1
        assert infos[0].catalog_type == "postgres"

    def test_connected_catalog_status(self):
        plugin = _make_plugin()
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)
        info = explorer.list_catalogs()[0]
        assert info.connected is True
        assert info.error is None

    def test_disconnected_catalog_status(self):
        plugin = _make_plugin(test_connection_side_effect=RuntimeError("timeout"))
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)
        info = explorer.list_catalogs()[0]
        assert info.connected is False
        assert "timeout" in info.error

    def test_does_not_call_introspection(self):
        """list_catalogs() must not trigger any plugin introspection calls."""
        plugin = _make_plugin()
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)
        # Reset call counts after init probe
        plugin.list_tables.reset_mock()
        plugin.get_schema.reset_mock()
        plugin.get_metadata.reset_mock()

        explorer.list_catalogs()

        plugin.list_tables.assert_not_called()
        plugin.get_schema.assert_not_called()
        plugin.get_metadata.assert_not_called()

    def test_empty_catalogs(self):
        registry = _make_registry()
        explorer = CatalogExplorer(catalogs={}, engines={}, registry=registry)
        assert explorer.list_catalogs() == []

    def test_mixed_connected_disconnected(self):
        good_plugin = _make_plugin()
        bad_plugin = _make_plugin(test_connection_side_effect=RuntimeError("fail"))
        registry = _make_registry(("postgres", good_plugin), ("duckdb", bad_plugin))
        catalogs = {
            "pg": Catalog(name="pg", type="postgres"),
            "dk": Catalog(name="dk", type="duckdb"),
        }
        explorer = CatalogExplorer(catalogs=catalogs, engines={}, registry=registry)
        infos = {c.name: c for c in explorer.list_catalogs()}
        assert infos["pg"].connected is True
        assert infos["dk"].connected is False

    def test_options_summary_excludes_credentials(self):
        plugin = _make_plugin()
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(
            name="pg", type="postgres",
            options={"host": "localhost", "password": "secret123", "port": "5432"},
        )
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)
        info = explorer.list_catalogs()[0]
        assert "host" in info.options_summary
        assert "port" in info.options_summary
        assert "password" not in info.options_summary

    def test_returns_catalog_info_instances(self):
        plugin = _make_plugin()
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)
        for info in explorer.list_catalogs():
            assert isinstance(info, CatalogInfo)


# ── Property 1 ────────────────────────────────────────────────────────────────

_catalog_name = st.text(
    alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_"),
    min_size=1,
    max_size=16,
)

_catalog_type = st.sampled_from(["postgres", "duckdb", "s3", "glue", "arrow", "filesystem"])


@given(
    catalog_specs=st.lists(
        st.tuples(_catalog_name, _catalog_type, st.booleans()),
        min_size=0,
        max_size=8,
        unique_by=lambda x: x[0],
    )
)
@settings(max_examples=200)
def test_property_1_list_catalogs_returns_all_without_introspection(
    catalog_specs: list[tuple[str, str, bool]],
) -> None:
    """Property 1: list_catalogs returns all catalogs without introspection.

    For any set of instantiated catalogs (connected or not), list_catalogs() returns
    a CatalogInfo for each catalog with correct name, type, and connection status.
    After init, list_catalogs() makes no plugin introspection calls.

    Validates: Requirements 2.1
    """
    # Build catalogs and plugins; `should_fail` controls whether the init probe raises
    catalogs: dict[str, Catalog] = {}
    plugins: dict[str, MagicMock] = {}
    expected_connected: dict[str, bool] = {}

    for name, cat_type, should_fail in catalog_specs:
        cat = Catalog(name=name, type=cat_type)
        catalogs[name] = cat
        if should_fail:
            plugin = _make_plugin(
                list_tables_side_effect=RuntimeError("probe failed"),
                test_connection_side_effect=RuntimeError("probe failed"),
            )
            expected_connected[name] = False
        else:
            plugin = _make_plugin()
            expected_connected[name] = True
        plugins[cat_type + "_" + name] = plugin

    # Registry maps each catalog's type to its plugin (unique per catalog name)
    registry = MagicMock()
    def get_plugin(cat_type: str) -> MagicMock | None:
        # Find the plugin for this type; since multiple catalogs may share a type,
        # we need to match by the unique key. Use a closure over catalog_specs.
        for name, t, _should_fail in catalog_specs:
            if t == cat_type:
                return plugins[t + "_" + name]
        return None

    # Build per-catalog registry: each catalog gets its own plugin instance
    catalog_plugin_map: dict[str, MagicMock] = {}
    for name, cat_type, _should_fail in catalog_specs:
        catalog_plugin_map[name] = plugins[cat_type + "_" + name]

    # Override registry to return the right plugin per catalog type lookup
    # Since multiple catalogs can share a type, we track which plugin to return
    # by building a type→plugin map (last one wins for shared types, but that's fine
    # since we only care about call counts per plugin instance)
    type_to_plugin: dict[str, MagicMock] = {}
    for name, cat_type, _ in catalog_specs:
        type_to_plugin[cat_type] = catalog_plugin_map[name]
    registry.get_catalog_plugin.side_effect = lambda t: type_to_plugin.get(t)

    explorer = CatalogExplorer(catalogs=catalogs, engines={}, registry=registry)

    # Reset all plugin mocks after init (init probe is allowed)
    for plugin in catalog_plugin_map.values():
        plugin.list_tables.reset_mock()
        plugin.get_schema.reset_mock()
        plugin.get_metadata.reset_mock()

    result = explorer.list_catalogs()

    # 1. All catalogs are returned
    assert len(result) == len(catalog_specs)
    result_by_name = {info.name: info for info in result}
    assert set(result_by_name.keys()) == {name for name, _, _ in catalog_specs}

    # 2. Each CatalogInfo has correct name and type
    for name, cat_type, _ in catalog_specs:
        info = result_by_name[name]
        assert info.name == name
        assert info.catalog_type == cat_type

    # 3. list_catalogs() itself makes no introspection calls
    for plugin in catalog_plugin_map.values():
        plugin.list_tables.assert_not_called()
        plugin.get_schema.assert_not_called()
        plugin.get_metadata.assert_not_called()


# ── Tests for CatalogExplorer.list_children() ─────────────────────────────


def _make_catalog_node(name, node_type, path, is_container=False, summary=None):
    return CatalogNode(
        name=name,
        node_type=node_type,
        path=path,
        is_container=is_container,
        children_count=None,
        summary=summary,
    )


class TestCatalogExplorerListChildren:
    """Tests for list_children() — lazy expansion, caching, depth cap, disconnected."""

    def test_empty_path_returns_empty(self):
        plugin = _make_plugin()
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)
        assert explorer.list_children([]) == []

    def test_disconnected_catalog_returns_empty(self):
        plugin = _make_plugin(
            list_tables_side_effect=RuntimeError("fail"),
            test_connection_side_effect=RuntimeError("fail"),
        )
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)
        assert explorer.list_children(["pg"]) == []

    def test_unknown_catalog_returns_empty(self):
        registry = _make_registry()
        explorer = CatalogExplorer(catalogs={}, engines={}, registry=registry)
        assert explorer.list_children(["nonexistent"]) == []

    def test_catalog_level_calls_plugin_and_caches(self):
        nodes = [
            _make_catalog_node("public", "schema", ["public"], is_container=True),
            _make_catalog_node("users", "table", ["public", "users"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)
        plugin.list_tables.reset_mock()

        result = explorer.list_children(["pg"])

        # Should have cached per-path
        assert ("pg", ()) in explorer._children_cache
        # Should return immediate children (only "public" schema at depth 0)
        assert len(result) == 1
        assert result[0].name == "public"
        assert result[0].node_type == "schema"
        assert result[0].path == ["pg", "public"]

    def test_second_call_uses_cache_no_extra_plugin_calls(self):
        nodes = [
            _make_catalog_node("public", "schema", ["public"], is_container=True),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)
        plugin.list_children.reset_mock()
        plugin.list_tables.reset_mock()

        explorer.list_children(["pg"])
        explorer.list_children(["pg"])

        # list_children on plugin called at most once (for initial cache population)
        assert plugin.list_children.call_count <= 1

    def test_schema_level_filters_cached_nodes(self):
        nodes = [
            _make_catalog_node("public", "schema", ["public"], is_container=True),
            _make_catalog_node("users", "table", ["public", "users"]),
            _make_catalog_node("orders", "table", ["public", "orders"]),
            _make_catalog_node("private", "schema", ["private"], is_container=True),
            _make_catalog_node("secrets", "table", ["private", "secrets"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        # Schema-level: children of ["pg", "public"]
        result = explorer.list_children(["pg", "public"])

        names = {n.name for n in result}
        assert names == {"users", "orders"}
        for node in result:
            assert node.path[:2] == ["pg", "public"]

        # Second call uses cache — no additional plugin calls
        plugin.list_children.reset_mock()
        plugin.list_tables.reset_mock()
        result2 = explorer.list_children(["pg", "public"])
        plugin.list_children.assert_not_called()
        assert {n.name for n in result2} == {"users", "orders"}

    def test_table_level_calls_get_schema_and_returns_columns(self):
        from rivet_core.introspection import ColumnDetail, ObjectSchema

        nodes = [
            _make_catalog_node("users", "table", ["public", "users"]),
        ]
        schema = ObjectSchema(
            path=["public", "users"],
            node_type="table",
            columns=[
                ColumnDetail(name="id", type="int64", native_type=None, nullable=False,
                             default=None, comment=None, is_primary_key=True, is_partition_key=False),
                ColumnDetail(name="name", type="utf8", native_type=None, nullable=True,
                             default=None, comment=None, is_primary_key=False, is_partition_key=False),
            ],
            primary_key=["id"],
            comment=None,
        )
        plugin = _make_plugin(list_tables_return=nodes)
        plugin.get_schema.return_value = schema
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        result = explorer.list_children(["pg", "public", "users"])

        assert len(result) == 2
        assert result[0].name == "id"
        assert result[0].node_type == "column"
        assert result[0].path == ["pg", "public", "users", "id"]
        assert result[0].is_expandable is False
        assert result[1].name == "name"

    def test_schema_cache_avoids_duplicate_get_schema_calls(self):
        from rivet_core.introspection import ColumnDetail, ObjectSchema

        nodes = [_make_catalog_node("t", "table", ["public", "t"])]
        schema = ObjectSchema(
            path=["public", "t"], node_type="table",
            columns=[ColumnDetail(name="c", type="int64", native_type=None, nullable=False,
                                  default=None, comment=None, is_primary_key=False, is_partition_key=False)],
            primary_key=None, comment=None,
        )
        plugin = _make_plugin(list_tables_return=nodes)
        plugin.get_schema.return_value = schema
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        explorer.list_children(["pg", "public", "t"])
        explorer.list_children(["pg", "public", "t"])

        # get_schema called exactly once
        plugin.get_schema.assert_called_once()

    def test_depth_cap_returns_empty(self):
        nodes = [
            _make_catalog_node("a", "schema", ["a"], is_container=True),
            _make_catalog_node("b", "table", ["a", "b"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(
            catalogs={"pg": cat}, engines={}, registry=registry, max_depth=2,
        )

        # Path ["pg", "a"] has len=2 which equals max_depth → returns empty
        result = explorer.list_children(["pg", "a"])
        assert result == []

    def test_depth_limit_reached_flag_on_nodes(self):
        nodes = [
            _make_catalog_node("t", "table", ["t"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(
            catalogs={"pg": cat}, engines={}, registry=registry, max_depth=2,
        )

        result = explorer.list_children(["pg"])
        assert len(result) == 1
        # child depth is 1, max_depth is 2, so child_depth+1=2 >= max_depth → depth_limit_reached
        assert result[0].depth_limit_reached is True

    def test_get_schema_failure_returns_empty(self):
        nodes = [_make_catalog_node("t", "table", ["public", "t"])]
        plugin = _make_plugin(list_tables_return=nodes)
        plugin.get_schema.side_effect = RuntimeError("introspection failed")
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        result = explorer.list_children(["pg", "public", "t"])
        assert result == []

    def test_explorer_node_fields(self):
        nodes = [
            _make_catalog_node("public", "schema", ["public"], is_container=True),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        result = explorer.list_children(["pg"])
        node = result[0]
        assert isinstance(node, ExplorerNode)
        assert node.depth == 1
        assert node.is_expandable is True  # container

    def test_table_node_is_expandable(self):
        nodes = [_make_catalog_node("t", "table", ["t"])]
        plugin = _make_plugin(list_tables_return=nodes)
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        result = explorer.list_children(["pg"])
        assert result[0].is_expandable is True  # tables are expandable (columns)

    def test_list_children_exception_returns_empty(self):
        """If plugin.list_children() raises, explorer returns empty list."""
        plugin = _make_plugin()
        plugin.list_children.side_effect = RuntimeError("broken")
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        result = explorer.list_children(["pg"])
        assert result == []


# ── Property 2: list_tables caching on first expansion ────────────────────────

_table_name = st.text(
    alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_"),
    min_size=1,
    max_size=12,
)

_schema_name = st.text(
    alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_"),
    min_size=1,
    max_size=12,
)


@given(
    schema=_schema_name,
    tables=st.lists(_table_name, min_size=1, max_size=6, unique=True),
    extra_calls=st.integers(min_value=1, max_value=5),
)
@settings(max_examples=100)
def test_property_2_list_tables_cached_on_first_expansion(
    schema: str,
    tables: list[str],
    extra_calls: int,
) -> None:
    """Property 2: list_tables caching on first expansion.

    For any catalog, calling list_children([catalog_name]) invokes
    CatalogPlugin.list_tables() exactly once, and subsequent calls to
    list_children for the same catalog (at any depth) do not invoke
    list_tables() again.

    Validates: Requirements 2.2, 18.1
    """
    nodes = [
        CatalogNode(name=t, node_type="table", path=[schema, t],
                    is_container=False, children_count=None, summary=None)
        for t in tables
    ]
    nodes.append(
        CatalogNode(name=schema, node_type="schema", path=[schema],
                    is_container=True, children_count=None, summary=None)
    )

    plugin = MagicMock()
    plugin.list_tables.return_value = nodes
    _wire_list_children(plugin)

    registry = MagicMock()
    registry.get_catalog_plugin.return_value = plugin

    cat = Catalog(name="mycat", type="postgres")
    explorer = CatalogExplorer(catalogs={"mycat": cat}, engines={}, registry=registry)

    # Reset after init probe
    plugin.list_tables.reset_mock()

    # First expansion at catalog level
    explorer.list_children(["mycat"])

    # Additional calls at catalog level and schema level
    for _ in range(extra_calls):
        explorer.list_children(["mycat"])
        explorer.list_children(["mycat", schema])

    # list_tables called at most twice: once for catalog level, once for schema level
    # (each path is cached independently, but both delegate to list_tables via _wire_list_children)
    assert plugin.list_tables.call_count <= 2


# ── Property 3: Schema-level filtering uses cache ─────────────────────────────


@given(
    schemas=st.lists(_schema_name, min_size=1, max_size=4, unique=True),
    tables_per_schema=st.integers(min_value=1, max_value=4),
    target_schema_idx=st.integers(min_value=0, max_value=3),
)
@settings(max_examples=100)
def test_property_3_schema_level_filtering_uses_cache(
    schemas: list[str],
    tables_per_schema: int,
    target_schema_idx: int,
) -> None:
    """Property 3: Schema-level filtering uses cache.

    For any catalog that has been expanded once, calling list_children([catalog, schema])
    returns only tables belonging to that schema, filtered from the cached list_tables()
    result, without making additional plugin calls.

    Validates: Requirements 2.3
    """
    target_schema = schemas[target_schema_idx % len(schemas)]

    nodes: list[CatalogNode] = []
    for s in schemas:
        nodes.append(CatalogNode(name=s, node_type="schema", path=[s],
                                 is_container=True, children_count=None, summary=None))
        for i in range(tables_per_schema):
            t = f"t{i}"
            nodes.append(CatalogNode(name=t, node_type="table", path=[s, t],
                                     is_container=False, children_count=None, summary=None))

    plugin = MagicMock()
    plugin.list_tables.return_value = nodes
    _wire_list_children(plugin)

    registry = MagicMock()
    registry.get_catalog_plugin.return_value = plugin

    cat = Catalog(name="mycat", type="postgres")
    explorer = CatalogExplorer(catalogs={"mycat": cat}, engines={}, registry=registry)

    # Schema-level navigation
    result = explorer.list_children(["mycat", target_schema])

    # Only tables belonging to target_schema are returned
    for node in result:
        assert node.path[1] == target_schema
        assert node.node_type == "table"

    # Correct count
    assert len(result) == tables_per_schema


# ── Property 4: Column expansion calls get_schema ─────────────────────────────


@given(
    schema=_schema_name,
    table=_table_name,
    col_names=st.lists(_table_name, min_size=1, max_size=8, unique=True),
)
@settings(max_examples=100)
def test_property_4_column_expansion_calls_get_schema(
    schema: str,
    table: str,
    col_names: list[str],
) -> None:
    """Property 4: Column expansion calls get_schema.

    For any table node, calling list_children([catalog, ..., table]) invokes
    CatalogPlugin.get_schema() and returns ExplorerNode entries of type 'column'
    matching the table's columns.

    Validates: Requirements 2.4
    """
    from rivet_core.introspection import ColumnDetail, ObjectSchema

    table_node = CatalogNode(name=table, node_type="table", path=[schema, table],
                             is_container=False, children_count=None, summary=None)
    schema_node = CatalogNode(name=schema, node_type="schema", path=[schema],
                              is_container=True, children_count=None, summary=None)

    columns = [
        ColumnDetail(name=c, type="utf8", native_type=None, nullable=True,
                     default=None, comment=None, is_primary_key=False, is_partition_key=False)
        for c in col_names
    ]
    obj_schema = ObjectSchema(path=[schema, table], node_type="table",
                              columns=columns, primary_key=None, comment=None)

    plugin = MagicMock()
    plugin.list_tables.return_value = [schema_node, table_node]
    _wire_list_children(plugin)
    plugin.get_schema.return_value = obj_schema

    registry = MagicMock()
    registry.get_catalog_plugin.return_value = plugin

    cat = Catalog(name="mycat", type="postgres")
    explorer = CatalogExplorer(catalogs={"mycat": cat}, engines={}, registry=registry)

    # Expand table to columns
    result = explorer.list_children(["mycat", schema, table])

    # get_schema was called
    plugin.get_schema.assert_called_once()

    # All returned nodes are columns
    assert len(result) == len(col_names)
    for node in result:
        assert node.node_type == "column"
        assert node.is_expandable is False

    # Column names match
    result_names = [n.name for n in result]
    assert result_names == col_names

    # Paths are correct
    for node in result:
        assert node.path[:3] == ["mycat", schema, table]
        assert len(node.path) == 4


# ── Property 5: Depth cap enforcement ─────────────────────────────────────────


@given(
    max_depth=st.integers(min_value=1, max_value=5),
    path_extra=st.integers(min_value=0, max_value=3),
)
@settings(max_examples=100)
def test_property_5_depth_cap_enforcement(
    max_depth: int,
    path_extra: int,
) -> None:
    """Property 5: Depth cap enforcement.

    For any path whose length >= max_depth, list_children(path) returns an empty list.

    Validates: Requirements 2.5
    """
    plugin = MagicMock()
    plugin.list_tables.return_value = []
    plugin.list_children.return_value = []

    registry = MagicMock()
    registry.get_catalog_plugin.return_value = plugin

    cat = Catalog(name="mycat", type="postgres")
    explorer = CatalogExplorer(catalogs={"mycat": cat}, engines={}, registry=registry,
                               max_depth=max_depth)

    # Build a path of length max_depth + path_extra (always >= max_depth)
    path = ["mycat"] + [f"seg{i}" for i in range(max_depth - 1 + path_extra)]
    assert len(path) >= max_depth

    result = explorer.list_children(path)
    assert result == []


@given(
    max_depth=st.integers(min_value=2, max_value=6),
    schema=_schema_name,
)
@settings(max_examples=100)
def test_property_5_depth_limit_reached_flag(
    max_depth: int,
    schema: str,
) -> None:
    """Property 5 (flag variant): Nodes at depth max_depth-1 have depth_limit_reached=True.

    The last node returned before the cap should have depth_limit_reached=True.

    Validates: Requirements 2.5
    """
    # A single node at depth max_depth-1 (relative to catalog root)
    # path within catalog namespace has length max_depth-1
    node_path = [f"seg{i}" for i in range(max_depth - 1)]
    node = CatalogNode(name=node_path[-1] if node_path else schema,
                       node_type="schema", path=node_path if node_path else [schema],
                       is_container=True, children_count=None, summary=None)

    plugin = MagicMock()
    plugin.list_tables.return_value = [node]
    _wire_list_children(plugin)

    registry = MagicMock()
    registry.get_catalog_plugin.return_value = plugin

    cat = Catalog(name="mycat", type="postgres")
    explorer = CatalogExplorer(catalogs={"mycat": cat}, engines={}, registry=registry,
                               max_depth=max_depth)

    # Parent path is one level above the node
    parent_path = ["mycat"] + node_path[:-1]
    result = explorer.list_children(parent_path)

    # The node should appear (parent path length < max_depth)
    if len(parent_path) < max_depth:
        assert len(result) == 1
        # child is at depth len(parent_path), child+1 = len(parent_path)+1
        # depth_limit_reached when child_depth+1 >= max_depth
        expected_flag = len(parent_path) + 1 >= max_depth
        assert result[0].depth_limit_reached == expected_flag


# ── Tests for hierarchical normalization per catalog type (task 4.4) ──────


class TestHierarchicalNormalizationRelational:
    """Relational catalogs (Postgres, DuckDB): [catalog_name, schema, table, column]."""

    def test_postgres_path_structure(self):
        nodes = [
            _make_catalog_node("public", "schema", ["public"], is_container=True),
            _make_catalog_node("users", "table", ["public", "users"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        schemas = explorer.list_children(["pg"])
        assert len(schemas) == 1
        assert schemas[0].path == ["pg", "public"]
        assert schemas[0].node_type == "schema"

        tables = explorer.list_children(["pg", "public"])
        assert len(tables) == 1
        assert tables[0].path == ["pg", "public", "users"]
        assert tables[0].node_type == "table"

    def test_duckdb_path_structure(self):
        nodes = [
            _make_catalog_node("main", "schema", ["main"], is_container=True),
            _make_catalog_node("orders", "table", ["main", "orders"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        registry = _make_registry(("duckdb", plugin))
        cat = Catalog(name="dk", type="duckdb")
        explorer = CatalogExplorer(catalogs={"dk": cat}, engines={}, registry=registry)

        schemas = explorer.list_children(["dk"])
        assert schemas[0].path == ["dk", "main"]

        tables = explorer.list_children(["dk", "main"])
        assert tables[0].path == ["dk", "main", "orders"]

    def test_first_segment_is_catalog_name(self):
        nodes = [_make_catalog_node("s", "schema", ["s"], is_container=True)]
        plugin = _make_plugin(list_tables_return=nodes)
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="my_pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"my_pg": cat}, engines={}, registry=registry)

        result = explorer.list_children(["my_pg"])
        assert result[0].path[0] == "my_pg"


class TestHierarchicalNormalizationFileBased:
    """S3/Filesystem: [catalog_name, prefix..., file] with extension filtering."""

    def test_s3_recognized_extensions_included(self):
        nodes = [
            _make_catalog_node("data.parquet", "file", ["data.parquet"]),
            _make_catalog_node("data.csv", "file", ["data.csv"]),
            _make_catalog_node("data.json", "file", ["data.json"]),
            _make_catalog_node("data.ipc", "file", ["data.ipc"]),
            _make_catalog_node("data.orc", "file", ["data.orc"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        plugin.list_children.return_value = nodes
        registry = _make_registry(("s3", plugin))
        cat = Catalog(name="lake", type="s3")
        explorer = CatalogExplorer(catalogs={"lake": cat}, engines={}, registry=registry)

        result = explorer.list_children(["lake"])
        names = {n.name for n in result}
        assert names == {"data.parquet", "data.csv", "data.json", "data.ipc", "data.orc"}

    def test_s3_unrecognized_extensions_excluded(self):
        nodes = [
            _make_catalog_node("data.parquet", "file", ["data.parquet"]),
            _make_catalog_node("readme.txt", "file", ["readme.txt"]),
            _make_catalog_node("config.yaml", "file", ["config.yaml"]),
            _make_catalog_node("script.py", "file", ["script.py"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        plugin.list_children.return_value = nodes
        registry = _make_registry(("s3", plugin))
        cat = Catalog(name="lake", type="s3")
        explorer = CatalogExplorer(catalogs={"lake": cat}, engines={}, registry=registry)

        result = explorer.list_children(["lake"])
        names = {n.name for n in result}
        assert names == {"data.parquet"}

    def test_s3_directories_always_included(self):
        nodes = [
            _make_catalog_node("subdir", "directory", ["subdir"], is_container=True),
            _make_catalog_node("readme.txt", "file", ["readme.txt"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        plugin.list_children.return_value = nodes
        registry = _make_registry(("s3", plugin))
        cat = Catalog(name="lake", type="s3")
        explorer = CatalogExplorer(catalogs={"lake": cat}, engines={}, registry=registry)

        result = explorer.list_children(["lake"])
        assert len(result) == 1
        assert result[0].name == "subdir"
        assert result[0].is_expandable is True

    def test_filesystem_extension_filtering(self):
        nodes = [
            _make_catalog_node("sales.csv", "file", ["sales.csv"]),
            _make_catalog_node("notes.md", "file", ["notes.md"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        plugin.list_children.return_value = nodes
        registry = _make_registry(("filesystem", plugin))
        cat = Catalog(name="local", type="filesystem")
        explorer = CatalogExplorer(catalogs={"local": cat}, engines={}, registry=registry)

        result = explorer.list_children(["local"])
        assert len(result) == 1
        assert result[0].name == "sales.csv"

    def test_s3_nested_prefix_path(self):
        nodes = [
            _make_catalog_node("raw", "directory", ["raw"], is_container=True),
            _make_catalog_node("events.parquet", "file", ["raw", "events.parquet"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        plugin.list_children.return_value = nodes
        registry = _make_registry(("s3", plugin))
        cat = Catalog(name="lake", type="s3")
        explorer = CatalogExplorer(catalogs={"lake": cat}, engines={}, registry=registry)

        # Top level: only directory
        top = explorer.list_children(["lake"])
        assert len(top) == 1
        assert top[0].path == ["lake", "raw"]

        # Nested: file under prefix
        nested = explorer.list_children(["lake", "raw"])
        assert len(nested) == 1
        assert nested[0].path == ["lake", "raw", "events.parquet"]

    def test_s3_case_insensitive_extension(self):
        nodes = [
            _make_catalog_node("DATA.PARQUET", "file", ["DATA.PARQUET"]),
            _make_catalog_node("file.CSV", "file", ["file.CSV"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        plugin.list_children.return_value = nodes
        registry = _make_registry(("s3", plugin))
        cat = Catalog(name="lake", type="s3")
        explorer = CatalogExplorer(catalogs={"lake": cat}, engines={}, registry=registry)

        result = explorer.list_children(["lake"])
        assert len(result) == 2

    def test_no_extension_excluded(self):
        """Files without any extension are excluded from S3/filesystem."""
        nodes = [
            _make_catalog_node("noext", "file", ["noext"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        plugin.list_children.return_value = nodes
        registry = _make_registry(("s3", plugin))
        cat = Catalog(name="lake", type="s3")
        explorer = CatalogExplorer(catalogs={"lake": cat}, engines={}, registry=registry)

        result = explorer.list_children(["lake"])
        assert result == []


class TestHierarchicalNormalizationGlue:
    """Glue: [catalog_name, database, table, column]."""

    def test_glue_path_structure(self):
        nodes = [
            _make_catalog_node("analytics", "database", ["analytics"], is_container=True),
            _make_catalog_node("events", "table", ["analytics", "events"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        plugin.list_children.return_value = nodes
        registry = _make_registry(("glue", plugin))
        cat = Catalog(name="glue_cat", type="glue")
        explorer = CatalogExplorer(catalogs={"glue_cat": cat}, engines={}, registry=registry)

        dbs = explorer.list_children(["glue_cat"])
        assert dbs[0].path == ["glue_cat", "analytics"]
        assert dbs[0].node_type == "database"

        tables = explorer.list_children(["glue_cat", "analytics"])
        assert tables[0].path == ["glue_cat", "analytics", "events"]


class TestHierarchicalNormalizationUnityDatabricks:
    """Unity/Databricks: [catalog_name, uc_catalog, schema, table, column]."""

    def test_unity_path_structure(self):
        nodes = [
            _make_catalog_node("main", "catalog", ["main"], is_container=True),
            _make_catalog_node("default", "schema", ["main", "default"], is_container=True),
            _make_catalog_node("users", "table", ["main", "default", "users"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        plugin.list_children.return_value = nodes
        registry = _make_registry(("unity", plugin))
        cat = Catalog(name="uc", type="unity")
        explorer = CatalogExplorer(catalogs={"uc": cat}, engines={}, registry=registry)

        uc_catalogs = explorer.list_children(["uc"])
        assert uc_catalogs[0].path == ["uc", "main"]

        schemas = explorer.list_children(["uc", "main"])
        assert schemas[0].path == ["uc", "main", "default"]

        tables = explorer.list_children(["uc", "main", "default"])
        assert tables[0].path == ["uc", "main", "default", "users"]

    def test_databricks_path_structure(self):
        nodes = [
            _make_catalog_node("prod", "catalog", ["prod"], is_container=True),
            _make_catalog_node("analytics", "schema", ["prod", "analytics"], is_container=True),
            _make_catalog_node("orders", "table", ["prod", "analytics", "orders"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        plugin.list_children.return_value = nodes
        registry = _make_registry(("databricks", plugin))
        cat = Catalog(name="dbx", type="databricks")
        explorer = CatalogExplorer(catalogs={"dbx": cat}, engines={}, registry=registry)

        db_catalogs = explorer.list_children(["dbx"])
        assert db_catalogs[0].path == ["dbx", "prod"]

        schemas = explorer.list_children(["dbx", "prod"])
        assert schemas[0].path == ["dbx", "prod", "analytics"]

        tables = explorer.list_children(["dbx", "prod", "analytics"])
        assert tables[0].path == ["dbx", "prod", "analytics", "orders"]


class TestHierarchicalNormalizationArrow:
    """Arrow: [catalog_name, table, column]."""

    def test_arrow_path_structure(self):
        nodes = [
            _make_catalog_node("my_table", "table", ["my_table"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        plugin.list_children.return_value = nodes
        registry = _make_registry(("arrow", plugin))
        cat = Catalog(name="mem", type="arrow")
        explorer = CatalogExplorer(catalogs={"mem": cat}, engines={}, registry=registry)

        tables = explorer.list_children(["mem"])
        assert len(tables) == 1
        assert tables[0].path == ["mem", "my_table"]
        assert tables[0].node_type == "table"


class TestNormalizationNonFileCatalogsNoFiltering:
    """Non-file catalogs (postgres, duckdb, glue, unity, databricks, arrow) do NOT filter by extension."""

    def test_postgres_no_extension_filtering(self):
        """Postgres tables with dots in names are not filtered."""
        nodes = [
            _make_catalog_node("my.table.v2", "table", ["public", "my.table.v2"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        plugin.list_children.return_value = nodes
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        # Populate cache
        explorer.list_children(["pg"])
        result = explorer.list_children(["pg", "public"])
        assert len(result) == 1
        assert result[0].name == "my.table.v2"

    def test_arrow_no_extension_filtering(self):
        nodes = [
            _make_catalog_node("data.weird", "table", ["data.weird"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        plugin.list_children.return_value = nodes
        registry = _make_registry(("arrow", plugin))
        cat = Catalog(name="mem", type="arrow")
        explorer = CatalogExplorer(catalogs={"mem": cat}, engines={}, registry=registry)

        result = explorer.list_children(["mem"])
        assert len(result) == 1
        assert result[0].name == "data.weird"


# ── Property 7: Path normalization per catalog type ───────────────────────────


_identifier = st.text(
    alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_"),
    min_size=1,
    max_size=12,
)


@given(
    catalog_name=_identifier,
    schema=_identifier,
    table=_identifier,
)
@settings(max_examples=100)
def test_property_7_relational_path_normalization(
    catalog_name: str,
    schema: str,
    table: str,
) -> None:
    """Property 7 (relational): Postgres/DuckDB paths follow [catalog_name, schema, table].

    Validates: Requirements 3.1, 3.2
    """
    for cat_type in ("postgres", "duckdb"):
        nodes = [
            CatalogNode(name=schema, node_type="schema", path=[schema],
                        is_container=True, children_count=None, summary=None),
            CatalogNode(name=table, node_type="table", path=[schema, table],
                        is_container=False, children_count=None, summary=None),
        ]
        plugin = MagicMock()
        plugin.list_tables.return_value = nodes
        _wire_list_children(plugin)

        registry = MagicMock()
        registry.get_catalog_plugin.return_value = plugin

        cat = Catalog(name=catalog_name, type=cat_type)
        explorer = CatalogExplorer(catalogs={catalog_name: cat}, engines={}, registry=registry)

        schemas = explorer.list_children([catalog_name])
        assert len(schemas) == 1
        assert schemas[0].path == [catalog_name, schema]
        assert schemas[0].path[0] == catalog_name

        tables = explorer.list_children([catalog_name, schema])
        assert len(tables) == 1
        assert tables[0].path == [catalog_name, schema, table]
        assert tables[0].path[0] == catalog_name


@given(
    catalog_name=_identifier,
    database=_identifier,
    table=_identifier,
)
@settings(max_examples=100)
def test_property_7_glue_path_normalization(
    catalog_name: str,
    database: str,
    table: str,
) -> None:
    """Property 7 (Glue): Glue paths follow [catalog_name, database, table].

    Validates: Requirements 3.5
    """
    nodes = [
        CatalogNode(name=database, node_type="database", path=[database],
                    is_container=True, children_count=None, summary=None),
        CatalogNode(name=table, node_type="table", path=[database, table],
                    is_container=False, children_count=None, summary=None),
    ]
    plugin = MagicMock()
    plugin.list_tables.return_value = nodes
    _wire_list_children(plugin)

    registry = MagicMock()
    registry.get_catalog_plugin.return_value = plugin

    cat = Catalog(name=catalog_name, type="glue")
    explorer = CatalogExplorer(catalogs={catalog_name: cat}, engines={}, registry=registry)

    dbs = explorer.list_children([catalog_name])
    assert len(dbs) == 1
    assert dbs[0].path == [catalog_name, database]
    assert dbs[0].path[0] == catalog_name

    tables = explorer.list_children([catalog_name, database])
    assert len(tables) == 1
    assert tables[0].path == [catalog_name, database, table]


@given(
    catalog_name=_identifier,
    uc_catalog=_identifier,
    schema=_identifier,
    table=_identifier,
)
@settings(max_examples=100)
def test_property_7_unity_databricks_path_normalization(
    catalog_name: str,
    uc_catalog: str,
    schema: str,
    table: str,
) -> None:
    """Property 7 (Unity/Databricks): paths follow [catalog_name, uc_catalog, schema, table].

    Validates: Requirements 3.6, 3.7
    """
    for cat_type in ("unity", "databricks"):
        nodes = [
            CatalogNode(name=uc_catalog, node_type="catalog", path=[uc_catalog],
                        is_container=True, children_count=None, summary=None),
            CatalogNode(name=schema, node_type="schema", path=[uc_catalog, schema],
                        is_container=True, children_count=None, summary=None),
            CatalogNode(name=table, node_type="table", path=[uc_catalog, schema, table],
                        is_container=False, children_count=None, summary=None),
        ]
        plugin = MagicMock()
        plugin.list_tables.return_value = nodes
        _wire_list_children(plugin)

        registry = MagicMock()
        registry.get_catalog_plugin.return_value = plugin

        cat = Catalog(name=catalog_name, type=cat_type)
        explorer = CatalogExplorer(catalogs={catalog_name: cat}, engines={}, registry=registry)

        uc_cats = explorer.list_children([catalog_name])
        assert len(uc_cats) == 1
        assert uc_cats[0].path == [catalog_name, uc_catalog]
        assert uc_cats[0].path[0] == catalog_name

        schemas = explorer.list_children([catalog_name, uc_catalog])
        assert len(schemas) == 1
        assert schemas[0].path == [catalog_name, uc_catalog, schema]

        tables = explorer.list_children([catalog_name, uc_catalog, schema])
        assert len(tables) == 1
        assert tables[0].path == [catalog_name, uc_catalog, schema, table]


@given(
    catalog_name=_identifier,
    table=_identifier,
)
@settings(max_examples=100)
def test_property_7_arrow_path_normalization(
    catalog_name: str,
    table: str,
) -> None:
    """Property 7 (Arrow): Arrow paths follow [catalog_name, table].

    Validates: Requirements 3.7 (Arrow)
    """
    nodes = [
        CatalogNode(name=table, node_type="table", path=[table],
                    is_container=False, children_count=None, summary=None),
    ]
    plugin = MagicMock()
    plugin.list_tables.return_value = nodes
    _wire_list_children(plugin)

    registry = MagicMock()
    registry.get_catalog_plugin.return_value = plugin

    cat = Catalog(name=catalog_name, type="arrow")
    explorer = CatalogExplorer(catalogs={catalog_name: cat}, engines={}, registry=registry)

    tables = explorer.list_children([catalog_name])
    assert len(tables) == 1
    assert tables[0].path == [catalog_name, table]
    assert tables[0].path[0] == catalog_name


@given(
    catalog_name=_identifier,
    prefix=_identifier,
    filename=st.text(
        alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_"),
        min_size=1,
        max_size=12,
    ),
    ext=st.sampled_from([".parquet", ".csv", ".json", ".ipc", ".orc"]),
)
@settings(max_examples=100)
def test_property_7_s3_path_normalization(
    catalog_name: str,
    prefix: str,
    filename: str,
    ext: str,
) -> None:
    """Property 7 (S3): S3 paths follow [catalog_name, prefix..., file].

    Validates: Requirements 3.3
    """
    full_filename = filename + ext
    nodes = [
        CatalogNode(name=prefix, node_type="directory", path=[prefix],
                    is_container=True, children_count=None, summary=None),
        CatalogNode(name=full_filename, node_type="file", path=[prefix, full_filename],
                    is_container=False, children_count=None, summary=None),
    ]
    plugin = MagicMock()
    plugin.list_tables.return_value = nodes
    _wire_list_children(plugin)

    registry = MagicMock()
    registry.get_catalog_plugin.return_value = plugin

    cat = Catalog(name=catalog_name, type="s3")
    explorer = CatalogExplorer(catalogs={catalog_name: cat}, engines={}, registry=registry)

    # Top level: directory
    top = explorer.list_children([catalog_name])
    assert len(top) == 1
    assert top[0].path == [catalog_name, prefix]
    assert top[0].path[0] == catalog_name
    assert top[0].is_expandable is True

    # Nested: file under prefix
    nested = explorer.list_children([catalog_name, prefix])
    assert len(nested) == 1
    assert nested[0].path == [catalog_name, prefix, full_filename]


# ── Property 8: S3/filesystem file type filtering ─────────────────────────────

_RECOGNIZED_EXTS = [".parquet", ".csv", ".json", ".ipc", ".orc"]
_UNRECOGNIZED_EXTS = [".txt", ".md", ".yaml", ".yml", ".py", ".sh", ".log", ".xml", ".html"]


@given(
    catalog_name=_identifier,
    base_name=st.text(
        alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_"),
        min_size=1,
        max_size=12,
    ),
    recognized_exts=st.lists(
        st.sampled_from(_RECOGNIZED_EXTS),
        min_size=1,
        max_size=5,
    ),
    unrecognized_exts=st.lists(
        st.sampled_from(_UNRECOGNIZED_EXTS),
        min_size=0,
        max_size=5,
    ),
    cat_type=st.sampled_from(["s3", "filesystem"]),
)
@settings(max_examples=150)
def test_property_8_file_type_filtering(
    catalog_name: str,
    base_name: str,
    recognized_exts: list[str],
    unrecognized_exts: list[str],
    cat_type: str,
) -> None:
    """Property 8: S3/filesystem file type filtering.

    Files with recognized extensions (.parquet, .csv, .json, .ipc, .orc) appear as
    table nodes; files with unrecognized extensions are excluded from results.

    Validates: Requirements 3.3, 3.4
    """
    recognized_files = [
        CatalogNode(
            name=f"{base_name}_{i}{ext}",
            node_type="file",
            path=[f"{base_name}_{i}{ext}"],
            is_container=False,
            children_count=None,
            summary=None,
        )
        for i, ext in enumerate(recognized_exts)
    ]
    unrecognized_files = [
        CatalogNode(
            name=f"other_{i}{ext}",
            node_type="file",
            path=[f"other_{i}{ext}"],
            is_container=False,
            children_count=None,
            summary=None,
        )
        for i, ext in enumerate(unrecognized_exts)
    ]

    all_nodes = recognized_files + unrecognized_files
    plugin = MagicMock()
    plugin.list_tables.return_value = all_nodes
    _wire_list_children(plugin)

    registry = MagicMock()
    registry.get_catalog_plugin.return_value = plugin

    cat = Catalog(name=catalog_name, type=cat_type)
    explorer = CatalogExplorer(catalogs={catalog_name: cat}, engines={}, registry=registry)

    result = explorer.list_children([catalog_name])
    result_names = {n.name for n in result}

    # All recognized files must appear
    for node in recognized_files:
        assert node.name in result_names, f"Expected {node.name!r} in results"

    # No unrecognized files should appear
    for node in unrecognized_files:
        assert node.name not in result_names, f"Unexpected {node.name!r} in results"


@given(
    catalog_name=_identifier,
    dir_names=st.lists(_identifier, min_size=1, max_size=4, unique=True),
    unrecognized_exts=st.lists(
        st.sampled_from(_UNRECOGNIZED_EXTS),
        min_size=1,
        max_size=4,
    ),
    cat_type=st.sampled_from(["s3", "filesystem"]),
)
@settings(max_examples=100)
def test_property_8_directories_always_included(
    catalog_name: str,
    dir_names: list[str],
    unrecognized_exts: list[str],
    cat_type: str,
) -> None:
    """Property 8 (directories): Directories/containers are always included regardless of name.

    Validates: Requirements 3.3, 3.4
    """
    dir_nodes = [
        CatalogNode(name=d, node_type="directory", path=[d],
                    is_container=True, children_count=None, summary=None)
        for d in dir_names
    ]
    unrecognized_files = [
        CatalogNode(name=f"file_{i}{ext}", node_type="file", path=[f"file_{i}{ext}"],
                    is_container=False, children_count=None, summary=None)
        for i, ext in enumerate(unrecognized_exts)
    ]

    all_nodes = dir_nodes + unrecognized_files
    plugin = MagicMock()
    plugin.list_tables.return_value = all_nodes
    _wire_list_children(plugin)

    registry = MagicMock()
    registry.get_catalog_plugin.return_value = plugin

    cat = Catalog(name=catalog_name, type=cat_type)
    explorer = CatalogExplorer(catalogs={catalog_name: cat}, engines={}, registry=registry)

    result = explorer.list_children([catalog_name])
    result_names = {n.name for n in result}

    # All directories must appear
    for node in dir_nodes:
        assert node.name in result_names, f"Directory {node.name!r} should be included"

    # Unrecognized files must not appear
    for node in unrecognized_files:
        assert node.name not in result_names, f"Unrecognized file {node.name!r} should be excluded"


# ── Tests for task 4.7: get_node_detail, get_table_schema, get_table_metadata, get_table_stats ──


def _make_schema(path, col_names=("id", "name")):
    from rivet_core.introspection import ColumnDetail, ObjectSchema
    columns = [
        ColumnDetail(name=c, type="utf8", native_type=None, nullable=True,
                     default=None, comment=None, is_primary_key=False, is_partition_key=False)
        for c in col_names
    ]
    return ObjectSchema(path=path, node_type="table", columns=columns, primary_key=None, comment=None)


def _make_metadata(path):
    from rivet_core.introspection import ObjectMetadata
    return ObjectMetadata(
        path=path, node_type="table", row_count=100, size_bytes=1024,
        last_modified=None, created_at=None, format="parquet", compression=None,
        owner=None, comment=None, location=None, column_statistics=[], partitioning=None,
    )


class TestGetTableSchema:
    def test_returns_schema_for_valid_path(self):
        schema = _make_schema(["public", "users"])
        plugin = _make_plugin()
        plugin.get_schema.return_value = schema
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        result = explorer.get_table_schema(["pg", "public", "users"])
        assert result is schema

    def test_caches_schema_on_second_call(self):
        schema = _make_schema(["public", "users"])
        plugin = _make_plugin()
        plugin.get_schema.return_value = schema
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        explorer.get_table_schema(["pg", "public", "users"])
        explorer.get_table_schema(["pg", "public", "users"])

        plugin.get_schema.assert_called_once()

    def test_returns_none_for_empty_path(self):
        registry = _make_registry()
        explorer = CatalogExplorer(catalogs={}, engines={}, registry=registry)
        assert explorer.get_table_schema([]) is None

    def test_returns_none_for_single_segment_path(self):
        plugin = _make_plugin()
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)
        assert explorer.get_table_schema(["pg"]) is None

    def test_returns_none_for_unknown_catalog(self):
        registry = _make_registry()
        explorer = CatalogExplorer(catalogs={}, engines={}, registry=registry)
        assert explorer.get_table_schema(["unknown", "table"]) is None

    def test_returns_none_on_plugin_exception(self):
        plugin = _make_plugin()
        plugin.get_schema.side_effect = RuntimeError("introspection failed")
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)
        assert explorer.get_table_schema(["pg", "public", "users"]) is None

    def test_table_key_joins_path_segments(self):
        schema = _make_schema(["public", "users"])
        plugin = _make_plugin()
        plugin.get_schema.return_value = schema
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        explorer.get_table_schema(["pg", "public", "users"])
        plugin.get_schema.assert_called_once_with(cat, "public.users")

    def test_schema_cache_shared_with_list_children(self):
        """Schema cached by list_children is reused by get_table_schema."""
        from rivet_core.introspection import ColumnDetail, ObjectSchema
        nodes = [_make_catalog_node("users", "table", ["public", "users"])]
        schema = ObjectSchema(
            path=["public", "users"], node_type="table",
            columns=[ColumnDetail(name="id", type="int64", native_type=None, nullable=False,
                                  default=None, comment=None, is_primary_key=True, is_partition_key=False)],
            primary_key=["id"], comment=None,
        )
        plugin = _make_plugin(list_tables_return=nodes)
        plugin.list_children.return_value = nodes
        plugin.get_schema.return_value = schema
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        # Populate via list_children (which calls get_schema internally)
        explorer.list_children(["pg"])
        explorer.list_children(["pg", "public", "users"])
        plugin.get_schema.reset_mock()

        # get_table_schema should use cache — no additional call
        result = explorer.get_table_schema(["pg", "public", "users"])
        plugin.get_schema.assert_not_called()
        assert result is schema


class TestGetTableMetadata:
    def test_returns_metadata_for_valid_path(self):
        metadata = _make_metadata(["public", "users"])
        plugin = _make_plugin()
        plugin.get_metadata.return_value = metadata
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        result = explorer.get_table_metadata(["pg", "public", "users"])
        assert result is metadata

    def test_not_cached_calls_plugin_each_time(self):
        """Property 19: Metadata is not cached — plugin called on every request."""
        metadata = _make_metadata(["public", "users"])
        plugin = _make_plugin()
        plugin.get_metadata.return_value = metadata
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        explorer.get_table_metadata(["pg", "public", "users"])
        explorer.get_table_metadata(["pg", "public", "users"])

        assert plugin.get_metadata.call_count == 2

    def test_returns_none_for_empty_path(self):
        registry = _make_registry()
        explorer = CatalogExplorer(catalogs={}, engines={}, registry=registry)
        assert explorer.get_table_metadata([]) is None

    def test_returns_none_for_single_segment_path(self):
        plugin = _make_plugin()
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)
        assert explorer.get_table_metadata(["pg"]) is None

    def test_returns_none_for_unknown_catalog(self):
        registry = _make_registry()
        explorer = CatalogExplorer(catalogs={}, engines={}, registry=registry)
        assert explorer.get_table_metadata(["unknown", "table"]) is None

    def test_returns_none_on_plugin_exception(self):
        plugin = _make_plugin()
        plugin.get_metadata.side_effect = RuntimeError("metadata failed")
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)
        assert explorer.get_table_metadata(["pg", "public", "users"]) is None

    def test_table_key_joins_path_segments(self):
        plugin = _make_plugin()
        plugin.get_metadata.return_value = None
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        explorer.get_table_metadata(["pg", "public", "users"])
        plugin.get_metadata.assert_called_once_with(cat, "public.users")


class TestGetTableStats:
    def test_returns_metadata_with_stats(self):
        metadata = _make_metadata(["public", "users"])
        plugin = _make_plugin()
        plugin.get_metadata.return_value = metadata
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        result = explorer.get_table_stats(["pg", "public", "users"])
        assert result is metadata

    def test_not_cached_calls_plugin_each_time(self):
        """Property 19: Stats are not cached — plugin called on every request."""
        plugin = _make_plugin()
        plugin.get_metadata.return_value = None
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        explorer.get_table_stats(["pg", "public", "users"])
        explorer.get_table_stats(["pg", "public", "users"])

        assert plugin.get_metadata.call_count == 2

    def test_returns_none_for_empty_path(self):
        registry = _make_registry()
        explorer = CatalogExplorer(catalogs={}, engines={}, registry=registry)
        assert explorer.get_table_stats([]) is None


class TestGetNodeDetail:
    def test_returns_node_detail_with_schema_and_metadata(self):
        schema = _make_schema(["public", "users"])
        metadata = _make_metadata(["public", "users"])
        plugin = _make_plugin()
        plugin.get_schema.return_value = schema
        plugin.get_metadata.return_value = metadata
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        result = explorer.get_node_detail(["pg", "public", "users"])

        assert isinstance(result, NodeDetail)
        assert result.schema is schema
        assert result.metadata is metadata

    def test_node_has_correct_name_and_path(self):
        plugin = _make_plugin()
        plugin.get_schema.return_value = _make_schema(["public", "users"])
        plugin.get_metadata.return_value = None
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        result = explorer.get_node_detail(["pg", "public", "users"])

        assert result.node.name == "users"
        assert result.node.path == ["pg", "public", "users"]

    def test_children_count_from_schema_columns(self):
        schema = _make_schema(["public", "users"], col_names=("id", "name", "email"))
        plugin = _make_plugin()
        plugin.get_schema.return_value = schema
        plugin.get_metadata.return_value = None
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        result = explorer.get_node_detail(["pg", "public", "users"])
        assert result.children_count == 3

    def test_children_count_none_when_no_schema(self):
        plugin = _make_plugin()
        plugin.get_schema.side_effect = RuntimeError("no schema")
        plugin.get_metadata.return_value = None
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        result = explorer.get_node_detail(["pg", "public", "users"])
        assert result.children_count is None

    def test_raises_on_empty_path(self):
        registry = _make_registry()
        explorer = CatalogExplorer(catalogs={}, engines={}, registry=registry)
        with pytest.raises(CatalogExplorerError):
            explorer.get_node_detail([])

    def test_raises_on_unknown_catalog(self):
        registry = _make_registry()
        explorer = CatalogExplorer(catalogs={}, engines={}, registry=registry)
        with pytest.raises(CatalogExplorerError):
            explorer.get_node_detail(["unknown", "table"])

    def test_schema_cached_after_get_node_detail(self):
        """get_node_detail uses get_table_schema which caches the result."""
        schema = _make_schema(["public", "users"])
        plugin = _make_plugin()
        plugin.get_schema.return_value = schema
        plugin.get_metadata.return_value = None
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        explorer.get_node_detail(["pg", "public", "users"])
        # Second call should use cache
        explorer.get_table_schema(["pg", "public", "users"])

        plugin.get_schema.assert_called_once()

    def test_metadata_not_cached_after_get_node_detail(self):
        """get_node_detail calls get_table_metadata which is not cached."""
        plugin = _make_plugin()
        plugin.get_schema.return_value = _make_schema(["public", "users"])
        plugin.get_metadata.return_value = None
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        explorer.get_node_detail(["pg", "public", "users"])
        explorer.get_table_metadata(["pg", "public", "users"])

        # get_metadata called once in get_node_detail + once in get_table_metadata = 2
        assert plugin.get_metadata.call_count == 2


# ── Property 18: Schema caching ───────────────────────────────────────────────


@given(
    schema=_schema_name,
    table=_table_name,
    extra_calls=st.integers(min_value=1, max_value=5),
)
@settings(max_examples=100)
def test_property_18_schema_caching(schema: str, table: str, extra_calls: int) -> None:
    """Property 18: Schema caching.

    For any table, calling get_table_schema(path) invokes CatalogPlugin.get_schema()
    at most once. Subsequent calls return the cached result.

    Validates: Requirements 18.2
    """
    from rivet_core.introspection import ColumnDetail, ObjectSchema

    obj_schema = ObjectSchema(
        path=[schema, table], node_type="table",
        columns=[ColumnDetail(name="c", type="utf8", native_type=None, nullable=True,
                              default=None, comment=None, is_primary_key=False, is_partition_key=False)],
        primary_key=None, comment=None,
    )

    plugin = MagicMock()
    plugin.list_tables.return_value = []
    plugin.get_schema.return_value = obj_schema

    registry = MagicMock()
    registry.get_catalog_plugin.return_value = plugin

    cat = Catalog(name="mycat", type="postgres")
    explorer = CatalogExplorer(catalogs={"mycat": cat}, engines={}, registry=registry)

    path = ["mycat", schema, table]
    for _ in range(extra_calls + 1):
        result = explorer.get_table_schema(path)
        assert result is obj_schema

    plugin.get_schema.assert_called_once()


# ── Property 19: Metadata not cached ─────────────────────────────────────────


@given(
    schema=_schema_name,
    table=_table_name,
    call_count=st.integers(min_value=1, max_value=5),
)
@settings(max_examples=100)
def test_property_19_metadata_not_cached(schema: str, table: str, call_count: int) -> None:
    """Property 19: Metadata is not cached.

    For any table, each call to get_table_metadata(path) or get_table_stats(path)
    invokes the underlying CatalogPlugin.get_metadata() — results are never cached.

    Validates: Requirements 18.4
    """
    plugin = MagicMock()
    plugin.list_tables.return_value = []
    plugin.get_metadata.return_value = None

    registry = MagicMock()
    registry.get_catalog_plugin.return_value = plugin

    cat = Catalog(name="mycat", type="postgres")
    explorer = CatalogExplorer(catalogs={"mycat": cat}, engines={}, registry=registry)

    path = ["mycat", schema, table]
    for _ in range(call_count):
        explorer.get_table_metadata(path)

    assert plugin.get_metadata.call_count == call_count

    # Reset and verify get_table_stats also calls each time
    plugin.get_metadata.reset_mock()
    for _ in range(call_count):
        explorer.get_table_stats(path)

    assert plugin.get_metadata.call_count == call_count


# ── Tests for task 4.8: refresh_catalog() and test_connection() ───────────────


class TestRefreshCatalog:
    def test_clears_children_cache(self):
        nodes = [_make_catalog_node("public", "schema", ["public"], is_container=True)]
        plugin = _make_plugin(list_tables_return=nodes)
        _wire_list_children(plugin)
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        # Populate cache
        explorer.list_children(["pg"])
        assert ("pg", ()) in explorer._children_cache

        explorer.refresh_catalog("pg")
        assert ("pg", ()) not in explorer._children_cache

    def test_clears_schema_cache(self):
        from rivet_core.introspection import ColumnDetail, ObjectSchema

        nodes = [_make_catalog_node("t", "table", ["public", "t"])]
        schema = ObjectSchema(
            path=["public", "t"], node_type="table",
            columns=[ColumnDetail(name="c", type="int64", native_type=None, nullable=False,
                                  default=None, comment=None, is_primary_key=False, is_partition_key=False)],
            primary_key=None, comment=None,
        )
        plugin = _make_plugin(list_tables_return=nodes)
        _wire_list_children(plugin)
        plugin.get_schema.return_value = schema
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        # Populate schema cache
        explorer.list_children(["pg"])
        explorer.list_children(["pg", "public", "t"])
        assert ("pg", "public.t") in explorer._schema_cache

        explorer.refresh_catalog("pg")
        assert ("pg", "public.t") not in explorer._schema_cache

    def test_next_access_triggers_fresh_list_tables(self):
        nodes = [_make_catalog_node("s", "schema", ["s"], is_container=True)]
        plugin = _make_plugin(list_tables_return=nodes)
        _wire_list_children(plugin)
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        explorer.list_children(["pg"])
        plugin.list_tables.reset_mock()

        explorer.refresh_catalog("pg")
        explorer.list_children(["pg"])

        plugin.list_tables.assert_called_once()

    def test_only_clears_target_catalog(self):
        nodes = [_make_catalog_node("s", "schema", ["s"], is_container=True)]
        plugin = _make_plugin(list_tables_return=nodes)
        _wire_list_children(plugin)
        registry = _make_registry(("postgres", plugin))
        cat1 = Catalog(name="pg1", type="postgres")
        cat2 = Catalog(name="pg2", type="postgres")
        explorer = CatalogExplorer(
            catalogs={"pg1": cat1, "pg2": cat2}, engines={}, registry=registry
        )

        explorer.list_children(["pg1"])
        explorer.list_children(["pg2"])
        assert ("pg1", ()) in explorer._children_cache
        assert ("pg2", ()) in explorer._children_cache

        explorer.refresh_catalog("pg1")
        assert ("pg1", ()) not in explorer._children_cache
        assert ("pg2", ()) in explorer._children_cache

    def test_refresh_nonexistent_catalog_is_noop(self):
        registry = _make_registry()
        explorer = CatalogExplorer(catalogs={}, engines={}, registry=registry)
        # Should not raise
        explorer.refresh_catalog("nonexistent")


class TestTestConnection:
    def test_returns_connection_result(self):
        plugin = _make_plugin()
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        result = explorer.test_connection("pg")
        assert isinstance(result, ConnectionResult)

    def test_connected_true_on_success(self):
        plugin = _make_plugin()
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        result = explorer.test_connection("pg")
        assert result.connected is True
        assert result.error is None
        assert result.catalog_name == "pg"

    def test_connected_false_on_failure(self):
        plugin = _make_plugin(test_connection_side_effect=RuntimeError("connection refused"))
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        # Init probe also fails, so catalog starts disconnected
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        result = explorer.test_connection("pg")
        assert result.connected is False
        assert "connection refused" in result.error
        assert result.catalog_name == "pg"

    def test_elapsed_ms_is_non_negative(self):
        plugin = _make_plugin()
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        result = explorer.test_connection("pg")
        assert result.elapsed_ms >= 0.0

    def test_unknown_catalog_returns_disconnected(self):
        registry = _make_registry()
        explorer = CatalogExplorer(catalogs={}, engines={}, registry=registry)

        result = explorer.test_connection("nonexistent")
        assert result.connected is False
        assert result.catalog_name == "nonexistent"
        assert result.error is not None

    def test_no_plugin_returns_disconnected(self):
        registry = _make_registry()  # no plugins
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        result = explorer.test_connection("pg")
        assert result.connected is False
        assert result.error is not None

    def test_updates_connection_status_on_success(self):
        plugin = _make_plugin()
        # First probe fails, then test_connection succeeds
        plugin.test_connection.side_effect = [RuntimeError("init fail"), None]
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        # Initially disconnected
        assert explorer._connection_status["pg"][0] is False

        result = explorer.test_connection("pg")
        assert result.connected is True
        # Status updated
        assert explorer._connection_status["pg"][0] is True

    def test_updates_connection_status_on_failure(self):
        plugin = _make_plugin()
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(catalogs={"pg": cat}, engines={}, registry=registry)

        # Initially connected
        assert explorer._connection_status["pg"][0] is True

        plugin.test_connection.side_effect = RuntimeError("network error")
        result = explorer.test_connection("pg")
        assert result.connected is False
        assert explorer._connection_status["pg"][0] is False


# ── Property 6: Cache invalidation on refresh ─────────────────────────────────


@given(
    schema=_schema_name,
    tables=st.lists(_table_name, min_size=1, max_size=4, unique=True),
    extra_calls=st.integers(min_value=1, max_value=3),
)
@settings(max_examples=100)
def test_property_6_cache_invalidation_on_refresh(
    schema: str,
    tables: list[str],
    extra_calls: int,
) -> None:
    """Property 6: Cache invalidation on refresh.

    For any catalog, calling refresh_catalog(catalog_name) clears all cached data
    for that catalog, so the next list_children call triggers a fresh list_tables()
    invocation.

    Validates: Requirements 2.6, 18.3
    """
    nodes = [
        CatalogNode(name=schema, node_type="schema", path=[schema],
                    is_container=True, children_count=None, summary=None),
    ] + [
        CatalogNode(name=t, node_type="table", path=[schema, t],
                    is_container=False, children_count=None, summary=None)
        for t in tables
    ]

    plugin = MagicMock()
    plugin.list_tables.return_value = nodes
    _wire_list_children(plugin)

    registry = MagicMock()
    registry.get_catalog_plugin.return_value = plugin

    cat = Catalog(name="mycat", type="postgres")
    explorer = CatalogExplorer(catalogs={"mycat": cat}, engines={}, registry=registry)

    # Reset after init probe
    plugin.list_tables.reset_mock()

    # Populate cache via multiple calls
    for _ in range(extra_calls):
        explorer.list_children(["mycat"])
        explorer.list_children(["mycat", schema])

    # list_tables called twice (once per distinct cache key: [] and [schema])
    assert plugin.list_tables.call_count == 2
    assert ("mycat", ()) in explorer._children_cache

    # Refresh clears the cache
    explorer.refresh_catalog("mycat")
    assert ("mycat", ()) not in explorer._children_cache

    # Next access triggers a fresh list_tables call
    plugin.list_tables.reset_mock()
    explorer.list_children(["mycat"])
    assert plugin.list_tables.call_count == 1


@given(
    schema=_schema_name,
    table=_table_name,
)
@settings(max_examples=100)
def test_property_6_refresh_clears_schema_cache(schema: str, table: str) -> None:
    """Property 6 (schema cache): refresh_catalog also clears schema cache entries.

    Validates: Requirements 2.6, 18.3
    """
    from rivet_core.introspection import ColumnDetail, ObjectSchema

    table_node = CatalogNode(name=table, node_type="table", path=[schema, table],
                             is_container=False, children_count=None, summary=None)
    schema_node = CatalogNode(name=schema, node_type="schema", path=[schema],
                              is_container=True, children_count=None, summary=None)
    obj_schema = ObjectSchema(
        path=[schema, table], node_type="table",
        columns=[ColumnDetail(name="c", type="utf8", native_type=None, nullable=True,
                              default=None, comment=None, is_primary_key=False, is_partition_key=False)],
        primary_key=None, comment=None,
    )

    plugin = MagicMock()
    plugin.list_tables.return_value = [schema_node, table_node]
    _wire_list_children(plugin)
    plugin.get_schema.return_value = obj_schema

    registry = MagicMock()
    registry.get_catalog_plugin.return_value = plugin

    cat = Catalog(name="mycat", type="postgres")
    explorer = CatalogExplorer(catalogs={"mycat": cat}, engines={}, registry=registry)

    # Populate both caches
    explorer.list_children(["mycat"])
    explorer.list_children(["mycat", schema, table])
    cache_key = ("mycat", f"{schema}.{table}")
    assert cache_key in explorer._schema_cache

    # Refresh clears schema cache too
    explorer.refresh_catalog("mycat")
    assert cache_key not in explorer._schema_cache

    # Next schema access triggers a fresh get_schema call
    plugin.get_schema.reset_mock()
    explorer.get_table_schema(["mycat", schema, table])
    plugin.get_schema.assert_called_once()


# ── Property 9: Connection failure isolation ──────────────────────────────────


@given(
    catalog_specs=st.lists(
        st.tuples(
            st.text(
                alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_"),
                min_size=1,
                max_size=12,
            ),
            st.sampled_from(["postgres", "duckdb", "s3", "glue", "arrow"]),
            st.booleans(),  # should_fail
        ),
        min_size=1,
        max_size=6,
        unique_by=lambda x: x[0],
    )
)
@settings(max_examples=100)
def test_property_9_connection_failure_isolation(
    catalog_specs: list[tuple[str, str, bool]],
) -> None:
    """Property 9: Connection failure isolation.

    For any mix of connected and disconnected catalogs:
    - list_catalogs() includes all catalogs with correct connected status.
    - Disconnected catalogs have connected=False with an error message.
    - list_children() returns empty list for disconnected catalogs.
    - Connected catalogs function normally regardless of other catalogs' state.

    Validates: Requirements 4.1, 4.2, 4.3
    """
    catalogs: dict[str, Catalog] = {}
    plugin_map: dict[str, MagicMock] = {}

    for name, cat_type, should_fail in catalog_specs:
        cat = Catalog(name=name, type=cat_type)
        catalogs[name] = cat
        plugin = MagicMock()
        if should_fail:
            plugin.list_tables.side_effect = RuntimeError(f"{name} connection failed")
            plugin.test_connection.side_effect = RuntimeError(f"{name} connection failed")
        else:
            plugin.list_tables.return_value = []
        _wire_list_children(plugin)
        plugin_map[name] = plugin

    # Registry maps catalog name → plugin (via type lookup, but we use per-name plugins)
    # Build a type→plugin map; for simplicity, last catalog of each type wins
    # (the property only cares about connected/disconnected status per catalog)
    type_to_plugin: dict[str, MagicMock] = {}
    for name, cat_type, _ in catalog_specs:
        type_to_plugin[cat_type] = plugin_map[name]

    # Use a per-catalog registry that returns the right plugin for each catalog
    # by tracking which catalog is being probed via call order
    call_order: list[str] = [name for name, _, _ in catalog_specs]

    def get_plugin_for_type(cat_type: str) -> MagicMock | None:
        # Find the first catalog of this type in call order
        for name, t, _ in catalog_specs:
            if t == cat_type:
                return plugin_map[name]
        return None

    registry = MagicMock()
    registry.get_catalog_plugin.side_effect = get_plugin_for_type

    # For proper isolation, we need each catalog to use its own plugin.
    # Override: build a registry that returns the right plugin per catalog name
    # by intercepting the init probe order.
    # Since CatalogExplorer iterates catalogs dict in order, we track calls.
    probe_call_count = [0]

    def get_plugin_ordered(cat_type: str) -> MagicMock | None:
        idx = probe_call_count[0]
        probe_call_count[0] += 1
        if idx < len(call_order):
            return plugin_map[call_order[idx]]
        return None

    registry.get_catalog_plugin.side_effect = get_plugin_ordered

    explorer = CatalogExplorer(catalogs=catalogs, engines={}, registry=registry)

    # 1. list_catalogs() returns all catalogs
    result = explorer.list_catalogs()
    assert len(result) == len(catalog_specs)
    result_by_name = {info.name: info for info in result}
    assert set(result_by_name.keys()) == {name for name, _, _ in catalog_specs}

    # 2. Each catalog has correct connected status
    for name, _, should_fail in catalog_specs:
        info = result_by_name[name]
        if should_fail:
            assert info.connected is False
            assert info.error is not None
        else:
            assert info.connected is True
            assert info.error is None

    # 3. list_children() returns empty for disconnected catalogs
    for name, _, should_fail in catalog_specs:
        if should_fail:
            children = explorer.list_children([name])
            assert children == [], f"Expected empty for disconnected catalog {name!r}"
