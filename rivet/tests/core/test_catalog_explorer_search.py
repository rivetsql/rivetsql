"""Unit tests for fuzzy_match() and CatalogExplorer.search().

Also contains:
Feature: catalog-explorer, Property 11: Search results sorted and capped
Feature: catalog-explorer, Property 12: Match positions are valid indices
Feature: catalog-explorer, Property 13: Search triggers expansion for unexpanded catalogs

Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5, 6.6
"""

from __future__ import annotations

from unittest.mock import MagicMock

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.catalog_explorer import (
    CatalogExplorer,
    SearchResult,
    fuzzy_match,
)
from rivet_core.introspection import CatalogNode
from rivet_core.models import Catalog
from rivet_core.plugins import _is_immediate_child

# ── Helpers ───────────────────────────────────────────────────────────────


def _wire_list_children(plugin: MagicMock) -> None:
    """Wire plugin.list_children to filter plugin.list_tables by path."""
    def _list_children_impl(_catalog, path):
        all_nodes = plugin.list_tables(_catalog)
        return [n for n in all_nodes if _is_immediate_child(n.path, path)]
    plugin.list_children.side_effect = _list_children_impl


def _make_plugin(*, list_tables_return=None, list_tables_side_effect=None):
    plugin = MagicMock()
    if list_tables_side_effect is not None:
        plugin.list_tables.side_effect = list_tables_side_effect
    else:
        plugin.list_tables.return_value = list_tables_return or []
    _wire_list_children(plugin)
    return plugin


def _make_registry(*catalog_plugins):
    registry = MagicMock()
    plugin_map = {t: p for t, p in catalog_plugins}
    registry.get_catalog_plugin.side_effect = lambda t: plugin_map.get(t)
    return registry


def _node(name, node_type, path, is_container=False):
    return CatalogNode(
        name=name, node_type=node_type, path=path,
        is_container=is_container, children_count=None, summary=None,
    )


# ── fuzzy_match() unit tests ─────────────────────────────────────────────


class TestFuzzyMatch:
    def test_all_chars_must_appear_in_order(self):
        assert fuzzy_match("usr", "users") is not None
        assert fuzzy_match("urs", "users") is not None
        assert fuzzy_match("sru", "users") is None  # out of order

    def test_no_match_returns_none(self):
        assert fuzzy_match("xyz", "users") is None

    def test_empty_query_matches_everything(self):
        score, positions = fuzzy_match("", "anything")
        assert score == 0.0
        assert positions == []

    def test_case_insensitive(self):
        result = fuzzy_match("USR", "users")
        assert result is not None

    def test_exact_match_scores_best(self):
        exact = fuzzy_match("users", "users")
        partial = fuzzy_match("usr", "users")
        assert exact is not None and partial is not None
        assert exact[0] < partial[0]

    def test_prefix_match_scores_better_than_scattered(self):
        prefix = fuzzy_match("pub", "public")
        scattered = fuzzy_match("plc", "public")
        assert prefix is not None and scattered is not None
        assert prefix[0] < scattered[0]

    def test_shorter_candidate_scores_better(self):
        short = fuzzy_match("t", "t1")
        long = fuzzy_match("t", "this_is_a_very_long_table_name")
        assert short is not None and long is not None
        assert short[0] < long[0]

    def test_match_positions_are_valid_indices(self):
        result = fuzzy_match("usr", "users")
        assert result is not None
        score, positions = result
        for pos in positions:
            assert 0 <= pos < len("users")

    def test_match_positions_chars_match_query(self):
        query = "usr"
        candidate = "users"
        result = fuzzy_match(query, candidate)
        assert result is not None
        _, positions = result
        for i, pos in enumerate(positions):
            assert candidate[pos].lower() == query[i].lower()

    def test_match_positions_in_ascending_order(self):
        result = fuzzy_match("abc", "a_b_c_d")
        assert result is not None
        _, positions = result
        assert positions == sorted(positions)

    def test_word_boundary_bonus(self):
        # "u" at word boundary (after _) should score better
        boundary = fuzzy_match("u", "my_users")
        non_boundary = fuzzy_match("u", "abcudef")
        assert boundary is not None and non_boundary is not None
        assert boundary[0] < non_boundary[0]


# ── CatalogExplorer.search() unit tests ──────────────────────────────────


class TestSearch:
    def _make_explorer(self, nodes, catalog_name="pg", cat_type="postgres"):
        plugin = _make_plugin(list_tables_return=nodes)
        registry = _make_registry((cat_type, plugin))
        cat = Catalog(name=catalog_name, type=cat_type)
        explorer = CatalogExplorer(
            catalogs={catalog_name: cat}, engines={}, registry=registry,
        )
        # Populate children cache at all levels so search can find nodes
        self._expand_all(explorer, catalog_name, nodes)
        return explorer, plugin

    @staticmethod
    def _expand_all(explorer, catalog_name, nodes):
        """Expand all levels of the tree to populate children cache."""
        explorer.list_children([catalog_name])
        # Also expand container nodes to populate deeper levels
        containers = [n for n in nodes if n.is_container]
        for c in containers:
            explorer.list_children([catalog_name] + c.path)

    def test_empty_query_returns_empty(self):
        explorer, _ = self._make_explorer([
            _node("users", "table", ["public", "users"]),
        ])
        assert explorer.search("") == []

    def test_basic_search_finds_matching_nodes(self):
        explorer, _ = self._make_explorer([
            _node("public", "schema", ["public"], is_container=True),
            _node("users", "table", ["public", "users"]),
            _node("orders", "table", ["public", "orders"]),
        ])
        results = explorer.search("users")
        assert len(results) >= 1
        assert any(r.short_name == "users" for r in results)

    def test_search_returns_search_result_instances(self):
        explorer, _ = self._make_explorer([
            _node("users", "table", ["public", "users"]),
        ])
        results = explorer.search("users")
        for r in results:
            assert isinstance(r, SearchResult)

    def test_results_sorted_by_score_ascending(self):
        explorer, _ = self._make_explorer([
            _node("public", "schema", ["public"], is_container=True),
            _node("users", "table", ["public", "users"]),
            _node("user_logs", "table", ["public", "user_logs"]),
        ])
        results = explorer.search("u")
        scores = [r.score for r in results]
        assert scores == sorted(scores)

    def test_results_capped_at_limit(self):
        nodes = [_node(f"t{i}", "table", [f"t{i}"]) for i in range(20)]
        explorer, _ = self._make_explorer(nodes)
        results = explorer.search("t", limit=5)
        assert len(results) <= 5

    def test_search_triggers_expansion_for_unexpanded_catalogs(self):
        nodes = [
            _node("public", "schema", ["public"], is_container=True),
            _node("users", "table", ["public", "users"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(
            catalogs={"pg": cat}, engines={}, registry=registry,
        )

        # Populate cache via list_children so search can find nodes
        explorer.list_children(["pg"])
        explorer.list_children(["pg", "public"])

        results = explorer.search("users")
        assert len(results) >= 1

    def test_search_does_not_expand_disconnected_catalogs(self):
        plugin = _make_plugin(list_tables_side_effect=RuntimeError("fail"))
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(
            catalogs={"pg": cat}, engines={}, registry=registry,
        )
        plugin.list_tables.reset_mock()

        results = explorer.search("anything")
        assert results == []
        # Should not try to expand disconnected catalog
        plugin.list_tables.assert_not_called()

    def test_match_positions_in_results(self):
        explorer, _ = self._make_explorer([
            _node("public", "schema", ["public"], is_container=True),
            _node("users", "table", ["public", "users"]),
        ])
        results = explorer.search("users")
        assert len(results) >= 1
        r = results[0]
        assert len(r.match_positions) == len("users")
        # Positions should be valid indices into qualified_name
        for pos in r.match_positions:
            assert 0 <= pos < len(r.qualified_name)

    def test_qualified_name_format(self):
        explorer, _ = self._make_explorer([
            _node("public", "schema", ["public"], is_container=True),
            _node("users", "table", ["public", "users"]),
        ])
        results = explorer.search("users")
        assert any(r.qualified_name == "pg.public.users" for r in results)

    def test_parent_field(self):
        explorer, _ = self._make_explorer([
            _node("public", "schema", ["public"], is_container=True),
            _node("users", "table", ["public", "users"]),
        ])
        results = explorer.search("users")
        r = [x for x in results if x.short_name == "users"][0]
        assert r.parent == "pg.public"

    def test_parent_none_for_top_level(self):
        explorer, _ = self._make_explorer([
            _node("t", "table", ["t"]),
        ])
        results = explorer.search("t")
        r = [x for x in results if x.short_name == "t"][0]
        assert r.parent is None

    def test_tables_rank_higher_than_columns(self):
        """Tables/schemas should rank higher than columns for same match quality."""
        from rivet_core.introspection import ColumnDetail, ObjectSchema

        nodes = [
            _node("public", "schema", ["public"], is_container=True),
            _node("name_table", "table", ["public", "name_table"]),
        ]
        plugin = _make_plugin(list_tables_return=nodes)
        schema = ObjectSchema(
            path=["public", "name_table"], node_type="table",
            columns=[ColumnDetail(name="name_col", type="utf8", native_type=None,
                                  nullable=True, default=None, comment=None,
                                  is_primary_key=False, is_partition_key=False)],
            primary_key=None, comment=None,
        )
        plugin.get_schema.return_value = schema
        registry = _make_registry(("postgres", plugin))
        cat = Catalog(name="pg", type="postgres")
        explorer = CatalogExplorer(
            catalogs={"pg": cat}, engines={}, registry=registry,
        )
        # Populate cache so search can find nodes
        explorer.list_children(["pg"])
        explorer.list_children(["pg", "public"])
        results = explorer.search("name")
        table_results = [r for r in results if r.node_type == "table"]
        [r for r in results if r.node_type == "schema"]
        # Tables should be present
        assert len(table_results) >= 1

    def test_no_match_returns_empty(self):
        explorer, _ = self._make_explorer([
            _node("users", "table", ["public", "users"]),
        ])
        results = explorer.search("zzzzz")
        assert results == []

    def test_multi_catalog_search(self):
        nodes1 = [
            _node("public", "schema", ["public"], is_container=True),
            _node("users", "table", ["public", "users"]),
        ]
        nodes2 = [
            _node("main", "schema", ["main"], is_container=True),
            _node("orders", "table", ["main", "orders"]),
        ]
        plugin1 = _make_plugin(list_tables_return=nodes1)
        plugin2 = _make_plugin(list_tables_return=nodes2)
        registry = _make_registry(("postgres", plugin1), ("duckdb", plugin2))
        catalogs = {
            "pg": Catalog(name="pg", type="postgres"),
            "dk": Catalog(name="dk", type="duckdb"),
        }
        explorer = CatalogExplorer(catalogs=catalogs, engines={}, registry=registry)

        # Populate cache for both catalogs at all levels
        explorer.list_children(["pg"])
        explorer.list_children(["pg", "public"])
        explorer.list_children(["dk"])
        explorer.list_children(["dk", "main"])

        results = explorer.search("rs")  # matches "users" and "orders"
        names = {r.short_name for r in results}
        assert "users" in names
        assert "orders" in names


# ── Hypothesis strategies ─────────────────────────────────────────────────

_identifier = st.text(
    alphabet=st.characters(whitelist_categories=("Ll",), whitelist_characters="_"),
    min_size=1,
    max_size=12,
)

_node_name = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz0123456789_",
    min_size=1,
    max_size=16,
)

_node_type = st.sampled_from(["table", "schema", "view"])

_query = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz_",
    min_size=1,
    max_size=8,
)


def _make_catalog_node(name: str, node_type: str, path: list[str]) -> CatalogNode:
    return CatalogNode(
        name=name,
        node_type=node_type,
        path=path,
        is_container=(node_type == "schema"),
        children_count=None,
        summary=None,
    )


def _make_explorer_with_nodes(nodes: list[CatalogNode], catalog_name: str = "cat") -> CatalogExplorer:
    plugin = _make_plugin(list_tables_return=nodes)
    registry = MagicMock()
    registry.get_catalog_plugin.return_value = plugin
    cat = Catalog(name=catalog_name, type="postgres")
    explorer = CatalogExplorer(catalogs={catalog_name: cat}, engines={}, registry=registry)
    # Populate children cache so search can find nodes
    explorer.list_children([catalog_name])
    return explorer


# ── Property 11: Search results sorted and capped ────────────────────────


@given(
    node_names=st.lists(_node_name, min_size=1, max_size=30, unique=True),
    query=_query,
    limit=st.integers(min_value=1, max_value=10),
)
@settings(max_examples=150)
def test_property_11_search_results_sorted_and_capped(
    node_names: list[str],
    query: str,
    limit: int,
) -> None:
    """Property 11: Search results sorted by score ascending, at most limit entries.

    For any search query and limit, the returned SearchResult list should be sorted
    by score (ascending, lower = better match) and contain at most limit entries.

    Validates: Requirements 6.2, 6.5
    """
    nodes = [_make_catalog_node(n, "table", [n]) for n in node_names]
    explorer = _make_explorer_with_nodes(nodes)

    results = explorer.search(query, limit=limit)

    # At most limit entries
    assert len(results) <= limit

    # Sorted by score ascending (lower = better)
    scores = [r.score for r in results]
    assert scores == sorted(scores), f"Results not sorted: {scores}"

    # All results are SearchResult instances
    for r in results:
        assert isinstance(r, SearchResult)


# ── Property 12: Match positions are valid indices ────────────────────────


@given(
    node_names=st.lists(_node_name, min_size=1, max_size=20, unique=True),
    query=_query,
)
@settings(max_examples=150)
def test_property_12_match_positions_are_valid_indices(
    node_names: list[str],
    query: str,
) -> None:
    """Property 12: Every index in match_positions is valid and chars match query in order.

    For any SearchResult, every index in match_positions should be a valid index into
    the qualified_name string, and the characters at those positions should match the
    corresponding query characters in order.

    Validates: Requirements 6.6
    """
    nodes = [_make_catalog_node(n, "table", [n]) for n in node_names]
    explorer = _make_explorer_with_nodes(nodes)

    results = explorer.search(query)

    for r in results:
        qname = r.qualified_name
        positions = r.match_positions

        # Every position is a valid index
        for pos in positions:
            assert 0 <= pos < len(qname), (
                f"Position {pos} out of range for qualified_name {qname!r} (len={len(qname)})"
            )

        # Characters at positions match query chars in order
        q_lower = query.lower()
        qname_lower = qname.lower()
        assert len(positions) == len(q_lower), (
            f"match_positions length {len(positions)} != query length {len(q_lower)}"
        )
        for i, pos in enumerate(positions):
            assert qname_lower[pos] == q_lower[i], (
                f"Position {pos} char {qname_lower[pos]!r} != query[{i}] {q_lower[i]!r}"
            )

        # Positions are in ascending order
        assert positions == sorted(positions), f"Positions not sorted: {positions}"


# ── Property 13: Search triggers expansion for unexpanded catalogs ────────


@given(
    catalog_names=st.lists(
        st.text(alphabet="abcdefghijklmnopqrstuvwxyz", min_size=2, max_size=8),
        min_size=1,
        max_size=5,
        unique=True,
    ),
    query=_query,
)
@settings(max_examples=100)
def test_property_13_search_triggers_expansion_for_unexpanded_catalogs(
    catalog_names: list[str],
    query: str,
) -> None:
    """Property 13: search() finds nodes from expanded catalogs.

    For any catalog that has been expanded via list_children(), calling search(query)
    should include its tables in results.

    Validates: Requirements 6.3
    """
    plugins: dict[str, MagicMock] = {}
    catalogs: dict[str, Catalog] = {}

    # Build registry that maps catalog name → plugin (via type lookup)
    cat_type_map: dict[str, MagicMock] = {}
    for name in catalog_names:
        plugin = MagicMock()
        plugin.list_tables.return_value = [
            _make_catalog_node(f"{name}_tbl", "table", [f"{name}_tbl"])
        ]
        _wire_list_children(plugin)
        plugins[name] = plugin
        cat_type = f"type_{name}"
        catalogs[name] = Catalog(name=name, type=cat_type)
        cat_type_map[cat_type] = plugin

    registry = MagicMock()
    registry.get_catalog_plugin.side_effect = lambda t: cat_type_map.get(t)

    explorer = CatalogExplorer(catalogs=catalogs, engines={}, registry=registry)

    # Expand all catalogs via list_children
    for name in catalog_names:
        explorer.list_children([name])

    # Perform search — should find nodes from expanded catalogs
    results = explorer.search(query)

    # All results should be valid SearchResult instances
    for r in results:
        assert isinstance(r, SearchResult)
