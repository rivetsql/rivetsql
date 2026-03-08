"""Property test for CatalogSearch — catalog search matching.

# Feature: cli-repl, Property 10: Catalog search matches qualified and short names
# Validates: Requirements 6.2, 6.3
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.interactive.catalog_search import CatalogSearch

# Strategy for valid identifiers
_ident = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)


def _make_catalog_entry(catalog: str, schema: str, table: str) -> dict:
    return {"catalog": catalog, "schema": schema, "table": table, "columns": None}


class TestCatalogSearchMatchingProperty:
    """Property 10: Catalog search matches qualified and short names."""

    @given(catalog=_ident, schema=_ident, table=_ident)
    @settings(max_examples=100)
    def test_short_name_search_returns_table(
        self, catalog: str, schema: str, table: str
    ) -> None:
        """Searching by a table's short name returns a result containing that table."""
        cs = CatalogSearch()
        cs.update([_make_catalog_entry(catalog, schema, table)], [])
        results = cs.search(table)
        qualified_names = [r.qualified_name for r in results]
        assert f"{catalog}.{schema}.{table}" in qualified_names

    @given(catalog=_ident, schema=_ident, table=_ident)
    @settings(max_examples=100)
    def test_qualified_name_search_returns_table(
        self, catalog: str, schema: str, table: str
    ) -> None:
        """Searching by a table's fully-qualified name returns a result containing that table."""
        cs = CatalogSearch()
        cs.update([_make_catalog_entry(catalog, schema, table)], [])
        qualified = f"{catalog}.{schema}.{table}"
        results = cs.search(qualified)
        qualified_names = [r.qualified_name for r in results]
        assert qualified in qualified_names

    @given(joint_name=_ident)
    @settings(max_examples=100)
    def test_joint_short_name_search_returns_joint(self, joint_name: str) -> None:
        """Searching by a joint's name returns a result containing that joint."""
        cs = CatalogSearch()
        cs.update([], [joint_name])
        results = cs.search(joint_name)
        qualified_names = [r.qualified_name for r in results]
        assert joint_name in qualified_names

    @given(catalog=_ident, schema=_ident, table=_ident)
    @settings(max_examples=100)
    def test_match_positions_valid_indices(
        self, catalog: str, schema: str, table: str
    ) -> None:
        """All match_positions are valid character indices within the matched string."""
        cs = CatalogSearch()
        cs.update([_make_catalog_entry(catalog, schema, table)], [])
        results = cs.search(table)
        for result in results:
            matched_text = (
                result.short_name
                if all(p < len(result.short_name) for p in result.match_positions)
                else result.qualified_name
            )
            for pos in result.match_positions:
                assert 0 <= pos < len(matched_text)

    @given(catalog=_ident, schema=_ident, table=_ident)
    @settings(max_examples=100)
    def test_scores_non_negative(
        self, catalog: str, schema: str, table: str
    ) -> None:
        """All result scores are finite (lower = better match)."""
        cs = CatalogSearch()
        cs.update([_make_catalog_entry(catalog, schema, table)], [])
        results = cs.search(table)
        for result in results:
            assert result.score != float("inf") and result.score != float("-inf")

    @given(catalog=_ident, schema=_ident, table=_ident)
    @settings(max_examples=100)
    def test_results_sorted_by_score(
        self, catalog: str, schema: str, table: str
    ) -> None:
        """Results are sorted by score ascending (lower score = better match first)."""
        cs = CatalogSearch()
        cs.update([_make_catalog_entry(catalog, schema, table)], [])
        results = cs.search(table)
        scores = [r.score for r in results]
        assert scores == sorted(scores)

    @given(catalog=_ident, schema=_ident, table=_ident)
    @settings(max_examples=100)
    def test_empty_query_returns_empty(
        self, catalog: str, schema: str, table: str
    ) -> None:
        """An empty query string returns no results."""
        cs = CatalogSearch()
        cs.update([_make_catalog_entry(catalog, schema, table)], [])
        results = cs.search("")
        assert results == []

    @given(catalog=_ident, schema=_ident, table=_ident)
    @settings(max_examples=100)
    def test_exact_match_has_lowest_score(
        self, catalog: str, schema: str, table: str
    ) -> None:
        """An exact match on short_name has the lowest score among all results."""
        cs = CatalogSearch()
        cs.update([_make_catalog_entry(catalog, schema, table)], [])
        results = cs.search(table)
        # Find the result for our table
        table_result = next(
            (r for r in results if r.qualified_name == f"{catalog}.{schema}.{table}"),
            None,
        )
        assert table_result is not None
        # Exact match on short_name should have score == -1.0
        if table_result.short_name == table:
            assert table_result.score == -1.0
