"""Tests for rivet_core.interactive.catalog_search."""

import pytest

from rivet_core.interactive.catalog_search import CatalogSearch
from rivet_core.interactive.types import CatalogSearchResult


@pytest.fixture
def search():
    s = CatalogSearch()
    s.update(
        catalog_entries=[
            {
                "catalog": "prod",
                "schema": "public",
                "table": "users",
                "columns": ["id", "email", "created_at"],
            },
            {
                "catalog": "prod",
                "schema": "public",
                "table": "orders",
                "columns": ["order_id", "user_id", "amount"],
            },
            {
                "catalog": "staging",
                "schema": "raw",
                "table": "raw_users",
                "columns": ["id", "name"],
            },
        ],
        joint_names=["transform_users", "load_orders"],
    )
    return s


# --- basic matching ---

def test_search_returns_list(search):
    results = search.search("users")
    assert isinstance(results, list)
    assert all(isinstance(r, CatalogSearchResult) for r in results)


def test_search_empty_query_returns_empty(search):
    assert search.search("") == []


def test_search_matches_short_name(search):
    results = search.search("users")
    names = [r.qualified_name for r in results]
    assert "prod.public.users" in names


def test_search_matches_qualified_name(search):
    results = search.search("prod.public.users")
    names = [r.qualified_name for r in results]
    assert "prod.public.users" in names


def test_search_matches_joint_name(search):
    results = search.search("transform")
    names = [r.qualified_name for r in results]
    assert "transform_users" in names


def test_search_matches_column(search):
    results = search.search("email")
    names = [r.qualified_name for r in results]
    assert "prod.public.users.email" in names


def test_search_matches_catalog(search):
    results = search.search("prod")
    names = [r.qualified_name for r in results]
    assert "prod" in names


def test_search_matches_schema(search):
    results = search.search("public")
    names = [r.qualified_name for r in results]
    assert "prod.public" in names


# --- fuzzy matching ---

def test_fuzzy_match_subsequence(search):
    # "usr" should match "users" via subsequence
    results = search.search("usr")
    names = [r.qualified_name for r in results]
    assert "prod.public.users" in names


def test_fuzzy_match_partial(search):
    # "ord" matches "orders"
    results = search.search("ord")
    names = [r.qualified_name for r in results]
    assert "prod.public.orders" in names


def test_no_match_returns_empty(search):
    results = search.search("zzzznotfound")
    assert results == []


# --- match positions ---

def test_match_positions_are_valid_indices(search):
    results = search.search("users")
    user_results = [r for r in results if r.short_name == "users"]
    assert user_results, "Expected at least one result with short_name='users'"
    r = user_results[0]
    assert len(r.match_positions) > 0
    for pos in r.match_positions:
        assert 0 <= pos < len(r.short_name) or 0 <= pos < len(r.qualified_name)


def test_match_positions_cover_query_chars(search):
    query = "usr"
    results = search.search(query)
    user_results = [r for r in results if "users" in r.qualified_name and r.kind == "table"]
    assert user_results
    r = user_results[0]
    assert len(r.match_positions) == len(query)


# --- score and ranking ---

def test_results_sorted_by_score(search):
    results = search.search("users")
    scores = [r.score for r in results]
    assert scores == sorted(scores)


def test_exact_short_name_match_ranks_first(search):
    results = search.search("users")
    assert results[0].short_name == "users"


def test_score_stored_in_result(search):
    results = search.search("users")
    for r in results:
        assert isinstance(r.score, float)


# --- result fields ---

def test_result_kind_table(search):
    results = search.search("orders")
    table_results = [r for r in results if r.qualified_name == "prod.public.orders"]
    assert table_results
    assert table_results[0].kind == "table"


def test_result_kind_joint(search):
    results = search.search("load_orders")
    joint_results = [r for r in results if r.qualified_name == "load_orders"]
    assert joint_results
    assert joint_results[0].kind == "joint"


def test_result_parent_for_table(search):
    results = search.search("users")
    table_results = [r for r in results if r.qualified_name == "prod.public.users"]
    assert table_results
    assert table_results[0].parent == "prod.public"


def test_result_parent_for_column(search):
    results = search.search("email")
    col_results = [r for r in results if r.qualified_name == "prod.public.users.email"]
    assert col_results
    assert col_results[0].parent == "prod.public.users"


def test_result_parent_none_for_catalog(search):
    results = search.search("prod")
    cat_results = [r for r in results if r.qualified_name == "prod" and r.kind == "catalog"]
    assert cat_results
    assert cat_results[0].parent is None


# --- update rebuilds index ---

def test_update_clears_old_index():
    s = CatalogSearch()
    s.update(
        catalog_entries=[{"catalog": "old", "schema": "s", "table": "t", "columns": []}],
        joint_names=["old_joint"],
    )
    s.update(
        catalog_entries=[{"catalog": "new", "schema": "s", "table": "t", "columns": []}],
        joint_names=["new_joint"],
    )
    old_results = s.search("old")
    new_results = s.search("new")
    old_names = [r.qualified_name for r in old_results]
    new_names = [r.qualified_name for r in new_results]
    assert "old" not in old_names or all(r.kind != "catalog" for r in old_results if r.qualified_name == "old")
    assert "new" in new_names


def test_update_with_empty_inputs():
    s = CatalogSearch()
    s.update(catalog_entries=[], joint_names=[])
    assert s.search("anything") == []


# --- limit ---

def test_limit_respected(search):
    results = search.search("a", limit=2)
    assert len(results) <= 2


# --- case insensitivity ---

def test_case_insensitive_match(search):
    results_lower = search.search("users")
    results_upper = search.search("USERS")
    lower_names = {r.qualified_name for r in results_lower}
    upper_names = {r.qualified_name for r in results_upper}
    assert lower_names == upper_names
