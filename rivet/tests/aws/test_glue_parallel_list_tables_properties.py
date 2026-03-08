"""Property-based tests for parallel Glue list_tables with TTL cache.

Property 12: Glue TTL Cache Hit
Calling list_tables() twice within TTL returns identical results and no second API call.
Validates: Requirements 8.2

Property 13: Glue Per-Database Fault Isolation
Partial database failures return tables from successful databases and log warnings.
Validates: Requirements 8.3
"""

from __future__ import annotations

from unittest.mock import patch

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_aws.glue_catalog import GlueCatalogPlugin
from rivet_core.introspection import CatalogNode, NodeSummary
from rivet_core.models import Catalog

# ── Strategies ────────────────────────────────────────────────────────────────

_ident_st = st.from_regex(r"[a-z][a-z0-9_]{0,19}", fullmatch=True)
_table_list_st = st.lists(_ident_st, min_size=1, max_size=5, unique=True)
_db_list_st = st.lists(_ident_st, min_size=1, max_size=4, unique=True)


def _make_catalog(database: str | None = None) -> Catalog:
    opts: dict = {"access_key_id": "AKID", "secret_access_key": "SECRET"}
    if database:
        opts["database"] = database
    return Catalog(name="glue_cat", type="glue", options=opts)


def _mock_glue_client_with_tables(table_names: list[str], database: str, catalog_name: str) -> list[CatalogNode]:
    """Build CatalogNode list as _list_tables_in_database would return."""
    return [
        CatalogNode(
            name=t,
            node_type="table",
            path=[catalog_name, database, t],
            is_container=False,
            children_count=None,
            summary=NodeSummary(
                row_count=None,
                size_bytes=None,
                format=None,
                last_modified=None,
                owner=None,
                comment=None,
            ),
        )
        for t in table_names
    ]


# ── Property 12: Glue TTL Cache Hit ─────────────────────────────────────────


@given(database=_ident_st, table_names=_table_list_st)
@settings(max_examples=100, deadline=None)
def test_ttl_cache_hit_returns_identical_results_no_second_api_call(
    database: str, table_names: list[str]
) -> None:
    """Calling list_tables() twice within TTL returns identical results without a second API call."""
    catalog = _make_catalog(database=database)
    plugin = GlueCatalogPlugin(cache_ttl=300.0)

    expected_nodes = _mock_glue_client_with_tables(table_names, database, catalog.name)

    call_count = 0
    _original_list = plugin._list_tables_in_database

    def _counting_list(cat, db):
        nonlocal call_count
        call_count += 1
        return expected_nodes

    with patch.object(plugin, "_list_tables_in_database", side_effect=_counting_list):
        first_result = plugin.list_tables(catalog)
        second_result = plugin.list_tables(catalog)

    assert call_count == 1, f"Expected 1 API call, got {call_count}"
    assert first_result == second_result
    assert len(first_result) == len(table_names)


# ── Property 12b: Cache expires after TTL ────────────────────────────────────


@given(database=_ident_st, table_names=_table_list_st)
@settings(max_examples=100, deadline=None)
def test_ttl_cache_expires_after_ttl(
    database: str, table_names: list[str]
) -> None:
    """After TTL expires, list_tables() makes a fresh API call."""
    catalog = _make_catalog(database=database)
    # Use a very short TTL so we can test expiry
    plugin = GlueCatalogPlugin(cache_ttl=0.0)

    expected_nodes = _mock_glue_client_with_tables(table_names, database, catalog.name)

    call_count = 0

    def _counting_list(cat, db):
        nonlocal call_count
        call_count += 1
        return expected_nodes

    with patch.object(plugin, "_list_tables_in_database", side_effect=_counting_list):
        plugin.list_tables(catalog)
        plugin.list_tables(catalog)

    assert call_count == 2, f"Expected 2 API calls after TTL expiry, got {call_count}"


# ── Property 13: Glue Per-Database Fault Isolation ───────────────────────────


@given(
    databases=_db_list_st,
    fail_indices=st.data(),
)
@settings(max_examples=100, deadline=None)
def test_per_database_fault_isolation(
    databases: list[str], fail_indices: st.DataObject
) -> None:
    """Tables from successful databases are returned; failed databases produce warnings."""
    # Pick a random subset of databases to fail
    fail_mask = fail_indices.draw(
        st.lists(st.booleans(), min_size=len(databases), max_size=len(databases))
    )

    catalog = _make_catalog(database=None)
    plugin = GlueCatalogPlugin()

    # Build expected tables per database
    tables_per_db: dict[str, list[CatalogNode]] = {}
    for db in databases:
        tables_per_db[db] = _mock_glue_client_with_tables(
            [f"{db}_table1"], db, catalog.name
        )

    def _mock_list_tables_in_db(cat, db):
        idx = databases.index(db)
        if fail_mask[idx]:
            raise RuntimeError(f"Simulated failure for {db}")
        return tables_per_db[db]

    def _mock_list_databases(cat):
        return databases

    with patch.object(plugin, "_list_databases", side_effect=_mock_list_databases), \
         patch.object(plugin, "_list_tables_in_database", side_effect=_mock_list_tables_in_db):
        result = plugin.list_tables(catalog)

    # Verify: tables from successful databases are present
    successful_dbs = [db for db, failed in zip(databases, fail_mask) if not failed]
    expected_tables = []
    for db in successful_dbs:
        expected_tables.extend(tables_per_db[db])

    result_names = sorted(n.name for n in result)
    expected_names = sorted(n.name for n in expected_tables)
    assert result_names == expected_names, (
        f"Expected tables from {successful_dbs}, got {result_names}"
    )


# ── Property 13b: All databases failing returns empty ────────────────────────


@given(databases=_db_list_st)
@settings(max_examples=100, deadline=None)
def test_all_databases_failing_returns_empty(databases: list[str]) -> None:
    """When all databases fail, list_tables returns empty list (no exception)."""
    catalog = _make_catalog(database=None)
    plugin = GlueCatalogPlugin()

    def _mock_list_tables_in_db(cat, db):
        raise RuntimeError(f"Simulated failure for {db}")

    def _mock_list_databases(cat):
        return databases

    with patch.object(plugin, "_list_databases", side_effect=_mock_list_databases), \
         patch.object(plugin, "_list_tables_in_database", side_effect=_mock_list_tables_in_db):
        result = plugin.list_tables(catalog)

    assert result == []
