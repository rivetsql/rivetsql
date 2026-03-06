"""Property test for Glue list_children hierarchy.

Feature: cross-storage-adapters, Property 10: Glue list_children hierarchy
Generate mock Glue responses at each hierarchy level; verify correct node types.
Validates: Requirements 6.9, 10.4
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_aws.glue_catalog import GlueCatalogPlugin
from rivet_core.models import Catalog

# Strategies for generating valid identifiers
_ident_st = st.from_regex(r"[a-z][a-z0-9_]{0,29}", fullmatch=True)
_catalog_name_st = st.from_regex(r"[a-z][a-z0-9_]{0,19}", fullmatch=True)
_table_list_st = st.lists(_ident_st, min_size=0, max_size=5, unique=True)
_col_list_st = st.lists(_ident_st, min_size=1, max_size=6, unique=True)


def _make_catalog(catalog_name: str, database: str) -> Catalog:
    return Catalog(
        name=catalog_name,
        type="glue",
        options={"database": database, "access_key_id": "AKID", "secret_access_key": "SECRET"},
    )


def _mock_glue_client_with_tables(table_names: list[str]) -> MagicMock:
    client = MagicMock()
    table_list = [
        {
            "Name": name,
            "StorageDescriptor": {"Location": f"s3://bucket/{name}/", "InputFormat": ""},
            "Parameters": {},
        }
        for name in table_names
    ]
    paginator = MagicMock()
    paginator.paginate.return_value = iter([{"TableList": table_list}])
    client.get_paginator.return_value = paginator
    return client


def _mock_glue_client_with_columns(col_names: list[str]) -> MagicMock:
    client = MagicMock()
    client.get_table.return_value = {
        "Table": {
            "Name": "some_table",
            "StorageDescriptor": {
                "Columns": [{"Name": c, "Type": "string"} for c in col_names],
            },
            "PartitionKeys": [],
        }
    }
    return client


@settings(max_examples=100, deadline=None)
@given(catalog_name=_catalog_name_st, database=_ident_st)
def test_property_list_children_root_returns_database_container(
    catalog_name: str, database: str
) -> None:
    """Property 10: list_children at root (depth=0) returns the database as a container node."""
    catalog = _make_catalog(catalog_name, database)
    plugin = GlueCatalogPlugin()

    nodes = plugin.list_children(catalog, [])

    assert len(nodes) == 1
    node = nodes[0]
    assert node.name == database
    assert node.node_type == "database"
    assert node.is_container is True
    assert node.path == [catalog_name, database]


@settings(max_examples=100, deadline=None)
@given(catalog_name=_catalog_name_st, database=_ident_st)
def test_property_list_children_catalog_level_returns_database_container(
    catalog_name: str, database: str
) -> None:
    """Property 10: list_children at catalog level (depth=1) returns the database as a container node."""
    catalog = _make_catalog(catalog_name, database)
    plugin = GlueCatalogPlugin()

    nodes = plugin.list_children(catalog, [catalog_name])

    assert len(nodes) == 1
    node = nodes[0]
    assert node.name == database
    assert node.node_type == "database"
    assert node.is_container is True


@settings(max_examples=100, deadline=None)
@given(catalog_name=_catalog_name_st, database=_ident_st, table_names=_table_list_st)
def test_property_list_children_database_level_returns_table_nodes(
    catalog_name: str, database: str, table_names: list[str]
) -> None:
    """Property 10: list_children at database level (depth=2) returns table nodes (not containers)."""
    catalog = _make_catalog(catalog_name, database)
    plugin = GlueCatalogPlugin()
    client = _mock_glue_client_with_tables(table_names)

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        nodes = plugin.list_children(catalog, [catalog_name, database])

    assert len(nodes) == len(table_names)
    for node in nodes:
        assert node.node_type == "table"
        assert node.is_container is False
        assert node.path[0] == catalog_name
        assert node.path[1] == database
        assert node.name in table_names


@settings(max_examples=100, deadline=None)
@given(catalog_name=_catalog_name_st, database=_ident_st, table=_ident_st, col_names=_col_list_st)
def test_property_list_children_table_level_returns_column_nodes(
    catalog_name: str, database: str, table: str, col_names: list[str]
) -> None:
    """Property 10: list_children at table level (depth=3) returns column nodes (not containers)."""
    catalog = _make_catalog(catalog_name, database)
    plugin = GlueCatalogPlugin()
    client = _mock_glue_client_with_columns(col_names)

    with patch("rivet_aws.glue_catalog._make_glue_client_for_table", return_value=client):
        nodes = plugin.list_children(catalog, [catalog_name, database, table])

    assert len(nodes) == len(col_names)
    for node in nodes:
        assert node.node_type == "column"
        assert node.is_container is False
        assert node.path == [catalog_name, database, table, node.name]
        assert node.name in col_names


@settings(max_examples=100, deadline=None)
@given(catalog_name=_catalog_name_st, database=_ident_st, table=_ident_st, col=_ident_st)
def test_property_list_children_beyond_column_level_returns_empty(
    catalog_name: str, database: str, table: str, col: str
) -> None:
    """Property 10: list_children beyond column level (depth>=4) returns empty list."""
    catalog = _make_catalog(catalog_name, database)
    plugin = GlueCatalogPlugin()

    nodes = plugin.list_children(catalog, [catalog_name, database, table, col])

    assert nodes == []
