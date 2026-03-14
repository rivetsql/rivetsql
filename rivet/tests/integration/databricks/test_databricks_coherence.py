"""Integration tests for Databricks catalog introspection methods.

Tests test_connection and list_children on DatabricksCatalogPlugin using
mocked Unity Catalog API responses (no real Databricks required).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from rivet_core.errors import ExecutionError
from rivet_core.models import Catalog


def _make_catalog() -> Catalog:
    return Catalog(
        name="test_db",
        type="databricks",
        options={
            "workspace_url": "https://test.databricks.com",
            "catalog": "main",
            "schema": "default",
            "token": "dapi_test_token",
        },
    )


def _mock_client(
    catalogs: list[dict] | None = None,
    schemas: list[dict] | None = None,
    tables: list[dict] | None = None,
    table_detail: dict | None = None,
) -> MagicMock:
    """Build a mock UnityCatalogClient."""
    client = MagicMock()
    client.list_catalogs.return_value = catalogs or []
    client.list_schemas.return_value = schemas or []
    client.list_tables.return_value = tables or []
    client.get_table.return_value = table_detail or {}
    client.close.return_value = None
    return client


# ── test_connection ──────────────────────────────────────────────────


def test_databricks_test_connection_succeeds():
    """test_connection completes without error when API responds."""
    from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin

    plugin = DatabricksCatalogPlugin()
    catalog = _make_catalog()

    client = _mock_client(catalogs=[{"name": "main"}])
    with patch("rivet_databricks.client.UnityCatalogClient", return_value=client):
        plugin.test_connection(catalog)  # should not raise

    client.list_catalogs.assert_called_once()
    client.close.assert_called_once()


def test_databricks_test_connection_raises_on_failure():
    """test_connection raises ExecutionError when API call fails."""
    from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin

    plugin = DatabricksCatalogPlugin()
    catalog = _make_catalog()

    client = _mock_client()
    client.list_catalogs.side_effect = ConnectionError("network unreachable")
    with patch("rivet_databricks.client.UnityCatalogClient", return_value=client):
        with pytest.raises(ExecutionError) as exc_info:
            plugin.test_connection(catalog)

    assert exc_info.value.error.code == "RVT-501"
    assert "rivet_databricks" in exc_info.value.error.context.get("plugin_name", "")


def test_databricks_test_connection_propagates_execution_error():
    """test_connection re-raises ExecutionError from client (e.g. 401)."""
    from rivet_core.errors import plugin_error
    from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin

    plugin = DatabricksCatalogPlugin()
    catalog = _make_catalog()

    auth_error = ExecutionError(
        plugin_error(
            "RVT-502",
            "Authentication failed",
            plugin_name="rivet_databricks",
            plugin_type="client",
            remediation="Check your token or credentials.",
        )
    )
    client = _mock_client()
    client.list_catalogs.side_effect = auth_error
    with patch("rivet_databricks.client.UnityCatalogClient", return_value=client):
        with pytest.raises(ExecutionError) as exc_info:
            plugin.test_connection(catalog)

    assert exc_info.value.error.code == "RVT-502"


# ── list_children ────────────────────────────────────────────────────


def test_databricks_list_children_root_returns_schemas():
    """list_children([]) returns schema nodes."""
    from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin

    plugin = DatabricksCatalogPlugin()
    catalog = _make_catalog()

    client = _mock_client(
        schemas=[
            {"name": "default", "owner": "admin", "comment": "Default schema"},
            {"name": "analytics", "owner": "data_team", "comment": None},
        ]
    )
    with patch("rivet_databricks.client.UnityCatalogClient", return_value=client):
        nodes = plugin.list_children(catalog, path=[])

    assert len(nodes) == 2
    assert nodes[0].name == "default"
    assert nodes[0].node_type == "schema"
    assert nodes[0].is_container is True
    assert nodes[0].path == ["default"]
    assert nodes[1].name == "analytics"
    client.list_schemas.assert_called_once_with("main")


def test_databricks_list_children_schema_returns_tables():
    """list_children([schema]) returns table nodes."""
    from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin

    plugin = DatabricksCatalogPlugin()
    catalog = _make_catalog()

    client = _mock_client(
        tables=[
            {
                "name": "users",
                "table_type": "MANAGED",
                "data_source_format": "DELTA",
                "owner": "admin",
                "comment": None,
                "properties": {},
                "updated_at": None,
            },
            {
                "name": "orders",
                "table_type": "EXTERNAL",
                "data_source_format": "PARQUET",
                "owner": "admin",
                "comment": "Order data",
                "properties": {},
                "updated_at": None,
            },
        ]
    )
    with patch("rivet_databricks.client.UnityCatalogClient", return_value=client):
        nodes = plugin.list_children(catalog, path=["default"])

    assert len(nodes) == 2
    assert nodes[0].name == "users"
    assert nodes[0].node_type == "managed"
    assert nodes[0].is_container is False
    assert nodes[0].path == ["default", "users"]
    assert nodes[1].name == "orders"
    client.list_tables.assert_called_once_with("main", "default")


def test_databricks_list_children_table_returns_columns():
    """list_children([schema, table]) returns column nodes via get_schema."""
    from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin

    plugin = DatabricksCatalogPlugin()
    catalog = _make_catalog()

    table_detail = {
        "columns": [
            {"name": "id", "type_text": "bigint", "nullable": False},
            {"name": "email", "type_text": "string", "nullable": True},
        ],
        "table_type": "MANAGED",
        "comment": None,
    }
    client = _mock_client(table_detail=table_detail)
    with patch("rivet_databricks.client.UnityCatalogClient", return_value=client):
        nodes = plugin.list_children(catalog, path=["default", "users"])

    assert len(nodes) == 2
    assert nodes[0].name == "id"
    assert nodes[0].node_type == "column"
    assert nodes[0].path == ["default", "users", "id"]
    assert nodes[1].name == "email"
    client.get_table.assert_called_once_with("main.default.users")


def test_databricks_list_children_deep_path_returns_empty():
    """list_children with depth > 2 returns an empty list."""
    from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin

    plugin = DatabricksCatalogPlugin()
    catalog = _make_catalog()

    nodes = plugin.list_children(catalog, path=["default", "users", "id"])
    assert nodes == []
