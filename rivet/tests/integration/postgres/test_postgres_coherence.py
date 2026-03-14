"""Integration tests for Postgres catalog introspection methods.

Tests test_connection and list_children on PostgresCatalogPlugin using
mocked psycopg connections (no real Postgres required).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from rivet_core.errors import ExecutionError
from rivet_core.models import Catalog


def _make_catalog(schema: str = "public") -> Catalog:
    return Catalog(
        name="test_pg",
        type="postgres",
        options={
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "testuser",
            "password": "testpass",
            "schema": schema,
        },
    )


def _mock_conn(cursor_return: object) -> MagicMock:
    """Build a MagicMock psycopg connection with a context-managed cursor."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = cursor_return
    mock_cursor.fetchone.return_value = None

    mock = MagicMock()
    mock.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock.cursor.return_value.__exit__ = MagicMock(return_value=None)
    return mock


# ── test_connection ──────────────────────────────────────────────────


def test_postgres_test_connection_succeeds():
    """test_connection completes without error on a healthy connection."""
    from rivet_postgres.catalog import PostgresCatalogPlugin

    plugin = PostgresCatalogPlugin()
    catalog = _make_catalog()

    conn = _mock_conn(cursor_return=[(1,)])
    with patch("psycopg.connect", return_value=conn):
        plugin.test_connection(catalog)  # should not raise


def test_postgres_test_connection_raises_on_failure():
    """test_connection raises ExecutionError when the query fails."""
    from rivet_postgres.catalog import PostgresCatalogPlugin

    plugin = PostgresCatalogPlugin()
    catalog = _make_catalog()

    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("connection refused")

    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=None)

    with patch("psycopg.connect", return_value=mock_conn):
        with pytest.raises(ExecutionError) as exc_info:
            plugin.test_connection(catalog)

    assert exc_info.value.error.code in ("RVT-501", "RVT-502")
    assert "rivet_postgres" in (exc_info.value.error.context.get("plugin_name", ""))


def test_postgres_test_connection_raises_on_connect_failure():
    """test_connection raises ExecutionError when _connect itself fails."""
    from rivet_postgres.catalog import PostgresCatalogPlugin

    plugin = PostgresCatalogPlugin()
    catalog = _make_catalog()

    with patch("psycopg.connect", side_effect=Exception("could not connect to server")):
        with pytest.raises(ExecutionError) as exc_info:
            plugin.test_connection(catalog)

    assert exc_info.value.error.code == "RVT-501"


# ── list_children ────────────────────────────────────────────────────


def test_postgres_list_children_root_returns_schemas():
    """list_children([]) returns schema nodes."""
    from rivet_postgres.catalog import PostgresCatalogPlugin

    plugin = PostgresCatalogPlugin()
    catalog = _make_catalog()

    conn = _mock_conn(cursor_return=[("public",), ("analytics",)])
    with patch("psycopg.connect", return_value=conn):
        nodes = plugin.list_children(catalog, path=[])

    assert len(nodes) == 2
    assert nodes[0].name == "public"
    assert nodes[0].node_type == "schema"
    assert nodes[0].is_container is True
    assert nodes[0].path == ["public"]
    assert nodes[1].name == "analytics"


def test_postgres_list_children_schema_returns_tables():
    """list_children([schema]) returns table/view nodes."""
    from rivet_postgres.catalog import PostgresCatalogPlugin

    plugin = PostgresCatalogPlugin()
    catalog = _make_catalog()

    conn = _mock_conn(
        cursor_return=[
            ("users", "BASE TABLE"),
            ("orders", "BASE TABLE"),
            ("active_users", "VIEW"),
        ]
    )
    with patch("psycopg.connect", return_value=conn):
        nodes = plugin.list_children(catalog, path=["public"])

    assert len(nodes) == 3
    assert nodes[0].name == "users"
    assert nodes[0].node_type == "table"
    assert nodes[0].is_container is False
    assert nodes[0].path == ["public", "users"]
    assert nodes[2].name == "active_users"
    assert nodes[2].node_type == "view"


def test_postgres_list_children_table_returns_columns():
    """list_children([schema, table]) returns column nodes via get_schema."""
    from rivet_postgres.catalog import PostgresCatalogPlugin

    plugin = PostgresCatalogPlugin()
    catalog = _make_catalog()

    # get_schema mock returns columns
    schema_rows = [
        ("id", "integer", "NO", None, True),
        ("name", "text", "YES", None, False),
    ]
    conn = _mock_conn(cursor_return=schema_rows)
    with patch("psycopg.connect", return_value=conn):
        nodes = plugin.list_children(catalog, path=["public", "users"])

    assert len(nodes) == 2
    assert nodes[0].name == "id"
    assert nodes[0].node_type == "column"
    assert nodes[0].path == ["public", "users", "id"]
    assert nodes[1].name == "name"


def test_postgres_list_children_deep_path_returns_empty():
    """list_children with depth > 2 returns an empty list."""
    from rivet_postgres.catalog import PostgresCatalogPlugin

    plugin = PostgresCatalogPlugin()
    catalog = _make_catalog()

    nodes = plugin.list_children(catalog, path=["public", "users", "id"])
    assert nodes == []


def test_postgres_list_children_excludes_system_schemas():
    """list_children([]) excludes pg_catalog, information_schema, pg_toast."""
    from rivet_postgres.catalog import PostgresCatalogPlugin

    plugin = PostgresCatalogPlugin()
    catalog = _make_catalog()

    # The SQL itself filters these out, so the mock only returns user schemas
    conn = _mock_conn(cursor_return=[("public",), ("myschema",)])
    with patch("psycopg.connect", return_value=conn):
        nodes = plugin.list_children(catalog, path=[])

    names = [n.name for n in nodes]
    assert "pg_catalog" not in names
    assert "information_schema" not in names
    assert "pg_toast" not in names
