"""Tests for task 11.1: PostgreSQL Source — execute SQL via psycopg3, return deferred Material."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pytest

from rivet_core.errors import ExecutionError
from rivet_core.models import Catalog, Joint, Material
from rivet_core.plugins import PluginRegistry, SourcePlugin
from rivet_postgres.source import PostgresDeferredMaterializedRef, PostgresSource

# ── PostgresSource registration ────────────────────────────────────────────


def test_source_has_catalog_type():
    assert PostgresSource.catalog_type == "postgres"


def test_source_is_source_plugin():
    assert isinstance(PostgresSource(), SourcePlugin)


def test_registry_can_register_source():
    registry = PluginRegistry()
    registry.register_source(PostgresSource())
    assert registry._sources.get("postgres") is not None


# ── PostgresDeferredMaterializedRef properties ─────────────────────────────


def test_deferred_ref_storage_type():
    ref = PostgresDeferredMaterializedRef("host=localhost dbname=test", "SELECT 1")
    assert ref.storage_type == "postgres"


def test_deferred_ref_size_bytes_is_none():
    ref = PostgresDeferredMaterializedRef("host=localhost dbname=test", "SELECT 1")
    assert ref.size_bytes is None


# ── PostgresSource.read() ──────────────────────────────────────────────────


def _make_catalog(options: dict | None = None) -> Catalog:
    return Catalog(
        name="pgdb",
        type="postgres",
        options=options or {
            "host": "localhost",
            "port": 5432,
            "database": "testdb",
            "user": "user",
            "password": "pass",
        },
    )


def _make_joint(sql: str = "SELECT 1 AS n", name: str = "j1") -> Joint:
    return Joint(name=name, joint_type="source", catalog="pgdb", sql=sql)


def test_read_returns_material():
    source = PostgresSource()
    result = source.read(_make_catalog(), _make_joint(), None)
    assert isinstance(result, Material)


def test_read_returns_deferred_state():
    source = PostgresSource()
    result = source.read(_make_catalog(), _make_joint(), None)
    assert result.state == "deferred"


def test_read_material_has_ref():
    source = PostgresSource()
    result = source.read(_make_catalog(), _make_joint(), None)
    assert result.materialized_ref is not None
    assert isinstance(result.materialized_ref, PostgresDeferredMaterializedRef)


def test_read_material_name_matches_joint():
    source = PostgresSource()
    result = source.read(_make_catalog(), _make_joint(name="my_joint"), None)
    assert result.name == "my_joint"


def test_read_material_catalog_matches():
    source = PostgresSource()
    result = source.read(_make_catalog(), _make_joint(), None)
    assert result.catalog == "pgdb"


def test_read_is_deferred_not_eager():
    """Material is deferred — no SQL execution until to_arrow() is called."""
    source = PostgresSource()
    # Even with invalid SQL, read() must not raise
    joint = Joint(name="j1", joint_type="source", catalog="pgdb", sql="SELECT * FROM no_such_table")
    result = source.read(_make_catalog(), joint, None)
    assert result.state == "deferred"


def test_read_uses_table_when_no_sql():
    """When joint has no sql but has table, uses SELECT * FROM <table>."""
    source = PostgresSource()
    joint = Joint(name="j1", joint_type="source", catalog="pgdb", table="public.users")
    result = source.read(_make_catalog(), joint, None)
    ref = result.materialized_ref
    assert isinstance(ref, PostgresDeferredMaterializedRef)
    assert "public.users" in ref._sql


# ── to_arrow() with mocked psycopg3 ───────────────────────────────────────


def _make_mock_psycopg(rows: list, col_names: list[str]):
    """Build a mock psycopg module that returns the given rows/columns."""
    mock_desc = [MagicMock(name=n) for n in col_names]
    for i, n in enumerate(col_names):
        mock_desc[i].name = n

    mock_cursor = AsyncMock()
    mock_cursor.__aenter__ = AsyncMock(return_value=mock_cursor)
    mock_cursor.__aexit__ = AsyncMock(return_value=False)
    mock_cursor.execute = AsyncMock()
    mock_cursor.fetchall = AsyncMock(return_value=rows)
    mock_cursor.description = mock_desc

    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)
    mock_conn.cursor = MagicMock(return_value=mock_cursor)

    mock_psycopg = MagicMock()
    mock_psycopg.AsyncConnection.connect = AsyncMock(return_value=mock_conn)
    return mock_psycopg


def test_to_arrow_returns_pyarrow_table():
    rows = [(1, "alice"), (2, "bob")]
    mock_psycopg = _make_mock_psycopg(rows, ["id", "name"])

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        ref = PostgresDeferredMaterializedRef("host=localhost dbname=test", "SELECT id, name FROM users")
        table = ref.to_arrow()

    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    assert table.column_names == ["id", "name"]
    assert table.column("id").to_pylist() == [1, 2]
    assert table.column("name").to_pylist() == ["alice", "bob"]


def test_to_arrow_executes_sql():
    rows = [(42,)]
    mock_psycopg = _make_mock_psycopg(rows, ["val"])

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        ref = PostgresDeferredMaterializedRef("host=localhost dbname=test", "SELECT 42 AS val")
        ref.to_arrow()

    mock_psycopg.AsyncConnection.connect.assert_awaited_once()


def test_to_arrow_empty_result():
    mock_psycopg = _make_mock_psycopg([], ["id"])

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        ref = PostgresDeferredMaterializedRef("host=localhost dbname=test", "SELECT id FROM t WHERE false")
        table = ref.to_arrow()

    assert isinstance(table, pa.Table)
    assert table.num_rows == 0


def test_to_arrow_raises_execution_error_on_db_failure():
    mock_psycopg = MagicMock()
    mock_psycopg.AsyncConnection.connect = AsyncMock(side_effect=Exception("connection refused"))

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        ref = PostgresDeferredMaterializedRef("host=badhost dbname=test", "SELECT 1")
        with pytest.raises(ExecutionError) as exc_info:
            ref.to_arrow()

    assert exc_info.value.error.code == "RVT-501"


def test_to_arrow_raises_execution_error_when_psycopg_missing():
    import sys
    from unittest.mock import patch

    # Block psycopg import entirely so the ImportError path is exercised
    original = sys.modules.pop("psycopg", None)
    try:
        with patch.dict("sys.modules", {"psycopg": None}):
            ref = PostgresDeferredMaterializedRef("host=localhost dbname=test", "SELECT 1")
            with pytest.raises(ExecutionError) as exc_info:
                ref.to_arrow()
            assert exc_info.value.error.code == "RVT-501"
            assert "psycopg" in exc_info.value.error.remediation.lower()
    finally:
        if original is not None:
            sys.modules["psycopg"] = original


def test_row_count_via_to_arrow():
    rows = [(1,), (2,), (3,)]
    mock_psycopg = _make_mock_psycopg(rows, ["n"])

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        ref = PostgresDeferredMaterializedRef("host=localhost dbname=test", "SELECT n FROM t")
        assert ref.row_count == 3


def test_schema_via_to_arrow():
    rows = [(1, "x")]
    mock_psycopg = _make_mock_psycopg(rows, ["id", "label"])

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        ref = PostgresDeferredMaterializedRef("host=localhost dbname=test", "SELECT id, label FROM t")
        schema = ref.schema

    names = [c.name for c in schema.columns]
    assert "id" in names
    assert "label" in names


def test_read_full_pipeline_with_mock():
    """Full pipeline: read() → to_arrow() with mocked psycopg3."""
    rows = [(10, "foo"), (20, "bar")]
    mock_psycopg = _make_mock_psycopg(rows, ["score", "tag"])

    with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
        source = PostgresSource()
        catalog = _make_catalog()
        joint = _make_joint(sql="SELECT score, tag FROM results")
        material = source.read(catalog, joint, None)

        assert material.state == "deferred"
        table = material.to_arrow()

    assert table.num_rows == 2
    assert table.column("score").to_pylist() == [10, 20]
    assert table.column("tag").to_pylist() == ["foo", "bar"]
