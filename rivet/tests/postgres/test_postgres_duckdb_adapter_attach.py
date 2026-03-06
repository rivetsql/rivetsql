"""Tests for task 12.3: PostgresDuckDBAdapter uses DuckDB postgres extension with ATTACH."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from rivet_postgres.adapters.duckdb import (
    PostgresDuckDBAdapter,
    _attach_alias,
    _build_dsn,
    _PostgresDuckDBMaterializedRef,
)

# ── Helpers ──────────────────────────────────────────────────────────

def _make_catalog(name: str = "my_pg", options: dict[str, Any] | None = None):
    from rivet_core.models import Catalog
    opts = options or {
        "host": "localhost",
        "port": 5432,
        "database": "testdb",
        "user": "testuser",
        "password": "testpass",
        "schema": "public",
    }
    return Catalog(name=name, type="postgres", options=opts)


def _make_joint(name: str = "j1", sql: str | None = None, table: str | None = None, write_strategy: str | None = None):
    from rivet_core.models import Joint
    return Joint(name=name, joint_type="source", sql=sql, table=table, write_strategy=write_strategy)


# ── DSN building ─────────────────────────────────────────────────────

def test_build_dsn_full():
    dsn = _build_dsn({"host": "db.example.com", "port": 5433, "database": "mydb", "user": "u", "password": "p"})
    assert "host=db.example.com" in dsn
    assert "port=5433" in dsn
    assert "dbname=mydb" in dsn
    assert "user=u" in dsn
    assert "password=p" in dsn


def test_build_dsn_defaults():
    dsn = _build_dsn({"host": "h", "database": "d"})
    assert "host=h" in dsn
    assert "port=5432" in dsn
    assert "dbname=d" in dsn
    assert "user=" not in dsn
    assert "password=" not in dsn


def test_attach_alias():
    assert _attach_alias("my_pg") == "pg_my_pg"


# ── read_dispatch returns deferred Material ──────────────────────────

def test_read_dispatch_returns_deferred_material():
    adapter = PostgresDuckDBAdapter()
    catalog = _make_catalog()
    joint = _make_joint(sql="SELECT 1")
    engine = MagicMock()

    result = adapter.read_dispatch(engine, catalog, joint)

    assert result.material.name == "j1"
    assert result.material.catalog == "my_pg"
    assert result.material.state == "deferred"
    assert result.material.materialized_ref is not None


def test_read_dispatch_ref_is_postgres_duckdb_ref():
    adapter = PostgresDuckDBAdapter()
    catalog = _make_catalog()
    joint = _make_joint(table="users")
    engine = MagicMock()

    result = adapter.read_dispatch(engine, catalog, joint)
    assert isinstance(result.material.materialized_ref, _PostgresDuckDBMaterializedRef)


# ── MaterializedRef calls ensure_extension and ATTACH ────────────────

def test_materialized_ref_calls_ensure_extension_and_attach():
    mock_conn = MagicMock()
    arrow_table = pa.table({"id": [1, 2]})
    mock_conn.execute.return_value.arrow.return_value = arrow_table

    mock_duckdb = MagicMock()
    mock_duckdb.connect.return_value = mock_conn

    with patch.dict("sys.modules", {"duckdb": mock_duckdb}), \
         patch("rivet_postgres.adapters.duckdb._ensure_duckdb_extension") as mock_ensure:
        ref = _PostgresDuckDBMaterializedRef(
            catalog_options={"host": "h", "port": 5432, "database": "d", "user": "u", "password": "p", "schema": "public"},
            catalog_name="my_pg",
            sql="SELECT * FROM pg_my_pg.public.users",
            table=None,
        )
        result = ref.to_arrow()

    mock_ensure.assert_called_once_with(mock_conn, "postgres", install_from="community")
    attach_calls = [c for c in mock_conn.execute.call_args_list if "ATTACH" in str(c)]
    assert len(attach_calls) == 1
    attach_sql = attach_calls[0][0][0]
    assert "TYPE postgres" in attach_sql
    assert "pg_my_pg" in attach_sql
    assert result.equals(arrow_table)


def test_materialized_ref_table_fallback():
    """When sql is None, ref reads from {alias}.{schema}.{table}."""
    mock_conn = MagicMock()
    arrow_table = pa.table({"name": ["a"]})
    mock_conn.execute.return_value.arrow.return_value = arrow_table

    mock_duckdb = MagicMock()
    mock_duckdb.connect.return_value = mock_conn

    with patch.dict("sys.modules", {"duckdb": mock_duckdb}), \
         patch("rivet_postgres.adapters.duckdb._ensure_duckdb_extension"):
        ref = _PostgresDuckDBMaterializedRef(
            catalog_options={"host": "h", "port": 5432, "database": "d", "schema": "myschema"},
            catalog_name="pg1",
            sql=None,
            table="orders",
        )
        ref.to_arrow()

    select_calls = [c for c in mock_conn.execute.call_args_list if "SELECT" in str(c)]
    assert len(select_calls) == 1
    assert "pg_pg1.myschema.orders" in select_calls[0][0][0]


def test_materialized_ref_error_wrapping():
    """Non-ExecutionError exceptions are wrapped in RVT-504."""
    mock_conn = MagicMock()
    mock_duckdb = MagicMock()
    mock_duckdb.connect.return_value = mock_conn

    with patch.dict("sys.modules", {"duckdb": mock_duckdb}), \
         patch("rivet_postgres.adapters.duckdb._ensure_duckdb_extension", side_effect=RuntimeError("boom")):
        ref = _PostgresDuckDBMaterializedRef(
            catalog_options={"host": "h", "database": "d"},
            catalog_name="pg1",
            sql="SELECT 1",
            table=None,
        )
        with pytest.raises(Exception) as exc_info:
            ref.to_arrow()

    from rivet_core.errors import ExecutionError
    assert isinstance(exc_info.value, ExecutionError)
    assert "RVT-504" in exc_info.value.error.code


# ── write_dispatch ───────────────────────────────────────────────────

def _run_write_dispatch(write_strategy: str):
    mock_conn = MagicMock()
    mock_duckdb = MagicMock()
    mock_duckdb.connect.return_value = mock_conn

    adapter = PostgresDuckDBAdapter()
    catalog = _make_catalog()
    joint = _make_joint(table="users", write_strategy=write_strategy)
    joint.joint_type = "sink"
    engine = MagicMock()
    material = MagicMock()
    material.to_arrow.return_value = pa.table({"id": [1]})

    with patch.dict("sys.modules", {"duckdb": mock_duckdb}), \
         patch("rivet_postgres.adapters.duckdb._ensure_duckdb_extension"):
        adapter.write_dispatch(engine, catalog, joint, material)

    return [str(c) for c in mock_conn.execute.call_args_list]


def test_write_dispatch_replace():
    calls = _run_write_dispatch("replace")
    assert any("DROP TABLE IF EXISTS" in c for c in calls)
    assert any("CREATE TABLE" in c and "AS SELECT * FROM __write_data" in c for c in calls)


def test_write_dispatch_append():
    calls = _run_write_dispatch("append")
    assert any("INSERT INTO" in c and "SELECT * FROM __write_data" in c for c in calls)


def test_write_dispatch_truncate_insert():
    calls = _run_write_dispatch("truncate_insert")
    assert any("DELETE FROM" in c for c in calls)
    assert any("INSERT INTO" in c for c in calls)


def test_write_dispatch_attach_not_read_only():
    """Write dispatch should ATTACH without READ_ONLY."""
    mock_conn = MagicMock()
    mock_duckdb = MagicMock()
    mock_duckdb.connect.return_value = mock_conn

    adapter = PostgresDuckDBAdapter()
    catalog = _make_catalog()
    joint = _make_joint(table="t", write_strategy="append")
    joint.joint_type = "sink"
    material = MagicMock()
    material.to_arrow.return_value = pa.table({"x": [1]})

    with patch.dict("sys.modules", {"duckdb": mock_duckdb}), \
         patch("rivet_postgres.adapters.duckdb._ensure_duckdb_extension"):
        adapter.write_dispatch(MagicMock(), catalog, joint, material)

    attach_calls = [c for c in mock_conn.execute.call_args_list if "ATTACH" in str(c)]
    assert len(attach_calls) == 1
    assert "READ_ONLY" not in attach_calls[0][0][0]


# ── read_dispatch ATTACH is READ_ONLY ───────────────────────────────

def test_read_dispatch_attach_is_read_only():
    mock_conn = MagicMock()
    mock_conn.execute.return_value.arrow.return_value = pa.table({"x": [1]})
    mock_duckdb = MagicMock()
    mock_duckdb.connect.return_value = mock_conn

    with patch.dict("sys.modules", {"duckdb": mock_duckdb}), \
         patch("rivet_postgres.adapters.duckdb._ensure_duckdb_extension"):
        ref = _PostgresDuckDBMaterializedRef(
            catalog_options={"host": "h", "port": 5432, "database": "d"},
            catalog_name="pg1",
            sql="SELECT 1",
            table=None,
        )
        ref.to_arrow()

    attach_calls = [c for c in mock_conn.execute.call_args_list if "ATTACH" in str(c)]
    assert len(attach_calls) == 1
    assert "READ_ONLY" in attach_calls[0][0][0]


# ── MaterializedRef properties ───────────────────────────────────────

def test_materialized_ref_storage_type():
    ref = _PostgresDuckDBMaterializedRef(
        catalog_options={"host": "h", "database": "d"},
        catalog_name="pg1",
        sql="SELECT 1",
        table=None,
    )
    assert ref.storage_type == "postgres"


def test_materialized_ref_size_bytes_is_none():
    ref = _PostgresDuckDBMaterializedRef(
        catalog_options={"host": "h", "database": "d"},
        catalog_name="pg1",
        sql="SELECT 1",
        table=None,
    )
    assert ref.size_bytes is None
