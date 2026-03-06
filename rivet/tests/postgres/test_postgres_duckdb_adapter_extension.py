"""Tests for task 12.4: check-then-load for postgres extension (INSTALL postgres FROM community)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa

from rivet_postgres.adapters.duckdb import (
    PostgresDuckDBAdapter,
    _PostgresDuckDBMaterializedRef,
)


def _make_catalog(name: str = "pg1"):
    from rivet_core.models import Catalog
    return Catalog(
        name=name,
        type="postgres",
        options={"host": "localhost", "port": 5432, "database": "db", "user": "u", "password": "p", "schema": "public"},
    )


def _make_joint(name: str = "j1", sql: str = "SELECT 1", table: str | None = None, write_strategy: str = "replace"):
    from rivet_core.models import Joint
    return Joint(name=name, joint_type="source", sql=sql, table=table, write_strategy=write_strategy)


# ── ensure_extension called with install_from="community" ────────────────────

def test_read_dispatch_calls_ensure_extension_with_community():
    """to_arrow() must call _ensure_duckdb_extension(conn, 'postgres', install_from='community')."""
    mock_conn = MagicMock()
    mock_conn.execute.return_value.arrow.return_value = pa.table({"x": [1]})
    mock_duckdb = MagicMock()
    mock_duckdb.connect.return_value = mock_conn

    with patch.dict("sys.modules", {"duckdb": mock_duckdb}), \
         patch("rivet_postgres.adapters.duckdb._ensure_duckdb_extension") as mock_ensure:
        ref = _PostgresDuckDBMaterializedRef(
            catalog_options={"host": "h", "port": 5432, "database": "d", "user": "u", "password": "p", "schema": "public"},
            catalog_name="pg1",
            sql="SELECT 1",
            table=None,
        )
        ref.to_arrow()

    mock_ensure.assert_called_once_with(mock_conn, "postgres", install_from="community")


def test_write_dispatch_calls_ensure_extension_with_community():
    """write_dispatch must call _ensure_duckdb_extension(conn, 'postgres', install_from='community')."""
    mock_conn = MagicMock()
    mock_duckdb = MagicMock()
    mock_duckdb.connect.return_value = mock_conn

    adapter = PostgresDuckDBAdapter()
    catalog = _make_catalog()
    joint = _make_joint(table="users", write_strategy="append")
    joint.joint_type = "sink"
    material = MagicMock()
    material.to_arrow.return_value = pa.table({"id": [1]})

    with patch.dict("sys.modules", {"duckdb": mock_duckdb}), \
         patch("rivet_postgres.adapters.duckdb._ensure_duckdb_extension") as mock_ensure:
        adapter.write_dispatch(MagicMock(), catalog, joint, material)

    mock_ensure.assert_called_once_with(mock_conn, "postgres", install_from="community")


# ── _ensure_duckdb_extension install_from parameter ──────────────────────────

def test_ensure_extension_install_from_community_sql():
    """_ensure_duckdb_extension with install_from='community' executes INSTALL ext FROM community."""
    from rivet_postgres.adapters.duckdb import _ensure_duckdb_extension

    conn = MagicMock()
    # Simulate: not installed, not loaded
    conn.execute.return_value.fetchone.return_value = (False, False)

    # Patch the LOAD call to succeed (avoid actual network call)
    with patch.object(conn, "execute") as mock_exec:
        mock_exec.return_value.fetchone.return_value = (False, False)
        try:
            _ensure_duckdb_extension(conn, "postgres", install_from="community")
        except Exception:
            pass  # LOAD may fail on mock; we only care about INSTALL call

    install_calls = [str(c) for c in mock_exec.call_args_list if "INSTALL" in str(c)]
    assert len(install_calls) == 1
    assert "FROM community" in install_calls[0]


def test_ensure_extension_no_install_from_uses_plain_install():
    """_ensure_duckdb_extension without install_from uses plain INSTALL (no FROM clause)."""
    from rivet_postgres.adapters.duckdb import _ensure_duckdb_extension

    conn = MagicMock()
    with patch.object(conn, "execute") as mock_exec:
        mock_exec.return_value.fetchone.return_value = (False, False)
        try:
            _ensure_duckdb_extension(conn, "json")
        except Exception:
            pass

    install_calls = [str(c) for c in mock_exec.call_args_list if "INSTALL" in str(c)]
    assert len(install_calls) == 1
    assert "FROM" not in install_calls[0]


def test_ensure_extension_already_loaded_skips_install():
    """_ensure_duckdb_extension skips INSTALL when extension is already loaded."""
    from rivet_postgres.adapters.duckdb import _ensure_duckdb_extension

    conn = MagicMock()
    with patch.object(conn, "execute") as mock_exec:
        mock_exec.return_value.fetchone.return_value = (True, True)  # installed + loaded
        _ensure_duckdb_extension(conn, "postgres", install_from="community")

    install_calls = [str(c) for c in mock_exec.call_args_list if "INSTALL" in str(c)]
    assert len(install_calls) == 0
