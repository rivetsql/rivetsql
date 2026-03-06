"""Tests for rivet_duckdb.extensions — check-then-load pattern (task 5.1)."""

from __future__ import annotations

import duckdb
import pytest

from rivet_core.errors import ExecutionError
from rivet_duckdb.extensions import ensure_extension, preload_extensions


def _conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(":memory:")


# ── Property 8: idempotency ────────────────────────────────────────────────────

def test_ensure_extension_already_loaded_is_idempotent():
    """Calling ensure_extension on an already-loaded extension does not raise."""
    conn = _conn()
    # json is a built-in extension that is always loaded in DuckDB
    # Load it once explicitly, then call ensure_extension — must not error
    conn.execute("LOAD json")
    # Should not raise and should not attempt to reload
    ensure_extension(conn, "json")


def test_ensure_extension_loads_unloaded_builtin():
    """ensure_extension loads a built-in extension that is installed but not loaded."""
    conn = _conn()
    # json is always installed; verify it loads without error
    ensure_extension(conn, "json")
    row = conn.execute(
        "SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'json'"
    ).fetchone()
    assert row is not None and row[0] is True


def test_ensure_extension_idempotent_second_call():
    """Calling ensure_extension twice on the same extension does not raise."""
    conn = _conn()
    ensure_extension(conn, "json")
    ensure_extension(conn, "json")  # second call must be idempotent


# ── Property 9: failure raises RVT-502 ────────────────────────────────────────

def test_ensure_extension_nonexistent_raises_rvt502():
    """ensure_extension raises ExecutionError(RVT-502) for a non-existent extension."""
    conn = _conn()
    with pytest.raises(ExecutionError) as exc_info:
        ensure_extension(conn, "nonexistent_extension_xyz_abc_123")
    err = exc_info.value.error
    assert err.code == "RVT-502"
    assert "nonexistent_extension_xyz_abc_123" in err.message
    assert err.remediation is not None


def test_ensure_extension_error_contains_extension_name():
    """RVT-502 error context includes the extension name."""
    conn = _conn()
    with pytest.raises(ExecutionError) as exc_info:
        ensure_extension(conn, "bad_ext_name_999")
    err = exc_info.value.error
    assert err.context.get("extension") == "bad_ext_name_999"


# ── Task 5.3: Pre-load extensions from engine `extensions` option at startup ──

def test_preload_extensions_empty_list_does_not_raise():
    """preload_extensions with an empty list does nothing and does not raise."""
    conn = _conn()
    preload_extensions(conn, [])  # should not raise


def test_preload_extensions_loads_single_extension():
    """preload_extensions loads a single extension."""
    conn = _conn()
    preload_extensions(conn, ["json"])
    row = conn.execute(
        "SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'json'"
    ).fetchone()
    assert row is not None and row[0] is True


def test_preload_extensions_loads_multiple_extensions():
    """preload_extensions loads all extensions in the list."""
    conn = _conn()
    preload_extensions(conn, ["json", "parquet"])
    for ext in ("json", "parquet"):
        row = conn.execute(
            "SELECT loaded FROM duckdb_extensions() WHERE extension_name = ?", [ext]
        ).fetchone()
        assert row is not None and row[0] is True, f"Extension '{ext}' not loaded"


def test_preload_extensions_is_idempotent():
    """preload_extensions called twice with the same list does not raise."""
    conn = _conn()
    preload_extensions(conn, ["json"])
    preload_extensions(conn, ["json"])  # second call must not raise


def test_preload_extensions_fails_fast_on_bad_extension():
    """preload_extensions raises ExecutionError(RVT-502) on first bad extension."""
    conn = _conn()
    with pytest.raises(ExecutionError) as exc_info:
        preload_extensions(conn, ["json", "nonexistent_xyz_abc_999"])
    err = exc_info.value.error
    assert err.code == "RVT-502"
    assert "nonexistent_xyz_abc_999" in err.message
