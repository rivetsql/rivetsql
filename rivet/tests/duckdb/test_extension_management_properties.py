"""Property-based tests: extension management idempotency and failure (Properties 8 & 9).

Property 8: DuckDB extension loading is idempotent.
  For any DuckDB extension name and connection, calling ensure_extension(conn, name)
  when the extension is already loaded does not raise and does not attempt to reload it.

Property 9: DuckDB extension load failure produces RVT-502.
  For any DuckDB extension name that cannot be loaded, ensure_extension raises
  ExecutionError with code RVT-502, and the error message contains the extension name
  and remediation.
"""

from __future__ import annotations

import duckdb
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.errors import ExecutionError
from rivet_duckdb.extensions import ensure_extension

# Built-in DuckDB extensions that are always installed (no network needed)
_BUILTIN_EXTENSIONS = ["json", "parquet"]

# Strategy for generating extension names that look plausible but don't exist
_invalid_ext_name = st.from_regex(r"[a-z][a-z0-9_]{4,29}_rivet_fake", fullmatch=True)


def _conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(":memory:")


# ── Property 8: Idempotency ────────────────────────────────────────────────────


@given(ext=st.sampled_from(_BUILTIN_EXTENSIONS))
@settings(max_examples=100)
def test_property8_ensure_extension_idempotent_when_already_loaded(ext: str) -> None:
    """Property 8: ensure_extension on an already-loaded extension does not raise."""
    conn = _conn()
    # Pre-load the extension so it is in loaded state
    conn.execute(f"LOAD {ext}")
    row = conn.execute(
        "SELECT loaded FROM duckdb_extensions() WHERE extension_name = ?", [ext]
    ).fetchone()
    assert row is not None and row[0] is True, f"Pre-condition: {ext} must be loaded"

    # Calling ensure_extension again must not raise
    ensure_extension(conn, ext)

    # Extension must still be loaded after the idempotent call
    row2 = conn.execute(
        "SELECT loaded FROM duckdb_extensions() WHERE extension_name = ?", [ext]
    ).fetchone()
    assert row2 is not None and row2[0] is True


@given(ext=st.sampled_from(_BUILTIN_EXTENSIONS))
@settings(max_examples=100)
def test_property8_ensure_extension_idempotent_multiple_calls(ext: str) -> None:
    """Property 8: calling ensure_extension N times on the same extension never raises."""
    conn = _conn()
    ensure_extension(conn, ext)
    ensure_extension(conn, ext)
    ensure_extension(conn, ext)


# ── Property 9: Failure produces RVT-502 ──────────────────────────────────────


@given(ext=_invalid_ext_name)
@settings(max_examples=100)
def test_property9_nonexistent_extension_raises_rvt502(ext: str) -> None:
    """Property 9: ensure_extension raises ExecutionError(RVT-502) for any unloadable extension."""
    conn = _conn()
    with pytest.raises(ExecutionError) as exc_info:
        ensure_extension(conn, ext)
    err = exc_info.value.error
    assert err.code == "RVT-502", f"Expected RVT-502, got {err.code}"
    assert ext in err.message, f"Extension name '{ext}' not in error message: {err.message}"
    assert err.remediation is not None and len(err.remediation) > 0


@given(ext=_invalid_ext_name)
@settings(max_examples=100)
def test_property9_error_context_contains_extension_name(ext: str) -> None:
    """Property 9: RVT-502 error context dict contains the extension name."""
    conn = _conn()
    with pytest.raises(ExecutionError) as exc_info:
        ensure_extension(conn, ext)
    err = exc_info.value.error
    assert err.context.get("extension") == ext
