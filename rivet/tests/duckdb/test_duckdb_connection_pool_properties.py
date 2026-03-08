"""Property-based tests: DuckDB Connection Pool (Properties 3, 4, 5).

Property 3: DuckDB Connection Reuse
  For any sequence of N >= 2 successful execute_sql() calls on a
  DuckDBComputeEnginePlugin instance, the underlying DuckDB connection object
  shall be created exactly once and reused for all calls.

Property 4: DuckDB View Cleanup on Reuse
  For any two consecutive execute_sql() calls with different input table name
  sets A and B, after the second call completes, only the table names in B
  shall be registered on the connection.

Property 5: DuckDB Connection Recovery After Error
  For any DuckDBComputeEnginePlugin instance where an execute_sql() call raises
  an unrecoverable error, the next successful execute_sql() call shall use a
  fresh connection object (not the one that errored).
"""

from __future__ import annotations

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.models import ComputeEngine
from rivet_duckdb.engine import DuckDBComputeEnginePlugin

# ── Helpers ─────────────────────────────────────────────────────────────────────

_ENGINE = ComputeEngine(name="test", engine_type="duckdb")


def _make_table(n_rows: int = 3) -> pa.Table:
    return pa.table({"x": list(range(n_rows))})


# ── Property 3: DuckDB Connection Reuse ─────────────────────────────────────────


@given(n_calls=st.integers(min_value=2, max_value=10))
@settings(max_examples=100)
def test_property3_connection_reused_across_calls(n_calls: int) -> None:
    """Property 3: execute_sql() reuses the same connection object across N calls."""
    plugin = DuckDBComputeEnginePlugin()
    table = _make_table()

    conn_ids: list[int] = []
    for _ in range(n_calls):
        plugin.execute_sql(_ENGINE, "SELECT * FROM t", {"t": table})
        conn_ids.append(id(plugin._conn))

    # All connection object ids should be identical
    assert len(set(conn_ids)) == 1, f"Expected 1 unique connection, got {len(set(conn_ids))}"


@given(n_calls=st.integers(min_value=2, max_value=10))
@settings(max_examples=100)
def test_property3_connection_created_exactly_once(n_calls: int) -> None:
    """Property 3: _get_connection() creates the connection on first call only."""
    plugin = DuckDBComputeEnginePlugin()
    table = _make_table()

    assert plugin._conn is None
    plugin.execute_sql(_ENGINE, "SELECT * FROM t", {"t": table})
    first_conn = plugin._conn
    assert first_conn is not None

    for _ in range(n_calls - 1):
        plugin.execute_sql(_ENGINE, "SELECT * FROM t", {"t": table})

    assert plugin._conn is first_conn


# ── Property 4: DuckDB View Cleanup on Reuse ────────────────────────────────────

_table_name = st.text(
    alphabet=st.characters(whitelist_categories=("Ll",), whitelist_characters="_"),
    min_size=2,
    max_size=8,
)


@given(
    names_a=st.lists(_table_name, min_size=1, max_size=4, unique=True),
    names_b=st.lists(_table_name, min_size=1, max_size=4, unique=True),
)
@settings(max_examples=100)
def test_property4_only_latest_views_registered(
    names_a: list[str], names_b: list[str]
) -> None:
    """Property 4: after two calls, only the second call's table names are tracked."""
    plugin = DuckDBComputeEnginePlugin()
    table = _make_table()

    tables_a = {n: table for n in names_a}
    tables_b = {n: table for n in names_b}

    # Quote table names to avoid DuckDB reserved word conflicts
    sql_a = f'SELECT * FROM "{names_a[0]}"'
    sql_b = f'SELECT * FROM "{names_b[0]}"'

    plugin.execute_sql(_ENGINE, sql_a, tables_a)
    assert plugin._registered_views == set(names_a)

    plugin.execute_sql(_ENGINE, sql_b, tables_b)
    assert plugin._registered_views == set(names_b)


# ── Property 5: DuckDB Connection Recovery After Error ───────────────────────────


@given(n_calls=st.integers(min_value=1, max_value=5))
@settings(max_examples=100)
def test_property5_fresh_connection_after_error(n_calls: int) -> None:
    """Property 5: after an error, the next call uses a different connection."""
    plugin = DuckDBComputeEnginePlugin()
    table = _make_table()

    # First: successful call to establish a connection
    plugin.execute_sql(_ENGINE, "SELECT * FROM t", {"t": table})
    errored_conn = plugin._conn

    # Trigger an error with invalid SQL
    try:
        plugin.execute_sql(_ENGINE, "INVALID SQL THAT WILL FAIL", {"t": table})
    except Exception:
        pass

    # Connection should have been discarded
    assert plugin._conn is None

    # Next successful call should create a fresh connection
    plugin.execute_sql(_ENGINE, "SELECT * FROM t", {"t": table})
    recovered_conn = plugin._conn

    assert recovered_conn is not None
    assert recovered_conn is not errored_conn


def test_property5_registered_views_cleared_after_error() -> None:
    """Property 5: _registered_views is cleared when an error discards the connection."""
    plugin = DuckDBComputeEnginePlugin()
    table = _make_table()

    plugin.execute_sql(_ENGINE, "SELECT * FROM t", {"t": table})
    assert len(plugin._registered_views) > 0

    try:
        plugin.execute_sql(_ENGINE, "INVALID SQL", {"t": table})
    except Exception:
        pass

    assert len(plugin._registered_views) == 0
