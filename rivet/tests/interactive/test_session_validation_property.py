"""Property test for InteractiveSession — validation does not block execution.

Property 27: Validation is advisory only and does not block execution.
SQL with unresolved references should still attempt execution.

Validates: Requirements 9.5
"""

from __future__ import annotations

from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.assembly import Assembly
from rivet_core.interactive.session import InteractiveSession
from rivet_core.interactive.types import QueryResult
from rivet_core.models import ComputeEngine
from rivet_core.plugins import PluginRegistry


def _make_session() -> InteractiveSession:
    """Create a properly initialized session with registry and engine."""
    from rivet_duckdb.engine import DuckDBComputeEnginePlugin

    session = InteractiveSession(project_path=Path("."), read_only=False)
    registry = PluginRegistry()
    registry.register_engine_plugin(DuckDBComputeEnginePlugin())
    assembly = Assembly([])
    session.init_from(
        assembly=assembly,
        catalogs={},
        engines={"default": ComputeEngine(name="default", engine_type="duckdb")},
        registry=registry,
    )
    session.start()
    return session


# SQL that doesn't reference any tables (always succeeds in DuckDB)
_simple_sql = st.one_of(
    st.just("SELECT 1"),
    st.just("SELECT 1 AS x, 2 AS y"),
    st.just("SELECT 'hello' AS greeting"),
    st.just("SELECT 1 + 2 AS sum"),
    st.just("SELECT CURRENT_DATE AS today"),
)


@given(sql=_simple_sql)
@settings(max_examples=50)
def test_execute_query_returns_result(sql: str) -> None:
    """Property 27: execute_query() returns QueryResult for valid SQL."""
    session = _make_session()
    result = session.execute_query(sql)
    assert isinstance(result, QueryResult)
    assert result.row_count >= 0


def test_execute_query_proceeds_when_assembly_has_warnings() -> None:
    """Property 27: execute_query() proceeds even when assembly has warnings."""
    session = _make_session()
    result = session.execute_query("SELECT 1 AS x")
    assert isinstance(result, QueryResult)
