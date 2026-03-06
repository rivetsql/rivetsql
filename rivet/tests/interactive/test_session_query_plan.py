"""Tests for InteractiveSession — execute_query returns QueryResult.

Validates that execute_query compiles and executes transient pipelines
through the standard Executor path.

Validates: Requirements 11.1, 15.2–15.5
"""

from __future__ import annotations

from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.interactive.session import InteractiveSession
from rivet_core.interactive.types import QueryResult
from rivet_core.plugins import PluginRegistry

# SQL reserved words to avoid in generated identifiers
_SQL_RESERVED = frozenset({
    "all", "alter", "and", "any", "as", "asc", "at", "between", "by", "case",
    "create", "cross", "delete", "desc", "distinct", "do", "drop", "else",
    "end", "exists", "false", "for", "from", "full", "group", "having", "if",
    "in", "index", "inner", "insert", "into", "is", "join", "left", "like",
    "limit", "not", "null", "of", "offset", "on", "or", "order", "outer",
    "right", "select", "set", "some", "table", "then", "to", "true", "union",
    "update", "values", "view", "when", "where", "with",
})

_identifier = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True).filter(
    lambda t: t not in _SQL_RESERVED
)


def _make_started_session() -> InteractiveSession:
    """Create a properly initialized session with registry and engine."""
    from rivet_core.assembly import Assembly
    from rivet_core.models import ComputeEngine
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


@given(table_name=_identifier)
@settings(max_examples=50)
def test_execute_query_returns_query_result(table_name: str) -> None:
    """execute_query() returns a QueryResult for any valid SQL."""
    session = _make_started_session()
    sql = f"SELECT 1 AS {table_name}"
    result = session.execute_query(sql)
    assert isinstance(result, QueryResult)
    assert result.row_count >= 0


def test_execute_query_simple_select() -> None:
    """execute_query() can run a simple SELECT without table refs."""
    session = _make_started_session()
    result = session.execute_query("SELECT 1 AS x, 2 AS y")
    assert result.row_count == 1
    assert result.column_names == ["x", "y"]


def test_execute_query_truncation() -> None:
    """Results exceeding max_results are truncated."""
    session = _make_started_session()
    session._max_results = 5
    result = session.execute_query(
        "SELECT * FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20)) AS t(x)"
    )
    assert result.row_count == 5
    assert result.truncated is True


def test_execute_query_records_history() -> None:
    """execute_query() records an entry in session history."""
    session = _make_started_session()
    session.execute_query("SELECT 42 AS answer")
    assert len(session.history) == 1
    assert session.history[0].action_type == "query"
    assert session.history[0].status == "success"


def test_execute_query_read_only_raises() -> None:
    """execute_query() raises ReadOnlyError in read-only mode."""
    from rivet_core.interactive.session import ReadOnlyError

    session = _make_started_session()
    session._read_only = True
    with pytest.raises(ReadOnlyError):
        session.execute_query("SELECT 1")
