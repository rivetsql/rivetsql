"""Property test: format_sql uses ReplState dialect (task 3.4).

Property 4: For any SQL and dialect in ReplState, `format_sql(sql)` matches
`format_sql(sql, dialect=<repl_state_dialect>)`.

Validates: Requirements 3.6
"""

from __future__ import annotations

from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.assembly import Assembly
from rivet_core.interactive.session import InteractiveSession
from rivet_core.models import ComputeEngine
from rivet_core.plugins import PluginRegistry

_VALID_SQL_POOL = [
    "SELECT 1",
    "SELECT id FROM users",
    "SELECT id, name FROM users WHERE id = 1",
    "SELECT a, b FROM t ORDER BY a",
    "SELECT COUNT(*) FROM orders",
    "SELECT a FROM t LIMIT 10",
    "SELECT a FROM t WHERE a IS NOT NULL",
    "SELECT DISTINCT a FROM t",
    "SELECT a AS alias FROM t",
]

_VALID_DIALECTS = ["duckdb", "spark", "postgres", "bigquery", "trino", "mysql", "sqlite"]

_sql_st = st.sampled_from(_VALID_SQL_POOL)
_dialect_st = st.sampled_from(_VALID_DIALECTS)


def _make_session() -> InteractiveSession:
    session = InteractiveSession(project_path=Path("."))
    session.init_from(
        assembly=Assembly([]),
        catalogs={},
        engines={"duckdb": ComputeEngine(name="duckdb", engine_type="duckdb")},
        registry=PluginRegistry(),
    )
    return session


@given(sql=_sql_st, dialect=_dialect_st)
@settings(max_examples=50)
def test_format_sql_uses_repl_state_dialect(sql: str, dialect: str) -> None:
    """format_sql(sql) with dialect in ReplState equals format_sql(sql, dialect=dialect)."""
    session = _make_session()
    session.set_dialect(dialect)
    assert session.repl_state.dialect == dialect
    result_implicit = session.format_sql(sql)
    result_explicit = session.format_sql(sql, dialect=dialect)
    assert result_implicit == result_explicit


@given(sql=_sql_st)
@settings(max_examples=30)
def test_format_sql_no_dialect_in_state_uses_none(sql: str) -> None:
    """format_sql(sql) with no dialect in ReplState equals format_sql(sql, dialect=None)."""
    session = _make_session()
    assert session.repl_state.dialect is None
    result_implicit = session.format_sql(sql)
    result_explicit = session.format_sql(sql, dialect=None)
    assert result_implicit == result_explicit
