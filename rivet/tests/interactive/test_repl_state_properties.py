"""Property test: State setter round-trip (task 2.3).

Property 1: For any field and valid value, calling the setter then reading
`repl_state` returns the set value with other fields unchanged.

Validates: Requirements 1.3, 1.4, 2.1
"""

from __future__ import annotations

from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.assembly import Assembly
from rivet_core.interactive.session import InteractiveSession
from rivet_core.models import ComputeEngine
from rivet_core.plugins import PluginRegistry

# Valid sqlglot dialect names (sampled from a known-good subset)
_VALID_DIALECTS = [
    d for d in ["duckdb", "spark", "postgres", "bigquery", "trino", "mysql", "sqlite"]
    if True  # all are valid sqlglot dialects
]

_dialect_st = st.sampled_from(_VALID_DIALECTS)
_sql_st = st.text(min_size=0, max_size=200)
_engine_st = st.sampled_from(["duckdb", "spark"])


def _make_session(project_path: Path = Path(".")) -> InteractiveSession:
    session = InteractiveSession(project_path=project_path)
    session.init_from(
        assembly=Assembly([]),
        catalogs={},
        engines={
            "duckdb": ComputeEngine(name="duckdb", engine_type="duckdb"),
            "spark": ComputeEngine(name="spark", engine_type="spark"),
        },
        registry=PluginRegistry(),
    )
    return session


@given(sql=_sql_st)
@settings(max_examples=50)
def test_update_editor_sql_round_trip(sql: str) -> None:
    """update_editor_sql sets editor_sql; other fields unchanged."""
    session = _make_session()
    before = session.repl_state
    session.update_editor_sql(sql)
    after = session.repl_state
    assert after.editor_sql == sql
    assert after.adhoc_engine == before.adhoc_engine
    assert after.dialect == before.dialect


@given(dialect=_dialect_st)
@settings(max_examples=30)
def test_set_dialect_round_trip(dialect: str) -> None:
    """set_dialect sets dialect; other fields unchanged."""
    session = _make_session()
    session.update_editor_sql("SELECT 1")
    session.adhoc_engine = "duckdb"
    before = session.repl_state
    session.set_dialect(dialect)
    after = session.repl_state
    assert after.dialect == dialect
    assert after.editor_sql == before.editor_sql
    assert after.adhoc_engine == before.adhoc_engine


@given(engine=_engine_st)
@settings(max_examples=30)
def test_adhoc_engine_round_trip(engine: str) -> None:
    """adhoc_engine setter sets adhoc_engine; other fields unchanged."""
    session = _make_session()
    session.update_editor_sql("SELECT 42")
    session.set_dialect("postgres")
    before = session.repl_state
    session.adhoc_engine = engine
    after = session.repl_state
    assert after.adhoc_engine == engine
    assert after.editor_sql == before.editor_sql
    assert after.dialect == before.dialect


@given(sql=_sql_st, dialect=_dialect_st, engine=_engine_st)
@settings(max_examples=30)
def test_all_setters_independent(sql: str, dialect: str, engine: str) -> None:
    """Each setter only changes its own field; all three can be set independently."""
    session = _make_session()
    session.update_editor_sql(sql)
    session.set_dialect(dialect)
    session.adhoc_engine = engine
    state = session.repl_state
    assert state.editor_sql == sql
    assert state.dialect == dialect
    assert state.adhoc_engine == engine
