"""Property test: Profile switch resets engine and dialect, preserves editor (task 2.5).

Property 3: After switch_profile(), adhoc_engine == None, dialect == None,
editor_sql unchanged.

Validates: Requirements 1.5
"""

from __future__ import annotations

from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.assembly import Assembly
from rivet_core.interactive.session import InteractiveSession
from rivet_core.models import ComputeEngine
from rivet_core.plugins import PluginRegistry

_VALID_DIALECTS = ["duckdb", "spark", "postgres", "bigquery", "trino"]
_dialect_st = st.one_of(st.none(), st.sampled_from(_VALID_DIALECTS))
_sql_st = st.text(min_size=0, max_size=200)
_engine_st = st.one_of(st.none(), st.just("duckdb"), st.just("spark"))


def _make_session() -> InteractiveSession:
    session = InteractiveSession(project_path=Path("."))
    session.init_from(
        assembly=Assembly([]),
        catalogs={},
        engines={
            "duckdb": ComputeEngine(name="duckdb", engine_type="duckdb"),
            "spark": ComputeEngine(name="spark", engine_type="spark"),
        },
        registry=PluginRegistry(),
    )
    session.start()
    return session


@given(editor_sql=_sql_st, engine=_engine_st, dialect=_dialect_st)
@settings(max_examples=50)
def test_switch_profile_resets_engine_and_dialect_preserves_editor(
    editor_sql: str,
    engine: str | None,
    dialect: str | None,
) -> None:
    """After switch_profile(), adhoc_engine and dialect are None; editor_sql unchanged."""
    session = _make_session()
    session.update_editor_sql(editor_sql)
    if engine is not None:
        session.adhoc_engine = engine
    if dialect is not None:
        session.set_dialect(dialect)

    session.switch_profile("default")

    state = session.repl_state
    assert state.adhoc_engine is None
    assert state.dialect is None
    assert state.editor_sql == editor_sql
