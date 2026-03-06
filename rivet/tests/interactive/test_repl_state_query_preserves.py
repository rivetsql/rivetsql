"""Property test: Query execution preserves state (task 2.4).

Property 2: For any ReplState values, after execute_query(), repl_state has
identical editor_sql, adhoc_engine, and dialect.

Validates: Requirements 1.2
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.assembly import Assembly
from rivet_core.interactive.session import InteractiveSession
from rivet_core.models import ComputeEngine
from rivet_core.plugins import PluginRegistry

_VALID_DIALECTS = ["duckdb", "spark", "postgres", "bigquery", "trino"]
_dialect_st = st.one_of(st.none(), st.sampled_from(_VALID_DIALECTS))
_sql_st = st.text(min_size=0, max_size=200)
_engine_st = st.one_of(st.none(), st.just("default"))

_MOCK_TABLE = pa.table({"x": [1]})


def _make_session() -> InteractiveSession:
    from rivet_duckdb.engine import DuckDBComputeEnginePlugin

    session = InteractiveSession(project_path=Path("."))
    registry = PluginRegistry()
    registry.register_engine_plugin(DuckDBComputeEnginePlugin())
    session.init_from(
        assembly=Assembly([]),
        catalogs={},
        engines={"default": ComputeEngine(name="default", engine_type="duckdb")},
        registry=registry,
    )
    session.start()
    return session


@given(
    editor_sql=_sql_st,
    engine=_engine_st,
    dialect=_dialect_st,
)
@settings(max_examples=50)
def test_execute_query_preserves_repl_state(
    editor_sql: str,
    engine: str | None,
    dialect: str | None,
) -> None:
    """execute_query() does not mutate editor_sql, adhoc_engine, or dialect."""
    session = _make_session()
    session.update_editor_sql(editor_sql)
    if engine is not None:
        session.adhoc_engine = engine
    if dialect is not None:
        session.set_dialect(dialect)

    before = session.repl_state

    with patch("rivet_core.executor.Executor.run_query", return_value=_MOCK_TABLE):
        session.execute_query("SELECT 1")

    after = session.repl_state
    assert after.editor_sql == before.editor_sql
    assert after.adhoc_engine == before.adhoc_engine
    assert after.dialect == before.dialect
