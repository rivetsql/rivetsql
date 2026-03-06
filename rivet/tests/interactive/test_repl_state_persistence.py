"""Property test: State persistence round-trip (task 2.6).

Property 11: Persist state, create new session loading same file, verify
identical editor_sql, adhoc_engine, dialect.

Validates: Requirements 1.7, 1.8
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.assembly import Assembly
from rivet_core.interactive.session import InteractiveSession
from rivet_core.models import ComputeEngine
from rivet_core.plugins import PluginRegistry

_VALID_DIALECTS = ["duckdb", "spark", "postgres", "bigquery", "trino", "mysql", "sqlite"]

_dialect_st = st.sampled_from(_VALID_DIALECTS)
_sql_st = st.text(min_size=0, max_size=200)
_engine_st = st.sampled_from(["duckdb", "spark"])


def _make_session(project_path: Path) -> InteractiveSession:
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


@given(sql=_sql_st, engine=_engine_st, dialect=_dialect_st)
@settings(max_examples=50)
def test_persistence_round_trip(sql: str, engine: str, dialect: str) -> None:
    """Persisted state is restored identically by a new session."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_path = Path(tmpdir)
        rivet_dir = project_path / ".rivet"
        rivet_dir.mkdir()

        # First session: set state and persist
        session1 = _make_session(project_path)
        session1.update_editor_sql(sql)
        session1.adhoc_engine = engine
        session1.set_dialect(dialect)

        # Second session: load from same project path
        session2 = _make_session(project_path)
        session2.start()

        state = session2.repl_state
        assert state.editor_sql == sql
        assert state.adhoc_engine == engine
        assert state.dialect == dialect


@given(sql=_sql_st)
@settings(max_examples=30)
def test_persistence_missing_file_uses_defaults(sql: str) -> None:
    """Missing repl-state.json starts with default values without error."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_path = Path(tmpdir)
        # No .rivet/ directory — file won't exist
        session = _make_session(project_path)
        session.start()

        state = session.repl_state
        assert state.editor_sql == ""
        assert state.adhoc_engine is None
        assert state.dialect is None


@given(sql=_sql_st, engine=_engine_st, dialect=_dialect_st)
@settings(max_examples=30)
def test_persistence_corrupt_file_uses_defaults(sql: str, engine: str, dialect: str) -> None:
    """Corrupt repl-state.json starts with default values without error."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_path = Path(tmpdir)
        rivet_dir = project_path / ".rivet"
        rivet_dir.mkdir()
        (rivet_dir / "repl-state.json").write_text("not valid json", encoding="utf-8")

        session = _make_session(project_path)
        session.start()

        state = session.repl_state
        assert state.editor_sql == ""
        assert state.adhoc_engine is None
        assert state.dialect is None
