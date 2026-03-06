"""Tests for ReplState read-only property and state mutation methods (task 2.2).

Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 2.1
"""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from rivet_core.assembly import Assembly
from rivet_core.interactive.session import InteractiveSession, SessionError
from rivet_core.interactive.types import ReplState
from rivet_core.models import ComputeEngine
from rivet_core.plugins import PluginRegistry


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


# --- repl_state property ---


def test_repl_state_returns_frozen_snapshot() -> None:
    session = _make_session(Path("."))
    state = session.repl_state
    assert isinstance(state, ReplState)
    with pytest.raises(FrozenInstanceError):
        state.editor_sql = "modified"  # type: ignore[misc]


def test_repl_state_returns_current_values() -> None:
    session = _make_session(Path("."))
    assert session.repl_state == ReplState()


# --- update_editor_sql ---


def test_update_editor_sql_updates_state() -> None:
    session = _make_session(Path("."))
    session.update_editor_sql("SELECT 1")
    assert session.repl_state.editor_sql == "SELECT 1"


def test_update_editor_sql_preserves_other_fields() -> None:
    session = _make_session(Path("."))
    session.adhoc_engine = "duckdb"
    session.set_dialect("postgres")
    session.update_editor_sql("SELECT 42")
    state = session.repl_state
    assert state.editor_sql == "SELECT 42"
    assert state.adhoc_engine == "duckdb"
    assert state.dialect == "postgres"


def test_update_editor_sql_persists(tmp_path: Path) -> None:
    rivet_dir = tmp_path / ".rivet"
    rivet_dir.mkdir()
    session = _make_session(tmp_path)
    session.update_editor_sql("SELECT 99")
    data = json.loads((rivet_dir / "repl-state.json").read_text())
    assert data["editor_sql"] == "SELECT 99"


# --- set_dialect ---


def test_set_dialect_updates_state() -> None:
    session = _make_session(Path("."))
    session.set_dialect("postgres")
    assert session.repl_state.dialect == "postgres"


def test_set_dialect_none_clears() -> None:
    session = _make_session(Path("."))
    session.set_dialect("postgres")
    session.set_dialect(None)
    assert session.repl_state.dialect is None


def test_set_dialect_invalid_raises() -> None:
    session = _make_session(Path("."))
    with pytest.raises(SessionError, match="Unknown dialect"):
        session.set_dialect("not_a_real_dialect_xyz")


def test_set_dialect_preserves_other_fields() -> None:
    session = _make_session(Path("."))
    session.update_editor_sql("SELECT 1")
    session.adhoc_engine = "duckdb"
    session.set_dialect("spark")
    state = session.repl_state
    assert state.editor_sql == "SELECT 1"
    assert state.adhoc_engine == "duckdb"
    assert state.dialect == "spark"


def test_set_dialect_persists(tmp_path: Path) -> None:
    rivet_dir = tmp_path / ".rivet"
    rivet_dir.mkdir()
    session = _make_session(tmp_path)
    session.set_dialect("bigquery")
    data = json.loads((rivet_dir / "repl-state.json").read_text())
    assert data["dialect"] == "bigquery"


# --- adhoc_engine setter syncs repl_state ---


def test_adhoc_engine_setter_updates_repl_state() -> None:
    session = _make_session(Path("."))
    session.adhoc_engine = "spark"
    assert session.repl_state.adhoc_engine == "spark"


def test_adhoc_engine_setter_none_updates_repl_state() -> None:
    session = _make_session(Path("."))
    session.adhoc_engine = "spark"
    session.adhoc_engine = None
    assert session.repl_state.adhoc_engine is None


def test_adhoc_engine_setter_persists(tmp_path: Path) -> None:
    rivet_dir = tmp_path / ".rivet"
    rivet_dir.mkdir()
    session = _make_session(tmp_path)
    session.adhoc_engine = "duckdb"
    data = json.loads((rivet_dir / "repl-state.json").read_text())
    assert data["adhoc_engine"] == "duckdb"


def test_adhoc_engine_setter_preserves_other_fields() -> None:
    session = _make_session(Path("."))
    session.update_editor_sql("SELECT 1")
    session.set_dialect("postgres")
    session.adhoc_engine = "spark"
    state = session.repl_state
    assert state.editor_sql == "SELECT 1"
    assert state.dialect == "postgres"
    assert state.adhoc_engine == "spark"


# --- switch_profile resets engine/dialect ---


def test_switch_profile_resets_engine_and_dialect() -> None:
    session = _make_session(Path("."))
    session.start()
    session.adhoc_engine = "spark"
    session.set_dialect("postgres")
    session.update_editor_sql("SELECT 1")
    session.switch_profile("default")
    state = session.repl_state
    assert state.adhoc_engine is None
    assert state.dialect is None
    assert state.editor_sql == "SELECT 1"


def test_switch_profile_persists_reset(tmp_path: Path) -> None:
    rivet_dir = tmp_path / ".rivet"
    rivet_dir.mkdir()
    session = _make_session(tmp_path)
    session.start()
    session.adhoc_engine = "duckdb"
    session.set_dialect("spark")
    session.update_editor_sql("SELECT 42")
    session.switch_profile("default")
    data = json.loads((rivet_dir / "repl-state.json").read_text())
    assert data["adhoc_engine"] is None
    assert data["dialect"] is None
    assert data["editor_sql"] == "SELECT 42"


# --- format_sql uses ReplState dialect (task 3.2) ---


def test_format_sql_uses_repl_state_dialect() -> None:
    """format_sql() without explicit dialect uses repl_state.dialect."""
    session = _make_session(Path("."))
    session.set_dialect("spark")
    sql = "select 1"
    assert session.format_sql(sql) == session.format_sql(sql, dialect="spark")


def test_format_sql_explicit_dialect_overrides_repl_state() -> None:
    """Explicit dialect argument takes precedence over repl_state.dialect."""
    session = _make_session(Path("."))
    session.set_dialect("spark")
    sql = "select 1"
    assert session.format_sql(sql, dialect="postgres") == session.format_sql(sql, dialect="postgres")


def test_format_sql_no_dialect_uses_none_when_repl_state_unset() -> None:
    """format_sql() without dialect and no repl_state.dialect uses None (default behavior)."""
    session = _make_session(Path("."))
    sql = "select 1"
    assert session.format_sql(sql) == session.format_sql(sql, dialect=None)
