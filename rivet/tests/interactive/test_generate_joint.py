"""Tests for generate_joint() method on InteractiveSession (task 5.1).

Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7
"""

from __future__ import annotations

from pathlib import Path

import pytest

from rivet_core.assembly import Assembly
from rivet_core.interactive.session import InteractiveSession, ReadOnlyError, SessionError
from rivet_core.models import ComputeEngine, Joint
from rivet_core.plugins import PluginRegistry


def _make_session(
    project_path: Path,
    *,
    read_only: bool = False,
    joints: list[Joint] | None = None,
) -> InteractiveSession:
    session = InteractiveSession(project_path=project_path, read_only=read_only)
    assembly = Assembly(joints or [])
    session.init_from(
        assembly=assembly,
        catalogs={},
        engines={
            "duckdb": ComputeEngine(name="duckdb", engine_type="duckdb"),
        },
        registry=PluginRegistry(),
        default_engine="duckdb",
    )
    session.start()
    return session


def _simulate_query(session: InteractiveSession, sql: str = "SELECT 1", engine: str = "duckdb", upstream: list[str] | None = None) -> None:
    """Simulate a query execution by setting internal tracking fields."""
    session._last_query_sql = sql
    session._last_query_engine = engine
    session._last_query_upstream = upstream or []


class TestGenerateJointValidation:
    """Validation checks before file creation."""

    def test_invalid_name_raises(self, tmp_path: Path) -> None:
        session = _make_session(tmp_path)
        _simulate_query(session)
        with pytest.raises(SessionError, match="valid Python identifier"):
            session.generate_joint("not-valid")

    def test_invalid_name_with_spaces_raises(self, tmp_path: Path) -> None:
        session = _make_session(tmp_path)
        _simulate_query(session)
        with pytest.raises(SessionError, match="valid Python identifier"):
            session.generate_joint("has space")

    def test_no_query_executed_raises(self, tmp_path: Path) -> None:
        session = _make_session(tmp_path)
        with pytest.raises(SessionError, match="No executed query"):
            session.generate_joint("my_joint")

    def test_duplicate_name_raises(self, tmp_path: Path) -> None:
        existing = Joint(name="existing_joint", joint_type="sql", sql="SELECT 1")
        session = _make_session(tmp_path, joints=[existing])
        _simulate_query(session)
        with pytest.raises(SessionError, match="already exists"):
            session.generate_joint("existing_joint")

    def test_read_only_raises(self, tmp_path: Path) -> None:
        session = _make_session(tmp_path, read_only=True)
        session._last_query_sql = "SELECT 1"
        with pytest.raises(ReadOnlyError):
            session.generate_joint("my_joint")


class TestGenerateJointFileCreation:
    """File creation and content checks."""

    def test_creates_file_in_joints_dir(self, tmp_path: Path) -> None:
        (tmp_path / "rivet.yaml").write_text("joints: joints\n")
        session = _make_session(tmp_path)
        _simulate_query(session)
        path = session.generate_joint("my_joint")
        assert path.exists()
        assert path.name == "my_joint.sql"
        assert path.parent.name == "joints"

    def test_file_contains_sql(self, tmp_path: Path) -> None:
        (tmp_path / "rivet.yaml").write_text("joints: joints\n")
        session = _make_session(tmp_path)
        _simulate_query(session, sql="SELECT id, name FROM users")
        path = session.generate_joint("user_query")
        content = path.read_text()
        assert "SELECT id, name FROM users" in content

    def test_file_contains_engine_annotation(self, tmp_path: Path) -> None:
        (tmp_path / "rivet.yaml").write_text("joints: joints\n")
        session = _make_session(tmp_path)
        _simulate_query(session, engine="duckdb")
        path = session.generate_joint("my_joint")
        content = path.read_text()
        assert "-- rivet:engine: duckdb" in content

    def test_file_contains_upstream_annotation(self, tmp_path: Path) -> None:
        (tmp_path / "rivet.yaml").write_text("joints: joints\n")
        session = _make_session(tmp_path)
        _simulate_query(session, upstream=["raw_orders", "raw_customers"])
        path = session.generate_joint("my_joint")
        content = path.read_text()
        assert "-- rivet:upstream: [raw_orders, raw_customers]" in content

    def test_file_contains_empty_upstream_when_none(self, tmp_path: Path) -> None:
        (tmp_path / "rivet.yaml").write_text("joints: joints\n")
        session = _make_session(tmp_path)
        _simulate_query(session, upstream=[])
        path = session.generate_joint("my_joint")
        content = path.read_text()
        assert "-- rivet:upstream: []" in content

    def test_file_contains_description_annotation(self, tmp_path: Path) -> None:
        (tmp_path / "rivet.yaml").write_text("joints: joints\n")
        session = _make_session(tmp_path)
        _simulate_query(session)
        path = session.generate_joint("my_joint", description="Aggregated totals")
        content = path.read_text()
        assert "-- rivet:description: Aggregated totals" in content

    def test_file_omits_description_when_none(self, tmp_path: Path) -> None:
        (tmp_path / "rivet.yaml").write_text("joints: joints\n")
        session = _make_session(tmp_path)
        _simulate_query(session)
        path = session.generate_joint("my_joint")
        content = path.read_text()
        assert "rivet:description" not in content

    def test_returns_path(self, tmp_path: Path) -> None:
        (tmp_path / "rivet.yaml").write_text("joints: joints\n")
        session = _make_session(tmp_path)
        _simulate_query(session)
        result = session.generate_joint("my_joint")
        assert isinstance(result, Path)
        assert result.exists()


class TestGenerateJointRecompilation:
    """Verify recompilation is triggered after file creation."""

    def test_triggers_recompilation(self, tmp_path: Path) -> None:
        (tmp_path / "rivet.yaml").write_text("joints: joints\n")
        session = _make_session(tmp_path)
        _simulate_query(session)
        compilations_before = session.metrics["compilations"]
        session.generate_joint("my_joint")
        assert session.metrics["compilations"] > compilations_before


class TestResolveJointsDir:
    """Tests for _resolve_joints_dir helper."""

    def test_reads_from_rivet_yaml(self, tmp_path: Path) -> None:
        (tmp_path / "rivet.yaml").write_text("joints: custom_joints\n")
        session = _make_session(tmp_path)
        assert session._resolve_joints_dir() == tmp_path / "custom_joints"

    def test_defaults_to_joints_when_no_manifest(self, tmp_path: Path) -> None:
        session = _make_session(tmp_path)
        assert session._resolve_joints_dir() == tmp_path / "joints"

    def test_creates_joints_dir_if_missing(self, tmp_path: Path) -> None:
        (tmp_path / "rivet.yaml").write_text("joints: joints\n")
        session = _make_session(tmp_path)
        _simulate_query(session)
        session.generate_joint("my_joint")
        assert (tmp_path / "joints").is_dir()
