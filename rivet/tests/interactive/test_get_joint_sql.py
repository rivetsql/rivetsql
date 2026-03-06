"""Tests for get_joint_sql() method on InteractiveSession (task 5.2).

Validates: Requirement 6.6
"""

from __future__ import annotations

from pathlib import Path

import pytest

from rivet_core.assembly import Assembly
from rivet_core.interactive.session import InteractiveSession, SessionError
from rivet_core.models import ComputeEngine, Joint
from rivet_core.plugins import PluginRegistry


def _make_session(project_path: Path, joints: list[Joint] | None = None) -> InteractiveSession:
    session = InteractiveSession(project_path=project_path)
    session.init_from(
        assembly=Assembly(joints or []),
        catalogs={},
        engines={"duckdb": ComputeEngine(name="duckdb", engine_type="duckdb")},
        registry=PluginRegistry(),
        default_engine="duckdb",
    )
    session.start()
    return session


class TestGetJointSql:
    def test_returns_sql_for_existing_joint(self, tmp_path: Path) -> None:
        joint = Joint(name="my_joint", joint_type="sql", sql="SELECT 1 AS x")
        session = _make_session(tmp_path, joints=[joint])
        result = session.get_joint_sql("my_joint")
        assert result == "SELECT 1 AS x"

    def test_raises_for_missing_joint(self, tmp_path: Path) -> None:
        session = _make_session(tmp_path)
        with pytest.raises(SessionError, match="not found in assembly"):
            session.get_joint_sql("nonexistent")

    def test_raises_for_joint_without_sql(self, tmp_path: Path) -> None:
        joint = Joint(name="source_joint", joint_type="source", catalog="my_catalog", table="my_table")
        session = _make_session(tmp_path, joints=[joint])
        with pytest.raises(SessionError, match="has no SQL content"):
            session.get_joint_sql("source_joint")

    def test_error_message_includes_joint_name(self, tmp_path: Path) -> None:
        session = _make_session(tmp_path)
        with pytest.raises(SessionError, match="missing_joint"):
            session.get_joint_sql("missing_joint")
