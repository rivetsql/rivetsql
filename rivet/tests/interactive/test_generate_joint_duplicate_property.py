"""Property test: generate_joint rejects duplicate joint names (task 5.4).

Property 7: For any name already in the assembly, generate_joint(name) raises
SessionError and creates no file.

Validates: Requirements 4.3
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.assembly import Assembly
from rivet_core.interactive.session import InteractiveSession, SessionError
from rivet_core.models import ComputeEngine, Joint
from rivet_core.plugins import PluginRegistry

_name_st = st.from_regex(r"[a-z][a-z0-9_]{0,19}", fullmatch=True)


def _make_session(project_path: Path, joints: list[Joint]) -> InteractiveSession:
    session = InteractiveSession(project_path=project_path)
    session.init_from(
        assembly=Assembly(joints),
        catalogs={},
        engines={"duckdb": ComputeEngine(name="duckdb", engine_type="duckdb")},
        registry=PluginRegistry(),
        default_engine="duckdb",
    )
    session.start()
    return session


@given(name=_name_st)
@settings(max_examples=50)
def test_generate_joint_rejects_duplicate(name: str) -> None:
    """generate_joint raises SessionError for names already in the assembly and creates no file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_path = Path(tmpdir)
        (project_path / "rivet.yaml").write_text("joints: joints\n")

        existing_joint = Joint(name=name, joint_type="sql", sql="SELECT 1")
        session = _make_session(project_path, joints=[existing_joint])
        session._last_query_sql = "SELECT 2"
        session._last_query_engine = "duckdb"
        session._last_query_upstream = []

        joints_dir = project_path / "joints"

        with pytest.raises(SessionError, match="already exists"):
            session.generate_joint(name)

        # No file should have been created
        assert not (joints_dir / f"{name}.sql").exists(), "No file must be created on duplicate name"
