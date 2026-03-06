"""Property test: get_joint_sql returns SQL for compiled joints (task 5.5).

Property 10: For any compiled joint with SQL, get_joint_sql(name) returns
non-empty string; for missing joints, raises SessionError.

Validates: Requirements 6.6
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
_sql_st = st.text(min_size=1, max_size=200, alphabet=st.characters(blacklist_categories=("Cs",)))


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


@given(name=_name_st, sql=_sql_st)
@settings(max_examples=50)
def test_get_joint_sql_returns_sql_for_compiled_joint(name: str, sql: str) -> None:
    """get_joint_sql returns the SQL string for any joint that has SQL."""
    with tempfile.TemporaryDirectory() as tmpdir:
        joint = Joint(name=name, joint_type="sql", sql=sql)
        session = _make_session(Path(tmpdir), joints=[joint])
        result = session.get_joint_sql(name)
        assert isinstance(result, str)
        assert len(result) > 0
        assert result == sql


@given(name=_name_st)
@settings(max_examples=50)
def test_get_joint_sql_raises_for_missing_joint(name: str) -> None:
    """get_joint_sql raises SessionError when the joint name is not in the assembly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        session = _make_session(Path(tmpdir), joints=[])
        with pytest.raises(SessionError, match="not found in assembly"):
            session.get_joint_sql(name)
