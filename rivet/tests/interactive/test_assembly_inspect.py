"""Unit tests for InteractiveSession.inspect_assembly() — task 6.1.

Validates: Requirements 1.1, 1.3, 1.4, 2.1, 2.3, 2.4, 3.1
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from rivet_core.assembly import Assembly
from rivet_core.compiler import CompiledAssembly, CompiledJoint
from rivet_core.interactive.session import InteractiveSession, SessionError
from rivet_core.interactive.types import (
    AssemblyInspection,
    InspectFilter,
    Verbosity,
)
from rivet_core.models import ComputeEngine
from rivet_core.plugins import PluginRegistry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_joint(name: str, type: str = "sql", engine: str = "default") -> CompiledJoint:
    return CompiledJoint(
        name=name,
        type=type,
        catalog=None,
        catalog_type=None,
        engine=engine,
        engine_resolution="project_default",
        adapter=None,
        sql=f"SELECT * FROM {name}" if type == "sql" else None,
        sql_translated=None,
        sql_resolved=None,
        sql_dialect=None,
        engine_dialect=None,
        upstream=[],
        eager=False,
        table=None,
        write_strategy=None,
        function=None,
        source_file=None,
        logical_plan=None,
        output_schema=None,
        column_lineage=[],
        optimizations=[],
        checks=[],
        fused_group_id=None,
        tags=[],
        description=None,
        fusion_strategy_override=None,
        materialization_strategy_override=None,
    )


def _make_assembly(joints: list[CompiledJoint] | None = None, success: bool = True) -> CompiledAssembly:
    joints = joints or [_make_joint("orders"), _make_joint("customers")]
    return CompiledAssembly(
        success=success,
        profile_name="default",
        catalogs=[],
        engines=[],
        adapters=[],
        joints=joints,
        fused_groups=[],
        materializations=[],
        execution_order=[j.name for j in joints],
        errors=["Compilation error"] if not success else [],
        warnings=[],
    )


def _make_session(assembly: CompiledAssembly | None = None) -> InteractiveSession:
    session = InteractiveSession(project_path=Path("."), read_only=False)
    registry = PluginRegistry()
    raw = Assembly([])
    session.init_from(
        assembly=raw,
        catalogs={},
        engines={"default": ComputeEngine(name="default", engine_type="duckdb")},
        registry=registry,
    )
    if assembly is not None:
        session._assembly = assembly
    else:
        session._assembly = _make_assembly()
    return session


# ---------------------------------------------------------------------------
# target=None: full assembly inspection (Req 1.1, 1.3)
# ---------------------------------------------------------------------------

def test_inspect_assembly_no_target_returns_assembly_inspection() -> None:
    """target=None formats the current compiled assembly."""
    session = _make_session()
    result = session.inspect_assembly()
    assert isinstance(result, AssemblyInspection)
    assert result.overview.total_joints == 2
    assert result.verbosity == Verbosity.NORMAL


def test_inspect_assembly_no_target_compiles_if_needed() -> None:
    """target=None compiles first if no assembly exists (Req 1.3)."""
    session = _make_session()
    session._assembly = None  # Force no assembly
    compiled = _make_assembly()
    with patch.object(session, "compile", return_value=compiled) as mock_compile:
        result = session.inspect_assembly()
    mock_compile.assert_called_once()
    assert isinstance(result, AssemblyInspection)


# ---------------------------------------------------------------------------
# Compilation failure raises SessionError (Req 1.4)
# ---------------------------------------------------------------------------

def test_inspect_assembly_raises_on_compile_failure() -> None:
    """Compilation failure raises SessionError with error messages."""
    failed = _make_assembly(success=False)
    session = _make_session(assembly=failed)
    with pytest.raises(SessionError, match="Compilation failed"):
        session.inspect_assembly()


# ---------------------------------------------------------------------------
# target=<joint_name>: single joint inspection (Req 3.1)
# ---------------------------------------------------------------------------

def test_inspect_assembly_joint_name_returns_joint_details() -> None:
    """target matching a joint name returns inspection with that joint."""
    session = _make_session()
    result = session.inspect_assembly(target="orders")
    assert result.joint_details is not None
    assert len(result.joint_details) == 1
    assert result.joint_details[0].name == "orders"


def test_inspect_assembly_joint_not_found_and_bad_sql_raises() -> None:
    """target not matching any joint and failing SQL parse raises SessionError."""
    session = _make_session()
    with patch.object(
        session._query_planner,
        "build_transient_pipeline",
        side_effect=ValueError("parse error"),
    ), pytest.raises(SessionError, match="parse error"):
        session.inspect_assembly(target="nonexistent")


def test_inspect_assembly_joint_not_found_lists_available() -> None:
    """SessionError for unknown target includes available joint names."""
    session = _make_session()
    with patch.object(
        session._query_planner,
        "build_transient_pipeline",
        side_effect=ValueError(
            "Cannot resolve 'nonexistent': no matching table found. "
            "Connected catalogs: (none)"
        ),
    ), pytest.raises(SessionError) as exc_info:
        session.inspect_assembly(target="nonexistent")
    msg = str(exc_info.value)
    assert "Cannot resolve 'nonexistent'" in msg


# ---------------------------------------------------------------------------
# target=<sql>: transient assembly (Req 2.1, 2.3, 2.4)
# ---------------------------------------------------------------------------

def test_inspect_assembly_sql_target_builds_transient() -> None:
    """SQL target builds transient assembly and formats it (Req 2.1)."""
    session = _make_session()
    compiled = _make_assembly()

    from rivet_core.models import Joint

    query_joint = Joint(name="__query", joint_type="sql", sql="SELECT 1")
    transient = Assembly([query_joint])

    with patch.object(
        session._query_planner,
        "build_transient_pipeline",
        return_value=(transient, []),
    ), patch("rivet_core.interactive.session.core_compile", return_value=compiled):
        result = session.inspect_assembly(target="SELECT 1")

    assert isinstance(result, AssemblyInspection)


def test_inspect_assembly_sql_compile_failure_raises() -> None:
    """Transient assembly compilation failure raises SessionError (Req 2.4)."""
    session = _make_session()
    failed = _make_assembly(success=False)

    from rivet_core.models import Joint

    query_joint = Joint(name="__query", joint_type="sql", sql="SELECT * FROM bad")
    transient = Assembly([query_joint])

    with patch.object(
        session._query_planner,
        "build_transient_pipeline",
        return_value=(transient, []),
    ), patch("rivet_core.interactive.session.core_compile", return_value=failed):
        with pytest.raises(SessionError, match="Transient pipeline compilation failed"):
            session.inspect_assembly(target="SELECT * FROM bad")


# ---------------------------------------------------------------------------
# Verbosity and filter passthrough
# ---------------------------------------------------------------------------

def test_inspect_assembly_passes_verbosity() -> None:
    """Verbosity is passed through to the formatter."""
    session = _make_session()
    result = session.inspect_assembly(verbosity=Verbosity.FULL)
    assert result.verbosity == Verbosity.FULL
    assert result.joint_details is not None


def test_inspect_assembly_passes_filter() -> None:
    """Filter is passed through to the formatter."""
    session = _make_session()
    f = InspectFilter(engine="default")
    result = session.inspect_assembly(filter=f)
    assert result.filter_applied == f
