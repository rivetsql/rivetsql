"""Unit tests for REPL-level error handling — task 5.

Verifies:
- ExecutionError propagates directly from Executor without wrapping (Requirement 4.3)
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from rivet_core.assembly import Assembly
from rivet_core.errors import ExecutionError, RivetError
from rivet_core.interactive.session import InteractiveSession
from rivet_core.models import ComputeEngine, Joint
from rivet_core.plugins import PluginRegistry


def _make_started_session() -> InteractiveSession:
    session = InteractiveSession(project_path=Path("."), read_only=False)
    registry = PluginRegistry()
    assembly = Assembly([])
    session.init_from(
        assembly=assembly,
        catalogs={},
        engines={"default": ComputeEngine(name="default", engine_type="duckdb")},
        registry=registry,
    )
    session.start()
    return session


def test_execution_error_propagates_without_wrapping() -> None:
    """ExecutionError from Executor.run_query propagates through execute_query unchanged."""
    session = _make_started_session()

    from rivet_core.compiler import CompiledAssembly

    compiled = CompiledAssembly(
        success=True,
        profile_name="default",
        catalogs=[],
        engines=[],
        adapters=[],
        joints=[],
        fused_groups=[],
        materializations=[],
        execution_order=[],
        errors=[],
        warnings=[],
    )

    error = RivetError(code="RVT-501", message="Engine crashed")
    exec_error = ExecutionError(error)

    transient = Assembly([Joint(name="__query", joint_type="sql", sql="SELECT 1")])
    with patch.object(
        session._query_planner,
        "build_transient_pipeline",
        return_value=(transient, []),
    ), patch("rivet_core.interactive.session.core_compile", return_value=compiled):
        with patch("rivet_core.executor.Executor.run_query_with_stats", side_effect=exec_error):
            with pytest.raises(ExecutionError) as exc_info:
                session.execute_query("SELECT 1")

    assert exc_info.value is exec_error
    assert exc_info.value.error.code == "RVT-501"
