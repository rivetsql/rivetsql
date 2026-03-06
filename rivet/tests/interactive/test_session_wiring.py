"""Unit tests for InteractiveSession wiring — task 5.2.

Verifies:
- execute_query() propagates compilation errors as SessionError with messages
- execute_query() propagates resolution ValueError from resolver

Validates: Requirements 6.4, 10.3
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from rivet_core.assembly import Assembly
from rivet_core.interactive.session import InteractiveSession, SessionError
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


# ---------------------------------------------------------------------------
# Compilation errors propagated as SessionError (Requirement 6.4)
# ---------------------------------------------------------------------------

def test_execute_query_raises_session_error_on_compile_failure() -> None:
    """Compilation failure raises SessionError containing the error messages."""
    session = _make_started_session()

    from rivet_core.compiler import CompiledAssembly

    failed_assembly = CompiledAssembly(
        success=False,
        profile_name="default",
        catalogs=[],
        engines=[],
        adapters=[],
        joints=[],
        fused_groups=[],
        materializations=[],
        execution_order=[],
        errors=["Unknown joint 'missing_table'", "Engine not found"],
        warnings=[],
    )

    # Patch build_transient_pipeline to bypass resolver, then patch compile to fail
    transient = Assembly([Joint(name="__query", joint_type="sql", sql="SELECT 1")])
    with patch.object(
        session._query_planner,
        "build_transient_pipeline",
        return_value=(transient, []),
    ), patch("rivet_core.interactive.session.core_compile", return_value=failed_assembly):
        with pytest.raises(SessionError) as exc_info:
            session.execute_query("SELECT * FROM missing_table")

    msg = str(exc_info.value)
    assert "Unknown joint 'missing_table'" in msg
    assert "Engine not found" in msg


def test_execute_query_session_error_message_contains_all_errors() -> None:
    """SessionError message includes all compilation error strings joined."""
    session = _make_started_session()

    from rivet_core.compiler import CompiledAssembly

    errors = ["Error A", "Error B", "Error C"]
    failed_assembly = CompiledAssembly(
        success=False,
        profile_name="default",
        catalogs=[],
        engines=[],
        adapters=[],
        joints=[],
        fused_groups=[],
        materializations=[],
        execution_order=[],
        errors=errors,
        warnings=[],
    )

    transient = Assembly([Joint(name="__query", joint_type="sql", sql="SELECT 1")])
    with patch.object(
        session._query_planner,
        "build_transient_pipeline",
        return_value=(transient, []),
    ), patch("rivet_core.interactive.session.core_compile", return_value=failed_assembly):
        with pytest.raises(SessionError) as exc_info:
            session.execute_query("SELECT 1")

    msg = str(exc_info.value)
    for err in errors:
        assert err in msg


# ---------------------------------------------------------------------------
# Resolution ValueError propagated (Requirement 10.3)
# ---------------------------------------------------------------------------

def test_execute_query_propagates_resolver_value_error() -> None:
    """ValueError from resolver is wrapped in SessionError by _build_and_compile_transient."""
    session = _make_started_session()

    with patch.object(
        session._query_planner,
        "build_transient_pipeline",
        side_effect=ValueError("Cannot resolve 'unknown_tbl': no matching table found"),
    ), pytest.raises(SessionError, match="Cannot resolve 'unknown_tbl'"):
        session.execute_query("SELECT * FROM unknown_tbl")


def test_execute_query_propagates_ambiguous_resolver_error() -> None:
    """Ambiguous reference ValueError from resolver is wrapped in SessionError."""
    session = _make_started_session()

    with patch.object(
        session._query_planner,
        "build_transient_pipeline",
        side_effect=ValueError("Ambiguous reference 'orders': matches cat1.s.orders, cat2.s.orders"),
    ):
        with pytest.raises(SessionError, match="Ambiguous reference 'orders'"):
            session.execute_query("SELECT * FROM orders")
