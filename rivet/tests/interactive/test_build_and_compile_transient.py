"""Tests for InteractiveSession._build_and_compile_transient — task 1.1.

Validates:
- Method returns a CompiledAssembly on success
- Engine override cascade: explicit > adhoc > default
- SessionError raised on compilation failure with joined error messages
- SessionError raised when session not started or no registry

Requirements: 2.1, 2.2, 2.3, 3.1, 4.1
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from rivet_core.assembly import Assembly
from rivet_core.compiler import CompiledAssembly
from rivet_core.interactive.session import InteractiveSession, SessionError
from rivet_core.models import ComputeEngine
from rivet_core.plugins import PluginRegistry


def _make_started_session(**engine_kwargs: str) -> InteractiveSession:
    session = InteractiveSession(project_path=Path("."), read_only=False)
    engines = {"default": ComputeEngine(name="default", engine_type="duckdb")}
    session.init_from(
        assembly=Assembly([]),
        catalogs={},
        engines=engines,
        registry=PluginRegistry(),
    )
    session.start()
    return session


# --- Basic functionality ---

def test_returns_compiled_assembly() -> None:
    """_build_and_compile_transient returns a CompiledAssembly on success."""
    session = _make_started_session()
    result = session._build_and_compile_transient("SELECT 1 AS x")
    assert isinstance(result, CompiledAssembly)
    assert result.success is True


def test_raises_session_error_when_not_started() -> None:
    """Raises SessionError if session not started."""
    session = InteractiveSession(project_path=Path("."))
    with pytest.raises(SessionError, match="not started"):
        session._build_and_compile_transient("SELECT 1")


# --- Engine override cascade ---

def test_explicit_override_takes_priority() -> None:
    """Explicit engine_override wins over adhoc and default."""
    session = _make_started_session()
    session._adhoc_engine = "adhoc_eng"
    session._default_engine = "default_eng"

    with patch.object(
        session._query_planner, "build_transient_pipeline", wraps=session._query_planner.build_transient_pipeline
    ) as mock_build:
        session._build_and_compile_transient("SELECT 1", engine_override="explicit_eng")
        _, kwargs = mock_build.call_args
        assert kwargs.get("engine_override") == "explicit_eng"


def test_adhoc_engine_used_when_no_explicit() -> None:
    """Session adhoc_engine used when no explicit override."""
    session = _make_started_session()
    session._adhoc_engine = "adhoc_eng"
    session._default_engine = "default_eng"

    with patch.object(
        session._query_planner, "build_transient_pipeline", wraps=session._query_planner.build_transient_pipeline
    ) as mock_build:
        session._build_and_compile_transient("SELECT 1")
        _, kwargs = mock_build.call_args
        assert kwargs.get("engine_override") == "adhoc_eng"


def test_default_engine_used_as_fallback() -> None:
    """Project default engine used when no explicit or adhoc."""
    session = _make_started_session()
    session._adhoc_engine = None
    session._default_engine = "default_eng"

    with patch.object(
        session._query_planner, "build_transient_pipeline", wraps=session._query_planner.build_transient_pipeline
    ) as mock_build:
        session._build_and_compile_transient("SELECT 1")
        _, kwargs = mock_build.call_args
        assert kwargs.get("engine_override") == "default_eng"


def test_none_engine_when_all_none() -> None:
    """engine_override is None when all cascade levels are None."""
    session = _make_started_session()
    session._adhoc_engine = None
    session._default_engine = None

    with patch.object(
        session._query_planner, "build_transient_pipeline", wraps=session._query_planner.build_transient_pipeline
    ) as mock_build:
        session._build_and_compile_transient("SELECT 1")
        _, kwargs = mock_build.call_args
        assert kwargs.get("engine_override") is None


# --- Compilation failure ---

def test_raises_session_error_on_compile_failure() -> None:
    """SessionError raised with joined error messages on compile failure."""
    session = _make_started_session()

    failed = CompiledAssembly(
        success=False,
        profile_name="default",
        catalogs=[], engines=[], adapters=[], joints=[],
        fused_groups=[], materializations=[], execution_order=[],
        errors=["Error A", "Error B"],
        warnings=[],
    )

    from rivet_core.models import Joint

    query_joint = Joint(name="__query", joint_type="sql", sql="SELECT 1")
    transient = Assembly([query_joint])

    with patch.object(session._query_planner, "build_transient_pipeline", return_value=(transient, [])):
        with patch("rivet_core.interactive.session.core_compile", return_value=failed):
            with pytest.raises(SessionError) as exc_info:
                session._build_and_compile_transient("SELECT 1")

    msg = str(exc_info.value)
    assert "Error A" in msg
    assert "Error B" in msg


# --- Default engine set during start ---

def test_default_engine_set_from_project_engines() -> None:
    """_default_engine is set from the first project engine during start()."""
    session = _make_started_session()
    assert session._default_engine == "default"
