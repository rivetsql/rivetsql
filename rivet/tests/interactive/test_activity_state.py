# Feature: repl-ux-improvements, Property 3: Activity state transitions
"""Property tests for Activity_State transitions in InteractiveSession."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.assembly import Assembly
from rivet_core.interactive.session import InteractiveSession
from rivet_core.interactive.types import Activity_State
from rivet_core.models import Joint
from rivet_core.plugins import PluginRegistry


def _make_session() -> InteractiveSession:
    """Create a minimal session for testing activity state transitions."""
    session = InteractiveSession(project_path=Path("/tmp/test"))
    # Provide minimal objects so compile() works
    source = Joint(name="src", joint_type="source", engine="test_engine")
    sink = Joint(name="snk", joint_type="sink", upstream=["src"], engine="test_engine")
    assembly = Assembly([source, sink])

    engine = MagicMock()
    engine.name = "test_engine"
    engine.engine_type = "duckdb"
    engine.options = {}

    registry = PluginRegistry()
    session.init_from(
        assembly=assembly,
        catalogs={},
        engines={"test_engine": engine},
        registry=registry,
        default_engine="test_engine",
    )
    return session


def test_compile_transitions() -> None:
    """compile() transitions IDLE → COMPILING → IDLE."""
    session = _make_session()
    transitions: list[Activity_State] = []
    session.on_activity_change = lambda s: transitions.append(s)

    session.compile()

    assert transitions == [Activity_State.COMPILING, Activity_State.IDLE]
    assert session.activity_state == Activity_State.IDLE


def test_compile_failure_returns_to_idle() -> None:
    """compile() returns to IDLE even on failure."""
    session = InteractiveSession(project_path=Path("/tmp/test"))
    transitions: list[Activity_State] = []
    session.on_activity_change = lambda s: transitions.append(s)

    try:
        session.compile()
    except Exception:
        pass

    assert session.activity_state == Activity_State.IDLE
    # Should have at least attempted COMPILING (if it got that far)
    # or stayed IDLE if it failed before setting state


def test_execute_joint_transitions() -> None:
    """execute_joint() transitions IDLE → EXECUTING → IDLE."""
    session = _make_session()
    session.start()
    transitions: list[Activity_State] = []
    session.on_activity_change = lambda s: transitions.append(s)

    session.execute_joint("src")

    assert transitions == [Activity_State.EXECUTING, Activity_State.IDLE]
    assert session.activity_state == Activity_State.IDLE


def test_execute_pipeline_transitions() -> None:
    """execute_pipeline() transitions IDLE → EXECUTING → IDLE."""
    session = _make_session()
    session.start()
    transitions: list[Activity_State] = []
    session.on_activity_change = lambda s: transitions.append(s)

    session.execute_pipeline()

    assert transitions == [Activity_State.EXECUTING, Activity_State.IDLE]
    assert session.activity_state == Activity_State.IDLE


@settings(max_examples=100)
@given(st.sampled_from(["compile", "execute_joint", "execute_pipeline"]))
def test_always_returns_to_idle(method_name: str) -> None:
    """Property 3: Every compile/execute call returns to IDLE."""
    session = _make_session()
    session.start()
    transitions: list[Activity_State] = []
    session.on_activity_change = lambda s: transitions.append(s)

    try:
        if method_name == "compile":
            session.compile()
        elif method_name == "execute_joint":
            session.execute_joint("src")
        elif method_name == "execute_pipeline":
            session.execute_pipeline()
    except Exception:
        pass

    assert session.activity_state == Activity_State.IDLE
    assert len(transitions) >= 2
    assert transitions[-1] == Activity_State.IDLE


def test_callback_exception_suppressed() -> None:
    """on_activity_change exceptions are caught and suppressed."""
    session = _make_session()

    def bad_callback(state: Activity_State) -> None:
        raise RuntimeError("boom")

    session.on_activity_change = bad_callback

    # Should not raise
    session.compile()
    assert session.activity_state == Activity_State.IDLE


def test_initial_state_is_idle() -> None:
    session = InteractiveSession(project_path=Path("/tmp/test"))
    assert session.activity_state == Activity_State.IDLE
