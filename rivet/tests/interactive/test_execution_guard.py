"""Tests for execution guard and progress callback wiring.

Feature: repl-query-guard
Validates: Requirements 1.1, 1.2, 1.3, 1.5, 3.1, 3.2, 3.3, 3.4, 5.1, 5.2, 5.3, 5.4
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from rivet_core.assembly import Assembly
from rivet_core.interactive.session import (
    ExecutionInProgressError,
    InteractiveSession,
    SessionError,
)
from rivet_core.interactive.types import Activity_State, QueryProgress
from rivet_core.models import ComputeEngine, Joint
from rivet_core.plugins import PluginRegistry


def _make_session() -> InteractiveSession:
    session = InteractiveSession(project_path=Path("."), read_only=False)
    registry = PluginRegistry()
    assembly = Assembly([Joint(name="j1", joint_type="sql", sql="SELECT 1")])
    session.init_from(
        assembly=assembly,
        catalogs={},
        engines={"default": ComputeEngine(name="default", engine_type="duckdb")},
        registry=registry,
    )
    session.start()
    return session


# --- ExecutionInProgressError ---

def test_execution_in_progress_error_is_session_error():
    assert issubclass(ExecutionInProgressError, SessionError)


def test_execution_in_progress_error_message():
    err = ExecutionInProgressError("test message")
    assert "test message" in str(err)


# --- _execution_guard: basic lifecycle ---

def test_guard_sets_executing_on_entry():
    session = _make_session()
    assert session.activity_state == Activity_State.IDLE
    with session._execution_guard():
        assert session.activity_state == Activity_State.EXECUTING


def test_guard_sets_idle_on_exit():
    session = _make_session()
    with session._execution_guard():
        pass
    assert session.activity_state == Activity_State.IDLE


def test_guard_sets_idle_on_exception():
    session = _make_session()
    with pytest.raises(RuntimeError), session._execution_guard():
        raise RuntimeError("boom")
    assert session.activity_state == Activity_State.IDLE


def test_guard_releases_lock_on_exception():
    session = _make_session()
    with pytest.raises(RuntimeError), session._execution_guard():
        raise RuntimeError("boom")
    with session._execution_guard():
        assert session.activity_state == Activity_State.EXECUTING


# --- _execution_guard: concurrency rejection ---

def test_guard_rejects_concurrent_acquisition():
    session = _make_session()
    entered = threading.Event()
    release = threading.Event()

    def hold_guard():
        with session._execution_guard():
            entered.set()
            release.wait(timeout=5)

    t = threading.Thread(target=hold_guard)
    t.start()
    entered.wait(timeout=5)

    with pytest.raises(ExecutionInProgressError, match="Execution in progress"):
        with session._execution_guard():
            pass

    release.set()
    t.join(timeout=5)


def test_guard_allows_sequential_acquisition():
    session = _make_session()
    with session._execution_guard():
        pass
    with session._execution_guard():
        assert session.activity_state == Activity_State.EXECUTING


# --- _execution_guard: activity callback ---

def test_guard_invokes_activity_callback():
    session = _make_session()
    states: list[Activity_State] = []
    session.on_activity_change = lambda s: states.append(s)

    with session._execution_guard():
        pass

    assert states == [Activity_State.EXECUTING, Activity_State.IDLE]


# --- _exec_lock exists ---

def test_exec_lock_is_threading_lock():
    session = _make_session()
    assert isinstance(session._exec_lock, type(threading.Lock()))


# ---------------------------------------------------------------------------
# Property 1: Guard rejects concurrent execution actions (Req 1.1, 1.3, 1.5)
# ---------------------------------------------------------------------------


def test_guard_rejects_concurrent_execute_joint() -> None:
    """Second execute_joint raises ExecutionInProgressError while lock is held."""
    session = _make_session()
    session._exec_lock.acquire()
    try:
        with pytest.raises(ExecutionInProgressError, match="Execution in progress"):
            session.execute_joint("j1")
    finally:
        session._exec_lock.release()


def test_guard_rejects_concurrent_execute_pipeline() -> None:
    """execute_pipeline raises ExecutionInProgressError when lock is held."""
    session = _make_session()
    session._exec_lock.acquire()
    try:
        with pytest.raises(ExecutionInProgressError, match="Execution in progress"):
            session.execute_pipeline()
    finally:
        session._exec_lock.release()


def test_guard_rejects_concurrent_execute_query() -> None:
    """execute_query raises ExecutionInProgressError when lock is held."""
    session = _make_session()
    session._exec_lock.acquire()
    try:
        with pytest.raises(ExecutionInProgressError, match="Execution in progress"):
            session.execute_query("SELECT 1")
    finally:
        session._exec_lock.release()


# ---------------------------------------------------------------------------
# Property 2: Guard releases under all termination modes (Req 1.2, 5.1, 5.4)
# ---------------------------------------------------------------------------


def test_guard_releases_on_success() -> None:
    """After successful execute_joint, lock is released and state is IDLE."""
    session = _make_session()
    session.execute_joint("j1")
    assert session.activity_state == Activity_State.IDLE
    assert not session._exec_lock.locked()


def test_guard_releases_on_exception() -> None:
    """After execute_joint raises, lock is released and state is IDLE."""
    session = _make_session()
    with pytest.raises(SessionError):
        session.execute_joint("nonexistent")
    assert session.activity_state == Activity_State.IDLE
    assert not session._exec_lock.locked()


def test_guard_releases_on_pipeline_success() -> None:
    """After successful execute_pipeline, lock is released."""
    session = _make_session()
    session.execute_pipeline()
    assert session.activity_state == Activity_State.IDLE
    assert not session._exec_lock.locked()


def test_guard_sets_executing_during_execution() -> None:
    """Guard sets activity state to EXECUTING during execution."""
    session = _make_session()
    observed_states: list[Activity_State] = []
    session.on_activity_change = lambda s: observed_states.append(s)
    session.execute_joint("j1")
    assert Activity_State.EXECUTING in observed_states
    assert observed_states[-1] == Activity_State.IDLE


# ---------------------------------------------------------------------------
# Property 3: Progress callback sequence contract (Req 3.1, 3.2, 3.3, 3.4)
# ---------------------------------------------------------------------------


def test_execute_joint_progress_sequence() -> None:
    """execute_joint invokes on_progress with compiling → executing → done."""
    session = _make_session()
    updates: list[QueryProgress] = []
    session.execute_joint("j1", on_progress=updates.append)

    assert len(updates) >= 3
    assert updates[0].status == "compiling"
    assert updates[-1].status == "done"
    assert updates[-1].current == updates[-1].total
    for u in updates:
        assert u.joint_name
        assert u.elapsed_ms >= 0


def test_execute_pipeline_progress_sequence() -> None:
    """execute_pipeline invokes on_progress with compiling → executing → done."""
    session = _make_session()
    updates: list[QueryProgress] = []
    session.execute_pipeline(on_progress=updates.append)

    assert len(updates) >= 2
    assert updates[0].status == "compiling"
    assert updates[-1].status == "done"
    assert updates[-1].current == updates[-1].total


# ---------------------------------------------------------------------------
# Property 4: Callback errors do not interrupt execution (Req 5.2)
# ---------------------------------------------------------------------------


def test_callback_error_does_not_interrupt_execute_joint() -> None:
    """on_progress raising RuntimeError does not prevent execution."""
    session = _make_session()

    def bad_callback(p: QueryProgress) -> None:
        raise RuntimeError("callback boom")

    result = session.execute_joint("j1", on_progress=bad_callback)
    assert result.row_count == 0
    assert session.activity_state == Activity_State.IDLE
    assert not session._exec_lock.locked()


def test_callback_error_does_not_interrupt_execute_pipeline() -> None:
    """on_progress raising RuntimeError does not prevent pipeline execution."""
    session = _make_session()

    def bad_callback(p: QueryProgress) -> None:
        raise RuntimeError("callback boom")

    result = session.execute_pipeline(on_progress=bad_callback)
    assert result.success
    assert session.activity_state == Activity_State.IDLE
    assert not session._exec_lock.locked()
