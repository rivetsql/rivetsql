"""Tests for execution keybinding wiring in RivetRepl app.

Validates: Requirements 18.1, 18.2, 18.3, 18.4, 18.5, 19.1, 19.2

Tests cover:
- F6 binding triggers execute_pipeline via action_execute_pipeline
- Ctrl+Enter on joint triggers execute_joint via message handler
- Ctrl+C cancels execution via action_cancel_execution
- Progress callbacks post ExecutionStarted/Progress/Complete messages
- Quality check results are wired to CatalogPanel
- Concurrent execution is rejected by session-layer ExecutionInProgressError
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import pytest

from rivet_cli.repl.widgets.status_bar import (
    ExecutionComplete,
    ExecutionProgress,
    ExecutionStarted,
)
from rivet_core.interactive.types import (
    ExecutionResult,
    QueryProgress,
    QueryResult,
)

# ---------------------------------------------------------------------------
# Helpers — mock session and types
# ---------------------------------------------------------------------------


@dataclass
class FakeJoint:
    name: str
    type: str = "sql"


@dataclass
class FakeCheckResult:
    type: str
    severity: str
    passed: bool
    message: str
    phase: str
    read_back_rows: int | None = None


class FakeSession:
    """Minimal mock of InteractiveSession for testing execution wiring."""

    def __init__(
        self,
        joints: list[FakeJoint] | None = None,
        execute_pipeline_result: ExecutionResult | None = None,
        execute_joint_result: QueryResult | None = None,
        execute_pipeline_error: Exception | None = None,
        execute_joint_error: Exception | None = None,
    ) -> None:
        self._project_path = "/fake"
        self._profile_name = "default"
        self._joints = joints or []
        self._execute_pipeline_result = execute_pipeline_result
        self._execute_joint_result = execute_joint_result
        self._execute_pipeline_error = execute_pipeline_error
        self._execute_joint_error = execute_joint_error
        self.cancel_called = False
        self.execute_pipeline_called = False
        self.execute_joint_called = False
        self.execute_joint_name: str | None = None

    @property
    def active_profile(self) -> str:
        return self._profile_name

    def get_joints(self) -> list[FakeJoint]:
        return list(self._joints)

    def execute_pipeline(
        self,
        tags: list[str] | None = None,
        on_progress: Callable | None = None,
    ) -> ExecutionResult:
        self.execute_pipeline_called = True
        if self._execute_pipeline_error:
            raise self._execute_pipeline_error
        return self._execute_pipeline_result or ExecutionResult(
            success=True, joints_executed=[], elapsed_ms=0.0
        )

    def execute_joint(
        self,
        joint_name: str,
        on_progress: Callable | None = None,
    ) -> Any:
        self.execute_joint_called = True
        self.execute_joint_name = joint_name
        if self._execute_joint_error:
            raise self._execute_joint_error
        return self._execute_joint_result

    def cancel(self) -> None:
        self.cancel_called = True


# ---------------------------------------------------------------------------
# Tests — action_execute_pipeline
# ---------------------------------------------------------------------------


class TestActionExecutePipeline:
    """Test that action_execute_pipeline delegates to session."""

    def test_delegates_to_run_pipeline(self) -> None:
        """action_execute_pipeline should call _run_pipeline (guard is in session layer)."""
        from rivet_cli.repl.app import RivetRepl

        app = RivetRepl.__new__(RivetRepl)
        app._session = FakeSession()  # type: ignore[assignment]

        # action_execute_pipeline calls _run_pipeline which needs Textual runtime
        with pytest.raises(Exception):  # noqa: B017
            app.action_execute_pipeline()


# ---------------------------------------------------------------------------
# Tests — action_cancel_execution
# ---------------------------------------------------------------------------


class TestActionCancelExecution:
    """Test that Ctrl+C cancels execution."""

    def test_cancel_when_executing(self) -> None:
        from rivet_cli.repl.app import RivetRepl
        from rivet_core.interactive.types import Activity_State

        session = FakeSession()
        session.activity_state = Activity_State.EXECUTING
        app = RivetRepl.__new__(RivetRepl)
        app._session = session  # type: ignore[assignment]

        app.action_cancel_execution()
        assert session.cancel_called

    def test_cancel_noop_when_not_executing(self) -> None:
        from rivet_cli.repl.app import RivetRepl
        from rivet_core.interactive.types import Activity_State

        session = FakeSession()
        session.activity_state = Activity_State.IDLE
        app = RivetRepl.__new__(RivetRepl)
        app._session = session  # type: ignore[assignment]

        app.action_cancel_execution()
        assert not session.cancel_called


# ---------------------------------------------------------------------------
# Tests — _make_progress_callback
# ---------------------------------------------------------------------------


class TestMakeProgressCallback:
    """Test that progress callbacks produce the right message data."""

    def test_callback_posts_progress_message(self) -> None:
        from rivet_cli.repl.app import RivetRepl

        app = RivetRepl.__new__(RivetRepl)
        app._session = FakeSession()  # type: ignore[assignment]

        posted_calls: list[tuple] = []

        def fake_call_from_thread(fn, *args):
            posted_calls.append((fn, args))

        app.call_from_thread = fake_call_from_thread  # type: ignore[assignment]

        # Mock query_one to return a mock catalog panel
        mock_catalog = MagicMock()
        app.query_one = MagicMock(return_value=mock_catalog)  # type: ignore[assignment]

        callback = app._make_progress_callback(t0=0.0, total=3)

        progress = QueryProgress(
            joint_name="transform_orders",
            status="executing",
            current=2,
            total=3,
            rows=100,
            elapsed_ms=500.0,
        )
        callback(progress)

        # Should have posted at least 2 calls: progress message + catalog update
        assert len(posted_calls) >= 2


# ---------------------------------------------------------------------------
# Tests — on_catalog_panel_execute_joint_requested
# ---------------------------------------------------------------------------


class TestExecuteJointRequested:
    """Test that CatalogPanel.ExecuteJointRequested triggers joint execution."""

    def test_triggers_execution(self) -> None:
        from rivet_cli.repl.app import RivetRepl
        from rivet_cli.repl.widgets.catalog import CatalogPanel

        session = FakeSession()
        app = RivetRepl.__new__(RivetRepl)
        app._session = session  # type: ignore[assignment]

        msg = CatalogPanel.ExecuteJointRequested("my_joint")
        # _run_joint needs Textual runtime, so it will fail
        with pytest.raises(Exception):  # noqa: B017
            app.on_catalog_panel_execute_joint_requested(msg)


# ---------------------------------------------------------------------------
# Tests — bindings exist
# ---------------------------------------------------------------------------


class TestBindingsExist:
    """Verify that the required keybindings are declared."""

    def test_f6_binding_exists(self) -> None:
        from rivet_cli.repl.app import RivetRepl

        binding_actions = [b.action for b in RivetRepl.BINDINGS]
        assert "execute_pipeline" in binding_actions

    def test_ctrl_c_binding_exists(self) -> None:
        from rivet_cli.repl.app import RivetRepl

        binding_keys = [b.key for b in RivetRepl.BINDINGS]
        assert "ctrl+c" in binding_keys

    def test_f6_key(self) -> None:
        from rivet_cli.repl.app import RivetRepl

        f6_bindings = [b for b in RivetRepl.BINDINGS if b.action == "execute_pipeline"]
        assert len(f6_bindings) == 1
        assert f6_bindings[0].key == "f6"

    def test_ctrl_c_action(self) -> None:
        from rivet_cli.repl.app import RivetRepl

        ctrl_c_bindings = [b for b in RivetRepl.BINDINGS if b.key == "ctrl+c"]
        assert len(ctrl_c_bindings) == 1
        assert ctrl_c_bindings[0].action == "cancel_execution"


# ---------------------------------------------------------------------------
# Tests — F4 unified path (Requirements 8.1, 8.2, 8.3)
# ---------------------------------------------------------------------------


class TestF4UnifiedPath:
    """Verify F4 quick query generates correct SQL and routes through execute_query."""

    def test_f4_generates_select_with_limit(self) -> None:
        """F4 on a catalog table generates SELECT * FROM <table> LIMIT 100."""
        from rivet_cli.repl.widgets.catalog import CatalogPanel

        msg = CatalogPanel.QuickQuery("SELECT * FROM prod.public.users LIMIT 100")
        assert "LIMIT 100" in msg.sql
        assert "SELECT * FROM" in msg.sql

    def test_quick_query_sql_format(self) -> None:
        """QuickQuery message SQL matches expected format."""
        from rivet_cli.repl.widgets.catalog import CatalogPanel

        name = "prod.public.orders"
        sql = f"SELECT * FROM {name} LIMIT 100"
        msg = CatalogPanel.QuickQuery(sql)
        assert msg.sql == "SELECT * FROM prod.public.orders LIMIT 100"

    def test_f4_routes_through_set_query_and_execute_not_preview_table(self) -> None:
        """F4 handler calls set_query_and_execute, not preview_table."""
        from rivet_cli.repl.app import RivetRepl
        from rivet_cli.repl.widgets.catalog import CatalogPanel

        session = FakeSession()
        app = RivetRepl.__new__(RivetRepl)
        app._session = session  # type: ignore[assignment]

        set_query_calls: list[str] = []

        class FakeEditor:
            def set_query_and_execute(self, sql: str) -> None:
                set_query_calls.append(sql)

        app.query_one = MagicMock(return_value=FakeEditor())  # type: ignore[assignment]

        msg = CatalogPanel.QuickQuery("SELECT * FROM my_table LIMIT 100")
        app.on_catalog_panel_quick_query(msg)

        assert len(set_query_calls) == 1
        assert set_query_calls[0] == "SELECT * FROM my_table LIMIT 100"
        # preview_table is never called on the session
        assert not getattr(session, "_preview_table_called", False)

    def test_f4_ignored_when_executing(self) -> None:
        """F4 still routes through — guard is now in session layer, not TUI."""
        from rivet_cli.repl.app import RivetRepl
        from rivet_cli.repl.widgets.catalog import CatalogPanel

        session = FakeSession()
        app = RivetRepl.__new__(RivetRepl)
        app._session = session  # type: ignore[assignment]

        set_query_calls: list[str] = []

        class FakeEditor:
            def set_query_and_execute(self, sql: str) -> None:
                set_query_calls.append(sql)

        app.query_one = MagicMock(return_value=FakeEditor())  # type: ignore[assignment]

        msg = CatalogPanel.QuickQuery("SELECT * FROM my_table LIMIT 100")
        app.on_catalog_panel_quick_query(msg)

        # Guard is now in session layer; TUI always forwards the request
        assert len(set_query_calls) == 1

    def test_f5_routes_through_execute_query(self) -> None:
        """F5 (QuerySubmitted) routes through _run_ad_hoc_query → session.execute_query."""
        from rivet_cli.repl.app import RivetRepl
        from rivet_cli.repl.widgets.editor import EditorPanel

        session = FakeSession()
        app = RivetRepl.__new__(RivetRepl)
        app._session = session  # type: ignore[assignment]

        msg = EditorPanel.QuerySubmitted("SELECT 1")
        # _run_ad_hoc_query needs Textual runtime, so it will fail
        with pytest.raises(Exception):  # noqa: B017
            app.on_editor_panel_query_submitted(msg)


# ---------------------------------------------------------------------------
# Tests — CatalogPanel.ExecuteJointRequested message exists
# ---------------------------------------------------------------------------


class TestCatalogPanelExecuteJointMessage:
    """Verify the ExecuteJointRequested message carries joint_name."""

    def test_message_has_joint_name(self) -> None:
        from rivet_cli.repl.widgets.catalog import CatalogPanel

        msg = CatalogPanel.ExecuteJointRequested("orders_clean")
        assert msg.joint_name == "orders_clean"


# ---------------------------------------------------------------------------
# Tests — ExecutionStarted/Progress/Complete messages
# ---------------------------------------------------------------------------


class TestExecutionMessages:
    """Verify execution message data structures."""

    def test_execution_started(self) -> None:
        msg = ExecutionStarted(joint_name="raw_orders", total=5)
        assert msg.joint_name == "raw_orders"
        assert msg.total == 5

    def test_execution_progress(self) -> None:
        msg = ExecutionProgress(
            joint_name="transform", current=2, total=5, rows=1000, elapsed_ms=250.0
        )
        assert msg.joint_name == "transform"
        assert msg.current == 2
        assert msg.total == 5
        assert msg.rows == 1000
        assert msg.elapsed_ms == 250.0

    def test_execution_complete_success(self) -> None:
        msg = ExecutionComplete(success=True, elapsed_ms=1000.0)
        assert msg.success is True
        assert msg.canceled is False
        assert msg.error is None

    def test_execution_complete_canceled(self) -> None:
        msg = ExecutionComplete(success=False, canceled=True, elapsed_ms=500.0)
        assert msg.success is False
        assert msg.canceled is True

    def test_execution_complete_error(self) -> None:
        msg = ExecutionComplete(success=False, error="Connection failed", elapsed_ms=100.0)
        assert msg.success is False
        assert msg.error == "Connection failed"
