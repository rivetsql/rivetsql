"""Tests for the debug mode overlay screen.

Validates: Requirements 24.1, 24.2, 24.3, 24.4, 24.5, 24.6
"""

from __future__ import annotations

import pyarrow as pa

from rivet_cli.repl.screens.debug import DebugState, _format_material_preview

# ---------------------------------------------------------------------------
# DebugState — construction
# ---------------------------------------------------------------------------


class TestDebugStateConstruction:
    def test_empty_state(self):
        state = DebugState()
        assert state.joint_names == []
        assert state.current_index == -1
        assert state.breakpoints == set()
        assert state.materials == {}
        assert state.stopped is False

    def test_with_joints(self):
        state = DebugState(joint_names=["a", "b", "c"])
        assert state.joint_names == ["a", "b", "c"]
        assert state.is_active is True
        assert state.current_joint is None  # index is -1 before first step

    def test_empty_not_active(self):
        state = DebugState()
        assert state.is_active is False


# ---------------------------------------------------------------------------
# DebugState — stepping
# ---------------------------------------------------------------------------


class TestDebugStateStep:
    def test_step_advances(self):
        state = DebugState(joint_names=["a", "b", "c"])
        assert state.step() == "a"
        assert state.current_index == 0
        assert state.current_joint == "a"

    def test_step_through_all(self):
        state = DebugState(joint_names=["a", "b"])
        assert state.step() == "a"
        assert state.step() == "b"
        assert state.step() is None
        assert state.is_complete is True

    def test_step_when_stopped(self):
        state = DebugState(joint_names=["a", "b"])
        state.stop()
        assert state.step() is None

    def test_step_when_complete(self):
        state = DebugState(joint_names=["a"])
        state.step()
        state.step()  # past end
        assert state.step() is None


# ---------------------------------------------------------------------------
# DebugState — continue to breakpoint
# ---------------------------------------------------------------------------


class TestDebugStateContinue:
    def test_continue_no_breakpoints_runs_to_end(self):
        state = DebugState(joint_names=["a", "b", "c"])
        result = state.continue_to_breakpoint()
        assert result is None
        assert state.is_complete is True

    def test_continue_stops_at_breakpoint(self):
        state = DebugState(joint_names=["a", "b", "c"])
        state.toggle_breakpoint("b")
        result = state.continue_to_breakpoint()
        assert result == "b"
        assert state.current_joint == "b"

    def test_continue_from_breakpoint_to_next(self):
        state = DebugState(joint_names=["a", "b", "c"])
        state.toggle_breakpoint("b")
        state.toggle_breakpoint("c")
        state.continue_to_breakpoint()  # stops at b
        result = state.continue_to_breakpoint()  # stops at c
        assert result == "c"

    def test_continue_when_stopped(self):
        state = DebugState(joint_names=["a", "b"])
        state.stop()
        assert state.continue_to_breakpoint() is None

    def test_continue_skips_current_breakpoint(self):
        """F5 from a breakpointed joint should advance past it."""
        state = DebugState(joint_names=["a", "b", "c"])
        state.toggle_breakpoint("a")
        state.toggle_breakpoint("c")
        # Step to "a" first
        state.step()
        assert state.current_joint == "a"
        # Continue should skip "a" (already there) and stop at "c"
        result = state.continue_to_breakpoint()
        assert result == "c"


# ---------------------------------------------------------------------------
# DebugState — breakpoints
# ---------------------------------------------------------------------------


class TestDebugStateBreakpoints:
    def test_toggle_on(self):
        state = DebugState(joint_names=["a", "b"])
        assert state.toggle_breakpoint("a") is True
        assert "a" in state.breakpoints

    def test_toggle_off(self):
        state = DebugState(joint_names=["a", "b"])
        state.toggle_breakpoint("a")
        assert state.toggle_breakpoint("a") is False
        assert "a" not in state.breakpoints

    def test_multiple_breakpoints(self):
        state = DebugState(joint_names=["a", "b", "c"])
        state.toggle_breakpoint("a")
        state.toggle_breakpoint("c")
        assert state.breakpoints == {"a", "c"}


# ---------------------------------------------------------------------------
# DebugState — materials
# ---------------------------------------------------------------------------


class TestDebugStateMaterials:
    def test_store_and_retrieve(self):
        state = DebugState(joint_names=["a"])
        table = pa.table({"x": [1, 2, 3]})
        state.store_material("a", table)
        assert state.get_material("a") is table

    def test_missing_material(self):
        state = DebugState(joint_names=["a"])
        assert state.get_material("a") is None

    def test_materials_retained(self):
        """Req 24.4: all intermediate Materials retained in memory."""
        state = DebugState(joint_names=["a", "b", "c"])
        for name in state.joint_names:
            state.store_material(name, pa.table({"col": [1]}))
        # All should still be accessible
        for name in state.joint_names:
            assert state.get_material(name) is not None


# ---------------------------------------------------------------------------
# DebugState — stop
# ---------------------------------------------------------------------------


class TestDebugStateStop:
    def test_stop_sets_flag(self):
        state = DebugState(joint_names=["a", "b"])
        state.stop()
        assert state.stopped is True
        assert state.is_active is False

    def test_stop_prevents_step(self):
        state = DebugState(joint_names=["a", "b"])
        state.step()
        state.stop()
        assert state.step() is None


# ---------------------------------------------------------------------------
# DebugState — status_for
# ---------------------------------------------------------------------------


class TestDebugStateStatusFor:
    def test_pending(self):
        state = DebugState(joint_names=["a", "b"])
        assert state.status_for("a") == "pending"
        assert state.status_for("b") == "pending"

    def test_current(self):
        state = DebugState(joint_names=["a", "b"])
        state.step()
        assert state.status_for("a") == "current"
        assert state.status_for("b") == "pending"

    def test_done(self):
        state = DebugState(joint_names=["a", "b"])
        state.step()
        state.step()
        assert state.status_for("a") == "done"
        assert state.status_for("b") == "current"

    def test_unknown_joint(self):
        state = DebugState(joint_names=["a"])
        assert state.status_for("z") == ""

    def test_stopped_status(self):
        state = DebugState(joint_names=["a", "b"])
        state.step()
        state.stop()
        assert state.status_for("a") == "done"
        assert state.status_for("b") == "stopped"


# ---------------------------------------------------------------------------
# _format_material_preview
# ---------------------------------------------------------------------------


class TestFormatMaterialPreview:
    def test_arrow_table(self):
        table = pa.table({"id": [1], "name": ["x"]})
        result = _format_material_preview(table)
        assert result == [("id", "int64"), ("name", "string")]

    def test_no_schema(self):
        assert _format_material_preview("not a table") == []

    def test_none(self):
        assert _format_material_preview(None) == []

    def test_empty_table(self):
        table = pa.table({})
        assert _format_material_preview(table) == []


# ---------------------------------------------------------------------------
# DebugScreen — construction
# ---------------------------------------------------------------------------


class TestDebugScreenConstruction:
    def test_creates_with_state(self):
        from rivet_cli.repl.screens.debug import DebugScreen

        state = DebugState(joint_names=["a", "b"])
        screen = DebugScreen(state=state)
        assert screen.state is state

    def test_creates_with_empty_state(self):
        from rivet_cli.repl.screens.debug import DebugScreen

        state = DebugState()
        screen = DebugScreen(state=state)
        assert screen.state.joint_names == []

    def test_state_with_breakpoints(self):
        from rivet_cli.repl.screens.debug import DebugScreen

        state = DebugState(joint_names=["a", "b", "c"])
        state.toggle_breakpoint("b")
        screen = DebugScreen(state=state)
        assert "b" in screen.state.breakpoints
