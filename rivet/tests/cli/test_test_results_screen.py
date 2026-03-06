"""Tests for the test results overlay screen.

Validates: Requirements 25.1, 25.2, 25.3, 25.4
"""

from __future__ import annotations

import pytest

from rivet_cli.repl.screens.test_results import (
    SnapshotAction,
    TestFailureDiff,
    TestResultEntry,
    TestResultsScreen,
    TestResultsState,
    _diff_summary,
    _format_duration,
    _status_icon,
)

# ---------------------------------------------------------------------------
# _format_duration
# ---------------------------------------------------------------------------


class TestFormatDuration:
    def test_sub_second(self):
        assert _format_duration(42.0) == "42ms"

    def test_exactly_1000ms(self):
        assert _format_duration(1000.0) == "1.00s"

    def test_over_1000ms(self):
        assert _format_duration(2500.0) == "2.50s"

    def test_zero(self):
        assert _format_duration(0.0) == "0ms"


# ---------------------------------------------------------------------------
# _status_icon
# ---------------------------------------------------------------------------


class TestStatusIcon:
    def test_pass(self):
        assert _status_icon("pass") == "✓"

    def test_fail(self):
        assert _status_icon("fail") == "✗"

    def test_snapshot_mismatch(self):
        assert _status_icon("snapshot_mismatch") == "≠"

    def test_error(self):
        assert _status_icon("error") == "!"

    def test_unknown_returns_question_mark(self):
        assert _status_icon("unknown") == "?"  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# _diff_summary
# ---------------------------------------------------------------------------


class TestDiffSummary:
    def test_row_count_mismatch(self):
        diff = TestFailureDiff(expected_row_count=10, actual_row_count=8, extra_rows=0, missing_rows=2)
        summary = _diff_summary(diff)
        assert "expected 10 rows" in summary
        assert "got 8" in summary

    def test_extra_rows(self):
        diff = TestFailureDiff(expected_row_count=5, actual_row_count=7, extra_rows=2, missing_rows=0)
        summary = _diff_summary(diff)
        assert "+2 extra" in summary

    def test_missing_rows(self):
        diff = TestFailureDiff(expected_row_count=5, actual_row_count=3, extra_rows=0, missing_rows=2)
        summary = _diff_summary(diff)
        assert "-2 missing" in summary

    def test_same_count_but_differs(self):
        diff = TestFailureDiff(expected_row_count=5, actual_row_count=5, extra_rows=0, missing_rows=0)
        summary = _diff_summary(diff)
        assert summary == "rows differ"

    def test_all_parts_combined(self):
        diff = TestFailureDiff(expected_row_count=5, actual_row_count=6, extra_rows=2, missing_rows=1)
        summary = _diff_summary(diff)
        assert "expected 5 rows" in summary
        assert "+2 extra" in summary
        assert "-1 missing" in summary


# ---------------------------------------------------------------------------
# TestFailureDiff
# ---------------------------------------------------------------------------


class TestTestFailureDiff:
    def test_frozen(self):
        diff = TestFailureDiff(expected_row_count=5, actual_row_count=3, extra_rows=0, missing_rows=2)
        with pytest.raises((AttributeError, TypeError)):
            diff.extra_rows = 99  # type: ignore[misc]

    def test_fields(self):
        diff = TestFailureDiff(expected_row_count=10, actual_row_count=8, extra_rows=1, missing_rows=3)
        assert diff.expected_row_count == 10
        assert diff.actual_row_count == 8
        assert diff.extra_rows == 1
        assert diff.missing_rows == 3


# ---------------------------------------------------------------------------
# TestResultEntry
# ---------------------------------------------------------------------------


class TestTestResultEntry:
    def test_pass_entry(self):
        entry = TestResultEntry(name="test_orders", status="pass", duration_ms=12.5)
        assert entry.name == "test_orders"
        assert entry.status == "pass"
        assert entry.duration_ms == 12.5
        assert entry.failure_diff is None
        assert entry.snapshot_updatable is False

    def test_fail_entry_with_diff(self):
        diff = TestFailureDiff(expected_row_count=10, actual_row_count=8, extra_rows=0, missing_rows=2)
        entry = TestResultEntry(name="test_orders", status="fail", duration_ms=5.0, failure_diff=diff)
        assert entry.failure_diff is not None
        assert entry.failure_diff.missing_rows == 2

    def test_snapshot_mismatch_entry(self):
        entry = TestResultEntry(
            name="test_snapshot",
            status="snapshot_mismatch",
            duration_ms=3.0,
            snapshot_updatable=True,
        )
        assert entry.snapshot_updatable is True

    def test_frozen(self):
        entry = TestResultEntry(name="t", status="pass", duration_ms=1.0)
        with pytest.raises((AttributeError, TypeError)):
            entry.name = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# TestResultsState
# ---------------------------------------------------------------------------


class TestTestResultsState:
    def test_empty_state(self):
        state = TestResultsState()
        assert state.entries == []
        assert state.passed == 0
        assert state.failed == 0
        assert state.summary == "No tests found."

    def test_all_pass(self):
        entries = [
            TestResultEntry(name=f"test_{i}", status="pass", duration_ms=1.0)
            for i in range(3)
        ]
        state = TestResultsState(entries=entries)
        assert state.passed == 3
        assert state.failed == 0
        assert state.summary == "3/3 passed"

    def test_mixed_results(self):
        entries = [
            TestResultEntry(name="test_a", status="pass", duration_ms=1.0),
            TestResultEntry(name="test_b", status="fail", duration_ms=2.0),
            TestResultEntry(name="test_c", status="snapshot_mismatch", duration_ms=3.0),
        ]
        state = TestResultsState(entries=entries)
        assert state.passed == 1
        assert state.failed == 2
        assert state.summary == "1/3 passed"

    def test_scope_all(self):
        state = TestResultsState(scope="all")
        assert state.scope == "all"
        assert state.joint_name is None

    def test_scope_joint(self):
        state = TestResultsState(scope="joint", joint_name="transform_orders")
        assert state.scope == "joint"
        assert state.joint_name == "transform_orders"

    def test_error_status_counts_as_failed(self):
        entries = [
            TestResultEntry(name="test_a", status="error", duration_ms=1.0),
        ]
        state = TestResultsState(entries=entries)
        assert state.failed == 1
        assert state.passed == 0


# ---------------------------------------------------------------------------
# SnapshotAction
# ---------------------------------------------------------------------------


class TestSnapshotAction:
    def test_update_action(self):
        action = SnapshotAction(test_name="test_orders", action="update")
        assert action.test_name == "test_orders"
        assert action.action == "update"

    def test_diff_action(self):
        action = SnapshotAction(test_name="test_orders", action="diff")
        assert action.action == "diff"

    def test_skip_action(self):
        action = SnapshotAction(test_name="test_orders", action="skip")
        assert action.action == "skip"

    def test_frozen(self):
        action = SnapshotAction(test_name="t", action="skip")
        with pytest.raises((AttributeError, TypeError)):
            action.action = "update"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# TestResultsScreen construction
# ---------------------------------------------------------------------------


class TestTestResultsScreenConstruction:
    def test_stores_state(self):
        state = TestResultsState()
        screen = TestResultsScreen(state=state)
        assert screen._state is state

    def test_empty_entries(self):
        state = TestResultsState(entries=[])
        screen = TestResultsScreen(state=state)
        assert screen._state.entries == []

    def test_with_entries(self):
        entries = [
            TestResultEntry(name="test_a", status="pass", duration_ms=10.0),
            TestResultEntry(name="test_b", status="fail", duration_ms=20.0),
        ]
        state = TestResultsState(entries=entries)
        screen = TestResultsScreen(state=state)
        assert len(screen._state.entries) == 2

    def test_joint_scope(self):
        state = TestResultsState(scope="joint", joint_name="my_joint")
        screen = TestResultsScreen(state=state)
        assert screen._state.joint_name == "my_joint"

    def test_snapshot_mismatch_entry_accepted(self):
        entries = [
            TestResultEntry(
                name="test_snap",
                status="snapshot_mismatch",
                duration_ms=5.0,
                snapshot_updatable=True,
            )
        ]
        state = TestResultsState(entries=entries)
        screen = TestResultsScreen(state=state)
        assert screen._state.entries[0].snapshot_updatable is True
