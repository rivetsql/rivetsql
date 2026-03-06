"""Test results overlay for the Rivet REPL TUI.

Triggered by F8 (all tests) or Shift+F8 (focused joint only).
Displays each test with name, pass/fail status, and execution time.
For failures, shows the diff (row count mismatch, extra/missing rows).
For snapshot mismatches, offers [Update snapshot] [Show diff] [Skip].

Requirements: 25.1, 25.2, 25.3, 25.4
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

try:
    from textual.app import ComposeResult
    from textual.binding import Binding
    from textual.screen import ModalScreen
    from textual.widgets import DataTable, Footer, Label, Static

    _TEXTUAL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _TEXTUAL_AVAILABLE = False


# ---------------------------------------------------------------------------
# Pure data types — no UI dependencies
# ---------------------------------------------------------------------------

TestStatus = Literal["pass", "fail", "snapshot_mismatch", "error"]


@dataclass(frozen=True)
class TestFailureDiff:
    """Diff details for a failed test."""

    expected_row_count: int
    actual_row_count: int
    extra_rows: int  # rows in actual but not expected
    missing_rows: int  # rows in expected but not actual


@dataclass(frozen=True)
class TestResultEntry:
    """A single test result."""

    name: str
    status: TestStatus
    duration_ms: float
    failure_diff: TestFailureDiff | None = None
    # True when status == "snapshot_mismatch" and snapshot can be updated
    snapshot_updatable: bool = False


@dataclass
class TestResultsState:
    """Pure state for the test results overlay."""

    entries: list[TestResultEntry] = field(default_factory=list)
    scope: Literal["all", "joint"] = "all"
    joint_name: str | None = None

    @property
    def passed(self) -> int:
        return sum(1 for e in self.entries if e.status == "pass")

    @property
    def failed(self) -> int:
        return sum(1 for e in self.entries if e.status != "pass")

    @property
    def summary(self) -> str:
        total = len(self.entries)
        if total == 0:
            return "No tests found."
        return f"{self.passed}/{total} passed"


# ---------------------------------------------------------------------------
# Snapshot action returned on dismiss
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SnapshotAction:
    """Action chosen for a snapshot mismatch."""

    test_name: str
    action: Literal["update", "diff", "skip"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _format_duration(ms: float) -> str:
    if ms < 1000:
        return f"{ms:.0f}ms"
    return f"{ms / 1000:.2f}s"


def _status_icon(status: TestStatus) -> str:
    return {
        "pass": "✓",
        "fail": "✗",
        "snapshot_mismatch": "≠",
        "error": "!",
    }.get(status, "?")


def _diff_summary(diff: TestFailureDiff) -> str:
    parts = []
    if diff.expected_row_count != diff.actual_row_count:
        parts.append(
            f"expected {diff.expected_row_count} rows, got {diff.actual_row_count}"
        )
    if diff.extra_rows:
        parts.append(f"+{diff.extra_rows} extra")
    if diff.missing_rows:
        parts.append(f"-{diff.missing_rows} missing")
    return "; ".join(parts) if parts else "rows differ"


# ---------------------------------------------------------------------------
# Textual screen
# ---------------------------------------------------------------------------

if _TEXTUAL_AVAILABLE:

    class TestResultsScreen(ModalScreen[SnapshotAction | None]):
        """Test results overlay.

        Displays all test results with name, status icon, and duration.
        Failed tests show a diff summary inline.
        Snapshot mismatches show action buttons.
        Dismiss with Escape or Enter (no action).
        """

        BINDINGS = [
            Binding("escape", "dismiss_none", "Close", show=True),
            Binding("enter", "dismiss_none", "Close", show=True),
        ]

        DEFAULT_CSS = """
        TestResultsScreen {
            align: center middle;
        }
        #test-results-container {
            width: 90%;
            height: 80%;
            border: thick $accent;
            background: $surface;
        }
        #test-results-title {
            height: 1;
            background: $accent;
            color: $text;
            content-align: center middle;
            text-style: bold;
        }
        #test-results-summary {
            height: 1;
            color: $text-muted;
            content-align: center middle;
        }
        #test-results-table {
            height: 1fr;
        }
        #test-results-empty {
            height: 1fr;
            content-align: center middle;
            color: $text-muted;
        }
        """

        def __init__(self, state: TestResultsState) -> None:
            super().__init__()
            self._state = state

        def compose(self) -> ComposeResult:
            from textual.containers import Vertical

            title = (
                f"Test Results — {self._state.joint_name}"
                if self._state.joint_name
                else "Test Results"
            )
            with Vertical(id="test-results-container"):
                yield Label(title, id="test-results-title")
                yield Static(self._state.summary, id="test-results-summary")
                if self._state.entries:
                    yield DataTable(id="test-results-table", cursor_type="row")
                else:
                    yield Label("No tests found.", id="test-results-empty")
            yield Footer()

        def on_mount(self) -> None:
            if not self._state.entries:
                return
            table = self.query_one("#test-results-table", DataTable)
            table.add_columns("", "Test", "Duration", "Details")
            for entry in self._state.entries:
                icon = _status_icon(entry.status)
                details = ""
                if entry.status == "snapshot_mismatch":
                    details = "snapshot mismatch — press U to update, D to diff, S to skip"
                elif entry.failure_diff is not None:
                    details = _diff_summary(entry.failure_diff)
                table.add_row(
                    icon,
                    entry.name,
                    _format_duration(entry.duration_ms),
                    details,
                )

        def action_dismiss_none(self) -> None:
            self.dismiss(None)

        def action_update_snapshot(self) -> None:
            """Update snapshot for the currently focused row."""
            table = self.query_one("#test-results-table", DataTable)
            row_key = table.cursor_row
            if row_key < len(self._state.entries):
                entry = self._state.entries[row_key]
                if entry.status == "snapshot_mismatch" and entry.snapshot_updatable:
                    self.dismiss(SnapshotAction(test_name=entry.name, action="update"))

        def on_key(self, event) -> None:  # type: ignore[no-untyped-def]
            """Handle snapshot action keys when a snapshot_mismatch row is focused."""
            if not self._state.entries:
                return
            table = self.query_one("#test-results-table", DataTable)
            row_idx = table.cursor_row
            if row_idx >= len(self._state.entries):
                return
            entry = self._state.entries[row_idx]
            if entry.status != "snapshot_mismatch":
                return
            if event.key == "u" and entry.snapshot_updatable:
                self.dismiss(SnapshotAction(test_name=entry.name, action="update"))
                event.stop()
            elif event.key == "d":
                self.dismiss(SnapshotAction(test_name=entry.name, action="diff"))
                event.stop()
            elif event.key == "s":
                self.dismiss(SnapshotAction(test_name=entry.name, action="skip"))
                event.stop()

else:  # pragma: no cover

    class TestResultsScreen:  # type: ignore[no-redef]
        """Stub when Textual is not installed."""

        def __init__(self, state: TestResultsState) -> None:
            self._state = state
