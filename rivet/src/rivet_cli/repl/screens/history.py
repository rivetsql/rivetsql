"""Execution history overlay for the Rivet REPL TUI.

Triggered by Ctrl+H — displays a scrollable list of past executions with
timestamp, action type, name, row count, duration, and status.

Requirements: 21.2
"""

from __future__ import annotations

from typing import TYPE_CHECKING

try:
    from textual.app import ComposeResult
    from textual.binding import Binding
    from textual.screen import ModalScreen
    from textual.widgets import DataTable, Footer, Label

    _TEXTUAL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _TEXTUAL_AVAILABLE = False

if TYPE_CHECKING:
    from rivet_core.interactive.types import QueryHistoryEntry


def _format_duration(ms: float) -> str:
    """Format duration in ms to a human-readable string."""
    if ms < 1000:
        return f"{ms:.0f}ms"
    return f"{ms / 1000:.2f}s"


def _format_row_count(count: int | None) -> str:
    return str(count) if count is not None else "—"


if _TEXTUAL_AVAILABLE:

    class HistoryScreen(ModalScreen):  # type: ignore[type-arg]
        """Scrollable execution history overlay.

        Displays all QueryHistoryEntry items from the session history with
        columns: timestamp, action type, name, rows, duration, status.
        Dismiss with Escape or Enter.
        """

        BINDINGS = [
            Binding("escape", "dismiss", "Close", show=True),
            Binding("enter", "dismiss", "Close", show=True),
        ]

        DEFAULT_CSS = """
        HistoryScreen {
            align: center middle;
        }
        #history-container {
            width: 90%;
            height: 80%;
            border: thick $accent;
            background: $surface;
        }
        #history-title {
            height: 1;
            background: $accent;
            color: $text;
            content-align: center middle;
            text-style: bold;
        }
        #history-table {
            height: 1fr;
        }
        #history-empty {
            height: 1fr;
            content-align: center middle;
            color: $text-muted;
        }
        """

        def __init__(self, entries: list[QueryHistoryEntry]) -> None:
            super().__init__()
            self._entries = entries

        def compose(self) -> ComposeResult:
            from textual.containers import Vertical

            with Vertical(id="history-container"):
                yield Label("Execution History", id="history-title")
                if self._entries:
                    yield DataTable(id="history-table", cursor_type="row")
                else:
                    yield Label("No history yet.", id="history-empty")
            yield Footer()

        def on_mount(self) -> None:
            if not self._entries:
                return
            table = self.query_one("#history-table", DataTable)
            table.add_columns("Timestamp", "Type", "Name", "Rows", "Duration", "Status")
            for entry in reversed(self._entries):
                table.add_row(
                    entry.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    entry.action_type,
                    entry.name,
                    _format_row_count(entry.row_count),
                    _format_duration(entry.duration_ms),
                    entry.status,
                )

else:  # pragma: no cover

    class HistoryScreen:  # type: ignore[no-redef]
        """Stub when Textual is not installed."""

        def __init__(self, entries: list) -> None:  # type: ignore[type-arg]
            self._entries = entries
