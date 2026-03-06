"""LogsView widget for the Rivet REPL TUI.

Renders Execution_Log entries in chronological order with level-based
color coding, auto-scroll, text search filtering, and clear support.

Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6
"""

from __future__ import annotations

from typing import TYPE_CHECKING

try:
    from textual.app import ComposeResult
    from textual.binding import Binding
    from textual.containers import Vertical, VerticalScroll
    from textual.widget import Widget
    from textual.widgets import Input, Static

    _TEXTUAL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _TEXTUAL_AVAILABLE = False

if TYPE_CHECKING:
    from rivet_core.interactive.types import Execution_Log

# Level → CSS class mapping
_LEVEL_CLASSES: dict[str, str] = {
    "DEBUG": "log-debug",
    "INFO": "log-info",
    "WARNING": "log-warning",
    "ERROR": "log-error",
}

# Source → Rich color tag for inline coloring
_SOURCE_COLORS: dict[str, str] = {
    "session": "bright_white",
    "compiler": "cyan",
    "executor": "magenta",
    "engine": "green",
}

_PLACEHOLDER = "No execution logs yet. Run a query to see engine logs."

if _TEXTUAL_AVAILABLE:

    class LogsView(Widget):
        """Logs tab content — renders Execution_Log entries."""

        DEFAULT_CSS = """
        LogsView {
            height: 1fr;
        }
        LogsView .log-debug {
            color: $text-muted;
        }
        LogsView .log-info {
            color: $text;
        }
        LogsView .log-warning {
            color: yellow;
        }
        LogsView .log-error {
            color: red;
        }
        LogsView .logs-placeholder {
            color: $text-muted;
            text-style: italic;
            padding: 1 2;
        }
        LogsView #search-bar {
            height: auto;
            display: none;
        }
        LogsView #search-bar.visible {
            display: block;
        }
        LogsView #search-input {
            width: 1fr;
        }
        """

        BINDINGS = [
            Binding("ctrl+f", "toggle_search", "Search logs", show=False),
        ]

        _auto_scroll: bool = True

        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)  # type: ignore[arg-type]
            self._entries: list[Execution_Log] = []
            self._filter_keyword: str = ""

        def compose(self) -> ComposeResult:
            with Vertical(id="search-bar"):
                yield Input(placeholder="Filter logs…", id="search-input")
            yield VerticalScroll(
                Static(_PLACEHOLDER, classes="logs-placeholder", id="placeholder"),
                id="log-scroll",
            )

        def append_log(self, entry: Execution_Log) -> None:
            """Add a single log entry (called via on_log callback)."""
            self._entries.append(entry)
            if self._matches_filter(entry):
                self._append_log_widget(entry)
                self._update_placeholder()
                if self._auto_scroll:
                    scroll = self.query_one("#log-scroll", VerticalScroll)
                    scroll.scroll_end(animate=False)

        def refresh_logs(self, entries: list[Execution_Log]) -> None:
            """Replace all displayed logs (called on tab activation)."""
            self._entries = list(entries)
            self._rebuild_display()

        def clear(self) -> None:
            """Clear the display (called on :clear-logs)."""
            self._entries.clear()
            self._filter_keyword = ""
            self._rebuild_display()

        def search(self, keyword: str) -> None:
            """Filter displayed entries by keyword."""
            self._filter_keyword = keyword.strip()
            self._rebuild_display()

        def action_toggle_search(self) -> None:
            """Toggle the search bar visibility."""
            bar = self.query_one("#search-bar", Vertical)
            if "visible" in bar.classes:
                bar.remove_class("visible")
                self._filter_keyword = ""
                self._rebuild_display()
            else:
                bar.add_class("visible")
                self.query_one("#search-input", Input).focus()

        def on_input_changed(self, event: Input.Changed) -> None:
            """React to search input changes."""
            if event.input.id == "search-input":
                self.search(event.value)

        def on_vertical_scroll_scroll_up(self) -> None:
            """Disable auto-scroll when user scrolls up."""
            self._auto_scroll = False

        def on_vertical_scroll_scroll_down(self) -> None:
            """Re-enable auto-scroll when user scrolls to bottom."""
            scroll = self.query_one("#log-scroll", VerticalScroll)
            if scroll.scroll_offset.y >= scroll.max_scroll_y:
                self._auto_scroll = True

        # -- internal helpers --

        def _matches_filter(self, entry: Execution_Log) -> bool:
            if not self._filter_keyword:
                return True
            kw = self._filter_keyword.lower()
            return kw in entry.message.lower()

        def _format_entry(self, entry: Execution_Log) -> str:
            ts = entry.timestamp.strftime("%H:%M:%S.%f")[:-3]
            color = _SOURCE_COLORS.get(entry.source, "bright_white")
            level = entry.level.ljust(7)
            return f"{ts} \\[{level}] [{color}]{entry.source}[/]: {entry.message}"

        def _append_log_widget(self, entry: Execution_Log) -> None:
            scroll = self.query_one("#log-scroll", VerticalScroll)
            css_class = _LEVEL_CLASSES.get(entry.level, "log-info")
            label = Static(self._format_entry(entry), classes=css_class)
            scroll.mount(label)

        def _rebuild_display(self) -> None:
            scroll = self.query_one("#log-scroll", VerticalScroll)
            # Remove all children except placeholder
            for child in list(scroll.children):
                if child.id != "placeholder":
                    child.remove()
            # Re-add filtered entries
            filtered = [e for e in self._entries if self._matches_filter(e)]
            for entry in filtered:
                self._append_log_widget(entry)
            self._update_placeholder()
            if self._auto_scroll:
                scroll.scroll_end(animate=False)

        def _update_placeholder(self) -> None:
            placeholder = self.query_one("#placeholder", Static)
            has_visible = any(self._matches_filter(e) for e in self._entries)
            placeholder.display = not has_visible
