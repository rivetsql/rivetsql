"""Tabbed SQL editor panel for the Rivet REPL TUI.

Provides a multi-tab editor with SQL syntax highlighting, autocomplete popup,
live validation, multi-cursor editing, find/replace, undo/redo, line numbers,
and bracket matching.

Tab types:
  - joint file: editable, saved to disk on Ctrl+S
  - ad-hoc query: editable, saved to EditorCache
  - preview: read-only, not persisted

Requirements: 7.1–7.10, 8.6, 9.1–9.5, 10.1, 10.2, 16.1, 16.2
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

from textual import on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.css.query import NoMatches
from textual.events import Key
from textual.message import Message
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Button, Input, Static, TextArea
from textual.widgets.text_area import Selection

from rivet_cli.repl.accessibility import (
    ARIA_AUTOCOMPLETE_LIST,
    ARIA_EDITOR_AREA,
    ARIA_FIND_INPUT,
    ARIA_REPLACE_INPUT,
    ARIA_TAB_BAR,
)
from rivet_cli.repl.widgets.status_bar import ActivityChanged
from rivet_core.interactive.types import Activity_State

if TYPE_CHECKING:
    from rivet_core.interactive.session import InteractiveSession
    from rivet_core.interactive.types import Completion


# ---------------------------------------------------------------------------
# Tab type enum
# ---------------------------------------------------------------------------


class TabKind(Enum):
    """The three editor tab types."""

    JOINT_FILE = "joint_file"
    AD_HOC = "ad_hoc"
    PREVIEW = "preview"


# ---------------------------------------------------------------------------
# Tab data model
# ---------------------------------------------------------------------------


@dataclass
class EditorTab:
    """State for a single editor tab."""

    kind: TabKind
    title: str
    content: str = ""
    path: Path | None = None  # only for JOINT_FILE tabs
    dirty: bool = False
    read_only: bool = False
    cursor_row: int = 0
    cursor_col: int = 0


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


class EditorPanel(Widget):
    """Tabbed SQL editor with syntax highlighting, autocomplete, and validation."""

    DEFAULT_CSS = """
    EditorPanel {
        height: 1fr;
        width: 1fr;
    }
    EditorPanel .tab-bar {
        height: 1;
        dock: top;
    }
    EditorPanel .tab-button {
        min-width: 10;
        height: 1;
        margin: 0 1;
    }
    EditorPanel .tab-button.active {
        text-style: bold;
    }
    EditorPanel .find-bar {
        height: 1;
        dock: bottom;
        display: none;
    }
    EditorPanel .find-bar.visible {
        display: block;
    }
    EditorPanel .autocomplete-list {
        display: none;
        layer: overlay;
        max-height: 8;
        width: 40;
        background: $surface;
        border: tall $accent;
        padding: 0 1;
    }
    EditorPanel .autocomplete-list.visible {
        display: block;
    }
    EditorPanel .validation-bar {
        height: 1;
        dock: bottom;
    }
    """

    BINDINGS: ClassVar[list[Binding]] = [  # type: ignore[assignment]
        Binding("f5", "execute_current", "Run Query", show=True),
        Binding("ctrl+enter", "execute_current", "Run Query", show=False),
        Binding("ctrl+n", "new_tab", "New Tab", show=False),
        Binding("ctrl+w", "close_tab", "Close Tab", show=False),
        Binding("ctrl+s", "save", "Save", show=False),
        Binding("ctrl+shift+f", "format_sql", "Format SQL", show=False),
        Binding("ctrl+f", "find", "Find", show=False),
        Binding("ctrl+h", "find_replace", "Find & Replace", show=False),
        Binding("ctrl+tab", "next_tab", "Next Tab", show=False),
        Binding("ctrl+shift+tab", "prev_tab", "Previous Tab", show=False),
        Binding("ctrl+space", "trigger_autocomplete", "Autocomplete", show=False),
        Binding("ctrl+d", "select_next_occurrence", "Select Next", show=False),
    ]

    # --- Messages ---

    class QuerySubmitted(Message):
        """Posted when the user submits SQL for execution."""

        def __init__(self, sql: str) -> None:
            super().__init__()
            self.sql = sql

    class EditorSaved(Message):
        """Posted when a file is saved to disk."""

        def __init__(self, path: Path) -> None:
            super().__init__()
            self.path = path

    class FormatRequested(Message):
        """Posted when the user requests SQL formatting."""

        def __init__(self, sql: str) -> None:
            super().__init__()
            self.sql = sql

    # --- State ---

    active_tab_index: reactive[int] = reactive(0)

    def __init__(
        self,
        session: InteractiveSession | None = None,
        *,
        show_line_numbers: bool = True,
        tab_size: int = 2,
        word_wrap: bool = False,
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        super().__init__(name=name, id=id, classes=classes)
        self._session = session
        self._show_line_numbers = show_line_numbers
        self._tab_size = tab_size
        self._word_wrap = word_wrap
        self._tabs: list[EditorTab] = []
        self._completions: list[Completion] = []
        self._completion_index: int = 0
        self._find_visible: bool = False
        self._replace_visible: bool = False
        self._validation_timer: asyncio.TimerHandle | None = None
        self._validation_errors: list[str] = []
        self._suppress_autocomplete: bool = False
        self._executing: bool = False

    # --- Compose ---

    def compose(self) -> ComposeResult:
        with Horizontal(classes="tab-bar"):
            yield Static("", id="tab-labels")
        yield TextArea(
            "",
            language="sql",
            show_line_numbers=self._show_line_numbers,
            soft_wrap=self._word_wrap,
            id="editor-area",
        )
        yield Static("", classes="validation-bar", id="validation-bar")
        with Horizontal(classes="find-bar", id="find-bar"):
            yield Input(placeholder="Find…", id="find-input")
            yield Input(placeholder="Replace…", id="replace-input")
            yield Button("Next", id="find-next")
            yield Button("Replace", id="replace-btn")
            yield Button("All", id="replace-all-btn")
        with Vertical(classes="autocomplete-list", id="autocomplete-list"):
            yield Static("", id="autocomplete-items")

    def on_mount(self) -> None:
        if not self._tabs:
            self._add_tab(TabKind.AD_HOC, "Query 1")
        self._render_tabs()
        # Set tab/indent width via reactive property (not a constructor arg)
        try:
            editor = self.query_one("#editor-area", TextArea)
            editor.indent_width = self._tab_size
            editor.tooltip = ARIA_EDITOR_AREA
        except NoMatches:
            pass
        # ARIA-style labels — Requirement 35.1
        try:
            self.query_one("#tab-labels", Static).tooltip = ARIA_TAB_BAR
            self.query_one("#find-input", Input).tooltip = ARIA_FIND_INPUT
            self.query_one("#replace-input", Input).tooltip = ARIA_REPLACE_INPUT
            self.query_one("#autocomplete-list").tooltip = ARIA_AUTOCOMPLETE_LIST
        except NoMatches:
            pass

    # --- Tab management ---

    def _add_tab(
        self,
        kind: TabKind,
        title: str,
        content: str = "",
        path: Path | None = None,
        read_only: bool = False,
    ) -> int:
        tab = EditorTab(
            kind=kind,
            title=title,
            content=content,
            path=path,
            read_only=read_only or kind == TabKind.PREVIEW,
        )
        self._tabs.append(tab)
        idx = len(self._tabs) - 1
        self.active_tab_index = idx
        return idx

    def _save_current_cursor(self) -> None:
        """Persist cursor position from the TextArea into the active tab."""
        if not self._tabs:
            return
        tab = self._tabs[self.active_tab_index]
        try:
            area = self.query_one("#editor-area", TextArea)
            tab.content = area.text
            row, col = area.cursor_location
            tab.cursor_row = row
            tab.cursor_col = col
        except NoMatches:
            pass

    def _load_tab(self, index: int) -> None:
        """Load tab content into the TextArea."""
        if index < 0 or index >= len(self._tabs):
            return
        tab = self._tabs[index]
        try:
            area = self.query_one("#editor-area", TextArea)
            area.load_text(tab.content)
            area.read_only = tab.read_only
            area.move_cursor((tab.cursor_row, tab.cursor_col))
        except NoMatches:
            pass

    def _render_tabs(self) -> None:
        """Render the tab bar labels."""
        parts: list[str] = []
        for i, tab in enumerate(self._tabs):
            marker = "● " if tab.dirty else ""
            prefix = "[bold]" if i == self.active_tab_index else ""
            suffix = "[/bold]" if i == self.active_tab_index else ""
            parts.append(f"{prefix}[{marker}{tab.title}]{suffix}")
        try:
            label = self.query_one("#tab-labels", Static)
            label.update(" ".join(parts))
        except NoMatches:
            pass

    def watch_active_tab_index(self, old: int, new: int) -> None:
        if old != new and self._tabs:
            if 0 <= old < len(self._tabs):
                self._save_current_cursor()
            self._load_tab(new)
            self._render_tabs()
            self._schedule_validation()
            # Notify session of the newly active tab's content
            if self._session is not None:
                try:
                    self._session.update_editor_sql(self.get_current_sql())
                except Exception:
                    pass

    # --- Public API ---

    def open_joint_file(self, path: Path) -> None:
        """Open a joint SQL file in a new tab."""
        for i, tab in enumerate(self._tabs):
            if tab.path == path:
                self.active_tab_index = i
                return
        content = path.read_text(encoding="utf-8")
        self._add_tab(TabKind.JOINT_FILE, path.stem, content=content, path=path)
        self._render_tabs()

    def open_preview(self, title: str, content: str) -> None:
        """Open a read-only preview tab."""
        self._add_tab(TabKind.PREVIEW, title, content=content, read_only=True)
        self._render_tabs()

    def new_ad_hoc_tab(self, title: str | None = None, content: str = "") -> int:
        """Create a new ad-hoc query tab with optional initial content."""
        n = sum(1 for t in self._tabs if t.kind == TabKind.AD_HOC) + 1
        return self._add_tab(TabKind.AD_HOC, title or f"Query {n}", content=content)

    def revalidate(self) -> None:
        """Re-run live validation using the current session state (e.g. after dialect change)."""
        self._schedule_validation()

    def restore_editor_sql(self, sql: str) -> None:
        """Restore SQL content into the active ad-hoc tab (used on startup).

        Sets the first ad-hoc tab's content to *sql*.
        Does nothing if *sql* is empty or no ad-hoc tab exists.
        """
        if not sql or not self._tabs:
            return
        tab = self._tabs[0]
        if tab.read_only or tab.kind != TabKind.AD_HOC:
            return
        tab.content = sql
        try:
            area = self.query_one("#editor-area", TextArea)
            area.load_text(sql)
        except NoMatches:
            pass

    def insert_text_at_cursor(self, text: str) -> None:
        """Insert text at the current cursor position."""
        try:
            area = self.query_one("#editor-area", TextArea)
            area.insert(text)
        except NoMatches:
            pass

    def get_current_sql(self) -> str:
        """Return the full SQL content of the active tab."""
        try:
            area = self.query_one("#editor-area", TextArea)
            return area.text
        except NoMatches:
            if self._tabs:
                return self._tabs[self.active_tab_index].content
            return ""

    def get_selected_sql(self) -> str:
        """Return the selected text, or the full content if nothing is selected."""
        try:
            area = self.query_one("#editor-area", TextArea)
            sel = area.selected_text
            return sel if sel else area.text
        except NoMatches:
            return self.get_current_sql()

    @property
    def tabs(self) -> list[EditorTab]:
        return list(self._tabs)

    @property
    def active_tab(self) -> EditorTab | None:
        if self._tabs:
            return self._tabs[self.active_tab_index]
        return None

    # --- Actions ---

    def action_new_tab(self) -> None:
        self.new_ad_hoc_tab()
        self._render_tabs()

    def action_close_tab(self) -> None:
        if not self._tabs:
            return
        tab = self._tabs[self.active_tab_index]
        if tab.dirty:
            # In a real implementation this would prompt; for now just close
            pass
        self._tabs.pop(self.active_tab_index)
        if not self._tabs:
            self._add_tab(TabKind.AD_HOC, "Query 1")
        if self.active_tab_index >= len(self._tabs):
            self.active_tab_index = len(self._tabs) - 1
        self._load_tab(self.active_tab_index)
        self._render_tabs()

    def action_save(self) -> None:
        if not self._tabs:
            return
        self._save_current_cursor()
        tab = self._tabs[self.active_tab_index]
        if tab.kind == TabKind.JOINT_FILE and tab.path is not None:
            tab.path.write_text(tab.content, encoding="utf-8")
            tab.dirty = False
            self._render_tabs()
            self.post_message(self.EditorSaved(tab.path))
        elif tab.kind == TabKind.AD_HOC:
            tab.dirty = False
            self._render_tabs()

    def action_format_sql(self) -> None:
        if not self._tabs:
            return
        tab = self._tabs[self.active_tab_index]
        if tab.read_only:
            return
        sql = self.get_current_sql()
        if not sql.strip():
            return
        if self._session is not None:
            try:
                formatted = self._session.format_sql(sql)
                area = self.query_one("#editor-area", TextArea)
                area.load_text(formatted)
                tab.content = formatted
                tab.dirty = True
                self._render_tabs()
            except Exception as exc:
                self._show_validation_message(f"⚠ Cannot format: {exc}")
        else:
            self.post_message(self.FormatRequested(sql))

    def action_execute_current(self) -> None:
        """F5 — run the current query (selected text or full buffer)."""
        if self._executing:
            return
        sql = self.get_selected_sql()
        if sql.strip():
            self.post_message(self.QuerySubmitted(sql))
    def set_query_and_execute(self, sql: str) -> None:
        """Set the editor content to *sql* and immediately submit it for execution.

        If the active tab is read-only or a joint file, opens a new ad-hoc tab.
        """
        tab = self.active_tab
        if tab is None or tab.read_only or tab.kind != TabKind.AD_HOC:
            self.new_ad_hoc_tab()
        try:
            area = self.query_one("#editor-area", TextArea)
            area.load_text(sql)
            if self._tabs:
                self._tabs[self.active_tab_index].content = sql
                self._tabs[self.active_tab_index].dirty = True
                self._render_tabs()
        except NoMatches:
            pass
        self.post_message(self.QuerySubmitted(sql))

    def action_find(self) -> None:
        self._find_visible = True
        self._replace_visible = False
        self._update_find_bar()
        try:
            self.query_one("#find-input", Input).focus()
        except NoMatches:
            pass

    def action_find_replace(self) -> None:
        self._find_visible = True
        self._replace_visible = True
        self._update_find_bar()
        try:
            self.query_one("#find-input", Input).focus()
        except NoMatches:
            pass

    def action_next_tab(self) -> None:
        if self._tabs:
            self.active_tab_index = (self.active_tab_index + 1) % len(self._tabs)

    def action_prev_tab(self) -> None:
        if self._tabs:
            self.active_tab_index = (self.active_tab_index - 1) % len(self._tabs)

    def action_trigger_autocomplete(self) -> None:
        self._show_autocomplete()

    def action_select_next_occurrence(self) -> None:
        """Select the next occurrence of the current selection (multi-cursor stub)."""
        # Textual's TextArea does not natively support multi-cursor.
        # This is a best-effort: select the next occurrence of the selected word.
        try:
            area = self.query_one("#editor-area", TextArea)
            selected = area.selected_text
            if not selected:
                return
            text = area.text
            start = text.find(selected, area.cursor_location[0])
            if start >= 0:
                # Move cursor to next occurrence
                row = text[:start].count("\n")
                col = start - text[:start].rfind("\n") - 1
                area.move_cursor((row, col))
        except NoMatches:
            pass

    def switch_to_tab(self, index: int) -> None:
        """Switch to a tab by index (0-based). Used for Ctrl+1..9."""
        if 0 <= index < len(self._tabs):
            self.active_tab_index = index

    # --- Find/Replace ---

    def _update_find_bar(self) -> None:
        try:
            bar = self.query_one("#find-bar")
            if self._find_visible:
                bar.add_class("visible")
            else:
                bar.remove_class("visible")
            replace_input = self.query_one("#replace-input", Input)
            replace_btn = self.query_one("#replace-btn", Button)
            replace_all = self.query_one("#replace-all-btn", Button)
            if self._replace_visible:
                replace_input.display = True
                replace_btn.display = True
                replace_all.display = True
            else:
                replace_input.display = False
                replace_btn.display = False
                replace_all.display = False
        except NoMatches:
            pass

    @on(Button.Pressed, "#find-next")
    def _on_find_next(self) -> None:
        try:
            pattern = self.query_one("#find-input", Input).value
            if not pattern:
                return
            area = self.query_one("#editor-area", TextArea)
            text = area.text
            row, col = area.cursor_location
            offset = sum(len(line) + 1 for line in text.split("\n")[:row]) + col
            idx = text.find(pattern, offset)
            if idx < 0:
                idx = text.find(pattern)  # wrap around
            if idx >= 0:
                r = text[:idx].count("\n")
                c = idx - text[:idx].rfind("\n") - 1
                end_idx = idx + len(pattern)
                er = text[:end_idx].count("\n")
                ec = end_idx - text[:end_idx].rfind("\n") - 1
                area.selection = Selection((r, c), (er, ec))
        except NoMatches:
            pass

    @on(Button.Pressed, "#replace-btn")
    def _on_replace(self) -> None:
        try:
            area = self.query_one("#editor-area", TextArea)
            selected = area.selected_text
            replacement = self.query_one("#replace-input", Input).value
            if selected:
                area.replace(replacement, *area.selection)
        except (NoMatches, Exception):
            pass

    @on(Button.Pressed, "#replace-all-btn")
    def _on_replace_all(self) -> None:
        try:
            pattern = self.query_one("#find-input", Input).value
            replacement = self.query_one("#replace-input", Input).value
            if not pattern:
                return
            area = self.query_one("#editor-area", TextArea)
            new_text = area.text.replace(pattern, replacement)
            area.load_text(new_text)
            if self._tabs:
                self._tabs[self.active_tab_index].dirty = True
                self._render_tabs()
        except NoMatches:
            pass

    def on_key(self, event: Key) -> None:
        # Dismiss find bar on Escape
        if event.key == "escape":
            if self._find_visible:
                self._find_visible = False
                self._replace_visible = False
                self._update_find_bar()
                try:
                    self.query_one("#editor-area", TextArea).focus()
                except NoMatches:
                    pass
                event.prevent_default()
                return
            # Dismiss autocomplete
            if self._completions:
                self._hide_autocomplete()
                event.prevent_default()
                return

        # Navigate autocomplete with up/down arrows
        if self._completions:
            if event.key == "down":
                self._completion_index = (self._completion_index + 1) % min(len(self._completions), 8)
                self._render_autocomplete()
                event.prevent_default()
                return
            if event.key == "up":
                self._completion_index = (self._completion_index - 1) % min(len(self._completions), 8)
                self._render_autocomplete()
                event.prevent_default()
                return

        # Accept autocomplete with Tab or Enter
        if event.key in ("tab", "enter") and self._completions:
            self._accept_completion()
            event.prevent_default()
            return

        # Ctrl+1..9 for tab switching
        if event.key in {f"ctrl+{i}" for i in range(1, 10)}:
            idx = int(event.key[-1]) - 1
            self.switch_to_tab(idx)
            event.prevent_default()

    # --- Autocomplete ---

    def _show_autocomplete(self) -> None:
        if self._session is None:
            return
        sql = self.get_current_sql()
        try:
            area = self.query_one("#editor-area", TextArea)
            row, col = area.cursor_location
            # Compute cursor position as offset into the text
            lines = sql.split("\n")
            cursor_pos = sum(len(lines[i]) + 1 for i in range(row)) + col
        except NoMatches:
            cursor_pos = len(sql)

        try:
            self._completions = self._session.get_completions(sql, cursor_pos)
        except Exception:
            self._completions = []

        if not self._completions:
            self._hide_autocomplete()
            return

        self._completion_index = 0
        self._render_autocomplete()

    def _render_autocomplete(self) -> None:
        if not self._completions:
            self._hide_autocomplete()
            return
        items = self._completions[:8]
        lines: list[str] = []
        for i, c in enumerate(items):
            prefix = "▸ " if i == self._completion_index else "  "
            detail = f" ({c.detail})" if c.detail else ""
            lines.append(f"{prefix}{c.label}{detail}")
        try:
            ac_list = self.query_one("#autocomplete-list")
            ac_list.add_class("visible")
            self.query_one("#autocomplete-items", Static).update("\n".join(lines))
            # Position near cursor
            area = self.query_one("#editor-area", TextArea)
            row, col = area.cursor_location
            # Offset: tab-bar is 1 row, then cursor row (minus scroll), col for horizontal
            line_number_width = 5 if self._show_line_numbers else 0
            ac_list.styles.offset = (
                line_number_width + col,
                row + 2,  # +1 for tab-bar, +1 to appear below cursor line
            )
        except NoMatches:
            pass

    def _hide_autocomplete(self) -> None:
        self._completions = []
        self._completion_index = 0
        try:
            self.query_one("#autocomplete-list").remove_class("visible")
        except NoMatches:
            pass

    def _accept_completion(self) -> None:
        if not self._completions:
            return
        completion = self._completions[self._completion_index]
        self._suppress_autocomplete = True
        self._hide_autocomplete()
        try:
            area = self.query_one("#editor-area", TextArea)
            row, col = area.cursor_location
            lines = area.text.split("\n")
            if row < len(lines):
                line = lines[row]
                word_start = col
                while word_start > 0 and (line[word_start - 1].isalnum() or line[word_start - 1] in "_.-"):
                    word_start -= 1
                insert_text = completion.insert_text + " "
                area.replace(insert_text, (row, word_start), (row, col))
                new_col = word_start + len(insert_text)
                area.move_cursor((row, new_col))
            # Defer focus to after Textual processes the DOM changes
            self.call_after_refresh(area.focus)
        except (NoMatches, Exception):
            pass

    # --- Live validation ---

    def _schedule_validation(self) -> None:
        """Schedule validation with 300ms debounce."""
        if self._validation_timer is not None:
            self._validation_timer.cancel()
            self._validation_timer = None
        try:
            loop = asyncio.get_running_loop()
            self._validation_timer = loop.call_later(0.3, self._run_validation)
        except RuntimeError:
            pass

    def _run_validation(self) -> None:
        """Run lightweight SQL validation on the current buffer.

        Delegates to InteractiveSession.format_sql() as a parse check — if the
        SQL cannot be parsed, format_sql raises, which we surface as a
        validation hint. This avoids importing sqlglot directly (module boundary).
        """
        self._validation_timer = None
        sql = self.get_current_sql()
        if not sql.strip():
            self._show_validation_message("")
            return

        if self._session is None:
            self._show_validation_message("")
            return

        errors: list[str] = []
        try:
            self._session.format_sql(sql)
        except Exception as exc:
            errors.append(str(exc))

        self._validation_errors = errors
        if errors:
            self._show_validation_message(f"⚠ {errors[0]}")
        else:
            self._show_validation_message("✓ Valid SQL")

    def _show_validation_message(self, msg: str) -> None:
        try:
            self.query_one("#validation-bar", Static).update(msg)
        except NoMatches:
            pass

    @on(TextArea.Changed, "#editor-area")
    def _on_text_changed(self) -> None:
        if self._tabs:
            tab = self._tabs[self.active_tab_index]
            if not tab.read_only:
                tab.dirty = True
                self._render_tabs()
        self._schedule_validation()
        # Notify session of current editor content
        if self._session is not None:
            try:
                self._session.update_editor_sql(self.get_current_sql())
            except Exception:
                pass
        # Trigger autocomplete after 2+ chars of current word
        self._maybe_auto_complete()

    def _maybe_auto_complete(self) -> None:
        """Trigger autocomplete if the current word is 2+ characters."""
        if self._suppress_autocomplete:
            self._suppress_autocomplete = False
            return
        if self._session is None:
            return
        try:
            area = self.query_one("#editor-area", TextArea)
            row, col = area.cursor_location
            lines = area.text.split("\n")
            if row >= len(lines):
                return
            line = lines[row]
            word_start = col
            while word_start > 0 and (line[word_start - 1].isalnum() or line[word_start - 1] in "_."):
                word_start -= 1
            word = line[word_start:col]
            if len(word) >= 2:
                self._show_autocomplete()
            else:
                self._hide_autocomplete()
        except (NoMatches, Exception):
            self._hide_autocomplete()

    # --- Activity state guard ---

    def on_activity_changed(self, message: ActivityChanged) -> None:
        """React to ActivityChanged — disable execute when EXECUTING, re-enable on IDLE."""
        self._executing = message.state == Activity_State.EXECUTING
        self._show_validation_message(
            "⏳ Executing…" if self._executing else ""
        )
