"""Command Palette screen for the Rivet REPL TUI.

Fuzzy-searchable overlay listing all REPL commands with keyboard shortcuts.
Supports substring and fuzzy matching. Recently used commands appear first.

Requirements: 17.1, 17.2, 17.3, 17.4
"""

from __future__ import annotations

from dataclasses import dataclass, field

try:
    from textual.app import ComposeResult
    from textual.binding import Binding
    from textual.containers import Vertical
    from textual.screen import ModalScreen
    from textual.widgets import Input, ListItem, ListView, Static

    from rivet_cli.repl.accessibility import ARIA_COMMAND_PALETTE

    _TEXTUAL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _TEXTUAL_AVAILABLE = False



# ---------------------------------------------------------------------------
# Command registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Command:
    """A single REPL command entry."""

    name: str
    shortcut: str  # keyboard shortcut display string, empty if none
    action: str  # internal action identifier


# Full command list per Requirement 17.4
COMMANDS: list[Command] = [
    Command("Compile Project", "F5", "compile_project"),
    Command("Compile Current Joint", "Ctrl+Shift+C", "compile_current"),
    Command("Run All Sinks", "F6", "run_all_sinks"),
    Command("Run Current Query", "F5", "run_current_query"),
    Command("Run Tests", "F8", "run_tests"),
    Command("Switch Profile", "Ctrl+Shift+P", "switch_profile"),
    Command("Toggle Catalog", "Ctrl+B", "toggle_catalog"),
    Command("Toggle Results", "Ctrl+\\", "toggle_results"),
    Command("New Ad-hoc Tab", "Ctrl+N", "new_tab"),
    Command("Open File", "Ctrl+O", "open_file"),
    Command("Save", "Ctrl+S", "save"),
    Command("Export Data", "Ctrl+Shift+E", "export_data"),
    Command("Find", "Ctrl+F", "find"),
    Command("Format SQL", "Ctrl+Shift+F", "format_sql"),
    Command("Go to Joint", "", "goto_joint"),
    Command("Search Catalog", "/", "search_catalog"),
    Command("Pin Result", "Ctrl+K", "pin_result"),
    Command("Unpin Result", "Ctrl+Shift+K", "unpin_result"),
    Command("Diff Results", "Ctrl+D", "diff_results"),
    Command("Profile Data", "Ctrl+Shift+D", "profile_data"),
    Command("Show Query Plan", "Ctrl+E", "show_query_plan"),
    Command("Doctor", "", "doctor"),
    Command("Refresh Catalogs", "", "refresh_catalogs"),
    Command("View History", "Ctrl+H", "view_history"),
    Command("Settings", "", "settings"),
    Command("Help", "?", "help"),
    Command("Quit", "Ctrl+Q", "quit"),
    Command("Debug Pipeline", "", "debug_pipeline"),
]


# ---------------------------------------------------------------------------
# Pure state — no UI dependencies
# ---------------------------------------------------------------------------


def _fuzzy_score(query: str, text: str) -> int | None:
    """Return a match score for *query* against *text*, or None if no match.

    Scoring (higher is better):
      3 — exact match (case-insensitive)
      2 — substring match
      1 — fuzzy (all query chars appear in order in text)
      None — no match
    """
    q = query.lower()
    t = text.lower()
    if not q:
        return 3
    if q == t:
        return 3
    if q in t:
        return 2
    # fuzzy: all chars of q appear in t in order
    idx = 0
    for ch in q:
        pos = t.find(ch, idx)
        if pos == -1:
            return None
        idx = pos + 1
    return 1


@dataclass
class CommandPaletteState:
    """Pure state for the command palette. No UI dependencies."""

    _commands: list[Command] = field(default_factory=lambda: list(COMMANDS))
    _recent: list[str] = field(default_factory=list)  # action identifiers, most-recent first

    def record_used(self, action: str) -> None:
        """Record that *action* was used (moves it to front of recent list)."""
        if action in self._recent:
            self._recent.remove(action)
        self._recent.insert(0, action)

    def search(self, query: str) -> list[Command]:
        """Return commands matching *query*, recently-used first within each tier."""
        scored: list[tuple[int, int, Command]] = []
        for cmd in self._commands:
            score = _fuzzy_score(query, cmd.name)
            if score is None:
                continue
            # recent_rank: lower index = more recent = higher priority
            try:
                recent_rank = self._recent.index(cmd.action)
            except ValueError:
                recent_rank = len(self._recent)
            scored.append((score, recent_rank, cmd))

        # Sort: higher score first, then lower recent_rank first
        scored.sort(key=lambda x: (-x[0], x[1]))
        return [cmd for _, _, cmd in scored]


# ---------------------------------------------------------------------------
# Textual screen
# ---------------------------------------------------------------------------

if _TEXTUAL_AVAILABLE:

    class CommandPalette(ModalScreen[str | None]):
        """Fuzzy-searchable command palette overlay.

        Dismisses with the selected command's action string, or None if
        the user cancels.
        """

        BINDINGS = [
            Binding("escape", "cancel", "Cancel", show=True),
            Binding("enter", "select", "Select", show=True),
        ]

        DEFAULT_CSS = """
        CommandPalette {
            align: center middle;
        }
        #palette-container {
            width: 60;
            height: 20;
            border: thick $accent;
            background: $surface;
            padding: 0 1;
        }
        #palette-input {
            width: 1fr;
            margin-bottom: 1;
        }
        #palette-list {
            height: 1fr;
        }
        .palette-item {
            height: 1;
        }
        .palette-shortcut {
            color: $text-muted;
        }
        """

        def __init__(self, state: CommandPaletteState | None = None) -> None:
            super().__init__()
            self._state = state if state is not None else CommandPaletteState()
            self._results: list[Command] = self._state.search("")

        def compose(self) -> ComposeResult:
            with Vertical(id="palette-container"):
                yield Input(placeholder="Search commands…", id="palette-input")
                yield ListView(id="palette-list")

        def on_mount(self) -> None:
            self._refresh_list(self._results)
            self.query_one("#palette-input", Input).focus()
            # ARIA-style label — Requirement 35.1
            self.tooltip = ARIA_COMMAND_PALETTE

        def _refresh_list(self, commands: list[Command]) -> None:
            lv = self.query_one("#palette-list", ListView)
            lv.clear()
            for cmd in commands:
                shortcut_text = f"  {cmd.shortcut}" if cmd.shortcut else ""
                item = ListItem(
                    Static(f"{cmd.name}{shortcut_text}", classes="palette-item"),
                )
                item.data = cmd.action  # type: ignore[attr-defined]
                lv.append(item)

        def on_input_changed(self, event: Input.Changed) -> None:
            self._results = self._state.search(event.value)
            self._refresh_list(self._results)

        def action_select(self) -> None:
            lv = self.query_one("#palette-list", ListView)
            highlighted = lv.highlighted_child
            if highlighted is not None:
                action = getattr(highlighted, "data", None)
                if action:
                    self._state.record_used(action)
                    self.dismiss(action)
            elif self._results:
                action = self._results[0].action
                self._state.record_used(action)
                self.dismiss(action)
            else:
                self.dismiss(None)

        def action_cancel(self) -> None:
            self.dismiss(None)

        def on_list_view_selected(self, event: ListView.Selected) -> None:
            action = getattr(event.item, "data", None)
            if action:
                self._state.record_used(action)
                self.dismiss(action)
