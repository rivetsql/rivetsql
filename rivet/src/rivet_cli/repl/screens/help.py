"""Help overlay with keybinding reference for the Rivet REPL TUI.

Triggered by `?` or `:help` — displays a scrollable keybinding reference.

Requirements: 17.4
"""

from __future__ import annotations

try:
    from textual.app import ComposeResult
    from textual.binding import Binding
    from textual.screen import ModalScreen
    from textual.widgets import DataTable, Footer, Label

    _TEXTUAL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _TEXTUAL_AVAILABLE = False


# ---------------------------------------------------------------------------
# Keybinding reference data — pure, no UI dependencies
# ---------------------------------------------------------------------------

#: (action, keybinding, description)
KEYBINDINGS: list[tuple[str, str, str]] = [
    # Execution
    ("Compile Project", "F5", "Compile the current project"),
    ("Compile Current Joint", "Ctrl+Shift+C", "Compile the focused joint"),
    ("Run All Sinks", "F6", "Execute the full pipeline"),
    ("Run Current Query", "F5", "Execute selected SQL or full buffer"),
    ("Run Tests", "F8", "Run all quality checks"),
    # Navigation
    ("Toggle Catalog", "Ctrl+B", "Show/hide the catalog panel"),
    ("Toggle Results", "Ctrl+\\", "Show/hide the results panel"),
    ("Fullscreen Panel", "F11", "Toggle fullscreen for focused panel"),
    ("New Tab", "Ctrl+N", "Open a new ad-hoc query tab"),
    ("Close Tab", "Ctrl+W", "Close the current tab"),
    ("Next Tab", "Ctrl+Tab", "Switch to next tab"),
    ("Prev Tab", "Ctrl+Shift+Tab", "Switch to previous tab"),
    ("Open File", "Ctrl+O", "Open a file in the editor"),
    # Editor
    ("Save", "Ctrl+S", "Save current buffer"),
    ("Find", "Ctrl+F", "Find in editor"),
    ("Replace", "Ctrl+H", "Find and replace in editor"),
    ("Format SQL", "Ctrl+Shift+F", "Format SQL with sqlglot"),
    ("Undo", "Ctrl+Z", "Undo last edit"),
    ("Redo", "Ctrl+Shift+Z", "Redo last undone edit"),
    ("Select Next", "Ctrl+D", "Select next occurrence (multi-cursor)"),
    ("Autocomplete", "Ctrl+Space", "Trigger autocomplete"),
    # Results
    ("Pin Result", "Ctrl+K", "Pin current result set"),
    ("Unpin Result", "Ctrl+Shift+K", "Remove pinned result"),
    ("Diff Results", "Ctrl+D", "Diff current vs pinned result"),
    ("Profile Data", "Ctrl+Shift+D", "Toggle column profiling panel"),
    ("Show Query Plan", "Ctrl+E", "Toggle query plan panel"),
    ("Copy Cell", "Ctrl+C", "Copy selected cells as TSV"),
    ("Export Data", "Ctrl+Shift+E", "Export results to file"),
    # Overlays
    ("Command Palette", "Ctrl+P", "Open command palette"),
    ("Switch Profile", "Ctrl+Shift+P", "Switch active profile"),
    ("View History", "Ctrl+H", "Open execution history"),
    ("Search Catalog", "/", "Fuzzy search in catalog panel"),
    ("Help", "?", "Show this help overlay"),
    # Session
    ("Quit", "Ctrl+Q", "Exit the REPL"),
]


if _TEXTUAL_AVAILABLE:

    class HelpScreen(ModalScreen):  # type: ignore[type-arg]
        """Keybinding reference overlay.

        Displays all REPL keybindings grouped by category.
        Dismiss with Escape or Enter.
        """

        BINDINGS = [
            Binding("escape", "dismiss", "Close", show=True),
            Binding("enter", "dismiss", "Close", show=True),
        ]

        DEFAULT_CSS = """
        HelpScreen {
            align: center middle;
        }
        #help-container {
            width: 80%;
            height: 80%;
            border: thick $accent;
            background: $surface;
        }
        #help-title {
            height: 1;
            background: $accent;
            color: $text;
            content-align: center middle;
            text-style: bold;
        }
        #help-table {
            height: 1fr;
        }
        """

        def compose(self) -> ComposeResult:
            from textual.containers import Vertical

            with Vertical(id="help-container"):
                yield Label("Keybinding Reference  (Escape to close)", id="help-title")
                yield DataTable(id="help-table", cursor_type="row")
            yield Footer()

        def on_mount(self) -> None:
            table = self.query_one("#help-table", DataTable)
            table.add_columns("Action", "Keybinding", "Description")
            for action, key, desc in KEYBINDINGS:
                table.add_row(action, key, desc)

else:  # pragma: no cover

    class HelpScreen:  # type: ignore[no-redef]
        """Stub when Textual is not installed."""
