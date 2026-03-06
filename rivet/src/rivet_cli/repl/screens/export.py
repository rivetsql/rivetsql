"""Export dialog screen for the Rivet REPL TUI.

Triggered by Ctrl+Shift+E in the Results panel. Provides format selection,
file path input, and row count display. On confirmation, calls the session
exporter and surfaces RVT-865 on failure.

Requirements: 26.1, 26.3
"""

from __future__ import annotations

from dataclasses import dataclass

try:
    from textual.app import ComposeResult
    from textual.binding import Binding
    from textual.containers import Horizontal, Vertical
    from textual.screen import ModalScreen
    from textual.widgets import Button, Footer, Input, Label, Select, Static

    _TEXTUAL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _TEXTUAL_AVAILABLE = False



# ---------------------------------------------------------------------------
# Supported export formats
# ---------------------------------------------------------------------------

EXPORT_FORMATS: list[tuple[str, str]] = [
    ("CSV (.csv)", "csv"),
    ("TSV (.tsv)", "tsv"),
    ("Parquet (.parquet)", "parquet"),
    ("JSON (.json)", "json"),
    ("JSON Lines (.jsonl)", "jsonl"),
    ("Clipboard (TSV)", "clipboard"),
]


# ---------------------------------------------------------------------------
# Pure state — no UI dependencies
# ---------------------------------------------------------------------------


@dataclass
class ExportDialogState:
    """Pure state for the export dialog. No UI dependencies."""

    row_count: int
    selected_format: str = "csv"
    file_path: str = ""

    def is_valid(self) -> bool:
        """Return True if the dialog has enough info to attempt an export."""
        if self.selected_format == "clipboard":
            return True
        return bool(self.file_path.strip())

    def format_label(self) -> str:
        """Return a human-readable label for the selected format."""
        for label, fmt in EXPORT_FORMATS:
            if fmt == self.selected_format:
                return label
        return self.selected_format


# ---------------------------------------------------------------------------
# Export result
# ---------------------------------------------------------------------------


@dataclass
class ExportResult:
    """Returned by ExportScreen on dismiss."""

    confirmed: bool
    format: str  # e.g. "csv", "tsv", "parquet", "json", "jsonl", "clipboard"
    file_path: str  # empty string for clipboard


# ---------------------------------------------------------------------------
# Textual screen
# ---------------------------------------------------------------------------

if _TEXTUAL_AVAILABLE:

    class ExportScreen(ModalScreen[ExportResult | None]):
        """Export dialog overlay.

        Accepts an Arrow table and its row count. On confirmation, dismisses
        with an ExportResult. On cancel, dismisses with None.

        The caller is responsible for performing the actual export and
        displaying RVT-865 on failure (Requirement 26.3).
        """

        BINDINGS = [
            Binding("escape", "cancel", "Cancel", show=True),
            Binding("ctrl+enter", "confirm", "Export", show=True),
        ]

        DEFAULT_CSS = """
        ExportScreen {
            align: center middle;
        }
        #export-container {
            width: 60;
            height: auto;
            border: thick $accent;
            background: $surface;
            padding: 1 2;
        }
        #export-title {
            height: 1;
            background: $accent;
            color: $text;
            content-align: center middle;
            text-style: bold;
            margin-bottom: 1;
        }
        #export-row-count {
            height: 1;
            color: $text-muted;
            margin-bottom: 1;
        }
        #format-label {
            height: 1;
            margin-bottom: 0;
        }
        #export-format {
            width: 1fr;
            margin-bottom: 1;
        }
        #path-label {
            height: 1;
            margin-bottom: 0;
        }
        #export-path {
            width: 1fr;
            margin-bottom: 1;
        }
        #export-buttons {
            height: 3;
            align: right middle;
        }
        #btn-cancel {
            margin-right: 1;
        }
        """

        def __init__(self, row_count: int, default_path: str = "") -> None:
            super().__init__()
            self._state = ExportDialogState(
                row_count=row_count,
                file_path=default_path,
            )

        def compose(self) -> ComposeResult:
            with Vertical(id="export-container"):
                yield Label("Export Data", id="export-title")
                yield Static(
                    f"{self._state.row_count:,} rows",
                    id="export-row-count",
                )
                yield Label("Format:", id="format-label")
                yield Select(
                    [(label, fmt) for label, fmt in EXPORT_FORMATS],
                    value=self._state.selected_format,
                    id="export-format",
                )
                yield Label("File path:", id="path-label")
                yield Input(
                    value=self._state.file_path,
                    placeholder="e.g. /tmp/results.csv  (empty for clipboard)",
                    id="export-path",
                )
                with Horizontal(id="export-buttons"):
                    yield Button("Cancel", variant="default", id="btn-cancel")
                    yield Button("Export", variant="primary", id="btn-export")
            yield Footer()

        def on_mount(self) -> None:
            self.query_one("#export-path", Input).focus()

        def on_select_changed(self, event: Select.Changed) -> None:
            if event.select.id == "export-format":
                self._state.selected_format = str(event.value)
                # Hide path input for clipboard
                path_input = self.query_one("#export-path", Input)
                path_label = self.query_one("#path-label", Label)
                is_clipboard = self._state.selected_format == "clipboard"
                path_input.display = not is_clipboard
                path_label.display = not is_clipboard

        def on_input_changed(self, event: Input.Changed) -> None:
            if event.input.id == "export-path":
                self._state.file_path = event.value

        def on_button_pressed(self, event: Button.Pressed) -> None:
            if event.button.id == "btn-cancel":
                self.action_cancel()
            elif event.button.id == "btn-export":
                self.action_confirm()

        def action_confirm(self) -> None:
            if not self._state.is_valid():
                return
            self.dismiss(
                ExportResult(
                    confirmed=True,
                    format=self._state.selected_format,
                    file_path=self._state.file_path.strip(),
                )
            )

        def action_cancel(self) -> None:
            self.dismiss(None)

else:  # pragma: no cover

    class ExportScreen:  # type: ignore[no-redef]
        """Stub when Textual is not installed."""

        def __init__(self, row_count: int, default_path: str = "") -> None:
            self._state = ExportDialogState(row_count=row_count, file_path=default_path)
