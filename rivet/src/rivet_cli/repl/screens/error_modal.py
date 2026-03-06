"""ErrorModal — modal overlay for REPL errors (RVT-860 through RVT-867).

Displays error code, description, actionable remediation, and three actions:
  [Copy error]     — copy the formatted error text to the clipboard
  [Show in editor] — open the relevant file in the editor (if applicable)
  [Dismiss]        — close the modal

Requirements: 34.1, 34.2
"""

from __future__ import annotations

from typing import TYPE_CHECKING

try:
    from textual.app import ComposeResult
    from textual.binding import Binding
    from textual.containers import Horizontal, Vertical
    from textual.screen import ModalScreen
    from textual.widgets import Button, Label, Static

    _TEXTUAL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _TEXTUAL_AVAILABLE = False

if TYPE_CHECKING:
    from ..errors import ReplError


# ---------------------------------------------------------------------------
# Pure state — no UI dependencies
# ---------------------------------------------------------------------------


class ErrorModalState:
    """Pure state for the error modal. No UI dependencies."""

    def __init__(self, error: ReplError, show_in_editor: bool = False) -> None:
        self.error = error
        self.show_in_editor = show_in_editor

    def header_text(self) -> str:
        return f"Error {self.error.code}"

    def description_text(self) -> str:
        return self.error.description

    def remediation_text(self) -> str:
        return self.error.remediation

    def copy_text(self) -> str:
        return self.error.format_text()


# ---------------------------------------------------------------------------
# Textual screen
# ---------------------------------------------------------------------------

if _TEXTUAL_AVAILABLE:

    class ErrorModal(ModalScreen[str | None]):
        """Modal overlay for REPL errors.

        Dismisses with:
          "copy"        — user clicked [Copy error]
          "show_editor" — user clicked [Show in editor]
          None          — user dismissed
        """

        BINDINGS = [
            Binding("escape", "dismiss_modal", "Dismiss", show=True),
        ]

        DEFAULT_CSS = """
        ErrorModal {
            align: center middle;
        }
        #error-container {
            width: 70;
            height: auto;
            border: thick $error;
            background: $surface;
            padding: 1 2;
        }
        #error-header {
            height: 1;
            background: $error;
            color: $text;
            content-align: center middle;
            text-style: bold;
            margin-bottom: 1;
        }
        #error-description {
            height: auto;
            margin-bottom: 1;
            color: $text;
        }
        #error-remediation-label {
            height: 1;
            color: $text-muted;
            text-style: italic;
            margin-bottom: 0;
        }
        #error-remediation {
            height: auto;
            margin-bottom: 1;
            color: $text;
        }
        #error-buttons {
            height: 3;
            align: right middle;
        }
        #btn-copy {
            margin-right: 1;
        }
        #btn-show-editor {
            margin-right: 1;
        }
        """

        def __init__(self, error: ReplError, show_in_editor: bool = False) -> None:
            super().__init__()
            self._state = ErrorModalState(error=error, show_in_editor=show_in_editor)

        def compose(self) -> ComposeResult:
            with Vertical(id="error-container"):
                yield Label(self._state.header_text(), id="error-header")
                yield Static(self._state.description_text(), id="error-description")
                yield Label("Remediation:", id="error-remediation-label")
                yield Static(self._state.remediation_text(), id="error-remediation")
                with Horizontal(id="error-buttons"):
                    yield Button("Copy error", variant="default", id="btn-copy")
                    if self._state.show_in_editor:
                        yield Button("Show in editor", variant="default", id="btn-show-editor")
                    yield Button("Dismiss", variant="primary", id="btn-dismiss")

        def on_button_pressed(self, event: Button.Pressed) -> None:
            if event.button.id == "btn-copy":
                self.dismiss("copy")
            elif event.button.id == "btn-show-editor":
                self.dismiss("show_editor")
            elif event.button.id == "btn-dismiss":
                self.dismiss(None)

        def action_dismiss_modal(self) -> None:
            self.dismiss(None)
