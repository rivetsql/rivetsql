"""ReplFooter widget for the Rivet REPL TUI.

Displays context-sensitive keybinding hints based on the currently focused
panel, and debug mode controls when a debug session is active.

Requirements: 3.3, 24.2
"""

from __future__ import annotations

try:
    from textual.app import ComposeResult
    from textual.message import Message
    from textual.reactive import reactive
    from textual.widget import Widget
    from textual.widgets import Label

    _TEXTUAL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _TEXTUAL_AVAILABLE = False


# ---------------------------------------------------------------------------
# Context hints per panel
# ---------------------------------------------------------------------------

_PANEL_HINTS: dict[str, str] = {
    "catalog-panel": "Enter: insert  F4: quick query  Ctrl+Enter: execute  /: search  F9: breakpoint",
    "editor-panel": "F5/Ctrl+Enter: run query  Ctrl+S: save  Ctrl+Shift+F: format  Ctrl+Space: complete",
    "results-panel": "Alt+1: result  Alt+2: compilation  Alt+3: logs  Ctrl+K: pin  Ctrl+D: diff  Ctrl+Shift+D: profile  Ctrl+E: plan  Ctrl+C: copy",
    "": "Ctrl+Q: quit  Ctrl+B: catalog  Ctrl+\\: results  F11: fullscreen  Ctrl+Shift+P: profile",
}

_DEBUG_HINTS = "F10: step  F5: continue  Shift+F5: stop  F9: breakpoint"


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------

if _TEXTUAL_AVAILABLE:

    class FocusedPanelChanged(Message):
        """Posted when the focused panel changes.

        Attributes:
            panel_id: The DOM id of the newly focused panel, or "" if none.
        """

        def __init__(self, panel_id: str) -> None:
            super().__init__()
            self.panel_id = panel_id

    class DebugModeChanged(Message):
        """Posted when debug mode is entered or exited.

        Attributes:
            active: True if debug mode is now active.
        """

        def __init__(self, active: bool) -> None:
            super().__init__()
            self.active = active


# ---------------------------------------------------------------------------
# ReplFooter
# ---------------------------------------------------------------------------

if _TEXTUAL_AVAILABLE:

    class ReplFooter(Widget):
        """Context-sensitive footer showing keybinding hints.

        Displays hints for the currently focused panel.  When a debug session
        is active the debug controls replace the normal hints.

        Requirements: 3.3, 24.2
        """

        DEFAULT_CSS = """
        ReplFooter {
            height: 1;
            background: $panel;
            color: $text-muted;
            padding: 0 1;
        }
        ReplFooter #hint-label {
            width: 1fr;
        }
        ReplFooter #debug-label {
            color: $warning;
        }
        """

        _focused_panel: reactive[str] = reactive("")
        _debug_active: reactive[bool] = reactive(False)

        def compose(self) -> ComposeResult:
            yield Label("", id="hint-label")

        def on_mount(self) -> None:
            self._refresh_hints()

        # ------------------------------------------------------------------
        # Message handlers
        # ------------------------------------------------------------------

        def on_focused_panel_changed(self, message: FocusedPanelChanged) -> None:
            self._focused_panel = message.panel_id
            self._refresh_hints()

        def on_debug_mode_changed(self, message: DebugModeChanged) -> None:
            self._debug_active = message.active
            self._refresh_hints()

        # ------------------------------------------------------------------
        # Internal
        # ------------------------------------------------------------------

        def _refresh_hints(self) -> None:
            label = self.query_one("#hint-label", Label)
            if self._debug_active:
                label.update(_DEBUG_HINTS)
                label.set_class(True, "debug-label")
            else:
                hint = _PANEL_HINTS.get(self._focused_panel, _PANEL_HINTS[""])
                label.update(hint)
                label.set_class(False, "debug-label")

        # ------------------------------------------------------------------
        # Public API
        # ------------------------------------------------------------------

        def set_focused_panel(self, panel_id: str) -> None:
            """Update the focused panel and refresh hints."""
            self._focused_panel = panel_id
            self._refresh_hints()

        def set_debug_mode(self, active: bool) -> None:
            """Enter or exit debug mode, updating hints accordingly."""
            self._debug_active = active
            self._refresh_hints()

else:  # pragma: no cover

    class ReplFooter:  # type: ignore[no-redef]
        """Stub when Textual is not installed."""

        def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            pass
