"""Keymap support for the Rivet REPL TUI.

Provides built-in keymaps (vscode, vim, emacs) and supports custom keymaps
via Python entry points under the ``rivet.keymap`` group.

Falls back to the default keymap with a RVT-864 error if the requested
keymap is not found or invalid.

Requirements: 28.1, 28.2, 28.3, 28.4
"""

from __future__ import annotations

import importlib.metadata
import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# RVT-864: keymap not found or invalid
_RVT_864 = "RVT-864"


# ---------------------------------------------------------------------------
# Keymap data model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class KeyBinding:
    """A single key binding: key string → action identifier."""

    key: str
    action: str
    description: str = ""
    show: bool = True


@dataclass(frozen=True)
class Keymap:
    """A complete keymap: a mapping from action → key binding."""

    name: str
    bindings: tuple[KeyBinding, ...] = field(default_factory=tuple)

    def get_key(self, action: str) -> str | None:
        """Return the key string for *action*, or None if not bound."""
        for b in self.bindings:
            if b.action == action:
                return b.key
        return None

    def as_textual_bindings(self) -> list[tuple[str, str, str, bool]]:
        """Return list of (key, action, description, show) tuples for Textual."""
        return [(b.key, b.action, b.description, b.show) for b in self.bindings]


# ---------------------------------------------------------------------------
# Built-in keymaps
# ---------------------------------------------------------------------------

# VS Code-style (default) — matches the command palette shortcuts
_VSCODE_BINDINGS: tuple[KeyBinding, ...] = (
    KeyBinding("ctrl+q", "request_quit", "Quit", show=True),
    KeyBinding("ctrl+b", "toggle_catalog", "Toggle Catalog", show=True),
    KeyBinding("ctrl+backslash", "toggle_results", "Toggle Results", show=True),
    KeyBinding("f11", "fullscreen_panel", "Fullscreen", show=True),
    KeyBinding("ctrl+shift+p", "open_profile_selector", "Switch Profile", show=True),
    KeyBinding("ctrl+k", "open_command_palette", "Command Palette", show=True),
    KeyBinding("ctrl+n", "new_tab", "New Tab", show=True),
    KeyBinding("ctrl+w", "close_tab", "Close Tab", show=True),
    KeyBinding("ctrl+s", "save", "Save", show=True),
    KeyBinding("ctrl+shift+f", "format_sql", "Format SQL", show=True),
    KeyBinding("ctrl+f", "find", "Find", show=True),
    KeyBinding("ctrl+h", "view_history", "History", show=True),
    KeyBinding("ctrl+shift+e", "export_data", "Export", show=True),
    KeyBinding("ctrl+d", "diff_results", "Diff Results", show=True),
    KeyBinding("ctrl+shift+d", "profile_data", "Profile Data", show=True),
    KeyBinding("ctrl+e", "show_query_plan", "Query Plan", show=True),
    KeyBinding("ctrl+shift+k", "unpin_result", "Unpin Result", show=True),
    KeyBinding("f6", "run_all_sinks", "Run All Sinks", show=True),
    KeyBinding("shift+f6", "run_selection", "Run Selection", show=True),
    KeyBinding("f8", "run_tests", "Run Tests", show=True),
    KeyBinding("shift+f8", "run_tests_current", "Run Tests (Current)", show=True),
    KeyBinding("f9", "toggle_breakpoint", "Toggle Breakpoint", show=True),
    KeyBinding("f10", "debug_step", "Debug Step", show=True),
    KeyBinding("f5", "debug_continue", "Debug Continue", show=True),
    KeyBinding("shift+f5", "debug_stop", "Debug Stop", show=True),
    KeyBinding("ctrl+c", "cancel_execution", "Cancel", show=True),
    KeyBinding("ctrl+z", "undo", "Undo", show=False),
    KeyBinding("ctrl+shift+z", "redo", "Redo", show=False),
    KeyBinding("ctrl+tab", "next_tab", "Next Tab", show=False),
    KeyBinding("ctrl+shift+tab", "prev_tab", "Prev Tab", show=False),
)

# Vim-style keymap — modal-inspired shortcuts
_VIM_BINDINGS: tuple[KeyBinding, ...] = (
    KeyBinding("ctrl+q", "request_quit", "Quit", show=True),
    KeyBinding("ctrl+b", "toggle_catalog", "Toggle Catalog", show=True),
    KeyBinding("ctrl+backslash", "toggle_results", "Toggle Results", show=True),
    KeyBinding("f11", "fullscreen_panel", "Fullscreen", show=True),
    KeyBinding("ctrl+shift+p", "open_profile_selector", "Switch Profile", show=True),
    KeyBinding("ctrl+k", "open_command_palette", "Command Palette", show=True),
    KeyBinding("ctrl+n", "new_tab", "New Tab", show=True),
    KeyBinding("ctrl+w", "close_tab", "Close Tab", show=True),
    KeyBinding("ctrl+s", "save", "Save", show=True),
    KeyBinding("ctrl+shift+f", "format_sql", "Format SQL", show=True),
    KeyBinding("ctrl+f", "find", "Find", show=True),
    KeyBinding("ctrl+h", "view_history", "History", show=True),
    KeyBinding("ctrl+shift+e", "export_data", "Export", show=True),
    KeyBinding("ctrl+d", "diff_results", "Diff Results", show=True),
    KeyBinding("ctrl+shift+d", "profile_data", "Profile Data", show=True),
    KeyBinding("ctrl+e", "show_query_plan", "Query Plan", show=True),
    KeyBinding("ctrl+shift+k", "unpin_result", "Unpin Result", show=True),
    KeyBinding("f6", "run_all_sinks", "Run All Sinks", show=True),
    KeyBinding("shift+f6", "run_selection", "Run Selection", show=True),
    KeyBinding("f8", "run_tests", "Run Tests", show=True),
    KeyBinding("shift+f8", "run_tests_current", "Run Tests (Current)", show=True),
    KeyBinding("f9", "toggle_breakpoint", "Toggle Breakpoint", show=True),
    KeyBinding("f10", "debug_step", "Debug Step", show=True),
    KeyBinding("f5", "debug_continue", "Debug Continue", show=True),
    KeyBinding("shift+f5", "debug_stop", "Debug Stop", show=True),
    KeyBinding("ctrl+c", "cancel_execution", "Cancel", show=True),
    KeyBinding("u", "undo", "Undo", show=False),
    KeyBinding("ctrl+r", "redo", "Redo", show=False),
    KeyBinding("ctrl+tab", "next_tab", "Next Tab", show=False),
    KeyBinding("ctrl+shift+tab", "prev_tab", "Prev Tab", show=False),
)

# Emacs-style keymap
_EMACS_BINDINGS: tuple[KeyBinding, ...] = (
    KeyBinding("ctrl+q", "request_quit", "Quit", show=True),
    KeyBinding("ctrl+b", "toggle_catalog", "Toggle Catalog", show=True),
    KeyBinding("ctrl+backslash", "toggle_results", "Toggle Results", show=True),
    KeyBinding("f11", "fullscreen_panel", "Fullscreen", show=True),
    KeyBinding("ctrl+shift+p", "open_profile_selector", "Switch Profile", show=True),
    KeyBinding("ctrl+k", "open_command_palette", "Command Palette", show=True),
    KeyBinding("ctrl+x+ctrl+n", "new_tab", "New Tab", show=True),
    KeyBinding("ctrl+x+k", "close_tab", "Close Tab", show=True),
    KeyBinding("ctrl+x+ctrl+s", "save", "Save", show=True),
    KeyBinding("ctrl+shift+f", "format_sql", "Format SQL", show=True),
    KeyBinding("ctrl+s", "find", "Find", show=True),
    KeyBinding("ctrl+h", "view_history", "History", show=True),
    KeyBinding("ctrl+shift+e", "export_data", "Export", show=True),
    KeyBinding("ctrl+d", "diff_results", "Diff Results", show=True),
    KeyBinding("ctrl+shift+d", "profile_data", "Profile Data", show=True),
    KeyBinding("ctrl+e", "show_query_plan", "Query Plan", show=True),
    KeyBinding("ctrl+shift+k", "unpin_result", "Unpin Result", show=True),
    KeyBinding("f6", "run_all_sinks", "Run All Sinks", show=True),
    KeyBinding("shift+f6", "run_selection", "Run Selection", show=True),
    KeyBinding("f8", "run_tests", "Run Tests", show=True),
    KeyBinding("shift+f8", "run_tests_current", "Run Tests (Current)", show=True),
    KeyBinding("f9", "toggle_breakpoint", "Toggle Breakpoint", show=True),
    KeyBinding("f10", "debug_step", "Debug Step", show=True),
    KeyBinding("f5", "debug_continue", "Debug Continue", show=True),
    KeyBinding("shift+f5", "debug_stop", "Debug Stop", show=True),
    KeyBinding("ctrl+g", "cancel_execution", "Cancel", show=True),
    KeyBinding("ctrl+slash", "undo", "Undo", show=False),
    KeyBinding("ctrl+shift+slash", "redo", "Redo", show=False),
    KeyBinding("ctrl+tab", "next_tab", "Next Tab", show=False),
    KeyBinding("ctrl+shift+tab", "prev_tab", "Prev Tab", show=False),
)

_BUILTIN_KEYMAPS: dict[str, Keymap] = {
    "vscode": Keymap(name="vscode", bindings=_VSCODE_BINDINGS),
    "vim": Keymap(name="vim", bindings=_VIM_BINDINGS),
    "emacs": Keymap(name="emacs", bindings=_EMACS_BINDINGS),
}

DEFAULT_KEYMAP = _BUILTIN_KEYMAPS["vscode"]


# ---------------------------------------------------------------------------
# Keymap loader
# ---------------------------------------------------------------------------


def load_keymap(name: str) -> tuple[Keymap, str | None]:
    """Load a keymap by name.

    Returns ``(keymap, error_code)`` where *error_code* is ``None`` on
    success or ``"RVT-864"`` if the keymap was not found / invalid (in
    which case the default keymap is returned).

    Resolution order:
    1. Built-in keymaps: ``vscode``, ``vim``, ``emacs``
    2. Python entry points under the ``rivet.keymap`` group
    3. Fall back to default with RVT-864 error
    """
    # 1. Built-in
    if name in _BUILTIN_KEYMAPS:
        return _BUILTIN_KEYMAPS[name], None

    # 2. Entry points
    keymap = _load_from_entry_point(name)
    if keymap is not None:
        return keymap, None

    # 3. Fall back
    logger.warning(
        "%s: keymap %r not found; falling back to default keymap %r",
        _RVT_864,
        name,
        DEFAULT_KEYMAP.name,
    )
    return DEFAULT_KEYMAP, _RVT_864


def _load_from_entry_point(name: str) -> Keymap | None:
    """Attempt to load a keymap from the ``rivet.keymap`` entry point group."""
    try:
        eps = importlib.metadata.entry_points(group="rivet.keymap")
    except Exception:  # noqa: BLE001
        return None

    for ep in eps:
        if ep.name == name:
            try:
                factory: Any = ep.load()
                keymap = factory() if callable(factory) else factory
                if isinstance(keymap, Keymap):
                    return keymap
                # If it's a dict mapping action→key, convert it
                if isinstance(keymap, dict):
                    return _keymap_from_dict(name, keymap)
            except Exception:  # noqa: BLE001
                logger.warning(
                    "%s: failed to load keymap %r from entry point",
                    _RVT_864,
                    name,
                )
            return None
    return None


def _keymap_from_dict(name: str, mapping: dict[str, str]) -> Keymap:
    """Build a Keymap from a ``{action: key}`` dict."""
    bindings = tuple(
        KeyBinding(key=key, action=action)
        for action, key in mapping.items()
        if isinstance(key, str) and isinstance(action, str)
    )
    return Keymap(name=name, bindings=bindings)
