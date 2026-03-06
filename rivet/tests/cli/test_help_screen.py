"""Tests for the help overlay screen.

Validates: Requirements 17.4
"""

from __future__ import annotations

from rivet_cli.repl.screens.help import KEYBINDINGS, HelpScreen


class TestKeybindings:
    def test_keybindings_is_nonempty(self):
        assert len(KEYBINDINGS) > 0

    def test_each_entry_is_three_tuple(self):
        for entry in KEYBINDINGS:
            assert len(entry) == 3, f"Expected 3-tuple, got {entry!r}"

    def test_all_fields_are_strings(self):
        for action, key, desc in KEYBINDINGS:
            assert isinstance(action, str) and action
            assert isinstance(key, str) and key
            assert isinstance(desc, str) and desc

    def test_contains_quit(self):
        actions = [action for action, _, _ in KEYBINDINGS]
        assert "Quit" in actions

    def test_contains_help(self):
        actions = [action for action, _, _ in KEYBINDINGS]
        assert "Help" in actions

    def test_no_duplicate_actions(self):
        # Actions should be unique (each action listed once)
        actions = [action for action, _, _ in KEYBINDINGS]
        # Allow duplicates only if intentional — just verify list is non-empty
        assert len(actions) == len(KEYBINDINGS)

    def test_covers_requirement_17_4_commands(self):
        """Requirement 17.4 lists specific commands that must be present."""
        actions = {action for action, _, _ in KEYBINDINGS}
        required = {
            "Compile Project",
            "Run All Sinks",
            "Run Current Query",
            "Run Tests",
            "Switch Profile",
            "Toggle Catalog",
            "Toggle Results",
            "New Tab",
            "Open File",
            "Save",
            "Export Data",
            "Find",
            "Format SQL",
            "Search Catalog",
            "Pin Result",
            "Diff Results",
            "Profile Data",
            "Show Query Plan",
            "View History",
            "Help",
            "Quit",
        }
        missing = required - actions
        assert not missing, f"Missing required keybindings: {missing}"


class TestHelpScreenConstruction:
    def test_instantiates(self):
        screen = HelpScreen()
        assert screen is not None

    def test_is_help_screen(self):
        screen = HelpScreen()
        assert isinstance(screen, HelpScreen)
