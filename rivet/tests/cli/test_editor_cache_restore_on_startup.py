"""Tests for restoring editor content from ReplState on TUI startup.

Validates: Requirement 2.2
"""

from __future__ import annotations

from unittest.mock import MagicMock

from rivet_cli.repl.widgets.editor import EditorPanel, TabKind

# ---------------------------------------------------------------------------
# EditorPanel.restore_editor_sql
# ---------------------------------------------------------------------------


class TestRestoreEditorSql:
    """Unit tests for EditorPanel.restore_editor_sql."""

    def _make_panel(self, content: str = "") -> EditorPanel:
        panel = EditorPanel.__new__(EditorPanel)
        panel._session = None
        panel._show_line_numbers = True
        panel._tab_size = 2
        panel._word_wrap = False
        panel._completions = []
        panel._completion_index = 0
        panel._find_visible = False
        panel._replace_visible = False
        panel._validation_timer = None
        panel._validation_errors = []
        panel._suppress_autocomplete = False
        panel._executing = False
        # Add a default ad-hoc tab
        from rivet_cli.repl.widgets.editor import EditorTab
        panel._tabs = [EditorTab(kind=TabKind.AD_HOC, title="Query 1", content=content)]
        # active_tab_index is a reactive with default 0; access via _reactive_active_tab_index
        # to avoid triggering Textual's reactive machinery in unit tests
        object.__setattr__(panel, "_reactive_active_tab_index", 0)
        # Mock query_one to raise NoMatches (no DOM in unit test)
        from textual.css.query import NoMatches
        panel.query_one = MagicMock(side_effect=NoMatches())
        return panel

    def test_restore_sets_tab_content(self) -> None:
        """restore_editor_sql sets the active tab's content."""
        panel = self._make_panel()
        panel.restore_editor_sql("SELECT 42")
        assert panel._tabs[0].content == "SELECT 42"

    def test_restore_empty_sql_is_noop(self) -> None:
        """restore_editor_sql does nothing when sql is empty."""
        panel = self._make_panel("original")
        panel.restore_editor_sql("")
        assert panel._tabs[0].content == "original"

    def test_restore_noop_on_read_only_tab(self) -> None:
        """restore_editor_sql does nothing on a read-only tab."""
        from rivet_cli.repl.widgets.editor import EditorTab
        panel = self._make_panel()
        panel._tabs[0] = EditorTab(kind=TabKind.PREVIEW, title="Preview", content="old", read_only=True)
        panel.restore_editor_sql("SELECT 1")
        assert panel._tabs[0].content == "old"

    def test_restore_noop_when_no_tabs(self) -> None:
        """restore_editor_sql does nothing when there are no tabs."""
        panel = self._make_panel()
        panel._tabs = []
        # Should not raise
        panel.restore_editor_sql("SELECT 1")


# ---------------------------------------------------------------------------
# app.py on_mount wires restore_editor_sql
# ---------------------------------------------------------------------------


class TestOnMountRestoresEditorSql:
    """Verify on_mount calls restore_editor_sql when repl_state.editor_sql is set."""

    def _make_app_with_repl_state(self, editor_sql: str):
        from rivet_cli.repl.app import RivetRepl
        from rivet_core.interactive.types import ReplState

        app = RivetRepl.__new__(RivetRepl)
        app._keymap_error = None
        app._editor_path = None
        app._initial_sql = None
        app._file_watcher = None

        # Mock session with repl_state
        mock_session = MagicMock()
        mock_session.active_profile = "default"
        mock_session.repl_state = ReplState(editor_sql=editor_sql)
        app._session = mock_session

        # Mock config
        mock_config = MagicMock()
        mock_config.file_watch = False
        app._config = mock_config

        # Track restore_editor_sql calls
        mock_editor = MagicMock()
        MagicMock()

        def fake_query_one(selector, widget_type=None):
            if selector == "#editor-panel" or widget_type is not None and "EditorPanel" in str(widget_type):
                return mock_editor
            return MagicMock()

        app.query_one = fake_query_one
        app.notify = MagicMock()
        app._run_startup = MagicMock()

        return app, mock_editor

    def test_on_mount_restores_nonempty_editor_sql(self) -> None:
        """on_mount calls restore_editor_sql when repl_state.editor_sql is non-empty."""
        app, mock_editor = self._make_app_with_repl_state("SELECT 1")
        app.on_mount()
        mock_editor.restore_editor_sql.assert_called_once_with("SELECT 1")

    def test_on_mount_skips_restore_when_editor_sql_empty(self) -> None:
        """on_mount does not call restore_editor_sql when repl_state.editor_sql is empty."""
        app, mock_editor = self._make_app_with_repl_state("")
        app.on_mount()
        mock_editor.restore_editor_sql.assert_not_called()
