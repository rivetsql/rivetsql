"""Tests for editor validation using ReplState dialect.

Validates: Requirement 3.5 — when dialect changes, editor validation uses the new dialect.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from rivet_cli.repl.widgets.editor import EditorPanel, EditorTab, TabKind


def _make_panel(session=None) -> EditorPanel:
    """Create an EditorPanel without a Textual DOM."""
    panel = EditorPanel.__new__(EditorPanel)
    panel._session = session
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
    panel._tabs = [EditorTab(kind=TabKind.AD_HOC, title="Query 1", content="SELECT 1")]
    object.__setattr__(panel, "_reactive_active_tab_index", 0)
    from textual.css.query import NoMatches
    panel.query_one = MagicMock(side_effect=NoMatches())
    return panel


class TestEditorPanelRevalidate:
    """revalidate() triggers _schedule_validation."""

    def test_revalidate_calls_schedule_validation(self) -> None:
        panel = _make_panel()
        panel._schedule_validation = MagicMock()
        panel.revalidate()
        panel._schedule_validation.assert_called_once()

    def test_revalidate_is_public(self) -> None:
        """revalidate is accessible as a public method."""
        panel = _make_panel()
        assert callable(panel.revalidate)


class TestRunValidationUsesReplStateDialect:
    """_run_validation delegates to session.format_sql which uses repl_state.dialect."""

    def test_run_validation_calls_format_sql_without_explicit_dialect(self) -> None:
        """_run_validation calls session.format_sql(sql) with no dialect arg,
        so format_sql picks up repl_state.dialect internally."""
        mock_session = MagicMock()
        mock_session.format_sql.return_value = "SELECT 1"
        panel = _make_panel(session=mock_session)
        panel.get_current_sql = lambda: "SELECT 1"
        panel._show_validation_message = MagicMock()

        panel._run_validation()

        # format_sql called with just the SQL, no dialect kwarg
        mock_session.format_sql.assert_called_once_with("SELECT 1")

    def test_run_validation_shows_valid_on_success(self) -> None:
        mock_session = MagicMock()
        mock_session.format_sql.return_value = "SELECT 1"
        panel = _make_panel(session=mock_session)
        panel.get_current_sql = lambda: "SELECT 1"

        shown: list[str] = []
        panel._show_validation_message = lambda msg: shown.append(msg)

        panel._run_validation()
        assert shown[-1] == "✓ Valid SQL"

    def test_run_validation_shows_error_on_format_failure(self) -> None:
        mock_session = MagicMock()
        mock_session.format_sql.side_effect = ValueError("bad dialect syntax")
        panel = _make_panel(session=mock_session)
        panel.get_current_sql = lambda: "SELECT 1"

        shown: list[str] = []
        panel._show_validation_message = lambda msg: shown.append(msg)

        panel._run_validation()
        assert shown[-1].startswith("⚠")
        assert "bad dialect syntax" in shown[-1]


class TestDialectChangeTriggersRevalidation:
    """When dialect changes in app._on_dialect_selected, editor.revalidate() is called."""

    def _make_app(self, dialect: str | None = "spark"):
        from rivet_cli.repl.app import RivetRepl
        from rivet_core.interactive.types import ReplState

        app = RivetRepl.__new__(RivetRepl)
        app._keymap_error = None

        mock_session = MagicMock()
        mock_session.repl_state = ReplState(dialect=None)
        app._session = mock_session

        mock_editor = MagicMock()
        mock_status_bar = MagicMock()

        def fake_query_one(selector, widget_type=None):
            if "#editor-panel" in selector:
                return mock_editor
            if "#status-bar" in selector:
                return mock_status_bar
            return MagicMock()

        app.query_one = fake_query_one
        app.notify = MagicMock()

        return app, mock_editor, mock_session

    def test_dialect_selection_calls_revalidate(self) -> None:
        app, mock_editor, mock_session = self._make_app()
        app._on_dialect_selected("spark")
        mock_editor.revalidate.assert_called_once()

    def test_dialect_selection_calls_set_dialect_before_revalidate(self) -> None:
        """set_dialect is called before revalidate so validation uses the new dialect."""
        call_order: list[str] = []
        app, mock_editor, mock_session = self._make_app()
        mock_session.set_dialect.side_effect = lambda d: call_order.append("set_dialect")
        mock_editor.revalidate.side_effect = lambda: call_order.append("revalidate")

        app._on_dialect_selected("spark")

        assert call_order == ["set_dialect", "revalidate"]

    def test_dialect_selection_error_does_not_call_revalidate(self) -> None:
        """If set_dialect raises, revalidate should not be called."""
        from rivet_core.interactive.session import SessionError
        app, mock_editor, mock_session = self._make_app()
        mock_session.set_dialect.side_effect = SessionError("Unknown dialect")

        app._on_dialect_selected("invalid_dialect")

        mock_editor.revalidate.assert_not_called()

    def test_none_dialect_also_triggers_revalidate(self) -> None:
        """Resetting dialect to None also triggers revalidation."""
        app, mock_editor, mock_session = self._make_app()
        app._on_dialect_selected(None)
        mock_editor.revalidate.assert_called_once()
