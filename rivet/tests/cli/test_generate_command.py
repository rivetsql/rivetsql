"""Tests for :generate command parsing and dispatch in app.py.

Validates: Requirements 4.1, 4.7
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from rivet_cli.repl.widgets.command_input import _COMMANDS, parse_command, resolve_command

# ---------------------------------------------------------------------------
# Command registry
# ---------------------------------------------------------------------------


class TestGenerateCommandRegistry:
    def test_generate_is_registered(self) -> None:
        assert "generate" in _COMMANDS

    def test_generate_resolves(self) -> None:
        assert resolve_command("generate") == "generate"

    def test_generate_parses_with_name(self) -> None:
        cmd, args = parse_command(":generate my_joint")
        assert cmd == "generate"
        assert args == ["my_joint"]

    def test_generate_parses_with_description(self) -> None:
        cmd, args = parse_command(":generate my_joint --description some text here")
        assert cmd == "generate"
        assert args == ["my_joint", "--description", "some", "text", "here"]


# ---------------------------------------------------------------------------
# _handle_generate_command dispatch
# ---------------------------------------------------------------------------


class TestHandleGenerateCommand:
    """Test _handle_generate_command in RivetRepl without a running Textual app."""

    def _make_app(self, generate_joint_result=None, generate_joint_error=None):
        from rivet_cli.repl.app import RivetRepl

        app = RivetRepl.__new__(RivetRepl)

        session = MagicMock()
        if generate_joint_error:
            session.generate_joint.side_effect = generate_joint_error
        else:
            session.generate_joint.return_value = generate_joint_result or Path("/fake/joints/my_joint.sql")
        app._session = session

        app._notifications = []
        app.notify = lambda msg, severity="information": app._notifications.append((msg, severity))

        return app

    def test_no_args_shows_error(self) -> None:
        app = self._make_app()
        app._handle_generate_command([])
        assert any("requires a joint name" in msg for msg, _ in app._notifications)
        assert any(sev == "error" for _, sev in app._notifications)
        app._session.generate_joint.assert_not_called()

    def test_name_only_calls_generate_joint(self) -> None:
        path = Path("/fake/joints/my_joint.sql")
        app = self._make_app(generate_joint_result=path)
        app._handle_generate_command(["my_joint"])
        app._session.generate_joint.assert_called_once_with("my_joint", None)

    def test_name_only_shows_success_notification(self) -> None:
        path = Path("/fake/joints/my_joint.sql")
        app = self._make_app(generate_joint_result=path)
        app._handle_generate_command(["my_joint"])
        assert any("my_joint" in msg for msg, _ in app._notifications)
        assert not any(sev == "error" for _, sev in app._notifications)

    def test_with_description_calls_generate_joint(self) -> None:
        path = Path("/fake/joints/my_joint.sql")
        app = self._make_app(generate_joint_result=path)
        app._handle_generate_command(["my_joint", "--description", "some", "text"])
        app._session.generate_joint.assert_called_once_with("my_joint", "some text")

    def test_description_flag_without_value_shows_error(self) -> None:
        app = self._make_app()
        app._handle_generate_command(["my_joint", "--description"])
        assert any(sev == "error" for _, sev in app._notifications)
        app._session.generate_joint.assert_not_called()

    def test_session_error_shows_error_notification(self) -> None:
        from rivet_core.interactive.session import SessionError
        app = self._make_app(generate_joint_error=SessionError("Joint 'my_joint' already exists."))
        app._handle_generate_command(["my_joint"])
        assert any(sev == "error" for _, sev in app._notifications)
        assert any("already exists" in msg for msg, _ in app._notifications)

    def test_no_query_error_shows_error_notification(self) -> None:
        from rivet_core.interactive.session import SessionError
        app = self._make_app(generate_joint_error=SessionError("No executed query to generate from. Run a query first."))
        app._handle_generate_command(["my_joint"])
        assert any(sev == "error" for _, sev in app._notifications)
        assert any("No executed query" in msg for msg, _ in app._notifications)

    def test_success_notification_contains_file_path(self) -> None:
        path = Path("/fake/joints/my_joint.sql")
        app = self._make_app(generate_joint_result=path)
        app._handle_generate_command(["my_joint"])
        assert any(str(path) in msg for msg, _ in app._notifications)
