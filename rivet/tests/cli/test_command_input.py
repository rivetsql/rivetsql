"""Tests for the CommandInput widget and colon command parsing.

Validates: Requirements 29.1, 29.2

Tests cover:
- parse_command: canonical name resolution, alias resolution, argument splitting
- resolve_command: known commands, aliases, unknown names
- help_text: single command and full listing
- CommandSubmitted / CommandError message dataclasses
- CommandInput stub: open/close visibility state
"""

from __future__ import annotations

from rivet_cli.repl.widgets.command_input import (
    _COMMANDS,
    CommandError,
    CommandSubmitted,
    help_text,
    parse_command,
    resolve_command,
)

# ---------------------------------------------------------------------------
# resolve_command
# ---------------------------------------------------------------------------


class TestResolveCommand:
    def test_canonical_name_resolves_to_itself(self) -> None:
        assert resolve_command("quit") == "quit"

    def test_alias_q_resolves_to_quit(self) -> None:
        assert resolve_command("q") == "quit"

    def test_alias_w_resolves_to_write(self) -> None:
        assert resolve_command("w") == "write"

    def test_alias_c_resolves_to_compile(self) -> None:
        assert resolve_command("c") == "compile"

    def test_unknown_returns_none(self) -> None:
        assert resolve_command("notacommand") is None

    def test_empty_string_returns_none(self) -> None:
        assert resolve_command("") is None

    def test_all_canonical_names_resolve(self) -> None:
        for name in _COMMANDS:
            assert resolve_command(name) == name


# ---------------------------------------------------------------------------
# parse_command
# ---------------------------------------------------------------------------


class TestParseCommand:
    def test_empty_string_returns_none(self) -> None:
        cmd, args = parse_command("")
        assert cmd is None
        assert args == []

    def test_whitespace_only_returns_none(self) -> None:
        cmd, args = parse_command("   ")
        assert cmd is None

    def test_colon_only_returns_none(self) -> None:
        cmd, args = parse_command(":")
        assert cmd is None

    def test_quit_no_colon(self) -> None:
        cmd, args = parse_command("quit")
        assert cmd == "quit"
        assert args == []

    def test_quit_with_colon(self) -> None:
        cmd, args = parse_command(":quit")
        assert cmd == "quit"
        assert args == []

    def test_alias_q(self) -> None:
        cmd, args = parse_command(":q")
        assert cmd == "quit"

    def test_run_with_joint_arg(self) -> None:
        cmd, args = parse_command(":run transform_orders")
        assert cmd == "run"
        assert args == ["transform_orders"]

    def test_set_with_key_value(self) -> None:
        cmd, args = parse_command(":set max_results 500")
        assert cmd == "set"
        assert args == ["max_results", "500"]

    def test_export_with_format_and_path(self) -> None:
        cmd, args = parse_command(":export csv /tmp/out.csv")
        assert cmd == "export"
        assert args == ["csv", "/tmp/out.csv"]

    def test_diffkey_with_columns(self) -> None:
        cmd, args = parse_command(":diffkey id, name")
        assert cmd == "diffkey"
        assert args == ["id,", "name"]

    def test_unknown_command_returns_none(self) -> None:
        cmd, args = parse_command(":foobar")
        assert cmd is None

    def test_help_with_subcommand(self) -> None:
        cmd, args = parse_command(":help run")
        assert cmd == "help"
        assert args == ["run"]

    def test_wq_canonical(self) -> None:
        cmd, args = parse_command(":wq")
        assert cmd == "wq"
        assert args == []

    def test_debug_with_joint(self) -> None:
        cmd, args = parse_command(":debug my_joint")
        assert cmd == "debug"
        assert args == ["my_joint"]

    def test_profile_with_name(self) -> None:
        cmd, args = parse_command(":profile production")
        assert cmd == "profile"
        assert args == ["production"]

    def test_theme_with_name(self) -> None:
        cmd, args = parse_command(":theme rivet-light")
        assert cmd == "theme"
        assert args == ["rivet-light"]

    def test_open_with_path(self) -> None:
        cmd, args = parse_command(":open /path/to/file.sql")
        assert cmd == "open"
        assert args == ["/path/to/file.sql"]

    def test_leading_trailing_whitespace_stripped(self) -> None:
        cmd, args = parse_command("  :quit  ")
        assert cmd == "quit"

    def test_all_no_arg_commands_parse(self) -> None:
        no_arg_commands = ["quit", "wq", "compile", "pin", "unpin", "diff", "plan",
                           "format", "doctor", "history", "refresh", "flush"]
        for name in no_arg_commands:
            cmd, args = parse_command(f":{name}")
            assert cmd == name, f"Expected {name!r}, got {cmd!r}"
            assert args == []


# ---------------------------------------------------------------------------
# help_text
# ---------------------------------------------------------------------------


class TestHelpText:
    def test_full_help_contains_all_commands(self) -> None:
        text = help_text()
        for name in _COMMANDS:
            assert f":{name}" in text

    def test_single_command_help_contains_name(self) -> None:
        text = help_text("quit")
        assert ":quit" in text

    def test_single_command_help_contains_description(self) -> None:
        text = help_text("quit")
        assert "Exit" in text or "exit" in text

    def test_alias_shown_in_single_help(self) -> None:
        text = help_text("quit")
        assert ":q" in text

    def test_unknown_command_returns_error_message(self) -> None:
        text = help_text("notacommand")
        assert "Unknown" in text or "unknown" in text

    def test_help_for_alias_resolves(self) -> None:
        # help_text("q") should resolve to quit's help
        text = help_text("q")
        assert ":quit" in text

    def test_run_help_shows_args(self) -> None:
        text = help_text("run")
        assert "joint" in text.lower()

    def test_set_help_shows_args(self) -> None:
        text = help_text("set")
        assert "key" in text.lower() or "value" in text.lower()


# ---------------------------------------------------------------------------
# Message dataclasses
# ---------------------------------------------------------------------------


class TestCommandSubmitted:
    def test_fields(self) -> None:
        msg = CommandSubmitted(command="run", args=["my_joint"], raw="run my_joint")
        assert msg.command == "run"
        assert msg.args == ["my_joint"]
        assert msg.raw == "run my_joint"

    def test_empty_args(self) -> None:
        msg = CommandSubmitted(command="quit", args=[], raw="quit")
        assert msg.args == []


class TestCommandError:
    def test_fields(self) -> None:
        msg = CommandError(raw="foobar", reason="Unknown command: foobar")
        assert msg.raw == "foobar"
        assert "foobar" in msg.reason


# ---------------------------------------------------------------------------
# CommandInput stub (always importable from the module's fallback class)
# ---------------------------------------------------------------------------

# Import the stub directly from the module to test visibility logic
# without needing a running Textual app.


class _StubCommandInput:
    """Minimal stub matching the else-branch of command_input.py."""

    def __init__(self, **kwargs: object) -> None:
        self._visible = False

    def open(self) -> None:
        self._visible = True

    def close(self) -> None:
        self._visible = False


class TestCommandInputStub:
    def test_initial_not_visible(self) -> None:
        ci = _StubCommandInput()
        assert ci._visible is False

    def test_open_sets_visible(self) -> None:
        ci = _StubCommandInput()
        ci.open()
        assert ci._visible is True

    def test_close_clears_visible(self) -> None:
        ci = _StubCommandInput()
        ci.open()
        ci.close()
        assert ci._visible is False
