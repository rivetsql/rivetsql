"""CommandInput widget for the Rivet REPL TUI.

Colon command input bar triggered by `:` (when the editor is not focused or
the cursor is at column 0 on an empty line).

Parses and dispatches all colon commands:
  :quit / :q        — exit the REPL
  :write / :w       — save current buffer
  :wq               — save and quit
  :compile / :c     — compile the project
  :run [joint]      — run all sinks or a specific joint
  :test [name]      — run tests (all or named)
  :profile <name>   — switch active profile
  :engine <name>    — switch ad-hoc query engine
  :theme <name>     — switch theme
  :set <key> <val>  — set a runtime config value
  :open <path>      — open a file in the editor
  :export <fmt> <p> — export results to file
  :pin              — pin current result
  :unpin            — clear pinned result
  :diff             — toggle diff view
  :diffkey <cols>   — set diff key columns
  :plan             — toggle query plan view
  :format           — format SQL in editor
  :doctor           — run diagnostics
  :history          — show execution history
  :refresh          — refresh catalog trees
  :flush            — flush material cache
  :help [command]   — show help
  :debug [joint]    — enter debug mode

Requirements: 29.1, 29.2
"""

from __future__ import annotations

from dataclasses import dataclass, field

try:
    from textual.app import ComposeResult
    from textual.binding import Binding
    from textual.message import Message
    from textual.reactive import reactive
    from textual.widget import Widget
    from textual.widgets import Input

    from rivet_cli.repl.accessibility import ARIA_COMMAND_INPUT

    _TEXTUAL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _TEXTUAL_AVAILABLE = False



# ---------------------------------------------------------------------------
# Command registry
# ---------------------------------------------------------------------------

# Maps canonical command name → (aliases, argument description, help text)
_COMMANDS: dict[str, tuple[list[str], str, str]] = {
    "quit": (["q"], "", "Exit the REPL"),
    "write": (["w"], "", "Save the current buffer"),
    "wq": ([], "", "Save and quit"),
    "compile": (["c"], "", "Compile the project"),
    "run": ([], "[joint]", "Run all sinks or a specific joint"),
    "test": ([], "[name]", "Run tests (all or named)"),
    "profile": ([], "<name>", "Switch active profile"),
    "engine": ([], "<name>", "Switch ad-hoc query engine"),
    "theme": ([], "<name>", "Switch theme"),
    "set": ([], "<key> <value>", "Set a runtime config value"),
    "open": ([], "<path>", "Open a file in the editor"),
    "export": ([], "<format> <path>", "Export results to file"),
    "pin": ([], "", "Pin the current result"),
    "unpin": ([], "", "Clear the pinned result"),
    "diff": ([], "", "Toggle diff view"),
    "diffkey": ([], "<col1[, col2]>", "Set diff key columns"),
    "plan": ([], "", "Toggle query plan view"),
    "format": ([], "", "Format SQL in the editor"),
    "doctor": ([], "", "Run diagnostics"),
    "history": ([], "", "Show execution history"),
    "refresh": ([], "", "Refresh catalog trees"),
    "flush": ([], "", "Flush the material cache"),
    "help": ([], "[command]", "Show help (optionally for a specific command)"),
    "debug": ([], "[joint]", "Enter debug mode"),
    "inspect": ([], "[target] [--engine <name>] [--tag <tag>] [--type <type>] [--full] [--export <path>] [--format json]", "Inspect compiled assembly"),
    "clear-logs": ([], "", "Clear execution logs"),
    "generate": ([], "<joint_name> [--description \"<text>\"]", "Generate a joint from the last executed query"),
}

# Reverse alias map: alias → canonical name
_ALIAS_MAP: dict[str, str] = {}
for _canonical, (_aliases, _, _) in _COMMANDS.items():
    for _alias in _aliases:
        _ALIAS_MAP[_alias] = _canonical


def resolve_command(name: str) -> str | None:
    """Return the canonical command name for *name*, or None if unknown."""
    if name in _COMMANDS:
        return name
    return _ALIAS_MAP.get(name)


def parse_command(text: str) -> tuple[str | None, list[str]]:
    """Parse a colon command string into (canonical_name, args).

    *text* may or may not include the leading ``:``.
    Returns ``(None, [])`` for empty or unrecognised input.
    """
    text = text.strip().lstrip(":")
    if not text:
        return None, []
    parts = text.split()
    canonical = resolve_command(parts[0])
    return canonical, parts[1:]


def help_text(command: str | None = None) -> str:
    """Return a help string for one command or all commands."""
    if command is not None:
        canonical = resolve_command(command)
        if canonical is None:
            return f"Unknown command: {command}"
        aliases, args, description = _COMMANDS[canonical]
        alias_str = f"  (aliases: :{', :'.join(aliases)})" if aliases else ""
        arg_str = f" {args}" if args else ""
        return f":{canonical}{arg_str} — {description}{alias_str}"

    lines = ["Available colon commands:"]
    for name, (aliases, args, description) in _COMMANDS.items():
        alias_str = f" (:{', :'.join(aliases)})" if aliases else ""
        arg_str = f" {args}" if args else ""
        lines.append(f"  :{name}{arg_str} — {description}{alias_str}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------

if _TEXTUAL_AVAILABLE:

    class CommandSubmitted(Message):
        """Posted when the user submits a colon command.

        Attributes:
            command:  Canonical command name (e.g. ``"quit"``).
            args:     Remaining tokens after the command name.
            raw:      The raw input string (without leading ``:``) as typed.
        """

        def __init__(self, *, command: str, args: list[str], raw: str) -> None:
            super().__init__()
            self.command = command
            self.args = args
            self.raw = raw

    class CommandError(Message):
        """Posted when the user submits an unrecognised command.

        Attributes:
            raw:     The raw input string as typed.
            reason:  Human-readable error description.
        """

        def __init__(self, *, raw: str, reason: str) -> None:
            super().__init__()
            self.raw = raw
            self.reason = reason

    # -----------------------------------------------------------------------
    # CommandInput widget
    # -----------------------------------------------------------------------

    class CommandInput(Widget):
        """Bottom-of-screen colon command input bar.

        Becomes visible when the user presses ``:`` outside the editor (or at
        column 0 on an empty line).  Hides itself after the command is
        submitted or dismissed.

        Posts :class:`CommandSubmitted` on a recognised command and
        :class:`CommandError` on an unrecognised one.
        """

        DEFAULT_CSS = """
        CommandInput {
            height: 1;
            background: $panel;
            color: $text;
            display: none;
        }
        CommandInput.visible {
            display: block;
        }
        CommandInput Input {
            height: 1;
            border: none;
            background: $panel;
            color: $text;
            padding: 0;
        }
        """

        BINDINGS = [
            Binding("escape", "dismiss", "Dismiss", show=False),
        ]

        _visible: reactive[bool] = reactive(False)

        # ------------------------------------------------------------------
        # Compose
        # ------------------------------------------------------------------

        def compose(self) -> ComposeResult:
            yield Input(placeholder="", id="cmd-input", tooltip=ARIA_COMMAND_INPUT)

        # ------------------------------------------------------------------
        # Public API
        # ------------------------------------------------------------------

        def open(self) -> None:
            """Show the command bar and focus the input."""
            self._visible = True
            self.add_class("visible")
            self.query_one("#cmd-input", Input).value = ":"
            self.query_one("#cmd-input", Input).focus()
            # Move cursor to end
            inp = self.query_one("#cmd-input", Input)
            inp.cursor_position = len(inp.value)

        def close(self) -> None:
            """Hide the command bar and clear the input."""
            self._visible = False
            self.remove_class("visible")
            self.query_one("#cmd-input", Input).value = ""

        # ------------------------------------------------------------------
        # Actions
        # ------------------------------------------------------------------

        def action_dismiss(self) -> None:
            """Escape — close without submitting."""
            self.close()

        # ------------------------------------------------------------------
        # Event handlers
        # ------------------------------------------------------------------

        def on_input_submitted(self, event: Input.Submitted) -> None:
            """Handle Enter in the input field."""
            event.stop()
            raw = event.value.strip().lstrip(":")
            self.close()

            canonical, args = parse_command(raw)
            if canonical is None:
                self.post_message(CommandError(raw=raw, reason=f"Unknown command: {raw.split()[0] if raw else '(empty)'}"))
            else:
                self.post_message(CommandSubmitted(command=canonical, args=args, raw=raw))

else:  # pragma: no cover — Textual not installed

    @dataclass
    class CommandSubmitted:  # type: ignore[no-redef]
        command: str
        args: list[str] = field(default_factory=list)
        raw: str = ""

    @dataclass
    class CommandError:  # type: ignore[no-redef]
        raw: str
        reason: str = ""

    class CommandInput:  # type: ignore[no-redef]
        """Stub CommandInput for environments without Textual installed."""

        def __init__(self, **kwargs: object) -> None:
            self._visible = False

        def open(self) -> None:
            self._visible = True

        def close(self) -> None:
            self._visible = False
