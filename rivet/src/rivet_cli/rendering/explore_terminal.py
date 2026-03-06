"""ANSI terminal renderer for the interactive catalog explorer.

Uses raw terminal mode via tty/termios for keyboard input and direct
ANSI escape codes for rendering. No Textual, no curses.

Requirements: 13.11
"""

from __future__ import annotations

import os
import sys
import termios
import tty
from typing import IO, Any

from rivet_core.catalog_explorer import ExplorerNode, NodeDetail, SearchResult

# ANSI escape sequences
_ESC = "\033["
_CLEAR_SCREEN = f"{_ESC}2J"
_CURSOR_HOME = f"{_ESC}H"
_CLEAR_LINE = f"{_ESC}2K"
_HIDE_CURSOR = f"{_ESC}?25l"
_SHOW_CURSOR = f"{_ESC}?25h"
_BOLD = f"{_ESC}1m"
_DIM = f"{_ESC}2m"
_REVERSE = f"{_ESC}7m"
_RESET = f"{_ESC}0m"
_CYAN = f"{_ESC}36m"
_YELLOW = f"{_ESC}33m"
_GREEN = f"{_ESC}32m"
_BLUE = f"{_ESC}34m"
_MAGENTA = f"{_ESC}35m"

# Node type icons
_ICONS: dict[str, str] = {
    "catalog": "📦",
    "database": "🗄️",
    "schema": "📂",
    "table": "📋",
    "view": "👁️",
    "file": "📄",
    "column": "│",
    "directory": "📁",
}


def _move_to(row: int, col: int) -> str:
    return f"{_ESC}{row};{col}H"


def _truncate(text: str, width: int) -> str:
    if len(text) <= width:
        return text
    return text[: width - 1] + "…"


class TerminalRenderer:
    """ANSI terminal renderer for the interactive explorer."""

    _NL = "\r\n"  # raw-mode safe newline

    def __init__(self, output: IO[str] | None = None) -> None:
        self._out = output or sys.stdout
        self._rows = 24
        self._cols = 80
        self._refresh_size()
        self._old_termios: list[Any] | None = None

    def _refresh_size(self) -> None:
        try:
            size = os.get_terminal_size(self._out.fileno())
            self._rows = size.lines
            self._cols = size.columns
        except (OSError, ValueError):
            pass

    def _write(self, text: str) -> None:
        self._out.write(text)

    def _flush(self) -> None:
        self._out.flush()

    def enter_raw(self) -> None:
        """Enter raw terminal mode for the entire explorer session."""
        try:
            fd = sys.stdin.fileno()
            self._old_termios = termios.tcgetattr(fd)
            tty.setraw(fd)
        except (OSError, ValueError):
            self._old_termios = None

    def leave_raw(self) -> None:
        """Restore original terminal mode."""
        if self._old_termios is not None:
            try:
                fd = sys.stdin.fileno()
                termios.tcsetattr(fd, termios.TCSADRAIN, self._old_termios)
            except (OSError, ValueError):
                pass
            self._old_termios = None

    def clear(self) -> None:
        self._write(f"{_CLEAR_SCREEN}{_CURSOR_HOME}")
        self._flush()

    def hide_cursor(self) -> None:
        self._write(_HIDE_CURSOR)
        self._flush()

    def show_cursor(self) -> None:
        self._write(_SHOW_CURSOR)
        self._flush()

    def render_tree(
        self,
        nodes: list[ExplorerNode],
        cursor: int,
        scroll_offset: int = 0,
        expanded: set[str] | None = None,
        tree_width: int | None = None,
        cursor_on_table: bool = False,
    ) -> None:
        """Draw tree with indentation, icons, cursor highlight, scroll position."""
        self._refresh_size()
        nl = self._NL
        width = tree_width or self._cols
        visible_rows = self._rows - 2  # reserve status line + header

        self._write(_CURSOR_HOME)
        # Header
        header = _truncate(" Catalog Explorer", width)
        self._write(f"{_BOLD}{_REVERSE}{header:<{width}}{_RESET}{nl}")

        exp = expanded or set()
        for i in range(visible_rows):
            idx = scroll_offset + i
            self._write(_CLEAR_LINE)
            if idx < len(nodes):
                node = nodes[idx]
                icon = _ICONS.get(node.node_type, " ")
                indent = "  " * node.depth
                if node.is_expandable:
                    path_key = ".".join(node.path)
                    arrow = "▼ " if path_key in exp else "▶ "
                else:
                    arrow = "  "
                label = f"{indent}{arrow}{icon} {node.name}"
                if node.depth_limit_reached:
                    label += f" {_DIM}(depth limit){_RESET}"
                label = _truncate(label, width)
                if idx == cursor:
                    self._write(f"{_REVERSE}{label:<{width}}{_RESET}{nl}")
                else:
                    color = _type_color(node.node_type)
                    self._write(f"{color}{label}{_RESET}{nl}")
            else:
                self._write(f"{'~':<{width}}{nl}")

        # Status bar
        total = len(nodes)
        pos = f" {cursor + 1}/{total}" if total else " (empty)"
        table_hints = "  d:detail  p:preview  g:generate" if cursor_on_table else ""
        status = _truncate(f" j/k:move  Enter:expand{table_hints}  /:search  q:quit{pos}", width)
        self._write(f"{_CLEAR_LINE}{_REVERSE}{status:<{width}}{_RESET}")
        self._flush()

    def render_detail(self, detail: NodeDetail, tree_width: int) -> None:
        """Draw detail pane alongside tree (right split)."""
        self._refresh_size()
        pane_width = self._cols - tree_width
        if pane_width < 20:
            return
        start_col = tree_width + 1

        row = 1
        # Title
        title = _truncate(f" {detail.node.name} ({detail.node.node_type})", pane_width)
        self._write(f"{_move_to(row, start_col)}{_BOLD}{_CYAN}{title:<{pane_width}}{_RESET}")
        row += 1

        if detail.schema is not None:
            self._write(
                f"{_move_to(row, start_col)}{_BOLD}{'  Name':<20}{'Type':<16}{'Null':<6}{_RESET}"
            )
            row += 1
            for col in detail.schema.columns:
                if row >= self._rows - 1:
                    self._write(f"{_move_to(row, start_col)}{_DIM}  ...more columns{_RESET}")
                    break
                nullable = "✓" if col.nullable else ""
                line = f"  {col.name:<20}{col.type:<16}{nullable:<6}"
                self._write(f"{_move_to(row, start_col)}{_truncate(line, pane_width)}")
                row += 1

        if detail.metadata is not None:
            row += 1
            if row < self._rows - 1:
                self._write(f"{_move_to(row, start_col)}{_BOLD}  Metadata{_RESET}")
                row += 1
                meta = detail.metadata
                for label, val in [
                    ("Rows", meta.row_count),
                    ("Size", f"{meta.size_bytes} bytes" if meta.size_bytes else None),
                    ("Format", meta.format),
                    ("Owner", meta.owner),
                ]:
                    if val is not None and row < self._rows - 1:
                        line = f"  {label}: {val}"
                        self._write(f"{_move_to(row, start_col)}{_truncate(line, pane_width)}")
                        row += 1

        self._flush()

    def render_search(self, query: str, results: list[SearchResult]) -> None:
        """Draw inline search bar with ranked results."""
        self._refresh_size()
        width = self._cols

        # Position search at bottom
        bar_row = self._rows - 1
        self._write(f"{_move_to(bar_row, 1)}{_CLEAR_LINE}")
        prompt = _truncate(f"/{query}█", width)
        self._write(f"{_BOLD}{prompt}{_RESET}")

        # Results above the search bar
        max_results = min(len(results), self._rows - 3)
        for i in range(max_results):
            r = results[i]
            row = bar_row - max_results + i
            self._write(f"{_move_to(row, 1)}{_CLEAR_LINE}")
            icon = _ICONS.get(r.node_type, " ")
            line = _truncate(f"  {icon} {r.qualified_name}  {_DIM}({r.kind}){_RESET}", width)
            self._write(line)

        self._flush()

    def render_generate_prompt(self, table_name: str) -> None:
        """Draw format selection prompt at the bottom of the screen."""
        self._refresh_size()
        width = self._cols
        bar_row = self._rows - 1
        self._write(f"{_move_to(bar_row, 1)}{_CLEAR_LINE}")
        prompt = _truncate(
            f" Generate source for {_CYAN}{table_name}{_RESET}  "
            f"{_BOLD}s{_RESET}:SQL  {_BOLD}y{_RESET}:YAML  Esc:cancel",
            width + 40,  # account for ANSI codes in length
        )
        self._write(f"{_REVERSE} Generate {_RESET} {prompt}")
        self._flush()

    def read_key(self, input_fd: int | None = None) -> str:
        """Read single keypress, handle escape sequences for arrow/special keys.

        Assumes the terminal is already in raw mode (via enter_raw).
        """
        fd = input_fd if input_fd is not None else sys.stdin.fileno()
        ch = os.read(fd, 1).decode("utf-8", errors="replace")
        if ch == "\x1b":
            # Escape sequence
            ch2 = os.read(fd, 1).decode("utf-8", errors="replace")
            if ch2 == "[":
                ch3 = os.read(fd, 1).decode("utf-8", errors="replace")
                return {
                    "A": "up",
                    "B": "down",
                    "C": "right",
                    "D": "left",
                    "H": "home",
                    "F": "end",
                    "5": "page_up",
                    "6": "page_down",
                }.get(ch3, "escape")
            return "escape"
        if ch == "\r" or ch == "\n":
            return "enter"
        if ch == "\x7f" or ch == "\x08":
            return "backspace"
        if ch == "\x03":
            return "ctrl_c"
        return ch


def _type_color(node_type: str) -> str:
    return {
        "catalog": _BOLD,
        "database": _BLUE,
        "schema": _CYAN,
        "table": _GREEN,
        "view": _MAGENTA,
        "file": _YELLOW,
        "column": _DIM,
        "directory": _BLUE,
    }.get(node_type, "")
