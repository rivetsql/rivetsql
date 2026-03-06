"""Dialect selector overlay for the Rivet REPL.

Triggered by Ctrl+Shift+D. Lets the user pick a SQL dialect for formatting,
validation, and transpilation. Independent of the engine selection.

Requirements: 3.1, 3.2
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Label, ListItem, ListView


class DialectSelectorScreen(ModalScreen[str | None]):
    """Modal overlay for selecting the SQL dialect.

    Returns the selected dialect name, or None if dismissed.
    """

    BINDINGS = [
        Binding("escape", "dismiss_none", "Cancel", show=True),
    ]

    DEFAULT_CSS = """
    DialectSelectorScreen {
        align: center middle;
    }
    DialectSelectorScreen #dialog {
        width: 50;
        height: auto;
        max-height: 20;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
    }
    DialectSelectorScreen #title {
        text-align: center;
        text-style: bold;
        margin-bottom: 1;
    }
    DialectSelectorScreen ListView {
        height: auto;
        max-height: 12;
    }
    """

    def __init__(
        self,
        dialect_names: list[str],
        auto_dialect: str | None = None,
        current_dialect: str | None = None,
    ) -> None:
        super().__init__()
        self._dialect_names = sorted(dialect_names)
        self._auto_dialect = auto_dialect
        self._current_dialect = current_dialect

    def compose(self) -> ComposeResult:
        with Vertical(id="dialog"):
            yield Label("Select SQL Dialect", id="title")
            items = []
            # Engine-inferred default at top with "(auto)" label
            auto_label = self._auto_dialect or "auto"
            marker = " ✓" if self._current_dialect is None else ""
            items.append(ListItem(Label(f"{auto_label} (auto){marker}"), id="dialect-__auto__"))
            # Remaining dialects alphabetically
            for name in self._dialect_names:
                marker = " ✓" if name == self._current_dialect else ""
                items.append(ListItem(Label(f"{name}{marker}"), id=f"dialect-{name}"))
            yield ListView(*items, id="dialect-list")

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        """Dismiss with the selected dialect name, or None for auto."""
        item_id = event.item.id or ""
        if item_id == "dialect-__auto__":
            self.dismiss(None)
        elif item_id.startswith("dialect-"):
            dialect_name = item_id[len("dialect-"):]
            self.dismiss(dialect_name)

    def action_dismiss_none(self) -> None:
        self.dismiss(None)
