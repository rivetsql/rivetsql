"""Engine selector overlay for the Rivet REPL.

Triggered by Ctrl+Shift+E. Lets the user pick an engine for ad-hoc queries.
Joint execution always uses the compiled engine; only ad-hoc queries are
affected by this selection.

Requirements: 22.4
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Label, ListItem, ListView


class EngineSelectorScreen(ModalScreen[str | None]):
    """Modal overlay for selecting the ad-hoc query engine.

    Returns the selected engine name, or None if dismissed.
    """

    BINDINGS = [
        Binding("escape", "dismiss_none", "Cancel", show=True),
    ]

    DEFAULT_CSS = """
    EngineSelectorScreen {
        align: center middle;
    }
    EngineSelectorScreen #dialog {
        width: 50;
        height: auto;
        max-height: 20;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
    }
    EngineSelectorScreen #title {
        text-align: center;
        text-style: bold;
        margin-bottom: 1;
    }
    EngineSelectorScreen ListView {
        height: auto;
        max-height: 12;
    }
    """

    def __init__(
        self,
        engine_names: list[str],
        current_engine: str | None = None,
    ) -> None:
        super().__init__()
        self._engine_names = engine_names
        self._current_engine = current_engine

    def compose(self) -> ComposeResult:
        with Vertical(id="dialog"):
            yield Label("Select Ad-hoc Query Engine", id="title")
            items = []
            for name in self._engine_names:
                marker = " ✓" if name == self._current_engine else ""
                items.append(ListItem(Label(f"{name}{marker}"), id=f"engine-{name}"))
            yield ListView(*items, id="engine-list")

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        """Dismiss with the selected engine name."""
        item_id = event.item.id or ""
        if item_id.startswith("engine-"):
            engine_name = item_id[len("engine-"):]
            self.dismiss(engine_name)

    def action_dismiss_none(self) -> None:
        self.dismiss(None)
