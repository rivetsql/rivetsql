"""ResultsTabBar widget for the Rivet REPL TUI.

Horizontal tab bar with three tabs — Result, Compilation, Logs — at the
top of the Results_Panel.  Highlights the active tab with accent color and
underline, shows a badge count on the Logs tab for unread entries, and
supports both click and keyboard (Alt+1/2/3) tab switching.

Requirements: 1.1, 1.2, 1.3, 1.4, 1.5
"""

from __future__ import annotations

from enum import Enum
from typing import ClassVar

try:
    from textual.app import ComposeResult
    from textual.binding import Binding
    from textual.message import Message
    from textual.reactive import reactive
    from textual.widget import Widget
    from textual.widgets import Label

    _TEXTUAL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _TEXTUAL_AVAILABLE = False


class PanelTab(Enum):
    """Top-level tab in the Results_Panel."""

    RESULT = "Result"
    COMPILATION = "Compilation"
    LOGS = "Logs"


if _TEXTUAL_AVAILABLE:

    class TabChanged(Message):
        """Posted when the active tab changes."""

        def __init__(self, tab: PanelTab) -> None:
            super().__init__()
            self.tab = tab

    class ResultsTabBar(Widget):
        """Horizontal tab bar for the Results_Panel."""

        DEFAULT_CSS = """
        ResultsTabBar {
            layout: horizontal;
            height: 1;
            dock: top;
        }
        ResultsTabBar .tab {
            width: auto;
            padding: 0 2;
            content-align: center middle;
        }
        ResultsTabBar .tab--active {
            color: $accent;
            text-style: bold underline;
        }
        ResultsTabBar .tab--inactive {
            color: $text-muted;
        }
        ResultsTabBar .tab-badge {
            color: $warning;
            padding: 0 0 0 1;
            width: auto;
        }
        """

        BINDINGS: ClassVar[list[Binding]] = [  # type: ignore[assignment]
            Binding("alt+1", "tab_result", "Result", show=False),
            Binding("alt+2", "tab_compilation", "Compilation", show=False),
            Binding("alt+3", "tab_logs", "Logs", show=False),
        ]

        active_tab: reactive[PanelTab] = reactive(PanelTab.RESULT)
        log_badge_count: reactive[int] = reactive(0)

        def compose(self) -> ComposeResult:
            for tab in PanelTab:
                yield Label(tab.value, classes="tab tab--active" if tab == self.active_tab else "tab tab--inactive", id=f"tab-{tab.name.lower()}")
            yield Label("", classes="tab-badge", id="log-badge")

        def _refresh_tabs(self) -> None:
            for tab in PanelTab:
                label = self.query_one(f"#tab-{tab.name.lower()}", Label)
                if tab == self.active_tab:
                    label.set_classes("tab tab--active")
                else:
                    label.set_classes("tab tab--inactive")
            badge = self.query_one("#log-badge", Label)
            badge.update(f"({self.log_badge_count})" if self.log_badge_count > 0 else "")

        def watch_active_tab(self) -> None:
            try:
                self._refresh_tabs()
            except Exception:  # noqa: BLE001 — widget may not be mounted yet
                pass

        def watch_log_badge_count(self) -> None:
            try:
                badge = self.query_one("#log-badge", Label)
                badge.update(f"({self.log_badge_count})" if self.log_badge_count > 0 else "")
            except Exception:  # noqa: BLE001
                pass

        def _switch_tab(self, tab: PanelTab) -> None:
            if self.active_tab != tab:
                self.active_tab = tab
                self.post_message(TabChanged(tab))

        def on_click(self, event: object) -> None:
            # Textual Click events carry a widget reference; walk up to find the label
            from textual.events import Click

            assert isinstance(event, Click)
            for tab in PanelTab:
                widget = self.query_one(f"#tab-{tab.name.lower()}", Label)
                if widget.region.contains(event.screen_x, event.screen_y):
                    self._switch_tab(tab)
                    return

        def action_tab_result(self) -> None:
            self._switch_tab(PanelTab.RESULT)

        def action_tab_compilation(self) -> None:
            self._switch_tab(PanelTab.COMPILATION)

        def action_tab_logs(self) -> None:
            self._switch_tab(PanelTab.LOGS)
