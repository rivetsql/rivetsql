"""Draggable panel splitter widgets for the Rivet REPL layout.

Provides:
  - VerticalSplitter: draggable bar between catalog panel and right pane
  - HorizontalSplitter: draggable bar between editor and results panels

Requirements: 3.4, 3.5
"""

from __future__ import annotations

from textual.events import MouseDown, MouseMove, MouseUp
from textual.widget import Widget


class VerticalSplitter(Widget):
    """Draggable vertical bar that resizes the catalog panel (left sibling).

    Constraints:
      - Catalog panel min width: 15 chars
      - Catalog panel max width: 40% of container
    """

    DEFAULT_CSS = """
    VerticalSplitter {
        width: 1;
        height: 100%;
        background: $panel;
    }
    VerticalSplitter:hover {
        background: $accent;
    }
    """

    _CATALOG_MIN = 15  # chars
    _CATALOG_MAX_PCT = 0.40

    def __init__(self, catalog_id: str, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self._catalog_id = catalog_id
        self._dragging = False
        self._drag_start_x = 0
        self._drag_start_width = 0

    def render(self) -> str:
        return ""

    def on_mouse_down(self, event: MouseDown) -> None:
        catalog = self.app.query_one(f"#{self._catalog_id}")
        self._drag_start_x = event.screen_x
        self._drag_start_width = catalog.size.width
        self._dragging = True
        self.capture_mouse()
        event.stop()

    def on_mouse_move(self, event: MouseMove) -> None:
        if not self._dragging:
            return
        delta = event.screen_x - self._drag_start_x
        new_width = self._drag_start_width + delta
        container_width = self.app.size.width
        max_width = max(self._CATALOG_MIN, int(container_width * self._CATALOG_MAX_PCT))
        new_width = max(self._CATALOG_MIN, min(max_width, new_width))
        catalog = self.app.query_one(f"#{self._catalog_id}")
        catalog.styles.width = new_width
        event.stop()

    def on_mouse_up(self, event: MouseUp) -> None:
        if self._dragging:
            self._dragging = False
            self.release_mouse()
            event.stop()


class HorizontalSplitter(Widget):
    """Draggable horizontal bar that resizes editor (top) and results (bottom).

    Default split is 50/50. Either panel can be collapsed to 0 (full-screen
    the other), but the splitter itself remains visible for restoration.
    """

    DEFAULT_CSS = """
    HorizontalSplitter {
        width: 100%;
        height: 1;
        background: $panel;
    }
    HorizontalSplitter:hover {
        background: $accent;
    }
    """

    _MIN_HEIGHT = 2  # minimum visible rows for each panel

    def __init__(self, top_id: str, bottom_id: str, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self._top_id = top_id
        self._bottom_id = bottom_id
        self._dragging = False
        self._drag_start_y = 0
        self._drag_start_top = 0

    def render(self) -> str:
        return ""

    def on_mouse_down(self, event: MouseDown) -> None:
        top = self.app.query_one(f"#{self._top_id}")
        self._drag_start_y = event.screen_y
        self._drag_start_top = top.size.height
        self._dragging = True
        self.capture_mouse()
        event.stop()

    def on_mouse_move(self, event: MouseMove) -> None:
        if not self._dragging:
            return
        delta = event.screen_y - self._drag_start_y
        top = self.app.query_one(f"#{self._top_id}")
        bottom = self.app.query_one(f"#{self._bottom_id}")
        total = top.size.height + bottom.size.height
        new_top = self._drag_start_top + delta
        # Allow collapsing to 0 (full-screen the other panel)
        new_top = max(0, min(total, new_top))
        new_bottom = total - new_top
        top.styles.height = new_top
        bottom.styles.height = new_bottom
        event.stop()

    def on_mouse_up(self, event: MouseUp) -> None:
        if self._dragging:
            self._dragging = False
            self.release_mouse()
            event.stop()
