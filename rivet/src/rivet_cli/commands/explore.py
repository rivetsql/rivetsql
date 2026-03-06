"""Interactive catalog explorer command."""

from __future__ import annotations

import logging
from enum import Enum, auto
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rivet_cli.rendering.explore_terminal import TerminalRenderer
    from rivet_core.catalog_explorer import (
        CatalogExplorer,
        ExplorerNode,
        GeneratedSource,
        NodeDetail,
        SearchResult,
    )

logger = logging.getLogger(__name__)


class Pane(Enum):
    """Active pane in the explorer UI."""

    TREE = auto()
    DETAIL = auto()
    SEARCH = auto()
    GENERATE = auto()


class ExploreController:
    """Manages interaction state for the interactive catalog explorer.

    Tracks cursor position, expansion state, active pane, and the visible
    node list. The main event loop reads keys, dispatches actions, and
    triggers re-renders via a pluggable renderer.

    Requirements: 13.1, 13.2, 13.3–13.10
    """

    def __init__(self, explorer: CatalogExplorer, renderer: TerminalRenderer | None = None) -> None:
        self._explorer = explorer
        self._renderer = renderer

        # Interaction state
        self.cursor: int = 0
        self.active_pane: Pane = Pane.TREE
        self.visible_nodes: list[ExplorerNode] = []
        self.expanded: set[tuple[str, ...]] = set()
        self.scroll_offset: int = 0
        self._running: bool = False

        # Action result state
        self.detail: NodeDetail | None = None
        self.repl_query: str | None = None
        self.search_query: str = ""
        self.search_results: list[SearchResult] = []
        self.search_cursor: int = 0
        self.generated_source: GeneratedSource | None = None
        self._generate_node: ExplorerNode | None = None
        self.error_message: str | None = None

        # Build initial visible node list from catalogs
        self._rebuild_visible_nodes()

    # ── Visible node list management ──────────────────────────────────

    def _rebuild_visible_nodes(self) -> None:
        """Rebuild the flat visible node list from current expansion state."""
        from rivet_core.catalog_explorer import ExplorerNode

        nodes: list[ExplorerNode] = []
        for info in self._explorer.list_catalogs():
            cat_path = (info.name,)
            node = ExplorerNode(
                name=info.name,
                node_type="catalog",
                path=[info.name],
                is_expandable=info.connected,
                depth=0,
                summary=None,
                depth_limit_reached=False,
            )
            nodes.append(node)
            if cat_path in self.expanded:
                self._collect_children(nodes, list(cat_path))
        self.visible_nodes = nodes
        # Clamp cursor
        if self.visible_nodes:
            self.cursor = min(self.cursor, len(self.visible_nodes) - 1)
        else:
            self.cursor = 0

    def _collect_children(self, nodes: list[ExplorerNode], path: list[str]) -> None:
        """Recursively collect expanded children into the flat node list."""
        children = self._explorer.list_children(path)
        for child in children:
            nodes.append(child)
            key = tuple(child.path)
            if child.is_expandable and key in self.expanded:
                self._collect_children(nodes, child.path)

    # ── Keyboard navigation ───────────────────────────────────────────

    def move_up(self) -> None:
        """Move cursor up one row (k / ↑)."""
        if self.cursor > 0:
            self.cursor -= 1
            self._adjust_scroll()

    def move_down(self) -> None:
        """Move cursor down one row (j / ↓)."""
        if self.cursor < len(self.visible_nodes) - 1:
            self.cursor += 1
            self._adjust_scroll()

    def jump_top(self) -> None:
        """Jump to first node (Home)."""
        self.cursor = 0
        self.scroll_offset = 0

    def jump_bottom(self) -> None:
        """Jump to last node (End)."""
        if self.visible_nodes:
            self.cursor = len(self.visible_nodes) - 1
        self._adjust_scroll()

    def page_up(self, page_size: int = 20) -> None:
        """Scroll up one page (Page Up)."""
        self.cursor = max(0, self.cursor - page_size)
        self._adjust_scroll()

    def page_down(self, page_size: int = 20) -> None:
        """Scroll down one page (Page Down)."""
        if self.visible_nodes:
            self.cursor = min(len(self.visible_nodes) - 1, self.cursor + page_size)
        self._adjust_scroll()

    def expand(self) -> None:
        """Expand the node at cursor (Enter / l)."""
        if not self.visible_nodes:
            return
        node = self.visible_nodes[self.cursor]
        if not node.is_expandable:
            return
        key = tuple(node.path)
        if key not in self.expanded:
            self.expanded.add(key)
            self._rebuild_visible_nodes()

    def collapse(self) -> None:
        """Collapse the node at cursor or move to parent (Backspace / h)."""
        if not self.visible_nodes:
            return
        node = self.visible_nodes[self.cursor]
        key = tuple(node.path)
        if key in self.expanded:
            self.expanded.discard(key)
            self._rebuild_visible_nodes()
        elif len(node.path) > 1:
            # Move cursor to parent
            parent_path = tuple(node.path[:-1])
            for i, n in enumerate(self.visible_nodes):
                if tuple(n.path) == parent_path:
                    self.cursor = i
                    break
            self._adjust_scroll()

    def _adjust_scroll(self) -> None:
        """Keep cursor within visible scroll window."""
        if self.cursor < self.scroll_offset:
            self.scroll_offset = self.cursor
        # Scroll window size is managed by the renderer; use a reasonable default
        elif self.cursor >= self.scroll_offset + 40:
            self.scroll_offset = self.cursor - 39

    # ── Action dispatch ──────────────────────────────────────────────

    def _current_node(self) -> ExplorerNode | None:
        """Return the node at cursor, or None if empty."""
        if not self.visible_nodes:
            return None
        return self.visible_nodes[self.cursor]

    def _is_table_or_view(self, node: ExplorerNode) -> bool:
        return node.node_type in ("table", "view", "file")

    def action_detail(self) -> None:
        """Show detail pane with schema + metadata (d key). Req 13.3."""
        node = self._current_node()
        if node is None or not self._is_table_or_view(node):
            return
        try:
            self.detail = self._explorer.get_node_detail(node.path)
            self.active_pane = Pane.DETAIL
            self.error_message = None
        except Exception as exc:
            self.error_message = str(exc)
            logger.debug("Detail failed: %s", exc)

    def action_stats(self) -> None:
        """Show column statistics in detail pane (s key). Req 13.4."""
        node = self._current_node()
        if node is None or not self._is_table_or_view(node):
            return
        try:
            stats = self._explorer.get_table_stats(node.path)
            self.detail = None
            # Build a NodeDetail with stats as metadata
            from rivet_core.catalog_explorer import NodeDetail

            self.detail = NodeDetail(
                node=node,
                schema=None,
                metadata=stats,
                children_count=None,
            )
            self.active_pane = Pane.DETAIL
            self.error_message = None
        except Exception as exc:
            self.error_message = str(exc)
            logger.debug("Stats failed: %s", exc)

    def action_preview(self) -> None:
        """Open preview in the REPL with SELECT * FROM table LIMIT 100 (p key)."""
        node = self._current_node()
        if node is None or not self._is_table_or_view(node):
            return
        table_ref = ".".join(node.path)
        self.repl_query = f"SELECT * FROM {table_ref} LIMIT 100"
        self._running = False

    def action_generate(self) -> None:
        """Open format selection for source generation (g key). Req 13.6."""
        node = self._current_node()
        if node is None or not self._is_table_or_view(node):
            return
        self._generate_node = node
        self.active_pane = Pane.GENERATE
        self.error_message = None

    def action_generate_confirm(self, fmt: str) -> None:
        """Generate source in chosen format and exit explorer to write file."""
        node = self._generate_node
        if node is None:
            return
        try:
            self.generated_source = self._explorer.generate_source(node.path, format=fmt)
            self.error_message = None
            self._running = False  # exit explorer to write the file
        except Exception as exc:
            self.error_message = str(exc)
            self.active_pane = Pane.TREE
            logger.debug("Generate failed: %s", exc)

    def action_search(self) -> None:
        """Open inline fuzzy search bar (/ key). Req 13.7."""
        self.search_query = ""
        self.search_results = []
        self.search_cursor = 0
        self.active_pane = Pane.SEARCH
        self.error_message = None

    def action_search_input(self, ch: str) -> None:
        """Handle a character typed in search mode."""
        if ch == "backspace":
            self.search_query = self.search_query[:-1]
        else:
            self.search_query += ch
        if self.search_query:
            self.search_results = self._explorer.search(self.search_query)
        else:
            self.search_results = []
        self.search_cursor = 0

    def action_search_confirm(self) -> None:
        """Navigate to selected search result (Enter in search mode). Req 13.7."""
        if self.search_results and self.search_cursor < len(self.search_results):
            result = self.search_results[self.search_cursor]
            path_parts = result.qualified_name.split(".")
            # Expand ancestors so the node is visible
            for i in range(1, len(path_parts)):
                self.expanded.add(tuple(path_parts[:i]))
            self._rebuild_visible_nodes()
            # Move cursor to the matching node
            for i, node in enumerate(self.visible_nodes):
                if node.path == path_parts:
                    self.cursor = i
                    break
        self.active_pane = Pane.TREE

    def action_refresh(self) -> None:
        """Refresh selected catalog (r key). Req 13.8."""
        node = self._current_node()
        if node is None:
            return
        catalog_name = node.path[0]
        try:
            self._explorer.refresh_catalog(catalog_name)
            # Clear expansion state for this catalog
            self.expanded = {k for k in self.expanded if k[0] != catalog_name}
            self._rebuild_visible_nodes()
            self.error_message = None
        except Exception as exc:
            self.error_message = str(exc)
            logger.debug("Refresh failed: %s", exc)

    # ── Event loop ────────────────────────────────────────────────────

    # Key constants for dispatch
    _KEY_MAP: dict[str, str] = {
        "k": "up",
        "j": "down",
        "h": "collapse",
        "l": "expand",
        "up": "up",
        "down": "down",
        "left": "collapse",
        "right": "expand",
        "enter": "expand",
        "backspace": "collapse",
        "home": "home",
        "end": "end",
        "page_up": "page_up",
        "page_down": "page_down",
        "q": "quit",
        "esc": "close_pane",
        "d": "detail",
        "s": "stats",
        "p": "preview",
        "g": "generate",
        "/": "search",
        "r": "refresh",
    }

    def handle_key(self, key: str) -> bool:
        """Dispatch a single key press. Returns False if the explorer should exit."""
        # In generate format selection mode
        if self.active_pane == Pane.GENERATE:
            if key == "esc" or key == "ctrl_c":
                self.active_pane = Pane.TREE
                return key != "ctrl_c"
            if key == "s":
                self.action_generate_confirm("sql")
                return self._running
            if key == "y":
                self.action_generate_confirm("yaml")
                return self._running
            return True

        # In search mode, most keys are typed into the search bar
        if self.active_pane == Pane.SEARCH:
            if key == "esc" or key == "ctrl_c":
                self.active_pane = Pane.TREE
                return key != "ctrl_c"
            if key == "enter":
                self.action_search_confirm()
                return True
            if key in ("up", "k"):
                if self.search_cursor > 0:
                    self.search_cursor -= 1
                return True
            if key in ("down", "j"):
                if self.search_cursor < len(self.search_results) - 1:
                    self.search_cursor += 1
                return True
            # Printable character or backspace
            if len(key) == 1 or key == "backspace":
                self.action_search_input(key)
            return True

        action = self._KEY_MAP.get(key)

        # In detail pane, collapse/back returns to tree
        if self.active_pane != Pane.TREE and action in ("collapse", "close_pane"):
            self.active_pane = Pane.TREE
            return True

        if action == "up":
            self.move_up()
        elif action == "down":
            self.move_down()
        elif action == "expand":
            self.expand()
        elif action == "collapse":
            self.collapse()
        elif action == "home":
            self.jump_top()
        elif action == "end":
            self.jump_bottom()
        elif action == "page_up":
            self.page_up()
        elif action == "page_down":
            self.page_down()
        elif action == "detail":
            self.action_detail()
        elif action == "stats":
            self.action_stats()
        elif action == "preview":
            self.action_preview()
        elif action == "generate":
            self.action_generate()
        elif action == "search":
            self.action_search()
        elif action == "refresh":
            self.action_refresh()
        elif action == "quit" or key == "ctrl_c":
            return False
        return True

    def run(self) -> None:
        """Main event loop: read key → dispatch → re-render.

        Requires a renderer with render_tree/render_detail/
        render_search and read_key methods.
        """
        if self._renderer is None:
            return
        self._running = True
        r = self._renderer
        if hasattr(r, "enter_raw"):
            r.enter_raw()
        try:
            while self._running:
                self._render()
                key = r.read_key()
                if not self.handle_key(key):
                    self._running = False
        except KeyboardInterrupt:
            self._running = False
        finally:
            if hasattr(r, "leave_raw"):
                r.leave_raw()
            r.show_cursor()
            r.clear()

    def _render(self) -> None:
        """Dispatch rendering to the appropriate pane."""
        r = self._renderer
        assert r is not None
        expanded_keys = {".".join(k) for k in self.expanded}
        node = self._current_node()
        on_table = node is not None and self._is_table_or_view(node)
        r.clear()
        r.hide_cursor()
        if self.active_pane == Pane.DETAIL and self.detail is not None:
            tree_width = r._cols // 2 if hasattr(r, "_cols") else 40
            r.render_tree(
                self.visible_nodes,
                self.cursor,
                self.scroll_offset,
                expanded=expanded_keys,
                tree_width=tree_width,
                cursor_on_table=on_table,
            )
            r.render_detail(self.detail, tree_width)
        elif self.active_pane == Pane.SEARCH:
            r.render_tree(
                self.visible_nodes,
                self.cursor,
                self.scroll_offset,
                expanded=expanded_keys,
                cursor_on_table=on_table,
            )
            r.render_search(self.search_query, self.search_results)
        elif self.active_pane == Pane.GENERATE:
            table_name = (
                ".".join(self._generate_node.path)
                if hasattr(self, "_generate_node") and self._generate_node is not None
                else ""
            )
            r.render_tree(
                self.visible_nodes,
                self.cursor,
                self.scroll_offset,
                expanded=expanded_keys,
                cursor_on_table=on_table,
            )
            r.render_generate_prompt(table_name)
        else:
            r.render_tree(
                self.visible_nodes,
                self.cursor,
                self.scroll_offset,
                expanded=expanded_keys,
                cursor_on_table=on_table,
            )
