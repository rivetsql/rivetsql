"""CatalogPanel widget — unified tree of source catalogs and pipeline joints.

Displays source catalogs (ordered as declared) followed by a visual divider
and a Joints section. Catalogs are populated asynchronously; joints come from
the CompiledAssembly in topological order.

Requirements: 4.1–4.10, 5.1–5.4, 6.1, 6.3–6.7, 18.4, 39.1, 39.3, 33.2, 33.4
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from textual import on, work
from textual.binding import Binding
from textual.message import Message
from textual.reactive import reactive
from textual.timer import Timer
from textual.widget import Widget
from textual.widgets import Input, Tree
from textual.widgets.tree import TreeNode

from rivet_cli.repl.accessibility import ARIA_CATALOG_SEARCH, ARIA_CATALOG_TREE
from rivet_cli.repl.widgets.status_bar import ActivityChanged
from rivet_core.interactive.types import Activity_State

if TYPE_CHECKING:
    from textual.app import ComposeResult

    from rivet_core.catalog_explorer import CatalogInfo
    from rivet_core.checks import CompiledCheck
    from rivet_core.compiler import CompiledJoint
    from rivet_core.interactive.session import InteractiveSession

# Joint type → icon mapping
JOINT_TYPE_ICONS: dict[str, str] = {
    "source": "⚪",
    "sql": "🔵",
    "python": "🟣",
    "sink": "🟢",
}

# Execution status → icon mapping
EXECUTION_STATUS_ICONS: dict[str, str] = {
    "success": "✅",
    "failed": "❌",
    "executing": "⏳",
    "warning": "⚠️",
}

# Check phase → symbol mapping
CHECK_SYMBOLS: dict[str, str] = {
    "assertion": "◆",
    "audit": "●",
}


@dataclass
class CatalogNodeData:
    """Data attached to catalog tree nodes."""

    node_kind: str  # "catalog", "schema", "table", "column", "divider", "joints_root",
    #                  "joint", "check", "retry"
    catalog_name: str | None = None
    schema_name: str | None = None
    table_name: str | None = None
    column_name: str | None = None
    column_type: str | None = None
    nullable: bool | None = None
    joint_name: str | None = None
    joint_type: str | None = None
    check: CompiledCheck | None = None
    check_passed: bool | None = None
    check_message: str | None = None
    qualified_name: str | None = None
    connected: bool = True
    error: str | None = None
    explorer_path: list[str] | None = None  # path for CatalogExplorer.list_children


class CatalogPanel(Widget):
    """Unified tree panel showing source catalogs and pipeline joints.

    The panel delegates all logic to InteractiveSession and only handles
    layout, styling, and user input.
    """

    BINDINGS = [
        Binding("enter", "activate_node", "Open / Insert", show=False),
        Binding("shift+enter", "extract_joint_sql", "Extract SQL", show=False),
        Binding("f4", "preview_node", "Preview", show=False),
        Binding("ctrl+enter", "execute_node", "Execute", show=False),
        Binding("space", "expand_metadata", "Expand", show=False),
        Binding("f9", "toggle_breakpoint", "Breakpoint", show=False),
        Binding("slash", "start_search", "Search", show=False),
        Binding("escape", "close_search", "Close search", show=False),
    ]

    search_visible: reactive[bool] = reactive(False)

    # --- Messages ---

    class JointSelected(Message):
        """Posted when a joint is selected (Enter on joint node)."""

        def __init__(self, joint_name: str) -> None:
            super().__init__()
            self.joint_name = joint_name

    class InsertName(Message):
        """Posted when a table/column name should be inserted into the editor."""

        def __init__(self, qualified_name: str) -> None:
            super().__init__()
            self.qualified_name = qualified_name

    class QuickQuery(Message):
        """Posted when F4 is pressed — runs SELECT * FROM {name}."""

        def __init__(self, sql: str) -> None:
            super().__init__()
            self.sql = sql

    class ExecuteJointRequested(Message):
        """Posted when Ctrl+Enter is pressed on a joint."""

        def __init__(self, joint_name: str) -> None:
            super().__init__()
            self.joint_name = joint_name

    class ShowCheckViolations(Message):
        """Posted when Enter is pressed on a failed quality check."""

        def __init__(self, joint_name: str, check: CompiledCheck) -> None:
            super().__init__()
            self.joint_name = joint_name
            self.check = check

    class JointPreviewRequested(Message):
        """Posted after 300ms debounce when cursor rests on a joint node."""

        def __init__(self, joint_name: str) -> None:
            super().__init__()
            self.joint_name = joint_name

    class JointSqlExtractRequested(Message):
        """Posted when Shift+Enter is pressed on a joint node."""

        def __init__(self, joint_name: str) -> None:
            super().__init__()
            self.joint_name = joint_name

    def __init__(
        self,
        session: InteractiveSession,
        *,
        profile: str = "default",
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        super().__init__(name=name, id=id, classes=classes)
        self._session = session
        self._profile = profile
        self._joint_statuses: dict[str, str] = {}
        self._breakpoints: set[str] = set()
        self._search_query: str = ""
        self._executing: bool = False
        self._preview_timer: Timer | None = None

    def compose(self) -> ComposeResult:
        yield Input(placeholder="Search catalog…", id="catalog-search", classes="hidden")
        yield Tree("Catalog", id="catalog-tree")

    def on_mount(self) -> None:
        tree = self.query_one("#catalog-tree", Tree)
        tree.show_root = False
        tree.guide_depth = 3
        # ARIA-style labels — Requirement 35.1
        tree.tooltip = ARIA_CATALOG_TREE
        self.query_one("#catalog-search", Input).tooltip = ARIA_CATALOG_SEARCH
        self.refresh_tree()

    @on(Tree.NodeHighlighted)
    def _on_node_highlighted(self, event: Tree.NodeHighlighted) -> None:  # type: ignore[type-arg]
        """Debounce cursor movement; post JointPreviewRequested after 300ms on a joint."""
        if self._preview_timer is not None:
            self._preview_timer.stop()
            self._preview_timer = None

        data = event.node.data
        if (
            not isinstance(data, CatalogNodeData)
            or data.node_kind != "joint"
            or not data.joint_name
        ):
            return

        joint_name = data.joint_name

        def _fire() -> None:
            self._preview_timer = None
            self.post_message(self.JointPreviewRequested(joint_name))

        self._preview_timer = self.set_timer(0.3, _fire)

    # --- Public API ---

    def get_selected_joint_name(self) -> str | None:
        """Return the joint name at the cursor, or None."""
        tree = self.query_one("#catalog-tree", Tree)
        node = tree.cursor_node
        if node is None:
            return None
        data = node.data
        if isinstance(data, CatalogNodeData) and data.node_kind == "joint":
            return data.joint_name
        return None

    def refresh_tree(self) -> None:
        """Rebuild the tree from session data."""
        self._build_tree_sync()
        self._load_catalogs_async()

    def update_joint_status(self, joint_name: str, status: str) -> None:
        """Update execution status icon for a joint."""
        self._joint_statuses[joint_name] = status
        self._rebuild_joints_section()

    def update_check_result(
        self, joint_name: str, check_type: str, passed: bool, message: str
    ) -> None:
        """Update a quality check result inline."""
        # Re-render joints section to reflect check results
        self._rebuild_joints_section()

    def switch_profile(self, new_profile: str) -> None:
        """Switch to a new profile, invalidating the old profile's cache entries."""
        self._profile = new_profile
        self.refresh_tree()

    # --- Tree building ---

    def _build_tree_sync(self) -> None:
        """Build the initial tree structure with joints (sync, fast).

        Also populates catalog nodes from cache if available, so the tree
        is immediately useful before live introspection completes.
        """
        tree = self.query_one("#catalog-tree", Tree)
        tree.clear()
        self._populate_catalogs_from_cache()
        self._add_joints_section(tree)

    def _populate_catalogs_from_cache(self) -> None:
        """Populate catalog nodes from SmartCache via the explorer (fast, no network).

        Shows top-level catalog entries so the tree is immediately useful
        before live introspection completes. The CatalogExplorer reads
        from SmartCache in READ_WRITE mode, providing warm-start data.
        """
        tree = self.query_one("#catalog-tree", Tree)
        catalogs = self._session.get_catalogs()
        for cat in catalogs:
            data = CatalogNodeData(
                node_kind="catalog",
                catalog_name=cat.name,
                connected=cat.connected,
                error=cat.error,
                explorer_path=[cat.name],
            )
            icon = "📁" if cat.connected else "⚠️"
            label = f"{icon} {cat.name} (cached)"
            tree.root.add(label, data=data, expand=False, allow_expand=True)

    @work(thread=True)
    async def _load_catalogs_async(self) -> None:
        """Load catalog data asynchronously and update the tree."""
        catalogs = self._session.get_catalogs()
        self.app.call_from_thread(self._populate_catalogs, catalogs)

    def _populate_catalogs(self, catalogs: list[CatalogInfo]) -> None:
        """Populate catalog nodes in the tree (called on main thread).

        Saves each connected catalog's node data to the disk cache.
        """
        tree = self.query_one("#catalog-tree", Tree)

        # Rebuild entire tree with live data
        tree.clear()

        for cat in catalogs:
            data = CatalogNodeData(
                node_kind="catalog",
                catalog_name=cat.name,
                connected=cat.connected,
                error=cat.error,
                explorer_path=[cat.name],
            )
            icon = "📁" if cat.connected else "⚠️"
            label = f"{icon} {cat.name}"
            if not cat.connected:
                label += " (connection failed)"
            node = tree.root.add(label, data=data, expand=False, allow_expand=True)

            if not cat.connected:
                retry_data = CatalogNodeData(
                    node_kind="retry",
                    catalog_name=cat.name,
                )
                node.add_leaf("🔄 Retry connection", data=retry_data)

        # Add divider
        divider_data = CatalogNodeData(node_kind="divider")
        tree.root.add_leaf("─" * 20, data=divider_data)

        # Re-add joints section
        self._add_joints_section(tree)

    def _add_joints_section(self, tree: Tree) -> None:  # type: ignore[type-arg]
        """Add the Joints section to the tree."""
        joints = self._session.get_joints()
        if not joints:
            return

        joints_data = CatalogNodeData(node_kind="joints_root")
        joints_root = tree.root.add("Joints", data=joints_data, expand=True)

        for joint in joints:
            self._add_joint_node(joints_root, joint)

    def _add_joint_node(self, parent: TreeNode, joint: CompiledJoint) -> None:  # type: ignore[type-arg]
        """Add a single joint node with its quality checks."""
        status = self._joint_statuses.get(joint.name)
        icon = (
            EXECUTION_STATUS_ICONS.get(status, "")
            if status
            else JOINT_TYPE_ICONS.get(joint.type, "")
        )
        bp = "🔴 " if joint.name in self._breakpoints else ""
        label = f"{bp}{icon} {joint.name}"

        data = CatalogNodeData(
            node_kind="joint",
            joint_name=joint.name,
            joint_type=joint.type,
            qualified_name=joint.name,
        )
        has_checks = bool(joint.checks)
        node = parent.add(label, data=data, expand=False, allow_expand=has_checks)

        # Add quality checks nested under the joint
        for check in joint.checks:
            self._add_check_node(node, check, joint.name)

    def _add_check_node(
        self,
        parent: TreeNode,
        check: CompiledCheck,
        joint_name: str,  # type: ignore[type-arg]
    ) -> None:
        """Add a quality check node under a joint."""
        symbol = CHECK_SYMBOLS.get(check.phase, "◆")
        severity = f"[{check.severity}]"
        cols = check.config.get("columns", check.config.get("expression", ""))
        label = f"{symbol} {check.type} {cols} {severity}"

        data = CatalogNodeData(
            node_kind="check",
            joint_name=joint_name,
            check=check,
        )
        parent.add_leaf(label, data=data)

    def _rebuild_joints_section(self) -> None:
        """Rebuild only the joints section of the tree."""
        tree = self.query_one("#catalog-tree", Tree)
        # Find and remove existing joints root, then re-add
        for child in list(tree.root.children):
            data = child.data
            if isinstance(data, CatalogNodeData) and data.node_kind == "joints_root":
                child.remove()
                break
        self._add_joints_section(tree)

    # --- Lazy catalog expansion ---

    @on(Tree.NodeExpanded)
    def _on_node_expanded(self, event: Tree.NodeExpanded) -> None:  # type: ignore[type-arg]
        """Lazy-load children when a catalog/table/schema node is expanded."""
        data = event.node.data
        if not isinstance(data, CatalogNodeData):
            return

        # Only load if expandable, connected, has an explorer_path, and not already loaded
        if data.explorer_path and data.connected and not event.node.children:
            self._load_catalog_children(event.node, data.explorer_path)

    @work(thread=True)
    async def _load_catalog_children(self, node: TreeNode, explorer_path: list[str]) -> None:  # type: ignore[type-arg]
        """Load children for a catalog/table/schema node asynchronously."""
        try:
            self.app.call_from_thread(self._add_loading_indicator, node)
            children = self._session.list_children(explorer_path)
            self.app.call_from_thread(self._populate_children, node, children)
        except Exception:
            self.app.call_from_thread(self._remove_loading_indicator, node)

    def _add_loading_indicator(self, node: TreeNode) -> None:  # type: ignore[type-arg]
        if not node.children:
            node.add_leaf("⏳ Loading…", data=CatalogNodeData(node_kind="divider"))

    def _remove_loading_indicator(self, node: TreeNode) -> None:  # type: ignore[type-arg]
        for child in list(node.children):
            data = child.data
            if isinstance(data, CatalogNodeData) and data.node_kind == "divider":
                child.remove()

    def _populate_children(self, node: TreeNode, children: list) -> None:  # type: ignore[type-arg]
        """Populate tree node with ExplorerNode children (called on main thread)."""
        self._remove_loading_indicator(node)
        if not children:
            return

        _NODE_ICONS = {
            "schema": "📁",
            "database": "📁",
            "directory": "📁",
            "table": "📋",
            "view": "📋",
            "file": "📋",
            "column": "📊",
        }

        for child in children:
            icon = _NODE_ICONS.get(child.node_type, "")
            label = f"{icon} {child.name}"

            # Map explorer node_type to our CatalogNodeData node_kind
            kind = child.node_type
            if kind in ("database", "directory"):
                kind = "schema"
            elif kind in ("view", "file"):
                kind = "table"

            # Build qualified name for insert
            qualified = ".".join(child.path)

            data = CatalogNodeData(
                node_kind=kind,
                catalog_name=child.path[0] if child.path else None,
                table_name=child.name if kind == "table" else None,
                column_name=child.name if kind == "column" else None,
                qualified_name=qualified,
                explorer_path=child.path,
                connected=True,
            )
            node.add(
                label,
                data=data,
                expand=False,
                allow_expand=child.is_expandable,
            )

    # --- Search ---

    def action_start_search(self) -> None:
        """Open the inline search bar."""
        self.search_visible = True
        search_input = self.query_one("#catalog-search", Input)
        search_input.remove_class("hidden")
        search_input.value = ""
        search_input.focus()

    def action_close_search(self) -> None:
        """Close the search bar and restore the full tree."""
        self.search_visible = False
        search_input = self.query_one("#catalog-search", Input)
        search_input.add_class("hidden")
        self._search_query = ""
        self._build_tree_sync()
        self._load_catalogs_async()

    @on(Input.Changed, "#catalog-search")
    def _on_search_changed(self, event: Input.Changed) -> None:
        """Filter the tree as the user types."""
        self._search_query = event.value
        if not event.value:
            self._build_tree_sync()
            self._load_catalogs_async()
            return
        self._apply_search_filter(event.value)

    @on(Input.Submitted, "#catalog-search")
    def _on_search_submitted(self, event: Input.Submitted) -> None:
        """Navigate to the selected search result."""
        tree = self.query_one("#catalog-tree", Tree)
        if tree.cursor_node is not None:
            data = tree.cursor_node.data
            if isinstance(data, CatalogNodeData) and data.qualified_name:
                self.post_message(self.InsertName(data.qualified_name))
        self.action_close_search()

    def _apply_search_filter(self, query: str) -> None:
        """Filter tree nodes using the session's catalog search."""
        results = self._session.search_catalog(query)
        tree = self.query_one("#catalog-tree", Tree)
        tree.clear()

        for result in results:
            icon = {
                "joint": "🔵",
                "table": "📋",
                "column": "📊",
                "schema": "📁",
                "catalog": "🗄️",
            }.get(result.kind, "")
            label = f"{icon} {result.qualified_name}"
            data = CatalogNodeData(
                node_kind=result.kind,
                qualified_name=result.qualified_name,
                joint_name=result.short_name if result.kind == "joint" else None,
            )
            tree.root.add_leaf(label, data=data)

    # --- Keybinding actions ---

    def action_activate_node(self) -> None:
        """Handle Enter: insert name for table/column, open SQL for joint."""
        tree = self.query_one("#catalog-tree", Tree)
        node = tree.cursor_node
        if node is None:
            return
        data = node.data
        if not isinstance(data, CatalogNodeData):
            return

        if data.node_kind == "joint" and data.joint_name:
            self.post_message(self.JointSelected(data.joint_name))
        elif data.node_kind in ("table", "column") and data.qualified_name:
            self.post_message(self.InsertName(data.qualified_name))
        elif data.node_kind == "check" and data.joint_name and data.check:
            self.post_message(self.ShowCheckViolations(data.joint_name, data.check))
        elif data.node_kind == "retry" and data.catalog_name:
            self._load_catalogs_async()

    def action_extract_joint_sql(self) -> None:
        """Handle Shift+Enter: extract joint SQL into a new editable ad-hoc tab."""
        tree = self.query_one("#catalog-tree", Tree)
        node = tree.cursor_node
        if node is None:
            return
        data = node.data
        if (
            not isinstance(data, CatalogNodeData)
            or data.node_kind != "joint"
            or not data.joint_name
        ):
            return
        self.post_message(self.JointSqlExtractRequested(data.joint_name))

    def action_preview_node(self) -> None:
        """Handle F4: run SELECT * FROM {name} for the selected node."""
        tree = self.query_one("#catalog-tree", Tree)
        node = tree.cursor_node
        if node is None:
            return
        data = node.data
        if not isinstance(data, CatalogNodeData):
            return

        name: str | None = None
        if data.node_kind == "joint" and data.joint_name:
            name = data.joint_name
        elif data.node_kind == "table" and data.qualified_name:
            name = data.qualified_name

        if name:
            self.post_message(self.QuickQuery(f"SELECT * FROM {name} LIMIT 100"))

    def action_execute_node(self) -> None:
        """Handle Ctrl+Enter: execute joint and upstream."""
        if self._executing:
            return
        tree = self.query_one("#catalog-tree", Tree)
        node = tree.cursor_node
        if node is None:
            return
        data = node.data
        if not isinstance(data, CatalogNodeData) or data.node_kind != "joint":
            return
        if data.joint_name:
            self.post_message(self.ExecuteJointRequested(data.joint_name))

    def action_expand_metadata(self) -> None:
        """Handle Space: expand inline metadata for a joint."""
        tree = self.query_one("#catalog-tree", Tree)
        node = tree.cursor_node
        if node is None:
            return
        data = node.data
        if not isinstance(data, CatalogNodeData) or data.node_kind != "joint":
            return
        # Toggle expansion
        node.toggle()

    def action_toggle_breakpoint(self) -> None:
        """Handle F9: toggle breakpoint on a joint."""
        tree = self.query_one("#catalog-tree", Tree)
        node = tree.cursor_node
        if node is None:
            return
        data = node.data
        if not isinstance(data, CatalogNodeData) or data.node_kind != "joint":
            return
        if data.joint_name:
            if data.joint_name in self._breakpoints:
                self._breakpoints.discard(data.joint_name)
            else:
                self._breakpoints.add(data.joint_name)
            self._rebuild_joints_section()

    # --- Activity state guard ---

    def on_activity_changed(self, message: ActivityChanged) -> None:
        """React to ActivityChanged — disable execute and show ⏳ when EXECUTING."""
        if message.state == Activity_State.EXECUTING:
            self._executing = True
        else:
            self._executing = False
            # Clear "executing" status icons, restoring type-based icons
            for name in [k for k, v in self._joint_statuses.items() if v == "executing"]:
                del self._joint_statuses[name]
            self._rebuild_joints_section()
