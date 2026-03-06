"""Tests for ExploreController — task 18.1."""

from __future__ import annotations

from unittest.mock import MagicMock

from rivet_cli.commands.explore import ExploreController, Pane
from rivet_core.catalog_explorer import CatalogExplorer, CatalogInfo, ExplorerNode

# ── Helpers ───────────────────────────────────────────────────────────


def _make_explorer(catalog_infos: list[CatalogInfo] | None = None, children: list[ExplorerNode] | None = None) -> MagicMock:
    """Create a mock CatalogExplorer."""
    explorer = MagicMock(spec=CatalogExplorer)
    if catalog_infos is None:
        catalog_infos = [
            CatalogInfo(name="pg", catalog_type="postgres", connected=True, error=None),
            CatalogInfo(name="ddb", catalog_type="duckdb", connected=True, error=None),
        ]
    explorer.list_catalogs.return_value = catalog_infos
    explorer.list_children.return_value = children or []
    return explorer


def _node(name: str, path: list[str], node_type: str = "table", expandable: bool = False, depth: int = 1) -> ExplorerNode:
    return ExplorerNode(
        name=name, node_type=node_type, path=path,
        is_expandable=expandable, depth=depth, summary=None, depth_limit_reached=False,
    )


# ── Initial state ────────────────────────────────────────────────────


class TestInitialState:
    def test_initial_cursor_at_zero(self):
        ctrl = ExploreController(_make_explorer())
        assert ctrl.cursor == 0

    def test_initial_pane_is_tree(self):
        ctrl = ExploreController(_make_explorer())
        assert ctrl.active_pane == Pane.TREE

    def test_visible_nodes_populated_from_catalogs(self):
        ctrl = ExploreController(_make_explorer())
        assert len(ctrl.visible_nodes) == 2
        assert ctrl.visible_nodes[0].name == "pg"
        assert ctrl.visible_nodes[1].name == "ddb"

    def test_empty_catalogs(self):
        ctrl = ExploreController(_make_explorer(catalog_infos=[]))
        assert ctrl.visible_nodes == []
        assert ctrl.cursor == 0


# ── Cursor movement ──────────────────────────────────────────────────


class TestCursorMovement:
    def test_move_down(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.move_down()
        assert ctrl.cursor == 1

    def test_move_down_at_bottom_stays(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.move_down()
        ctrl.move_down()  # already at last
        assert ctrl.cursor == 1

    def test_move_up(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.move_down()
        ctrl.move_up()
        assert ctrl.cursor == 0

    def test_move_up_at_top_stays(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.move_up()
        assert ctrl.cursor == 0

    def test_jump_top(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.move_down()
        ctrl.jump_top()
        assert ctrl.cursor == 0

    def test_jump_bottom(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.jump_bottom()
        assert ctrl.cursor == 1

    def test_page_up(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.cursor = 1
        ctrl.page_up()
        assert ctrl.cursor == 0

    def test_page_down(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.page_down()
        assert ctrl.cursor == 1

    def test_move_on_empty_list(self):
        ctrl = ExploreController(_make_explorer(catalog_infos=[]))
        ctrl.move_down()
        assert ctrl.cursor == 0
        ctrl.move_up()
        assert ctrl.cursor == 0


# ── Expand / Collapse ────────────────────────────────────────────────


class TestExpandCollapse:
    def test_expand_adds_to_expanded_set(self):
        children = [_node("public", ["pg", "public"], "schema", expandable=True)]
        explorer = _make_explorer(children=children)
        ctrl = ExploreController(explorer)
        # Cursor on "pg" catalog (expandable=True)
        ctrl.expand()
        assert ("pg",) in ctrl.expanded

    def test_expand_rebuilds_visible_nodes(self):
        children = [_node("public", ["pg", "public"], "schema", expandable=True)]
        explorer = _make_explorer(children=children)
        ctrl = ExploreController(explorer)
        ctrl.expand()
        # Should now show pg, public, ddb
        assert len(ctrl.visible_nodes) == 3
        assert ctrl.visible_nodes[1].name == "public"

    def test_collapse_removes_from_expanded(self):
        children = [_node("public", ["pg", "public"], "schema")]
        explorer = _make_explorer(children=children)
        ctrl = ExploreController(explorer)
        ctrl.expand()
        assert ("pg",) in ctrl.expanded
        ctrl.collapse()
        assert ("pg",) not in ctrl.expanded

    def test_collapse_non_expanded_moves_to_parent(self):
        children = [_node("public", ["pg", "public"], "schema")]
        explorer = _make_explorer(children=children)
        ctrl = ExploreController(explorer)
        ctrl.expand()
        # Move cursor to "public" child
        ctrl.move_down()
        assert ctrl.visible_nodes[ctrl.cursor].name == "public"
        # Collapse should move cursor to parent "pg"
        ctrl.collapse()
        assert ctrl.cursor == 0
        assert ctrl.visible_nodes[ctrl.cursor].name == "pg"

    def test_expand_non_expandable_is_noop(self):
        explorer = _make_explorer(catalog_infos=[
            CatalogInfo(name="broken", catalog_type="pg", connected=False, error="fail"),
        ])
        ctrl = ExploreController(explorer)
        # "broken" has is_expandable=False (connected=False)
        ctrl.expand()
        assert ("broken",) not in ctrl.expanded


# ── Key dispatch ─────────────────────────────────────────────────────


class TestKeyDispatch:
    def test_j_moves_down(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.handle_key("j")
        assert ctrl.cursor == 1

    def test_k_moves_up(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.handle_key("j")
        ctrl.handle_key("k")
        assert ctrl.cursor == 0

    def test_enter_expands(self):
        explorer = _make_explorer(children=[_node("s", ["pg", "s"], "schema")])
        ctrl = ExploreController(explorer)
        ctrl.handle_key("enter")
        assert ("pg",) in ctrl.expanded

    def test_backspace_collapses(self):
        explorer = _make_explorer(children=[_node("s", ["pg", "s"], "schema")])
        ctrl = ExploreController(explorer)
        ctrl.handle_key("enter")
        ctrl.handle_key("backspace")
        assert ("pg",) not in ctrl.expanded

    def test_l_expands(self):
        explorer = _make_explorer(children=[_node("s", ["pg", "s"], "schema")])
        ctrl = ExploreController(explorer)
        ctrl.handle_key("l")
        assert ("pg",) in ctrl.expanded

    def test_h_collapses(self):
        explorer = _make_explorer(children=[_node("s", ["pg", "s"], "schema")])
        ctrl = ExploreController(explorer)
        ctrl.handle_key("l")
        ctrl.handle_key("h")
        assert ("pg",) not in ctrl.expanded

    def test_arrow_keys(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.handle_key("down")
        assert ctrl.cursor == 1
        ctrl.handle_key("up")
        assert ctrl.cursor == 0

    def test_home_end(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.handle_key("end")
        assert ctrl.cursor == 1
        ctrl.handle_key("home")
        assert ctrl.cursor == 0

    def test_q_returns_false(self):
        ctrl = ExploreController(_make_explorer())
        assert ctrl.handle_key("q") is False

    def test_ctrl_c_returns_false(self):
        ctrl = ExploreController(_make_explorer())
        assert ctrl.handle_key("ctrl_c") is False

    def test_esc_closes_pane(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.active_pane = Pane.DETAIL
        ctrl.handle_key("esc")
        assert ctrl.active_pane == Pane.TREE

    def test_esc_in_tree_pane_is_noop(self):
        ctrl = ExploreController(_make_explorer())
        result = ctrl.handle_key("esc")
        assert result is True
        assert ctrl.active_pane == Pane.TREE

    def test_unknown_key_is_noop(self):
        ctrl = ExploreController(_make_explorer())
        result = ctrl.handle_key("z")
        assert result is True
        assert ctrl.cursor == 0


# ── Event loop ────────────────────────────────────────────────────────


class TestEventLoop:
    def test_run_without_renderer_returns_immediately(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.run()  # should not hang

    def test_run_with_renderer_calls_render_and_read(self):
        renderer = MagicMock()
        renderer.read_key.side_effect = ["j", "q"]
        ctrl = ExploreController(_make_explorer(), renderer=renderer)
        ctrl.run()
        assert renderer.render_tree.call_count == 2
        assert renderer.read_key.call_count == 2

    def test_run_handles_keyboard_interrupt(self):
        renderer = MagicMock()
        renderer.read_key.side_effect = KeyboardInterrupt
        ctrl = ExploreController(_make_explorer(), renderer=renderer)
        ctrl.run()  # should not raise


# ── Scroll offset ─────────────────────────────────────────────────────


class TestScrollOffset:
    def test_scroll_offset_starts_at_zero(self):
        ctrl = ExploreController(_make_explorer())
        assert ctrl.scroll_offset == 0

    def test_jump_top_resets_scroll(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.scroll_offset = 5
        ctrl.jump_top()
        assert ctrl.scroll_offset == 0


# ── Action dispatch (task 18.2) ───────────────────────────────────────


def _table_node(name: str = "users", catalog: str = "pg") -> ExplorerNode:
    return _node(name, [catalog, "public", name], "table", expandable=True, depth=2)


def _view_node(name: str = "active_users", catalog: str = "pg") -> ExplorerNode:
    return _node(name, [catalog, "public", name], "view", expandable=True, depth=2)


def _ctrl_with_table() -> tuple[ExploreController, MagicMock]:
    """Create a controller with cursor on a table node."""
    children = [
        _node("public", ["pg", "public"], "schema", expandable=True, depth=1),
        _table_node(),
    ]
    explorer = _make_explorer(children=children)
    ctrl = ExploreController(explorer)
    # Expand pg → public
    ctrl.expanded.add(("pg",))
    ctrl._rebuild_visible_nodes()
    # Move cursor to the table node
    for i, n in enumerate(ctrl.visible_nodes):
        if n.node_type == "table":
            ctrl.cursor = i
            break
    return ctrl, explorer


class TestActionDetail:
    def test_d_on_table_opens_detail_pane(self):
        ctrl, explorer = _ctrl_with_table()
        from rivet_core.catalog_explorer import NodeDetail
        explorer.get_node_detail.return_value = NodeDetail(
            node=ctrl.visible_nodes[ctrl.cursor], schema=None, metadata=None, children_count=0,
        )
        ctrl.handle_key("d")
        assert ctrl.active_pane == Pane.DETAIL
        explorer.get_node_detail.assert_called_once()

    def test_d_on_catalog_is_noop(self):
        ctrl = ExploreController(_make_explorer())
        # Cursor on catalog node
        ctrl.handle_key("d")
        assert ctrl.active_pane == Pane.TREE

    def test_d_stores_detail_result(self):
        ctrl, explorer = _ctrl_with_table()
        from rivet_core.catalog_explorer import NodeDetail
        detail = NodeDetail(
            node=ctrl.visible_nodes[ctrl.cursor], schema=None, metadata=None, children_count=5,
        )
        explorer.get_node_detail.return_value = detail
        ctrl.handle_key("d")
        assert ctrl.detail is detail

    def test_d_on_error_sets_error_message(self):
        ctrl, explorer = _ctrl_with_table()
        explorer.get_node_detail.side_effect = RuntimeError("connection lost")
        ctrl.handle_key("d")
        assert ctrl.error_message == "connection lost"
        assert ctrl.active_pane != Pane.DETAIL


class TestActionStats:
    def test_s_on_table_opens_detail_pane(self):
        ctrl, explorer = _ctrl_with_table()
        explorer.get_table_stats.return_value = None
        ctrl.handle_key("s")
        assert ctrl.active_pane == Pane.DETAIL
        explorer.get_table_stats.assert_called_once()

    def test_s_on_catalog_is_noop(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.handle_key("s")
        assert ctrl.active_pane == Pane.TREE

    def test_s_on_error_sets_error_message(self):
        ctrl, explorer = _ctrl_with_table()
        explorer.get_table_stats.side_effect = RuntimeError("fail")
        ctrl.handle_key("s")
        assert ctrl.error_message == "fail"


class TestActionPreview:
    def test_p_on_table_sets_repl_query_and_stops(self):
        ctrl, explorer = _ctrl_with_table()
        result = ctrl.handle_key("p")
        assert ctrl.repl_query == "SELECT * FROM pg.public.users LIMIT 100"
        assert ctrl._running is False

    def test_p_on_catalog_is_noop(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.handle_key("p")
        assert ctrl.active_pane == Pane.TREE
        assert ctrl.repl_query is None

    def test_p_on_view_sets_repl_query(self):
        children = [
            _node("public", ["pg", "public"], "schema", expandable=True, depth=1),
            _view_node(),
        ]
        explorer = _make_explorer(children=children)
        ctrl = ExploreController(explorer)
        ctrl.expanded.add(("pg",))
        ctrl._rebuild_visible_nodes()
        for i, n in enumerate(ctrl.visible_nodes):
            if n.node_type == "view":
                ctrl.cursor = i
                break
        ctrl.handle_key("p")
        assert ctrl.repl_query == "SELECT * FROM pg.public.active_users LIMIT 100"


class TestActionGenerate:
    def test_g_on_table_opens_generate_pane(self):
        ctrl, explorer = _ctrl_with_table()
        ctrl.handle_key("g")
        assert ctrl.active_pane == Pane.GENERATE
        assert ctrl._generate_node is not None

    def test_g_then_y_generates_yaml(self):
        ctrl, explorer = _ctrl_with_table()
        from rivet_core.catalog_explorer import GeneratedSource
        explorer.generate_source.return_value = GeneratedSource(
            content="name: raw_users", format="yaml", suggested_filename="raw_users.yaml",
            catalog_name="pg", table_name="users", column_count=3,
        )
        ctrl.handle_key("g")
        ctrl.handle_key("y")
        explorer.generate_source.assert_called_once_with(explorer.generate_source.call_args[0][0], format="yaml")
        assert ctrl.generated_source is not None

    def test_g_then_s_generates_sql(self):
        ctrl, explorer = _ctrl_with_table()
        from rivet_core.catalog_explorer import GeneratedSource
        explorer.generate_source.return_value = GeneratedSource(
            content="-- rivet:name: raw_users", format="sql", suggested_filename="raw_users.sql",
            catalog_name="pg", table_name="users", column_count=3,
        )
        ctrl.handle_key("g")
        ctrl.handle_key("s")
        explorer.generate_source.assert_called_once_with(explorer.generate_source.call_args[0][0], format="sql")
        assert ctrl.generated_source is not None

    def test_g_then_esc_cancels(self):
        ctrl, explorer = _ctrl_with_table()
        ctrl.handle_key("g")
        assert ctrl.active_pane == Pane.GENERATE
        ctrl.handle_key("esc")
        assert ctrl.active_pane == Pane.TREE
        assert ctrl.generated_source is None

    def test_g_on_catalog_is_noop(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.handle_key("g")
        assert ctrl.generated_source is None

    def test_g_on_error_sets_error_message(self):
        ctrl, explorer = _ctrl_with_table()
        explorer.generate_source.side_effect = RuntimeError("no schema")
        ctrl.handle_key("g")
        ctrl.handle_key("y")
        assert ctrl.error_message == "no schema"


class TestActionSearch:
    def test_slash_opens_search_pane(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.handle_key("/")
        assert ctrl.active_pane == Pane.SEARCH
        assert ctrl.search_query == ""

    def test_typing_in_search_mode_updates_query(self):
        ctrl = ExploreController(_make_explorer())
        ctrl._explorer.search.return_value = []
        ctrl.handle_key("/")
        ctrl.handle_key("u")
        ctrl.handle_key("s")
        assert ctrl.search_query == "us"

    def test_backspace_in_search_removes_char(self):
        ctrl = ExploreController(_make_explorer())
        ctrl._explorer.search.return_value = []
        ctrl.handle_key("/")
        ctrl.handle_key("a")
        ctrl.handle_key("b")
        ctrl.handle_key("backspace")
        assert ctrl.search_query == "a"

    def test_search_calls_explorer_search(self):
        ctrl = ExploreController(_make_explorer())
        ctrl._explorer.search.return_value = []
        ctrl.handle_key("/")
        ctrl.handle_key("t")
        ctrl._explorer.search.assert_called_with("t")

    def test_esc_in_search_returns_to_tree(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.handle_key("/")
        assert ctrl.active_pane == Pane.SEARCH
        ctrl.handle_key("esc")
        assert ctrl.active_pane == Pane.TREE

    def test_enter_in_search_confirms_and_returns_to_tree(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.handle_key("/")
        ctrl.handle_key("enter")
        assert ctrl.active_pane == Pane.TREE

    def test_search_cursor_navigation(self):
        from rivet_core.catalog_explorer import SearchResult
        ctrl = ExploreController(_make_explorer())
        ctrl.handle_key("/")
        ctrl.search_results = [
            SearchResult(kind="table", qualified_name="pg.public.a", short_name="a",
                         parent="pg.public", match_positions=[0], score=1.0, node_type="table"),
            SearchResult(kind="table", qualified_name="pg.public.b", short_name="b",
                         parent="pg.public", match_positions=[0], score=2.0, node_type="table"),
        ]
        ctrl.handle_key("down")
        assert ctrl.search_cursor == 1
        ctrl.handle_key("up")
        assert ctrl.search_cursor == 0

    def test_ctrl_c_in_search_exits(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.handle_key("/")
        result = ctrl.handle_key("ctrl_c")
        assert result is False


class TestActionRefresh:
    def test_r_calls_refresh_catalog(self):
        ctrl = ExploreController(_make_explorer())
        ctrl.handle_key("r")
        ctrl._explorer.refresh_catalog.assert_called_once_with("pg")

    def test_r_clears_expansion_for_catalog(self):
        children = [_node("public", ["pg", "public"], "schema", expandable=True)]
        explorer = _make_explorer(children=children)
        ctrl = ExploreController(explorer)
        ctrl.expand()  # expand pg
        assert ("pg",) in ctrl.expanded
        ctrl.handle_key("r")
        assert ("pg",) not in ctrl.expanded

    def test_r_on_error_sets_error_message(self):
        explorer = _make_explorer()
        explorer.refresh_catalog.side_effect = RuntimeError("fail")
        ctrl = ExploreController(explorer)
        ctrl.handle_key("r")
        assert ctrl.error_message == "fail"

    def test_r_on_empty_list_is_noop(self):
        ctrl = ExploreController(_make_explorer(catalog_infos=[]))
        ctrl.handle_key("r")  # should not raise


class TestActionOnViewNode:
    """Verify actions work on view nodes too, not just tables."""

    def test_d_on_view_opens_detail(self):
        children = [
            _node("public", ["pg", "public"], "schema", expandable=True, depth=1),
            _view_node(),
        ]
        explorer = _make_explorer(children=children)
        from rivet_core.catalog_explorer import NodeDetail
        explorer.get_node_detail.return_value = NodeDetail(
            node=children[1], schema=None, metadata=None, children_count=0,
        )
        ctrl = ExploreController(explorer)
        ctrl.expanded.add(("pg",))
        ctrl._rebuild_visible_nodes()
        # Move to view node
        for i, n in enumerate(ctrl.visible_nodes):
            if n.node_type == "view":
                ctrl.cursor = i
                break
        ctrl.handle_key("d")
        assert ctrl.active_pane == Pane.DETAIL
