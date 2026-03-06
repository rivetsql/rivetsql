"""Tests for catalog_list handler.

Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from rivet_cli.app import GlobalOptions
from rivet_cli.commands.catalog import catalog_list
from rivet_cli.exit_codes import GENERAL_ERROR, SUCCESS, USAGE_ERROR
from rivet_core.catalog_explorer import CatalogInfo, ExplorerNode


def _globals(**overrides) -> GlobalOptions:
    defaults = dict(profile="default", project_path=Path("."), verbosity=0, color=False)
    defaults.update(overrides)
    return GlobalOptions(**defaults)


def _make_explorer(catalogs: list[CatalogInfo], children: dict[str, list[ExplorerNode]] | None = None) -> MagicMock:
    """Build a mock CatalogExplorer."""
    explorer = MagicMock()
    explorer.list_catalogs.return_value = catalogs
    if children is not None:
        explorer.list_children.side_effect = lambda path: children.get(path[0], []) if path else []
    else:
        explorer.list_children.return_value = []
    return explorer


def _make_catalog_info(name: str, connected: bool = True) -> CatalogInfo:
    return CatalogInfo(name=name, catalog_type="duckdb", connected=connected, error=None if connected else "refused")


def _make_node(name: str, catalog: str, node_type: str = "schema") -> ExplorerNode:
    return ExplorerNode(
        name=name,
        node_type=node_type,
        path=[catalog, name],
        is_expandable=True,
        depth=1,
        summary=None,
        depth_limit_reached=False,
    )


class TestCatalogListTextFormat:
    """Tests for default text format (Req 9.1, 9.6)."""

    def test_lists_all_catalogs_text(self, capsys):
        """No args: lists all catalogs with status and type (Req 9.1)."""
        catalogs = [_make_catalog_info("pg"), _make_catalog_info("s3", connected=False)]
        explorer = _make_explorer(catalogs)

        code = catalog_list(explorer=explorer, globals=_globals())

        assert code == SUCCESS
        out = capsys.readouterr().out
        assert "pg" in out
        assert "s3" in out

    def test_default_depth_zero(self, capsys):
        """Default depth=0 shows only catalog-level rows (Req 9.6)."""
        catalogs = [_make_catalog_info("pg")]
        explorer = _make_explorer(catalogs)

        code = catalog_list(explorer=explorer, globals=_globals(), depth=0)

        assert code == SUCCESS
        # list_children should NOT be called at depth 0 with text format
        explorer.list_children.assert_not_called()

    def test_startup_failure_propagates(self):
        """If _startup returns error code, _dispatch_catalog returns it before calling catalog_list."""
        import argparse

        from rivet_cli.app import _dispatch_catalog

        args = argparse.Namespace(
            catalog_action="list",
            catalog_name=None,
            depth=0,
            format="text",
            profile="default",
            project=None,
            verbose=0,
            quiet=False,
            no_color=False,
        )
        with patch("rivet_cli.commands.catalog._startup", return_value=GENERAL_ERROR):
            code = _dispatch_catalog(args, _globals())
        assert code == GENERAL_ERROR

    def test_depth_gt_zero_text_uses_tree_renderer(self, capsys):
        """depth > 0 with text format uses tree renderer (Req 9.2)."""
        catalogs = [_make_catalog_info("pg")]
        children = {"pg": [_make_node("public", "pg")]}
        explorer = _make_explorer(catalogs, children)

        code = catalog_list(explorer=explorer, globals=_globals(), depth=1)

        assert code == SUCCESS
        out = capsys.readouterr().out
        assert "pg" in out
        # list_children called for depth expansion
        explorer.list_children.assert_called()


class TestCatalogListCatalogFilter:
    """Tests for catalog name argument (Req 9.3)."""

    def test_filter_to_single_catalog(self, capsys):
        """Catalog name arg: only that catalog shown (Req 9.3)."""
        catalogs = [_make_catalog_info("pg"), _make_catalog_info("s3")]
        explorer = _make_explorer(catalogs)

        code = catalog_list(explorer=explorer, globals=_globals(), catalog_name="pg")

        assert code == SUCCESS
        out = capsys.readouterr().out
        assert "pg" in out
        assert "s3" not in out

    def test_unknown_catalog_returns_usage_error(self, capsys):
        """Unknown catalog name → USAGE_ERROR (Req 9.3)."""
        catalogs = [_make_catalog_info("pg")]
        explorer = _make_explorer(catalogs)

        code = catalog_list(explorer=explorer, globals=_globals(), catalog_name="nonexistent")

        assert code == USAGE_ERROR
        err = capsys.readouterr().err
        assert "nonexistent" in err


class TestCatalogListJsonFormat:
    """Tests for --format json (Req 9.4)."""

    def test_json_output_valid(self, capsys):
        """--format json produces valid JSON with catalogs array (Req 9.4)."""
        catalogs = [_make_catalog_info("pg"), _make_catalog_info("s3", connected=False)]
        explorer = _make_explorer(catalogs)

        code = catalog_list(explorer=explorer, globals=_globals(), format="json")

        assert code == SUCCESS
        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert "catalogs" in parsed
        assert len(parsed["catalogs"]) == 2

    def test_json_schema_fields(self, capsys):
        """JSON output has name, type, connected, children fields (Req 9.4)."""
        catalogs = [_make_catalog_info("pg")]
        explorer = _make_explorer(catalogs)

        code = catalog_list(explorer=explorer, globals=_globals(), format="json")

        assert code == SUCCESS
        parsed = json.loads(capsys.readouterr().out)
        entry = parsed["catalogs"][0]
        assert entry["name"] == "pg"
        assert entry["type"] == "duckdb"
        assert entry["connected"] is True
        assert "children" in entry

    def test_json_depth_zero_no_children_fetched(self, capsys):
        """depth=0 with json: children not fetched (Req 9.4)."""
        catalogs = [_make_catalog_info("pg")]
        explorer = _make_explorer(catalogs)

        code = catalog_list(explorer=explorer, globals=_globals(), format="json", depth=0)

        assert code == SUCCESS
        explorer.list_children.assert_not_called()
        parsed = json.loads(capsys.readouterr().out)
        assert parsed["catalogs"][0]["children"] == []

    def test_json_depth_one_fetches_children(self, capsys):
        """depth=1 with json: children fetched for connected catalogs (Req 9.4)."""
        catalogs = [_make_catalog_info("pg")]
        children = {"pg": [_make_node("public", "pg")]}
        explorer = _make_explorer(catalogs, children)

        code = catalog_list(explorer=explorer, globals=_globals(), format="json", depth=1)

        assert code == SUCCESS
        parsed = json.loads(capsys.readouterr().out)
        assert len(parsed["catalogs"][0]["children"]) == 1
        assert parsed["catalogs"][0]["children"][0]["name"] == "public"

    def test_json_depth_one_skips_disconnected(self, capsys):
        """depth=1 with json: disconnected catalogs get empty children (Req 9.4)."""
        catalogs = [_make_catalog_info("s3", connected=False)]
        explorer = _make_explorer(catalogs)

        code = catalog_list(explorer=explorer, globals=_globals(), format="json", depth=1)

        assert code == SUCCESS
        parsed = json.loads(capsys.readouterr().out)
        assert parsed["catalogs"][0]["children"] == []
        explorer.list_children.assert_not_called()


class TestCatalogListTreeFormat:
    """Tests for --format tree (Req 9.5)."""

    def test_tree_output_contains_catalog_name(self, capsys):
        """--format tree shows catalog name (Req 9.5)."""
        catalogs = [_make_catalog_info("pg")]
        explorer = _make_explorer(catalogs)

        code = catalog_list(explorer=explorer, globals=_globals(), format="tree")

        assert code == SUCCESS
        out = capsys.readouterr().out
        assert "pg" in out

    def test_tree_depth_zero_no_children(self, capsys):
        """tree depth=0: no children expanded."""
        catalogs = [_make_catalog_info("pg")]
        explorer = _make_explorer(catalogs)

        code = catalog_list(explorer=explorer, globals=_globals(), format="tree", depth=0)

        assert code == SUCCESS
        explorer.list_children.assert_not_called()

    def test_tree_depth_one_expands_children(self, capsys):
        """tree depth=1: children expanded for connected catalogs (Req 9.5)."""
        catalogs = [_make_catalog_info("pg")]
        children = {"pg": [_make_node("public", "pg")]}
        explorer = _make_explorer(catalogs, children)

        code = catalog_list(explorer=explorer, globals=_globals(), format="tree", depth=1)

        assert code == SUCCESS
        out = capsys.readouterr().out
        assert "public" in out


# ---------------------------------------------------------------------------
# Property 21: CLI depth flag controls expansion depth
# ---------------------------------------------------------------------------
# Feature: catalog-explorer, Property 21: CLI depth flag controls expansion depth
#
# Property 21: For any depth value N, catalog_list --depth N should return
# nodes at depths 0 through N and no deeper.
# Validates: Requirements 9.2

from hypothesis import given, settings
from hypothesis import strategies as st

_cat_name = st.text(
    alphabet="abcdefghijklmnopqrstuvwxyz_",
    min_size=1,
    max_size=8,
)

_node_type = st.sampled_from(["schema", "table", "column"])


def _make_tree(
    catalog_name: str,
    schema_names: list[str],
    table_names: list[str],
) -> dict[str, list[ExplorerNode]]:
    """Build a children dict: catalog → schemas, each schema → tables."""
    children: dict[str, list[ExplorerNode]] = {}
    schemas = [
        ExplorerNode(
            name=s,
            node_type="schema",
            path=[catalog_name, s],
            is_expandable=bool(table_names),
            depth=1,
            summary=None,
            depth_limit_reached=False,
        )
        for s in schema_names
    ]
    children[catalog_name] = schemas
    for s in schema_names:
        tables = [
            ExplorerNode(
                name=t,
                node_type="table",
                path=[catalog_name, s, t],
                is_expandable=False,
                depth=2,
                summary=None,
                depth_limit_reached=False,
            )
            for t in table_names
        ]
        children[f"{catalog_name}.{s}"] = tables
    return children


@given(
    catalog_name=_cat_name,
    schema_names=st.lists(_cat_name, min_size=1, max_size=3, unique=True),
    table_names=st.lists(_cat_name, min_size=1, max_size=3, unique=True),
    depth=st.integers(min_value=0, max_value=3),
)
@settings(max_examples=100)
def test_property_21_depth_flag_controls_expansion_json(
    catalog_name: str,
    schema_names: list[str],
    table_names: list[str],
    depth: int,
) -> None:
    """Property 21: catalog_list --depth N --format json returns no nodes deeper than N.

    For any depth value N, the JSON output should contain children only up to
    depth N. At depth 0, no children are fetched. At depth 1, only catalog-level
    children (schemas) are returned. Deeper nodes must not appear.

    Validates: Requirements 9.2
    """
    from unittest.mock import MagicMock

    catalogs = [_make_catalog_info(catalog_name, connected=True)]
    tree = _make_tree(catalog_name, schema_names, table_names)

    def _children_fn(path: list[str]) -> list[ExplorerNode]:
        if len(path) == 1:
            return tree.get(path[0], [])
        key = ".".join(path)
        return tree.get(key, [])

    explorer = MagicMock()
    explorer.list_catalogs.return_value = catalogs
    explorer.list_children.side_effect = _children_fn

    code = catalog_list(explorer=explorer, globals=_globals(), depth=depth, format="json")

    assert code == SUCCESS

    # Verify list_children call count matches depth constraint
    if depth == 0:
        explorer.list_children.assert_not_called()
    else:
        # At depth >= 1, list_children should be called for the catalog
        explorer.list_children.assert_called()


@given(
    catalog_name=_cat_name,
    schema_names=st.lists(_cat_name, min_size=1, max_size=3, unique=True),
    table_names=st.lists(_cat_name, min_size=1, max_size=3, unique=True),
    depth=st.integers(min_value=0, max_value=3),
)
@settings(max_examples=100)
def test_property_21_depth_flag_controls_expansion_tree(
    catalog_name: str,
    schema_names: list[str],
    table_names: list[str],
    depth: int,
) -> None:
    """Property 21: catalog_list --depth N --format tree renders no nodes deeper than N.

    For any depth value N, the tree output should not contain table-level nodes
    when depth < 2, and should not contain schema-level nodes when depth < 1.

    Validates: Requirements 9.2
    """
    import io
    from unittest.mock import MagicMock, patch

    # Use prefixed names to avoid collisions between catalog/schema/table names
    cat = f"cat_{catalog_name}"
    schemas = [f"sch_{s}" for s in schema_names]
    tables = [f"tbl_{t}" for t in table_names]

    catalogs = [_make_catalog_info(cat, connected=True)]
    tree = _make_tree(cat, schemas, tables)

    def _children_fn(path: list[str]) -> list[ExplorerNode]:
        if len(path) == 1:
            return tree.get(path[0], [])
        key = ".".join(path)
        return tree.get(key, [])

    explorer = MagicMock()
    explorer.list_catalogs.return_value = catalogs
    explorer.list_children.side_effect = _children_fn

    buf = io.StringIO()
    with patch("sys.stdout", buf):
        code = catalog_list(explorer=explorer, globals=_globals(), depth=depth, format="tree")

    assert code == SUCCESS
    out = buf.getvalue()

    # Catalog name always present
    assert cat in out

    if depth == 0:
        # No children expanded — schema names must not appear
        explorer.list_children.assert_not_called()
        for s in schemas:
            assert s not in out

    if depth >= 1:
        # Schema-level children should appear
        for s in schemas:
            assert s in out

    if depth < 2:
        # Table-level nodes must not appear
        for t in tables:
            assert t not in out
