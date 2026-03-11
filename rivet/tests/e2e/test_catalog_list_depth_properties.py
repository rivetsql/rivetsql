"""Property-based tests for ``rivet catalog list`` path-based navigation.

Uses Hypothesis to generate random catalog trees and paths, then exercises
``catalog_list`` directly with mock ``CatalogExplorer`` objects.

Validates Requirements: 1.1–1.5, 2.1–2.3, 3.1–3.4
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from io import StringIO
from typing import Any
from unittest.mock import MagicMock

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_cli.app import GlobalOptions
from rivet_cli.commands.catalog import catalog_list
from rivet_cli.exit_codes import GENERAL_ERROR, SUCCESS, USAGE_ERROR
from rivet_core.catalog_explorer import CatalogInfo, ExplorerNode

# ---------------------------------------------------------------------------
# Strategies — catalog tree generation
# ---------------------------------------------------------------------------

# Simple identifier names: lowercase ASCII letters, 2-8 chars, no dots
_name_st = st.from_regex(r"[a-z]{2,8}", fullmatch=True)


@dataclass
class CatalogTree:
    """A generated catalog tree for property testing."""

    catalogs: list[CatalogInfo]
    # Mapping from tuple(path) -> list[ExplorerNode]
    children: dict[tuple[str, ...], list[ExplorerNode]]


@st.composite
def catalog_tree_st(draw: st.DrawFn) -> CatalogTree:
    """Generate a random catalog tree with 1-3 catalogs, each with schemas and tables."""
    num_catalogs = draw(st.integers(min_value=1, max_value=3))
    cat_names = draw(st.lists(_name_st, min_size=num_catalogs, max_size=num_catalogs, unique=True))

    catalogs: list[CatalogInfo] = []
    children: dict[tuple[str, ...], list[ExplorerNode]] = {}

    for cat_name in cat_names:
        catalogs.append(
            CatalogInfo(
                name=cat_name,
                catalog_type="duckdb",
                connected=True,
                error=None,
            )
        )

        # Generate 1-3 schemas per catalog
        num_schemas = draw(st.integers(min_value=1, max_value=3))
        schema_names = draw(
            st.lists(_name_st, min_size=num_schemas, max_size=num_schemas, unique=True)
        )

        schema_nodes: list[ExplorerNode] = []
        for schema_name in schema_names:
            schema_path = [cat_name, schema_name]
            schema_nodes.append(
                ExplorerNode(
                    name=schema_name,
                    node_type="schema",
                    path=schema_path,
                    is_expandable=True,
                    depth=1,
                    summary=None,
                    depth_limit_reached=False,
                )
            )

            # Generate 1-3 tables per schema
            num_tables = draw(st.integers(min_value=1, max_value=3))
            table_names = draw(
                st.lists(_name_st, min_size=num_tables, max_size=num_tables, unique=True)
            )

            table_nodes: list[ExplorerNode] = []
            for table_name in table_names:
                table_path = [cat_name, schema_name, table_name]
                table_nodes.append(
                    ExplorerNode(
                        name=table_name,
                        node_type="table",
                        path=table_path,
                        is_expandable=True,
                        depth=2,
                        summary=None,
                        depth_limit_reached=False,
                    )
                )

                # Generate 1-4 columns per table
                num_cols = draw(st.integers(min_value=1, max_value=4))
                col_names = draw(
                    st.lists(_name_st, min_size=num_cols, max_size=num_cols, unique=True)
                )

                col_nodes: list[ExplorerNode] = []
                for col_name in col_names:
                    col_nodes.append(
                        ExplorerNode(
                            name=col_name,
                            node_type="column",
                            path=[cat_name, schema_name, table_name, col_name],
                            is_expandable=False,
                            depth=3,
                            summary=None,
                            depth_limit_reached=False,
                        )
                    )
                children[tuple(table_path)] = col_nodes

            children[tuple(schema_path)] = table_nodes

        children[(cat_name,)] = schema_nodes

    return CatalogTree(catalogs=catalogs, children=children)


def _make_explorer(tree: CatalogTree, *, connected: bool = True) -> MagicMock:
    """Build a mock CatalogExplorer from a CatalogTree."""
    explorer = MagicMock()
    if connected:
        explorer.list_catalogs.return_value = tree.catalogs
    else:
        # Override catalogs to be disconnected
        disconnected = [
            CatalogInfo(
                name=c.name,
                catalog_type=c.catalog_type,
                connected=False,
                error="connection refused",
            )
            for c in tree.catalogs
        ]
        explorer.list_catalogs.return_value = disconnected

    def _list_children(path: list[str]) -> list[ExplorerNode]:
        key = tuple(path)
        return tree.children.get(key, [])

    explorer.list_children.side_effect = _list_children
    return explorer


_GLOBALS = GlobalOptions(color=False)


def _multi_segment_path_st(tree: CatalogTree) -> st.SearchStrategy[list[str]]:
    """Strategy that picks a random valid multi-segment path (2+ segments).

    Single-segment paths use backward-compatible catalog name filtering
    (Req 4.3), so property tests for path resolution target 2+ segments.
    """
    all_paths: list[list[str]] = []
    for cat in tree.catalogs:
        for schema_node in tree.children.get((cat.name,), []):
            all_paths.append(list(schema_node.path))
            for table_node in tree.children.get(tuple(schema_node.path), []):
                all_paths.append(list(table_node.path))
    if not all_paths:
        # Fallback: shouldn't happen with min 1 catalog + 1 schema
        return st.just([tree.catalogs[0].name])
    return st.sampled_from(all_paths)


def _capture_output(
    explorer: MagicMock,
    path: str | None,
    fmt: str = "text",
    depth: int = 0,
) -> tuple[int, str, str]:
    """Call catalog_list and capture stdout/stderr."""
    old_stdout, old_stderr = sys.stdout, sys.stderr
    sys.stdout = StringIO()
    sys.stderr = StringIO()
    try:
        exit_code = catalog_list(
            explorer=explorer,
            globals=_GLOBALS,
            path=path,
            depth=depth,
            format=fmt,
        )
        stdout = sys.stdout.getvalue()
        stderr = sys.stderr.getvalue()
    finally:
        sys.stdout, sys.stderr = old_stdout, old_stderr
    return exit_code, stdout, stderr


# ---------------------------------------------------------------------------
# Property 1: Path resolution returns correct children
# ---------------------------------------------------------------------------


@given(data=st.data())
@settings(max_examples=100)
def test_property1_path_resolution_returns_correct_children(data: st.DataObject) -> None:
    """Feature: catalog-list-depth, Property 1: Path resolution returns correct children.

    For any valid dot-separated path within a random catalog tree, calling
    ``catalog_list`` with that path and depth 0 returns output containing
    exactly the names of the immediate children of the target node.

    **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5**
    """
    tree = data.draw(catalog_tree_st())
    path_segments = data.draw(_multi_segment_path_st(tree))
    explorer = _make_explorer(tree)

    dot_path = ".".join(path_segments)
    exit_code, stdout, stderr = _capture_output(explorer, dot_path, fmt="text", depth=0)

    assert exit_code == SUCCESS, f"Expected SUCCESS, got {exit_code}. stderr: {stderr}"

    # The expected children are what list_children returns for this path
    expected_children = tree.children.get(tuple(path_segments), [])
    for child in expected_children:
        assert child.name in stdout, (
            f"Child '{child.name}' not found in output for path '{dot_path}'.\nstdout: {stdout}"
        )


# ---------------------------------------------------------------------------
# Property 2: Invalid path segments produce exit code 10 with identifying error
# ---------------------------------------------------------------------------


@st.composite
def _invalid_path_st(draw: st.DrawFn, tree: CatalogTree) -> tuple[str, str]:
    """Generate a path with at least one invalid segment.

    Returns (dot_path, bad_segment).
    """
    strategy = draw(st.sampled_from(["bad_catalog", "bad_intermediate"]))

    if strategy == "bad_catalog":
        # First segment doesn't match any catalog
        existing_names = {c.name for c in tree.catalogs}
        bad_name = draw(_name_st.filter(lambda n: n not in existing_names))
        return bad_name, bad_name

    # bad_intermediate: valid catalog, invalid second segment
    cat = draw(st.sampled_from(tree.catalogs))
    existing_children = {n.name for n in tree.children.get((cat.name,), [])}
    bad_segment = draw(_name_st.filter(lambda n: n not in existing_children))
    return f"{cat.name}.{bad_segment}", bad_segment


@given(data=st.data())
@settings(max_examples=100)
def test_property2_invalid_path_produces_exit_10(data: st.DataObject) -> None:
    """Feature: catalog-list-depth, Property 2: Invalid path segments produce exit code 10.

    For any path where at least one segment does not resolve, ``catalog_list``
    returns exit code 10 and the error message contains the bad segment name.

    **Validates: Requirements 2.1, 2.2**
    """
    tree = data.draw(catalog_tree_st())
    dot_path, bad_segment = data.draw(_invalid_path_st(tree))
    explorer = _make_explorer(tree)

    exit_code, stdout, stderr = _capture_output(explorer, dot_path)

    assert exit_code == USAGE_ERROR, (
        f"Expected USAGE_ERROR (10), got {exit_code}. path={dot_path!r}, stderr: {stderr}"
    )
    assert bad_segment.lower() in stderr.lower(), (
        f"Bad segment '{bad_segment}' not found in stderr.\nstderr: {stderr}"
    )


# ---------------------------------------------------------------------------
# Property 3: Disconnected catalog produces exit code 1
# ---------------------------------------------------------------------------


@given(data=st.data())
@settings(max_examples=100)
def test_property3_disconnected_catalog_produces_exit_1(data: st.DataObject) -> None:
    """Feature: catalog-list-depth, Property 3: Disconnected catalog produces exit code 1.

    For any path whose first segment names a disconnected catalog,
    ``catalog_list`` returns exit code 1.

    **Validates: Requirements 2.3**
    """
    tree = data.draw(catalog_tree_st())
    cat = data.draw(st.sampled_from(tree.catalogs))
    # Need at least 2 segments to hit _resolve_path (single segment uses
    # backward-compatible filter which doesn't check connection status)
    schema_nodes = tree.children.get((cat.name,), [])
    if not schema_nodes:
        return  # skip if no children to form a multi-segment path
    schema = data.draw(st.sampled_from(schema_nodes))
    dot_path = f"{cat.name}.{schema.name}"

    explorer = _make_explorer(tree, connected=False)

    exit_code, stdout, stderr = _capture_output(explorer, dot_path)

    assert exit_code == GENERAL_ERROR, (
        f"Expected GENERAL_ERROR (1), got {exit_code}. path={dot_path!r}, stderr: {stderr}"
    )
    assert "not connected" in stderr.lower() or "disconnected" in stderr.lower(), (
        f"Expected disconnected error in stderr.\nstderr: {stderr}"
    )


# ---------------------------------------------------------------------------
# Property 4: All output formats include target node children names
# ---------------------------------------------------------------------------


@given(data=st.data())
@settings(max_examples=100)
def test_property4_all_formats_include_children_names(data: st.DataObject) -> None:
    """Feature: catalog-list-depth, Property 4: All output formats include children names.

    For any valid path and any output format (text, tree, json), the rendered
    output contains the ``name`` of every immediate child of the target node.

    **Validates: Requirements 3.1, 3.2, 3.3**
    """
    tree = data.draw(catalog_tree_st())
    path_segments = data.draw(_multi_segment_path_st(tree))
    fmt = data.draw(st.sampled_from(["text", "tree", "json"]))
    explorer = _make_explorer(tree)

    dot_path = ".".join(path_segments)
    exit_code, stdout, stderr = _capture_output(explorer, dot_path, fmt=fmt, depth=0)

    assert exit_code == SUCCESS, f"Expected SUCCESS, got {exit_code}. stderr: {stderr}"

    expected_children = tree.children.get(tuple(path_segments), [])
    for child in expected_children:
        assert child.name in stdout, (
            f"Child '{child.name}' not found in {fmt} output for path '{dot_path}'.\n"
            f"stdout: {stdout}"
        )


# ---------------------------------------------------------------------------
# Property 5: JSON output contains required fields and respects depth
# ---------------------------------------------------------------------------


@given(data=st.data())
@settings(max_examples=100)
def test_property5_json_output_has_required_fields_and_respects_depth(
    data: st.DataObject,
) -> None:
    """Feature: catalog-list-depth, Property 5: JSON output has required fields and respects depth.

    For any valid path with JSON format, the output is valid JSON where each
    node has ``name``, ``node_type``, ``path``, and ``is_expandable`` fields.
    When depth > 0, expandable nodes include a ``children`` array.

    **Validates: Requirements 3.3, 3.4**
    """
    tree = data.draw(catalog_tree_st())
    # Pick multi-segment paths that have children (2+ segments to hit
    # the path-based JSON renderer, not the backward-compat catalog filter)
    paths_with_children = [list(k) for k, v in tree.children.items() if v and len(k) >= 2]
    if not paths_with_children:
        return  # skip degenerate trees
    path_segments = data.draw(st.sampled_from(paths_with_children))
    depth = data.draw(st.integers(min_value=0, max_value=2))
    explorer = _make_explorer(tree)

    dot_path = ".".join(path_segments)
    exit_code, stdout, stderr = _capture_output(explorer, dot_path, fmt="json", depth=depth)

    assert exit_code == SUCCESS, f"Expected SUCCESS, got {exit_code}. stderr: {stderr}"

    parsed = json.loads(stdout)
    assert isinstance(parsed, list), f"Expected JSON array, got {type(parsed)}"

    _REQUIRED_FIELDS = {"name", "node_type", "path", "is_expandable"}

    def _check_node(node: dict[str, Any], remaining_depth: int) -> None:
        for field in _REQUIRED_FIELDS:
            assert field in node, f"Missing field '{field}' in node: {node}"
        if remaining_depth > 0 and node.get("is_expandable"):
            assert "children" in node, (
                f"Expandable node missing 'children' at depth {remaining_depth}: {node}"
            )
            for child in node["children"]:
                _check_node(child, remaining_depth - 1)

    for node in parsed:
        _check_node(node, depth)
