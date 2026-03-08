"""Property-based tests for list_children prefix filtering.

Property 11: list_children Returns Immediate Children

For any catalog and path, list_children(catalog, path) shall return only nodes
whose path is an immediate child of the given path (len(node.path) == len(path) + 1
and node.path[:len(path)] == path).

Validates: Requirements 7.2, 7.3
"""

from __future__ import annotations

from unittest.mock import MagicMock

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.introspection import CatalogNode
from rivet_core.plugins import CatalogPlugin, _is_immediate_child

# ── Strategies ────────────────────────────────────────────────────────────────

_segment_st = st.from_regex(r"[a-z][a-z0-9_]{0,9}", fullmatch=True)
_path_st = st.lists(_segment_st, min_size=0, max_size=4, unique=True)


def _node_st(parent_path: list[str]) -> st.SearchStrategy[list[CatalogNode]]:
    """Generate a list of CatalogNode with paths at various depths relative to parent."""

    def _build_nodes(segments_list: list[list[str]]) -> list[CatalogNode]:
        nodes = []
        for segs in segments_list:
            path = parent_path + segs
            nodes.append(
                CatalogNode(
                    name=segs[-1] if segs else "root",
                    node_type="table",
                    path=path,
                    is_container=False,
                    children_count=None,
                    summary=None,
                )
            )
        return nodes

    # Generate paths that are 1, 2, or 3 segments deeper than parent
    return st.lists(
        st.lists(_segment_st, min_size=1, max_size=3),
        min_size=0,
        max_size=10,
    ).map(_build_nodes)


class _ConcretePlugin(CatalogPlugin):
    """Minimal concrete subclass for testing the default list_children."""

    type = "test"
    required_options: list = []
    optional_options: dict = {}
    credential_options: list = []

    def __init__(self, nodes: list[CatalogNode] | None = None):
        self._nodes = nodes or []

    def validate(self, options):
        pass

    def instantiate(self, name, options):
        pass

    def default_table_reference(self, logical_name, options):
        return logical_name

    def list_tables(self, catalog):
        return self._nodes


# ── Property 11a: returned nodes are immediate children ──────────────────────


@given(parent_path=_path_st, data=st.data())
@settings(max_examples=100)
def test_list_children_returns_only_immediate_children(
    parent_path: list[str], data: st.DataObject
) -> None:
    """All nodes returned by list_children are immediate children of the given path."""
    nodes = data.draw(_node_st(parent_path))
    plugin = _ConcretePlugin(nodes=nodes)
    catalog = MagicMock()

    result = plugin.list_children(catalog, parent_path)

    for node in result:
        assert len(node.path) == len(parent_path) + 1, (
            f"Node path {node.path} is not exactly one level deeper than {parent_path}"
        )
        assert node.path[: len(parent_path)] == parent_path, (
            f"Node path {node.path} does not start with parent {parent_path}"
        )


# ── Property 11b: no immediate children are missed ──────────────────────────


@given(parent_path=_path_st, data=st.data())
@settings(max_examples=100)
def test_list_children_includes_all_immediate_children(
    parent_path: list[str], data: st.DataObject
) -> None:
    """All immediate children in the full node list appear in the result."""
    nodes = data.draw(_node_st(parent_path))
    plugin = _ConcretePlugin(nodes=nodes)
    catalog = MagicMock()

    result = plugin.list_children(catalog, parent_path)
    result_paths = [tuple(n.path) for n in result]

    expected = [
        n for n in nodes if _is_immediate_child(n.path, parent_path)
    ]
    expected_paths = [tuple(n.path) for n in expected]

    assert sorted(result_paths) == sorted(expected_paths), (
        f"Expected {expected_paths}, got {result_paths}"
    )


# ── Property 11c: deeper nodes are excluded ──────────────────────────────────


@given(parent_path=_path_st, data=st.data())
@settings(max_examples=100)
def test_list_children_excludes_deeper_nodes(
    parent_path: list[str], data: st.DataObject
) -> None:
    """Nodes more than one level deeper than parent are excluded."""
    nodes = data.draw(_node_st(parent_path))
    plugin = _ConcretePlugin(nodes=nodes)
    catalog = MagicMock()

    result = plugin.list_children(catalog, parent_path)

    deeper_nodes = [n for n in nodes if len(n.path) > len(parent_path) + 1]
    result_paths = {tuple(n.path) for n in result}

    for deep in deeper_nodes:
        assert tuple(deep.path) not in result_paths, (
            f"Deeper node {deep.path} should not appear in result"
        )
