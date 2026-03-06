"""Tests for CatalogPlugin.list_children() default implementation and _is_immediate_child helper."""

from unittest.mock import MagicMock

from rivet_core.introspection import CatalogNode
from rivet_core.plugins import CatalogPlugin, _is_immediate_child


def _make_node(path: list[str]) -> CatalogNode:
    return CatalogNode(
        name=path[-1],
        node_type="table",
        path=path,
        is_container=False,
        children_count=None,
        summary=None,
    )


class _ConcretePlugin(CatalogPlugin):
    """Minimal concrete subclass for testing."""

    type = "test"
    required_options: list = []
    optional_options: dict = {}
    credential_options: list = []

    def validate(self, options):
        pass

    def instantiate(self, name, options):
        pass

    def default_table_reference(self, logical_name, options):
        return logical_name


# ── _is_immediate_child ────────────────────────────────────────────────────────

def test_is_immediate_child_true():
    assert _is_immediate_child(["a", "b", "c"], ["a", "b"]) is True


def test_is_immediate_child_false_wrong_prefix():
    assert _is_immediate_child(["a", "x", "c"], ["a", "b"]) is False


def test_is_immediate_child_false_same_length():
    assert _is_immediate_child(["a", "b"], ["a", "b"]) is False


def test_is_immediate_child_false_two_deeper():
    assert _is_immediate_child(["a", "b", "c", "d"], ["a", "b"]) is False


def test_is_immediate_child_empty_parent():
    assert _is_immediate_child(["a"], []) is True


def test_is_immediate_child_empty_both():
    assert _is_immediate_child([], []) is False


# ── list_children default implementation ──────────────────────────────────────

def test_list_children_returns_immediate_children_only():
    plugin = _ConcretePlugin()
    catalog = MagicMock()

    nodes = [
        _make_node(["cat", "schema1", "table1"]),
        _make_node(["cat", "schema1", "table2"]),
        _make_node(["cat", "schema2", "table3"]),
        _make_node(["cat", "schema1", "table1", "col1"]),  # too deep
    ]
    plugin.list_tables = MagicMock(return_value=nodes)

    result = plugin.list_children(catalog, ["cat", "schema1"])

    assert len(result) == 2
    assert all(n.path[: 2] == ["cat", "schema1"] for n in result)
    assert all(len(n.path) == 3 for n in result)


def test_list_children_calls_list_tables():
    plugin = _ConcretePlugin()
    catalog = MagicMock()
    plugin.list_tables = MagicMock(return_value=[])

    plugin.list_children(catalog, ["cat"])

    plugin.list_tables.assert_called_once_with(catalog)


def test_list_children_empty_when_no_match():
    plugin = _ConcretePlugin()
    catalog = MagicMock()
    plugin.list_tables = MagicMock(return_value=[_make_node(["other", "schema", "table"])])

    result = plugin.list_children(catalog, ["cat"])

    assert result == []


def test_list_children_top_level():
    """list_children with empty parent path returns top-level nodes."""
    plugin = _ConcretePlugin()
    catalog = MagicMock()
    nodes = [
        _make_node(["schema1"]),
        _make_node(["schema2"]),
        _make_node(["schema1", "table1"]),  # too deep
    ]
    plugin.list_tables = MagicMock(return_value=nodes)

    result = plugin.list_children(catalog, [])

    assert len(result) == 2
    assert {n.name for n in result} == {"schema1", "schema2"}
