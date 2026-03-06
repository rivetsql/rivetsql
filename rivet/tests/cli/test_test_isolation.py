"""Tests for build_isolated_assembly (task 8.1)."""

from __future__ import annotations

import pyarrow as pa
import pytest

from rivet_cli.commands.test import TestDiscoveryError, build_isolated_assembly
from rivet_core.assembly import Assembly
from rivet_core.builtins.arrow_catalog import _get_shared_store
from rivet_core.models import Joint
from rivet_core.plugins import PluginRegistry
from rivet_core.testing.models import TestDef


@pytest.fixture(autouse=True)
def _clean_arrow_tables():
    """Ensure _ARROW_TABLES is clean before and after each test."""
    _get_shared_store().clear()
    yield
    _get_shared_store().clear()


def _registry() -> PluginRegistry:
    r = PluginRegistry()
    r.register_builtins()
    return r


def _make_table(**cols: list) -> pa.Table:
    return pa.table(cols)


# ---------------------------------------------------------------------------
# Joint-scope tests
# ---------------------------------------------------------------------------


class TestJointScopeIsolation:
    def test_replaces_all_upstreams_with_fixtures(self):
        """scope=joint: all upstreams of target become source joints."""
        src_a = Joint(name="src_a", joint_type="source", catalog="prod")
        src_b = Joint(name="src_b", joint_type="source", catalog="prod")
        target = Joint(name="transform", joint_type="sql", catalog="prod", upstream=["src_a", "src_b"])
        assembly = Assembly([src_a, src_b, target])

        fixtures = {
            "src_a": _make_table(x=[1, 2]),
            "src_b": _make_table(y=[3, 4]),
        }
        td = TestDef(name="t1", target="transform", scope="joint")

        iso_asm, catalogs, engines = build_isolated_assembly(td, fixtures, assembly, _registry())

        # Isolated assembly has 3 joints: 2 sources + target
        assert len(iso_asm.joints) == 3
        assert iso_asm.joints["src_a"].joint_type == "source"
        assert iso_asm.joints["src_b"].joint_type == "source"
        assert iso_asm.joints["transform"].joint_type == "sql"
        # Catalogs and engines returned
        assert len(catalogs) == 1
        assert catalogs[0].type == "arrow"
        assert len(engines) == 1
        # Fixture data registered in _ARROW_TABLES
        cat_name = catalogs[0].name
        assert (cat_name, "src_a") in _get_shared_store()
        assert (cat_name, "src_b") in _get_shared_store()

    def test_rvt903_on_missing_upstream_input(self):
        """scope=joint: missing fixture for an upstream raises RVT-903."""
        src = Joint(name="src", joint_type="source", catalog="prod")
        target = Joint(name="transform", joint_type="sql", catalog="prod", upstream=["src"])
        assembly = Assembly([src, target])

        fixtures = {}  # no fixture for "src"
        td = TestDef(name="t1", target="transform", scope="joint")

        with pytest.raises(TestDiscoveryError) as exc_info:
            build_isolated_assembly(td, fixtures, assembly, _registry())
        assert exc_info.value.error.code == "RVT-903"

    def test_preserves_target_joint_properties(self):
        """scope=joint: target joint retains sql, assertions, etc."""
        src = Joint(name="src", joint_type="source", catalog="prod")
        target = Joint(
            name="transform", joint_type="sql", catalog="prod",
            upstream=["src"], sql="SELECT * FROM src", tags=["important"],
        )
        assembly = Assembly([src, target])
        fixtures = {"src": _make_table(x=[1])}
        td = TestDef(name="t1", target="transform", scope="joint")

        iso_asm, _, _ = build_isolated_assembly(td, fixtures, assembly, _registry())

        iso_target = iso_asm.joints["transform"]
        assert iso_target.sql == "SELECT * FROM src"
        assert iso_target.tags == ["important"]
        assert iso_target.upstream == ["src"]


# ---------------------------------------------------------------------------
# Assembly-scope tests
# ---------------------------------------------------------------------------


class TestAssemblyScopeIsolation:
    def test_replaces_only_leaf_sources(self):
        """scope=assembly: only leaf sources (no upstream) are replaced."""
        src = Joint(name="src", joint_type="source", catalog="prod")
        mid = Joint(name="mid", joint_type="sql", catalog="prod", upstream=["src"])
        target = Joint(name="target", joint_type="sql", catalog="prod", upstream=["mid"])
        assembly = Assembly([src, mid, target])

        fixtures = {"src": _make_table(x=[1, 2])}
        td = TestDef(name="t1", target="target", scope="assembly")

        iso_asm, catalogs, _ = build_isolated_assembly(td, fixtures, assembly, _registry())

        # All 3 joints present
        assert len(iso_asm.joints) == 3
        # Leaf source replaced
        assert iso_asm.joints["src"].joint_type == "source"
        # Intermediate joint preserved
        assert iso_asm.joints["mid"].joint_type == "sql"
        assert iso_asm.joints["mid"].upstream == ["src"]
        # Fixture data registered
        cat_name = catalogs[0].name
        assert (cat_name, "src") in _get_shared_store()

    def test_multiple_leaf_sources(self):
        """scope=assembly: multiple leaf sources all get replaced."""
        src_a = Joint(name="src_a", joint_type="source", catalog="prod")
        src_b = Joint(name="src_b", joint_type="source", catalog="prod")
        mid = Joint(name="mid", joint_type="sql", catalog="prod", upstream=["src_a", "src_b"])
        target = Joint(name="target", joint_type="sql", catalog="prod", upstream=["mid"])
        assembly = Assembly([src_a, src_b, mid, target])

        fixtures = {
            "src_a": _make_table(x=[1]),
            "src_b": _make_table(y=[2]),
        }
        td = TestDef(name="t1", target="target", scope="assembly")

        iso_asm, catalogs, _ = build_isolated_assembly(td, fixtures, assembly, _registry())

        assert len(iso_asm.joints) == 4
        cat_name = catalogs[0].name
        assert (cat_name, "src_a") in _get_shared_store()
        assert (cat_name, "src_b") in _get_shared_store()


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


class TestIsolationErrors:
    def test_rvt907_target_not_found(self):
        """Raises RVT-907 when target joint doesn't exist in assembly."""
        src = Joint(name="src", joint_type="source", catalog="prod")
        assembly = Assembly([src])

        td = TestDef(name="t1", target="nonexistent", scope="joint")

        with pytest.raises(TestDiscoveryError) as exc_info:
            build_isolated_assembly(td, {}, assembly, _registry())
        assert exc_info.value.error.code == "RVT-907"


# ---------------------------------------------------------------------------
# Isolation guarantees
# ---------------------------------------------------------------------------


class TestIsolationGuarantees:
    def test_each_test_gets_unique_catalog(self):
        """Different tests get different catalog names (no state sharing)."""
        src = Joint(name="src", joint_type="source", catalog="prod")
        target = Joint(name="t", joint_type="sql", catalog="prod", upstream=["src"])
        assembly = Assembly([src, target])
        fixtures = {"src": _make_table(x=[1])}

        td1 = TestDef(name="test_one", target="t", scope="joint")
        td2 = TestDef(name="test_two", target="t", scope="joint")

        _, cats1, _ = build_isolated_assembly(td1, fixtures, assembly, _registry())
        _, cats2, _ = build_isolated_assembly(td2, fixtures, assembly, _registry())

        assert cats1[0].name != cats2[0].name

    def test_returns_arrow_engine(self):
        """Returned engine is an Arrow engine for in-memory execution."""
        src = Joint(name="src", joint_type="source", catalog="prod")
        target = Joint(name="t", joint_type="sql", catalog="prod", upstream=["src"])
        assembly = Assembly([src, target])
        fixtures = {"src": _make_table(x=[1])}
        td = TestDef(name="t1", target="t", scope="joint")

        _, _, engines = build_isolated_assembly(td, fixtures, assembly, _registry())

        assert len(engines) == 1
        assert engines[0].engine_type == "arrow"
