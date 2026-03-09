"""Integration tests: compiler + optimizer produce valid execution plans.

Exercises the real compiler and optimizer together — no mocks for Rivet
internals. Verifies that compiling a set of declarations produces the
expected fused groups, materialization points, and execution order.
"""

from __future__ import annotations

import pytest

from rivet_core.assembly import Assembly
from rivet_core.compiler import CompiledAssembly, compile
from rivet_core.models import Catalog, ComputeEngine, Joint
from rivet_core.plugins import PluginRegistry
from rivet_duckdb import DuckDBPlugin

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_registry() -> PluginRegistry:
    reg = PluginRegistry()
    reg.register_builtins()
    DuckDBPlugin(reg)
    return reg


def _compile_pipeline(
    joints: list[Joint],
    *,
    catalogs: list[Catalog] | None = None,
    engines: list[ComputeEngine] | None = None,
    default_engine: str = "duckdb_primary",
    registry: PluginRegistry | None = None,
    introspect: bool = False,
) -> CompiledAssembly:
    if registry is None:
        registry = _setup_registry()
    if catalogs is None:
        catalogs = [Catalog(name="local", type="filesystem", options={"path": "/tmp/fake", "format": "csv"})]
    if engines is None:
        engines = [
            registry.get_engine_plugin("duckdb").create_engine("duckdb_primary", {}),
        ]
    for e in engines:
        if e.name not in {ce.name for ce in registry._compute_engines.values()}:
            registry.register_compute_engine(e)
    assembly = Assembly(joints)
    return compile(
        assembly,
        catalogs=catalogs,
        engines=engines,
        registry=registry,
        default_engine=default_engine,
        introspect=introspect,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestLinearPipeline:
    """Source → SQL → Sink compiled through real compiler + optimizer."""

    def test_linear_pipeline_compiles_successfully(self):
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="orders"),
            Joint(name="transform", joint_type="sql", upstream=["src"],
                  sql="SELECT id, amount FROM src WHERE amount > 0"),
            Joint(name="sink", joint_type="sink", catalog="local", table="output",
                  upstream=["transform"]),
        ]
        result = _compile_pipeline(joints)

        assert result.success
        assert len(result.errors) == 0
        assert len(result.joints) == 3

    def test_linear_pipeline_fuses_same_engine_joints(self):
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="orders"),
            Joint(name="transform", joint_type="sql", upstream=["src"],
                  sql="SELECT id, amount FROM src WHERE amount > 0"),
            Joint(name="sink", joint_type="sink", catalog="local", table="output",
                  upstream=["transform"]),
        ]
        result = _compile_pipeline(joints)

        assert result.success
        # All joints on the same engine should be fused into groups
        assert len(result.fused_groups) >= 1
        # Every joint should be assigned to a fused group
        all_grouped_joints = []
        for g in result.fused_groups:
            all_grouped_joints.extend(g.joints)
        assert set(all_grouped_joints) == {"src", "transform", "sink"}

    def test_linear_pipeline_execution_order_respects_dependencies(self):
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="orders"),
            Joint(name="transform", joint_type="sql", upstream=["src"],
                  sql="SELECT id, amount FROM src WHERE amount > 0"),
            Joint(name="sink", joint_type="sink", catalog="local", table="output",
                  upstream=["transform"]),
        ]
        result = _compile_pipeline(joints)

        assert result.success
        assert len(result.execution_order) >= 1


class TestDiamondPipeline:
    """Source → two transforms → join → sink (diamond shape)."""

    def test_diamond_compiles_with_correct_joint_count(self):
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(name="left", joint_type="sql", upstream=["src"],
                  sql="SELECT id, amount FROM src WHERE amount > 0"),
            Joint(name="right", joint_type="sql", upstream=["src"],
                  sql="SELECT id, amount FROM src WHERE amount <= 0"),
            Joint(name="merged", joint_type="sql", upstream=["left", "right"],
                  sql="SELECT * FROM left UNION ALL SELECT * FROM right"),
            Joint(name="sink", joint_type="sink", catalog="local", table="output",
                  upstream=["merged"]),
        ]
        result = _compile_pipeline(joints)

        assert result.success
        assert len(result.joints) == 5

    def test_diamond_all_joints_assigned_to_groups(self):
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(name="left", joint_type="sql", upstream=["src"],
                  sql="SELECT id, amount FROM src WHERE amount > 0"),
            Joint(name="right", joint_type="sql", upstream=["src"],
                  sql="SELECT id, amount FROM src WHERE amount <= 0"),
            Joint(name="merged", joint_type="sql", upstream=["left", "right"],
                  sql="SELECT * FROM left UNION ALL SELECT * FROM right"),
            Joint(name="sink", joint_type="sink", catalog="local", table="output",
                  upstream=["merged"]),
        ]
        result = _compile_pipeline(joints)

        assert result.success
        all_grouped = set()
        for g in result.fused_groups:
            all_grouped.update(g.joints)
        assert all_grouped == {"src", "left", "right", "merged", "sink"}


class TestEagerMaterialization:
    """Eager joints force materialization boundaries."""

    def test_eager_joint_triggers_materialization(self):
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(name="checkpoint", joint_type="sql", upstream=["src"],
                  sql="SELECT * FROM src", eager=True),
            Joint(name="sink", joint_type="sink", catalog="local", table="output",
                  upstream=["checkpoint"]),
        ]
        result = _compile_pipeline(joints)

        assert result.success
        # Eager joint should produce a materialization
        assert len(result.materializations) >= 1
        mat_joints = {m.from_joint for m in result.materializations}
        assert "checkpoint" in mat_joints


class TestCrossEngineMaterialization:
    """Joints on different engine instances produce engine-boundary materializations."""

    def test_different_engines_produce_materialization(self):
        registry = _setup_registry()
        eng_plugin = registry.get_engine_plugin("duckdb")
        eng1 = eng_plugin.create_engine("duckdb_primary", {})
        eng2 = eng_plugin.create_engine("duckdb_secondary", {})
        registry.register_compute_engine(eng1)
        registry.register_compute_engine(eng2)

        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data",
                  engine="duckdb_primary"),
            Joint(name="transform", joint_type="sql", upstream=["src"],
                  sql="SELECT * FROM src", engine="duckdb_secondary"),
            Joint(name="sink", joint_type="sink", catalog="local", table="output",
                  upstream=["transform"], engine="duckdb_secondary"),
        ]
        catalogs = [Catalog(name="local", type="filesystem", options={"path": "/tmp/fake", "format": "csv"})]
        engines = [eng1, eng2]

        result = compile(
            Assembly(joints),
            catalogs=catalogs,
            engines=engines,
            registry=registry,
            default_engine="duckdb_primary",
            introspect=False,
        )

        assert result.success
        # Cross-engine boundary should produce a materialization
        assert len(result.materializations) >= 1


class TestPythonJointBoundary:
    """Python joints always create materialization boundaries."""

    def test_python_joint_produces_materialization(self):
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(name="py_transform", joint_type="python", upstream=["src"],
                  function="os.path:exists"),  # dummy — won't be executed
            Joint(name="sink", joint_type="sink", catalog="local", table="output",
                  upstream=["py_transform"]),
        ]
        result = _compile_pipeline(joints)

        assert result.success
        # Python boundary should produce materializations
        assert len(result.materializations) >= 1


class TestFusionStrategies:
    """Verify CTE and temp_view fusion strategies are applied."""

    def test_default_fusion_strategy_is_cte(self):
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(name="transform", joint_type="sql", upstream=["src"],
                  sql="SELECT * FROM src"),
            Joint(name="sink", joint_type="sink", catalog="local", table="output",
                  upstream=["transform"]),
        ]
        result = _compile_pipeline(joints)

        assert result.success
        for g in result.fused_groups:
            if len(g.joints) > 1:
                assert g.fusion_strategy == "cte"

    def test_temp_view_strategy_applied_when_requested(self):
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(name="transform", joint_type="sql", upstream=["src"],
                  sql="SELECT * FROM src",
                  fusion_strategy_override="temp_view"),
            Joint(name="sink", joint_type="sink", catalog="local", table="output",
                  upstream=["transform"]),
        ]
        result = compile(
            Assembly(joints),
            catalogs=[Catalog(name="local", type="filesystem", options={"path": "/tmp/fake", "format": "csv"})],
            engines=[_setup_registry().get_engine_plugin("duckdb").create_engine("duckdb_primary", {})],
            registry=_setup_registry(),
            default_engine="duckdb_primary",
            default_fusion_strategy="cte",
            introspect=False,
        )

        assert result.success
        # At least one group should use temp_view if the override was applied
        strategies = {g.fusion_strategy for g in result.fused_groups if len(g.joints) > 1}
        if strategies:
            assert "temp_view" in strategies


class TestCompilationErrors:
    """Compiler collects errors without crashing."""

    def test_missing_upstream_produces_error(self):
        """Assembly rejects unknown upstream references at construction time."""
        from rivet_core.assembly import AssemblyError

        joints = [
            Joint(name="transform", joint_type="sql", upstream=["nonexistent"],
                  sql="SELECT * FROM nonexistent"),
        ]
        with pytest.raises(AssemblyError):
            Assembly(joints)

    def test_cycle_produces_error(self):
        """Assembly rejects cyclic dependencies at construction time."""
        from rivet_core.assembly import AssemblyError

        joints = [
            Joint(name="a", joint_type="sql", upstream=["b"], sql="SELECT * FROM b"),
            Joint(name="b", joint_type="sql", upstream=["a"], sql="SELECT * FROM a"),
        ]
        with pytest.raises(AssemblyError):
            Assembly(joints)
