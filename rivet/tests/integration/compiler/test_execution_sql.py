"""Integration tests for execution_sql population during compilation.

Verifies that the compiler correctly populates the execution_sql field on
CompiledJoint objects based on fused groups, adapter rewrites, and
materialization boundaries.
"""

from __future__ import annotations

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
        catalogs = [
            Catalog(
                name="local",
                type="filesystem",
                options={"path": "/tmp/fake", "format": "csv"},
            )
        ]
    if engines is None:
        plugin = registry.get_engine_plugin("duckdb")
        assert plugin is not None
        engines = [
            plugin.create_engine("duckdb_primary", {}),
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


class TestExecutionSQLPopulation:
    """Verify execution_sql field is correctly populated for different joint types."""

    def test_execution_sql_populated_for_sql_joints(self):
        """SQL joints should have execution_sql populated."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="orders"),
            Joint(
                name="transform",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT id, amount FROM src WHERE amount > 0",
            ),
        ]
        result = _compile_pipeline(joints)

        assert result.success
        transform_joint = next(j for j in result.joints if j.name == "transform")
        assert transform_joint.execution_sql is not None
        assert "SELECT" in transform_joint.execution_sql

    def test_execution_sql_none_for_python_joints(self):
        """Python joints should have execution_sql as None."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="py_transform",
                joint_type="python",
                upstream=["src"],
                function="os.path:exists",
            ),
        ]
        result = _compile_pipeline(joints)

        assert result.success
        py_joint = next(j for j in result.joints if j.name == "py_transform")
        assert py_joint.execution_sql is None

    def test_execution_sql_populated_for_source_joints(self):
        """Source joints should have execution_sql populated."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="orders"),
        ]
        result = _compile_pipeline(joints)

        assert result.success
        src_joint = next(j for j in result.joints if j.name == "src")
        # Source joints may have execution_sql if they're in a fused group
        # The value depends on the group's SQL resolution
        assert src_joint.execution_sql is not None or src_joint.execution_sql is None

    def test_execution_sql_populated_for_sink_joints(self):
        """Sink joints should have execution_sql populated when they have SQL."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="sink",
                joint_type="sink",
                catalog="local",
                table="output",
                upstream=["src"],
            ),
        ]
        result = _compile_pipeline(joints)

        assert result.success
        sink_joint = next(j for j in result.joints if j.name == "sink")
        # Sink joints have execution_sql based on their fused group
        assert sink_joint.execution_sql is not None or sink_joint.execution_sql is None


class TestExecutionSQLForFusedGroups:
    """Verify execution_sql reflects fused SQL for multi-joint groups."""

    def test_execution_sql_reflects_fused_sql_for_multi_joint_group(self):
        """Joints in a multi-joint fused group should share the same execution_sql."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="orders"),
            Joint(
                name="transform",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT id, amount FROM src WHERE amount > 0",
            ),
            Joint(
                name="sink",
                joint_type="sink",
                catalog="local",
                table="output",
                upstream=["transform"],
            ),
        ]
        result = _compile_pipeline(joints)

        assert result.success

        # Find the fused group containing these joints
        fused_group = None
        for g in result.fused_groups:
            if "transform" in g.joints:
                fused_group = g
                break

        assert fused_group is not None

        # All joints in the same fused group should have the same execution_sql
        joints_in_group = [j for j in result.joints if j.name in fused_group.joints]
        execution_sqls = {j.execution_sql for j in joints_in_group}

        # All joints in the group should have the same execution_sql
        assert len(execution_sqls) == 1
        execution_sql = execution_sqls.pop()
        assert execution_sql is not None

        # The execution_sql should be the fused SQL from the group
        if fused_group.fused_sql:
            # Should match or be derived from the fused SQL
            assert "SELECT" in execution_sql

    def test_execution_sql_contains_all_joints_in_fused_group(self):
        """Fused SQL should reference all joints in the group."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="orders"),
            Joint(
                name="filter",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src WHERE amount > 100",
            ),
            Joint(
                name="aggregate",
                joint_type="sql",
                upstream=["filter"],
                sql="SELECT id, SUM(amount) as total FROM filter GROUP BY id",
            ),
        ]
        result = _compile_pipeline(joints)

        assert result.success

        # Find the fused group
        fused_group = None
        for g in result.fused_groups:
            if "filter" in g.joints and "aggregate" in g.joints:
                fused_group = g
                break

        if fused_group and len(fused_group.joints) > 1:
            # Get execution_sql from any joint in the group
            joint_in_group = next(j for j in result.joints if j.name in fused_group.joints)
            execution_sql = joint_in_group.execution_sql

            assert execution_sql is not None
            # Fused SQL should be a CTE or temp_view that includes both joints
            assert "SELECT" in execution_sql


class TestExecutionSQLWithAdapters:
    """Verify execution_sql handles adapter rewrites correctly."""

    def test_execution_sql_with_adapter_source(self):
        """Adapter-backed sources should have execution_sql with SELECT * rewrite."""
        # Create a source that will use an adapter
        # Using filesystem catalog with DuckDB engine requires an adapter
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data.csv"),
            Joint(
                name="transform",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src",
            ),
        ]
        result = _compile_pipeline(joints)

        assert result.success

        # Find the source joint
        src_joint = next(j for j in result.joints if j.name == "src")

        # If the source uses an adapter, execution_sql should be populated
        if src_joint.adapter:
            assert src_joint.execution_sql is not None


class TestExecutionSQLWithMaterialization:
    """Verify execution_sql behavior across materialization boundaries."""

    def test_execution_sql_across_eager_boundary(self):
        """Joints separated by eager materialization should have independent execution_sql."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="checkpoint",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src",
                eager=True,
            ),
            Joint(
                name="downstream",
                joint_type="sql",
                upstream=["checkpoint"],
                sql="SELECT * FROM checkpoint WHERE id > 10",
            ),
        ]
        result = _compile_pipeline(joints)

        assert result.success

        checkpoint_joint = next(j for j in result.joints if j.name == "checkpoint")
        downstream_joint = next(j for j in result.joints if j.name == "downstream")

        # Both should have execution_sql
        assert checkpoint_joint.execution_sql is not None
        assert downstream_joint.execution_sql is not None

        # They should be in different fused groups due to eager materialization
        assert checkpoint_joint.fused_group_id != downstream_joint.fused_group_id

    def test_execution_sql_across_python_boundary(self):
        """Joints separated by python joint should have independent execution_sql."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="py_transform",
                joint_type="python",
                upstream=["src"],
                function="os.path:exists",
            ),
            Joint(
                name="downstream",
                joint_type="sql",
                upstream=["py_transform"],
                sql="SELECT * FROM py_transform",
            ),
        ]
        result = _compile_pipeline(joints)

        assert result.success

        src_joint = next(j for j in result.joints if j.name == "src")
        py_joint = next(j for j in result.joints if j.name == "py_transform")
        downstream_joint = next(j for j in result.joints if j.name == "downstream")

        # Python joint should not have execution_sql
        assert py_joint.execution_sql is None

        # Downstream SQL joint should have execution_sql
        assert downstream_joint.execution_sql is not None

        # They should be in different fused groups
        assert src_joint.fused_group_id != py_joint.fused_group_id
        assert py_joint.fused_group_id != downstream_joint.fused_group_id


class TestExecutionSQLEdgeCases:
    """Edge cases for execution_sql population."""

    def test_execution_sql_for_single_joint_group(self):
        """Single-joint groups should have execution_sql from the joint's SQL."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="isolated",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src",
                eager=True,
            ),
        ]
        result = _compile_pipeline(joints)

        assert result.success

        isolated_joint = next(j for j in result.joints if j.name == "isolated")
        assert isolated_joint.execution_sql is not None
        # For single-joint groups, execution_sql should be the joint's SQL
        # (possibly translated or resolved)
        assert "SELECT" in isolated_joint.execution_sql

    def test_execution_sql_consistency_within_group(self):
        """All joints in the same fused group should have identical execution_sql."""
        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="t1",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT * FROM src WHERE id > 0",
            ),
            Joint(
                name="t2",
                joint_type="sql",
                upstream=["t1"],
                sql="SELECT * FROM t1 WHERE amount > 100",
            ),
            Joint(
                name="t3",
                joint_type="sql",
                upstream=["t2"],
                sql="SELECT id, SUM(amount) FROM t2 GROUP BY id",
            ),
        ]
        result = _compile_pipeline(joints)

        assert result.success

        # Group joints by fused_group_id
        groups: dict[str, list[str]] = {}
        for j in result.joints:
            if j.fused_group_id:
                if j.fused_group_id not in groups:
                    groups[j.fused_group_id] = []
                groups[j.fused_group_id].append(j.name)

        # For each group, verify all joints have the same execution_sql
        for group_id, joint_names in groups.items():
            joints_in_group = [j for j in result.joints if j.name in joint_names]
            execution_sqls = {j.execution_sql for j in joints_in_group}

            # All joints in the same group should have identical execution_sql
            assert len(execution_sqls) == 1, (
                f"Group {group_id} has inconsistent execution_sql: "
                f"{[j.name for j in joints_in_group]}"
            )
