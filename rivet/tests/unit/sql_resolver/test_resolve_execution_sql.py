"""Unit tests for resolve_execution_sql function.

Tests the SQL resolution logic that determines the final SQL string
executed on the engine after all optimizations and transformations.
"""

from __future__ import annotations

from rivet_core.compiler import CompiledJoint
from rivet_core.optimizer import FusedGroup, FusionResult
from rivet_core.sql_resolver import resolve_execution_sql


def _make_joint(
    name: str,
    *,
    sql: str | None = None,
    sql_translated: str | None = None,
    sql_resolved: str | None = None,
) -> CompiledJoint:
    """Create a minimal CompiledJoint for testing."""
    return CompiledJoint(
        name=name,
        type="sql",
        catalog=None,
        catalog_type=None,
        engine="duckdb",
        engine_resolution=None,
        adapter=None,
        sql=sql,
        sql_translated=sql_translated,
        sql_resolved=sql_resolved,
        sql_dialect=None,
        engine_dialect=None,
        upstream=[],
        eager=False,
        table=None,
        write_strategy=None,
        function=None,
        source_file=None,
        logical_plan=None,
        output_schema=None,
        column_lineage=[],
        optimizations=[],
        checks=[],
        fused_group_id=None,
        tags=[],
        description=None,
        fusion_strategy_override=None,
        materialization_strategy_override=None,
    )


def _make_group(
    joints: list[str],
    *,
    fused_sql: str | None = None,
    resolved_sql: str | None = None,
    fusion_result: FusionResult | None = None,
) -> FusedGroup:
    """Create a minimal FusedGroup for testing."""
    return FusedGroup(
        id="test_group",
        joints=joints,
        engine="duckdb",
        engine_type="duckdb",
        adapters={j: None for j in joints},
        fused_sql=fused_sql,
        resolved_sql=resolved_sql,
        fusion_result=fusion_result,
    )


class TestResolvedSqlPath:
    """Tests for resolved SQL preference path (Requirements 1.1, 1.3, 1.4)."""

    def test_fusion_result_resolved_fused_sql_preferred(self) -> None:
        """When fusion_result.resolved_fused_sql exists, it is returned."""
        fusion_result = FusionResult(
            fused_sql="SELECT * FROM table",
            statements=[],
            final_select="SELECT * FROM table",
            resolved_fused_sql="SELECT * FROM catalog.schema.table",
        )
        group = _make_group(["joint1"], fusion_result=fusion_result)
        joint_map = {"joint1": _make_joint("joint1")}

        result = resolve_execution_sql(group, joint_map, set())

        assert result == "SELECT * FROM catalog.schema.table"

    def test_resolved_sql_used_when_fusion_result_has_no_resolved(self) -> None:
        """When fusion_result exists but has no resolved_fused_sql, use group.resolved_sql."""
        fusion_result = FusionResult(
            fused_sql="SELECT * FROM table",
            statements=[],
            final_select="SELECT * FROM table",
            resolved_fused_sql=None,
        )
        group = _make_group(
            ["joint1"],
            fusion_result=fusion_result,
            resolved_sql="SELECT * FROM catalog.schema.table",
        )
        joint_map = {"joint1": _make_joint("joint1")}

        result = resolve_execution_sql(group, joint_map, set())

        assert result == "SELECT * FROM catalog.schema.table"

    def test_resolved_sql_used_when_no_fusion_result(self) -> None:
        """When no fusion_result, use group.resolved_sql directly."""
        group = _make_group(
            ["joint1"],
            resolved_sql="SELECT * FROM catalog.schema.table",
        )
        joint_map = {"joint1": _make_joint("joint1")}

        result = resolve_execution_sql(group, joint_map, set())

        assert result == "SELECT * FROM catalog.schema.table"

    def test_resolved_sql_skipped_with_materialized_inputs(self) -> None:
        """When has_materialized_inputs=True, resolved SQL is skipped."""
        group = _make_group(
            ["joint1"],
            resolved_sql="SELECT * FROM catalog.schema.table",
            fused_sql="SELECT * FROM joint1",
        )
        joint_map = {"joint1": _make_joint("joint1")}

        result = resolve_execution_sql(group, joint_map, set(), has_materialized_inputs=True)

        # Should skip resolved SQL and use fused_sql
        assert result == "SELECT * FROM joint1"


class TestAdapterRewritePath:
    """Tests for adapter rewrite logic (Requirements 1.4)."""

    def test_adapter_source_rewritten_to_select_star(self):
        """Adapter-read sources are rewritten to SELECT * FROM <name>."""
        joint1 = _make_joint("joint1", sql="SELECT id FROM source")
        joint2 = _make_joint("joint2", sql="SELECT * FROM joint1")

        group = _make_group(["joint1", "joint2"])
        joint_map = {"joint1": joint1, "joint2": joint2}
        adapter_read_sources = {"joint1"}

        result = resolve_execution_sql(group, joint_map, adapter_read_sources)

        # Should contain rewritten CTE for joint1
        assert result is not None
        assert "joint1 AS" in result
        assert "SELECT * FROM joint1" in result
        # joint2 should use its original SQL
        assert "SELECT * FROM joint1" in result

    def test_multiple_adapter_sources_all_rewritten(self):
        """Multiple adapter sources are all rewritten."""
        joint1 = _make_joint("joint1", sql="SELECT id FROM source1")
        joint2 = _make_joint("joint2", sql="SELECT id FROM source2")
        joint3 = _make_joint("joint3", sql="SELECT * FROM joint1 JOIN joint2")

        group = _make_group(["joint1", "joint2", "joint3"])
        joint_map = {"joint1": joint1, "joint2": joint2, "joint3": joint3}
        adapter_read_sources = {"joint1", "joint2"}

        result = resolve_execution_sql(group, joint_map, adapter_read_sources)

        assert result is not None
        assert "joint1 AS" in result
        assert "joint2 AS" in result
        # Both should be rewritten to SELECT *
        assert result.count("SELECT * FROM joint1") >= 1
        assert result.count("SELECT * FROM joint2") >= 1

    def test_adapter_rewrite_only_for_multi_joint_groups(self):
        """Adapter rewrite only applies when group has multiple joints."""
        joint1 = _make_joint("joint1", sql="SELECT id FROM source")

        group = _make_group(["joint1"], fused_sql="SELECT id FROM source")
        joint_map = {"joint1": joint1}
        adapter_read_sources = {"joint1"}

        result = resolve_execution_sql(group, joint_map, adapter_read_sources)

        # Single joint group should use fused_sql, not rewrite
        assert result == "SELECT id FROM source"

    def test_non_adapter_joints_use_translated_sql(self):
        """Non-adapter joints use sql_translated when available."""
        joint1 = _make_joint("joint1", sql="SELECT id FROM source")
        joint2 = _make_joint(
            "joint2",
            sql="SELECT * FROM joint1",
            sql_translated="SELECT * FROM joint1 -- translated",
        )

        group = _make_group(["joint1", "joint2"])
        joint_map = {"joint1": joint1, "joint2": joint2}
        adapter_read_sources = {"joint1"}

        result = resolve_execution_sql(group, joint_map, adapter_read_sources)

        assert result is not None
        # joint2 should use translated SQL
        assert "-- translated" in result

    def test_non_adapter_joints_fallback_to_original_sql(self):
        """Non-adapter joints fall back to original sql when no translation."""
        joint1 = _make_joint("joint1", sql="SELECT id FROM source")
        joint2 = _make_joint("joint2", sql="SELECT * FROM joint1")

        group = _make_group(["joint1", "joint2"])
        joint_map = {"joint1": joint1, "joint2": joint2}
        adapter_read_sources = {"joint1"}

        result = resolve_execution_sql(group, joint_map, adapter_read_sources)

        assert result is not None
        assert "SELECT * FROM joint1" in result


class TestMaterializedInputsPath:
    """Tests for materialized inputs handling (Requirements 1.1, 1.3)."""

    def test_materialized_inputs_skips_resolved_sql(self):
        """When has_materialized_inputs=True, resolved SQL is skipped."""
        fusion_result = FusionResult(
            fused_sql="SELECT * FROM joint1",
            statements=[],
            final_select="SELECT * FROM joint1",
            resolved_fused_sql="SELECT * FROM catalog.schema.table",
        )
        group = _make_group(["joint1"], fusion_result=fusion_result)
        joint_map = {"joint1": _make_joint("joint1")}

        result = resolve_execution_sql(group, joint_map, set(), has_materialized_inputs=True)

        # Should use fused_sql, not resolved_fused_sql
        assert result == "SELECT * FROM joint1"

    def test_materialized_inputs_uses_fusion_result_fused_sql(self):
        """With materialized inputs, fusion_result.fused_sql is used."""
        fusion_result = FusionResult(
            fused_sql="SELECT * FROM joint1",
            statements=[],
            final_select="SELECT * FROM joint1",
            resolved_fused_sql="SELECT * FROM catalog.schema.table",
        )
        group = _make_group(["joint1"], fusion_result=fusion_result)
        joint_map = {"joint1": _make_joint("joint1")}

        result = resolve_execution_sql(group, joint_map, set(), has_materialized_inputs=True)

        assert result == "SELECT * FROM joint1"

    def test_materialized_inputs_falls_back_to_group_fused_sql(self):
        """With materialized inputs and no fusion_result, use group.fused_sql."""
        group = _make_group(
            ["joint1"],
            fused_sql="SELECT * FROM joint1",
            resolved_sql="SELECT * FROM catalog.schema.table",
        )
        joint_map = {"joint1": _make_joint("joint1")}

        result = resolve_execution_sql(group, joint_map, set(), has_materialized_inputs=True)

        assert result == "SELECT * FROM joint1"


class TestSingleJointFallback:
    """Tests for single joint fallback path (Requirements 1.1)."""

    def test_single_joint_without_fused_sql_returns_none(self):
        """Single joint group without fused_sql returns None."""
        joint1 = _make_joint(
            "joint1",
            sql="SELECT * FROM table",
            sql_translated="SELECT * FROM table -- translated",
        )
        group = _make_group(["joint1"])
        joint_map = {"joint1": joint1}

        result = resolve_execution_sql(group, joint_map, set())

        # Function doesn't fall back to joint SQL, returns None
        assert result is None

    def test_single_joint_with_fused_sql_returns_fused(self):
        """Single joint group with fused_sql returns it."""
        joint1 = _make_joint("joint1", sql="SELECT * FROM table")
        group = _make_group(["joint1"], fused_sql="SELECT * FROM table -- fused")
        joint_map = {"joint1": joint1}

        result = resolve_execution_sql(group, joint_map, set())

        assert result == "SELECT * FROM table -- fused"

    def test_single_joint_with_fusion_result_returns_fused_sql(self):
        """Single joint group with fusion_result returns fusion_result.fused_sql."""
        fusion_result = FusionResult(
            fused_sql="SELECT * FROM table -- fusion",
            statements=[],
            final_select="SELECT * FROM table",
        )
        group = _make_group(["joint1"], fusion_result=fusion_result)
        joint_map = {"joint1": _make_joint("joint1")}

        result = resolve_execution_sql(group, joint_map, set(), has_materialized_inputs=True)

        assert result == "SELECT * FROM table -- fusion"

    def test_single_joint_with_resolved_sql_prefers_resolved(self):
        """Single joint with resolved SQL prefers resolved over fused."""
        joint1 = _make_joint("joint1", sql="SELECT * FROM table")
        group = _make_group(
            ["joint1"],
            fused_sql="SELECT * FROM table",
            resolved_sql="SELECT * FROM catalog.schema.table",
        )
        joint_map = {"joint1": joint1}

        result = resolve_execution_sql(group, joint_map, set())

        assert result == "SELECT * FROM catalog.schema.table"


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_group_returns_none(self):
        """Group with no joints returns None."""
        group = _make_group([])
        joint_map = {}

        result = resolve_execution_sql(group, joint_map, set())

        assert result is None

    def test_no_sql_available_returns_none(self):
        """Group with no SQL sources returns None."""
        group = _make_group(["joint1"])
        joint_map = {"joint1": _make_joint("joint1", sql=None)}

        result = resolve_execution_sql(group, joint_map, set())

        assert result is None

    def test_missing_joint_in_map_handled_gracefully(self):
        """Missing joint in joint_map is handled gracefully with adapter rewrite."""
        group = _make_group(["joint1", "joint2"])
        joint_map = {"joint1": _make_joint("joint1", sql="SELECT 1")}
        # joint2 is missing from joint_map
        adapter_read_sources = {"joint1"}

        result = resolve_execution_sql(group, joint_map, adapter_read_sources)

        # Should compose with available joints (single joint results in no CTE)
        assert result is not None
        assert "SELECT * FROM joint1" in result

    def test_adapter_rewrite_with_none_sql_handled(self):
        """Adapter rewrite handles joints with None SQL."""
        joint1 = _make_joint("joint1", sql=None)
        joint2 = _make_joint("joint2", sql="SELECT * FROM joint1")

        group = _make_group(["joint1", "joint2"])
        joint_map = {"joint1": joint1, "joint2": joint2}
        adapter_read_sources = {"joint1"}

        result = resolve_execution_sql(group, joint_map, adapter_read_sources)

        # Should handle None SQL gracefully
        assert result is not None


class TestResolutionPriority:
    """Tests for SQL resolution priority order."""

    def test_priority_resolved_over_fused(self):
        """Resolved SQL has priority over fused SQL."""
        group = _make_group(
            ["joint1"],
            fused_sql="SELECT * FROM table",
            resolved_sql="SELECT * FROM catalog.schema.table",
        )
        joint_map = {"joint1": _make_joint("joint1")}

        result = resolve_execution_sql(group, joint_map, set())

        assert result == "SELECT * FROM catalog.schema.table"

    def test_priority_fusion_result_resolved_over_group_resolved(self):
        """fusion_result.resolved_fused_sql has priority over group.resolved_sql."""
        fusion_result = FusionResult(
            fused_sql="SELECT * FROM table",
            statements=[],
            final_select="SELECT * FROM table",
            resolved_fused_sql="SELECT * FROM catalog.schema.table_fusion",
        )
        group = _make_group(
            ["joint1"],
            fusion_result=fusion_result,
            resolved_sql="SELECT * FROM catalog.schema.table_group",
        )
        joint_map = {"joint1": _make_joint("joint1")}

        result = resolve_execution_sql(group, joint_map, set())

        assert result == "SELECT * FROM catalog.schema.table_fusion"

    def test_priority_adapter_rewrite_over_fused_sql(self):
        """Adapter rewrite has priority over fused_sql when applicable."""
        joint1 = _make_joint("joint1", sql="SELECT id FROM source")
        joint2 = _make_joint("joint2", sql="SELECT * FROM joint1")

        group = _make_group(
            ["joint1", "joint2"],
            fused_sql="WITH joint1 AS (SELECT id FROM source) SELECT * FROM joint1",
        )
        joint_map = {"joint1": joint1, "joint2": joint2}
        adapter_read_sources = {"joint1"}

        result = resolve_execution_sql(group, joint_map, adapter_read_sources)

        # Should use rewritten CTE, not original fused_sql
        assert result is not None
        assert "SELECT * FROM joint1" in result
        assert "SELECT id FROM source" not in result

    def test_priority_fused_sql_over_joint_sql(self):
        """group.fused_sql has priority over individual joint SQL."""
        joint1 = _make_joint("joint1", sql="SELECT * FROM table")
        group = _make_group(["joint1"], fused_sql="SELECT * FROM table -- fused")
        joint_map = {"joint1": joint1}

        result = resolve_execution_sql(group, joint_map, set())

        assert result == "SELECT * FROM table -- fused"

    def test_priority_fusion_result_fused_sql_over_group_fused_sql(self):
        """fusion_result.fused_sql has priority over group.fused_sql."""
        fusion_result = FusionResult(
            fused_sql="SELECT * FROM table -- fusion_result",
            statements=[],
            final_select="SELECT * FROM table",
        )
        group = _make_group(
            ["joint1"],
            fusion_result=fusion_result,
            fused_sql="SELECT * FROM table -- group",
        )
        joint_map = {"joint1": _make_joint("joint1")}

        result = resolve_execution_sql(group, joint_map, set(), has_materialized_inputs=True)

        assert result == "SELECT * FROM table -- fusion_result"


class TestComplexScenarios:
    """Tests for complex multi-joint scenarios."""

    def test_three_joint_group_with_adapter_rewrite(self):
        """Three-joint group with adapter source is rewritten correctly."""
        joint1 = _make_joint("joint1", sql="SELECT id, name FROM source")
        joint2 = _make_joint("joint2", sql="SELECT id, UPPER(name) as name FROM joint1")
        joint3 = _make_joint("joint3", sql="SELECT * FROM joint2 WHERE id > 10")

        group = _make_group(["joint1", "joint2", "joint3"])
        joint_map = {"joint1": joint1, "joint2": joint2, "joint3": joint3}
        adapter_read_sources = {"joint1"}

        result = resolve_execution_sql(group, joint_map, adapter_read_sources)

        assert result is not None
        # joint1 should be rewritten
        assert "joint1 AS" in result
        assert "SELECT * FROM joint1" in result
        # joint2 and joint3 should use original SQL
        assert "UPPER(name)" in result
        assert "WHERE id > 10" in result

    def test_mixed_adapter_and_sql_joints(self):
        """Group with mix of adapter and SQL joints."""
        joint1 = _make_joint("joint1", sql="SELECT id FROM source1")
        joint2 = _make_joint("joint2", sql="SELECT id FROM source2")
        joint3 = _make_joint("joint3", sql="SELECT * FROM joint1 UNION SELECT * FROM joint2")

        group = _make_group(["joint1", "joint2", "joint3"])
        joint_map = {"joint1": joint1, "joint2": joint2, "joint3": joint3}
        adapter_read_sources = {"joint1"}  # Only joint1 is adapter-read

        result = resolve_execution_sql(group, joint_map, adapter_read_sources)

        assert result is not None
        # joint1 rewritten
        assert "SELECT * FROM joint1" in result
        # joint2 uses original SQL
        assert "SELECT id FROM source2" in result

    def test_adapter_rewrite_with_sql_resolved(self):
        """Adapter rewrite uses sql_resolved when available."""
        joint1 = _make_joint(
            "joint1",
            sql="SELECT id FROM source",
            sql_resolved="SELECT id FROM catalog.schema.source",
        )
        joint2 = _make_joint("joint2", sql="SELECT * FROM joint1")

        group = _make_group(["joint1", "joint2"])
        joint_map = {"joint1": joint1, "joint2": joint2}
        adapter_read_sources = {"joint1"}

        result = resolve_execution_sql(group, joint_map, adapter_read_sources)

        assert result is not None
        # joint1 should still be rewritten to SELECT * (adapter rewrite takes precedence)
        assert "SELECT * FROM joint1" in result

    def test_resolved_sql_bypasses_adapter_rewrite(self):
        """When resolved SQL exists and no materialized inputs, adapter rewrite is bypassed."""
        joint1 = _make_joint("joint1", sql="SELECT id FROM source")
        joint2 = _make_joint("joint2", sql="SELECT * FROM joint1")

        fusion_result = FusionResult(
            fused_sql="WITH joint1 AS (SELECT id FROM source) SELECT * FROM joint1",
            statements=[],
            final_select="SELECT * FROM joint1",
            resolved_fused_sql="SELECT * FROM catalog.schema.table",
        )
        group = _make_group(["joint1", "joint2"], fusion_result=fusion_result)
        joint_map = {"joint1": joint1, "joint2": joint2}
        adapter_read_sources = {"joint1"}

        result = resolve_execution_sql(group, joint_map, adapter_read_sources)

        # Resolved SQL should be used, bypassing adapter rewrite
        assert result == "SELECT * FROM catalog.schema.table"
