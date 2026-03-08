"""Tests for run_text rendering — wave grouping display."""

from __future__ import annotations

from rivet_cli.rendering.run_text import render_run_text
from rivet_core.compiler import (
    CompiledAssembly,
    CompiledCatalog,
    CompiledEngine,
    CompiledJoint,
    ExecutionWave,
)
from rivet_core.executor import ExecutionResult, JointExecutionResult, PhasedTiming
from rivet_core.optimizer import FusedGroup


def _make_joint(name: str, engine: str, group_id: str) -> CompiledJoint:
    return CompiledJoint(
        name=name, type="sql", catalog=None, catalog_type=None,
        engine=engine, engine_resolution="project_default", adapter=None,
        sql="SELECT 1", sql_translated=None, sql_resolved=None,
        sql_dialect=None, engine_dialect=None, upstream=[], eager=False,
        table=None, write_strategy=None, function=None, source_file=None,
        logical_plan=None, output_schema=None, column_lineage=[],
        optimizations=[], checks=[], fused_group_id=group_id, tags=[],
        description=None, fusion_strategy_override=None,
        materialization_strategy_override=None,
    )


def _make_compiled(
    groups: list[FusedGroup],
    joints: list[CompiledJoint],
    waves: list[ExecutionWave],
) -> CompiledAssembly:
    return CompiledAssembly(
        success=True,
        profile_name="default",
        catalogs=[CompiledCatalog(name="cat", type="fs")],
        engines=[CompiledEngine(name="duckdb", engine_type="duckdb", native_catalog_types=[])],
        adapters=[],
        joints=joints,
        fused_groups=groups,
        materializations=[],
        execution_order=[g.id for g in groups],
        errors=[],
        warnings=[],
        parallel_execution_plan=waves,
    )


def _make_result(joints: list[CompiledJoint]) -> ExecutionResult:
    return ExecutionResult(
        success=True,
        status="success",
        joint_results=[
            JointExecutionResult(
                name=j.name, success=True, rows_in=None, rows_out=10,
                timing=PhasedTiming(total_ms=5.0, engine_ms=4.0, materialize_ms=0.0, residual_ms=0.0, check_ms=0.0),
                fused_group_id=j.fused_group_id, materialized=False,
                materialization_trigger=None, materialization_stats=None,
                check_results=[], plugin_metrics=None, error=None,
            )
            for j in joints
        ],
        group_results=[],
        total_time_ms=10.0,
        total_materializations=0,
        total_failures=0,
        total_check_failures=0,
        total_check_warnings=0,
    )


class TestExecutionPlanWaveRendering:
    def test_single_wave(self) -> None:
        groups = [
            FusedGroup(id="group_a", joints=["j1"], engine="duckdb", engine_type="duckdb", adapters={}, fused_sql=None, fusion_strategy="cte"),
            FusedGroup(id="group_b", joints=["j2"], engine="duckdb", engine_type="duckdb", adapters={}, fused_sql=None, fusion_strategy="cte"),
        ]
        joints = [_make_joint("j1", "duckdb", "group_a"), _make_joint("j2", "duckdb", "group_b")]
        waves = [ExecutionWave(wave_number=1, groups=["group_a", "group_b"], engines={"duckdb": ["group_a", "group_b"]})]
        compiled = _make_compiled(groups, joints, waves)
        result = _make_result(joints)

        output = render_run_text(result, compiled, verbosity=0, color=False)

        assert "Execution Plan:" in output
        assert "Wave 1: [group_a (engine: duckdb), group_b (engine: duckdb)]" in output

    def test_multiple_waves(self) -> None:
        groups = [
            FusedGroup(id="group_a", joints=["j1"], engine="duckdb", engine_type="duckdb", adapters={}, fused_sql=None, fusion_strategy="cte"),
            FusedGroup(id="group_b", joints=["j2"], engine="databricks", engine_type="databricks", adapters={}, fused_sql=None, fusion_strategy="cte"),
            FusedGroup(id="group_c", joints=["j3"], engine="duckdb", engine_type="duckdb", adapters={}, fused_sql=None, fusion_strategy="cte"),
        ]
        joints = [
            _make_joint("j1", "duckdb", "group_a"),
            _make_joint("j2", "databricks", "group_b"),
            _make_joint("j3", "duckdb", "group_c"),
        ]
        waves = [
            ExecutionWave(wave_number=1, groups=["group_a", "group_b"], engines={"duckdb": ["group_a"], "databricks": ["group_b"]}),
            ExecutionWave(wave_number=2, groups=["group_c"], engines={"duckdb": ["group_c"]}),
        ]
        compiled = _make_compiled(groups, joints, waves)
        result = _make_result(joints)

        output = render_run_text(result, compiled, verbosity=0, color=False)

        assert "Wave 1: [group_a (engine: duckdb), group_b (engine: databricks)]" in output
        assert "Wave 2: [group_c (engine: duckdb)]" in output

    def test_no_execution_plan(self) -> None:
        groups = [
            FusedGroup(id="g1", joints=["j1"], engine="duckdb", engine_type="duckdb", adapters={}, fused_sql=None, fusion_strategy="cte"),
        ]
        joints = [_make_joint("j1", "duckdb", "g1")]
        compiled = _make_compiled(groups, joints, waves=[])
        result = _make_result(joints)

        output = render_run_text(result, compiled, verbosity=0, color=False)

        assert "Execution Plan:" not in output
