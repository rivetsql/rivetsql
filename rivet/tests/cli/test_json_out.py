"""Tests for JSON renderer (rendering/json_out.py).

Validates Property 4: JSON compile output is valid JSON containing required keys.
"""

from __future__ import annotations

import json

from rivet_cli.rendering.json_out import render_compile_json, render_run_json
from rivet_core.checks import CompiledCheck
from rivet_core.compiler import (
    CompiledAssembly,
    CompiledCatalog,
    CompiledEngine,
    CompiledJoint,
    Materialization,
    OptimizationResult,
)
from rivet_core.executor import (
    ExecutionResult,
    FusedGroupExecutionResult,
    JointExecutionResult,
)
from rivet_core.lineage import ColumnLineage, ColumnOrigin
from rivet_core.metrics import PhasedTiming
from rivet_core.models import Column, Schema
from rivet_core.optimizer import FusedGroup

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_compiled(
    joints: list[CompiledJoint] | None = None,
    fused_groups: list[FusedGroup] | None = None,
) -> CompiledAssembly:
    if joints is None:
        joints = [
            CompiledJoint(
                name="src",
                type="source",
                catalog="my_cat",
                catalog_type="filesystem",
                engine="duckdb_1",
                engine_resolution="project_default",
                adapter=None,
                sql=None,
                sql_translated=None,
                sql_resolved=None,
                sql_dialect=None,
                engine_dialect=None,
                upstream=[],
                eager=False,
                table="raw_data",
                write_strategy=None,
                function=None,
                source_file=None,
                logical_plan=None,
                output_schema=Schema(columns=[Column(name="id", type="int64", nullable=False)]),
                column_lineage=[],
                optimizations=[],
                checks=[],
                fused_group_id="g1",
                tags=["etl"],
                description="source joint",
                fusion_strategy_override=None,
                materialization_strategy_override=None,
            ),
            CompiledJoint(
                name="transform",
                type="sql",
                catalog=None,
                catalog_type=None,
                engine="duckdb_1",
                engine_resolution="project_default",
                adapter=None,
                sql="SELECT id FROM src",
                sql_translated=None,
                sql_resolved=None,
                sql_dialect=None,
                engine_dialect=None,
                upstream=["src"],
                eager=False,
                table=None,
                write_strategy=None,
                function=None,
                source_file=None,
                logical_plan=None,
                output_schema=None,
                column_lineage=[
                    ColumnLineage(
                        output_column="id",
                        transform="direct",
                        origins=[ColumnOrigin(joint="src", column="id")],
                        expression=None,
                    )
                ],
                optimizations=[
                    OptimizationResult(
                        rule="predicate_pushdown",
                        status="not_applicable",
                        detail="no predicates",
                    )
                ],
                checks=[
                    CompiledCheck(type="not_null", severity="error", config={"column": "id"}, phase="assertion"),
                ],
                fused_group_id="g1",
                tags=[],
                description=None,
                fusion_strategy_override=None,
                materialization_strategy_override=None,
            ),
        ]
    if fused_groups is None:
        fused_groups = [
            FusedGroup(
                id="g1",
                joints=["src", "transform"],
                engine="duckdb_1",
                engine_type="duckdb",
                adapters={"src": None, "transform": None},
                fused_sql="WITH src AS (...) SELECT id FROM src",
                fusion_strategy="cte",
            )
        ]
    return CompiledAssembly(
        success=True,
        profile_name="default",
        catalogs=[CompiledCatalog(name="my_cat", type="filesystem")],
        engines=[CompiledEngine(name="duckdb_1", engine_type="duckdb", native_catalog_types=["filesystem"])],
        adapters=[],
        joints=joints,
        fused_groups=fused_groups,
        materializations=[],
        execution_order=["g1"],
        errors=[],
        warnings=[],
    )


def _make_execution_result() -> ExecutionResult:
    return ExecutionResult(
        success=True,
        status="success",
        joint_results=[
            JointExecutionResult(
                name="src",
                success=True,
                rows_in=None,
                rows_out=100,
                timing=PhasedTiming(total_ms=10.0, engine_ms=8.0, materialize_ms=1.0, residual_ms=0.5, check_ms=0.5),
                fused_group_id="g1",
                materialized=False,
                materialization_trigger=None,
                materialization_stats=None,
                check_results=[],
                plugin_metrics=None,
                error=None,
            ),
        ],
        group_results=[
            FusedGroupExecutionResult(
                group_id="g1",
                joints=["src", "transform"],
                success=True,
                rows_in=0,
                rows_out=100,
                timing=PhasedTiming(total_ms=15.0, engine_ms=12.0, materialize_ms=1.0, residual_ms=1.0, check_ms=1.0),
                materialization_stats=None,
                plugin_metrics=None,
                error=None,
            ),
        ],
        total_time_ms=15.0,
        total_materializations=0,
        total_failures=0,
        total_check_failures=0,
        total_check_warnings=0,
    )


# ---------------------------------------------------------------------------
# render_compile_json
# ---------------------------------------------------------------------------


class TestRenderCompileJson:
    def test_produces_valid_json(self):
        compiled = _make_compiled()
        output = render_compile_json(compiled)
        data = json.loads(output)
        assert isinstance(data, dict)

    def test_contains_required_keys(self):
        """Property 4: JSON output contains fused_groups, joints, execution_order, catalogs, engines."""
        compiled = _make_compiled()
        data = json.loads(render_compile_json(compiled))
        for key in ("fused_groups", "joints", "execution_order", "catalogs", "engines"):
            assert key in data, f"Missing required key: {key}"

    def test_contains_column_lineage(self):
        compiled = _make_compiled()
        data = json.loads(render_compile_json(compiled))
        transform = [j for j in data["joints"] if j["name"] == "transform"][0]
        assert len(transform["column_lineage"]) == 1
        assert transform["column_lineage"][0]["output_column"] == "id"

    def test_contains_output_schema(self):
        compiled = _make_compiled()
        data = json.loads(render_compile_json(compiled))
        src = [j for j in data["joints"] if j["name"] == "src"][0]
        assert src["output_schema"] is not None
        assert src["output_schema"]["columns"][0]["name"] == "id"

    def test_contains_checks_with_phase(self):
        compiled = _make_compiled()
        data = json.loads(render_compile_json(compiled))
        transform = [j for j in data["joints"] if j["name"] == "transform"][0]
        assert len(transform["checks"]) == 1
        assert transform["checks"][0]["phase"] == "assertion"

    def test_fused_groups_structure(self):
        compiled = _make_compiled()
        data = json.loads(render_compile_json(compiled))
        assert len(data["fused_groups"]) == 1
        group = data["fused_groups"][0]
        assert group["id"] == "g1"
        assert group["joints"] == ["src", "transform"]

    def test_empty_assembly(self):
        compiled = CompiledAssembly(
            success=True,
            profile_name="default",
            catalogs=[],
            engines=[],
            adapters=[],
            joints=[],
            fused_groups=[],
            materializations=[],
            execution_order=[],
            errors=[],
            warnings=[],
        )
        data = json.loads(render_compile_json(compiled))
        assert data["joints"] == []
        assert data["fused_groups"] == []

    def test_materializations_included(self):
        compiled = _make_compiled()
        compiled = CompiledAssembly(
            success=compiled.success,
            profile_name=compiled.profile_name,
            catalogs=compiled.catalogs,
            engines=compiled.engines,
            adapters=compiled.adapters,
            joints=compiled.joints,
            fused_groups=compiled.fused_groups,
            materializations=[
                Materialization(
                    from_joint="src",
                    to_joint="transform",
                    trigger="engine_instance_change",
                    detail="Engine changes",
                    strategy="arrow",
                )
            ],
            execution_order=compiled.execution_order,
            errors=compiled.errors,
            warnings=compiled.warnings,
        )
        data = json.loads(render_compile_json(compiled))
        assert len(data["materializations"]) == 1
        assert data["materializations"][0]["trigger"] == "engine_instance_change"


# ---------------------------------------------------------------------------
# render_run_json
# ---------------------------------------------------------------------------


class TestRenderRunJson:
    def test_produces_valid_json(self):
        result = _make_execution_result()
        compiled = _make_compiled()
        output = render_run_json(result, compiled)
        data = json.loads(output)
        assert isinstance(data, dict)

    def test_contains_execution_and_compilation(self):
        result = _make_execution_result()
        compiled = _make_compiled()
        data = json.loads(render_run_json(result, compiled))
        assert "execution" in data
        assert "compilation" in data

    def test_execution_has_expected_fields(self):
        result = _make_execution_result()
        compiled = _make_compiled()
        data = json.loads(render_run_json(result, compiled))
        ex = data["execution"]
        assert ex["success"] is True
        assert ex["status"] == "success"
        assert ex["total_time_ms"] == 15.0
        assert len(ex["joint_results"]) == 1

    def test_joint_result_fields(self):
        result = _make_execution_result()
        compiled = _make_compiled()
        data = json.loads(render_run_json(result, compiled))
        jr = data["execution"]["joint_results"][0]
        assert jr["name"] == "src"
        assert jr["rows_out"] == 100
        assert jr["timing"]["total_ms"] == 10.0

    def test_compilation_section_has_required_keys(self):
        result = _make_execution_result()
        compiled = _make_compiled()
        data = json.loads(render_run_json(result, compiled))
        for key in ("fused_groups", "joints", "execution_order", "catalogs", "engines"):
            assert key in data["compilation"]
