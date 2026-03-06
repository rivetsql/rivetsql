"""Tests for executor core loop (task 14.1)."""

from __future__ import annotations

import pyarrow
import pytest

from rivet_core.compiler import (
    CompiledAssembly,
    CompiledEngine,
    CompiledJoint,
    Materialization,
)
from rivet_core.errors import CompilationError, RivetError
from rivet_core.executor import (
    ExecutionResult,
    Executor,
    FusedGroupExecutionResult,
    JointExecutionResult,
    _apply_residuals,
)
from rivet_core.optimizer import (
    Cast,
    FusedGroup,
    FusionResult,
    ResidualPlan,
)
from rivet_core.sql_parser import Predicate

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_compiled_joint(
    name: str,
    joint_type: str = "sql",
    upstream: list[str] | None = None,
    fused_group_id: str | None = None,
    **kwargs: object,
) -> CompiledJoint:
    defaults = dict(
        catalog=None,
        catalog_type=None,
        engine="arrow",
        engine_resolution="project_default",
        adapter=None,
        sql=None,
        sql_translated=None,
        sql_resolved=None,
        sql_dialect=None,
        engine_dialect=None,
        upstream=upstream or [],
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
        fused_group_id=fused_group_id,
        tags=[],
        description=None,
        fusion_strategy_override=None,
        materialization_strategy_override=None,
    )
    defaults.update(kwargs)
    return CompiledJoint(name=name, type=joint_type, **defaults)  # type: ignore[arg-type]


def _make_group(
    group_id: str,
    joints: list[str],
    fused_sql: str | None = None,
    fusion_strategy: str = "cte",
    fusion_result: FusionResult | None = None,
    residual: ResidualPlan | None = None,
    entry_joints: list[str] | None = None,
    exit_joints: list[str] | None = None,
) -> FusedGroup:
    return FusedGroup(
        id=group_id,
        joints=joints,
        engine="arrow",
        engine_type="arrow",
        adapters={j: None for j in joints},
        fused_sql=fused_sql,
        fusion_strategy=fusion_strategy,
        fusion_result=fusion_result,
        entry_joints=entry_joints or [joints[0]],
        exit_joints=exit_joints or [joints[-1]],
        residual=residual,
    )


def _make_compiled_assembly(
    joints: list[CompiledJoint],
    groups: list[FusedGroup],
    execution_order: list[str],
    materializations: list[Materialization] | None = None,
    success: bool = True,
    errors: list[RivetError] | None = None,
) -> CompiledAssembly:
    return CompiledAssembly(
        success=success,
        profile_name="default",
        catalogs=[],
        engines=[CompiledEngine(name="arrow", engine_type="arrow", native_catalog_types=["arrow"])],
        adapters=[],
        joints=joints,
        fused_groups=groups,
        materializations=materializations or [],
        execution_order=execution_order,
        errors=errors or [],
        warnings=[],
    )


# ---------------------------------------------------------------------------
# Tests: Executor refuses on failed compilation (Req 22.2)
# ---------------------------------------------------------------------------


class TestExecutorRefusesFailedCompilation:
    def test_raises_compilation_error_when_success_false(self) -> None:
        error = RivetError(code="RVT-401", message="No engine", context={})
        assembly = _make_compiled_assembly(
            joints=[], groups=[], execution_order=[], success=False, errors=[error]
        )
        executor = Executor()
        with pytest.raises(CompilationError) as exc_info:
            executor.run(assembly)
        assert len(exc_info.value.errors) == 1
        assert exc_info.value.errors[0].code == "RVT-401"


# ---------------------------------------------------------------------------
# Tests: Execution follows execution_order (Req 22.1, 22.3)
# ---------------------------------------------------------------------------


class TestExecutionOrder:
    def test_follows_execution_order_exactly(self) -> None:
        """Executor processes groups in the order specified by execution_order."""
        j1 = _make_compiled_joint("src", "source", fused_group_id="g1")
        j2 = _make_compiled_joint("transform", "sql", upstream=["src"], fused_group_id="g2")

        g1 = _make_group("g1", ["src"])
        g2 = _make_group("g2", ["transform"])

        assembly = _make_compiled_assembly(
            joints=[j1, j2],
            groups=[g1, g2],
            execution_order=["g1", "g2"],
        )

        executor = Executor()
        result = executor.run(assembly)

        assert result.success is True
        assert len(result.joint_results) == 2
        assert result.joint_results[0].name == "src"
        assert result.joint_results[1].name == "transform"

    def test_empty_execution_order(self) -> None:
        assembly = _make_compiled_assembly(joints=[], groups=[], execution_order=[])
        executor = Executor()
        result = executor.run(assembly)
        assert result.success is True
        assert result.total_failures == 0
        assert result.joint_results == []


# ---------------------------------------------------------------------------
# Tests: CTE strategy dispatch (Req 22.4)
# ---------------------------------------------------------------------------


class TestCTEStrategy:
    def test_cte_uses_resolved_fused_sql_when_available(self) -> None:
        """CTE strategy prefers resolved_fused_sql over fused_sql."""
        j1 = _make_compiled_joint("a", "source", fused_group_id="g1")
        fr = FusionResult(
            fused_sql="SELECT * FROM raw",
            statements=[],
            final_select="SELECT * FROM raw",
            resolved_fused_sql="SELECT * FROM resolved_raw",
        )
        g1 = _make_group("g1", ["a"], fused_sql="SELECT * FROM raw", fusion_result=fr)

        assembly = _make_compiled_assembly(
            joints=[j1], groups=[g1], execution_order=["g1"]
        )
        executor = Executor()
        result = executor.run(assembly)
        assert result.success is True

    def test_cte_falls_back_to_fused_sql(self) -> None:
        """CTE strategy uses fused_sql when resolved_fused_sql is None."""
        j1 = _make_compiled_joint("a", "source", fused_group_id="g1")
        fr = FusionResult(
            fused_sql="SELECT * FROM raw",
            statements=[],
            final_select="SELECT * FROM raw",
        )
        g1 = _make_group("g1", ["a"], fused_sql="SELECT * FROM raw", fusion_result=fr)

        assembly = _make_compiled_assembly(
            joints=[j1], groups=[g1], execution_order=["g1"]
        )
        executor = Executor()
        result = executor.run(assembly)
        assert result.success is True


# ---------------------------------------------------------------------------
# Tests: TempView strategy dispatch (Req 22.5)
# ---------------------------------------------------------------------------


class TestTempViewStrategy:
    def test_temp_view_creates_and_drops_views(self) -> None:
        """TempView strategy executes CREATE TEMP VIEW, final SELECT, then drops."""
        j1 = _make_compiled_joint("a", "sql", fused_group_id="g1")
        j2 = _make_compiled_joint("b", "sql", upstream=["a"], fused_group_id="g1")

        fr = FusionResult(
            fused_sql="CREATE TEMPORARY VIEW a AS (SELECT 1 as x);\nSELECT * FROM a",
            statements=["CREATE TEMPORARY VIEW a AS (SELECT 1 as x)"],
            final_select="SELECT * FROM a",
        )
        g1 = _make_group(
            "g1",
            ["a", "b"],
            fusion_strategy="temp_view",
            fusion_result=fr,
            entry_joints=["a"],
            exit_joints=["b"],
        )

        assembly = _make_compiled_assembly(
            joints=[j1, j2], groups=[g1], execution_order=["g1"]
        )
        executor = Executor()
        result = executor.run(assembly)
        assert result.success is True

    def test_temp_view_single_joint_no_views(self) -> None:
        """Single-joint temp_view group needs no temp views."""
        j1 = _make_compiled_joint("a", "source", fused_group_id="g1")
        fr = FusionResult(
            fused_sql="SELECT 1 as x",
            statements=[],
            final_select="SELECT 1 as x",
        )
        g1 = _make_group("g1", ["a"], fusion_strategy="temp_view", fusion_result=fr)

        assembly = _make_compiled_assembly(
            joints=[j1], groups=[g1], execution_order=["g1"]
        )
        executor = Executor()
        result = executor.run(assembly)
        assert result.success is True


# ---------------------------------------------------------------------------
# Tests: Residual operations (Req 22.6, 39.1, 39.2, 39.3, 39.4)
# ---------------------------------------------------------------------------


class TestResidualOperations:
    def test_residual_limit_slices_table(self) -> None:
        """Residual limit applies table.slice(0, n)."""
        table = pyarrow.table({"x": [1, 2, 3, 4, 5]})
        residual = ResidualPlan(predicates=[], limit=3, casts=[])
        result = _apply_residuals(table, residual)
        assert result.num_rows == 3
        assert result.column("x").to_pylist() == [1, 2, 3]

    def test_residual_predicate_filters_rows(self) -> None:
        """Residual predicate applies pyarrow.compute.filter()."""
        table = pyarrow.table({"x": [1, 2, 3, 4, 5]})
        pred = Predicate(expression="x > 3", columns=["x"], location="where")
        residual = ResidualPlan(predicates=[pred], limit=None, casts=[])
        result = _apply_residuals(table, residual)
        assert result.column("x").to_pylist() == [4, 5]

    def test_residual_cast_changes_types(self) -> None:
        """Residual cast applies table.cast(target_schema)."""
        table = pyarrow.table({"x": pyarrow.array([1, 2, 3], type=pyarrow.int32())})
        cast = Cast(column="x", from_type="int32", to_type="int64")
        residual = ResidualPlan(predicates=[], limit=None, casts=[cast])
        result = _apply_residuals(table, residual)
        assert result.schema.field("x").type == pyarrow.int64()

    def test_residual_combined_operations(self) -> None:
        """Residual operations applied in order: predicates, limit, casts."""
        table = pyarrow.table({"x": pyarrow.array([1, 2, 3, 4, 5], type=pyarrow.int32())})
        pred = Predicate(expression="x > 1", columns=["x"], location="where")
        cast = Cast(column="x", from_type="int32", to_type="int64")
        residual = ResidualPlan(predicates=[pred], limit=2, casts=[cast])
        result = _apply_residuals(table, residual)
        # Filter: [2, 3, 4, 5], Limit: [2, 3], Cast: int64
        assert result.num_rows == 2
        assert result.column("x").to_pylist() == [2, 3]
        assert result.schema.field("x").type == pyarrow.int64()

    def test_residual_equality_predicate(self) -> None:
        table = pyarrow.table({"name": ["alice", "bob", "carol"]})
        pred = Predicate(expression="name = 'bob'", columns=["name"], location="where")
        residual = ResidualPlan(predicates=[pred], limit=None, casts=[])
        result = _apply_residuals(table, residual)
        assert result.column("name").to_pylist() == ["bob"]

    def test_residual_is_not_null_predicate(self) -> None:
        table = pyarrow.table({"x": [1, None, 3]})
        pred = Predicate(expression="x IS NOT NULL", columns=["x"], location="where")
        residual = ResidualPlan(predicates=[pred], limit=None, casts=[])
        result = _apply_residuals(table, residual)
        assert result.column("x").to_pylist() == [1, 3]

    def test_residual_is_null_predicate(self) -> None:
        table = pyarrow.table({"x": [1, None, 3]})
        pred = Predicate(expression="x IS NULL", columns=["x"], location="where")
        residual = ResidualPlan(predicates=[pred], limit=None, casts=[])
        result = _apply_residuals(table, residual)
        assert result.column("x").to_pylist() == [None]

    def test_residual_no_ops(self) -> None:
        """Empty residual plan returns table unchanged."""
        table = pyarrow.table({"x": [1, 2, 3]})
        residual = ResidualPlan(predicates=[], limit=None, casts=[])
        result = _apply_residuals(table, residual)
        assert result.equals(table)

    def test_residual_unknown_column_predicate_skipped(self) -> None:
        """Predicate referencing unknown column is skipped gracefully."""
        table = pyarrow.table({"x": [1, 2, 3]})
        pred = Predicate(expression="y > 1", columns=["y"], location="where")
        residual = ResidualPlan(predicates=[pred], limit=None, casts=[])
        result = _apply_residuals(table, residual)
        # Unknown column — filter skipped, table unchanged
        assert result.num_rows == 3


# ---------------------------------------------------------------------------
# Tests: Materialization where specified (Req 22.8)
# ---------------------------------------------------------------------------


class TestMaterialization:
    def test_materializes_where_compiled_assembly_specifies(self) -> None:
        """Executor materializes at edges specified in CompiledAssembly.materializations."""
        j1 = _make_compiled_joint("src", "source", fused_group_id="g1")
        j2 = _make_compiled_joint("transform", "sql", upstream=["src"], fused_group_id="g2")

        g1 = _make_group("g1", ["src"])
        g2 = _make_group("g2", ["transform"])

        mat = Materialization(
            from_joint="src",
            to_joint="transform",
            trigger="engine_instance_change",
            detail="Engine changes",
            strategy="arrow",
        )

        assembly = _make_compiled_assembly(
            joints=[j1, j2],
            groups=[g1, g2],
            execution_order=["g1", "g2"],
            materializations=[mat],
        )

        executor = Executor()
        result = executor.run(assembly)
        assert result.success is True
        assert result.total_materializations >= 1


# ---------------------------------------------------------------------------
# Tests: ExecutionResult structure (Req 23.1, 23.2, 23.3)
# ---------------------------------------------------------------------------


class TestExecutionResultStructure:
    def test_execution_result_fields(self) -> None:
        j1 = _make_compiled_joint("src", "source", fused_group_id="g1")
        g1 = _make_group("g1", ["src"])

        assembly = _make_compiled_assembly(
            joints=[j1], groups=[g1], execution_order=["g1"]
        )
        executor = Executor()
        result = executor.run(assembly)

        assert isinstance(result, ExecutionResult)
        assert isinstance(result.success, bool)
        assert isinstance(result.joint_results, list)
        assert isinstance(result.group_results, list)
        assert isinstance(result.total_time_ms, float)
        assert result.total_time_ms >= 0
        assert isinstance(result.total_materializations, int)
        assert isinstance(result.total_failures, int)
        assert isinstance(result.total_check_failures, int)
        assert isinstance(result.total_check_warnings, int)

    def test_joint_execution_result_fields(self) -> None:
        j1 = _make_compiled_joint("src", "source", fused_group_id="g1")
        g1 = _make_group("g1", ["src"])

        assembly = _make_compiled_assembly(
            joints=[j1], groups=[g1], execution_order=["g1"]
        )
        executor = Executor()
        result = executor.run(assembly)

        jr = result.joint_results[0]
        assert isinstance(jr, JointExecutionResult)
        assert jr.name == "src"
        assert jr.success is True
        assert jr.fused_group_id == "g1"
        assert jr.timing is not None
        assert jr.timing.total_ms >= 0

    def test_fused_group_execution_result_fields(self) -> None:
        j1 = _make_compiled_joint("src", "source", fused_group_id="g1")
        g1 = _make_group("g1", ["src"])

        assembly = _make_compiled_assembly(
            joints=[j1], groups=[g1], execution_order=["g1"]
        )
        executor = Executor()
        result = executor.run(assembly)

        gr = result.group_results[0]
        assert isinstance(gr, FusedGroupExecutionResult)
        assert gr.group_id == "g1"
        assert gr.joints == ["src"]
        assert gr.success is True
        assert gr.timing.total_ms >= 0


# ---------------------------------------------------------------------------
# Tests: Multi-joint fused group
# ---------------------------------------------------------------------------


class TestMultiJointGroup:
    def test_multi_joint_group_produces_results_for_all_joints(self) -> None:
        j1 = _make_compiled_joint("a", "sql", fused_group_id="g1")
        j2 = _make_compiled_joint("b", "sql", upstream=["a"], fused_group_id="g1")

        fr = FusionResult(
            fused_sql="WITH a AS (SELECT 1 as x)\nSELECT * FROM a",
            statements=["a AS (\n    SELECT 1 as x\n)"],
            final_select="SELECT * FROM a",
        )
        g1 = _make_group(
            "g1", ["a", "b"], fused_sql=fr.fused_sql, fusion_result=fr,
            entry_joints=["a"], exit_joints=["b"],
        )

        assembly = _make_compiled_assembly(
            joints=[j1, j2], groups=[g1], execution_order=["g1"]
        )
        executor = Executor()
        result = executor.run(assembly)

        assert result.success is True
        assert len(result.joint_results) == 2
        assert result.joint_results[0].name == "a"
        assert result.joint_results[1].name == "b"
        assert len(result.group_results) == 1
        assert result.group_results[0].group_id == "g1"
