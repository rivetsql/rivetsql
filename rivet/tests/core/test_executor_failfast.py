"""Tests for fail-fast and partial failure (task 14.4).

Requirements covered:
  22.10 - fail_fast=True: stop immediately on error-severity assertion failure,
          prevent sink writes
  22.11 - fail_fast=False: continue independent branches, skip downstream of
          failed joints, accumulate all errors
  40.1  - fail_fast=True stops pipeline immediately, prevents downstream and sink writes
  40.2  - fail_fast=False: continue independent branches, produce ErrorMaterial,
          skip downstream, set "partial_failure" status
  40.3  - ExecutionResult accumulates all errors and skipped joints regardless of
          fail_fast setting
"""

from __future__ import annotations

from rivet_core.checks import CompiledCheck
from rivet_core.compiler import (
    CompiledAssembly,
    CompiledEngine,
    CompiledJoint,
    Materialization,
)
from rivet_core.errors import RivetError
from rivet_core.executor import (
    Executor,
    _make_error_material,
)
from rivet_core.optimizer import FusedGroup, FusionResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_compiled_joint(
    name: str,
    joint_type: str = "sql",
    upstream: list[str] | None = None,
    fused_group_id: str | None = None,
    checks: list[CompiledCheck] | None = None,
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
        checks=checks or [],
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
    )


def _make_compiled_assembly(
    joints: list[CompiledJoint],
    groups: list[FusedGroup],
    execution_order: list[str],
    materializations: list[Materialization] | None = None,
    success: bool = True,
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
        errors=[],
        warnings=[],
    )


# ---------------------------------------------------------------------------
# Tests: fail_fast=True stops immediately (Req 22.10, 40.1)
# ---------------------------------------------------------------------------


class TestFailFastTrue:
    def test_stops_on_error_assertion_failure(self) -> None:
        """fail_fast=True stops pipeline immediately on error-severity assertion."""
        failing_check = CompiledCheck(
            type="row_count", severity="error", config={"min": 100}, phase="assertion"
        )
        j1 = _make_compiled_joint("a", "sql", fused_group_id="g1", checks=[failing_check])
        j2 = _make_compiled_joint("b", "sql", upstream=["a"], fused_group_id="g2")

        g1 = _make_group("g1", ["a"])
        g2 = _make_group("g2", ["b"])

        assembly = _make_compiled_assembly(
            joints=[j1, j2],
            groups=[g1, g2],
            execution_order=["g1", "g2"],
        )

        executor = Executor()
        result = executor.run(assembly, fail_fast=True)

        assert result.success is False
        assert result.status == "failure"
        # Only group g1 should have been executed; g2 should be skipped
        executed_names = {jr.name for jr in result.joint_results}
        assert "a" in executed_names
        # b should not have been executed (pipeline stopped)
        assert "b" not in executed_names

    def test_prevents_sink_write_on_assertion_failure(self) -> None:
        """fail_fast=True prevents sink writes when assertion fails."""
        failing_check = CompiledCheck(
            type="row_count", severity="error", config={"min": 100}, phase="assertion"
        )
        j0 = _make_compiled_joint("src", "source", fused_group_id="g0")
        j1 = _make_compiled_joint(
            "sink1", "sink", upstream=["src"], fused_group_id="g1",
            checks=[failing_check],
        )

        g0 = _make_group("g0", ["src"])
        g1 = _make_group("g1", ["sink1"])

        assembly = _make_compiled_assembly(
            joints=[j0, j1],
            groups=[g0, g1],
            execution_order=["g0", "g1"],
        )

        executor = Executor()
        result = executor.run(assembly, fail_fast=True)

        assert result.success is False
        sink_result = next(jr for jr in result.joint_results if jr.name == "sink1")
        assert sink_result.success is False
        # No audit should have run (write was prevented)
        audit_results = [cr for cr in sink_result.check_results if cr.phase == "audit"]
        assert len(audit_results) == 0

    def test_stops_on_execution_exception(self) -> None:
        """fail_fast=True stops on execution exception."""
        # Use a PythonJoint with a bad function path to trigger an exception
        j1 = _make_compiled_joint(
            "bad", "python", fused_group_id="g1",
            function="nonexistent.module.func",
        )
        j2 = _make_compiled_joint("after", "sql", upstream=["bad"], fused_group_id="g2")

        g1 = _make_group("g1", ["bad"])
        g2 = _make_group("g2", ["after"])

        assembly = _make_compiled_assembly(
            joints=[j1, j2],
            groups=[g1, g2],
            execution_order=["g1", "g2"],
        )

        executor = Executor()
        result = executor.run(assembly, fail_fast=True)

        assert result.success is False
        assert result.status == "failure"
        # "after" should not have been executed
        executed_names = {jr.name for jr in result.joint_results}
        assert "bad" in executed_names
        assert "after" not in executed_names

    def test_warning_assertion_does_not_stop(self) -> None:
        """Warning-severity assertion does not trigger fail-fast stop."""
        warning_check = CompiledCheck(
            type="row_count", severity="warning", config={"min": 100}, phase="assertion"
        )
        j1 = _make_compiled_joint("a", "sql", fused_group_id="g1", checks=[warning_check])
        j2 = _make_compiled_joint("b", "sql", upstream=["a"], fused_group_id="g2")

        g1 = _make_group("g1", ["a"])
        g2 = _make_group("g2", ["b"])

        assembly = _make_compiled_assembly(
            joints=[j1, j2],
            groups=[g1, g2],
            execution_order=["g1", "g2"],
        )

        executor = Executor()
        result = executor.run(assembly, fail_fast=True)

        assert result.success is True
        assert result.status == "success"
        assert len(result.joint_results) == 2


# ---------------------------------------------------------------------------
# Tests: fail_fast=False continues independent branches (Req 22.11, 40.2)
# ---------------------------------------------------------------------------


class TestFailFastFalse:
    def test_continues_independent_branches(self) -> None:
        """fail_fast=False continues executing independent branches after failure.

        DAG: src -> a (fails), src -> b (independent, should succeed)
        """
        failing_check = CompiledCheck(
            type="row_count", severity="error", config={"min": 100}, phase="assertion"
        )
        j_src = _make_compiled_joint("src", "source", fused_group_id="g0")
        j_a = _make_compiled_joint(
            "a", "sql", upstream=["src"], fused_group_id="g1",
            checks=[failing_check],
        )
        j_b = _make_compiled_joint("b", "sql", upstream=["src"], fused_group_id="g2")

        g0 = _make_group("g0", ["src"])
        g1 = _make_group("g1", ["a"])
        g2 = _make_group("g2", ["b"])

        assembly = _make_compiled_assembly(
            joints=[j_src, j_a, j_b],
            groups=[g0, g1, g2],
            execution_order=["g0", "g1", "g2"],
        )

        executor = Executor()
        result = executor.run(assembly, fail_fast=False)

        assert result.success is False
        assert result.status == "partial_failure"

        a_result = next(jr for jr in result.joint_results if jr.name == "a")
        b_result = next(jr for jr in result.joint_results if jr.name == "b")
        assert a_result.success is False
        assert b_result.success is True

    def test_skips_downstream_of_failed_joint(self) -> None:
        """fail_fast=False skips downstream joints of failed joints.

        DAG: src -> a (fails) -> c (should be skipped)
        """
        failing_check = CompiledCheck(
            type="row_count", severity="error", config={"min": 100}, phase="assertion"
        )
        j_src = _make_compiled_joint("src", "source", fused_group_id="g0")
        j_a = _make_compiled_joint(
            "a", "sql", upstream=["src"], fused_group_id="g1",
            checks=[failing_check],
        )
        j_c = _make_compiled_joint("c", "sql", upstream=["a"], fused_group_id="g3")

        g0 = _make_group("g0", ["src"])
        g1 = _make_group("g1", ["a"])
        g3 = _make_group("g3", ["c"])

        assembly = _make_compiled_assembly(
            joints=[j_src, j_a, j_c],
            groups=[g0, g1, g3],
            execution_order=["g0", "g1", "g3"],
        )

        executor = Executor()
        result = executor.run(assembly, fail_fast=False)

        assert result.success is False
        c_result = next(jr for jr in result.joint_results if jr.name == "c")
        assert c_result.success is False
        assert c_result.error is not None
        assert "upstream" in c_result.error.message.lower() or "skipped" in c_result.error.message.lower()

    def test_partial_failure_status(self) -> None:
        """fail_fast=False sets status to 'partial_failure' when some succeed and some fail."""
        failing_check = CompiledCheck(
            type="row_count", severity="error", config={"min": 100}, phase="assertion"
        )
        j_src = _make_compiled_joint("src", "source", fused_group_id="g0")
        j_a = _make_compiled_joint(
            "a", "sql", upstream=["src"], fused_group_id="g1",
            checks=[failing_check],
        )

        g0 = _make_group("g0", ["src"])
        g1 = _make_group("g1", ["a"])

        assembly = _make_compiled_assembly(
            joints=[j_src, j_a],
            groups=[g0, g1],
            execution_order=["g0", "g1"],
        )

        executor = Executor()
        result = executor.run(assembly, fail_fast=False)

        assert result.success is False
        assert result.status == "partial_failure"

    def test_all_failures_gives_failure_status(self) -> None:
        """fail_fast=False with all joints failing gives 'failure' status."""
        failing_check = CompiledCheck(
            type="row_count", severity="error", config={"min": 100}, phase="assertion"
        )
        j1 = _make_compiled_joint(
            "a", "sql", fused_group_id="g1", checks=[failing_check]
        )

        g1 = _make_group("g1", ["a"])

        assembly = _make_compiled_assembly(
            joints=[j1],
            groups=[g1],
            execution_order=["g1"],
        )

        executor = Executor()
        result = executor.run(assembly, fail_fast=False)

        assert result.success is False
        assert result.status == "failure"

    def test_accumulates_all_errors(self) -> None:
        """fail_fast=False accumulates errors from all failed joints.

        DAG: a (fails), b (fails) — independent branches.
        """
        failing_check = CompiledCheck(
            type="row_count", severity="error", config={"min": 100}, phase="assertion"
        )
        j_a = _make_compiled_joint(
            "a", "sql", fused_group_id="g1", checks=[failing_check]
        )
        j_b = _make_compiled_joint(
            "b", "sql", fused_group_id="g2", checks=[failing_check]
        )

        g1 = _make_group("g1", ["a"])
        g2 = _make_group("g2", ["b"])

        assembly = _make_compiled_assembly(
            joints=[j_a, j_b],
            groups=[g1, g2],
            execution_order=["g1", "g2"],
        )

        executor = Executor()
        result = executor.run(assembly, fail_fast=False)

        assert result.success is False
        assert result.total_failures == 2
        assert result.total_check_failures == 2

    def test_exception_continues_independent_branches(self) -> None:
        """fail_fast=False continues after execution exception on independent branch.

        DAG: bad (exception) and good (independent, should succeed)
        """
        j_bad = _make_compiled_joint(
            "bad", "python", fused_group_id="g1",
            function="nonexistent.module.func",
        )
        j_good = _make_compiled_joint("good", "sql", fused_group_id="g2")

        g1 = _make_group("g1", ["bad"])
        g2 = _make_group("g2", ["good"])

        assembly = _make_compiled_assembly(
            joints=[j_bad, j_good],
            groups=[g1, g2],
            execution_order=["g1", "g2"],
        )

        executor = Executor()
        result = executor.run(assembly, fail_fast=False)

        assert result.success is False
        assert result.status == "partial_failure"

        bad_result = next(jr for jr in result.joint_results if jr.name == "bad")
        good_result = next(jr for jr in result.joint_results if jr.name == "good")
        assert bad_result.success is False
        assert bad_result.error is not None
        assert good_result.success is True

    def test_skips_downstream_chain(self) -> None:
        """fail_fast=False skips entire downstream chain of failed joint.

        DAG: a (fails) -> b -> c — both b and c should be skipped.
        """
        failing_check = CompiledCheck(
            type="row_count", severity="error", config={"min": 100}, phase="assertion"
        )
        j_a = _make_compiled_joint(
            "a", "sql", fused_group_id="g1", checks=[failing_check]
        )
        j_b = _make_compiled_joint("b", "sql", upstream=["a"], fused_group_id="g2")
        j_c = _make_compiled_joint("c", "sql", upstream=["b"], fused_group_id="g3")

        g1 = _make_group("g1", ["a"])
        g2 = _make_group("g2", ["b"])
        g3 = _make_group("g3", ["c"])

        assembly = _make_compiled_assembly(
            joints=[j_a, j_b, j_c],
            groups=[g1, g2, g3],
            execution_order=["g1", "g2", "g3"],
        )

        executor = Executor()
        result = executor.run(assembly, fail_fast=False)

        assert result.success is False
        b_result = next(jr for jr in result.joint_results if jr.name == "b")
        c_result = next(jr for jr in result.joint_results if jr.name == "c")
        assert b_result.success is False
        assert c_result.success is False


# ---------------------------------------------------------------------------
# Tests: ErrorMaterial (Req 40.2)
# ---------------------------------------------------------------------------


class TestErrorMaterial:
    def test_error_material_has_error_state(self) -> None:
        """ErrorMaterial has state='error'."""
        error = RivetError(code="RVT-501", message="test", context={})
        mat = _make_error_material("failed_joint", error)
        assert mat.state == "error"
        assert mat.name == "failed_joint"


# ---------------------------------------------------------------------------
# Tests: ExecutionResult status field (Req 40.2, 40.3)
# ---------------------------------------------------------------------------


class TestExecutionResultStatus:
    def test_success_status(self) -> None:
        """Successful pipeline has status='success'."""
        j1 = _make_compiled_joint("a", "source", fused_group_id="g1")
        g1 = _make_group("g1", ["a"])

        assembly = _make_compiled_assembly(
            joints=[j1], groups=[g1], execution_order=["g1"]
        )

        executor = Executor()
        result = executor.run(assembly)

        assert result.success is True
        assert result.status == "success"

    def test_fail_fast_failure_status(self) -> None:
        """fail_fast=True failure has status='failure'."""
        failing_check = CompiledCheck(
            type="row_count", severity="error", config={"min": 100}, phase="assertion"
        )
        j1 = _make_compiled_joint("a", "sql", fused_group_id="g1", checks=[failing_check])
        g1 = _make_group("g1", ["a"])

        assembly = _make_compiled_assembly(
            joints=[j1], groups=[g1], execution_order=["g1"]
        )

        executor = Executor()
        result = executor.run(assembly, fail_fast=True)

        assert result.success is False
        assert result.status == "failure"

    def test_result_accumulates_errors_regardless_of_fail_fast(self) -> None:
        """ExecutionResult accumulates errors regardless of fail_fast setting (Req 40.3)."""
        failing_check = CompiledCheck(
            type="row_count", severity="error", config={"min": 100}, phase="assertion"
        )
        j1 = _make_compiled_joint("a", "sql", fused_group_id="g1", checks=[failing_check])
        g1 = _make_group("g1", ["a"])

        assembly = _make_compiled_assembly(
            joints=[j1], groups=[g1], execution_order=["g1"]
        )

        # fail_fast=True
        executor = Executor()
        result_ff = executor.run(assembly, fail_fast=True)
        assert result_ff.total_failures >= 1
        assert result_ff.total_check_failures >= 1

        # fail_fast=False
        result_nff = executor.run(assembly, fail_fast=False)
        assert result_nff.total_failures >= 1
        assert result_nff.total_check_failures >= 1
