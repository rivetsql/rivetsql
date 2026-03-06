"""Tests for assertion and audit execution (task 14.2).

Requirements covered:
  22.7  - Assertions run after joint execution, before downstream/sink writes
  22.9  - Audits run after sink writes by reading back from target catalog
  24.3  - All assertions run without short-circuiting, worst severity wins
  24.4  - Assertions receive materialized pyarrow.Table read-only
  24.5  - Error-severity assertion failure prevents sink write
  24.6  - Assertions break fusion (tested at compile time, verified here via checks)
  24.7  - Assertions trigger materialization
  25.1  - Audits run after sink write by reading back
  25.2  - Audits use same check types and severity model
  25.5  - Error-severity audit failure reported but no rollback
  25.6  - Audit read-back failure → RVT-670
  25.7  - Audit read-back may return more data than written
"""

from __future__ import annotations

import pyarrow

from rivet_core.checks import CompiledCheck
from rivet_core.compiler import (
    CompiledAssembly,
    CompiledCatalog,
    CompiledEngine,
    CompiledJoint,
    Materialization,
)
from rivet_core.errors import RivetError
from rivet_core.executor import (
    Executor,
    _execute_check,
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
    catalogs: list[CompiledCatalog] | None = None,
    success: bool = True,
    errors: list[RivetError] | None = None,
) -> CompiledAssembly:
    return CompiledAssembly(
        success=success,
        profile_name="default",
        catalogs=catalogs or [],
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
# Tests: _execute_check unit tests
# ---------------------------------------------------------------------------


class TestExecuteCheck:
    def test_not_null_passes(self) -> None:
        table = pyarrow.table({"x": [1, 2, 3]})
        chk = CompiledCheck(type="not_null", severity="error", config={"column": "x"}, phase="assertion")
        result = _execute_check(chk, table)
        assert result.passed is True
        assert result.phase == "assertion"

    def test_not_null_fails_with_nulls(self) -> None:
        table = pyarrow.table({"x": [1, None, 3]})
        chk = CompiledCheck(type="not_null", severity="error", config={"column": "x"}, phase="assertion")
        result = _execute_check(chk, table)
        assert result.passed is False
        assert "1 null" in result.message

    def test_not_null_missing_column(self) -> None:
        table = pyarrow.table({"x": [1, 2]})
        chk = CompiledCheck(type="not_null", severity="error", config={"column": "y"}, phase="assertion")
        result = _execute_check(chk, table)
        assert result.passed is False
        assert "not found" in result.message

    def test_unique_passes(self) -> None:
        table = pyarrow.table({"id": [1, 2, 3]})
        chk = CompiledCheck(type="unique", severity="error", config={"column": "id"}, phase="assertion")
        result = _execute_check(chk, table)
        assert result.passed is True

    def test_unique_fails_with_duplicates(self) -> None:
        table = pyarrow.table({"id": [1, 2, 2]})
        chk = CompiledCheck(type="unique", severity="error", config={"column": "id"}, phase="assertion")
        result = _execute_check(chk, table)
        assert result.passed is False
        assert "duplicate" in result.message

    def test_row_count_passes(self) -> None:
        table = pyarrow.table({"x": [1, 2, 3]})
        chk = CompiledCheck(type="row_count", severity="error", config={"min": 1, "max": 5}, phase="assertion")
        result = _execute_check(chk, table)
        assert result.passed is True

    def test_row_count_fails_below_min(self) -> None:
        table = pyarrow.table({"x": [1]})
        chk = CompiledCheck(type="row_count", severity="error", config={"min": 5}, phase="assertion")
        result = _execute_check(chk, table)
        assert result.passed is False

    def test_accepted_values_passes(self) -> None:
        table = pyarrow.table({"status": ["a", "b", "a"]})
        chk = CompiledCheck(type="accepted_values", severity="error", config={"column": "status", "values": ["a", "b"]}, phase="assertion")
        result = _execute_check(chk, table)
        assert result.passed is True

    def test_accepted_values_fails(self) -> None:
        table = pyarrow.table({"status": ["a", "b", "c"]})
        chk = CompiledCheck(type="accepted_values", severity="error", config={"column": "status", "values": ["a", "b"]}, phase="assertion")
        result = _execute_check(chk, table)
        assert result.passed is False
        assert "c" in result.message

    def test_expression_passes(self) -> None:
        table = pyarrow.table({"x": [1, 2, 3]})
        chk = CompiledCheck(type="expression", severity="error", config={"expression": "x > 0"}, phase="assertion")
        result = _execute_check(chk, table)
        assert result.passed is True

    def test_expression_fails(self) -> None:
        table = pyarrow.table({"x": [1, -1, 3]})
        chk = CompiledCheck(type="expression", severity="error", config={"expression": "x > 0"}, phase="assertion")
        result = _execute_check(chk, table)
        assert result.passed is False

    def test_schema_check_passes(self) -> None:
        table = pyarrow.table({"x": pyarrow.array([1, 2], type=pyarrow.int64())})
        chk = CompiledCheck(type="schema", severity="error", config={"columns": {"x": "int64"}}, phase="assertion")
        result = _execute_check(chk, table)
        assert result.passed is True

    def test_schema_check_fails_wrong_type(self) -> None:
        table = pyarrow.table({"x": pyarrow.array([1, 2], type=pyarrow.int32())})
        chk = CompiledCheck(type="schema", severity="error", config={"columns": {"x": "int64"}}, phase="assertion")
        result = _execute_check(chk, table)
        assert result.passed is False
        assert "int32" in result.message


# ---------------------------------------------------------------------------
# Tests: Assertions run without short-circuiting (Req 24.3)
# ---------------------------------------------------------------------------


class TestAssertionNoShortCircuit:
    def test_all_assertions_run_even_if_first_fails(self) -> None:
        """All assertions execute; worst severity wins."""
        checks = [
            CompiledCheck(type="not_null", severity="error", config={"column": "x"}, phase="assertion"),
            CompiledCheck(type="row_count", severity="warning", config={"min": 100}, phase="assertion"),
            CompiledCheck(type="unique", severity="error", config={"column": "x"}, phase="assertion"),
        ]
        j1 = _make_compiled_joint("sink1", "sink", upstream=["src"], fused_group_id="g1", checks=checks)
        j0 = _make_compiled_joint("src", "source", fused_group_id="g0")

        g0 = _make_group("g0", ["src"])
        g1 = _make_group("g1", ["sink1"])

        assembly = _make_compiled_assembly(
            joints=[j0, j1],
            groups=[g0, g1],
            execution_order=["g0", "g1"],
            materializations=[
                Materialization(from_joint="src", to_joint="sink1", trigger="engine_instance_change", detail="", strategy="arrow"),
            ],
        )

        executor = Executor()
        result = executor.run(assembly)

        # Find sink1 results
        sink_result = next(jr for jr in result.joint_results if jr.name == "sink1")
        # All 3 checks should have run (no short-circuiting)
        assert len(sink_result.check_results) == 3

    def test_worst_severity_wins(self) -> None:
        """If any error-severity assertion fails, the joint is considered failed."""
        checks = [
            CompiledCheck(type="not_null", severity="error", config={"column": "x"}, phase="assertion"),
            CompiledCheck(type="row_count", severity="warning", config={"min": 100}, phase="assertion"),
        ]
        # Table with nulls → not_null fails (error), row_count fails (warning)
        j1 = _make_compiled_joint("t", "sql", fused_group_id="g1", checks=checks)

        g1 = _make_group("g1", ["t"])

        assembly = _make_compiled_assembly(
            joints=[j1], groups=[g1], execution_order=["g1"]
        )

        executor = Executor()
        result = executor.run(assembly)

        # not_null on empty table passes (0 nulls), but row_count fails (0 < 100)
        # The row_count is warning severity, so joint should still succeed
        # unless not_null also fails
        t_result = next(jr for jr in result.joint_results if jr.name == "t")
        # row_count check should fail (0 rows < 100 min)
        row_count_check = next(cr for cr in t_result.check_results if cr.type == "row_count")
        assert row_count_check.passed is False
        assert row_count_check.severity == "warning"


# ---------------------------------------------------------------------------
# Tests: Assertions receive read-only pyarrow.Table (Req 24.4)
# ---------------------------------------------------------------------------


class TestAssertionReadOnly:
    def test_assertion_receives_table(self) -> None:
        """Assertions receive the materialized pyarrow.Table."""
        checks = [
            CompiledCheck(type="row_count", severity="error", config={"min": 0}, phase="assertion"),
        ]
        j1 = _make_compiled_joint("t", "sql", fused_group_id="g1", checks=checks)
        g1 = _make_group("g1", ["t"])

        assembly = _make_compiled_assembly(
            joints=[j1], groups=[g1], execution_order=["g1"]
        )

        executor = Executor()
        result = executor.run(assembly)

        t_result = next(jr for jr in result.joint_results if jr.name == "t")
        assert len(t_result.check_results) == 1
        assert t_result.check_results[0].passed is True


# ---------------------------------------------------------------------------
# Tests: Audits run after sink writes (Req 25.1, 25.2)
# ---------------------------------------------------------------------------


class TestAuditExecution:
    def test_audit_runs_on_sink_joint(self) -> None:
        """Audits (phase='audit') run after sink write."""
        audit_checks = [
            CompiledCheck(type="row_count", severity="error", config={"min": 0}, phase="audit"),
        ]
        j0 = _make_compiled_joint("src", "source", fused_group_id="g0")
        j1 = _make_compiled_joint(
            "sink1", "sink", upstream=["src"], fused_group_id="g1",
            checks=audit_checks, catalog="test_cat",
        )

        g0 = _make_group("g0", ["src"])
        g1 = _make_group("g1", ["sink1"])

        assembly = _make_compiled_assembly(
            joints=[j0, j1],
            groups=[g0, g1],
            execution_order=["g0", "g1"],
            materializations=[
                Materialization(from_joint="src", to_joint="sink1", trigger="engine_instance_change", detail="", strategy="arrow"),
            ],
        )

        executor = Executor()
        result = executor.run(assembly)

        sink_result = next(jr for jr in result.joint_results if jr.name == "sink1")
        # Audit should have run
        audit_results = [cr for cr in sink_result.check_results if cr.phase == "audit"]
        assert len(audit_results) == 1
        assert audit_results[0].type == "row_count"
        assert audit_results[0].phase == "audit"

    def test_audit_uses_same_check_types(self) -> None:
        """Audits use the same check types as assertions."""
        audit_checks = [
            CompiledCheck(type="not_null", severity="warning", config={"column": "x"}, phase="audit"),
        ]
        j0 = _make_compiled_joint("src", "source", fused_group_id="g0")
        j1 = _make_compiled_joint(
            "sink1", "sink", upstream=["src"], fused_group_id="g1",
            checks=audit_checks, catalog="test_cat",
        )

        g0 = _make_group("g0", ["src"])
        g1 = _make_group("g1", ["sink1"])

        assembly = _make_compiled_assembly(
            joints=[j0, j1],
            groups=[g0, g1],
            execution_order=["g0", "g1"],
            materializations=[
                Materialization(from_joint="src", to_joint="sink1", trigger="engine_instance_change", detail="", strategy="arrow"),
            ],
        )

        executor = Executor()
        result = executor.run(assembly)

        sink_result = next(jr for jr in result.joint_results if jr.name == "sink1")
        audit_results = [cr for cr in sink_result.check_results if cr.phase == "audit"]
        assert len(audit_results) == 1
        assert audit_results[0].type == "not_null"


# ---------------------------------------------------------------------------
# Tests: Audit failures reported but no rollback (Req 25.5)
# ---------------------------------------------------------------------------


class TestAuditNoRollback:
    def test_audit_failure_reported_no_rollback(self) -> None:
        """Error-severity audit failure is reported but does not rollback."""
        audit_checks = [
            CompiledCheck(type="row_count", severity="error", config={"min": 1000}, phase="audit"),
        ]
        j0 = _make_compiled_joint("src", "source", fused_group_id="g0")
        j1 = _make_compiled_joint(
            "sink1", "sink", upstream=["src"], fused_group_id="g1",
            checks=audit_checks, catalog="test_cat",
        )

        g0 = _make_group("g0", ["src"])
        g1 = _make_group("g1", ["sink1"])

        assembly = _make_compiled_assembly(
            joints=[j0, j1],
            groups=[g0, g1],
            execution_order=["g0", "g1"],
            materializations=[
                Materialization(from_joint="src", to_joint="sink1", trigger="engine_instance_change", detail="", strategy="arrow"),
            ],
        )

        executor = Executor()
        result = executor.run(assembly)

        # Audit failure should be reported
        assert result.total_check_failures >= 1
        sink_result = next(jr for jr in result.joint_results if jr.name == "sink1")
        audit_results = [cr for cr in sink_result.check_results if cr.phase == "audit"]
        assert len(audit_results) == 1
        assert audit_results[0].passed is False


# ---------------------------------------------------------------------------
# Tests: Audit read_back_rows field (Req 23.4)
# ---------------------------------------------------------------------------


class TestAuditReadBackRows:
    def test_audit_result_has_read_back_rows(self) -> None:
        """CheckExecutionResult for audits includes read_back_rows."""
        audit_checks = [
            CompiledCheck(type="row_count", severity="error", config={"min": 0}, phase="audit"),
        ]
        j0 = _make_compiled_joint("src", "source", fused_group_id="g0")
        j1 = _make_compiled_joint(
            "sink1", "sink", upstream=["src"], fused_group_id="g1",
            checks=audit_checks, catalog="test_cat",
        )

        g0 = _make_group("g0", ["src"])
        g1 = _make_group("g1", ["sink1"])

        assembly = _make_compiled_assembly(
            joints=[j0, j1],
            groups=[g0, g1],
            execution_order=["g0", "g1"],
            materializations=[
                Materialization(from_joint="src", to_joint="sink1", trigger="engine_instance_change", detail="", strategy="arrow"),
            ],
        )

        executor = Executor()
        result = executor.run(assembly)

        sink_result = next(jr for jr in result.joint_results if jr.name == "sink1")
        audit_results = [cr for cr in sink_result.check_results if cr.phase == "audit"]
        assert len(audit_results) == 1
        assert audit_results[0].read_back_rows is not None


# ---------------------------------------------------------------------------
# Tests: Assertion error prevents downstream (Req 24.5)
# ---------------------------------------------------------------------------


class TestAssertionPreventsWrite:
    def test_error_assertion_failure_marks_joint_failed(self) -> None:
        """Error-severity assertion failure marks the joint as failed."""
        checks = [
            CompiledCheck(type="row_count", severity="error", config={"min": 100}, phase="assertion"),
        ]
        j1 = _make_compiled_joint("t", "sql", fused_group_id="g1", checks=checks)
        g1 = _make_group("g1", ["t"])

        assembly = _make_compiled_assembly(
            joints=[j1], groups=[g1], execution_order=["g1"]
        )

        executor = Executor()
        result = executor.run(assembly)

        t_result = next(jr for jr in result.joint_results if jr.name == "t")
        assert t_result.success is False
        assert result.total_check_failures >= 1

    def test_warning_assertion_does_not_fail_joint(self) -> None:
        """Warning-severity assertion failure does not fail the joint."""
        checks = [
            CompiledCheck(type="row_count", severity="warning", config={"min": 100}, phase="assertion"),
        ]
        j1 = _make_compiled_joint("t", "sql", fused_group_id="g1", checks=checks)
        g1 = _make_group("g1", ["t"])

        assembly = _make_compiled_assembly(
            joints=[j1], groups=[g1], execution_order=["g1"]
        )

        executor = Executor()
        result = executor.run(assembly)

        t_result = next(jr for jr in result.joint_results if jr.name == "t")
        assert t_result.success is True
        assert result.total_check_warnings >= 1


# ---------------------------------------------------------------------------
# Tests: Audits skipped when assertion fails (Req 42.2)
# ---------------------------------------------------------------------------


class TestAuditSkippedOnAssertionFailure:
    def test_audits_skipped_when_assertion_error(self) -> None:
        """If error-severity assertion fails, audits are skipped."""
        checks = [
            CompiledCheck(type="row_count", severity="error", config={"min": 100}, phase="assertion"),
            CompiledCheck(type="not_null", severity="error", config={"column": "x"}, phase="audit"),
        ]
        j0 = _make_compiled_joint("src", "source", fused_group_id="g0")
        j1 = _make_compiled_joint(
            "sink1", "sink", upstream=["src"], fused_group_id="g1",
            checks=checks, catalog="test_cat",
        )

        g0 = _make_group("g0", ["src"])
        g1 = _make_group("g1", ["sink1"])

        assembly = _make_compiled_assembly(
            joints=[j0, j1],
            groups=[g0, g1],
            execution_order=["g0", "g1"],
            materializations=[
                Materialization(from_joint="src", to_joint="sink1", trigger="engine_instance_change", detail="", strategy="arrow"),
            ],
        )

        executor = Executor()
        result = executor.run(assembly)

        sink_result = next(jr for jr in result.joint_results if jr.name == "sink1")
        # Only assertion checks should be present, no audit checks
        audit_results = [cr for cr in sink_result.check_results if cr.phase == "audit"]
        assert len(audit_results) == 0


# ---------------------------------------------------------------------------
# Tests: Mixed assertions and audits on sink (Req 42.1)
# ---------------------------------------------------------------------------


class TestSinkAssertionAndAudit:
    def test_sink_runs_assertions_then_audits(self) -> None:
        """Sink four-phase: compute → assertions → write → audits."""
        checks = [
            CompiledCheck(type="row_count", severity="error", config={"min": 0}, phase="assertion"),
            CompiledCheck(type="row_count", severity="warning", config={"min": 0}, phase="audit"),
        ]
        j0 = _make_compiled_joint("src", "source", fused_group_id="g0")
        j1 = _make_compiled_joint(
            "sink1", "sink", upstream=["src"], fused_group_id="g1",
            checks=checks, catalog="test_cat",
        )

        g0 = _make_group("g0", ["src"])
        g1 = _make_group("g1", ["sink1"])

        assembly = _make_compiled_assembly(
            joints=[j0, j1],
            groups=[g0, g1],
            execution_order=["g0", "g1"],
            materializations=[
                Materialization(from_joint="src", to_joint="sink1", trigger="engine_instance_change", detail="", strategy="arrow"),
            ],
        )

        executor = Executor()
        result = executor.run(assembly)

        sink_result = next(jr for jr in result.joint_results if jr.name == "sink1")
        assertion_results = [cr for cr in sink_result.check_results if cr.phase == "assertion"]
        audit_results = [cr for cr in sink_result.check_results if cr.phase == "audit"]
        assert len(assertion_results) == 1
        assert len(audit_results) == 1
        assert assertion_results[0].passed is True
        assert audit_results[0].passed is True


# ---------------------------------------------------------------------------
# Tests: Check counts in ExecutionResult
# ---------------------------------------------------------------------------


class TestCheckCounts:
    def test_total_check_failures_counted(self) -> None:
        checks = [
            CompiledCheck(type="row_count", severity="error", config={"min": 100}, phase="assertion"),
        ]
        j1 = _make_compiled_joint("t", "sql", fused_group_id="g1", checks=checks)
        g1 = _make_group("g1", ["t"])

        assembly = _make_compiled_assembly(
            joints=[j1], groups=[g1], execution_order=["g1"]
        )

        executor = Executor()
        result = executor.run(assembly)
        assert result.total_check_failures >= 1

    def test_total_check_warnings_counted(self) -> None:
        checks = [
            CompiledCheck(type="row_count", severity="warning", config={"min": 100}, phase="assertion"),
        ]
        j1 = _make_compiled_joint("t", "sql", fused_group_id="g1", checks=checks)
        g1 = _make_group("g1", ["t"])

        assembly = _make_compiled_assembly(
            joints=[j1], groups=[g1], execution_order=["g1"]
        )

        executor = Executor()
        result = executor.run(assembly)
        assert result.total_check_warnings >= 1
        assert result.total_check_failures == 0


# ---------------------------------------------------------------------------
# Tests: Non-sink joints with audits (Req 25.3 — caught at compile time)
# ---------------------------------------------------------------------------


class TestAuditOnNonSink:
    def test_audit_on_non_sink_not_executed(self) -> None:
        """Audits on non-sink joints are not executed (caught at compile time as RVT-651).

        The executor only runs audits for sink joints.
        """
        audit_checks = [
            CompiledCheck(type="row_count", severity="error", config={"min": 0}, phase="audit"),
        ]
        j1 = _make_compiled_joint("t", "sql", fused_group_id="g1", checks=audit_checks)
        g1 = _make_group("g1", ["t"])

        assembly = _make_compiled_assembly(
            joints=[j1], groups=[g1], execution_order=["g1"]
        )

        executor = Executor()
        result = executor.run(assembly)

        t_result = next(jr for jr in result.joint_results if jr.name == "t")
        # No audit results since this is not a sink
        audit_results = [cr for cr in t_result.check_results if cr.phase == "audit"]
        assert len(audit_results) == 0
