"""Tests for Run Command (commands/run.py).

Validates task 9.2 requirements: format validation, compilation flow,
execution, exit code resolution, output rendering, and Property 13.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_cli.app import GlobalOptions
from rivet_cli.commands.run import run_run
from rivet_cli.exit_codes import (
    ASSERTION_FAILURE,
    AUDIT_FAILURE,
    GENERAL_ERROR,
    PARTIAL_FAILURE,
    SUCCESS,
    USAGE_ERROR,
)
from rivet_core.compiler import (
    CompiledAssembly,
    CompiledCatalog,
    CompiledEngine,
    CompiledJoint,
)
from rivet_core.errors import RivetError
from rivet_core.executor import (
    CheckExecutionResult,
    ExecutionResult,
    JointExecutionResult,
)
from rivet_core.metrics import PhasedTiming
from rivet_core.optimizer import FusedGroup

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _globals(**overrides) -> GlobalOptions:
    defaults = dict(profile="default", project_path=Path("."), verbosity=0, color=False)
    defaults.update(overrides)
    return GlobalOptions(**defaults)


def _compiled(success=True, errors=None) -> CompiledAssembly:
    return CompiledAssembly(
        success=success,
        profile_name="default",
        catalogs=[CompiledCatalog(name="cat", type="fs")],
        engines=[CompiledEngine(name="eng", engine_type="duckdb", native_catalog_types=["fs"])],
        adapters=[],
        joints=[
            CompiledJoint(
                name="j1", type="sql", catalog=None, catalog_type=None,
                engine="eng", engine_resolution="project_default", adapter=None,
                sql="SELECT 1", sql_translated=None, sql_resolved=None,
                sql_dialect=None, engine_dialect=None, upstream=[], eager=False,
                table=None, write_strategy=None, function=None, source_file=None,
                logical_plan=None, output_schema=None, column_lineage=[],
                optimizations=[], checks=[], fused_group_id="g1", tags=[],
                description=None, fusion_strategy_override=None,
                materialization_strategy_override=None,
            ),
        ],
        fused_groups=[FusedGroup(id="g1", joints=["j1"], engine="eng", engine_type="duckdb", adapters={}, fused_sql=None, fusion_strategy="cte")],
        materializations=[],
        execution_order=["g1"],
        errors=errors or [],
        warnings=[],
    )


def _exec_result(success=True, status="success", check_results=None, joint_error=None) -> ExecutionResult:
    return ExecutionResult(
        success=success,
        status=status,
        joint_results=[
            JointExecutionResult(
                name="j1", success=success, rows_in=None, rows_out=10,
                timing=PhasedTiming(total_ms=5.0, engine_ms=4.0, materialize_ms=0.5, residual_ms=0.3, check_ms=0.2),
                fused_group_id="g1", materialized=False, materialization_trigger=None,
                materialization_stats=None, check_results=check_results or [],
                plugin_metrics=None, error=joint_error,
            ),
        ],
        group_results=[],
        total_time_ms=5.0,
        total_materializations=0,
        total_failures=0 if success else 1,
        total_check_failures=0,
        total_check_warnings=0,
    )


# ---------------------------------------------------------------------------
# Format validation (Property 8 — RVT-856)
# ---------------------------------------------------------------------------

class TestFormatValidation:
    def test_invalid_format_returns_usage_error(self, capsys):
        code = run_run(None, [], False, True, "mermaid", _globals())
        assert code == USAGE_ERROR
        assert "RVT-856" in capsys.readouterr().err

    def test_valid_formats_accepted(self):
        """text, json, quiet should not trigger RVT-856 (they'll fail on load_config instead)."""
        for fmt in ("text", "json", "quiet"):
            code = run_run(None, [], False, True, fmt, _globals(project_path=Path("/nonexistent")))
            assert code != USAGE_ERROR


# ---------------------------------------------------------------------------
# Compilation failure → exit 1, no execution
# ---------------------------------------------------------------------------

class TestCompilationFailure:
    @patch("rivet_cli.commands.run.load_config")
    @patch("rivet_cli.commands.run.build_assembly")
    @patch("rivet_cli.commands.run.compile")
    @patch("rivet_cli.commands.run.Executor")
    def test_compile_failure_exits_1_no_execution(self, mock_executor, mock_compile, mock_bridge, mock_config):
        mock_config.return_value = MagicMock(success=True, profile=MagicMock())
        mock_bridge.return_value = MagicMock(assembly=MagicMock(), catalogs={}, engines={})
        mock_compile.return_value = _compiled(
            success=False,
            errors=[RivetError(code="RVT-306", message="cycle detected", remediation="fix DAG")],
        )
        code = run_run(None, [], False, True, "text", _globals())
        assert code == GENERAL_ERROR
        mock_executor.assert_not_called()


# ---------------------------------------------------------------------------
# Successful execution → exit 0
# ---------------------------------------------------------------------------

class TestSuccessfulExecution:
    @patch("rivet_cli.commands.run.load_config")
    @patch("rivet_cli.commands.run.build_assembly")
    @patch("rivet_cli.commands.run.compile")
    @patch("rivet_cli.commands.run.Executor")
    def test_success_exits_0(self, mock_executor_cls, mock_compile, mock_bridge, mock_config):
        mock_config.return_value = MagicMock(success=True, profile=MagicMock())
        mock_bridge.return_value = MagicMock(assembly=MagicMock(), catalogs={}, engines={})
        mock_compile.return_value = _compiled()
        mock_executor_cls.return_value.run.return_value = _exec_result()
        code = run_run(None, [], False, True, "text", _globals())
        assert code == SUCCESS


# ---------------------------------------------------------------------------
# Assertion failure → exit 4
# ---------------------------------------------------------------------------

class TestAssertionFailure:
    @patch("rivet_cli.commands.run.load_config")
    @patch("rivet_cli.commands.run.build_assembly")
    @patch("rivet_cli.commands.run.compile")
    @patch("rivet_cli.commands.run.Executor")
    def test_assertion_failure_exits_4(self, mock_executor_cls, mock_compile, mock_bridge, mock_config):
        mock_config.return_value = MagicMock(success=True, profile=MagicMock())
        mock_bridge.return_value = MagicMock(assembly=MagicMock(), catalogs={}, engines={})
        mock_compile.return_value = _compiled()
        mock_executor_cls.return_value.run.return_value = _exec_result(
            check_results=[
                CheckExecutionResult(type="not_null", severity="error", passed=False, message="null found", phase="assertion"),
            ],
        )
        code = run_run(None, [], False, True, "text", _globals())
        assert code == ASSERTION_FAILURE


# ---------------------------------------------------------------------------
# Audit failure → exit 5
# ---------------------------------------------------------------------------

class TestAuditFailure:
    @patch("rivet_cli.commands.run.load_config")
    @patch("rivet_cli.commands.run.build_assembly")
    @patch("rivet_cli.commands.run.compile")
    @patch("rivet_cli.commands.run.Executor")
    def test_audit_failure_exits_5(self, mock_executor_cls, mock_compile, mock_bridge, mock_config):
        mock_config.return_value = MagicMock(success=True, profile=MagicMock())
        mock_bridge.return_value = MagicMock(assembly=MagicMock(), catalogs={}, engines={})
        mock_compile.return_value = _compiled()
        mock_executor_cls.return_value.run.return_value = _exec_result(
            check_results=[
                CheckExecutionResult(type="row_count", severity="error", passed=False, message="mismatch", phase="audit"),
            ],
        )
        code = run_run(None, [], False, True, "text", _globals())
        assert code == AUDIT_FAILURE


# ---------------------------------------------------------------------------
# Assertion + audit → assertion takes precedence (exit 4)
# ---------------------------------------------------------------------------

class TestAssertionPrecedence:
    @patch("rivet_cli.commands.run.load_config")
    @patch("rivet_cli.commands.run.build_assembly")
    @patch("rivet_cli.commands.run.compile")
    @patch("rivet_cli.commands.run.Executor")
    def test_assertion_overrides_audit(self, mock_executor_cls, mock_compile, mock_bridge, mock_config):
        mock_config.return_value = MagicMock(success=True, profile=MagicMock())
        mock_bridge.return_value = MagicMock(assembly=MagicMock(), catalogs={}, engines={})
        mock_compile.return_value = _compiled()
        mock_executor_cls.return_value.run.return_value = _exec_result(
            check_results=[
                CheckExecutionResult(type="not_null", severity="error", passed=False, message="null", phase="assertion"),
                CheckExecutionResult(type="row_count", severity="error", passed=False, message="mismatch", phase="audit"),
            ],
        )
        code = run_run(None, [], False, True, "text", _globals())
        assert code == ASSERTION_FAILURE


# ---------------------------------------------------------------------------
# Partial failure → exit 2
# ---------------------------------------------------------------------------

class TestPartialFailure:
    @patch("rivet_cli.commands.run.load_config")
    @patch("rivet_cli.commands.run.build_assembly")
    @patch("rivet_cli.commands.run.compile")
    @patch("rivet_cli.commands.run.Executor")
    def test_partial_failure_exits_2(self, mock_executor_cls, mock_compile, mock_bridge, mock_config):
        mock_config.return_value = MagicMock(success=True, profile=MagicMock())
        mock_bridge.return_value = MagicMock(assembly=MagicMock(), catalogs={}, engines={})
        mock_compile.return_value = _compiled()
        mock_executor_cls.return_value.run.return_value = _exec_result(
            success=False, status="partial_failure",
        )
        code = run_run(None, [], False, False, "text", _globals())
        assert code == PARTIAL_FAILURE


# ---------------------------------------------------------------------------
# JSON format output
# ---------------------------------------------------------------------------

class TestJsonFormat:
    @patch("rivet_cli.commands.run.load_config")
    @patch("rivet_cli.commands.run.build_assembly")
    @patch("rivet_cli.commands.run.compile")
    @patch("rivet_cli.commands.run.Executor")
    def test_json_format_produces_valid_json(self, mock_executor_cls, mock_compile, mock_bridge, mock_config, capsys):
        import json
        mock_config.return_value = MagicMock(success=True, profile=MagicMock())
        mock_bridge.return_value = MagicMock(assembly=MagicMock(), catalogs={}, engines={})
        mock_compile.return_value = _compiled()
        mock_executor_cls.return_value.run.return_value = _exec_result()
        code = run_run(None, [], False, True, "json", _globals())
        assert code == SUCCESS
        output = capsys.readouterr().out
        data = json.loads(output)
        assert "execution" in data
        assert "compilation" in data


# ---------------------------------------------------------------------------
# Quiet format output
# ---------------------------------------------------------------------------

class TestQuietFormat:
    @patch("rivet_cli.commands.run.load_config")
    @patch("rivet_cli.commands.run.build_assembly")
    @patch("rivet_cli.commands.run.compile")
    @patch("rivet_cli.commands.run.Executor")
    def test_quiet_success_no_output(self, mock_executor_cls, mock_compile, mock_bridge, mock_config, capsys):
        mock_config.return_value = MagicMock(success=True, profile=MagicMock())
        mock_bridge.return_value = MagicMock(assembly=MagicMock(), catalogs={}, engines={})
        mock_compile.return_value = _compiled()
        mock_executor_cls.return_value.run.return_value = _exec_result()
        code = run_run(None, [], False, True, "quiet", _globals())
        assert code == SUCCESS
        captured = capsys.readouterr()
        assert captured.out == ""
        assert captured.err == ""

    @patch("rivet_cli.commands.run.load_config")
    @patch("rivet_cli.commands.run.build_assembly")
    @patch("rivet_cli.commands.run.compile")
    @patch("rivet_cli.commands.run.Executor")
    def test_quiet_failure_shows_errors(self, mock_executor_cls, mock_compile, mock_bridge, mock_config, capsys):
        mock_config.return_value = MagicMock(success=True, profile=MagicMock())
        mock_bridge.return_value = MagicMock(assembly=MagicMock(), catalogs={}, engines={})
        mock_compile.return_value = _compiled()
        mock_executor_cls.return_value.run.return_value = _exec_result(
            success=False, status="failure",
            joint_error=RivetError(code="RVT-500", message="exec failed", remediation="check logs"),
        )
        run_run(None, [], False, True, "quiet", _globals())
        captured = capsys.readouterr()
        assert "RVT-500" in captured.err


# ---------------------------------------------------------------------------
# Property 13: Run exit code matches execution outcome
# ---------------------------------------------------------------------------

# Strategy: generate combinations of check results and partial failure status
_check_st = st.fixed_dictionaries({
    "has_assertion": st.booleans(),
    "has_audit": st.booleans(),
    "is_partial": st.booleans(),
})


class TestProperty13ExitCodeMatchesOutcome:
    @given(scenario=_check_st)
    @settings(max_examples=100)
    @patch("rivet_cli.commands.run.load_config")
    @patch("rivet_cli.commands.run.build_assembly")
    @patch("rivet_cli.commands.run.compile")
    @patch("rivet_cli.commands.run.Executor")
    def test_exit_code_matches_execution_outcome(
        self, mock_executor_cls, mock_compile, mock_bridge, mock_config, scenario
    ):
        """Property 13: exit code is 4 if assertion failure, 5 if audit (no assertion),
        2 if partial (no assertion/audit), 0 otherwise."""
        mock_config.return_value = MagicMock(success=True, profile=MagicMock())
        mock_bridge.return_value = MagicMock(assembly=MagicMock(), catalogs={}, engines={})
        mock_compile.return_value = _compiled()

        checks = []
        if scenario["has_assertion"]:
            checks.append(CheckExecutionResult(
                type="not_null", severity="error", passed=False,
                message="null", phase="assertion",
            ))
        if scenario["has_audit"]:
            checks.append(CheckExecutionResult(
                type="row_count", severity="error", passed=False,
                message="mismatch", phase="audit",
            ))

        status = "partial_failure" if scenario["is_partial"] else "success"
        success = not scenario["is_partial"] and not scenario["has_assertion"] and not scenario["has_audit"]

        mock_executor_cls.return_value.run.return_value = _exec_result(
            success=success, status=status, check_results=checks,
        )

        code = run_run(None, [], False, True, "text", _globals())

        if scenario["has_assertion"]:
            assert code == ASSERTION_FAILURE
        elif scenario["has_audit"]:
            assert code == AUDIT_FAILURE
        elif scenario["is_partial"]:
            assert code == PARTIAL_FAILURE
        else:
            assert code == SUCCESS
