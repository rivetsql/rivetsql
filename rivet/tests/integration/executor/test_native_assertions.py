"""Integration tests: engine-native assertion execution with real DuckDB.

Exercises the DuckDB plugin's ``execute_assertion_sql`` method, assertion
result consistency across both execution paths, and per-check fallback on
engine errors.

Requirements: 4.1, 4.4, 6.1
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pyarrow as pa

from rivet_core.assembly import Assembly
from rivet_core.checks import Assertion, CompiledCheck
from rivet_core.compiler import compile
from rivet_core.executor import (
    Executor,
    _execute_check,
    _generate_check_sql,
    _interpret_check_sql_result,
)
from rivet_core.models import Catalog, Joint
from rivet_core.plugins import PluginRegistry
from rivet_duckdb import DuckDBPlugin


def _setup_registry() -> PluginRegistry:
    reg = PluginRegistry()
    reg.register_builtins()
    DuckDBPlugin(reg)
    return reg


def _compile_and_run(
    joints: list[Joint],
    data_dir: Path,
    *,
    registry: PluginRegistry | None = None,
) -> tuple:
    if registry is None:
        registry = _setup_registry()
    catalogs = [
        Catalog(name="local", type="filesystem", options={"path": str(data_dir), "format": "csv"})
    ]
    eng = registry.get_engine_plugin("duckdb").create_engine("duckdb_primary", {})
    if eng.name not in {ce.name for ce in registry._compute_engines.values()}:
        registry.register_compute_engine(eng)

    assembly = Assembly(joints)
    compiled = compile(
        assembly,
        catalogs=catalogs,
        engines=[eng],
        registry=registry,
        default_engine="duckdb_primary",
        introspect=True,
    )
    assert compiled.success, (
        f"Compilation failed: {[e.message for e in compiled.diagnostics.errors]}"
    )

    executor = Executor(registry=registry)
    result = executor.run_sync(compiled)
    return compiled, result


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDuckDBExecuteAssertionSQL:
    """Verify DuckDB plugin executes assertion SQL and returns correct Arrow table."""

    def test_duckdb_execute_assertion_sql(self) -> None:
        """DuckDB plugin's execute_assertion_sql returns correct result for a COUNT query."""
        registry = _setup_registry()
        plugin = registry.get_engine_plugin("duckdb")
        assert plugin is not None
        assert plugin.supports_native_assertions

        engine = plugin.create_engine("test_engine", {})

        # Create a table with some NULL values
        table = pa.table({"id": [1, 2, 3, 4, 5], "name": ["a", None, "c", None, "e"]})

        # Execute assertion SQL: count NULLs in 'name' column
        sql = "SELECT COUNT(*) AS result_value FROM test_data WHERE name IS NULL"
        result = plugin.execute_assertion_sql(engine, sql, {"test_data": table})

        assert isinstance(result, pa.Table)
        assert result.num_rows == 1
        assert "result_value" in result.column_names
        assert result.column("result_value")[0].as_py() == 2


class TestAssertionResultConsistencyDuckDB:
    """Run same assertion via both paths on DuckDB, verify identical passed result."""

    def test_assertion_result_consistency_duckdb(self) -> None:
        """Engine-native and Arrow-based paths produce the same passed result for not_null."""
        registry = _setup_registry()
        plugin = registry.get_engine_plugin("duckdb")
        engine = plugin.create_engine("test_engine", {})

        # Table with one NULL in 'value' column
        table = pa.table({"id": [1, 2, 3], "value": [10, None, 30]})

        check = CompiledCheck(
            type="not_null",
            severity="error",
            config={"columns": ["value"]},
            phase="assertion",
        )

        # Arrow-based path
        arrow_result = _execute_check(check, table)

        # Engine-native path
        sqls = _generate_check_sql(check, "test_data")
        assert len(sqls) == 1
        sql_result_table = plugin.execute_assertion_sql(engine, sqls[0], {"test_data": table})
        native_result = _interpret_check_sql_result(check, sql_result_table)

        # Both should agree on passed=False (there is a NULL)
        assert arrow_result.passed == native_result.passed
        assert arrow_result.passed is False
        assert native_result.execution_method == "engine_native"
        assert arrow_result.execution_method == "arrow"

    def test_assertion_result_consistency_passing(self) -> None:
        """Both paths agree when assertion passes (no NULLs)."""
        registry = _setup_registry()
        plugin = registry.get_engine_plugin("duckdb")
        engine = plugin.create_engine("test_engine", {})

        table = pa.table({"id": [1, 2, 3], "value": [10, 20, 30]})

        check = CompiledCheck(
            type="not_null",
            severity="error",
            config={"columns": ["value"]},
            phase="assertion",
        )

        arrow_result = _execute_check(check, table)
        sqls = _generate_check_sql(check, "test_data")
        sql_result_table = plugin.execute_assertion_sql(engine, sqls[0], {"test_data": table})
        native_result = _interpret_check_sql_result(check, sql_result_table)

        assert arrow_result.passed == native_result.passed
        assert arrow_result.passed is True


class TestFallbackOnEngineError:
    """Simulate engine error, verify fallback to Arrow-based execution."""

    def test_fallback_on_engine_error(self, tmp_path: Path) -> None:
        """When execute_assertion_sql raises, executor falls back to Arrow path."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "items.csv").write_text("id,name\n1,Alice\n2,Bob\n3,Carol\n")

        joints = [
            Joint(
                name="src",
                joint_type="source",
                catalog="local",
                table="items",
            ),
            Joint(
                name="transform",
                joint_type="sql",
                upstream=["src"],
                sql="SELECT id, name FROM src",
                assertions=[
                    Assertion(type="not_null", config={"columns": ["name"]}, severity="error"),
                ],
            ),
            Joint(
                name="sink",
                joint_type="sink",
                catalog="local",
                table="result",
                upstream=["transform"],
            ),
        ]

        registry = _setup_registry()

        # Patch execute_assertion_sql to raise an error
        def _failing_execute(self_plugin, engine, sql, input_tables):
            raise RuntimeError("Simulated engine error")

        from rivet_duckdb.engine import DuckDBComputeEnginePlugin

        with patch.object(DuckDBComputeEnginePlugin, "execute_assertion_sql", _failing_execute):
            compiled, result = _compile_and_run(joints, data_dir, registry=registry)

        # Pipeline should still succeed — fallback to Arrow
        assert result.success
        assert result.status == "success"

        # Verify the assertion passed (no NULLs in name column)
        transform_results = [jr for jr in result.joint_results if jr.name == "transform"]
        assert len(transform_results) == 1
        check_results = transform_results[0].check_results
        assert len(check_results) >= 1
        assert all(cr.passed for cr in check_results)
        # Fallback means execution_method should be "arrow"
        assert all(cr.execution_method == "arrow" for cr in check_results)
