"""E2E tests for flexible assertion materialization.

Exercises the full pipeline lifecycle — compile → execute → verify — with
engine-native assertion execution on DuckDB.  Tests verify that assertions
run engine-natively when possible, that failures halt the pipeline, that
mixed native/residual assertions both execute, and that backward
compatibility is preserved for engines without native support.

Requirements: 4.1, 4.3, 6.2, 6.3, 8.1
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pyarrow.csv as pcsv

from rivet_core.assembly import Assembly
from rivet_core.checks import Assertion
from rivet_core.compiler import compile
from rivet_core.executor import Executor
from rivet_core.models import Catalog, Joint
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


def _compile_and_run(
    joints: list[Joint],
    data_dir: Path,
    *,
    registry: PluginRegistry | None = None,
) -> tuple:
    if registry is None:
        registry = _setup_registry()
    catalogs = [
        Catalog(name="local", type="filesystem", options={"path": str(data_dir), "format": "csv"}),
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


def test_native_assertion_pipeline(tmp_path: Path) -> None:
    """Full pipeline with DuckDB, not_null assertion, verify engine_native execution."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "users.csv").write_text("id,name\n1,Alice\n2,Bob\n3,Carol\n")

    joints = [
        Joint(name="src", joint_type="source", catalog="local", table="users"),
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
            name="sink", joint_type="sink", catalog="local", table="result", upstream=["transform"]
        ),
    ]

    compiled, result = _compile_and_run(joints, data_dir)

    assert result.success
    assert result.status == "success"

    # Find the transform joint's check results
    transform_jr = next(jr for jr in result.joint_results if jr.name == "transform")
    assert len(transform_jr.check_results) >= 1

    cr = transform_jr.check_results[0]
    assert cr.passed is True
    assert cr.type == "not_null"
    assert cr.execution_method == "engine_native"

    # Verify sink output is correct
    sink_csv = data_dir / "result.csv"
    assert sink_csv.exists()
    table = pcsv.read_csv(str(sink_csv))
    assert len(table) == 3


def test_native_assertion_failure_halts(tmp_path: Path) -> None:
    """Failing not_null assertion on DuckDB halts pipeline with error severity."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "users.csv").write_text("id,name\n1,Alice\n2,Bob\n3,Carol\n")

    joints = [
        Joint(name="src", joint_type="source", catalog="local", table="users"),
        Joint(
            name="transform",
            joint_type="sql",
            upstream=["src"],
            # Introduce NULLs via SQL so the not_null assertion fails
            sql="SELECT id, CASE WHEN id = 2 THEN NULL ELSE name END AS name FROM src",
            assertions=[
                Assertion(type="not_null", config={"columns": ["name"]}, severity="error"),
            ],
        ),
        Joint(
            name="sink", joint_type="sink", catalog="local", table="result", upstream=["transform"]
        ),
    ]

    compiled, result = _compile_and_run(joints, data_dir)

    # Pipeline should fail due to assertion error
    assert not result.success or result.total_check_failures > 0

    # Find the transform joint's check results
    transform_jr = next(jr for jr in result.joint_results if jr.name == "transform")
    assert len(transform_jr.check_results) >= 1

    cr = transform_jr.check_results[0]
    assert cr.passed is False
    assert cr.severity == "error"
    assert cr.type == "not_null"
    assert cr.execution_method == "engine_native"


def test_mixed_assertion_pipeline(tmp_path: Path) -> None:
    """Both not_null (native) and custom (residual) assertions execute correctly."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "orders.csv").write_text("id,amount\n1,100\n2,200\n3,150\n")

    joints = [
        Joint(name="src", joint_type="source", catalog="local", table="orders"),
        Joint(
            name="transform",
            joint_type="sql",
            upstream=["src"],
            sql="SELECT id, amount FROM src",
            assertions=[
                # SQL-translatable: will run engine-natively
                Assertion(type="not_null", config={"columns": ["id"]}, severity="error"),
                # Residual: must fall back to Arrow
                Assertion(
                    type="custom",
                    config={"sql": "SELECT COUNT(*) > 0 AS passed FROM __table__"},
                    severity="warning",
                ),
            ],
        ),
        Joint(
            name="sink", joint_type="sink", catalog="local", table="result", upstream=["transform"]
        ),
    ]

    compiled, result = _compile_and_run(joints, data_dir)

    # Pipeline should succeed (not_null passes, custom may warn)
    transform_jr = next(jr for jr in result.joint_results if jr.name == "transform")
    assert len(transform_jr.check_results) >= 2

    # Find the not_null check result
    not_null_cr = next(cr for cr in transform_jr.check_results if cr.type == "not_null")
    assert not_null_cr.passed is True

    # Find the custom check result
    custom_cr = next(cr for cr in transform_jr.check_results if cr.type == "custom")
    # Custom checks run via Arrow path
    assert custom_cr.execution_method == "arrow"

    # Verify sink output
    sink_csv = data_dir / "result.csv"
    assert sink_csv.exists()
    table = pcsv.read_csv(str(sink_csv))
    assert len(table) == 3


def test_backward_compat_no_native_support(tmp_path: Path) -> None:
    """Engine without native support uses Arrow execution and assertion_boundary."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "items.csv").write_text("id,name\n1,Widget\n2,Gadget\n3,Gizmo\n")

    joints = [
        Joint(name="src", joint_type="source", catalog="local", table="items"),
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
            name="sink", joint_type="sink", catalog="local", table="result", upstream=["transform"]
        ),
    ]

    # Patch DuckDB to not support native assertions
    from rivet_duckdb.engine import DuckDBComputeEnginePlugin

    with patch.object(
        DuckDBComputeEnginePlugin,
        "supports_native_assertions",
        new_callable=lambda: property(lambda self: False),
    ):
        compiled, result = _compile_and_run(joints, data_dir)

    assert result.success
    assert result.status == "success"

    # Verify assertion_boundary is present in compilation
    has_assertion_boundary = any(
        m.trigger == "assertion_boundary" for m in compiled.materializations
    )
    assert has_assertion_boundary, (
        "assertion_boundary should be present when engine lacks native support"
    )

    # Verify check results use Arrow execution method
    transform_jr = next(jr for jr in result.joint_results if jr.name == "transform")
    assert len(transform_jr.check_results) >= 1

    cr = transform_jr.check_results[0]
    assert cr.passed is True
    assert cr.execution_method == "arrow"

    # Verify sink output
    sink_csv = data_dir / "result.csv"
    assert sink_csv.exists()
    table = pcsv.read_csv(str(sink_csv))
    assert len(table) == 3
