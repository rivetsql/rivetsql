"""Integration tests: ProgressCallback protocol with real DuckDB execution.

Compiles and executes pipelines with a recording callback to verify the
Executor invokes callback methods at the correct lifecycle points with
correct data. Uses real DuckDB engine — no mocks.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from rivet_core.assembly import Assembly
from rivet_core.compiler import compile
from rivet_core.errors import RivetError
from rivet_core.executor import (
    Executor,
    JointExecutionResult,
)
from rivet_core.models import Catalog, ComputeEngine, Joint
from rivet_core.plugins import PluginRegistry
from rivet_duckdb import DuckDBPlugin

# ---------------------------------------------------------------------------
# Recording callback
# ---------------------------------------------------------------------------


class RecordingCallback:
    """Stores all ProgressCallback invocations for later assertion."""

    def __init__(self) -> None:
        self.group_starts: list[tuple[str, str]] = []
        self.group_completes: list[tuple[str, bool, list[JointExecutionResult], float]] = []
        self.materializations: list[tuple[str, str, str]] = []
        self.check_results: list[tuple[str, str, bool, str]] = []
        self.errors: list[tuple[str, Any]] = []

    def on_group_start(self, group_id: str, engine: str) -> None:
        self.group_starts.append((group_id, engine))

    def on_group_complete(
        self,
        group_id: str,
        success: bool,
        joint_results: list[JointExecutionResult],
        elapsed_ms: float,
    ) -> None:
        self.group_completes.append((group_id, success, joint_results, elapsed_ms))

    def on_materialization(
        self,
        source_joint: str,
        target_engine: str,
        strategy: str,
    ) -> None:
        self.materializations.append((source_joint, target_engine, strategy))

    def on_check_result(
        self,
        joint_name: str,
        check_type: str,
        passed: bool,
        phase: str,
    ) -> None:
        self.check_results.append((joint_name, check_type, passed, phase))

    def on_error(self, group_id: str, error: RivetError) -> None:
        self.errors.append((group_id, error))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_registry() -> PluginRegistry:
    reg = PluginRegistry()
    reg.register_builtins()
    DuckDBPlugin(reg)
    return reg


def _setup_dual_engine_registry() -> tuple[PluginRegistry, ComputeEngine, ComputeEngine]:
    """Create a registry with two separate DuckDB engine instances."""
    reg = PluginRegistry()
    reg.register_builtins()
    DuckDBPlugin(reg)
    eng_plugin = reg.get_engine_plugin("duckdb")
    assert eng_plugin is not None
    eng1 = eng_plugin.create_engine("duckdb_primary", {})
    eng2 = eng_plugin.create_engine("duckdb_secondary", {})
    reg.register_compute_engine(eng1)
    reg.register_compute_engine(eng2)
    return reg, eng1, eng2


def _compile_pipeline(
    joints: list[Joint],
    data_dir: Path,
    *,
    default_engine: str = "duckdb_primary",
    engines: list[ComputeEngine] | None = None,
    registry: PluginRegistry | None = None,
) -> tuple[Any, PluginRegistry, Executor]:
    """Compile a pipeline and return (compiled, registry, executor)."""
    if registry is None:
        registry = _setup_registry()
    catalogs = [
        Catalog(name="local", type="filesystem", options={"path": str(data_dir), "format": "csv"})
    ]
    if engines is None:
        eng_plugin = registry.get_engine_plugin("duckdb")
        assert eng_plugin is not None
        eng = eng_plugin.create_engine("duckdb_primary", {})
        engines = [eng]
    for e in engines:
        if e.name not in {ce.name for ce in registry._compute_engines.values()}:
            registry.register_compute_engine(e)

    assembly = Assembly(joints)
    compiled = compile(
        assembly,
        catalogs=catalogs,
        engines=engines,
        registry=registry,
        default_engine=default_engine,
        introspect=True,
    )
    assert compiled.success, (
        f"Compilation failed: {[e.message for e in compiled.diagnostics.errors]}"
    )
    executor = Executor(registry=registry)
    return compiled, registry, executor


def _make_multi_group_data(tmp_path: Path) -> tuple[Path, list[Joint]]:
    """Create CSV data and joints for a cross-engine (multi-group) pipeline.

    Returns (data_dir, joints) where joints span two DuckDB engine instances,
    forcing at least two fused groups with a materialization boundary.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "orders.csv").write_text("id,amount\n1,100\n2,200\n3,300\n")

    joints = [
        Joint(
            name="src",
            joint_type="source",
            catalog="local",
            table="orders",
            engine="duckdb_primary",
        ),
        Joint(
            name="transform",
            joint_type="sql",
            upstream=["src"],
            sql="SELECT id, amount * 2 AS doubled FROM src",
            engine="duckdb_secondary",
        ),
        Joint(
            name="sink",
            joint_type="sink",
            catalog="local",
            table="result",
            upstream=["transform"],
            engine="duckdb_secondary",
        ),
    ]
    return data_dir, joints


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCallbackGroupLifecycle:
    """Verify on_group_start and on_group_complete are called for every group."""

    def test_callback_group_lifecycle(self, tmp_path: Path) -> None:
        """Every fused group triggers on_group_start and on_group_complete."""
        reg, eng1, eng2 = _setup_dual_engine_registry()
        data_dir, joints = _make_multi_group_data(tmp_path)

        compiled, _reg, executor = _compile_pipeline(
            joints,
            data_dir,
            registry=reg,
            engines=[eng1, eng2],
        )

        callback = RecordingCallback()
        result = executor.run_sync(compiled, progress=callback)

        assert result.success

        # Every fused group should have a start and a complete
        compiled_group_ids = {g.id for g in compiled.fused_groups}
        started_ids = {gid for gid, _engine in callback.group_starts}
        completed_ids = {gid for gid, _ok, _jr, _ms in callback.group_completes}

        assert started_ids == compiled_group_ids, (
            f"Started {started_ids} != compiled {compiled_group_ids}"
        )
        assert completed_ids == compiled_group_ids, (
            f"Completed {completed_ids} != compiled {compiled_group_ids}"
        )

        # Each group_start should have the correct engine
        engine_by_group = {g.id: g.engine for g in compiled.fused_groups}
        for gid, engine in callback.group_starts:
            assert engine == engine_by_group[gid], (
                f"Group {gid}: expected engine {engine_by_group[gid]}, got {engine}"
            )

        # All completions should be successful
        for gid, success, _jr, elapsed_ms in callback.group_completes:
            assert success, f"Group {gid} reported failure"
            assert elapsed_ms >= 0, f"Group {gid} has negative elapsed_ms"

    def test_single_group_pipeline(self, tmp_path: Path) -> None:
        """Single-engine pipeline still triggers callbacks for its group(s)."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "data.csv").write_text("id,val\n1,10\n2,20\n")

        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="sink",
                joint_type="sink",
                catalog="local",
                table="result",
                upstream=["src"],
            ),
        ]

        compiled, _reg, executor = _compile_pipeline(joints, data_dir)

        callback = RecordingCallback()
        result = executor.run_sync(compiled, progress=callback)

        assert result.success
        assert len(callback.group_starts) >= 1
        assert len(callback.group_completes) >= 1

        compiled_group_ids = {g.id for g in compiled.fused_groups}
        started_ids = {gid for gid, _engine in callback.group_starts}
        assert started_ids == compiled_group_ids


class TestCallbackNotProvided:
    """Verify that omitting the callback produces the same result as a no-op callback."""

    def test_callback_not_provided(self, tmp_path: Path) -> None:
        """Executing without a callback yields the same result as with a no-op callback.

        Validates: Requirements 4.7 (Property 6: No callback invocation without ProgressCallback)
        """
        reg, eng1, eng2 = _setup_dual_engine_registry()
        data_dir, joints = _make_multi_group_data(tmp_path)

        compiled, _reg, executor = _compile_pipeline(
            joints,
            data_dir,
            registry=reg,
            engines=[eng1, eng2],
        )

        # Execute WITHOUT a callback (default progress=None)
        result_no_cb = executor.run_sync(compiled)

        # Execute WITH a no-op recording callback
        noop_cb = RecordingCallback()
        result_with_cb = executor.run_sync(compiled, progress=noop_cb)

        # Both should succeed
        assert result_no_cb.success
        assert result_with_cb.success

        # Same success status
        assert result_no_cb.status == result_with_cb.status

        # Same number of joint results
        assert len(result_no_cb.joint_results) == len(result_with_cb.joint_results)

        # Same row counts per joint (order may differ, so compare as sorted sets)
        rows_no_cb = sorted((jr.name, jr.rows_out) for jr in result_no_cb.joint_results)
        rows_with_cb = sorted((jr.name, jr.rows_out) for jr in result_with_cb.joint_results)
        assert rows_no_cb == rows_with_cb

        # Same materialization and failure counts
        assert result_no_cb.total_materializations == result_with_cb.total_materializations
        assert result_no_cb.total_failures == result_with_cb.total_failures


class TestCallbackErrorHandling:
    """Verify that callback errors don't abort pipeline execution (Req 4.6)."""

    def test_callback_error_handling(self, tmp_path: Path) -> None:
        """Pipeline succeeds even when a callback raises in on_group_complete.

        Validates: Requirements 4.6
        """
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "data.csv").write_text("id,val\n1,10\n2,20\n")

        joints = [
            Joint(name="src", joint_type="source", catalog="local", table="data"),
            Joint(
                name="sink",
                joint_type="sink",
                catalog="local",
                table="result",
                upstream=["src"],
            ),
        ]

        class ErrorCallback(RecordingCallback):
            """Callback that raises on every on_group_complete invocation."""

            def on_group_complete(
                self,
                group_id: str,
                success: bool,
                joint_results: list[JointExecutionResult],
                elapsed_ms: float,
            ) -> None:
                super().on_group_complete(group_id, success, joint_results, elapsed_ms)
                raise RuntimeError("boom from callback")

        compiled, _reg, executor = _compile_pipeline(joints, data_dir)

        callback = ErrorCallback()
        result = executor.run_sync(compiled, progress=callback)

        # Pipeline must succeed despite the callback error
        assert result.success
        assert len(result.joint_results) > 0

        # The callback was still invoked (recorded before raising)
        assert len(callback.group_starts) >= 1
        assert len(callback.group_completes) >= 1
