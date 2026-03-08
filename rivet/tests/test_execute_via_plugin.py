"""Tests for Executor._execute_via_plugin (task 6.2)."""

from __future__ import annotations

import asyncio
from typing import Any

import pyarrow
import pytest

from rivet_core.compiler import CompiledJoint
from rivet_core.errors import ExecutionError
from rivet_core.executor import Executor
from rivet_core.models import ComputeEngine
from rivet_core.optimizer import FusedGroup, FusionResult
from rivet_core.plugins import ComputeEnginePlugin, PluginRegistry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_joint(
    name: str,
    joint_type: str = "sql",
    upstream: list[str] | None = None,
    engine: str = "duckdb",
    **kwargs: Any,
) -> CompiledJoint:
    defaults = dict(
        catalog=None, catalog_type=None, engine=engine,
        engine_resolution="project_default", adapter=None,
        sql=None, sql_translated=None, sql_resolved=None,
        sql_dialect=None, engine_dialect=None,
        upstream=upstream or [], eager=False, table=None,
        write_strategy=None, function=None, source_file=None,
        logical_plan=None, output_schema=None, column_lineage=[],
        optimizations=[], checks=[], fused_group_id=None,
        tags=[], description=None, fusion_strategy_override=None,
        materialization_strategy_override=None,
    )
    defaults.update(kwargs)
    return CompiledJoint(name=name, type=joint_type, **defaults)


def _make_group(
    group_id: str,
    joints: list[str],
    fused_sql: str | None = None,
    engine: str = "duckdb",
    engine_type: str = "duckdb",
    entry_joints: list[str] | None = None,
    exit_joints: list[str] | None = None,
    fusion_result: FusionResult | None = None,
) -> FusedGroup:
    return FusedGroup(
        id=group_id, joints=joints, engine=engine, engine_type=engine_type,
        adapters={j: None for j in joints}, fused_sql=fused_sql,
        entry_joints=entry_joints or [joints[0]],
        exit_joints=exit_joints or [joints[-1]],
        fusion_result=fusion_result,
    )


class FakePlugin(ComputeEnginePlugin):
    engine_type = "fake"
    supported_catalog_types: dict[str, list[str]] = {}

    def __init__(self, result: pyarrow.Table | None = None, error: Exception | None = None):
        self._result = result or pyarrow.table({"x": [1]})
        self._error = error
        self.calls: list[tuple] = []

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type=self.engine_type, config=config)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(self, engine: Any, sql: str, input_tables: dict[str, pyarrow.Table]) -> pyarrow.Table:
        self.calls.append((engine, sql, dict(input_tables)))
        if self._error:
            raise self._error
        return self._result


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestExecuteViaPlugin:
    """Tests for _execute_via_plugin method."""

    def test_calls_plugin_execute_sql_with_correct_args(self) -> None:
        """Plugin's execute_sql is called with engine, sql, and input_tables."""
        plugin = FakePlugin(result=pyarrow.table({"out": [42]}))
        registry = PluginRegistry()
        registry.register_engine_plugin(plugin)
        engine = plugin.create_engine("fake_engine", {})
        registry.register_compute_engine(engine)

        executor = Executor(registry=registry)

        src_table = pyarrow.table({"a": [1, 2]})
        materials = {"src": src_table}
        j_src = _make_joint("src", "source", engine="fake_engine")
        j_sql = _make_joint("transform", "sql", upstream=["src"], engine="fake_engine")
        joint_map = {"src": j_src, "transform": j_sql}
        group = _make_group("g1", ["transform"], fused_sql="SELECT * FROM src",
                            engine="fake_engine", engine_type="fake")

        result, adapter_residual = asyncio.run(executor._execute_via_plugin(group, materials, joint_map, None, plugin))

        assert len(plugin.calls) == 1
        call_engine, call_sql, call_tables = plugin.calls[0]
        assert call_engine is engine
        assert call_sql == "SELECT * FROM src"
        assert "src" in call_tables
        assert call_tables["src"].equals(src_table)
        assert result.equals(pyarrow.table({"out": [42]}))

    def test_wraps_execute_sql_exception_in_rvt_503(self) -> None:
        """Exceptions from plugin.execute_sql are wrapped in RVT-503."""
        plugin = FakePlugin(error=RuntimeError("boom"))
        registry = PluginRegistry()
        registry.register_engine_plugin(plugin)
        engine = plugin.create_engine("fake_engine", {})
        registry.register_compute_engine(engine)

        executor = Executor(registry=registry)
        group = _make_group("g1", ["t"], fused_sql="SELECT 1",
                            engine="fake_engine", engine_type="fake")
        j = _make_joint("t", "sql", engine="fake_engine")

        with pytest.raises(ExecutionError) as exc_info:
            asyncio.run(executor._execute_via_plugin(group, {}, {"t": j}, None, plugin))

        assert exc_info.value.error.code == "RVT-503"
        assert "boom" in exc_info.value.error.message
        assert exc_info.value.error.context["engine_type"] == "fake"
        assert exc_info.value.error.context["group_id"] == "g1"

    def test_no_sql_returns_first_input(self) -> None:
        """When group has no SQL, returns first available input table."""
        plugin = FakePlugin()
        executor = Executor()

        src_table = pyarrow.table({"v": [10]})
        materials = {"src": src_table}
        j = _make_joint("t", "sql", upstream=["src"])
        group = _make_group("g1", ["t"], fused_sql=None)

        result, _ = asyncio.run(executor._execute_via_plugin(group, materials, {"t": j}, None, plugin))
        assert result.equals(src_table)
        assert len(plugin.calls) == 0  # execute_sql not called

    def test_no_sql_no_input_returns_empty_table(self) -> None:
        """When group has no SQL and no inputs, returns empty table."""
        plugin = FakePlugin()
        executor = Executor()

        j = _make_joint("t", "sql")
        group = _make_group("g1", ["t"], fused_sql=None)

        result, _ = asyncio.run(executor._execute_via_plugin(group, {}, {"t": j}, None, plugin))
        assert result.num_rows == 0

    def test_uses_fusion_result_resolved_sql(self) -> None:
        """Prefers fusion_result.resolved_fused_sql over fused_sql."""
        plugin = FakePlugin()
        registry = PluginRegistry()
        registry.register_engine_plugin(plugin)
        engine = plugin.create_engine("fake_engine", {})
        registry.register_compute_engine(engine)

        executor = Executor(registry=registry)

        fr = FusionResult(
            fused_sql="SELECT * FROM raw",
            statements=[],
            final_select="SELECT * FROM raw",
            resolved_fused_sql="SELECT * FROM resolved",
        )
        group = _make_group("g1", ["t"], fused_sql="SELECT * FROM fallback",
                            engine="fake_engine", engine_type="fake",
                            fusion_result=fr)
        j = _make_joint("t", "sql", engine="fake_engine")

        asyncio.run(executor._execute_via_plugin(group, {}, {"t": j}, None, plugin))

        assert plugin.calls[0][1] == "SELECT * FROM resolved"

    def test_execution_error_from_plugin_not_double_wrapped(self) -> None:
        """ExecutionError from plugin.execute_sql is re-raised, not wrapped again."""
        from rivet_core.errors import RivetError

        inner_error = ExecutionError(
            RivetError(code="RVT-502", message="unsupported", context={})
        )
        plugin = FakePlugin(error=inner_error)
        registry = PluginRegistry()
        registry.register_engine_plugin(plugin)
        engine = plugin.create_engine("fake_engine", {})
        registry.register_compute_engine(engine)

        executor = Executor(registry=registry)
        group = _make_group("g1", ["t"], fused_sql="SELECT 1",
                            engine="fake_engine", engine_type="fake")
        j = _make_joint("t", "sql", engine="fake_engine")

        with pytest.raises(ExecutionError) as exc_info:
            asyncio.run(executor._execute_via_plugin(group, {}, {"t": j}, None, plugin))

        # Should be the original RVT-502, not wrapped in RVT-503
        assert exc_info.value.error.code == "RVT-502"

    def test_collects_upstream_from_multiple_joints(self) -> None:
        """Upstream materials from multiple joints are collected into input_tables."""
        plugin = FakePlugin()
        registry = PluginRegistry()
        registry.register_engine_plugin(plugin)
        engine = plugin.create_engine("fake_engine", {})
        registry.register_compute_engine(engine)

        executor = Executor(registry=registry)

        t1 = pyarrow.table({"a": [1]})
        t2 = pyarrow.table({"b": [2]})
        materials = {"s1": t1, "s2": t2}

        j = _make_joint("t", "sql", upstream=["s1", "s2"], engine="fake_engine")
        joint_map = {
            "s1": _make_joint("s1", "source", engine="fake_engine"),
            "s2": _make_joint("s2", "source", engine="fake_engine"),
            "t": j,
        }
        group = _make_group("g1", ["t"], fused_sql="SELECT * FROM s1 JOIN s2",
                            engine="fake_engine", engine_type="fake")

        asyncio.run(executor._execute_via_plugin(group, materials, joint_map, None, plugin))

        call_tables = plugin.calls[0][2]
        assert "s1" in call_tables
        assert "s2" in call_tables
