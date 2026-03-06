"""Tests for Executor._resolve_cross_joint method (task 6.3).

Validates:
- Same engine type → arrow_passthrough
- Unknown producer (no fused_group_id) → arrow_passthrough
- Different engine type, no adapter → arrow_passthrough
- Different engine type, adapter registered → adapter.resolve_upstream() called
"""

from __future__ import annotations

from typing import Any

import pyarrow

from rivet_core.compiler import CompiledJoint
from rivet_core.executor import Executor
from rivet_core.optimizer import FusedGroup
from rivet_core.plugins import (
    CrossJointAdapter,
    CrossJointContext,
    PluginRegistry,
    UpstreamResolution,
)
from rivet_core.strategies import ArrowMaterialization, MaterializationContext, MaterializedRef


def _make_joint(name: str, *, catalog_type: str | None = None, table: str | None = None, fused_group_id: str | None = None) -> CompiledJoint:
    return CompiledJoint(
        name=name,
        type="sql",
        catalog=None,
        catalog_type=catalog_type,
        engine="eng1",
        engine_resolution="project_default",
        adapter=None,
        sql=None,
        sql_translated=None,
        sql_resolved=None,
        sql_dialect=None,
        engine_dialect=None,
        upstream=[],
        eager=False,
        table=table,
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


def _make_group(group_id: str, engine_type: str, joints: list[str] | None = None) -> FusedGroup:
    return FusedGroup(
        id=group_id,
        joints=joints or [group_id],
        engine="eng1",
        engine_type=engine_type,
        adapters={},
        fused_sql=None,
    )


def _make_ref() -> MaterializedRef:
    table = pyarrow.table({"x": [1]})
    return ArrowMaterialization().materialize(
        table, MaterializationContext(joint_name="test", strategy_name="arrow", options={})
    )


class _FakeAdapter(CrossJointAdapter):
    def __init__(self, consumer: str, producer: str, resolution: UpstreamResolution) -> None:
        self.consumer_engine_type = consumer
        self.producer_engine_type = producer
        self._resolution = resolution

    def resolve_upstream(self, producer_ref: Any, consumer_engine: Any, joint_context: Any) -> UpstreamResolution:
        self.last_call = (producer_ref, consumer_engine, joint_context)
        return self._resolution


class TestResolveCrossJoint:
    def test_same_engine_type_returns_arrow_passthrough(self):
        executor = Executor(registry=None)
        ref = _make_ref()
        producer_group = _make_group("g1", "duckdb", joints=["producer"])
        consumer_group = _make_group("g2", "duckdb", joints=["consumer"])
        joint_map = {
            "producer": _make_joint("producer", fused_group_id="g1"),
            "consumer": _make_joint("consumer", fused_group_id="g2"),
        }
        group_map = {"g1": producer_group, "g2": consumer_group}

        result = executor._resolve_cross_joint(ref, consumer_group, "producer", "consumer", joint_map, group_map)
        assert result.strategy == "arrow_passthrough"

    def test_no_producer_fused_group_returns_arrow_passthrough(self):
        executor = Executor(registry=None)
        ref = _make_ref()
        consumer_group = _make_group("g2", "databricks", joints=["consumer"])
        joint_map = {
            "producer": _make_joint("producer", fused_group_id=None),
            "consumer": _make_joint("consumer", fused_group_id="g2"),
        }
        group_map = {"g2": consumer_group}

        result = executor._resolve_cross_joint(ref, consumer_group, "producer", "consumer", joint_map, group_map)
        assert result.strategy == "arrow_passthrough"

    def test_different_engine_no_adapter_returns_arrow_passthrough(self):
        registry = PluginRegistry()
        executor = Executor(registry=registry)
        ref = _make_ref()
        producer_group = _make_group("g1", "duckdb", joints=["producer"])
        consumer_group = _make_group("g2", "databricks", joints=["consumer"])
        joint_map = {
            "producer": _make_joint("producer", fused_group_id="g1"),
            "consumer": _make_joint("consumer", fused_group_id="g2"),
        }
        group_map = {"g1": producer_group, "g2": consumer_group}

        result = executor._resolve_cross_joint(ref, consumer_group, "producer", "consumer", joint_map, group_map)
        assert result.strategy == "arrow_passthrough"

    def test_different_engine_with_adapter_delegates(self):
        registry = PluginRegistry()
        expected = UpstreamResolution(strategy="native_reference", table_reference="catalog.schema.table")
        adapter = _FakeAdapter("databricks", "databricks", expected)
        registry.register_cross_joint_adapter(adapter)

        executor = Executor(registry=registry)
        ref = _make_ref()
        producer_group = _make_group("g1", "databricks", joints=["producer"])
        consumer_group = _make_group("g2", "databricks", joints=["consumer"])
        # Different engine types to trigger adapter lookup
        producer_group = _make_group("g1", "databricks", joints=["producer"])
        consumer_group = _make_group("g2", "databricks", joints=["consumer"])
        # Override engine_types to be different so adapter is looked up
        # Actually for this test, we need different engine types
        # Let's use the adapter key (databricks, duckdb) instead
        adapter2 = _FakeAdapter("databricks", "duckdb", expected)
        registry.register_cross_joint_adapter(adapter2)

        producer_group = _make_group("g1", "duckdb", joints=["producer"])
        consumer_group = _make_group("g2", "databricks", joints=["consumer"])
        joint_map = {
            "producer": _make_joint("producer", fused_group_id="g1", catalog_type="local", table="my_table"),
            "consumer": _make_joint("consumer", fused_group_id="g2", catalog_type="unity"),
        }
        group_map = {"g1": producer_group, "g2": consumer_group}

        result = executor._resolve_cross_joint(ref, consumer_group, "producer", "consumer", joint_map, group_map)
        assert result is expected
        # Verify adapter was called with correct context
        assert adapter2.last_call[0] is ref
        ctx = adapter2.last_call[2]
        assert isinstance(ctx, CrossJointContext)
        assert ctx.producer_joint_name == "producer"
        assert ctx.consumer_joint_name == "consumer"
        assert ctx.producer_catalog_type == "local"
        assert ctx.producer_table == "my_table"
        assert ctx.consumer_catalog_type == "unity"

    def test_unknown_producer_joint_returns_arrow_passthrough(self):
        executor = Executor(registry=None)
        ref = _make_ref()
        consumer_group = _make_group("g2", "databricks", joints=["consumer"])
        joint_map = {"consumer": _make_joint("consumer", fused_group_id="g2")}
        group_map = {"g2": consumer_group}

        result = executor._resolve_cross_joint(ref, consumer_group, "nonexistent", "consumer", joint_map, group_map)
        assert result.strategy == "arrow_passthrough"

    def test_no_registry_returns_arrow_passthrough(self):
        executor = Executor(registry=None)
        ref = _make_ref()
        producer_group = _make_group("g1", "duckdb", joints=["producer"])
        consumer_group = _make_group("g2", "databricks", joints=["consumer"])
        joint_map = {
            "producer": _make_joint("producer", fused_group_id="g1"),
            "consumer": _make_joint("consumer", fused_group_id="g2"),
        }
        group_map = {"g1": producer_group, "g2": consumer_group}

        result = executor._resolve_cross_joint(ref, consumer_group, "producer", "consumer", joint_map, group_map)
        assert result.strategy == "arrow_passthrough"
