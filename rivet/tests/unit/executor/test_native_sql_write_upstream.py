"""Unit tests for _try_native_sql_write with upstream SQL construction.

Tests the fix for sink joints that have no fused SQL but have upstream
materialized tables. The method should construct SELECT * FROM {upstream}
when exactly one upstream exists.

Requirements: bugfix.md 2.1, 2.3
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from rivet_core.compiler import CompiledCatalog, CompiledJoint
from rivet_core.executor import Executor
from rivet_core.optimizer import FusedGroup
from rivet_core.plugins import ComputeEngineAdapter, NativeSqlWriteContext, PluginRegistry
from rivet_core.strategies import MaterializedRef


class _SimpleRef(MaterializedRef):
    """Simple MaterializedRef for testing."""

    def __init__(self, tbl: pa.Table) -> None:
        self._tbl = tbl

    def to_arrow(self) -> pa.Table:
        return self._tbl

    @property
    def schema(self) -> Any:
        return None

    @property
    def row_count(self) -> int:
        return self._tbl.num_rows

    @property
    def size_bytes(self) -> int | None:
        return None

    @property
    def storage_type(self) -> str:
        return "arrow"


class _MockAdapter(ComputeEngineAdapter):
    """Mock adapter that supports native SQL write and captures context."""

    target_engine_type = "duckdb"
    catalog_type = "duckdb"
    capabilities: list[str] = []
    source = "engine_plugin"
    source_plugin = None

    def __init__(self) -> None:
        self.write_dispatch_calls: list[NativeSqlWriteContext] = []

    def supports_native_sql_write(self, write_strategy: str) -> bool:
        return write_strategy in ("replace", "append", "truncate_insert")

    def read_dispatch(self, engine: Any, catalog: Any, joint: Any, pushdown: Any = None) -> Any:
        raise NotImplementedError

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        if isinstance(material, NativeSqlWriteContext):
            self.write_dispatch_calls.append(material)
        return None


def _make_sink_joint(
    name: str = "my_sink",
    catalog: str = "test_catalog",
    table: str = "output_table",
    write_strategy: str = "replace",
    upstream: list[str] | None = None,
) -> CompiledJoint:
    """Create a minimal CompiledJoint for a sink."""
    return CompiledJoint(
        name=name,
        type="sink",
        catalog=catalog,
        catalog_type="duckdb",
        engine="duckdb_engine",
        engine_resolution="catalog_default",
        adapter="duckdb_adapter",
        sql=None,
        sql_translated=None,
        sql_resolved=None,
        sql_dialect=None,
        engine_dialect="duckdb",
        upstream=upstream if upstream is not None else ["upstream_transform"],
        eager=False,
        table=table,
        write_strategy=write_strategy,
        function=None,
        source_file=None,
        logical_plan=None,
        output_schema=None,
        column_lineage=[],
        optimizations=[],
        checks=[],
        fused_group_id="group_1",
        tags=[],
        description=None,
        fusion_strategy_override=None,
        materialization_strategy_override=None,
    )


def _make_fused_group(
    fused_sql: str | None = None,
    engine: str = "duckdb_engine",
    engine_type: str = "duckdb",
    exit_joints: list[str] | None = None,
) -> FusedGroup:
    """Create a minimal FusedGroup for testing."""
    return FusedGroup(
        id="group_1",
        joints=["my_sink"],
        engine=engine,
        engine_type=engine_type,
        adapters={"my_sink": "duckdb_adapter"},
        fused_sql=fused_sql,
        fusion_strategy="cte",
        fusion_result=None,
        resolved_sql=None,
        entry_joints=[],
        exit_joints=exit_joints or ["my_sink"],
        pushdown=None,
        residual=None,
        materialization_strategy_name="arrow",
    )


@pytest.fixture
def mock_adapter() -> _MockAdapter:
    return _MockAdapter()


@pytest.fixture
def registry(mock_adapter: _MockAdapter) -> PluginRegistry:
    """Create a registry with the mock adapter."""
    from rivet_core.builtins.arrow_catalog import ArrowComputeEnginePlugin

    reg = PluginRegistry()
    reg.register_engine_plugin(ArrowComputeEnginePlugin())
    reg.register_adapter(mock_adapter)
    # Register a mock compute engine instance
    reg._compute_engines["duckdb_engine"] = MagicMock()
    return reg


@pytest.fixture
def executor(registry: PluginRegistry) -> Executor:
    return Executor(registry=registry)


@pytest.fixture
def catalog_map() -> dict[str, CompiledCatalog]:
    return {
        "test_catalog": CompiledCatalog(
            name="test_catalog",
            type="duckdb",
            options={"path": ":memory:"},
        )
    }


@pytest.fixture
def upstream_materials() -> dict[str, MaterializedRef]:
    """Single upstream materialized table."""
    tbl = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    return {"upstream_transform": _SimpleRef(tbl)}


def test_sink_no_fused_sql_one_upstream_constructs_sql(
    executor: Executor,
    mock_adapter: _MockAdapter,
    catalog_map: dict[str, CompiledCatalog],
    upstream_materials: dict[str, MaterializedRef],
) -> None:
    """Sink with no fused SQL and one upstream should construct SELECT * FROM upstream."""
    group = _make_fused_group(fused_sql=None)
    exit_cj = _make_sink_joint()

    result = asyncio.run(
        executor._try_native_sql_write(group, exit_cj, catalog_map, upstream_materials)
    )

    assert result is True
    assert len(mock_adapter.write_dispatch_calls) == 1

    ctx = mock_adapter.write_dispatch_calls[0]
    assert ctx.fused_sql == "SELECT * FROM upstream_transform"
    assert ctx.target_table == "output_table"
    assert ctx.write_strategy == "replace"


def test_sink_no_fused_sql_one_upstream_ctx_has_input_tables(
    executor: Executor,
    mock_adapter: _MockAdapter,
    catalog_map: dict[str, CompiledCatalog],
    upstream_materials: dict[str, MaterializedRef],
) -> None:
    """NativeSqlWriteContext should include upstream tables for registration."""
    group = _make_fused_group(fused_sql=None)
    exit_cj = _make_sink_joint()

    asyncio.run(executor._try_native_sql_write(group, exit_cj, catalog_map, upstream_materials))

    ctx = mock_adapter.write_dispatch_calls[0]
    assert "upstream_transform" in ctx.input_tables
    assert ctx.input_tables["upstream_transform"].num_rows == 3


def test_sink_no_fused_sql_multiple_upstreams_returns_false(
    executor: Executor,
    mock_adapter: _MockAdapter,
    catalog_map: dict[str, CompiledCatalog],
) -> None:
    """Sink with no fused SQL and multiple upstreams should fall back to Arrow."""
    group = _make_fused_group(fused_sql=None)
    exit_cj = _make_sink_joint(upstream=["upstream_a", "upstream_b"])

    multi_materials: dict[str, MaterializedRef] = {
        "upstream_a": _SimpleRef(pa.table({"id": [1]})),
        "upstream_b": _SimpleRef(pa.table({"id": [2]})),
    }

    result = asyncio.run(
        executor._try_native_sql_write(group, exit_cj, catalog_map, multi_materials)
    )

    assert result is False
    assert len(mock_adapter.write_dispatch_calls) == 0


def test_sink_with_fused_sql_uses_existing_sql(
    executor: Executor,
    mock_adapter: _MockAdapter,
    catalog_map: dict[str, CompiledCatalog],
    upstream_materials: dict[str, MaterializedRef],
) -> None:
    """Sink with fused SQL should use that SQL directly, not construct from upstream.

    Preservation: when the group already has fused SQL (e.g. sink fused with
    upstream transform), the existing SQL must be passed through unchanged.
    Requirements: bugfix.md 3.1, 3.2
    """
    existing_sql = "SELECT id, value FROM upstream_transform WHERE id > 0"
    group = _make_fused_group(fused_sql=existing_sql)
    exit_cj = _make_sink_joint()

    result = asyncio.run(
        executor._try_native_sql_write(group, exit_cj, catalog_map, upstream_materials)
    )

    assert result is True
    assert len(mock_adapter.write_dispatch_calls) == 1

    ctx = mock_adapter.write_dispatch_calls[0]
    assert ctx.fused_sql == existing_sql
    assert ctx.target_table == "output_table"
    assert ctx.write_strategy == "replace"


def test_native_sql_write_exception_logs_warning(
    executor: Executor,
    catalog_map: dict[str, CompiledCatalog],
    upstream_materials: dict[str, MaterializedRef],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When _try_native_sql_write raises, _execute_group_success logs a warning and falls back.

    Requirements: bugfix.md 2.2
    """
    import logging
    import time
    from unittest.mock import AsyncMock, patch

    group = _make_fused_group(fused_sql=None)
    exit_cj = _make_sink_joint()

    # Create a mock result for _execute_fused_group fallback
    mock_result_ref = _SimpleRef(pa.table({"id": [1, 2, 3]}))

    # Make _try_native_sql_write raise an exception, and mock _execute_fused_group
    # to return a valid result so the method can complete
    with (
        patch.object(
            executor,
            "_try_native_sql_write",
            new_callable=AsyncMock,
            side_effect=RuntimeError("simulated native write failure"),
        ),
        patch.object(
            executor,
            "_execute_fused_group",
            new_callable=AsyncMock,
            return_value=(mock_result_ref, None),
        ),
        caplog.at_level(logging.WARNING, logger="rivet_core.executor"),
    ):
        joint_map = {"my_sink": exit_cj}
        mat_map: dict[str, list[Any]] = {}
        failed_joints: set[str] = set()
        joint_results: list[Any] = []
        group_results: list[Any] = []

        asyncio.run(
            executor._execute_group_success(
                group=group,
                joint_map=joint_map,
                catalog_map=catalog_map,
                mat_map=mat_map,
                materials=dict(upstream_materials),
                failed_joints=failed_joints,
                joint_results=joint_results,
                group_results=group_results,
                fail_fast=False,
                step_start=time.monotonic(),
                engine_start=time.monotonic(),
            )
        )

    # Verify warning was logged with the sink name and exc_info
    warning_records = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "native_sql_write failed" in r.message
    ]
    assert len(warning_records) == 1
    assert "my_sink" in warning_records[0].message
    assert warning_records[0].exc_info is not None
