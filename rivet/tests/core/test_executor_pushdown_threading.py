"""Tests for pushdown threading in Executor._read_sources_into() (task 2.1).

Validates Requirement 2.1: executor passes group.pushdown to adapter.read_dispatch
and handles AdapterPushdownResult return type.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pyarrow

from rivet_core.compiler import CompiledCatalog, CompiledJoint
from rivet_core.executor import Executor
from rivet_core.optimizer import (
    EMPTY_RESIDUAL,
    AdapterPushdownResult,
    CastPushdownResult,
    FusedGroup,
    LimitPushdownResult,
    PredicatePushdownResult,
    ProjectionPushdownResult,
    PushdownPlan,
)
from rivet_core.plugins import PluginRegistry


def _cj(name: str, **kwargs: object) -> CompiledJoint:
    defaults = dict(
        type="source",
        catalog=None,
        catalog_type=None,
        engine="duckdb",
        engine_resolution="project_default",
        adapter=None,
        sql=None,
        sql_translated=None,
        sql_resolved=None,
        sql_dialect=None,
        engine_dialect=None,
        upstream=[],
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
        fused_group_id=None,
        tags=[],
        description=None,
        fusion_strategy_override=None,
        materialization_strategy_override=None,
    )
    defaults.update(kwargs)
    return CompiledJoint(name=name, **defaults)  # type: ignore[arg-type]


def _group(joints: list[str], pushdown: PushdownPlan | None = None) -> FusedGroup:
    return FusedGroup(
        id="g1",
        joints=joints,
        engine="duckdb",
        engine_type="duckdb",
        adapters={j: None for j in joints},
        fused_sql=None,
        entry_joints=[joints[0]],
        exit_joints=[joints[-1]],
        pushdown=pushdown,
    )


def _make_pushdown() -> PushdownPlan:
    return PushdownPlan(
        predicates=PredicatePushdownResult(pushed=[], residual=[]),
        projections=ProjectionPushdownResult(pushed_columns=["a", "b"], reason=None),
        limit=LimitPushdownResult(pushed_limit=10, residual_limit=None, reason=None),
        casts=CastPushdownResult(pushed=[], residual=[]),
    )


class TestPushdownThreading:
    """Requirement 2.1: group.pushdown is passed to adapter.read_dispatch."""

    def test_pushdown_passed_to_read_dispatch(self):
        pushdown = _make_pushdown()
        arrow_table = pyarrow.table({"a": [1], "b": [2]})
        mock_mat = MagicMock()
        mock_mat.materialized_ref = MagicMock()
        mock_mat.to_arrow.return_value = arrow_table

        mock_adapter = MagicMock()
        mock_adapter.read_dispatch.return_value = mock_mat

        registry = PluginRegistry()
        registry._adapters[("duckdb", "unity")] = mock_adapter
        registry._compute_engines["duckdb"] = MagicMock()

        source = _cj("src", catalog="cat", catalog_type="unity",
                      adapter="duckdb:unity", table="t", fused_group_id="g1")
        group = _group(["src"], pushdown=pushdown)

        executor = Executor(registry=registry)
        tables: dict[str, pyarrow.Table] = {}
        asyncio.run(executor._read_sources_into(
            tables, group=group,
            joint_map={"src": source},
            catalog_map={"cat": CompiledCatalog(name="cat", type="unity", options={})},
        ))

        _, kwargs = mock_adapter.read_dispatch.call_args
        # pushdown should be the 4th positional arg (or keyword)
        call_args = mock_adapter.read_dispatch.call_args
        assert call_args[0][2] is not None  # joint arg
        assert pushdown in call_args[0] or call_args[0][3] is pushdown

    def test_none_pushdown_passed_when_group_has_no_pushdown(self):
        arrow_table = pyarrow.table({"x": [1]})
        mock_mat = MagicMock()
        mock_mat.materialized_ref = MagicMock()
        mock_mat.to_arrow.return_value = arrow_table

        mock_adapter = MagicMock()
        mock_adapter.read_dispatch.return_value = mock_mat

        registry = PluginRegistry()
        registry._adapters[("duckdb", "unity")] = mock_adapter
        registry._compute_engines["duckdb"] = MagicMock()

        source = _cj("src", catalog="cat", catalog_type="unity",
                      adapter="duckdb:unity", table="t", fused_group_id="g1")
        group = _group(["src"], pushdown=None)

        executor = Executor(registry=registry)
        tables: dict[str, pyarrow.Table] = {}
        asyncio.run(executor._read_sources_into(
            tables, group=group,
            joint_map={"src": source},
            catalog_map={"cat": CompiledCatalog(name="cat", type="unity", options={})},
        ))

        call_args = mock_adapter.read_dispatch.call_args[0]
        assert call_args[3] is None

    def test_adapter_pushdown_result_material_extracted(self):
        """When adapter returns AdapterPushdownResult, material is extracted."""
        arrow_table = pyarrow.table({"a": [1]})
        mock_mat = MagicMock()
        mock_mat.materialized_ref = MagicMock()
        mock_mat.to_arrow.return_value = arrow_table

        result = AdapterPushdownResult(material=mock_mat, residual=EMPTY_RESIDUAL)

        mock_adapter = MagicMock()
        mock_adapter.read_dispatch.return_value = result

        registry = PluginRegistry()
        registry._adapters[("duckdb", "unity")] = mock_adapter
        registry._compute_engines["duckdb"] = MagicMock()

        source = _cj("src", catalog="cat", catalog_type="unity",
                      adapter="duckdb:unity", table="t", fused_group_id="g1")
        group = _group(["src"])

        executor = Executor(registry=registry)
        tables: dict[str, pyarrow.Table] = {}
        asyncio.run(executor._read_sources_into(
            tables, group=group,
            joint_map={"src": source},
            catalog_map={"cat": CompiledCatalog(name="cat", type="unity", options={})},
        ))

        assert "src" in tables
        assert tables["src"].equals(arrow_table)

    def test_legacy_material_return_still_works(self):
        """When adapter returns plain Material (not AdapterPushdownResult), it still works."""
        arrow_table = pyarrow.table({"x": [1]})
        mock_mat = MagicMock()
        mock_mat.materialized_ref = MagicMock()
        mock_mat.to_arrow.return_value = arrow_table

        mock_adapter = MagicMock()
        mock_adapter.read_dispatch.return_value = mock_mat

        registry = PluginRegistry()
        registry._adapters[("duckdb", "unity")] = mock_adapter
        registry._compute_engines["duckdb"] = MagicMock()

        source = _cj("src", catalog="cat", catalog_type="unity",
                      adapter="duckdb:unity", table="t", fused_group_id="g1")
        group = _group(["src"])

        executor = Executor(registry=registry)
        tables: dict[str, pyarrow.Table] = {}
        asyncio.run(executor._read_sources_into(
            tables, group=group,
            joint_map={"src": source},
            catalog_map={"cat": CompiledCatalog(name="cat", type="unity", options={})},
        ))

        assert "src" in tables
        assert tables["src"].equals(arrow_table)
