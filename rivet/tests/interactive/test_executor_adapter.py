"""Tests for adapter dispatch in Executor._read_sources_into() (task 4.1, 4.2).

Validates Requirements 7.1, 7.2, 7.3, 7.4, 10.4.

# Feature: repl-query-planner, Property 11: Adapter dispatch takes precedence over source.read()
# Validates: Requirements 7.1, 7.4
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pyarrow
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.compiler import (
    CompiledCatalog,
    CompiledJoint,
)
from rivet_core.errors import ExecutionError
from rivet_core.executor import Executor
from rivet_core.optimizer import FusedGroup
from rivet_core.plugins import PluginRegistry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cj(
    name: str,
    joint_type: str = "source",
    upstream: list[str] | None = None,
    fused_group_id: str | None = None,
    **kwargs: object,
) -> CompiledJoint:
    defaults = dict(
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
        checks=[],
        fused_group_id=fused_group_id,
        tags=[],
        description=None,
        fusion_strategy_override=None,
        materialization_strategy_override=None,
    )
    defaults.update(kwargs)
    return CompiledJoint(name=name, type=joint_type, **defaults)  # type: ignore[arg-type]


def _group(
    group_id: str,
    joints: list[str],
    fused_sql: str | None = None,
    entry_joints: list[str] | None = None,
    exit_joints: list[str] | None = None,
) -> FusedGroup:
    return FusedGroup(
        id=group_id,
        joints=joints,
        engine="duckdb",
        engine_type="duckdb",
        adapters={j: None for j in joints},
        fused_sql=fused_sql,
        fusion_strategy="cte",
        fusion_result=None,
        entry_joints=entry_joints or [joints[0]],
        exit_joints=exit_joints or [joints[-1]],
        residual=None,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestAdapterDispatchPrecedence:
    """Requirement 7.1, 7.4: adapter dispatch takes precedence over source.read()."""

    def test_adapter_called_instead_of_source_read(self):
        """When cj.adapter is set and adapter is registered, read_dispatch is used."""
        arrow_table = pyarrow.table({"x": [1, 2, 3]})
        mock_mat = MagicMock()
        mock_mat.materialized_ref = MagicMock()
        mock_mat.to_arrow.return_value = arrow_table

        mock_adapter = MagicMock()
        mock_adapter.read_dispatch.return_value = mock_mat

        mock_engine = MagicMock()

        registry = PluginRegistry()
        registry._adapters[("duckdb", "unity")] = mock_adapter
        registry._compute_engines["duckdb"] = mock_engine

        source_joint = _cj(
            "users", joint_type="source",
            catalog="prod", catalog_type="unity",
            adapter="duckdb:unity", table="users",
            fused_group_id="g1",
        )
        query_joint = _cj(
            "__query", joint_type="sql",
            upstream=["users"],
            sql="SELECT * FROM users",
            fused_group_id="g1",
        )
        group = _group("g1", ["users", "__query"], fused_sql="SELECT * FROM users",
                        entry_joints=["users"], exit_joints=["__query"])
        catalog_map = {"prod": CompiledCatalog(name="prod", type="unity", options={})}
        joint_map = {"users": source_joint, "__query": query_joint}

        executor = Executor(registry=registry)
        input_tables: dict[str, pyarrow.Table] = {}
        asyncio.run(executor._read_sources_into(
            input_tables,
            group=group,
            joint_map=joint_map,
            catalog_map=catalog_map,
        ))

        mock_adapter.read_dispatch.assert_called_once()
        # source.read() should NOT have been called — no source plugin registered
        assert "unity" not in registry._sources

    def test_null_adapter_falls_through_to_source_read(self):
        """When cj.adapter is None, source.read() is used."""
        arrow_table = pyarrow.table({"x": [1]})
        mock_mat = MagicMock()
        mock_mat.materialized_ref = MagicMock()
        mock_mat.to_arrow.return_value = arrow_table

        mock_source = MagicMock()
        mock_source.read.return_value = mock_mat

        registry = PluginRegistry()
        registry._sources["filesystem"] = mock_source

        source_joint = _cj(
            "data", joint_type="source",
            catalog="local", catalog_type="filesystem",
            adapter=None, table="data.csv",
            fused_group_id="g1",
        )
        query_joint = _cj(
            "__query", joint_type="sql",
            upstream=["data"],
            sql="SELECT * FROM data",
            fused_group_id="g1",
        )
        group = _group("g1", ["data", "__query"], fused_sql="SELECT * FROM data",
                        entry_joints=["data"], exit_joints=["__query"])
        catalog_map = {"local": CompiledCatalog(name="local", type="filesystem", options={})}
        joint_map = {"data": source_joint, "__query": query_joint}

        executor = Executor(registry=registry)
        input_tables: dict[str, pyarrow.Table] = {}
        asyncio.run(executor._read_sources_into(
            input_tables,
            group=group,
            joint_map=joint_map,
            catalog_map=catalog_map,
        ))

        mock_source.read.assert_called_once()


class TestAdapterDeferredMaterializedRef:
    """Requirement 7.2: deferred MaterializedRef correctly calls to_arrow()."""

    def test_to_arrow_called_on_materialized_ref(self):
        arrow_table = pyarrow.table({"col": [10, 20]})
        mock_mat = MagicMock()
        mock_mat.materialized_ref = MagicMock()  # non-None
        mock_mat.to_arrow.return_value = arrow_table

        mock_adapter = MagicMock()
        mock_adapter.read_dispatch.return_value = mock_mat

        registry = PluginRegistry()
        registry._adapters[("duckdb", "unity")] = mock_adapter
        registry._compute_engines["duckdb"] = MagicMock()

        source_joint = _cj(
            "t", joint_type="source",
            catalog="prod", catalog_type="unity",
            adapter="duckdb:unity", table="t",
            fused_group_id="g1",
        )
        query_joint = _cj(
            "__query", joint_type="sql",
            upstream=["t"], sql="SELECT * FROM t",
            fused_group_id="g1",
        )
        group = _group("g1", ["t", "__query"], fused_sql="SELECT * FROM t",
                        entry_joints=["t"], exit_joints=["__query"])
        catalog_map = {"prod": CompiledCatalog(name="prod", type="unity", options={})}
        joint_map = {"t": source_joint, "__query": query_joint}

        executor = Executor(registry=registry)
        input_tables: dict[str, pyarrow.Table] = {}
        asyncio.run(executor._read_sources_into(
            input_tables,
            group=group,
            joint_map=joint_map,
            catalog_map=catalog_map,
        ))

        mock_mat.to_arrow.assert_called_once()


class TestMissingAdapterError:
    """Requirements 7.3, 10.4: missing adapter produces actionable RVT-501 error."""

    def test_no_adapter_no_source_raises_rvt501(self):
        """When adapter is specified but not found and no source plugin, raise RVT-501."""
        registry = PluginRegistry()
        # No adapter registered, no source plugin registered

        source_joint = _cj(
            "users", joint_type="source",
            catalog="prod", catalog_type="unity",
            adapter="duckdb:unity", table="users",
            fused_group_id="g1",
        )
        group = _group("g1", ["users"], entry_joints=["users"], exit_joints=["users"])
        catalog_map = {"prod": CompiledCatalog(name="prod", type="unity", options={})}
        joint_map = {"users": source_joint}

        executor = Executor(registry=registry)
        with pytest.raises(ExecutionError) as exc_info:
            input_tables: dict[str, pyarrow.Table] = {}
            asyncio.run(executor._read_sources_into(
                input_tables,
                group=group,
                joint_map=joint_map,
                catalog_map=catalog_map,
            ))

        err = exc_info.value.error
        assert err.code == "RVT-501"
        assert "unity" in err.message
        assert "adapter" in err.message.lower()

    def test_adapter_specified_source_read_fails_actionable_error(self):
        """When adapter not found but source plugin exists and fails, error mentions adapter."""
        mock_source = MagicMock()
        mock_source.read.side_effect = RuntimeError("connection refused")

        registry = PluginRegistry()
        registry._sources["unity"] = mock_source
        # No adapter registered for (duckdb, unity)

        source_joint = _cj(
            "users", joint_type="source",
            catalog="prod", catalog_type="unity",
            adapter="duckdb:unity", table="users",
            fused_group_id="g1",
        )
        group = _group("g1", ["users"], entry_joints=["users"], exit_joints=["users"])
        catalog_map = {"prod": CompiledCatalog(name="prod", type="unity", options={})}
        joint_map = {"users": source_joint}

        executor = Executor(registry=registry)
        with pytest.raises(ExecutionError) as exc_info:
            input_tables: dict[str, pyarrow.Table] = {}
            asyncio.run(executor._read_sources_into(
                input_tables,
                group=group,
                joint_map=joint_map,
                catalog_map=catalog_map,
            ))

        err = exc_info.value.error
        assert err.code == "RVT-501"
        assert "unity" in err.message
        assert "adapter" in err.message.lower()


# ---------------------------------------------------------------------------
# Property 11: Adapter dispatch takes precedence over source.read()
# Feature: repl-query-planner, Property 11
# Validates: Requirements 7.1, 7.4
# ---------------------------------------------------------------------------

_ident = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)
_catalog_type = st.sampled_from(["unity", "glue", "databricks", "postgres", "custom"])
_engine_type = st.sampled_from(["duckdb", "polars", "pyspark"])


class TestAdapterDispatchPrecedenceProperty:
    """Property 11: Adapter dispatch takes precedence over source.read().

    For any source joint whose CompiledJoint has a non-null adapter field,
    the Executor SHALL use the adapter's read_dispatch() method to obtain
    the data, rather than calling source.read().
    """

    @given(
        joint_name=_ident,
        catalog_name=_ident,
        catalog_type=_catalog_type,
        engine_type=_engine_type,
        table_name=_ident,
    )
    @settings(max_examples=100)
    def test_adapter_dispatch_called_not_source_read(
        self,
        joint_name: str,
        catalog_name: str,
        catalog_type: str,
        engine_type: str,
        table_name: str,
    ) -> None:
        """For any source joint with non-null adapter, read_dispatch() is called
        and source.read() is NOT called."""
        arrow_table = pyarrow.table({"col": [1, 2, 3]})
        mock_mat = MagicMock()
        mock_mat.materialized_ref = MagicMock()
        mock_mat.to_arrow.return_value = arrow_table

        mock_adapter = MagicMock()
        mock_adapter.read_dispatch.return_value = mock_mat

        mock_source = MagicMock()  # registered but should NOT be called

        registry = PluginRegistry()
        registry._adapters[(engine_type, catalog_type)] = mock_adapter
        registry._compute_engines[engine_type] = MagicMock()
        registry._sources[catalog_type] = mock_source

        adapter_str = f"{engine_type}:{catalog_type}"
        source_joint = _cj(
            joint_name, joint_type="source",
            catalog=catalog_name, catalog_type=catalog_type,
            adapter=adapter_str, table=table_name,
            engine=engine_type,
            fused_group_id="g1",
        )
        query_joint = _cj(
            "__query", joint_type="sql",
            upstream=[joint_name],
            sql=f"SELECT * FROM {joint_name}",
            fused_group_id="g1",
        )
        group = _group(
            "g1", [joint_name, "__query"],
            fused_sql=f"SELECT * FROM {joint_name}",
            entry_joints=[joint_name],
            exit_joints=["__query"],
        )
        catalog_map = {catalog_name: CompiledCatalog(name=catalog_name, type=catalog_type, options={})}
        joint_map = {joint_name: source_joint, "__query": query_joint}

        executor = Executor(registry=registry)
        input_tables: dict[str, pyarrow.Table] = {}
        asyncio.run(executor._read_sources_into(
            input_tables,
            group=group,
            joint_map=joint_map,
            catalog_map=catalog_map,
        ))

        # Adapter's read_dispatch must have been called
        mock_adapter.read_dispatch.assert_called_once()
        # source.read() must NOT have been called
        mock_source.read.assert_not_called()
