"""Engine dispatch functions for the executor.

Extracted from ``Executor._dispatch_to_engine``, ``Executor._execute_via_plugin``,
``Executor._execute_temp_view_via_plugin``, ``Executor._resolve_cross_joint``,
``Executor._execute_fused_group``, ``Executor._materialize_result``, and
``Executor._execute_python_joint``.

``execute_via_plugin`` and ``execute_fused_group`` receive a ``read_sources_fn``
callable parameter to avoid cross-phase imports from ``source_reading.py``.

This is Level 3 of the executor package dependency hierarchy — it imports
from ``models`` (Level 1) and ``helpers/`` (Level 2) only within the
executor package.  It does NOT import from ``pipeline.py`` or other phase
modules.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import re
import sys
import traceback
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any

import pyarrow

from rivet_core.compiler import CompiledCatalog, CompiledJoint
from rivet_core.context import RivetContext
from rivet_core.errors import ExecutionError, RivetError
from rivet_core.executor.helpers.python_helpers import _normalize_python_result
from rivet_core.executor.helpers.utils import _extract_table_references
from rivet_core.models import ComputeEngine, Material
from rivet_core.optimizer import FusedGroup, ResidualPlan
from rivet_core.plugins import (
    ComputeEnginePlugin,
    CrossJointContext,
    PluginRegistry,
    UpstreamResolution,
)
from rivet_core.sql_resolver import resolve_execution_sql
from rivet_core.stats import StatsCollector
from rivet_core.strategies import (
    MaterializationContext,
    MaterializationStrategy,
    MaterializedRef,
)

# Type alias for the read_sources_fn callable parameter.
# Signature matches source_reading.read_sources_into.
ReadSourcesFn = Callable[
    ...,
    Coroutine[Any, Any, ResidualPlan | None],
]


async def dispatch_to_engine(
    plugin: ComputeEnginePlugin,
    engine_instance: ComputeEngine | None,
    sql: str,
    input_tables: dict[str, pyarrow.Table],
    group: FusedGroup,
) -> pyarrow.Table:
    """Call plugin.execute_sql, wrapping non-ExecutionError failures as RVT-503."""
    assert engine_instance is not None, "engine_instance required for SQL execution"
    try:
        return await asyncio.to_thread(plugin.execute_sql, engine_instance, sql, input_tables)
    except ExecutionError:
        raise
    except Exception as exc:
        raise ExecutionError(  # noqa: B904
            RivetError(
                code="RVT-503",
                message=f"Plugin execute_sql failed for group '{group.id}': {exc}",
                context={
                    "engine_type": group.engine_type,
                    "group_id": group.id,
                    "sql": (sql or "")[:200],
                },
                remediation="Check engine connectivity and SQL syntax.",
            )
        )


async def execute_via_plugin(
    group: FusedGroup,
    materials: dict[str, pyarrow.Table],
    joint_map: dict[str, CompiledJoint],
    catalog_map: dict[str, CompiledCatalog] | None,
    plugin: ComputeEnginePlugin,
    registry: PluginRegistry,
    read_sources_fn: ReadSourcesFn,
    stats_collector: StatsCollector | None = None,
    ref_materials: dict[str, MaterializedRef] | None = None,
) -> tuple[pyarrow.Table, ResidualPlan | None]:
    """Execute via plugin's execute_sql with cross-joint adapter resolution.

    Handles both CTE and temp_view fusion strategies.
    Resolves upstream references, builds input_tables, reads sources,
    and delegates SQL execution to the engine plugin.
    Returns (result_table, adapter_residual).
    Raises RVT-503 if plugin.execute_sql fails.
    """
    engine_instance = registry.get_compute_engine(group.engine) if registry else None

    # Build input_tables from upstream materials and source reads
    input_tables: dict[str, pyarrow.Table] = {}
    has_materialized_inputs = False

    # Checkpoint sources handled by CTE injection already have
    # engine-native references in the resolved SQL — don't count
    # them as materialized inputs (which would force fallback to
    # unresolved SQL).
    checkpoint_cte_names: set[str] = (
        set(group.checkpoint_sources) if group.checkpoint_sources else set()
    )

    for jn in group.joints:
        cj = joint_map.get(jn)
        if not cj:
            continue
        for up in cj.upstream:
            if up in materials:
                input_tables[up] = materials[up]
                if up not in checkpoint_cte_names:
                    has_materialized_inputs = True

    # Add cross-wave materialized tables referenced in fused SQL
    # When joints are fused into CTEs, the fused SQL may reference joints
    # from previous waves that aren't in any individual joint's upstream list.
    # Extract all table references from all possible SQL sources and check if
    # they're available in materials from previous waves.

    # Check all possible SQL sources
    sql_sources: list[str] = []
    if group.fusion_result:
        if group.fusion_result.fused_sql:
            sql_sources.append(group.fusion_result.fused_sql)
        if group.fusion_result.resolved_fused_sql:
            sql_sources.append(group.fusion_result.resolved_fused_sql)
    if group.fused_sql:
        sql_sources.append(group.fused_sql)
    if group.resolved_sql:
        sql_sources.append(group.resolved_sql)

    # Also check individual joint SQL for references
    for jn in group.joints:
        cj = joint_map.get(jn)
        if cj:
            if cj.sql:
                sql_sources.append(cj.sql)
            if cj.sql_translated:
                sql_sources.append(cj.sql_translated)
            if cj.sql_resolved:
                sql_sources.append(cj.sql_resolved)

    # Extract all referenced tables from all SQL sources
    referenced_tables = _extract_table_references(sql_sources)

    # Add any referenced tables that are in materials
    for table_name in referenced_tables:
        if table_name in materials and table_name not in input_tables:
            input_tables[table_name] = materials[table_name]
            has_materialized_inputs = True

    # Read source joints into input_tables.
    # When a reference resolver has rewritten the fused SQL (resolved_sql
    # is set), table references are fully-qualified catalog names and the
    # engine does not need adapter-read data in input_tables.  Skip adapter
    # reads for fused sources to avoid redundant queries.
    group_has_resolved_sql = group.resolved_sql is not None or (
        group.fusion_result is not None and group.fusion_result.resolved_fused_sql is not None
    )
    skip_source_reads = group_has_resolved_sql and len(group.joints) > 1
    adapter_residual = await read_sources_fn(
        input_tables,
        group,
        joint_map,
        catalog_map,
        stats_collector=stats_collector,
        skip_fused_sources=skip_source_reads,
        ref_materials=ref_materials,
    )

    adapter_read_sources = {
        jn
        for jn in group.joints
        if jn in input_tables and joint_map.get(jn) and joint_map[jn].type == "source"
    }

    # Standalone source groups: when all joints are sources that were
    # read into input_tables (via adapter or fallback source plugin),
    # return the data directly.  The source SQL (from YAML columns/filter
    # or explicit SQL annotation) was used only for LogicalPlan extraction
    # and pushdown — it references catalog table names that don't exist in
    # the engine's namespace.
    all_sources_read = (
        all(
            joint_map.get(jn) is not None and joint_map[jn].type == "source" and jn in input_tables
            for jn in group.joints
        )
        if group.joints
        else False
    )
    if all_sources_read:
        exit_jn = group.exit_joints[-1] if group.exit_joints else group.joints[-1]
        if exit_jn in input_tables:
            return input_tables[exit_jn], adapter_residual

    # Handle temp_view strategy: execute intermediate statements, then final select
    if group.fusion_strategy == "temp_view" and group.fusion_result:
        return await execute_temp_view_via_plugin(
            group,
            input_tables,
            plugin,
            engine_instance,
            adapter_read_sources,
        ), adapter_residual

    # CTE strategy
    sql = resolve_execution_sql(
        group,
        joint_map,
        adapter_read_sources,
        has_materialized_inputs=has_materialized_inputs,
    )

    if not sql:
        if input_tables:
            return next(iter(input_tables.values())), adapter_residual
        return pyarrow.table({}), adapter_residual

    # Source-only groups with no input tables: return empty table
    if not input_tables and all(
        joint_map.get(jn) is not None and joint_map[jn].type == "source"
        for jn in group.joints
        if jn in joint_map
    ):
        return pyarrow.table({}), adapter_residual

    return await dispatch_to_engine(
        plugin, engine_instance, sql, input_tables, group
    ), adapter_residual


# CLEANUP-RISK: execute_temp_view_via_plugin (complexity 13) — plugin dispatch with error handling; refactoring risks changing error messages/types
async def execute_temp_view_via_plugin(
    group: FusedGroup,
    input_tables: dict[str, pyarrow.Table],
    plugin: ComputeEnginePlugin,
    engine_instance: ComputeEngine | None,
    adapter_read_sources: set[str] | None = None,
) -> pyarrow.Table:
    """Execute temp_view strategy by running intermediate statements through plugin."""
    assert engine_instance is not None, "engine_instance required for temp_view execution"

    fr = group.fusion_result
    assert fr is not None, f"fusion_result must not be None for group '{group.id}'"
    statements = fr.resolved_statements or fr.statements
    final_select = fr.resolved_final_select or fr.final_select

    # Rewrite statements for source joints read into input_tables: replace
    # their view SQL with SELECT * FROM <joint_name> so the engine reads
    # from the registered table.
    if adapter_read_sources and statements:
        rewritten_stmts: list[str] = []
        for stmt in statements:
            m = re.match(
                r"CREATE\s+TEMPORARY\s+VIEW\s+(\w+)\s+AS\s*\((.+)\)",
                stmt,
                re.IGNORECASE | re.DOTALL,
            )
            if m and m.group(1) in adapter_read_sources:
                view_name = m.group(1)
                rewritten_stmts.append(
                    f"CREATE TEMPORARY VIEW {view_name} AS (SELECT * FROM {view_name})"
                )
            else:
                rewritten_stmts.append(stmt)
        statements = rewritten_stmts

    if not statements:
        if not final_select:
            if input_tables:
                return next(iter(input_tables.values()))
            return pyarrow.table({})
        return await dispatch_to_engine(
            plugin,
            engine_instance,
            final_select,
            input_tables,
            group,
        )

    # Execute intermediate view statements, storing results as input tables
    created_views: list[str] = []
    try:
        for stmt in statements:
            m = re.match(
                r"CREATE\s+TEMPORARY\s+VIEW\s+(\w+)\s+AS\s*\((.+)\)",
                stmt,
                re.IGNORECASE | re.DOTALL,
            )
            if m:
                view_name = m.group(1)
                view_sql = m.group(2)
                result = await dispatch_to_engine(
                    plugin,
                    engine_instance,
                    view_sql,
                    input_tables,
                    group,
                )
                input_tables[view_name] = result
                created_views.append(view_name)

        return await dispatch_to_engine(
            plugin,
            engine_instance,
            final_select,
            input_tables,
            group,
        )
    finally:
        for view_name in created_views:
            input_tables.pop(view_name, None)


def resolve_cross_joint(
    upstream_ref: MaterializedRef,
    consumer_group: FusedGroup,
    producer_joint_name: str,
    consumer_joint_name: str,
    joint_map: dict[str, CompiledJoint],
    group_map: dict[str, FusedGroup],
    registry: PluginRegistry | None,
) -> UpstreamResolution:
    """Resolve how the consumer engine accesses upstream data."""
    producer_cj = joint_map.get(producer_joint_name)
    consumer_cj = joint_map.get(consumer_joint_name)

    # Determine producer engine type via its fused group
    producer_engine_type: str | None = None
    if producer_cj and producer_cj.fused_group_id:
        producer_group = group_map.get(producer_cj.fused_group_id)
        if producer_group:
            producer_engine_type = producer_group.engine_type

    consumer_engine_type = consumer_group.engine_type

    # Same engine type or unknown producer → default arrow passthrough
    if producer_engine_type is None or producer_engine_type == consumer_engine_type:
        return UpstreamResolution(strategy="arrow_passthrough")

    # Look up registered cross-joint adapter
    adapter = (
        registry.get_cross_joint_adapter(consumer_engine_type, producer_engine_type)
        if registry
        else None
    )

    if adapter is None:
        return UpstreamResolution(strategy="arrow_passthrough")

    # Adapter found → delegate resolution
    engine_instance = registry.get_compute_engine(consumer_group.engine) if registry else None
    ctx = CrossJointContext(
        producer_joint_name=producer_joint_name,
        consumer_joint_name=consumer_joint_name,
        producer_catalog_type=producer_cj.catalog_type if producer_cj else None,
        producer_table=producer_cj.table if producer_cj else None,
        consumer_catalog_type=consumer_cj.catalog_type if consumer_cj else None,
    )
    return adapter.resolve_upstream(upstream_ref, engine_instance, ctx)


async def execute_fused_group(
    group: FusedGroup,
    materials: dict[str, pyarrow.Table],
    joint_map: dict[str, CompiledJoint],
    catalog_map: dict[str, CompiledCatalog] | None,
    registry: PluginRegistry,
    materialization_strategies: dict[str, MaterializationStrategy],
    read_sources_fn: ReadSourcesFn,
    ref_materials: dict[str, MaterializedRef] | None = None,
    stats_collector: StatsCollector | None = None,
) -> tuple[MaterializedRef, ResidualPlan | None]:
    """Dispatch a fused group for execution and materialize the result.

    Single dispatch path: all non-Python groups go through plugin.execute_sql.
    Python joints are dispatched to execute_python_joint.
    Returns a (MaterializedRef, adapter_residual) tuple.
    """
    # PythonJoint groups are always standalone (single joint)
    if len(group.joints) == 1:
        cj = joint_map.get(group.joints[0])
        if cj and cj.type == "python":
            return await execute_python_joint(
                cj, ref_materials or {}, registry, None, materialization_strategies
            ), None

    # Look up engine plugin — required for all engine types
    plugin = registry.get_engine_plugin(group.engine_type) if registry else None
    if plugin is None:
        raise ExecutionError(
            RivetError(
                code="RVT-501",
                message=f"No plugin registered for engine type '{group.engine_type}'",
                context={
                    "engine_type": group.engine_type,
                    "group_id": group.id,
                    "joints": group.joints,
                },
                remediation=f"Register a ComputeEnginePlugin for '{group.engine_type}'.",
            )
        )

    # Single dispatch path: resolve upstream, build input_tables, call execute_sql
    result_table, adapter_residual = await execute_via_plugin(
        group,
        materials,
        joint_map,
        catalog_map,
        plugin,
        registry,
        read_sources_fn,
        stats_collector=stats_collector,
        ref_materials=ref_materials,
    )
    return materialize_result(result_table, group, materialization_strategies), adapter_residual


def materialize_result(
    table: pyarrow.Table,
    group: FusedGroup,
    materialization_strategies: dict[str, MaterializationStrategy],
) -> MaterializedRef:
    """Materialize a pyarrow.Table using the group's materialization strategy."""
    exit_joint = group.exit_joints[-1] if group.exit_joints else group.joints[-1]
    strategy = (
        materialization_strategies.get(group.materialization_strategy_name)
        or materialization_strategies["arrow"]
    )
    return strategy.materialize(
        table,
        MaterializationContext(
            joint_name=exit_joint,
            strategy_name=group.materialization_strategy_name,
            options={},
        ),
    )


async def execute_python_joint(
    cj: CompiledJoint,
    materials: dict[str, MaterializedRef],
    registry: PluginRegistry,
    project_root: Path | None,
    materialization_strategies: dict[str, MaterializationStrategy],
) -> MaterializedRef:
    """Execute a PythonJoint by calling its referenced function.

    Builds Material inputs from upstream MaterializedRef directly (no
    materialize() round-trip), supports single-input shorthand,
    optional RivetContext, async functions, and normalizes return to Arrow.
    Returns a MaterializedRef.
    """
    # Import the callable — temporarily add project root to sys.path
    func_path = cj.function or ""
    mod_path, func_name = func_path.rsplit(":", 1)
    root_str = str(project_root) if project_root else None
    added = False
    try:
        if root_str and root_str not in sys.path:
            sys.path.insert(0, root_str)
            added = True
        mod = importlib.import_module(mod_path)
        func = getattr(mod, func_name)
    except Exception as exc:
        raise ExecutionError(  # noqa: B904
            RivetError(
                code="RVT-751",
                message=f"PythonJoint '{cj.name}' failed to import '{func_path}': {exc}",
                context={
                    "joint": cj.name,
                    "function": func_path,
                    "traceback": traceback.format_exc(),
                },
                remediation="Ensure the function path is importable.",
            )
        )
    finally:
        if added and root_str:
            try:
                sys.path.remove(root_str)
            except ValueError:
                pass

    # Build Material inputs from upstream — use MaterializedRef directly
    inputs: dict[str, Material] = {}
    for up in cj.upstream:
        ref = materials.get(up)
        if ref is not None:
            inputs[up] = Material(
                name=up, catalog=cj.catalog or "", materialized_ref=ref, state="materialized"
            )

    # Determine if function accepts RivetContext
    sig = inspect.signature(func)
    params = list(sig.parameters.values())
    wants_context = any(
        p.annotation is RivetContext
        or (isinstance(p.annotation, str) and "RivetContext" in p.annotation)
        for p in params
    )

    # Build call args
    try:
        if len(cj.upstream) == 1 and len(inputs) == 1:
            # Single-input shorthand
            single_material = next(iter(inputs.values()))
            args: tuple[Any, ...] = (single_material,)
        else:
            args = (inputs,)

        kwargs: dict[str, Any] = {}
        if wants_context:
            kwargs["context"] = RivetContext(joint_name=cj.name)

        # Support async functions
        if inspect.iscoroutinefunction(func):
            result = await func(*args, **kwargs)
        else:
            result = await asyncio.to_thread(func, *args, **kwargs)
    except ExecutionError:
        raise
    except Exception as exc:
        raise ExecutionError(  # noqa: B904
            RivetError(
                code="RVT-751",
                message=f"PythonJoint '{cj.name}' raised an exception: {exc}",
                context={
                    "joint": cj.name,
                    "function": func_path,
                    "upstream": cj.upstream,
                    "traceback": traceback.format_exc(),
                },
                remediation="Fix the Python function or check upstream data.",
            )
        )

    # Normalize return to Material, then extract Arrow table and materialize
    material = _normalize_python_result(cj.name, func_path, result)
    assert material.materialized_ref is not None  # guaranteed by _normalize_python_result
    arrow_table = material.materialized_ref.to_arrow()
    arrow_strategy = materialization_strategies.get("arrow") or materialization_strategies["arrow"]
    return arrow_strategy.materialize(
        arrow_table,
        MaterializationContext(joint_name=cj.name, strategy_name="arrow", options={}),
    )
