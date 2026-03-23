"""Executor orchestration pipeline.

Contains the ``Executor`` class with its public entry points
(``run``, ``run_sync``, ``run_query``, etc.) and the wavefront
scheduling loop.

This is Level 4 of the executor package dependency hierarchy — it
imports from ``models`` (Level 1), ``helpers/`` (Level 2), and
``phases/`` (Level 3).
"""

from __future__ import annotations

import asyncio
import functools
import time
from collections.abc import Coroutine
from pathlib import Path
from typing import Any

import pyarrow

from rivet_core.compiler import (
    CompiledAssembly,
    CompiledCatalog,
    CompiledJoint,
    Materialization,
)
from rivet_core.errors import CompilationError, ExecutionError, RivetError
from rivet_core.executor.helpers.arrow_helpers import _apply_residuals
from rivet_core.executor.helpers.pushdown import _merge_residuals
from rivet_core.executor.helpers.utils import (
    _cached_arrow_materials,
    _notify,
)
from rivet_core.executor.models import (
    ExecutionContext,
    ExecutionResult,
    ExecutionState,
    FusedGroupExecutionResult,
    JointExecutionResult,
    ProgressCallback,
    T,
)
from rivet_core.executor.phases.engine_dispatch import (
    execute_fused_group,
    materialize_result,
)
from rivet_core.executor.phases.group_execution import (
    compute_group_row_counts,
    execute_group_success,
    has_upstream_failure,
    record_group_engine_metrics,
    record_group_failure,
)
from rivet_core.executor.phases.scheduling import (
    DependencyGraph,
    EngineConcurrencyPool,
    _nullcontext,
    _resolve_concurrency_limits,
)
from rivet_core.executor.phases.sink_checkpoint import (
    checkpoint_read_back,
    execute_checkpoint,
    run_sink_audits,
    try_native_sql_write,
)
from rivet_core.executor.phases.source_reading import read_sources_into
from rivet_core.metrics import PhasedTiming
from rivet_core.models import ComputeEngine
from rivet_core.optimizer import FusedGroup, ResidualPlan
from rivet_core.plugins import PluginRegistry
from rivet_core.stats import RunStats, StatsCollector
from rivet_core.strategies import (
    ArrowMaterialization,
    MaterializationStrategy,
    MaterializedRef,
)


class Executor:
    """Executes a CompiledAssembly deterministically.

    Follows execution_order exactly. No re-resolution or re-optimization.
    """

    def __init__(
        self, registry: PluginRegistry | None = None, project_root: Path | None = None
    ) -> None:
        self._materialization_strategies: dict[str, MaterializationStrategy] = {
            "arrow": ArrowMaterialization(),
        }
        if registry is None:
            # Create a minimal default registry with the built-in arrow engine plugin
            from rivet_core.builtins.arrow_catalog import ArrowComputeEnginePlugin

            registry = PluginRegistry()
            registry.register_engine_plugin(ArrowComputeEnginePlugin())
        self._registry = registry
        self._project_root = project_root

    def _get_materialization_strategy(self, name: str) -> MaterializationStrategy:
        return (
            self._materialization_strategies.get(name) or self._materialization_strategies["arrow"]
        )

    @staticmethod
    def _build_execution_context(
        compiled: CompiledAssembly,
        registry: PluginRegistry,
        stats_collector: StatsCollector | None = None,
    ) -> ExecutionContext:
        joint_map: dict[str, CompiledJoint] = {cj.name: cj for cj in compiled.joints}
        group_map: dict[str, FusedGroup] = {g.id: g for g in compiled.fused_groups}
        materialization_map: dict[str, list[Materialization]] = {}
        for materialization in compiled.materializations:
            materialization_map.setdefault(materialization.from_joint, []).append(materialization)
        catalog_map: dict[str, CompiledCatalog] = {cc.name: cc for cc in compiled.catalogs}
        return ExecutionContext(
            registry=registry,
            joint_map=joint_map,
            group_map=group_map,
            catalog_map=catalog_map,
            materialization_map=materialization_map,
            stats_collector=stats_collector,
        )

    @staticmethod
    def _build_execution_result(
        joint_results: list[JointExecutionResult],
        group_results: list[FusedGroupExecutionResult],
        total_ms: float,
        total_materializations: int,
        total_failures: int,
        total_check_failures: int,
        total_check_warnings: int,
        fail_fast: bool,
        run_stats: RunStats | None = None,
    ) -> ExecutionResult:
        """Build the final ExecutionResult from accumulated state."""
        if total_failures == 0:
            status = "success"
        elif fail_fast:
            status = "failure"
        else:
            any_success = any(jr.success for jr in joint_results)
            status = "partial_failure" if any_success else "failure"

        return ExecutionResult(
            success=total_failures == 0,
            status=status,
            joint_results=joint_results,
            group_results=group_results,
            total_time_ms=total_ms,
            total_materializations=total_materializations,
            total_failures=total_failures,
            total_check_failures=total_check_failures,
            total_check_warnings=total_check_warnings,
            run_stats=run_stats,
        )

    async def _execute_query_internal(
        self,
        compiled: CompiledAssembly,
        target_joint: str,
        stats_collector: StatsCollector | None = None,
    ) -> tuple[pyarrow.Table, RunStats | None]:
        if not compiled.success:
            raise CompilationError(compiled.diagnostics.errors)

        start_time = time.monotonic() if stats_collector is not None else 0.0
        ctx = self._build_execution_context(compiled, self._registry, stats_collector)
        state = ExecutionState()

        # Build bound callables for phase delegation
        _execute_fused_group_fn = functools.partial(
            execute_fused_group,
            registry=self._registry,
            materialization_strategies=self._materialization_strategies,
            read_sources_fn=functools.partial(read_sources_into, registry=self._registry),
        )

        for step in compiled.execution_order:
            group = ctx.group_map.get(step)
            if group is None:
                continue

            step_start = time.monotonic() if stats_collector is not None else 0.0
            engine_start = time.monotonic() if stats_collector is not None else 0.0
            arrow_materials = _cached_arrow_materials(state.materials)
            result_ref, adapter_residual = await _execute_fused_group_fn(
                group,
                arrow_materials,
                ctx.joint_map,
                ctx.catalog_map,
                ref_materials=state.materials,
                stats_collector=stats_collector,
            )
            engine_ms = ((time.monotonic() - engine_start) * 1000) if stats_collector else 0.0
            record_group_engine_metrics(group, engine_ms, stats_collector, self._registry)

            merged_residual = (
                _merge_residuals(group.residual, adapter_residual)
                if adapter_residual
                else group.residual
            )
            if merged_residual is not None:
                result_table = _apply_residuals(result_ref.to_arrow(), merged_residual)
                result_ref = materialize_result(
                    result_table, group, self._materialization_strategies
                )

            for joint_name in group.joints:
                state.materials[joint_name] = result_ref

            if stats_collector is not None:
                step_ms = (time.monotonic() - step_start) * 1000
                rows_in, rows_out = compute_group_row_counts(
                    group,
                    ctx.joint_map,
                    state.materials,
                    result_ref,
                )
                timing = PhasedTiming(
                    total_ms=step_ms,
                    engine_ms=engine_ms,
                    materialize_ms=0.0,
                    residual_ms=0.0,
                    check_ms=0.0,
                )
                stats_collector.record_group_timing(
                    group.id,
                    list(group.joints),
                    timing,
                    success=True,
                )
                for joint_name in group.joints:
                    stats_collector.record_joint_stats(
                        joint_name,
                        rows_in=(
                            rows_in
                            if joint_name == (group.entry_joints or group.joints)[0]
                            else None
                        ),
                        rows_out=rows_out,
                        timing=timing,
                        materialization_stats=None,
                    )

        if target_joint not in state.materials:
            raise ExecutionError(
                RivetError(
                    code="RVT-501",
                    message=f"Target joint '{target_joint}' not found in execution results.",
                    context={
                        "target_joint": target_joint,
                        "available": list(state.materials.keys()),
                    },
                    remediation="Check that the target joint name is correct.",
                )
            )

        run_stats = None
        if stats_collector is not None:
            total_ms = (time.monotonic() - start_time) * 1000
            run_stats = stats_collector.build_run_stats(total_ms)

        return state.materials[target_joint].to_arrow(), run_stats

    async def run(
        self,
        compiled: CompiledAssembly,
        fail_fast: bool = True,
        progress: ProgressCallback | None = None,
    ) -> ExecutionResult:
        """Execute the compiled assembly using a wavefront parallel scheduler.

        Builds a dependency graph from fused groups, creates per-engine
        concurrency pools, and schedules groups for concurrent execution.
        Groups with all upstream dependencies satisfied are submitted in
        parallel, constrained by engine concurrency limits.

        Raises CompilationError if compiled.success is False.

        fail_fast=True: stop scheduling new groups on first failure,
            let running groups complete.
        fail_fast=False: continue independent branches, skip downstream of
            failed joints, produce ErrorMaterial, accumulate all errors,
            set "partial_failure" status.
        """
        if not compiled.success:
            raise CompilationError(compiled.diagnostics.errors)

        start_time = time.monotonic()
        stats_collector = StatsCollector()
        ctx = self._build_execution_context(compiled, self._registry, stats_collector)
        state = ExecutionState()
        pipeline_stopped = False

        # --- Build dependency graph ---
        dep_graph = DependencyGraph.build(compiled.fused_groups, ctx.joint_map)

        # --- Resolve concurrency limits and create engine pools ---
        # Collect unique engine names from fused groups, look up ComputeEngine
        # instances from the registry.
        unique_engines: dict[str, ComputeEngine] = {}
        for g in compiled.fused_groups:
            if g.engine not in unique_engines and self._registry:
                engine_instance = self._registry.get_compute_engine(g.engine)
                if engine_instance is not None:
                    unique_engines[g.engine] = engine_instance

        # Build list of ComputeEngine objects for concurrency limit resolution.
        # For engines not found in the registry, create a minimal ComputeEngine
        # so _resolve_concurrency_limits can still apply defaults.
        engine_list: list[ComputeEngine] = []
        for g in compiled.fused_groups:
            if g.engine not in {e.name for e in engine_list}:
                if g.engine in unique_engines:
                    engine_list.append(unique_engines[g.engine])
                else:
                    engine_list.append(
                        ComputeEngine(name=g.engine, engine_type=g.engine_type, config={})
                    )

        if self._registry:
            concurrency_limits = _resolve_concurrency_limits(engine_list, self._registry)
        else:
            concurrency_limits = {e.name: 1 for e in engine_list}

        engine_pools: dict[str, EngineConcurrencyPool] = {
            engine_name: EngineConcurrencyPool(engine_name, limit)
            for engine_name, limit in concurrency_limits.items()
        }

        # --- Build bound callables for phase delegation ---
        _read_sources_fn = functools.partial(read_sources_into, registry=self._registry)
        _execute_fused_group_fn = functools.partial(
            execute_fused_group,
            registry=self._registry,
            materialization_strategies=self._materialization_strategies,
            read_sources_fn=_read_sources_fn,
        )
        _try_native_sql_write_fn = functools.partial(try_native_sql_write, registry=self._registry)
        _checkpoint_read_back_fn = functools.partial(checkpoint_read_back, registry=self._registry)
        _execute_checkpoint_fn = functools.partial(execute_checkpoint, registry=self._registry)
        _materialize_result_fn = functools.partial(
            materialize_result, materialization_strategies=self._materialization_strategies
        )
        _run_sink_audits_fn = functools.partial(run_sink_audits, registry=self._registry)

        # --- Per-group coroutine ---
        async def _run_group(
            group_id: str,
        ) -> tuple[str, bool, int, int, int, int]:
            """Execute a single group within its engine's concurrency pool.

            Returns (group_id, success, materializations, failures,
                     check_failures, check_warnings).
            """
            group = ctx.group_map[group_id]
            pool = engine_pools.get(group.engine)
            context_manager: Any = pool if pool is not None else _nullcontext()

            async with context_manager:
                if progress:
                    _notify(progress.on_group_start, group.id, group.engine)

                # Check for upstream failures (between awaits, safe)
                if has_upstream_failure(group, ctx.joint_map, state.failed_joints):
                    error = RivetError(
                        code="RVT-501",
                        message="Skipped: upstream dependency failed.",
                        context={"group_id": group.id, "joints": group.joints},
                        remediation="Fix the upstream failure first.",
                    )
                    record_group_failure(
                        group,
                        error,
                        state.failed_joints,
                        state.joint_results,
                        state.group_results,
                        stats_collector=stats_collector,
                    )
                    if progress:
                        _notify(progress.on_error, group.id, error)
                        _notify(progress.on_group_complete, group.id, False, [], 0.0)
                    return group_id, False, 0, 1, 0, 0

                step_start = time.monotonic()
                engine_start = time.monotonic()

                try:
                    mats, fails, cf, cw, stop = await execute_group_success(
                        group,
                        ctx,
                        state,
                        fail_fast=fail_fast,
                        step_start=step_start,
                        engine_start=engine_start,
                        progress=progress,
                        execute_fused_group_fn=_execute_fused_group_fn,
                        try_native_sql_write_fn=_try_native_sql_write_fn,
                        checkpoint_read_back_fn=_checkpoint_read_back_fn,
                        execute_checkpoint_fn=_execute_checkpoint_fn,
                        materialize_result_fn=_materialize_result_fn,
                        run_sink_audits_fn=_run_sink_audits_fn,
                    )
                    if progress:
                        elapsed_ms = (time.monotonic() - step_start) * 1000
                        # Collect joint results for this group
                        group_joint_results = [
                            jr for jr in state.joint_results if jr.fused_group_id == group.id
                        ]
                        _notify(
                            progress.on_group_complete,
                            group.id,
                            fails == 0,
                            group_joint_results,
                            elapsed_ms,
                        )
                    return group_id, (fails == 0), mats, fails, cf, cw

                except Exception as e:
                    step_ms = (time.monotonic() - step_start) * 1000
                    error = RivetError(
                        code="RVT-501",
                        message=f"Execution failed for group '{group.id}': {e}",
                        context={"group_id": group.id, "joints": group.joints},
                        remediation="Check the SQL and upstream data.",
                    )
                    record_group_failure(
                        group,
                        error,
                        state.failed_joints,
                        state.joint_results,
                        state.group_results,
                        step_ms=step_ms,
                        stats_collector=stats_collector,
                    )
                    if progress:
                        _notify(progress.on_error, group.id, error)
                        _notify(progress.on_group_complete, group.id, False, [], step_ms)
                    return group_id, False, 0, 1, 0, 0

        # --- Wavefront scheduling loop ---
        pending_tasks: dict[asyncio.Task[tuple[str, bool, int, int, int, int]], str] = {}

        # Seed with initially ready groups
        for gid in dep_graph.submit_ready():
            task = asyncio.create_task(_run_group(gid))
            pending_tasks[task] = gid

        while pending_tasks:
            # Wait for at least one task to complete
            done, _ = await asyncio.wait(pending_tasks.keys(), return_when=asyncio.FIRST_COMPLETED)

            # Process ALL completed tasks before checking pipeline_stopped.
            # This ensures running tasks that finished in the same batch
            # have their results recorded.
            for task in done:
                completed_gid = pending_tasks.pop(task)

                try:
                    _gid, success, mats, fails, cf, cw = task.result()
                except asyncio.CancelledError:
                    # Task was cancelled (fail-fast); don't record
                    continue
                except Exception:
                    # Unexpected error in the coroutine wrapper itself
                    success = False
                    mats, fails, cf, cw = 0, 1, 0, 0

                state.total_materializations += mats
                state.total_failures += fails
                state.total_check_failures += cf
                state.total_check_warnings += cw

                newly_ready = dep_graph.mark_complete(completed_gid)

                if not success and fail_fast:
                    pipeline_stopped = True
                elif not success and not fail_fast:
                    # Non-fail-fast: skip transitive downstream dependents,
                    # continue independent branches.
                    downstream_ids = dep_graph.mark_failed(completed_gid)
                    for ds_gid in downstream_ids:
                        ds_group = ctx.group_map.get(ds_gid)
                        if ds_group is not None:
                            error = RivetError(
                                code="RVT-501",
                                message=(f"Skipped: upstream dependency '{completed_gid}' failed."),
                                context={
                                    "group_id": ds_gid,
                                    "joints": list(ds_group.joints),
                                    "failed_upstream": completed_gid,
                                },
                                remediation="Fix the upstream failure first.",
                            )
                            record_group_failure(
                                ds_group,
                                error,
                                state.failed_joints,
                                state.joint_results,
                                state.group_results,
                                stats_collector=stats_collector,
                            )
                            state.total_failures += 1
                        # Mark as submitted so ready_groups() won't return them
                        dep_graph.mark_submitted(ds_gid)

                # When pipeline is stopped, don't schedule new groups
                # but continue processing remaining done tasks
                if not pipeline_stopped:
                    # Schedule newly ready groups
                    for new_gid in newly_ready:
                        if not dep_graph.is_submitted(new_gid):
                            dep_graph.mark_submitted(new_gid)
                            new_task = asyncio.create_task(_run_group(new_gid))
                            pending_tasks[new_task] = new_gid

            if pipeline_stopped:
                # Cancel all pending tasks (not yet running or waiting
                # for semaphore). cancel() is a no-op on already-done tasks.
                for t in list(pending_tasks.keys()):
                    t.cancel()
                # Wait for cancelled/running tasks to settle, then collect
                # results from tasks that completed (were already running).
                if pending_tasks:
                    await asyncio.wait(pending_tasks.keys())
                    for t in list(pending_tasks.keys()):
                        try:
                            _gid, success, mats, fails, cf, cw = t.result()
                        except asyncio.CancelledError:
                            # Cancelled task — don't record in results
                            continue
                        except Exception:
                            success = False
                            mats, fails, cf, cw = 0, 1, 0, 0
                        state.total_materializations += mats
                        state.total_failures += fails
                        state.total_check_failures += cf
                        state.total_check_warnings += cw
                    pending_tasks.clear()
                break

        total_ms = (time.monotonic() - start_time) * 1000
        run_stats = stats_collector.build_run_stats(total_ms)

        return self._build_execution_result(
            state.joint_results,
            state.group_results,
            total_ms,
            state.total_materializations,
            state.total_failures,
            state.total_check_failures,
            state.total_check_warnings,
            fail_fast,
            run_stats=run_stats,
        )

    async def run_query(
        self, compiled: CompiledAssembly, target_joint: str = "__query"
    ) -> pyarrow.Table:
        """Execute a compiled assembly and return the target joint's result table.

        Runs the execution pipeline without sink writes or audits, capturing
        intermediate materials to return the target joint's output.

        Raises:
            CompilationError: If compiled.success is False.
            ExecutionError: If execution fails or target joint not found.
        """
        table, _ = await self._execute_query_internal(compiled, target_joint)
        return table

    async def run_query_with_stats(
        self, compiled: CompiledAssembly, target_joint: str = "__query"
    ) -> tuple[pyarrow.Table, RunStats]:
        """Like run_query but also returns RunStats with timing breakdown.

        Raises:
            CompilationError: If compiled.success is False.
            ExecutionError: If execution fails or target joint not found.
        """
        stats_collector = StatsCollector()
        table, run_stats = await self._execute_query_internal(
            compiled,
            target_joint,
            stats_collector=stats_collector,
        )
        assert run_stats is not None
        return table, run_stats

    def run_sync(
        self,
        compiled: CompiledAssembly,
        fail_fast: bool = True,
        progress: ProgressCallback | None = None,
    ) -> ExecutionResult:
        """Synchronous entry point — creates an event loop and runs the async scheduler."""
        return self._run_in_loop(self.run(compiled, fail_fast=fail_fast, progress=progress))

    def run_query_sync(
        self, compiled: CompiledAssembly, target_joint: str = "__query"
    ) -> pyarrow.Table:
        """Synchronous wrapper for run_query."""
        return self._run_in_loop(self.run_query(compiled, target_joint))

    def run_query_with_stats_sync(
        self, compiled: CompiledAssembly, target_joint: str = "__query"
    ) -> tuple[pyarrow.Table, RunStats]:
        """Synchronous wrapper for run_query_with_stats."""
        return self._run_in_loop(self.run_query_with_stats(compiled, target_joint))

    def _run_in_loop(self, coro: Coroutine[Any, Any, T]) -> T:
        """Run a coroutine, handling both sync and async contexts.

        When called from within an existing event loop (e.g., Textual REPL),
        runs the coroutine in a new thread with its own event loop.
        Otherwise, creates a new event loop with asyncio.run().
        """
        from rivet_core.async_utils import safe_run_async

        return safe_run_async(coro)

    # ------------------------------------------------------------------
    # Backward-compatible delegation methods
    #
    # These thin wrappers preserve the ``self._method(...)`` calling
    # convention used by existing test code.  They delegate to the
    # module-level functions in ``phases/`` passing ``self._registry``
    # and other instance state as explicit parameters.
    # ------------------------------------------------------------------

    async def _read_sources_into(
        self,
        input_tables: dict[str, pyarrow.Table],
        group: FusedGroup,
        joint_map: dict[str, CompiledJoint],
        catalog_map: dict[str, CompiledCatalog] | None,
        stats_collector: StatsCollector | None = None,
        skip_fused_sources: bool = False,
        ref_materials: dict[str, MaterializedRef] | None = None,
    ) -> ResidualPlan | None:
        return await read_sources_into(
            input_tables,
            group,
            joint_map,
            catalog_map,
            self._registry,
            stats_collector=stats_collector,
            skip_fused_sources=skip_fused_sources,
            ref_materials=ref_materials,
        )

    async def _checkpoint_read_back(
        self,
        cj: CompiledJoint,
        catalog_map: dict[str, CompiledCatalog],
        engine_type: str | None = None,
        engine_name: str | None = None,
        result_table: pyarrow.Table | None = None,
    ) -> MaterializedRef:
        return await checkpoint_read_back(
            cj,
            catalog_map,
            self._registry,
            engine_type=engine_type,
            engine_name=engine_name,
            result_table=result_table,
        )

    async def _try_native_sql_write(
        self,
        group: FusedGroup,
        exit_cj: CompiledJoint,
        catalog_map: dict[str, CompiledCatalog],
        materials: dict[str, MaterializedRef],
    ) -> bool:
        return await try_native_sql_write(
            group,
            exit_cj,
            catalog_map,
            materials,
            self._registry,
        )

    async def _execute_group_success(
        self,
        group: FusedGroup,
        joint_map: dict[str, CompiledJoint],
        catalog_map: dict[str, CompiledCatalog],
        mat_map: dict[str, list[Materialization]],
        materials: dict[str, MaterializedRef],
        failed_joints: set[str],
        joint_results: list[JointExecutionResult],
        group_results: list[FusedGroupExecutionResult],
        fail_fast: bool,
        step_start: float,
        engine_start: float,
        stats_collector: StatsCollector | None = None,
        progress: ProgressCallback | None = None,
    ) -> tuple[int, int, int, int, bool]:
        assert self._registry is not None, "Executor requires a registry"
        ctx = ExecutionContext(
            registry=self._registry,
            joint_map=joint_map,
            group_map={group.id: group},
            catalog_map=catalog_map,
            materialization_map=mat_map,
            stats_collector=stats_collector,
        )
        state = ExecutionState(
            materials=materials,
            failed_joints=failed_joints,
            joint_results=joint_results,
            group_results=group_results,
        )
        return await execute_group_success(
            group,
            ctx,
            state,
            fail_fast=fail_fast,
            step_start=step_start,
            engine_start=engine_start,
            progress=progress,
            execute_fused_group_fn=self._execute_fused_group,
            try_native_sql_write_fn=self._try_native_sql_write,
            checkpoint_read_back_fn=self._checkpoint_read_back,
            execute_checkpoint_fn=functools.partial(execute_checkpoint, registry=self._registry),
            materialize_result_fn=self._materialize_result,
            run_sink_audits_fn=functools.partial(run_sink_audits, registry=self._registry),
        )

    async def _execute_fused_group(
        self,
        group: FusedGroup,
        materials: dict[str, pyarrow.Table],
        joint_map: dict[str, CompiledJoint],
        catalog_map: dict[str, CompiledCatalog] | None = None,
        ref_materials: dict[str, MaterializedRef] | None = None,
        stats_collector: StatsCollector | None = None,
    ) -> tuple[MaterializedRef, ResidualPlan | None]:
        return await execute_fused_group(
            group,
            materials,
            joint_map,
            catalog_map,
            registry=self._registry,
            materialization_strategies=self._materialization_strategies,
            read_sources_fn=functools.partial(read_sources_into, registry=self._registry),
            ref_materials=ref_materials,
            stats_collector=stats_collector,
        )

    def _materialize_result(self, table: pyarrow.Table, group: FusedGroup) -> MaterializedRef:
        return materialize_result(table, group, self._materialization_strategies)

    def _record_group_engine_metrics(
        self,
        group: FusedGroup,
        engine_ms: float,
        stats_collector: StatsCollector | None = None,
    ) -> None:
        record_group_engine_metrics(group, engine_ms, stats_collector, self._registry)

    @staticmethod
    def _has_upstream_failure(
        group: FusedGroup,
        joint_map: dict[str, CompiledJoint],
        failed_joints: set[str],
    ) -> bool:
        return has_upstream_failure(group, joint_map, failed_joints)

    @staticmethod
    def _record_group_failure(
        group: FusedGroup,
        error: RivetError,
        failed_joints: set[str],
        joint_results: list[JointExecutionResult],
        group_results: list[FusedGroupExecutionResult],
        step_ms: float = 0.0,
        stats_collector: StatsCollector | None = None,
    ) -> None:
        record_group_failure(
            group,
            error,
            failed_joints,
            joint_results,
            group_results,
            step_ms=step_ms,
            stats_collector=stats_collector,
        )

    @staticmethod
    def _compute_group_row_counts(
        group: FusedGroup,
        joint_map: dict[str, CompiledJoint],
        materials: dict[str, MaterializedRef],
        result_ref: MaterializedRef,
    ) -> tuple[int, int]:
        return compute_group_row_counts(group, joint_map, materials, result_ref)
