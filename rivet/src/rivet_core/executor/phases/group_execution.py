"""Group execution orchestration functions for the executor.

Extracted from ``Executor._has_upstream_failure``,
``Executor._record_group_failure``, ``Executor._compute_group_row_counts``,
``Executor._run_group_engine_phase``, ``Executor._run_group_postprocess_phase``,
``Executor._run_group_checks_phase``, ``Executor._record_group_success_results``,
``Executor._run_assertion_checks``, ``Executor._execute_group_success``,
``Executor._record_group_engine_metrics``, and
``Executor._get_materialization_strategy``.

This is Level 3 of the executor package dependency hierarchy — it imports
from ``models`` (Level 1) and ``helpers/`` (Level 2) only within the
executor package.  It does NOT import from ``pipeline.py`` or other phase
modules.
"""

from __future__ import annotations

import time
from typing import Any

import pyarrow

from rivet_core.compiler import CompiledJoint
from rivet_core.errors import RivetError
from rivet_core.executor.helpers.arrow_helpers import (
    _apply_residuals,
    _compute_materialization_stats,
)
from rivet_core.executor.helpers.checks import (
    _execute_check,
    _generate_check_sql,
    _interpret_check_sql_result,
    _is_sql_translatable,
)
from rivet_core.executor.helpers.pushdown import _merge_residuals
from rivet_core.executor.helpers.utils import _cached_arrow_materials, _notify
from rivet_core.executor.models import (
    CheckExecutionResult,
    ExecutionContext,
    ExecutionState,
    FusedGroupExecutionResult,
    GroupCheckPhaseResult,
    GroupEnginePhaseResult,
    GroupPostprocessResult,
    JointExecutionResult,
    ProgressCallback,
)
from rivet_core.metrics import MaterializationStats, PhasedTiming, PluginMetrics
from rivet_core.models import ComputeEngine
from rivet_core.optimizer import FusedGroup
from rivet_core.plugins import ComputeEnginePlugin, PluginRegistry
from rivet_core.stats import StatsCollector
from rivet_core.strategies import (
    MaterializationStrategy,
    MaterializedRef,
    _ArrowMaterializedRef,
)


def get_materialization_strategy(
    name: str,
    materialization_strategies: dict[str, MaterializationStrategy],
) -> MaterializationStrategy:
    return materialization_strategies.get(name) or materialization_strategies["arrow"]


def has_upstream_failure(
    group: FusedGroup,
    joint_map: dict[str, CompiledJoint],
    failed_joints: set[str],
) -> bool:
    """Check if any upstream joint of this group has failed."""
    for jn in group.joints:
        cj = joint_map.get(jn)
        if cj:
            for up in cj.upstream:
                if up in failed_joints:
                    return True
    return False


def record_group_failure(
    group: FusedGroup,
    error: RivetError,
    failed_joints: set[str],
    joint_results: list[JointExecutionResult],
    group_results: list[FusedGroupExecutionResult],
    step_ms: float = 0.0,
    stats_collector: StatsCollector | None = None,
) -> None:
    """Record failure results for all joints in a group."""
    timing = PhasedTiming(
        total_ms=step_ms,
        engine_ms=0.0,
        materialize_ms=0.0,
        residual_ms=0.0,
        check_ms=0.0,
    )
    for jn in group.joints:
        failed_joints.add(jn)
        joint_results.append(
            JointExecutionResult(
                name=jn,
                success=False,
                rows_in=None,
                rows_out=None,
                timing=timing,
                fused_group_id=group.id,
                materialized=False,
                materialization_trigger=None,
                materialization_stats=None,
                check_results=[],
                plugin_metrics=None,
                error=error,
            )
        )
    group_results.append(
        FusedGroupExecutionResult(
            group_id=group.id,
            joints=list(group.joints),
            success=False,
            rows_in=0,
            rows_out=0,
            timing=timing,
            materialization_stats=None,
            plugin_metrics=None,
            error=error,
        )
    )
    if stats_collector is not None:
        stats_collector.record_group_timing(
            group.id,
            list(group.joints),
            timing,
            success=False,
            error=error,
        )
        for jn in group.joints:
            stats_collector.record_joint_stats(
                jn,
                rows_in=None,
                rows_out=None,
                timing=timing,
                materialization_stats=None,
                skipped=True,
                skip_reason=error.message if error else "upstream failure",
            )


def compute_group_row_counts(
    group: FusedGroup,
    joint_map: dict[str, CompiledJoint],
    materials: dict[str, MaterializedRef],
    result_ref: MaterializedRef,
) -> tuple[int, int]:
    rows_out = 0 if not result_ref.has_cached_arrow() else result_ref.row_count
    rows_in = 0
    for jn in group.entry_joints or group.joints:
        cj = joint_map.get(jn)
        if cj is None:
            continue
        for up in cj.upstream:
            up_ref = materials.get(up)
            if up_ref is not None and up_ref.has_cached_arrow():
                rows_in += up_ref.row_count
    return rows_in, rows_out


async def run_group_engine_phase(
    group: FusedGroup,
    exit_cj: CompiledJoint | None,
    ctx: ExecutionContext,
    state: ExecutionState,
    engine_start: float,
    *,
    execute_fused_group_fn: Any,
    try_native_sql_write_fn: Any,
    checkpoint_read_back_fn: Any,
    materialize_result_fn: Any,
) -> GroupEnginePhaseResult:
    write_path: str | None = None
    native_write_used = False
    exit_joint_name = group.exit_joints[-1] if group.exit_joints else group.joints[-1]

    if exit_cj and exit_cj.type in ("sink", "checkpoint"):
        try:
            native_write_used = await try_native_sql_write_fn(
                group,
                exit_cj,
                ctx.catalog_map,
                state.materials,
            )
        except Exception:
            import logging

            logging.getLogger("rivet_core.executor").warning(
                "native_sql_write failed for '%s'; falling back to Arrow path",
                exit_cj.name if exit_cj else "unknown",
                exc_info=True,
            )
            native_write_used = False

    if native_write_used:
        write_path = "native_sql"
        engine_ms = (time.monotonic() - engine_start) * 1000
        if exit_cj and exit_cj.type == "checkpoint":
            result_ref = await checkpoint_read_back_fn(
                exit_cj,
                ctx.catalog_map,
                engine_type=group.engine_type,
                engine_name=group.engine,
                result_table=None,
            )
        else:
            result_ref = _ArrowMaterializedRef(pyarrow.table({}))
        arrow_materials = _cached_arrow_materials(
            state.materials,
            checkpoint_sources=set(group.checkpoint_sources or {}),
        )
        return GroupEnginePhaseResult(
            result_ref=result_ref,
            adapter_residual=None,
            arrow_materials=arrow_materials,
            engine_ms=engine_ms,
            write_path=write_path,
            native_write_used=True,
            exit_joint_name=exit_joint_name,
        )

    if exit_cj and exit_cj.type in ("sink", "checkpoint"):
        write_path = "arrow_fallback"

    arrow_materials = _cached_arrow_materials(
        state.materials,
        checkpoint_sources=set(group.checkpoint_sources or {}),
    )
    result_ref, adapter_residual = await execute_fused_group_fn(
        group,
        arrow_materials,
        ctx.joint_map,
        ctx.catalog_map,
        ref_materials=state.materials,
        stats_collector=ctx.stats_collector,
    )
    engine_ms = (time.monotonic() - engine_start) * 1000
    return GroupEnginePhaseResult(
        result_ref=result_ref,
        adapter_residual=adapter_residual,
        arrow_materials=arrow_materials,
        engine_ms=engine_ms,
        write_path=write_path,
        native_write_used=False,
        exit_joint_name=exit_joint_name,
    )


async def run_group_postprocess_phase(
    group: FusedGroup,
    exit_cj: CompiledJoint | None,
    ctx: ExecutionContext,
    state: ExecutionState,
    engine_phase: GroupEnginePhaseResult,
    progress: ProgressCallback | None = None,
    *,
    execute_checkpoint_fn: Any,
    materialize_result_fn: Any,
) -> GroupPostprocessResult:
    result_ref = engine_phase.result_ref

    residual_start = time.monotonic()
    if not engine_phase.native_write_used:
        merged_residual = (
            _merge_residuals(group.residual, engine_phase.adapter_residual)
            if engine_phase.adapter_residual
            else group.residual
        )
        if merged_residual is not None:
            result_table = _apply_residuals(result_ref.to_arrow(), merged_residual)
            result_ref = materialize_result_fn(result_table, group)
    residual_ms = (time.monotonic() - residual_start) * 1000

    mat_start = time.monotonic()
    materialized = False
    materialization_trigger: str | None = None
    materialization_stats: MaterializationStats | None = None

    for joint_name in group.joints:
        if joint_name in ctx.materialization_map:
            materialized = True
            materialization_trigger = ctx.materialization_map[joint_name][0].trigger
            break

    total_materializations = 0
    if materialized:
        total_materializations = 1
        if result_ref.has_cached_arrow():
            materialization_stats = _compute_materialization_stats(result_ref.to_arrow())
        if progress:
            # Collect unique (from_joint, strategy) pairs to avoid
            # duplicate materialization notifications within a fused group.
            seen: set[tuple[str, str]] = set()
            for joint_name in group.joints:
                mats = ctx.materialization_map.get(joint_name)
                if mats:
                    for mat in mats:
                        key = (mat.from_joint, mat.strategy)
                        if key not in seen:
                            seen.add(key)
                            _notify(
                                progress.on_materialization,
                                mat.from_joint,
                                group.engine,
                                mat.strategy,
                            )

    if not engine_phase.native_write_used and exit_cj and exit_cj.type == "checkpoint":
        result_ref = await execute_checkpoint_fn(
            exit_cj,
            result_ref,
            ctx.catalog_map,
            engine_type=group.engine_type,
            engine_name=group.engine,
        )

    for joint_name in group.joints:
        state.materials[joint_name] = result_ref

    materialize_ms = (time.monotonic() - mat_start) * 1000
    return GroupPostprocessResult(
        result_ref=result_ref,
        residual_ms=residual_ms,
        materialized=materialized,
        materialization_trigger=materialization_trigger,
        materialization_stats=materialization_stats,
        total_materializations=total_materializations,
        materialize_ms=materialize_ms,
    )


async def run_group_checks_phase(
    group: FusedGroup,
    ctx: ExecutionContext,
    engine_phase: GroupEnginePhaseResult,
    postprocess_phase: GroupPostprocessResult,
    progress: ProgressCallback | None = None,
) -> GroupCheckPhaseResult:
    check_start = time.monotonic()
    assertion_plugin = ctx.registry.get_engine_plugin(group.engine_type)
    assertion_engine: ComputeEngine | None = None
    if assertion_plugin is not None and assertion_plugin.supports_native_assertions:
        assertion_engine = ctx.registry.get_compute_engine(group.engine)

    assertion_input_tables = dict(engine_phase.arrow_materials)
    if assertion_engine is not None and postprocess_phase.result_ref.has_cached_arrow():
        result_arrow = postprocess_phase.result_ref.to_arrow()
        for joint_name in group.joints:
            assertion_input_tables[joint_name] = result_arrow

    (
        all_check_results,
        assertion_error,
        check_failures,
        check_warnings,
    ) = await run_assertion_checks(
        group,
        ctx.joint_map,
        postprocess_phase.result_ref,
        plugin=assertion_plugin,
        engine_instance=assertion_engine,
        input_tables=assertion_input_tables,
    )
    check_ms = (time.monotonic() - check_start) * 1000

    if progress:
        for joint_name in group.joints:
            joint_checks = all_check_results.get(joint_name, [])
            for check in joint_checks:
                _notify(
                    progress.on_check_result,
                    joint_name,
                    check.type,
                    check.passed,
                    check.phase,
                )

    if ctx.stats_collector is not None:
        for joint_name in group.joints:
            joint_checks = all_check_results.get(joint_name, [])
            if joint_checks:
                passed = sum(1 for check in joint_checks if check.passed)
                failed = sum(
                    1 for check in joint_checks if not check.passed and check.severity == "error"
                )
                warned = sum(
                    1 for check in joint_checks if not check.passed and check.severity != "error"
                )
                ctx.stats_collector.record_check_results(
                    joint_name, "assertion", passed, failed, warned
                )

    return GroupCheckPhaseResult(
        all_check_results=all_check_results,
        assertion_error=assertion_error,
        check_failures=check_failures,
        check_warnings=check_warnings,
        check_ms=check_ms,
    )


async def record_group_success_results(
    group: FusedGroup,
    ctx: ExecutionContext,
    state: ExecutionState,
    engine_phase: GroupEnginePhaseResult,
    postprocess_phase: GroupPostprocessResult,
    check_phase: GroupCheckPhaseResult,
    *,
    fail_fast: bool,
    step_start: float,
    run_sink_audits_fn: Any,
) -> tuple[int, int, int, int, bool]:
    step_ms = (time.monotonic() - step_start) * 1000
    timing = PhasedTiming(
        total_ms=step_ms,
        engine_ms=engine_phase.engine_ms,
        materialize_ms=postprocess_phase.materialize_ms,
        residual_ms=postprocess_phase.residual_ms,
        check_ms=check_phase.check_ms,
    )
    rows_in, rows_out = compute_group_row_counts(
        group,
        ctx.joint_map,
        state.materials,
        postprocess_phase.result_ref,
    )
    group_success = not check_phase.assertion_error

    exit_joint_name = engine_phase.exit_joint_name
    entry_joint_name = (group.entry_joints or group.joints)[0]
    for joint_name in group.joints:
        joint_materialized = postprocess_phase.materialized and joint_name == exit_joint_name
        joint_write_path = engine_phase.write_path if joint_name == exit_joint_name else None
        # rows_in only on entry joint; rows_out and timing only on exit joint
        is_exit = joint_name == exit_joint_name
        state.joint_results.append(
            JointExecutionResult(
                name=joint_name,
                success=group_success,
                rows_in=rows_in if joint_name == entry_joint_name else None,
                rows_out=rows_out if is_exit else None,
                timing=timing if is_exit else None,
                fused_group_id=group.id,
                materialized=joint_materialized,
                materialization_trigger=(
                    postprocess_phase.materialization_trigger if joint_materialized else None
                ),
                materialization_stats=(
                    postprocess_phase.materialization_stats if joint_materialized else None
                ),
                check_results=check_phase.all_check_results.get(joint_name, []),
                plugin_metrics=None,
                error=None,
                write_path=joint_write_path,
            )
        )

    state.group_results.append(
        FusedGroupExecutionResult(
            group_id=group.id,
            joints=list(group.joints),
            success=group_success,
            rows_in=rows_in,
            rows_out=rows_out,
            timing=timing,
            materialization_stats=postprocess_phase.materialization_stats,
            plugin_metrics=None,
            error=None,
        )
    )

    if ctx.stats_collector is not None:
        ctx.stats_collector.record_group_timing(
            group.id,
            list(group.joints),
            timing,
            success=group_success,
        )
        for joint_name in group.joints:
            joint_materialized = postprocess_phase.materialized and joint_name == exit_joint_name
            joint_rows_in = rows_in if joint_name == entry_joint_name else None
            is_exit = joint_name == exit_joint_name
            ctx.stats_collector.record_joint_stats(
                joint_name,
                rows_in=joint_rows_in,
                rows_out=rows_out if is_exit else None,
                timing=timing if is_exit else None,
                materialization_stats=(
                    postprocess_phase.materialization_stats if joint_materialized else None
                ),
            )

    total_failures = 0
    stop = False
    if check_phase.assertion_error:
        total_failures = 1
        for joint_name in group.joints:
            state.failed_joints.add(joint_name)
        if fail_fast:
            stop = True
            return (
                postprocess_phase.total_materializations,
                total_failures,
                check_phase.check_failures,
                check_phase.check_warnings,
                stop,
            )

    audit_failures, audit_warnings = await run_sink_audits_fn(
        group,
        ctx.joint_map,
        postprocess_phase.result_ref,
        ctx.catalog_map,
        check_phase.assertion_error,
        state.joint_results,
        skip_write=engine_phase.native_write_used,
    )
    check_failures = check_phase.check_failures + audit_failures
    check_warnings = check_phase.check_warnings + audit_warnings

    if ctx.stats_collector is not None:
        for joint_result in state.joint_results:
            if joint_result.fused_group_id != group.id:
                continue
            audit_checks = [check for check in joint_result.check_results if check.phase == "audit"]
            if not audit_checks:
                continue
            passed = sum(1 for check in audit_checks if check.passed)
            failed = sum(
                1 for check in audit_checks if not check.passed and check.severity == "error"
            )
            warned = sum(
                1 for check in audit_checks if not check.passed and check.severity != "error"
            )
            read_back_rows = next(
                (
                    check.read_back_rows
                    for check in audit_checks
                    if check.read_back_rows is not None
                ),
                None,
            )
            ctx.stats_collector.record_check_results(
                joint_result.name,
                "audit",
                passed,
                failed,
                warned,
                read_back_rows=read_back_rows,
            )

    return (
        postprocess_phase.total_materializations,
        total_failures,
        check_failures,
        check_warnings,
        stop,
    )


async def run_assertion_checks(
    group: FusedGroup,
    joint_map: dict[str, CompiledJoint],
    result_ref: MaterializedRef,
    plugin: ComputeEnginePlugin | None = None,
    engine_instance: ComputeEngine | None = None,
    input_tables: dict[str, pyarrow.Table] | None = None,
) -> tuple[dict[str, list[CheckExecutionResult]], bool, int, int]:
    """Run assertion-phase checks for all joints in a group.

    When *plugin* supports native assertions and a check is SQL-translatable,
    the check is executed engine-natively via ``_generate_check_sql`` +
    ``plugin.execute_assertion_sql``.  On failure the method falls back to
    Arrow-based ``_execute_check``.

    Returns (check_results_by_joint, has_error, error_count, warning_count).
    """
    all_check_results: dict[str, list[CheckExecutionResult]] = {jn: [] for jn in group.joints}
    assertion_error = False
    check_failures = 0
    check_warnings = 0
    assertion_table: pyarrow.Table | None = None

    native_supported = (
        plugin is not None and engine_instance is not None and plugin.supports_native_assertions
    )

    for jn in group.joints:
        cj = joint_map.get(jn)
        if not cj or not cj.checks:
            continue
        assertion_checks = [c for c in cj.checks if c.phase == "assertion"]
        if not assertion_checks:
            continue

        for chk in assertion_checks:
            cr: CheckExecutionResult | None = None

            # Try engine-native path
            if native_supported and _is_sql_translatable(chk):
                try:
                    sqls = _generate_check_sql(chk, jn)
                    all_passed = True
                    last_cr: CheckExecutionResult | None = None
                    for sql in sqls:
                        assert plugin is not None  # for type narrowing
                        assert engine_instance is not None
                        result = plugin.execute_assertion_sql(
                            engine_instance, sql, input_tables or {}
                        )
                        last_cr = _interpret_check_sql_result(chk, result)
                        if not last_cr.passed:
                            all_passed = False
                            break
                    # For multi-column checks, if all passed use the last result,
                    # otherwise use the failing result
                    if last_cr is not None:
                        if all_passed:
                            cr = last_cr
                        else:
                            cr = last_cr
                except Exception:
                    import logging

                    logging.getLogger("rivet_core.executor").warning(
                        "Engine-native assertion failed for check '%s' on joint '%s'; "
                        "falling back to Arrow-based execution",
                        chk.type,
                        jn,
                    )
                    cr = None  # fall through to Arrow path

            # Arrow fallback
            if cr is None:
                if assertion_table is None:
                    assertion_table = result_ref.to_arrow()
                cr = _execute_check(chk, assertion_table)

            all_check_results[jn].append(cr)
            if not cr.passed:
                if cr.severity == "error":
                    check_failures += 1
                    assertion_error = True
                else:
                    check_warnings += 1

    return all_check_results, assertion_error, check_failures, check_warnings


def record_group_engine_metrics(
    group: FusedGroup,
    engine_ms: float,
    stats_collector: StatsCollector | None,
    registry: PluginRegistry | None,
) -> None:
    if stats_collector is None or not registry:
        return
    plugin = registry.get_engine_plugin(group.engine_type)
    if plugin is None:
        return

    sql = group.resolved_sql or group.fused_sql or ""
    if group.fusion_result:
        sql = group.fusion_result.resolved_fused_sql or group.fusion_result.fused_sql or sql
    execution_context: dict[str, Any] = {
        "sql": sql[:1000],
        "group_id": group.id,
        "engine_type": group.engine_type,
        "engine_ms": engine_ms,
    }
    response_metadata = getattr(plugin, "_last_response_metadata", None)
    if isinstance(response_metadata, dict):
        for key, value in response_metadata.items():
            if key not in execution_context:
                execution_context[key] = value
    try:
        metrics = plugin.collect_metrics(execution_context) or PluginMetrics()
        stats_collector.record_engine_metrics(group.id, metrics)
    except Exception:
        import logging

        logging.getLogger("rivet_core.executor").warning(
            "collect_metrics failed for group '%s'; continuing without engine metrics",
            group.id,
        )
        stats_collector.record_engine_metrics(group.id, PluginMetrics())


async def execute_group_success(
    group: FusedGroup,
    ctx: ExecutionContext,
    state: ExecutionState,
    *,
    fail_fast: bool,
    step_start: float,
    engine_start: float,
    progress: ProgressCallback | None = None,
    execute_fused_group_fn: Any,
    try_native_sql_write_fn: Any,
    checkpoint_read_back_fn: Any,
    execute_checkpoint_fn: Any,
    materialize_result_fn: Any,
    run_sink_audits_fn: Any,
) -> tuple[int, int, int, int, bool]:
    """Execute a single group successfully and record results.

    Returns (materializations, failures, check_failures, check_warnings, stop).
    """
    exit_joint = group.exit_joints[-1] if group.exit_joints else group.joints[-1]
    exit_cj = ctx.joint_map.get(exit_joint)
    engine_phase = await run_group_engine_phase(
        group,
        exit_cj,
        ctx,
        state,
        engine_start,
        execute_fused_group_fn=execute_fused_group_fn,
        try_native_sql_write_fn=try_native_sql_write_fn,
        checkpoint_read_back_fn=checkpoint_read_back_fn,
        materialize_result_fn=materialize_result_fn,
    )
    record_group_engine_metrics(group, engine_phase.engine_ms, ctx.stats_collector, ctx.registry)
    postprocess_phase = await run_group_postprocess_phase(
        group,
        exit_cj,
        ctx,
        state,
        engine_phase,
        progress=progress,
        execute_checkpoint_fn=execute_checkpoint_fn,
        materialize_result_fn=materialize_result_fn,
    )
    check_phase = await run_group_checks_phase(
        group,
        ctx,
        engine_phase,
        postprocess_phase,
        progress=progress,
    )
    return await record_group_success_results(
        group,
        ctx,
        state,
        engine_phase,
        postprocess_phase,
        check_phase,
        fail_fast=fail_fast,
        step_start=step_start,
        run_sink_audits_fn=run_sink_audits_fn,
    )
