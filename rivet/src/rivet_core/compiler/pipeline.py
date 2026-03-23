"""Level 4 — Compilation pipeline orchestration.

Contains :class:`CompilationPipeline`, the ``compile()`` and
``compile_until()`` public entry points, option/state builders, and legacy
orchestration functions preserved for backward compatibility.

This is a Level 4 module — it may import from any level within the compiler
package (``models``, ``state``, ``helpers/``, ``phases/``).
"""

from __future__ import annotations

import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from dataclasses import replace
from pathlib import Path
from typing import Any

from rivet_core.assembly import Assembly
from rivet_core.compiler.helpers.resolution import (
    _build_checkpoint_sources,
    _build_compilation_diagnostics,
    _build_compiled_adapters,
    _build_compiled_catalogs,
    _build_compiled_engines,
    _detect_engine_boundaries,
    _determine_materializations,
    _do_introspect,
    _inject_checkpoint_ctes,
    _resolve_references,
    _resolve_strategy,
)
from rivet_core.compiler.helpers.sql_helpers import (
    _assign_schema_confidence,
    _compute_source_transform_schema,
    _infer_sink_schemas,
    _warn_unresolved_column_refs,
)
from rivet_core.compiler.helpers.validation import (
    _compile_joint_with_context,
)
from rivet_core.compiler.models import (
    CompilationContext,
    CompilationStats,
    CompiledAdapter,
    CompiledAssembly,
    CompiledCatalog,
    CompiledEngine,
    CompiledJoint,
    EngineBoundary,
    ExecutionWave,
    FusionStrategyName,
    Materialization,
    MaterializationStrategyName,
    TagMode,
    logger,
)
from rivet_core.compiler.phases.phase01_prune import prune_dag_phase
from rivet_core.compiler.phases.phase02_metadata import resolve_metadata_phase
from rivet_core.compiler.phases.phase03_introspect import introspect_sources_phase
from rivet_core.compiler.phases.phase04_compile_sql import compile_sql_phase
from rivet_core.compiler.phases.phase05_fusion import fusion_phase
from rivet_core.compiler.phases.phase06_optimization import optimization_phase
from rivet_core.compiler.phases.phase07_strategy import strategy_resolution_phase
from rivet_core.compiler.phases.phase08_engine_boundaries import engine_boundary_phase
from rivet_core.compiler.phases.phase09_materialization import materialization_phase
from rivet_core.compiler.phases.phase10_finalization import finalization_phase
from rivet_core.compiler.state import CompileOptions, CompilerPhase, PhaseState
from rivet_core.errors import RivetError
from rivet_core.models import Catalog, ComputeEngine, Schema
from rivet_core.optimizer import (
    FusedGroup,
    FusionJoint,
    cross_group_pushdown_pass,
    fusion_pass,
    pushdown_pass,
)
from rivet_core.plugins import PluginRegistry, ReferenceResolver
from rivet_core.sql_parser import LogicalPlan, SQLParser


class CompilationPipeline:
    """Orchestrator that chains ``CompilerPhase`` instances sequentially.

    The pipeline records per-phase wall-clock timing in
    ``PhaseState.phase_timings`` and emits a ``DEBUG`` log after each phase
    transition.  Pass *stop_after* to ``run()`` to halt after a specific
    phase — this powers the ``compile_until()`` public API.
    """

    def __init__(self, phases: list[CompilerPhase]) -> None:
        self._phases = phases

    def run(
        self,
        initial_state: PhaseState,
        stop_after: str | None = None,
    ) -> PhaseState:
        """Run phases sequentially, recording per-phase timing.

        If *stop_after* is given, execution stops after the named phase.
        """
        state = initial_state
        for phase in self._phases:
            t0 = time.monotonic()
            state = phase(state)
            duration_ms = (time.monotonic() - t0) * 1000
            state = replace(
                state,
                phase_timings={**state.phase_timings, phase.name: duration_ms},
                completed_phases=(*state.completed_phases, phase.name),
            )
            logger.debug(
                "Phase '%s' completed in %.1fms",
                phase.name,
                duration_ms,
                extra={"phase": phase.name, "duration_ms": duration_ms},
            )
            if stop_after and phase.name == stop_after:
                break
        return state


# ---------------------------------------------------------------------------
# Legacy orchestration functions
# ---------------------------------------------------------------------------


def _prune_dag(
    assembly: Assembly,
    target_sink: str | None,
    tags: list[str] | None,
    tag_mode: str,
    profile_name: str,
    errors: list[RivetError],
    warnings: list[str],
) -> Assembly | CompiledAssembly:
    """Step 1: Prune the DAG to the target subgraph.

    Returns the pruned Assembly on success, or a failed CompiledAssembly on error.
    """
    try:
        return assembly.subgraph(target_sink=target_sink, tags=tags, tag_mode=tag_mode)
    except Exception as e:
        errors.append(
            RivetError(
                code="RVT-306",
                message=str(e),
                context={"target_sink": target_sink, "tags": tags},
                remediation="Check target_sink and tags parameters.",
            )
        )
        return CompiledAssembly(
            success=False,
            profile_name=profile_name,
            catalogs=[],
            engines=[],
            adapters=[],
            joints=[],
            fused_groups=[],
            materializations=[],
            execution_order=[],
            diagnostics=_build_compilation_diagnostics(errors, warnings),
        )


def _compile_all_joints(
    pruned: Assembly,
    catalog_map: dict[str, Catalog],
    engine_map: dict[str, ComputeEngine],
    registry: PluginRegistry,
    default_engine: str | None,
    errors: list[RivetError],
    warnings: list[str],
    introspect: bool,
    introspect_timeout: float,
    catalogs: list[Catalog],
    engines: list[ComputeEngine],
    project_root: Path | None = None,
) -> tuple[
    list[str],
    list[CompiledJoint],
    dict[str, CompiledJoint],
    list[CompiledCatalog],
    list[CompiledEngine],
    list[CompiledAdapter],
    int,
    int,
    int,
    int,
]:
    """Steps 2–3b: Topological ordering, per-joint compilation, schema confidence."""
    topo_order = pruned.topological_order()
    parser = SQLParser()
    upstream_schemas: dict[str, Schema] = {}
    ctx = CompilationContext(
        catalog_map=catalog_map,
        engine_map=engine_map,
        registry=registry,
        default_engine=default_engine,
        parser=parser,
        upstream_schemas=upstream_schemas,
        errors=errors,
        warnings=warnings,
        project_root=project_root,
    )

    introspection_attempted = 0
    introspection_succeeded = 0
    introspection_failed = 0
    introspection_skipped = 0

    # ── Submit all source introspections concurrently via a shared pool ──
    introspection_futures: dict[str, Any] = {}
    source_joints: list[str] = []
    if introspect:
        for jn in topo_order:
            joint = pruned.joints[jn]
            if joint.joint_type == "source":
                source_joints.append(jn)

    pool: ThreadPoolExecutor | None = None
    if introspect and source_joints:
        pool = ThreadPoolExecutor(max_workers=min(8, len(source_joints)))
        for jn in source_joints:
            joint = pruned.joints[jn]
            catalog = catalog_map.get(joint.catalog) if joint.catalog else None
            catalog_type = catalog.type if catalog else None
            catalog_plugin = registry.get_catalog_plugin(catalog_type) if catalog_type else None
            if catalog and catalog_plugin:
                future = pool.submit(
                    _do_introspect,
                    joint,
                    catalog,
                    catalog_plugin,
                    catalog_map,
                )
                introspection_futures[jn] = future

    # ── Compile joints (introspection disabled inline; results attached after) ──
    introspected_sources: set[str] = set()
    compiled_joints: list[CompiledJoint] = []
    ctx.adapter_cache = {}
    try:
        for jn in topo_order:
            joint = pruned.joints[jn]
            is_source = joint.joint_type == "source"

            if is_source and not introspect:
                introspection_skipped += 1
            elif is_source and introspect:
                introspection_attempted += 1

            # Compile without inline introspection — we handle it from the pool
            cj = _compile_joint_with_context(
                joint,
                ctx,
                introspect=False,
                introspect_timeout=introspect_timeout,
            )

            # Attach introspection results from the shared pool
            if is_source and introspect and jn in introspection_futures:
                try:
                    schema, stats, local_warnings = introspection_futures[jn].result(
                        timeout=introspect_timeout
                    )
                    warnings.extend(local_warnings)
                    if schema is not None or stats is not None:
                        cj = replace(
                            cj,
                            output_schema=schema or cj.output_schema,
                            source_stats=stats if stats is not None else cj.source_stats,
                        )
                except TimeoutError:
                    warnings.append(
                        f"Introspection timed out for source '{jn}' after {introspect_timeout}s"
                    )
                except Exception as exc:
                    warnings.append(f"Introspection failed for source '{jn}': {exc}")

                # Re-run source inline transform validation with introspected schema
                # to compute the transformed output schema and emit column warnings.
                # Single-table constraint errors were already emitted by _compile_joint.
                if cj.output_schema is not None and cj.logical_plan is not None:
                    # Emit column reference warnings now that we have the catalog schema
                    _warn_unresolved_column_refs(
                        cj.name,
                        cj.logical_plan,
                        cj.output_schema,
                        warnings,
                    )
                    # Compute transformed schema from projections
                    transformed = _compute_source_transform_schema(
                        cj.name,
                        cj.logical_plan.projections,
                        cj.output_schema,
                        warnings,
                    )
                    if transformed is not None:
                        cj = replace(cj, output_schema=transformed)

            if is_source and introspect:
                if cj.output_schema is not None:
                    introspection_succeeded += 1
                    introspected_sources.add(cj.name)
                    # Propagate schema for downstream SQL inference
                    upstream_schemas[cj.name] = cj.output_schema
                else:
                    introspection_failed += 1

            compiled_joints.append(cj)
    finally:
        if pool is not None:
            pool.shutdown(wait=False)

    # Infer sink schemas from upstream joints
    compiled_joints = _infer_sink_schemas(compiled_joints, warnings)

    compiled_joints = _assign_schema_confidence(compiled_joints, introspected_sources)
    cj_map: dict[str, CompiledJoint] = {cj.name: cj for cj in compiled_joints}

    compiled_catalogs = _build_compiled_catalogs(compiled_joints, catalogs)
    compiled_engines = _build_compiled_engines(compiled_joints, engines, registry)
    compiled_adapters = _build_compiled_adapters(compiled_joints, engine_map, registry)

    return (
        topo_order,
        compiled_joints,
        cj_map,
        compiled_catalogs,
        compiled_engines,
        compiled_adapters,
        introspection_attempted,
        introspection_succeeded,
        introspection_failed,
        introspection_skipped,
    )


def _run_optimizer_passes(
    compiled_joints: list[CompiledJoint],
    cj_map: dict[str, CompiledJoint],
    engine_map: dict[str, ComputeEngine],
    registry: PluginRegistry,
    catalog_map: dict[str, Catalog],
    default_fusion_strategy: str,
    resolve_references_fn: ReferenceResolver | None,
    errors: list[RivetError],
    warnings: list[str],
) -> tuple[list[FusedGroup], dict[str, str]]:
    """Steps 4–7: Fusion, pushdown, strategy resolution, reference resolution."""
    from rivet_core.executor import _is_sql_translatable

    engine_plugin_cache: dict[str, Any] = {}

    def _engine_plugin(engine_type: str) -> Any:
        if not engine_type:
            return None
        if engine_type not in engine_plugin_cache:
            engine_plugin_cache[engine_type] = registry.get_engine_plugin(engine_type)
        return engine_plugin_cache[engine_type]

    # Fusion pass
    fusion_joints: list[FusionJoint] = []
    for cj in compiled_joints:
        eng = engine_map.get(cj.engine)
        et = eng.engine_type if eng else ""
        # When the engine supports native assertions and all assertion-phase
        # checks are SQL-translatable, treat the joint as having no assertions
        # for fusion purposes — this allows it to fuse with downstream joints.
        has_assertions = bool(cj.checks)
        if has_assertions:
            plugin = _engine_plugin(et)
            assertion_checks = [c for c in cj.checks if c.phase == "assertion"]
            if (
                assertion_checks
                and plugin is not None
                and plugin.supports_native_assertions
                and all(_is_sql_translatable(c) for c in assertion_checks)
            ):
                has_assertions = False
        fusion_joints.append(
            FusionJoint(
                name=cj.name,
                joint_type=cj.type,
                upstream=cj.upstream,
                engine=cj.engine,
                engine_type=et,
                adapter=cj.adapter,
                eager=cj.eager,
                has_assertions=has_assertions,
                sql=cj.sql_translated or cj.sql,
                sql_dialect=cj.engine_dialect or cj.sql_dialect or "duckdb",
            )
        )

    fused_groups = fusion_pass(fusion_joints, fusion_strategy=default_fusion_strategy)

    joint_to_group: dict[str, str] = {}
    for group in fused_groups:
        for jn in group.joints:
            joint_to_group[jn] = group.id

    # Pushdown pass
    logical_plans: dict[str, LogicalPlan | None] = {
        cj.name: cj.logical_plan for cj in compiled_joints
    }
    catalog_types_map: dict[str, str | None] = {cj.name: cj.catalog_type for cj in compiled_joints}
    cap_map: dict[str, list[str]] = {}
    for cj in compiled_joints:
        eng = engine_map.get(cj.engine)
        et = eng.engine_type if eng else ""
        if et and cj.catalog_type:
            key = f"{et}:{cj.catalog_type}"
            if key not in cap_map:
                caps = registry.resolve_capabilities(et, cj.catalog_type)
                if caps is not None:
                    cap_map[key] = caps

    fused_groups = pushdown_pass(fused_groups, logical_plans, cap_map, catalog_types_map)

    # Cross-group predicate pushdown
    fused_groups, xgroup_results = cross_group_pushdown_pass(
        fused_groups,
        cj_map,
        cap_map,
        catalog_types_map,
    )
    # Attach cross-group optimization results to the relevant compiled joints
    for result in xgroup_results:
        if result.target_joint and result.target_joint in cj_map:
            cj = cj_map[result.target_joint]
            cj_map[result.target_joint] = replace(cj, optimizations=[*cj.optimizations, result])

    # Strategy + reference resolution
    fused_groups = _resolve_strategy(fused_groups, cj_map, default_fusion_strategy, errors)
    fused_groups = _resolve_references(
        fused_groups,
        cj_map,
        compiled_joints,
        engine_map,
        catalog_map,
        registry,
        resolve_references_fn,
        warnings,
    )

    # Build checkpoint source metadata, then inject checkpoint CTEs
    fused_groups = _build_checkpoint_sources(
        fused_groups, cj_map, joint_to_group, registry, warnings
    )
    fused_groups = _inject_checkpoint_ctes(fused_groups, cj_map, catalog_map)

    return fused_groups, joint_to_group


def _determine_materializations_and_boundaries(
    cj_map: dict[str, CompiledJoint],
    joint_to_group: dict[str, str],
    engine_map: dict[str, ComputeEngine],
    registry: PluginRegistry,
    default_materialization_strategy: MaterializationStrategyName,
    fused_groups: list[FusedGroup],
    warnings: list[str],
) -> tuple[list[Materialization], list[EngineBoundary], list[FusedGroup]]:
    """Steps 8–9: Engine boundaries, then materializations, group mat-strategy."""
    engine_boundaries = _detect_engine_boundaries(
        fused_groups,
        cj_map,
        joint_to_group,
        registry,
        warnings,
    )

    # Build boundary_joints set from engine boundaries
    boundary_joints: set[str] = set()
    for boundary in engine_boundaries:
        for jn in boundary.boundary_joints:
            boundary_joints.add(jn)

    materializations = _determine_materializations(
        cj_map,
        joint_to_group,
        engine_map,
        registry,
        default_materialization_strategy,
        boundary_joints,
    )

    # Resolve materialization_strategy_name per group
    for group in fused_groups:
        resolved_mat_name: str | None = None
        for jn in group.joints:
            cj = cj_map.get(jn)
            if cj and cj.materialization_strategy_override:
                resolved_mat_name = cj.materialization_strategy_override
                break
        if resolved_mat_name is None:
            engine_plugin = registry.get_engine_plugin(group.engine_type)
            if engine_plugin:
                resolved_mat_name = engine_plugin.materialization_strategy_name
        if not resolved_mat_name:
            resolved_mat_name = "arrow"
        if resolved_mat_name != group.materialization_strategy_name:
            fused_groups = [
                replace(group, materialization_strategy_name=resolved_mat_name)
                if g.id == group.id
                else g
                for g in fused_groups
            ]

    return materializations, engine_boundaries, fused_groups


def _finalize_assembly(
    cj_map: dict[str, CompiledJoint],
    topo_order: list[str],
    joint_to_group: dict[str, str],
    profile_name: str,
    compiled_catalogs: list[CompiledCatalog],
    compiled_engines: list[CompiledEngine],
    compiled_adapters: list[CompiledAdapter],
    fused_groups: list[FusedGroup],
    materializations: list[Materialization],
    engine_boundaries: list[EngineBoundary],
    errors: list[RivetError],
    warnings: list[str],
    compile_duration_ms: int,
    introspection_attempted: int,
    introspection_succeeded: int,
    introspection_failed: int,
    introspection_skipped: int,
) -> CompiledAssembly:
    """Step 10: Build final joint list, execution order, and CompiledAssembly."""
    from rivet_core.sql_resolver import resolve_execution_sql

    deduped_warnings = list(dict.fromkeys(warnings))

    # Populate execution_sql for each joint based on its fused group

    # Build set of groups that have materialized inputs (cross engine boundary)
    groups_with_materialized_inputs: set[str] = set()
    for boundary in engine_boundaries:
        groups_with_materialized_inputs.add(boundary.consumer_group_id)

    # Resolve execution SQL for each group and populate joints
    group_execution_sql: dict[str, str | None] = {}
    for group in fused_groups:
        # Determine adapter-read sources in this group
        adapter_read_sources = {
            jn
            for jn in group.joints
            if cj_map.get(jn) and cj_map[jn].type == "source" and cj_map[jn].adapter is not None
        }

        # Check if this group has materialized inputs
        has_materialized_inputs = group.id in groups_with_materialized_inputs

        # Resolve execution SQL for this group
        execution_sql = resolve_execution_sql(
            group,
            cj_map,
            adapter_read_sources,
            has_materialized_inputs=has_materialized_inputs,
        )
        group_execution_sql[group.id] = execution_sql

    # Build final joints with fused_group_id and execution_sql
    final_joints = [
        replace(
            cj_map[jn],
            fused_group_id=joint_to_group.get(jn),
            execution_sql=(
                group_execution_sql.get(gid)
                if (gid := joint_to_group.get(jn)) is not None
                else None
            ),
        )
        for jn in topo_order
    ]

    execution_order: list[str] = []
    seen_groups: set[str] = set()
    for jn in topo_order:
        gid = joint_to_group.get(jn)
        if gid and gid not in seen_groups:
            seen_groups.add(gid)
            execution_order.append(gid)

    # Compute parallel execution plan (wave assignment)
    parallel_execution_plan = _compute_parallel_execution_plan(
        fused_groups, cj_map, deduped_warnings
    )

    compilation_stats = CompilationStats(
        compile_duration_ms=compile_duration_ms,
        joints_with_schema=sum(1 for j in final_joints if j.output_schema is not None),
        joints_total=len(final_joints),
        introspection_attempted=introspection_attempted,
        introspection_succeeded=introspection_succeeded,
        introspection_failed=introspection_failed,
        introspection_skipped=introspection_skipped,
    )

    return CompiledAssembly(
        success=len(errors) == 0,
        profile_name=profile_name,
        catalogs=compiled_catalogs,
        engines=compiled_engines,
        adapters=compiled_adapters,
        joints=final_joints,
        fused_groups=fused_groups,
        materializations=materializations,
        execution_order=execution_order,
        diagnostics=_build_compilation_diagnostics(
            errors,
            deduped_warnings,
            compilation_stats,
        ),
        engine_boundaries=engine_boundaries,
        parallel_execution_plan=parallel_execution_plan,
    )


def _compute_parallel_execution_plan(
    fused_groups: list[FusedGroup],
    cj_map: dict[str, CompiledJoint],
    warnings: list[str] | None = None,
) -> list[ExecutionWave]:
    """Compute the parallel execution plan using wavefront analysis.

    Locally reimplements the DependencyGraph edge-building logic to avoid
    circular imports (compiler → executor).

    Algorithm:
    1. Build upstream/in-degree maps from fused groups and compiled joints.
    2. Groups with in-degree 0 → wave 1.
    3. Remove wave 1 groups, find new in-degree 0 groups → wave 2.
    4. Repeat until all groups are assigned.
    """
    if not fused_groups:
        return []

    # Map each joint name to its owning fused group ID
    joint_to_group: dict[str, str] = {}
    group_by_id: dict[str, FusedGroup] = {}
    for group in fused_groups:
        group_by_id[group.id] = group
        for joint_name in group.joints:
            joint_to_group[joint_name] = group.id

    # Build upstream and downstream edges
    upstream: dict[str, set[str]] = {g.id: set() for g in fused_groups}
    downstream: dict[str, set[str]] = {g.id: set() for g in fused_groups}

    for group in fused_groups:
        for joint_name in group.joints:
            compiled_joint = cj_map.get(joint_name)
            if compiled_joint is None:
                continue
            for up_name in compiled_joint.upstream:
                up_group_id = joint_to_group.get(up_name)
                if up_group_id is None or up_group_id == group.id:
                    continue
                upstream[group.id].add(up_group_id)
                downstream[up_group_id].add(group.id)

    in_degree: dict[str, int] = {gid: len(ups) for gid, ups in upstream.items()}
    group_order = [group.id for group in fused_groups]
    order_index = {gid: idx for idx, gid in enumerate(group_order)}

    # Wavefront assignment
    ready = deque(gid for gid in group_order if in_degree[gid] == 0)
    waves: list[ExecutionWave] = []
    wave_number = 0
    assigned: set[str] = set()

    while ready:
        wave_number += 1
        current_wave = [ready.popleft() for _ in range(len(ready))]
        assigned.update(current_wave)

        # Build engine mapping for this wave
        engines: dict[str, list[str]] = {}
        for gid in current_wave:
            engine_name = group_by_id[gid].engine
            engines.setdefault(engine_name, []).append(gid)

        waves.append(
            ExecutionWave(
                wave_number=wave_number,
                groups=current_wave,
                engines=engines,
            )
        )

        next_ready: set[str] = set()
        for gid in current_wave:
            for ds_id in downstream.get(gid, set()):
                in_degree[ds_id] -= 1
                if in_degree[ds_id] == 0 and ds_id not in assigned:
                    next_ready.add(ds_id)

        for gid in sorted(next_ready, key=order_index.__getitem__):
            ready.append(gid)

    unresolved = [gid for gid in group_order if gid not in assigned]
    if unresolved and warnings is not None:
        warnings.append(
            "Parallel execution plan could not be fully computed because fused group "
            "dependencies are cyclic or inconsistent. Remaining groups: "
            f"{', '.join(unresolved)}."
        )

    return waves


# ---------------------------------------------------------------------------
# Option / state builders
# ---------------------------------------------------------------------------


def _build_compile_options(
    resolved_options: CompileOptions,
    profile_name: str | None,
    target_sink: str | None,
    tags: list[str] | None,
    tag_mode: TagMode | None,
    default_fusion_strategy: FusionStrategyName | None,
    default_materialization_strategy: MaterializationStrategyName | None,
    resolve_references: ReferenceResolver | None,
    default_engine: str | None,
    introspect: bool | None,
    introspect_timeout: float | None,
    project_root: Path | None,
) -> CompileOptions:
    """Merge explicit keyword arguments with a CompileOptions base."""
    return CompileOptions(
        profile_name=profile_name or resolved_options.profile_name,
        target_sink=target_sink if target_sink is not None else resolved_options.target_sink,
        tags=tags if tags is not None else resolved_options.tags,
        tag_mode=tag_mode or resolved_options.tag_mode,
        default_fusion_strategy=(
            default_fusion_strategy or resolved_options.default_fusion_strategy
        ),
        default_materialization_strategy=(
            default_materialization_strategy or resolved_options.default_materialization_strategy
        ),
        resolve_references=(
            resolve_references
            if resolve_references is not None
            else resolved_options.resolve_references
        ),
        default_engine=(
            default_engine if default_engine is not None else resolved_options.default_engine
        ),
        introspect=introspect if introspect is not None else resolved_options.introspect,
        introspect_timeout=(
            introspect_timeout
            if introspect_timeout is not None
            else resolved_options.introspect_timeout
        ),
        project_root=(project_root if project_root is not None else resolved_options.project_root),
    )


def _build_initial_phase_state(
    assembly: Assembly,
    catalogs: list[Catalog],
    engines: list[ComputeEngine],
    registry: PluginRegistry,
    opts: CompileOptions,
) -> PhaseState:
    """Build the initial PhaseState from compile() parameters."""
    catalog_map: dict[str, Catalog] = {c.name: c for c in catalogs}
    engine_map: dict[str, ComputeEngine] = {e.name: e for e in engines}

    default_engine = opts.default_engine

    # Build unified engine lookup map: merge registry engines for all names
    # referenced in the assembly that aren't already in the provided engines.
    for name in {j.engine for j in assembly.joints.values() if j.engine}:
        if name not in engine_map:
            eng = registry.get_compute_engine(name)
            if eng:
                engine_map[name] = eng
    if default_engine and default_engine not in engine_map:
        eng = registry.get_compute_engine(default_engine)
        if eng:
            engine_map[default_engine] = eng

    if default_engine is None and engines:
        default_engine = engines[0].name

    # Update options with resolved default_engine
    if default_engine != opts.default_engine:
        opts = replace(opts, default_engine=default_engine)

    return PhaseState(
        assembly=assembly,
        catalogs=catalogs,
        engines=engines,
        registry=registry,
        options=opts,
        catalog_map=catalog_map,
        engine_map=engine_map,
    )


# ---------------------------------------------------------------------------
# Default pipeline instance and public entry points
# ---------------------------------------------------------------------------


_DEFAULT_PIPELINE = CompilationPipeline(
    phases=[
        prune_dag_phase,
        resolve_metadata_phase,
        introspect_sources_phase,
        compile_sql_phase,
        fusion_phase,
        optimization_phase,
        strategy_resolution_phase,
        engine_boundary_phase,
        materialization_phase,
        finalization_phase,
    ]
)


def compile(
    assembly: Assembly,
    catalogs: list[Catalog],
    engines: list[ComputeEngine],
    registry: PluginRegistry,
    profile_name: str | None = None,
    target_sink: str | None = None,
    tags: list[str] | None = None,
    tag_mode: TagMode | None = None,
    default_fusion_strategy: FusionStrategyName | None = None,
    default_materialization_strategy: MaterializationStrategyName | None = None,
    resolve_references: ReferenceResolver | None = None,
    default_engine: str | None = None,
    introspect: bool | None = None,
    introspect_timeout: float | None = None,
    project_root: Path | None = None,
    options: CompileOptions | None = None,
) -> CompiledAssembly:
    """Compile an Assembly into an immutable CompiledAssembly.

    Internally executes a 10-phase compilation pipeline. Each phase is a
    pure function ``PhaseState → PhaseState``. The pipeline records per-phase
    timing in ``CompilationStats.phase_durations_ms``.

    The function signature and return type are unchanged from the pre-refactor
    version — this is a structural refactor only.
    """
    _t0 = time.monotonic()

    opts = _build_compile_options(
        options or CompileOptions(),
        profile_name,
        target_sink,
        tags,
        tag_mode,
        default_fusion_strategy,
        default_materialization_strategy,
        resolve_references,
        default_engine,
        introspect,
        introspect_timeout,
        project_root,
    )

    initial_state = _build_initial_phase_state(assembly, catalogs, engines, registry, opts)
    final_state = _DEFAULT_PIPELINE.run(initial_state)

    # If DAG pruning failed (Phase 1), pruned is None and no assembly was built.
    # Build a failed CompiledAssembly from accumulated errors.
    if final_state.compiled_assembly is None:
        all_errors = list(final_state.errors)
        all_warnings = list(dict.fromkeys(final_state.warnings))
        return CompiledAssembly(
            success=False,
            profile_name=opts.profile_name,
            catalogs=[],
            engines=[],
            adapters=[],
            joints=[],
            fused_groups=[],
            materializations=[],
            execution_order=[],
            diagnostics=_build_compilation_diagnostics(all_errors, all_warnings),
        )

    # Patch compile_duration_ms with the actual wall-clock time
    compile_duration_ms = int((time.monotonic() - _t0) * 1000)
    ca = final_state.compiled_assembly
    if ca.diagnostics.stats is not None:
        patched_stats = replace(ca.diagnostics.stats, compile_duration_ms=compile_duration_ms)
        patched_diag = replace(ca.diagnostics, stats=patched_stats)
        ca = replace(ca, diagnostics=patched_diag)

    return ca


def compile_until(
    assembly: Assembly,
    catalogs: list[Catalog],
    engines: list[ComputeEngine],
    registry: PluginRegistry,
    stop_after: str,
    options: CompileOptions | None = None,
) -> PhaseState:
    """Compile up to and including the named phase, returning intermediate state.

    This is useful for testing and debugging individual compilation phases.
    The *stop_after* parameter must be one of the ``PHASE_*`` constants
    (e.g. ``PHASE_FUSION``, ``PHASE_OPTIMIZATION``).
    """
    opts = options or CompileOptions()
    initial_state = _build_initial_phase_state(assembly, catalogs, engines, registry, opts)
    return _DEFAULT_PIPELINE.run(initial_state, stop_after=stop_after)
