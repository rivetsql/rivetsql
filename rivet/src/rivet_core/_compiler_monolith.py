"""Compilation data models and compile() function for rivet-core.

CompiledAssembly is the single source of truth produced by compile() and
consumed by the Executor. All models are immutable frozen dataclasses.
"""

from __future__ import annotations

import importlib
import sys
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

from rivet_core.assembly import Assembly
from rivet_core.checks import CompiledCheck

# ---------------------------------------------------------------------------
# Data models — imported from the canonical compiler.models / compiler.state
# modules so that there is a single type identity across the codebase.
# ---------------------------------------------------------------------------
from rivet_core.compiler.models import (
    PHASE_COMPILE_SQL,
    PHASE_ENGINE_BOUNDARIES,
    PHASE_FINALIZATION,
    PHASE_FUSION,
    PHASE_INTROSPECT_SOURCES,
    PHASE_MATERIALIZATION,
    PHASE_OPTIMIZATION,
    PHASE_PRUNE_DAG,
    PHASE_RESOLVE_METADATA,
    PHASE_STRATEGY_RESOLUTION,
    AdapterDecision,
    CompilationContext,
    CompilationDiagnostics,
    CompilationStats,
    CompilationWarning,
    CompiledAdapter,
    CompiledAssembly,
    CompiledCatalog,
    CompiledEngine,
    CompiledJoint,
    EngineBoundary,
    EngineResolutionSource,
    ExecutionWave,
    FusionStrategyName,
    IntrospectionRecord,
    Materialization,
    MaterializationStrategyName,
    MaterializationTrigger,
    OptimizationResult,
    PluginAnnotation,
    ResolvedJointMetadata,
    SchemaConfidence,
    SourceSQLAnalysis,
    SourceStats,
    TagMode,
    logger,
)
from rivet_core.compiler.state import (
    CompileOptions,
    CompilerPhase,
    PhaseState,
)
from rivet_core.errors import RivetError, SQLParseError
from rivet_core.lineage import ColumnLineage, ColumnOrigin
from rivet_core.models import Catalog, ComputeEngine, Joint, Schema
from rivet_core.optimizer import (
    CheckpointSourceInfo,
    FusedGroup,
    FusionJoint,
    _compose_cte,
    _compose_temp_view,
    cross_group_pushdown_pass,
    fusion_pass,
    pushdown_pass,
)
from rivet_core.plugins import CatalogPlugin, PluginRegistry, ReferenceResolver
from rivet_core.sql_parser import LogicalPlan, Projection, SQLParser


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
# Phase functions (phases 1–5)
# ---------------------------------------------------------------------------


class _PruneDagPhase:
    """Phase 1: Prune the DAG to the target subgraph."""

    @property
    def name(self) -> str:
        return PHASE_PRUNE_DAG

    def __call__(self, state: PhaseState) -> PhaseState:
        try:
            pruned = state.assembly.subgraph(
                target_sink=state.options.target_sink,
                tags=state.options.tags,
                tag_mode=state.options.tag_mode,
            )
        except Exception as e:
            error = RivetError(
                code="RVT-306",
                message=str(e),
                context={
                    "target_sink": state.options.target_sink,
                    "tags": state.options.tags,
                },
                remediation="Check target_sink and tags parameters.",
            )
            return replace(state, errors=(*state.errors, error))
        return replace(state, pruned=pruned)


prune_dag_phase = _PruneDagPhase()


class _ResolveMetadataPhase:
    """Phase 2: Resolve metadata (catalog, engine, adapter) for each joint."""

    @property
    def name(self) -> str:
        return PHASE_RESOLVE_METADATA

    def __call__(self, state: PhaseState) -> PhaseState:
        if state.pruned is None:
            return state  # Phase 1 failed; skip remaining phases
        topo_order = state.pruned.topological_order()
        errors: list[RivetError] = []
        warnings: list[str] = []
        adapter_decisions: list[AdapterDecision] = []
        annotations: list[PluginAnnotation] = []
        joint_metadata: dict[str, ResolvedJointMetadata] = {}
        adapter_cache: dict[tuple[str, str], str | None] = {}
        adapter_decision_cache: dict[tuple[str, str], str] = {}  # resolution_method cache

        ctx = CompilationContext(
            catalog_map=state.catalog_map,
            engine_map=state.engine_map,
            registry=state.registry,
            default_engine=state.options.default_engine,
            parser=SQLParser(),
            upstream_schemas={},
            errors=errors,
            warnings=warnings,
            adapter_cache=adapter_cache,
            project_root=state.options.project_root,
        )

        new_poisoned: set[str] = set(state.poisoned_joints)

        for jn in topo_order:
            joint = state.pruned.joints[jn]

            # Propagate poison from upstream — skip resolution entirely
            if any(up in new_poisoned for up in joint.upstream):
                new_poisoned.add(jn)
                # Still need a placeholder so later phases don't KeyError
                joint_metadata[jn] = ResolvedJointMetadata(
                    catalog=None,
                    catalog_type=None,
                    catalog_plugin=None,
                    engine_name="",
                    engine_type="",
                    resolution=None,
                    adapter_name=None,
                )
                continue

            errors_before = len(errors)
            resolved = _resolve_joint_metadata(joint, ctx)
            joint_metadata[jn] = resolved

            # Poison this joint if resolution produced errors (RVT-401/RVT-402)
            if len(errors) > errors_before:
                new_poisoned.add(jn)

            # Record PluginAnnotation for catalog plugin access
            if resolved.catalog_plugin is not None:
                annotations.append(
                    PluginAnnotation(
                        phase=PHASE_RESOLVE_METADATA,
                        joint_name=jn,
                        plugin_type="catalog_plugin",
                        plugin_class=type(resolved.catalog_plugin).__qualname__,
                        operation="get_catalog_plugin",
                        result="success",
                    )
                )

            # Record PluginAnnotation for engine plugin access
            if resolved.engine_type:
                engine_plugin = state.registry.get_engine_plugin(resolved.engine_type)
                if engine_plugin is not None:
                    annotations.append(
                        PluginAnnotation(
                            phase=PHASE_RESOLVE_METADATA,
                            joint_name=jn,
                            plugin_type="engine_plugin",
                            plugin_class=type(engine_plugin).__qualname__,
                            operation="get_engine_plugin",
                            result="success",
                        )
                    )

            # Record AdapterDecision
            catalog_type = resolved.catalog_type
            engine_type = resolved.engine_type
            if engine_type and catalog_type:
                ad_key = (engine_type, catalog_type)
                if ad_key not in adapter_decision_cache:
                    # Derive resolution method from adapter_cache populated by
                    # _resolve_joint_metadata — avoids extra registry.get_adapter() calls.
                    if adapter_cache.get(ad_key) is not None:
                        adapter_decision_cache[ad_key] = "exact_match"
                    else:
                        caps = state.registry.resolve_capabilities(engine_type, catalog_type)
                        adapter_decision_cache[ad_key] = (
                            "wildcard_fallback" if caps is not None else "none"
                        )
                resolution_method = adapter_decision_cache[ad_key]

                available_for_engine = [
                    f"{engine_type}:{ct}"
                    for ct in _list_adapter_catalog_types(state.registry, engine_type)
                ]
                available_for_catalog = [
                    f"{et}:{catalog_type}"
                    for et in _list_adapter_engine_types(state.registry, catalog_type)
                ]

                adapter_decisions.append(
                    AdapterDecision(
                        joint_name=jn,
                        engine_type=engine_type,
                        catalog_type=catalog_type,
                        adapter_found=resolved.adapter_name,
                        resolution_method=resolution_method,
                        available_for_engine=available_for_engine,
                        available_for_catalog=available_for_catalog,
                    )
                )

        return replace(
            state,
            topo_order=topo_order,
            joint_metadata=joint_metadata,
            adapter_decisions=adapter_decisions,
            poisoned_joints=frozenset(new_poisoned),
            errors=(*state.errors, *errors),
            warnings=(*state.warnings, *warnings),
            plugin_annotations=[*state.plugin_annotations, *annotations],
        )


resolve_metadata_phase = _ResolveMetadataPhase()


def _list_adapter_catalog_types(registry: PluginRegistry, engine_type: str) -> list[str]:
    """List catalog types that have adapters registered for the given engine type."""
    # Access the internal adapter dict — keys are (engine_type, catalog_type) tuples
    result: list[str] = []
    for et, ct in registry._adapters:  # type: ignore[attr-defined]
        if et == engine_type:
            result.append(ct)
    return result


def _list_adapter_engine_types(registry: PluginRegistry, catalog_type: str) -> list[str]:
    """List engine types that have adapters registered for the given catalog type."""
    result: list[str] = []
    for et, ct in registry._adapters:  # type: ignore[attr-defined]
        if ct == catalog_type:
            result.append(et)
    return result


class _IntrospectSourcesPhase:
    """Phase 3: Introspect source joints for schema and metadata."""

    @property
    def name(self) -> str:
        return PHASE_INTROSPECT_SOURCES

    def __call__(self, state: PhaseState) -> PhaseState:
        if state.pruned is None:
            return state  # Phase 1 failed; skip
        if state.topo_order is None or state.joint_metadata is None:
            return state  # Phase 2 didn't run

        if not state.options.introspect:
            return replace(
                state,
                introspection_results={},
                introspection_skipped=sum(
                    1 for jn in state.topo_order if state.pruned.joints[jn].joint_type == "source"
                ),
            )

        warnings: list[str] = []
        annotations: list[PluginAnnotation] = []
        introspection_results: dict[str, IntrospectionRecord] = {}
        attempted = 0
        succeeded = 0
        failed = 0
        skipped = 0

        # Submit all source introspections concurrently
        source_joints: list[str] = [
            jn for jn in state.topo_order if state.pruned.joints[jn].joint_type == "source"
        ]

        introspection_futures: dict[str, Any] = {}
        pool: ThreadPoolExecutor | None = None
        if source_joints:
            pool = ThreadPoolExecutor(max_workers=min(8, len(source_joints)))
            for jn in source_joints:
                joint = state.pruned.joints[jn]
                meta = state.joint_metadata[jn]
                if meta.catalog and meta.catalog_plugin:
                    introspection_futures[jn] = pool.submit(
                        _do_introspect,
                        joint,
                        meta.catalog,
                        meta.catalog_plugin,
                        state.catalog_map,
                    )

        timeout = state.options.introspect_timeout
        try:
            for jn in source_joints:
                joint = state.pruned.joints[jn]
                meta = state.joint_metadata[jn]
                attempted += 1
                t0 = time.monotonic()

                if jn not in introspection_futures:
                    # No catalog/plugin — record skipped
                    duration_ms = (time.monotonic() - t0) * 1000
                    introspection_results[jn] = IntrospectionRecord(
                        joint_name=jn,
                        catalog_type=meta.catalog_type,
                        catalog_plugin_class=(
                            type(meta.catalog_plugin).__qualname__ if meta.catalog_plugin else None
                        ),
                        result="skipped",
                        duration_ms=duration_ms,
                        schema_obtained=False,
                        stats_obtained=False,
                    )
                    skipped += 1
                    continue

                try:
                    schema, stats, local_warnings = introspection_futures[jn].result(
                        timeout=timeout
                    )
                    duration_ms = (time.monotonic() - t0) * 1000
                    warnings.extend(local_warnings)

                    got_schema = schema is not None
                    got_stats = stats is not None
                    result_status = "success" if got_schema or got_stats else "failed"

                    if result_status == "success":
                        succeeded += 1
                    else:
                        failed += 1

                    introspection_results[jn] = IntrospectionRecord(
                        joint_name=jn,
                        catalog_type=meta.catalog_type,
                        catalog_plugin_class=(
                            type(meta.catalog_plugin).__qualname__ if meta.catalog_plugin else None
                        ),
                        result=result_status,
                        duration_ms=duration_ms,
                        schema_obtained=got_schema,
                        stats_obtained=got_stats,
                    )

                    # Record plugin annotations for catalog introspection calls
                    if meta.catalog_plugin is not None:
                        plugin_cls = type(meta.catalog_plugin).__qualname__
                        annotations.append(
                            PluginAnnotation(
                                phase=PHASE_INTROSPECT_SOURCES,
                                joint_name=jn,
                                plugin_type="catalog_plugin",
                                plugin_class=plugin_cls,
                                operation="get_schema",
                                result="success" if got_schema else "failed",
                            )
                        )
                        annotations.append(
                            PluginAnnotation(
                                phase=PHASE_INTROSPECT_SOURCES,
                                joint_name=jn,
                                plugin_type="catalog_plugin",
                                plugin_class=plugin_cls,
                                operation="get_metadata",
                                result="success" if got_stats else "failed",
                            )
                        )

                except TimeoutError:
                    duration_ms = (time.monotonic() - t0) * 1000
                    warnings.append(f"Introspection timed out for source '{jn}' after {timeout}s")
                    failed += 1
                    introspection_results[jn] = IntrospectionRecord(
                        joint_name=jn,
                        catalog_type=meta.catalog_type,
                        catalog_plugin_class=(
                            type(meta.catalog_plugin).__qualname__ if meta.catalog_plugin else None
                        ),
                        result="timeout",
                        duration_ms=duration_ms,
                        schema_obtained=False,
                        stats_obtained=False,
                        error_message=f"Timed out after {timeout}s",
                    )
                except Exception as exc:
                    duration_ms = (time.monotonic() - t0) * 1000
                    warnings.append(f"Introspection failed for source '{jn}': {exc}")
                    failed += 1
                    introspection_results[jn] = IntrospectionRecord(
                        joint_name=jn,
                        catalog_type=meta.catalog_type,
                        catalog_plugin_class=(
                            type(meta.catalog_plugin).__qualname__ if meta.catalog_plugin else None
                        ),
                        result="failed",
                        duration_ms=duration_ms,
                        schema_obtained=False,
                        stats_obtained=False,
                        error_message=str(exc),
                    )
        finally:
            if pool is not None:
                pool.shutdown(wait=False)

        return replace(
            state,
            introspection_results=introspection_results,
            introspection_attempted=attempted,
            introspection_succeeded=succeeded,
            introspection_failed=failed,
            introspection_skipped=skipped,
            warnings=(*state.warnings, *warnings),
            plugin_annotations=[*state.plugin_annotations, *annotations],
        )


introspect_sources_phase = _IntrospectSourcesPhase()


class _CompileSQLPhase:
    """Phase 4: Compile SQL, build CompiledJoints, upstream schemas."""

    @property
    def name(self) -> str:
        return PHASE_COMPILE_SQL

    def __call__(self, state: PhaseState) -> PhaseState:
        if state.pruned is None or state.topo_order is None or state.joint_metadata is None:
            return state  # Earlier phase failed; skip

        errors: list[RivetError] = []
        warnings: list[str] = []
        upstream_schemas: dict[str, Schema] = {}
        introspected_sources: set[str] = set()

        ctx = CompilationContext(
            catalog_map=state.catalog_map,
            engine_map=state.engine_map,
            registry=state.registry,
            default_engine=state.options.default_engine,
            parser=SQLParser(),
            upstream_schemas=upstream_schemas,
            errors=errors,
            warnings=warnings,
            adapter_cache={},
            project_root=state.options.project_root,
        )

        compiled_joints: list[CompiledJoint] = []
        introspection_results = state.introspection_results or {}
        new_poisoned: set[str] = set(state.poisoned_joints)

        for jn in state.topo_order:
            joint = state.pruned.joints[jn]

            # Skip poisoned joints and propagate poison downstream
            if jn in new_poisoned or any(up in new_poisoned for up in joint.upstream):
                new_poisoned.add(jn)
                continue

            # Compile without inline introspection — Phase 3 handled it
            cj = _compile_joint_with_context(
                joint,
                ctx,
                introspect=False,
                introspect_timeout=state.options.introspect_timeout,
            )

            # Attach introspection results from Phase 3
            if joint.joint_type == "source" and state.options.introspect:
                record = introspection_results.get(jn)
                if record and record.result == "success":
                    # Re-run introspection to get actual schema/stats objects
                    meta = state.joint_metadata[jn]
                    if meta.catalog and meta.catalog_plugin:
                        try:
                            schema, stats, local_warnings = _do_introspect(
                                joint,
                                meta.catalog,
                                meta.catalog_plugin,
                                state.catalog_map,
                            )
                            warnings.extend(local_warnings)
                            if schema is not None or stats is not None:
                                cj = replace(
                                    cj,
                                    output_schema=schema or cj.output_schema,
                                    source_stats=stats if stats is not None else cj.source_stats,
                                )
                        except Exception:
                            pass

                    # Re-run source inline transform validation with introspected schema
                    if cj.output_schema is not None and cj.logical_plan is not None:
                        _warn_unresolved_column_refs(
                            cj.name, cj.logical_plan, cj.output_schema, warnings
                        )
                        transformed = _compute_source_transform_schema(
                            cj.name,
                            cj.logical_plan.projections,
                            cj.output_schema,
                            warnings,
                        )
                        if transformed is not None:
                            cj = replace(cj, output_schema=transformed)

                if cj.output_schema is not None:
                    introspected_sources.add(cj.name)
                    upstream_schemas[cj.name] = cj.output_schema

            compiled_joints.append(cj)

        # Infer sink schemas and assign schema confidence
        compiled_joints = _infer_sink_schemas(compiled_joints, warnings)
        compiled_joints = _assign_schema_confidence(compiled_joints, introspected_sources)
        cj_map: dict[str, CompiledJoint] = {cj.name: cj for cj in compiled_joints}

        # Checkpoint no-downstream warning
        referenced_as_upstream = {up for cj in compiled_joints for up in cj.upstream}
        for cj in compiled_joints:
            if cj.type == "checkpoint" and cj.name not in referenced_as_upstream:
                warnings.append(f"Checkpoint joint '{cj.name}' has no downstream consumers.")

        return replace(
            state,
            compiled_joints=compiled_joints,
            cj_map=cj_map,
            upstream_schemas=upstream_schemas,
            poisoned_joints=frozenset(new_poisoned),
            errors=(*state.errors, *errors),
            warnings=(*state.warnings, *warnings),
        )


compile_sql_phase = _CompileSQLPhase()


class _FusionPhase:
    """Phase 5: Build fused groups from compiled joints."""

    @property
    def name(self) -> str:
        return PHASE_FUSION

    def __call__(self, state: PhaseState) -> PhaseState:
        if state.compiled_joints is None or state.cj_map is None:
            return state  # Earlier phase failed; skip

        from rivet_core.executor import _is_sql_translatable

        engine_plugin_cache: dict[str, Any] = {}

        def _engine_plugin(engine_type: str) -> Any:
            if not engine_type:
                return None
            if engine_type not in engine_plugin_cache:
                engine_plugin_cache[engine_type] = state.registry.get_engine_plugin(engine_type)
            return engine_plugin_cache[engine_type]

        fusion_joints: list[FusionJoint] = []
        for cj in state.compiled_joints:
            eng = state.engine_map.get(cj.engine)
            et = eng.engine_type if eng else ""
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

        fused_groups = fusion_pass(
            fusion_joints,
            fusion_strategy=state.options.default_fusion_strategy,
        )

        joint_to_group: dict[str, str] = {}
        for group in fused_groups:
            for jn in group.joints:
                joint_to_group[jn] = group.id

        return replace(
            state,
            fused_groups=fused_groups,
            joint_to_group=joint_to_group,
        )


fusion_phase = _FusionPhase()


# ---------------------------------------------------------------------------
# Phase functions (phases 6–10)
# ---------------------------------------------------------------------------


class _OptimizationPhase:
    """Phase 6: Pushdown optimization passes."""

    @property
    def name(self) -> str:
        return PHASE_OPTIMIZATION

    def __call__(self, state: PhaseState) -> PhaseState:
        if state.compiled_joints is None or state.cj_map is None or state.fused_groups is None:
            return state  # Earlier phase failed; skip

        annotations: list[PluginAnnotation] = []

        logical_plans: dict[str, LogicalPlan | None] = {
            cj.name: cj.logical_plan for cj in state.compiled_joints
        }
        catalog_types_map: dict[str, str | None] = {
            cj.name: cj.catalog_type for cj in state.compiled_joints
        }
        cap_map: dict[str, list[str]] = {}
        for cj in state.compiled_joints:
            eng = state.engine_map.get(cj.engine)
            et = eng.engine_type if eng else ""
            if et and cj.catalog_type:
                key = f"{et}:{cj.catalog_type}"
                if key not in cap_map:
                    caps = state.registry.resolve_capabilities(et, cj.catalog_type)
                    if caps is not None:
                        cap_map[key] = caps
                        annotations.append(
                            PluginAnnotation(
                                phase=PHASE_OPTIMIZATION,
                                joint_name=cj.name,
                                plugin_type="adapter",
                                plugin_class=f"{et}:{cj.catalog_type}",
                                operation="resolve_capabilities",
                                result="success",
                            )
                        )

        fused_groups = pushdown_pass(state.fused_groups, logical_plans, cap_map, catalog_types_map)

        # Cross-group predicate pushdown
        cj_map = dict(state.cj_map)
        fused_groups, xgroup_results = cross_group_pushdown_pass(
            fused_groups, cj_map, cap_map, catalog_types_map
        )
        for result in xgroup_results:
            if result.target_joint and result.target_joint in cj_map:
                cj = cj_map[result.target_joint]
                cj_map[result.target_joint] = replace(cj, optimizations=[*cj.optimizations, result])

        return replace(
            state,
            fused_groups=fused_groups,
            cj_map=cj_map,
            plugin_annotations=[*state.plugin_annotations, *annotations],
        )


optimization_phase = _OptimizationPhase()


class _StrategyResolutionPhase:
    """Phase 7: Strategy resolution, reference resolution, checkpoint CTEs."""

    @property
    def name(self) -> str:
        return PHASE_STRATEGY_RESOLUTION

    def __call__(self, state: PhaseState) -> PhaseState:
        if (
            state.fused_groups is None
            or state.cj_map is None
            or state.compiled_joints is None
            or state.joint_to_group is None
        ):
            return state  # Earlier phase failed; skip

        errors: list[RivetError] = []
        warnings: list[str] = []
        annotations: list[PluginAnnotation] = []

        # Make a mutable copy of cj_map for reference resolution updates
        cj_map = dict(state.cj_map)

        fused_groups = _resolve_strategy(
            state.fused_groups, cj_map, state.options.default_fusion_strategy, errors
        )
        fused_groups = _resolve_references(
            fused_groups,
            cj_map,
            state.compiled_joints,
            state.engine_map,
            state.catalog_map,
            state.registry,
            state.options.resolve_references,
            warnings,
        )

        # Record PluginAnnotation for reference resolver invocations
        for group in fused_groups:
            if group.resolved_sql and group.resolved_sql != group.fused_sql:
                plugin = state.registry.get_engine_plugin(group.engine_type)
                if plugin:
                    resolver = plugin.get_reference_resolver()
                    if resolver:
                        before_sql = (group.fused_sql or "")[:200]
                        after_sql = (group.resolved_sql or "")[:200]
                        annotations.append(
                            PluginAnnotation(
                                phase=PHASE_STRATEGY_RESOLUTION,
                                joint_name=None,
                                plugin_type="reference_resolver",
                                plugin_class=type(resolver).__qualname__,
                                operation="resolve_references",
                                result="success",
                                detail=f"Group '{group.id}' before: {before_sql} | after: {after_sql}",
                            )
                        )

        # Build checkpoint source metadata, then inject checkpoint CTEs
        fused_groups = _build_checkpoint_sources(
            fused_groups, cj_map, state.joint_to_group, state.registry, warnings
        )
        fused_groups = _inject_checkpoint_ctes(
            fused_groups, cj_map, state.catalog_map, state.registry
        )

        return replace(
            state,
            fused_groups=fused_groups,
            cj_map=cj_map,
            errors=(*state.errors, *errors),
            warnings=(*state.warnings, *warnings),
            plugin_annotations=[*state.plugin_annotations, *annotations],
        )


strategy_resolution_phase = _StrategyResolutionPhase()


class _MaterializationPhase:
    """Phase 9: Determine materialization points.

    Runs after ``_EngineBoundaryPhase`` so it can use the validated
    ``state.engine_boundaries`` to detect engine-instance changes instead
    of performing its own ad-hoc engine-name comparison.
    """

    @property
    def name(self) -> str:
        return PHASE_MATERIALIZATION

    def __call__(self, state: PhaseState) -> PhaseState:
        if state.cj_map is None or state.joint_to_group is None:
            return state  # Earlier phase failed; skip

        # Build the set of boundary joints from the engine boundary phase
        boundary_joints: set[str] = set()
        if state.engine_boundaries:
            for boundary in state.engine_boundaries:
                for jn in boundary.boundary_joints:
                    boundary_joints.add(jn)

        # _determine_materializations mutates cj_map for optimization results
        cj_map = dict(state.cj_map)
        materializations = _determine_materializations(
            cj_map,
            state.joint_to_group,
            state.engine_map,
            state.registry,
            state.options.default_materialization_strategy,
            boundary_joints,
        )

        return replace(
            state,
            materializations=materializations,
            cj_map=cj_map,
        )


materialization_phase = _MaterializationPhase()


class _EngineBoundaryPhase:
    """Phase 8: Detect engine boundaries and resolve materialization strategies."""

    @property
    def name(self) -> str:
        return PHASE_ENGINE_BOUNDARIES

    def __call__(self, state: PhaseState) -> PhaseState:
        if state.fused_groups is None or state.cj_map is None or state.joint_to_group is None:
            return state  # Earlier phase failed; skip

        warnings: list[str] = []
        adapter_decisions: list[AdapterDecision] = []

        engine_boundaries = _detect_engine_boundaries(
            state.fused_groups,
            state.cj_map,
            state.joint_to_group,
            state.registry,
            warnings,
        )

        # Record AdapterDecision for each CrossJointAdapter lookup
        for boundary in engine_boundaries:
            # Determine resolution method from the adapter lookup result
            adapter_obj = state.registry.get_cross_joint_adapter(
                boundary.consumer_engine_type,
                boundary.producer_engine_type,
            )
            if adapter_obj is not None:
                resolution = "exact_match"
            else:
                resolution = "none"

            # List registered cross-joint adapters involving this consumer engine
            available = [
                f"{cons}:{prod}"
                for (cons, prod) in state.registry._cross_joint_adapters
                if cons == boundary.consumer_engine_type or prod == boundary.producer_engine_type
            ]

            adapter_decisions.append(
                AdapterDecision(
                    joint_name=f"boundary:{boundary.producer_group_id}->{boundary.consumer_group_id}",
                    engine_type=boundary.consumer_engine_type,
                    catalog_type=None,
                    adapter_found=boundary.adapter_strategy,
                    resolution_method=resolution,
                    available_for_engine=available,
                    available_for_catalog=[],
                    is_cross_joint=True,
                    producer_engine_type=boundary.producer_engine_type,
                    consumer_engine_type=boundary.consumer_engine_type,
                )
            )

        # Resolve materialization_strategy_name per group
        fused_groups = list(state.fused_groups)
        for i, group in enumerate(fused_groups):
            resolved_mat_name: str | None = None
            for jn in group.joints:
                cj = state.cj_map.get(jn)
                if cj and cj.materialization_strategy_override:
                    resolved_mat_name = cj.materialization_strategy_override
                    break
            if resolved_mat_name is None:
                engine_plugin = state.registry.get_engine_plugin(group.engine_type)
                if engine_plugin:
                    resolved_mat_name = engine_plugin.materialization_strategy_name
            if not resolved_mat_name:
                resolved_mat_name = "arrow"
            if resolved_mat_name != group.materialization_strategy_name:
                fused_groups[i] = replace(group, materialization_strategy_name=resolved_mat_name)

        # Collect checkpoint-downstream adapter pairs
        existing_adapter_decisions = list(state.adapter_decisions or [])

        return replace(
            state,
            engine_boundaries=engine_boundaries,
            fused_groups=fused_groups,
            adapter_decisions=[*existing_adapter_decisions, *adapter_decisions],
            warnings=(*state.warnings, *warnings),
        )


engine_boundary_phase = _EngineBoundaryPhase()


class _FinalizationPhase:
    """Phase 10: Build final CompiledAssembly."""

    @property
    def name(self) -> str:
        return PHASE_FINALIZATION

    def __call__(self, state: PhaseState) -> PhaseState:
        if (
            state.topo_order is None
            or state.cj_map is None
            or state.compiled_joints is None
            or state.fused_groups is None
            or state.joint_to_group is None
            or state.materializations is None
        ):
            return state  # Earlier phase failed; skip

        from rivet_core.sql_resolver import resolve_execution_sql

        engine_boundaries = state.engine_boundaries or []
        warnings: list[str] = []

        # Build compiled catalogs/engines/adapters from compiled joints
        compiled_catalogs = _build_compiled_catalogs(state.compiled_joints, state.catalogs)
        compiled_engines = _build_compiled_engines(
            state.compiled_joints, state.engines, state.registry
        )
        compiled_adapters = _build_compiled_adapters(
            state.compiled_joints, state.engine_map, state.registry
        )

        # Include checkpoint-downstream adapter pairs
        existing_adapter_keys = {(a.engine_type, a.catalog_type) for a in compiled_adapters}
        for group in state.fused_groups:
            for cp_info in group.checkpoint_sources.values():
                if cp_info.adapter and cp_info.catalog_type:
                    adapter = state.registry.get_adapter(group.engine_type, cp_info.catalog_type)
                    if adapter:
                        key = (adapter.target_engine_type, adapter.catalog_type)
                        if key not in existing_adapter_keys:
                            existing_adapter_keys.add(key)
                            compiled_adapters.append(
                                CompiledAdapter(
                                    engine_type=adapter.target_engine_type,
                                    catalog_type=adapter.catalog_type,
                                    source=adapter.source,
                                )
                            )

        # Resolve execution SQL per group
        groups_with_materialized_inputs: set[str] = set()
        for boundary in engine_boundaries:
            groups_with_materialized_inputs.add(boundary.consumer_group_id)

        group_execution_sql: dict[str, str | None] = {}
        for group in state.fused_groups:
            adapter_read_sources = {
                jn
                for jn in group.joints
                if state.cj_map.get(jn) and state.cj_map[jn].type == "source"
            }
            has_materialized_inputs = group.id in groups_with_materialized_inputs
            execution_sql = resolve_execution_sql(
                group,
                state.cj_map,
                adapter_read_sources,
                has_materialized_inputs=has_materialized_inputs,
            )
            group_execution_sql[group.id] = execution_sql

        # Build final joints with fused_group_id and execution_sql
        final_joints = [
            replace(
                state.cj_map[jn],
                fused_group_id=state.joint_to_group.get(jn),
                execution_sql=(
                    group_execution_sql.get(gid)
                    if (gid := state.joint_to_group.get(jn)) is not None
                    else None
                ),
            )
            for jn in state.topo_order
            if jn not in state.poisoned_joints and jn in state.cj_map
        ]

        # Execution order
        execution_order: list[str] = []
        seen_groups: set[str] = set()
        for jn in state.topo_order:
            if jn in state.poisoned_joints:
                continue
            gid = state.joint_to_group.get(jn)
            if gid and gid not in seen_groups:
                seen_groups.add(gid)
                execution_order.append(gid)

        # Parallel execution plan
        all_warnings = [*state.warnings, *warnings]
        deduped_warnings = list(dict.fromkeys(all_warnings))
        parallel_execution_plan = _compute_parallel_execution_plan(
            state.fused_groups, state.cj_map, deduped_warnings
        )

        # Compilation stats with phase timings
        compilation_stats = CompilationStats(
            compile_duration_ms=0,  # placeholder — pipeline will set total
            joints_with_schema=sum(1 for j in final_joints if j.output_schema is not None),
            joints_total=len(final_joints),
            introspection_attempted=state.introspection_attempted,
            introspection_succeeded=state.introspection_succeeded,
            introspection_failed=state.introspection_failed,
            introspection_skipped=state.introspection_skipped,
            phase_durations_ms=dict(state.phase_timings),
        )

        all_errors = list(state.errors)
        compiled_assembly = CompiledAssembly(
            success=len(all_errors) == 0,
            profile_name=state.options.profile_name,
            catalogs=compiled_catalogs,
            engines=compiled_engines,
            adapters=compiled_adapters,
            joints=final_joints,
            fused_groups=state.fused_groups,
            materializations=state.materializations,
            execution_order=execution_order,
            diagnostics=_build_compilation_diagnostics(
                all_errors,
                deduped_warnings,
                compilation_stats,
            ),
            engine_boundaries=engine_boundaries,
            parallel_execution_plan=parallel_execution_plan,
            adapter_decisions=list(state.adapter_decisions or []),
            introspection_records=list((state.introspection_results or {}).values()),
            plugin_annotations=list(state.plugin_annotations),
        )

        return replace(
            state,
            compiled_assembly=compiled_assembly,
            warnings=tuple(deduped_warnings),
        )


finalization_phase = _FinalizationPhase()


def _build_compilation_diagnostics(
    errors: list[RivetError],
    warnings: list[str],
    stats: CompilationStats | None = None,
) -> CompilationDiagnostics:
    return CompilationDiagnostics(
        errors=errors,
        warnings=[CompilationWarning(message=warning) for warning in warnings],
        stats=stats,
    )


def _resolve_engine(
    joint: Joint,
    engines: dict[str, ComputeEngine],
    default_engine: str | None,
) -> tuple[str, str, EngineResolutionSource | None]:
    """Resolve engine for a joint. Returns (engine_name, engine_type, resolution_path) or raises."""
    # Joint-level override
    if joint.engine:
        engine = engines.get(joint.engine)
        if engine:
            return engine.name, engine.engine_type, "joint_override"
        return joint.engine, "", "joint_override"  # will error on adapter lookup

    # Profile-level default
    if default_engine:
        engine = engines.get(default_engine)
        if engine:
            return engine.name, engine.engine_type, "project_default"
        return default_engine, "", "project_default"

    return "", "", ""


def _verify_callable(function_path: str, project_root: Path | None = None) -> bool:
    """Check if a colon-separated function path (module:func) is importable.

    When *project_root* is provided it is temporarily prepended to
    ``sys.path`` so that project-local modules (e.g. ``joints/``) are
    importable without the user having to set ``PYTHONPATH``.
    """
    parts = function_path.rsplit(":", 1)
    if len(parts) != 2:
        return False
    module_path, func_name = parts
    root_str = str(project_root) if project_root else None
    added = False
    try:
        if root_str and root_str not in sys.path:
            sys.path.insert(0, root_str)
            added = True
        mod = importlib.import_module(module_path)
        return callable(getattr(mod, func_name, None))
    except Exception:
        return False
    finally:
        if added and root_str:
            try:
                sys.path.remove(root_str)
            except ValueError:
                pass


def _resolve_table_map_name(
    joint: Joint,
    catalog: Catalog | None,
    catalog_map: dict[str, Catalog],
) -> str:
    """Resolve the physical table name via ``table_map``.

    Applies the same ``table_map`` alias resolution used during compilation
    (Phase 4) so that introspection, compilation, and execution all see the
    same physical name.

    Falls back to ``joint.table or joint.path or joint.name`` when no
    ``table_map`` entry matches.
    """
    lookup_key = joint.table or joint.name
    cat_obj = catalog

    # When the joint's catalog name doesn't match any profile catalog,
    # search all catalogs for a table_map entry that matches.
    if cat_obj is None and joint.catalog:
        for candidate in catalog_map.values():
            candidate_map: dict[str, str] = candidate.options.get("table_map", {})
            if lookup_key in candidate_map:
                cat_obj = candidate
                break

    if cat_obj is not None:
        table_map: dict[str, str] = cat_obj.options.get("table_map", {})
        if lookup_key in table_map:
            return table_map[lookup_key]

    return joint.table or joint.path or joint.name


def _do_introspect(
    joint: Joint,
    catalog: Catalog,
    catalog_plugin: CatalogPlugin,
    catalog_map: dict[str, Catalog],
) -> tuple[Schema | None, SourceStats | None, list[str]]:
    """Inner introspection logic — runs inside thread for timeout."""
    if not catalog or not catalog_plugin:
        return None, None, []

    from rivet_core.models import Column

    schema: Schema | None = None
    source_stats: SourceStats | None = None
    local_warnings: list[str] = []
    table_name = _resolve_table_map_name(joint, catalog, catalog_map)

    # Schema
    try:
        obj_schema = catalog_plugin.get_schema(catalog, table_name)
        schema = Schema(
            columns=[
                Column(name=c.name, type=c.type, nullable=c.nullable) for c in obj_schema.columns
            ]
        )
    except NotImplementedError:
        pass
    except Exception as e:
        local_warnings.append(f"Introspection failed for source '{joint.name}': {e}")

    # Metadata
    try:
        meta = catalog_plugin.get_metadata(catalog, table_name)
        if meta is not None:
            source_stats = SourceStats(
                row_count=meta.row_count,
                size_bytes=meta.size_bytes,
                last_modified=meta.last_modified,
                partition_count=(len(meta.partitioning.partitions) if meta.partitioning else None),
            )
    except NotImplementedError:
        pass
    except Exception as e:
        local_warnings.append(f"Introspection failed for source '{joint.name}': {e}")

    return schema, source_stats, local_warnings


def _introspect_source(
    joint: Joint,
    catalog: Catalog | None,
    catalog_plugin: CatalogPlugin | None,
    warnings: list[str],
    timeout_seconds: float = 5.0,
    catalog_map: dict[str, Catalog] | None = None,
) -> tuple[Schema | None, SourceStats | None]:
    """Attempt introspection for source joints. Returns (schema, source_stats).

    Enforces per-source timeout. Never raises.
    """
    if not catalog or not catalog_plugin:
        return None, None
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            _do_introspect,
            joint,
            catalog,
            catalog_plugin,
            catalog_map or {},
        )
        try:
            schema, source_stats, local_warnings = future.result(timeout=timeout_seconds)
            warnings.extend(local_warnings)
            return schema, source_stats
        except TimeoutError:
            warnings.append(
                f"Introspection timed out for source '{joint.name}' after {timeout_seconds}s"
            )
            return None, None


def _resolve_adapter(
    engine_type: str,
    catalog_type: str | None,
    engine_name: str,
    joint_name: str,
    registry: PluginRegistry,
    errors: list[RivetError],
    adapter_cache: dict[tuple[str, str], str | None] | None = None,
) -> str | None:
    """Resolve adapter for an engine/catalog pair. Returns adapter key or None."""
    if not engine_type or not catalog_type:
        return None

    key = (engine_type, catalog_type)
    if adapter_cache is not None and key in adapter_cache:
        return adapter_cache[key]

    adapter = registry.get_adapter(engine_type, catalog_type)
    if adapter:
        result = f"{engine_type}:{catalog_type}"
        if adapter_cache is not None:
            adapter_cache[key] = result
        return result

    caps = registry.resolve_capabilities(engine_type, catalog_type)
    if caps is None and engine_name:
        errors.append(
            RivetError(
                code="RVT-402",
                message=f"Engine '{engine_name}' (type '{engine_type}') does not support "
                f"catalog type '{catalog_type}' for joint '{joint_name}'.",
                context={
                    "joint": joint_name,
                    "engine": engine_name,
                    "engine_type": engine_type,
                    "catalog_type": catalog_type,
                },
                remediation=f"Register an adapter for ({engine_type}, {catalog_type}) "
                f"or use an engine that supports this catalog type.",
            )
        )

    if adapter_cache is not None:
        adapter_cache[key] = None
    return None


def _compile_sql_joint(
    joint: Joint,
    engine_type: str,
    registry: PluginRegistry,
    parser: SQLParser,
    upstream_schemas: dict[str, Schema],
    errors: list[RivetError],
    warnings: list[str],
) -> tuple[LogicalPlan | None, list[ColumnLineage], str | None, str | None, Schema | None]:
    """Compile SQL parsing, lineage, and translation for a SQL joint.

    Returns (logical_plan, column_lineage, sql_translated, engine_dialect, output_schema).
    """
    logical_plan: LogicalPlan | None = None
    column_lineage: list[ColumnLineage] = []
    sql_translated: str | None = None
    output_schema: Schema | None = None
    sql_dialect = joint.dialect

    engine_plugin = registry.get_engine_plugin(engine_type) if engine_type else None
    engine_dialect = getattr(engine_plugin, "dialect", None) if engine_plugin else None

    try:
        assert joint.sql is not None, f"SQL must not be None for joint '{joint.name}'"
        ast = parser.parse(joint.sql, dialect=sql_dialect)
        ast = parser.normalize(ast)
        parser.extract_table_references(ast, dialect=sql_dialect)

        joint_upstream_schemas: dict[str, Schema] = {
            up: upstream_schemas[up] for up in joint.upstream if up in upstream_schemas
        }

        logical_plan = parser.extract_logical_plan(ast)

        inferred_schema, schema_warnings = parser.infer_schema(
            ast, joint_upstream_schemas, dialect=sql_dialect
        )
        warnings.extend(schema_warnings)
        if inferred_schema:
            output_schema = inferred_schema

        column_lineage = parser.extract_lineage(ast, joint_upstream_schemas, joint_name=joint.name)

        target_dialect = sql_dialect or engine_dialect or "duckdb"
        if sql_dialect and target_dialect != sql_dialect:
            try:
                sql_translated = parser.translate(ast, sql_dialect, target_dialect)
            except SQLParseError as e:
                errors.append(e.error)
        elif engine_dialect and engine_dialect != (sql_dialect or ""):
            try:
                source = sql_dialect or "duckdb"
                sql_translated = parser.translate(ast, source, engine_dialect)
            except SQLParseError as e:
                errors.append(e.error)

    except SQLParseError as e:
        errors.append(e.error)

    return logical_plan, column_lineage, sql_translated, engine_dialect, output_schema


def _compile_python_joint(
    joint: Joint,
    errors: list[RivetError],
    project_root: Path | None = None,
) -> list[ColumnLineage]:
    """Validate and produce lineage for a PythonJoint."""
    if joint.function and not _verify_callable(joint.function, project_root):
        errors.append(
            RivetError(
                code="RVT-753",
                message=f"PythonJoint '{joint.name}' references non-importable "
                f"callable '{joint.function}'.",
                context={"joint": joint.name, "function": joint.function},
                remediation="Ensure the function path is importable and uses the "
                "form 'module:callable' (e.g., 'mymodule:my_function').",
            )
        )
    return [
        ColumnLineage(
            output_column="*",
            transform="opaque",
            origins=[ColumnOrigin(joint=up, column="*") for up in joint.upstream],
            expression=None,
        )
    ]


def _compile_checks(
    joint: Joint,
    errors: list[RivetError],
) -> list[CompiledCheck]:
    """Compile assertion/audit checks for a joint."""
    checks: list[CompiledCheck] = []
    for assertion in joint.assertions:
        if assertion.phase == "audit" and joint.joint_type != "sink":
            errors.append(
                RivetError(
                    code="RVT-651",
                    message=f"Audit assertion on non-sink joint '{joint.name}' is not allowed.",
                    context={"joint": joint.name, "assertion_type": assertion.type},
                    remediation="Move audit assertions to sink joints only, "
                    "or change the phase to 'assertion'.",
                )
            )
        checks.append(
            CompiledCheck(
                type=assertion.type,
                severity=assertion.severity,
                config=assertion.config,
                phase=assertion.phase,
            )
        )
    return checks


def _warn_unresolved_column_refs(
    joint_name: str,
    logical_plan: LogicalPlan,
    output_schema: Schema | None,
    warnings: list[str],
) -> None:
    """Emit warnings for column references not found in the introspected catalog schema."""
    if output_schema is None:
        return

    known_columns = {col.name.lower() for col in output_schema.columns}

    # Warn about filter references to unknown columns
    for pred in logical_plan.predicates:
        for col_ref in pred.columns:
            col_name = col_ref.rsplit(".", 1)[-1].lower()
            if col_name not in known_columns:
                warnings.append(
                    f"Source joint '{joint_name}' filter references column "
                    f"'{col_ref}' not found in catalog schema."
                )

    # Warn about column expression references to unknown columns
    for proj in logical_plan.projections:
        for col_ref in proj.source_columns:
            col_name = col_ref.rsplit(".", 1)[-1].lower()
            if col_name not in known_columns:
                alias_label = proj.alias or proj.expression
                warnings.append(
                    f"Source joint '{joint_name}' column '{alias_label}' "
                    f"expression references '{col_ref}' not found in catalog schema."
                )


def _analyze_source_sql(
    joint: Joint,
    parser: SQLParser,
) -> SourceSQLAnalysis:
    """Parse source SQL once and reuse the AST for rewrite and logical-plan extraction.

    Source SQL parsing remains best-effort: failures produce no logical plan and
    leave the original SQL untouched.
    """
    if joint.joint_type != "source" or not joint.sql:
        return SourceSQLAnalysis(joint=joint, logical_plan=None, parsed_ast=None)

    try:
        from sqlglot import exp as sg_exp

        parsed_ast = parser.parse(joint.sql, dialect=joint.dialect)

        if joint.table:
            source_table = sg_exp.to_table(joint.table)
            for table_node in parsed_ast.find_all(sg_exp.Table):
                if table_node.name in ("__self", joint.name):
                    table_node.set("this", source_table.this)
                    table_node.set("db", source_table.args.get("db"))
                    table_node.set("catalog", source_table.args.get("catalog"))
            rewritten_sql = parsed_ast.sql()
            if rewritten_sql != joint.sql:
                joint = replace(joint, sql=rewritten_sql)

        normalized_ast = parser.normalize(parsed_ast)
        logical_plan = parser.extract_logical_plan(normalized_ast)
        return SourceSQLAnalysis(joint=joint, logical_plan=logical_plan, parsed_ast=parsed_ast)
    except Exception:
        return SourceSQLAnalysis(joint=joint, logical_plan=None, parsed_ast=None)


def _validate_source_inline_transforms(
    joint_name: str,
    logical_plan: LogicalPlan | None,
    output_schema: Schema | None,
    errors: list[RivetError],
    warnings: list[str],
    sql: str | None = None,
    parsed_ast: Any | None = None,
) -> Schema | None:
    """Validate source inline transforms and compute transformed output schema.

    Checks:
    1. Single-table constraint (no joins, CTEs, subqueries) → RVT-760, RVT-761, RVT-762
    2. Column/filter reference resolution against introspected schema (warnings)
    3. Transformed output schema computation from LogicalPlan projections

    Returns the transformed output schema, or the original if no transforms apply.
    """
    if logical_plan is None:
        return output_schema

    # --- Single-table constraint checks ---

    # RVT-760: Reject JOINs (also catches comma-separated FROM which parses as implicit join)
    if logical_plan.joins:
        errors.append(
            RivetError(
                code="RVT-760",
                message=(
                    f"Source joint '{joint_name}' violates single-table constraint: "
                    f"JOINs are not allowed in source SQL."
                ),
                context={"joint": joint_name},
                remediation="Remove JOINs from the source SQL. Source joints must reference a single table.",
            )
        )

    # RVT-761 / RVT-762: Detect CTEs and subqueries via sqlglot AST.
    # The LogicalPlan's source_tables don't reliably surface these, so we
    # parse the SQL directly when available.
    if parsed_ast is not None:
        try:
            from sqlglot import exp as sg_exp

            if parsed_ast.find(sg_exp.With):
                errors.append(
                    RivetError(
                        code="RVT-761",
                        message=(
                            f"Source joint '{joint_name}' violates single-table constraint: "
                            f"CTEs are not allowed in source SQL."
                        ),
                        context={"joint": joint_name},
                        remediation="Remove CTEs from the source SQL. Source joints must reference a single table.",
                    )
                )

            if parsed_ast.find(sg_exp.Subquery):
                errors.append(
                    RivetError(
                        code="RVT-762",
                        message=(
                            f"Source joint '{joint_name}' violates single-table constraint: "
                            f"subqueries are not allowed in source SQL."
                        ),
                        context={"joint": joint_name},
                        remediation="Remove subqueries from the source SQL. Use simple WHERE conditions only.",
                    )
                )
        except Exception:
            pass
    elif sql:
        try:
            import sqlglot
            from sqlglot import exp as sg_exp

            parsed = sqlglot.parse_one(sql)

            # RVT-761: Reject CTEs (WITH clause)
            if parsed.find(sg_exp.With):
                errors.append(
                    RivetError(
                        code="RVT-761",
                        message=(
                            f"Source joint '{joint_name}' violates single-table constraint: "
                            f"CTEs are not allowed in source SQL."
                        ),
                        context={"joint": joint_name},
                        remediation="Remove CTEs from the source SQL. Source joints must reference a single table.",
                    )
                )

            # RVT-762: Reject subqueries
            if parsed.find(sg_exp.Subquery):
                errors.append(
                    RivetError(
                        code="RVT-762",
                        message=(
                            f"Source joint '{joint_name}' violates single-table constraint: "
                            f"subqueries are not allowed in source SQL."
                        ),
                        context={"joint": joint_name},
                        remediation="Remove subqueries from the source SQL. Use simple WHERE conditions only.",
                    )
                )
        except Exception:
            pass  # Best-effort: if SQL can't be re-parsed, skip these checks

    # --- Column reference warnings (only when introspected schema is available) ---

    _warn_unresolved_column_refs(joint_name, logical_plan, output_schema, warnings)

    # --- Compute transformed output schema ---

    transformed_schema = _compute_source_transform_schema(
        joint_name, logical_plan.projections, output_schema, warnings
    )
    return transformed_schema if transformed_schema is not None else output_schema


def _compute_source_transform_schema(
    joint_name: str,
    projections: list[Projection],
    catalog_schema: Schema | None,
    warnings: list[str],
) -> Schema | None:
    """Compute the output schema for a source joint with inline transform projections.

    For SELECT * (no explicit projections), returns None (use catalog schema as-is).
    For explicit projections, builds a schema from the projected columns using the
    catalog schema for type information.
    """
    from rivet_core.models import Column

    if not projections:
        return None

    # Check for SELECT * (single Star projection)
    if len(projections) == 1 and projections[0].expression == "*":
        return None

    if catalog_schema is None:
        # No introspected schema — can't compute types, but can compute column names
        columns: list[Column] = []
        for proj in projections:
            col_name = proj.alias if proj.alias else proj.expression
            columns.append(Column(name=col_name, type="large_binary", nullable=True))
            warnings.append(
                f"Source joint '{joint_name}' column '{col_name}': "
                f"cannot infer output type for expression '{proj.expression}' "
                f"(no catalog schema available)."
            )
        return Schema(columns=columns)

    # Build lookup from catalog schema
    catalog_col_map = {col.name.lower(): col for col in catalog_schema.columns}

    columns = []
    for proj in projections:
        col_name = proj.alias if proj.alias else proj.expression
        col_type = _infer_projection_type(proj, catalog_col_map, joint_name, warnings)
        columns.append(Column(name=col_name, type=col_type, nullable=True))

    return Schema(columns=columns)


def _infer_projection_type(
    proj: Projection,
    catalog_col_map: dict[str, Any],
    joint_name: str,
    warnings: list[str],
) -> str:
    """Infer the output type of a single projection expression.

    Returns the Arrow type string. Falls back to 'large_binary' with a warning
    when the type cannot be determined.
    """
    expr = proj.expression
    alias = proj.alias

    # Simple column reference (no alias, or alias with expression = column name)
    if not proj.alias and len(proj.source_columns) == 1:
        col_name = proj.source_columns[0].rsplit(".", 1)[-1].lower()
        cat_col = catalog_col_map.get(col_name)
        if cat_col is not None:
            return str(cat_col.type)
    elif (
        proj.alias
        and len(proj.source_columns) == 1
        and proj.expression.lower() == proj.source_columns[0].rsplit(".", 1)[-1].lower()
    ):
        # Simple rename: alias for a single column reference
        col_name = proj.source_columns[0].rsplit(".", 1)[-1].lower()
        cat_col = catalog_col_map.get(col_name)
        if cat_col is not None:
            return str(cat_col.type)

    # CAST expression: try to extract target type from the expression string
    expr_upper = expr.strip().upper()
    if expr_upper.startswith("CAST("):
        # Parse "CAST(x AS TYPE)" to extract TYPE
        try:
            import sqlglot
            from sqlglot import exp as sg_exp

            parsed = sqlglot.parse_one(expr)
            if isinstance(parsed, sg_exp.Cast):
                from rivet_core.sql_parser import SQLParser

                return SQLParser._normalize_sqlglot_type(parsed.to)
        except Exception:
            pass

    # Cannot determine type — emit warning
    col_label = alias or expr
    warnings.append(
        f"Source joint '{joint_name}' column '{col_label}': "
        f"cannot infer output type for expression '{expr}'."
    )
    return "large_binary"


def _resolve_joint_metadata(
    joint: Joint,
    ctx: CompilationContext,
) -> ResolvedJointMetadata:
    """Resolve catalog, engine, and adapter metadata for a joint."""
    catalog = ctx.catalog_map.get(joint.catalog) if joint.catalog else None
    catalog_type = catalog.type if catalog else None
    catalog_plugin = ctx.registry.get_catalog_plugin(catalog_type) if catalog_type else None

    engine_name, engine_type, resolution = _resolve_engine(
        joint, ctx.engine_map, ctx.default_engine
    )
    if not engine_name:
        ctx.errors.append(
            RivetError(
                code="RVT-401",
                message=f"No compute engine resolved for joint '{joint.name}'. "
                f"Specify an engine on the joint or provide a default engine.",
                context={"joint": joint.name},
                remediation="Set engine on the joint or pass engines to compile().",
            )
        )
        engine_name = ""
        engine_type = ""
        resolution = ""

    if engine_name and not engine_type:
        eng = ctx.engine_map.get(engine_name)
        if eng:
            engine_type = eng.engine_type

    adapter_name = _resolve_adapter(
        engine_type,
        catalog_type,
        engine_name,
        joint.name,
        ctx.registry,
        ctx.errors,
        adapter_cache=ctx.adapter_cache,
    )

    return ResolvedJointMetadata(
        catalog=catalog,
        catalog_type=catalog_type,
        catalog_plugin=catalog_plugin,
        engine_name=engine_name,
        engine_type=engine_type,
        resolution=resolution,
        adapter_name=adapter_name,
    )


def _compile_source_joint(
    joint: Joint,
    resolved: ResolvedJointMetadata,
    ctx: CompilationContext,
    *,
    introspect: bool,
    introspect_timeout: float,
) -> tuple[Joint, LogicalPlan | None, Schema | None, SourceStats | None]:
    """Compile source-specific SQL analysis, introspection, and validation."""
    output_schema: Schema | None = None
    source_stats: SourceStats | None = None
    if joint.joint_type == "source" and introspect:
        output_schema, source_stats = _introspect_source(
            joint,
            resolved.catalog,
            resolved.catalog_plugin,
            ctx.warnings,
            timeout_seconds=introspect_timeout,
        )

    source_sql_analysis: SourceSQLAnalysis | None = None
    if joint.joint_type == "source" and joint.sql:
        source_sql_analysis = _analyze_source_sql(joint, ctx.parser)
        joint = source_sql_analysis.joint
        logical_plan = source_sql_analysis.logical_plan
    else:
        logical_plan = None

    output_schema = _validate_source_inline_transforms(
        joint.name,
        logical_plan,
        output_schema,
        ctx.errors,
        ctx.warnings,
        sql=joint.sql,
        parsed_ast=(source_sql_analysis.parsed_ast if source_sql_analysis else None),
    )
    return joint, logical_plan, output_schema, source_stats


def _compile_sql_like_joint(
    joint: Joint,
    engine_type: str,
    ctx: CompilationContext,
) -> tuple[LogicalPlan | None, list[ColumnLineage], str | None, str | None, Schema | None]:
    """Compile SQL-bearing SQL, sink, and checkpoint joints."""
    if joint.joint_type not in ("sql", "sink", "checkpoint") or not joint.sql:
        return None, [], None, None, None
    return _compile_sql_joint(
        joint,
        engine_type,
        ctx.registry,
        ctx.parser,
        ctx.upstream_schemas,
        ctx.errors,
        ctx.warnings,
    )


def _default_write_strategy(joint: Joint) -> str | None:
    """Resolve the default write strategy for sink-like joints."""
    if joint.joint_type in ("sink", "checkpoint"):
        return joint.write_strategy or "replace"
    return joint.write_strategy


def _validate_checkpoint_joint(joint: Joint, errors: list[RivetError]) -> None:
    """Validate checkpoint-specific required fields."""
    if joint.joint_type == "checkpoint":
        if not joint.catalog:
            errors.append(
                RivetError(
                    code="RVT-401",
                    message=f"Checkpoint joint '{joint.name}' requires a 'catalog' field.",
                    context={"joint": joint.name},
                    remediation="Add a 'catalog' field to the joint configuration.",
                )
            )
        if not joint.table:
            errors.append(
                RivetError(
                    code="RVT-401",
                    message=f"Checkpoint joint '{joint.name}' requires a 'table' field.",
                    context={"joint": joint.name},
                    remediation="Add a 'table' field to the joint configuration.",
                )
            )


def _compile_joint_with_context(
    joint: Joint,
    ctx: CompilationContext,
    *,
    introspect: bool = True,
    introspect_timeout: float = 5.0,
) -> CompiledJoint:
    """Compile a single joint using a shared compilation context."""
    resolved = _resolve_joint_metadata(joint, ctx)

    logical_plan: LogicalPlan | None = None
    column_lineage: list[ColumnLineage] = []
    sql_translated: str | None = None
    engine_dialect: str | None = None
    output_schema: Schema | None = None
    source_stats: SourceStats | None = None

    # Resolve table_map aliases early so that __self substitution in source
    # joints and all downstream adapters see the mapped (physical) name.
    if joint.catalog:
        resolved_table = _resolve_table_map_name(
            joint,
            ctx.catalog_map.get(joint.catalog),
            ctx.catalog_map,
        )
        if resolved_table != (joint.table or joint.path or joint.name):
            joint.table = resolved_table

    if joint.joint_type == "source":
        joint, logical_plan, output_schema, source_stats = _compile_source_joint(
            joint,
            resolved,
            ctx,
            introspect=introspect,
            introspect_timeout=introspect_timeout,
        )
    else:
        logical_plan, column_lineage, sql_translated, engine_dialect, sql_schema = (
            _compile_sql_like_joint(joint, resolved.engine_type, ctx)
        )
        if sql_schema:
            output_schema = sql_schema

    if joint.joint_type == "python":
        column_lineage = _compile_python_joint(joint, ctx.errors, ctx.project_root)

    write_strategy = _default_write_strategy(joint)
    _validate_checkpoint_joint(joint, ctx.errors)
    checks = _compile_checks(joint, ctx.errors)

    if output_schema:
        ctx.upstream_schemas[joint.name] = output_schema

    return CompiledJoint(
        name=joint.name,
        type=joint.joint_type,
        catalog=joint.catalog,
        catalog_type=resolved.catalog_type,
        engine=resolved.engine_name,
        engine_resolution=resolved.resolution,
        adapter=resolved.adapter_name,
        sql=joint.sql,
        sql_translated=sql_translated,
        sql_resolved=None,
        sql_dialect=joint.dialect,
        engine_dialect=engine_dialect,
        upstream=list(joint.upstream),
        eager=joint.eager,
        table=joint.table,
        write_strategy=write_strategy,
        function=joint.function,
        source_file=joint.source_file,
        logical_plan=logical_plan,
        output_schema=output_schema,
        column_lineage=column_lineage,
        optimizations=[],
        checks=checks,
        fused_group_id=None,
        tags=list(joint.tags),
        description=joint.description,
        fusion_strategy_override=joint.fusion_strategy_override,
        materialization_strategy_override=joint.materialization_strategy_override,
        source_stats=source_stats,
    )


def _compile_joint(
    joint: Joint,
    catalog_map: dict[str, Catalog],
    engine_map: dict[str, ComputeEngine],
    registry: PluginRegistry,
    default_engine: str | None,
    parser: SQLParser,
    upstream_schemas: dict[str, Schema],
    errors: list[RivetError],
    warnings: list[str],
    introspect: bool = True,
    introspect_timeout: float = 5.0,
    adapter_cache: dict[tuple[str, str], str | None] | None = None,
    project_root: Path | None = None,
) -> CompiledJoint:
    """Backward-compatible wrapper for compiling a single joint."""
    ctx = CompilationContext(
        catalog_map=catalog_map,
        engine_map=engine_map,
        registry=registry,
        default_engine=default_engine,
        parser=parser,
        upstream_schemas=upstream_schemas,
        errors=errors,
        warnings=warnings,
        adapter_cache=adapter_cache if adapter_cache is not None else {},
        project_root=project_root,
    )
    return _compile_joint_with_context(
        joint,
        ctx,
        introspect=introspect,
        introspect_timeout=introspect_timeout,
    )


def _build_compiled_catalogs(
    compiled_joints: list[CompiledJoint],
    catalogs: list[Catalog],
) -> list[CompiledCatalog]:
    """Build compiled catalog list from used catalogs."""
    used = {cj.catalog for cj in compiled_joints if cj.catalog}
    return [
        CompiledCatalog(name=c.name, type=c.type, options=dict(c.options))
        for c in catalogs
        if c.name in used
    ]


def _build_compiled_engines(
    compiled_joints: list[CompiledJoint],
    engines: list[ComputeEngine],
    registry: PluginRegistry,
) -> list[CompiledEngine]:
    """Build compiled engine list from used engines."""
    used = {cj.engine for cj in compiled_joints if cj.engine}
    result: list[CompiledEngine] = []
    for e in engines:
        if e.name in used:
            plugin = registry.get_engine_plugin(e.engine_type)
            native = list(plugin.supported_catalog_types.keys()) if plugin else []
            result.append(
                CompiledEngine(name=e.name, engine_type=e.engine_type, native_catalog_types=native)
            )
    return result


def _build_compiled_adapters(
    compiled_joints: list[CompiledJoint],
    engine_map: dict[str, ComputeEngine],
    registry: PluginRegistry,
) -> list[CompiledAdapter]:
    """Build compiled adapter list from used adapters."""
    used_keys: set[tuple[str, str]] = set()
    for cj in compiled_joints:
        if cj.adapter and cj.catalog_type:
            used_keys.add((cj.engine, cj.catalog_type))
    result: list[CompiledAdapter] = []
    for et, ct in used_keys:
        eng = engine_map.get(et)
        e_type = eng.engine_type if eng else et
        adapter = registry.get_adapter(e_type, ct)
        if adapter:
            result.append(
                CompiledAdapter(
                    engine_type=adapter.target_engine_type,
                    catalog_type=adapter.catalog_type,
                    source=adapter.source,
                )
            )
    return result


def _resolve_strategy(
    fused_groups: list[FusedGroup],
    cj_map: dict[str, CompiledJoint],
    default_fusion_strategy: str,
    errors: list[RivetError],
) -> list[FusedGroup]:
    """Resolve fusion and materialization strategies for each group."""
    VALID_FUSION = {"cte", "temp_view"}
    VALID_MATERIALIZATION = {"arrow", "temp_table"}

    result = list(fused_groups)
    for idx, group in enumerate(result):
        overrides: set[str] = set()
        for jn in group.joints:
            cj = cj_map[jn]
            if cj.fusion_strategy_override:
                overrides.add(cj.fusion_strategy_override)

        if len(overrides) > 1:
            errors.append(
                RivetError(
                    code="RVT-603",
                    message=f"Conflicting fusion strategy overrides in group '{group.id}': {sorted(overrides)}.",
                    context={"group_id": group.id, "overrides": sorted(overrides)},
                    remediation="Ensure all joints in a fused group use the same fusion strategy override.",
                )
            )
        resolved_fusion = overrides.pop() if len(overrides) == 1 else default_fusion_strategy
        if resolved_fusion not in VALID_FUSION:
            errors.append(
                RivetError(
                    code="RVT-601",
                    message=f"Invalid fusion strategy '{resolved_fusion}'. Valid options: {sorted(VALID_FUSION)}.",
                    context={"strategy": resolved_fusion},
                    remediation=f"Use one of: {sorted(VALID_FUSION)}.",
                )
            )
            resolved_fusion = default_fusion_strategy

        if resolved_fusion != group.fusion_strategy:
            joint_sql: dict[str, str | None] = {jn: cj_map[jn].sql for jn in group.joints}
            if resolved_fusion == "cte":
                new_result = _compose_cte(group.joints, joint_sql)
            else:
                new_result = _compose_temp_view(group.joints, joint_sql)
            group = replace(
                group,
                fusion_strategy=resolved_fusion,
                fusion_result=new_result,
                fused_sql=new_result.fused_sql if new_result else None,
            )

        for jn in group.joints:
            cj = cj_map[jn]
            mat_override = cj.materialization_strategy_override
            if mat_override and mat_override not in VALID_MATERIALIZATION:
                errors.append(
                    RivetError(
                        code="RVT-602",
                        message=f"Invalid materialization strategy '{mat_override}' on joint '{jn}'. "
                        f"Valid options: {sorted(VALID_MATERIALIZATION)}.",
                        context={"joint": jn, "strategy": mat_override},
                        remediation=f"Use one of: {sorted(VALID_MATERIALIZATION)}.",
                    )
                )

        result[idx] = group
    return result


def _get_resolver_for_engine_type(
    engine_type: str,
    registry: PluginRegistry,
) -> ReferenceResolver | None:
    """Return the reference resolver for a specific engine type, or None."""
    plugin = registry.get_engine_plugin(engine_type)
    if plugin:
        return plugin.get_reference_resolver()
    return None


def _resolve_references(
    fused_groups: list[FusedGroup],
    cj_map: dict[str, CompiledJoint],
    compiled_joints: list[CompiledJoint],
    engine_map: dict[str, ComputeEngine],
    catalog_map: dict[str, Catalog],
    registry: PluginRegistry,
    resolve_references: ReferenceResolver | None,
    warnings: list[str],
) -> list[FusedGroup]:
    """Resolve SQL references in fused groups.

    Each group is resolved using the reference resolver from its own engine
    plugin.  An explicitly provided *resolve_references* overrides auto-
    discovery and is applied to all groups (for backward compatibility with
    tests and single-engine projects).

    In multi-engine plans, this prevents a resolver from one engine type
    (e.g. postgres) from rewriting SQL in groups belonging to a different
    engine type (e.g. duckdb).
    """
    result = list(fused_groups)
    resolver_cache: dict[str, ReferenceResolver | None] = {}
    compiled_catalog_cache: dict[str, CompiledCatalog | None] = {}
    for idx, group in enumerate(result):
        # Per-group resolver: use the explicit override if provided,
        # otherwise look up the resolver for this group's engine type.
        resolver: ReferenceResolver | None
        if resolve_references is not None:
            resolver = resolve_references
        else:
            if group.engine_type not in resolver_cache:
                resolver_cache[group.engine_type] = _get_resolver_for_engine_type(
                    group.engine_type,
                    registry,
                )
            resolver = resolver_cache[group.engine_type]
        if resolver is None:
            continue

        any_resolved = False
        group_joints = list(group.joints)
        for jn in group.joints:
            cj = cj_map[jn]
            if not (cj.sql_translated or cj.sql):
                continue
            # Source joints only need resolution when fused with other joints
            # (to avoid self-referencing CTEs like `x AS (SELECT * FROM x)`).
            if cj.type == "source" and len(group.joints) < 2:
                continue
            if cj.type not in ("sql", "sink", "source"):
                continue
            input_sql = cj.sql_translated or cj.sql
            assert input_sql is not None
            compiled_cat = None
            if cj.catalog:
                if cj.catalog not in compiled_catalog_cache:
                    cat = catalog_map.get(cj.catalog)
                    compiled_catalog_cache[cj.catalog] = (
                        CompiledCatalog(name=cat.name, type=cat.type, options=dict(cat.options))
                        if cat
                        else None
                    )
                compiled_cat = compiled_catalog_cache[cj.catalog]
            try:
                resolved = resolver.resolve_references(
                    input_sql,
                    cj,
                    compiled_cat,
                    compiled_joints=cj_map,
                    catalog_map=catalog_map,
                    fused_group_joints=group_joints,
                )
                if resolved and resolved != input_sql:
                    cj_map[jn] = replace(cj, sql_resolved=resolved)
                    any_resolved = True
            except Exception as e:
                warnings.append(f"Reference resolution failed for joint '{jn}': {e}")

        if any_resolved:
            resolved_joint_sql: dict[str, str | None] = {}
            for jn in group.joints:
                cj = cj_map[jn]
                resolved_joint_sql[jn] = cj.sql_resolved or cj.sql_translated or cj.sql
            if group.fusion_strategy == "cte":
                resolved_result = _compose_cte(group.joints, resolved_joint_sql)
            else:
                resolved_result = _compose_temp_view(group.joints, resolved_joint_sql)
            if resolved_result:
                new_fusion_result = group.fusion_result
                if new_fusion_result:
                    new_fusion_result = replace(
                        new_fusion_result,
                        resolved_fused_sql=resolved_result.fused_sql,
                        resolved_statements=resolved_result.statements,
                        resolved_final_select=resolved_result.final_select,
                    )
                result[idx] = replace(
                    group,
                    resolved_sql=resolved_result.fused_sql,
                    fusion_result=new_fusion_result,
                )
    return result


def _build_downstream_map(cj_map: dict[str, CompiledJoint]) -> dict[str, list[str]]:
    """Build a downstream dependency map in O(V+E).

    For each joint, collect the list of joints that depend on it by iterating
    each joint's upstream list once.
    """
    downstream: dict[str, list[str]] = {jn: [] for jn in cj_map}
    for cj in cj_map.values():
        for up in cj.upstream:
            if up in downstream:
                downstream[up].append(cj.name)
    return downstream


def _determine_materializations(
    cj_map: dict[str, CompiledJoint],
    joint_to_group: dict[str, str],
    engine_map: dict[str, ComputeEngine],
    registry: PluginRegistry,
    default_materialization_strategy: MaterializationStrategyName,
    boundary_joints: set[str] | None = None,
) -> list[Materialization]:
    """Determine materialization points between joints.

    When a joint has assertion-phase checks and the engine plugin declares
    ``supports_native_assertions = True`` and all assertion-phase checks are
    SQL-translatable, the ``"assertion_boundary"`` trigger is suppressed.
    An ``OptimizationResult`` is recorded on the joint for observability.

    Parameters
    ----------
    boundary_joints:
        Joint names that sit on an engine-type boundary, as computed by
        ``_detect_engine_boundaries``.  When provided, the
        ``"engine_instance_change"`` trigger is derived from this set
        instead of performing an ad-hoc engine-name comparison.
    """
    from rivet_core.executor import _is_sql_translatable

    VALID_MATERIALIZATION = {"arrow", "temp_table"}
    downstream_map = _build_downstream_map(cj_map)

    materializations: list[Materialization] = []
    for cj in cj_map.values():
        # Record assertion boundary optimization results for joints with checks
        has_assertion_checks = any(c.phase == "assertion" for c in cj.checks)
        if has_assertion_checks:
            eng = engine_map.get(cj.engine)
            engine_type = eng.engine_type if eng else ""
            plugin = registry.get_engine_plugin(engine_type) if engine_type else None
            assertion_checks = [c for c in cj.checks if c.phase == "assertion"]
            native_ok = (
                plugin is not None
                and plugin.supports_native_assertions
                and all(_is_sql_translatable(c) for c in assertion_checks)
            )
            if native_ok:
                cj_map[cj.name] = replace(
                    cj,
                    optimizations=[
                        *cj.optimizations,
                        OptimizationResult(
                            rule="assertion_boundary_suppressed",
                            status="applied",
                            detail=f"Joint '{cj.name}' assertions executed engine-natively on {engine_type}",
                        ),
                    ],
                )
            else:
                reason = (
                    f"Engine '{engine_type}' does not support native assertions"
                    if plugin is None or not plugin.supports_native_assertions
                    else f"Joint '{cj.name}' has non-SQL-translatable checks: "
                    + ", ".join(
                        sorted({c.type for c in assertion_checks if not _is_sql_translatable(c)})
                    )
                )
                cj_map[cj.name] = replace(
                    cj,
                    optimizations=[
                        *cj.optimizations,
                        OptimizationResult(
                            rule="assertion_boundary_suppressed",
                            status="not_applicable",
                            detail=reason,
                        ),
                    ],
                )
        else:
            native_ok = False

        for ds_name in downstream_map.get(cj.name, []):
            ds = cj_map[ds_name]
            trigger: MaterializationTrigger | None = None
            detail = ""

            if cj.type == "checkpoint":
                trigger = "checkpoint_boundary"
                detail = f"Joint '{cj.name}' is a checkpoint exit joint"
            elif cj.eager:
                trigger = "eager"
                detail = f"Joint '{cj.name}' declares eager=true"
            elif ds.type == "python":
                trigger = "python_boundary"
                detail = f"Downstream joint '{ds_name}' is a PythonJoint"
            elif bool(cj.checks) and not native_ok:
                trigger = "assertion_boundary"
                detail = f"Joint '{cj.name}' has assertions"
            elif len(downstream_map.get(cj.name, [])) > 1:
                trigger = "multi_consumer"
                detail = (
                    f"Joint '{cj.name}' has {len(downstream_map[cj.name])} downstream consumers"
                )
            else:
                _boundary_joints = boundary_joints or set()
                if cj.name in _boundary_joints:
                    eng_from = engine_map.get(cj.engine)
                    eng_to = engine_map.get(ds.engine)
                    from_name = eng_from.name if eng_from else cj.engine
                    to_name = eng_to.name if eng_to else ds.engine
                    trigger = "engine_instance_change"
                    detail = f"Engine changes from '{from_name}' to '{to_name}'"
                elif joint_to_group.get(cj.name) != joint_to_group.get(ds_name):
                    trigger = "capability_gap"
                    detail = f"Joints '{cj.name}' and '{ds_name}' are in different fused groups"

            if trigger:
                if trigger == "checkpoint_boundary":
                    mat_strategy: MaterializationStrategyName = "arrow"
                else:
                    candidate_strategy = (
                        cj.materialization_strategy_override or default_materialization_strategy
                    )
                    if candidate_strategy not in VALID_MATERIALIZATION:
                        candidate_strategy = default_materialization_strategy
                    mat_strategy = cast(MaterializationStrategyName, candidate_strategy)
                materializations.append(
                    Materialization(
                        from_joint=cj.name,
                        to_joint=ds_name,
                        trigger=trigger,
                        detail=detail,
                        strategy=mat_strategy,
                    )
                )
    return materializations


def _detect_engine_boundaries(
    fused_groups: list[FusedGroup],
    cj_map: dict[str, CompiledJoint],
    joint_to_group: dict[str, str],
    registry: PluginRegistry,
    warnings: list[str],
) -> list[EngineBoundary]:
    """Detect engine type changes between adjacent fused groups."""
    group_map: dict[str, FusedGroup] = {g.id: g for g in fused_groups}
    boundary_joints_map: dict[tuple[str, str], list[str]] = {}

    for group in fused_groups:
        group_et = group.engine_type
        for jn in group.entry_joints or group.joints:
            cj = cj_map.get(jn)
            if not cj:
                continue
            for up in cj.upstream:
                up_gid = joint_to_group.get(up)
                if not up_gid or up_gid == group.id:
                    continue
                # up_gid is guaranteed to be in group_map since it came from
                # joint_to_group which is built from the same fused_groups list.
                up_et = group_map[up_gid].engine_type
                if up_et == group_et:
                    continue
                key = (up_gid, group.id)
                bj = boundary_joints_map.setdefault(key, [])
                if up not in bj:
                    bj.append(up)

    boundaries: list[EngineBoundary] = []
    for (prod_gid, cons_gid), joints in boundary_joints_map.items():
        prod_et = group_map[prod_gid].engine_type
        cons_et = group_map[cons_gid].engine_type
        adapter = registry.get_cross_joint_adapter(cons_et, prod_et)
        if adapter is None:
            warnings.append(
                f"RVT-504: No CrossJointAdapter registered for "
                f"({cons_et}, {prod_et}) boundary "
                f"at joints {joints}. Default arrow passthrough will be used."
            )
            strategy = "default: arrow_passthrough"
        else:
            strategy = type(adapter).__qualname__
        boundaries.append(
            EngineBoundary(
                producer_group_id=prod_gid,
                consumer_group_id=cons_gid,
                producer_engine_type=prod_et,
                consumer_engine_type=cons_et,
                boundary_joints=joints,
                adapter_strategy=strategy,
            )
        )
    return boundaries


def _build_checkpoint_sources(
    fused_groups: list[FusedGroup],
    cj_map: dict[str, CompiledJoint],
    joint_to_group: dict[str, str],
    registry: PluginRegistry,
    warnings: list[str],
) -> list[FusedGroup]:
    """Pre-resolve checkpoint-to-downstream adapter metadata on each FusedGroup.

    For each group, finds upstream joints that are checkpoints (type == "checkpoint")
    and resolves the adapter for (group.engine_type, checkpoint.catalog_type).
    Stores the result in group.checkpoint_sources.

    This makes checkpoint resolution visible at compile time:
    - Missing adapters produce warnings
    - The compiled output shows which adapters will be used
    """
    updated: list[FusedGroup] = []
    for group in fused_groups:
        cp_sources: dict[str, CheckpointSourceInfo] = {}
        # Iterate ALL joints in the group, not just entry_joints.
        # A joint can have upstream both inside and outside the group,
        # so it won't be an entry joint but still needs checkpoint resolution.
        for jn in group.joints:
            cj = cj_map.get(jn)
            if not cj:
                continue
            for up in cj.upstream:
                up_gid = joint_to_group.get(up)
                up_cj = cj_map.get(up)
                if not up_gid or up_gid == group.id:
                    continue
                if not up_cj or up_cj.type != "checkpoint":
                    continue
                if up in cp_sources:
                    continue
                catalog_type = up_cj.catalog_type
                catalog_name = up_cj.catalog or ""
                table_name = up_cj.table or up_cj.name

                adapter_name: str | None = None
                if catalog_type:
                    adapter = registry.get_adapter(group.engine_type, catalog_type)
                    if adapter:
                        adapter_name = f"{group.engine_type}:{catalog_type}"
                    else:
                        warnings.append(
                            f"No adapter for ({group.engine_type}, {catalog_type}) "
                            f"to read checkpoint '{up}' in group '{group.id}'. "
                            f"Will fall back to SourcePlugin or Arrow passthrough."
                        )

                cp_sources[up] = CheckpointSourceInfo(
                    checkpoint_joint=up,
                    catalog=catalog_name,
                    catalog_type=catalog_type or "",
                    table=table_name,
                    adapter=adapter_name,
                )
        if cp_sources:
            updated.append(replace(group, checkpoint_sources=cp_sources))
        else:
            updated.append(group)
    return updated


def _build_checkpoint_fq_name(
    cj: CompiledJoint,
    catalog: Catalog | None,
) -> str:
    """Build fully-qualified table name for a checkpoint CTE.

    Uses catalog options (catalog_name/catalog, schema) + checkpoint's table field.
    Consistent with DatabricksReferenceResolver FQ name logic.
    """
    table = cj.table or cj.name

    if not catalog:
        return table

    opts = catalog.options
    db_catalog = opts.get("catalog_name") or opts.get("catalog")
    if not db_catalog:
        return table

    parts = table.split(".")
    if len(parts) == 3:
        return table

    db_schema = opts.get("schema", "default")
    if len(parts) == 2:
        return f"{db_catalog}.{table}"

    return f"{db_catalog}.{db_schema}.{table}"


def _prepend_ctes(sql: str, cte_parts: list[str]) -> str:
    """Prepend checkpoint CTE definitions to a SQL string.

    If the SQL already starts with WITH, the checkpoint CTEs are inserted
    before the existing CTEs. Otherwise a new WITH clause is created.
    """
    joined = ",\n".join(cte_parts)
    if sql.upper().startswith("WITH "):
        # Strip "WITH " and prepend checkpoint CTEs before existing ones
        rest = sql[5:]  # everything after "WITH "
        return f"WITH {joined},\n{rest}"
    else:
        # No existing WITH clause — wrap checkpoint CTEs + original SQL
        return f"WITH {joined}\n{sql}"


def _resolve_checkpoint_cte_body(
    cp_name: str,
    cp_cj: CompiledJoint,
    catalog_map: dict[str, Catalog],
    cj_map: dict[str, CompiledJoint],
    resolver: ReferenceResolver,
    group_joints: list[str],
) -> str | None:
    """Ask the engine's resolver to produce an engine-native SELECT for a checkpoint CTE.

    Constructs ``SELECT * FROM <cp_name>`` and passes it through the resolver
    with a synthetic joint whose upstream is ``[cp_name]``.  The resolver
    rewrites ``<cp_name>`` to the engine-native expression (e.g.
    ``read_parquet(...)`` for DuckDB filesystem catalogs).

    Returns the resolved SQL string, or ``None`` if the resolver cannot
    resolve the reference.
    """
    from types import SimpleNamespace

    synthetic_sql = f"SELECT * FROM {cp_name}"
    synthetic_joint = SimpleNamespace(
        name="__checkpoint_cte_synthetic__",
        type="sql",
        upstream=[cp_name],
        sql=synthetic_sql,
        sql_translated=None,
        catalog=None,
        table=None,
    )
    try:
        resolved = resolver.resolve_references(
            synthetic_sql,
            synthetic_joint,
            None,
            compiled_joints=cj_map,
            catalog_map=catalog_map,
            fused_group_joints=group_joints,
        )
        if resolved and resolved != synthetic_sql:
            return resolved
    except Exception:
        pass
    return None


def _inject_checkpoint_ctes(
    fused_groups: list[FusedGroup],
    cj_map: dict[str, CompiledJoint],
    catalog_map: dict[str, Catalog],
    registry: PluginRegistry | None = None,
) -> list[FusedGroup]:
    """Prepend cross-group checkpoint CTEs to downstream fused groups.

    For each group with checkpoint_sources, builds CTE entries and prepends
    them to the group's fused SQL.  When a *registry* is provided, the
    engine's :class:`ReferenceResolver` is used to produce engine-native
    table references (e.g. ``read_parquet(...)`` for DuckDB filesystem
    catalogs).  Falls back to :func:`_build_checkpoint_fq_name` when no
    resolver is available or when the resolver cannot resolve the reference.
    """
    updated: list[FusedGroup] = []
    resolver_cache: dict[str, ReferenceResolver | None] = {}
    for group in fused_groups:
        if not group.checkpoint_sources:
            updated.append(group)
            continue

        # Obtain the resolver for this group's engine type (cached).
        resolver: ReferenceResolver | None = None
        if registry is not None:
            if group.engine_type not in resolver_cache:
                resolver_cache[group.engine_type] = _get_resolver_for_engine_type(
                    group.engine_type, registry
                )
            resolver = resolver_cache[group.engine_type]

        cte_parts: list[str] = []
        for cp_name, _cp_info in group.checkpoint_sources.items():
            cp_cj = cj_map.get(cp_name)
            if not cp_cj:
                continue

            # Try engine-native resolution via the resolver first.
            resolved_body: str | None = None
            if resolver is not None:
                resolved_body = _resolve_checkpoint_cte_body(
                    cp_name, cp_cj, catalog_map, cj_map, resolver, list(group.joints)
                )

            if resolved_body is not None:
                cte_parts.append(f"{cp_name} AS (\n    {resolved_body}\n)")
            else:
                # No resolver available — the engine will receive checkpoint
                # data via input_tables registered under the joint name.
                cte_parts.append(f"{cp_name} AS (\n    SELECT * FROM {cp_name}\n)")

        if not cte_parts:
            updated.append(group)
            continue

        # Update group-level fused SQL
        new_fused_sql = (
            _prepend_ctes(group.fused_sql, cte_parts) if group.fused_sql else group.fused_sql
        )
        new_resolved_sql = (
            _prepend_ctes(group.resolved_sql, cte_parts)
            if group.resolved_sql
            else group.resolved_sql
        )

        # Update fusion_result fields
        new_fusion_result = group.fusion_result
        if new_fusion_result:
            new_fr_fused_sql = _prepend_ctes(new_fusion_result.fused_sql, cte_parts)
            new_fr_resolved_fused_sql = (
                _prepend_ctes(new_fusion_result.resolved_fused_sql, cte_parts)
                if new_fusion_result.resolved_fused_sql
                else None
            )
            new_statements = cte_parts + list(new_fusion_result.statements)
            new_resolved_statements = (
                cte_parts + list(new_fusion_result.resolved_statements)
                if new_fusion_result.resolved_statements is not None
                else None
            )
            new_fusion_result = replace(
                new_fusion_result,
                fused_sql=new_fr_fused_sql,
                resolved_fused_sql=new_fr_resolved_fused_sql,
                statements=new_statements,
                resolved_statements=new_resolved_statements,
            )

        updated.append(
            replace(
                group,
                fused_sql=new_fused_sql,
                resolved_sql=new_resolved_sql,
                fusion_result=new_fusion_result,
            )
        )
    return updated


def _assign_schema_confidence(
    compiled_joints: list[CompiledJoint],
    introspected_sources: set[str],
) -> list[CompiledJoint]:
    """Assign schema_confidence to each joint based on how its schema was determined."""
    confidence_map: dict[str, SchemaConfidence] = {}
    joint_map = {cj.name: cj for cj in compiled_joints}

    for cj in compiled_joints:
        if cj.type == "source":
            if cj.name in introspected_sources:
                confidence_map[cj.name] = "introspected"
            else:
                confidence_map[cj.name] = "none"
        elif cj.type == "python":
            confidence_map[cj.name] = "none"
        elif cj.type == "sql":
            if cj.output_schema is None:
                # Check if some upstream had schemas (partial) or none at all
                upstream_have_schema = any(
                    joint_map[u].output_schema is not None for u in cj.upstream if u in joint_map
                )
                confidence_map[cj.name] = "partial" if upstream_have_schema else "none"
            else:
                all_upstream_have_schema = all(
                    joint_map[u].output_schema is not None for u in cj.upstream if u in joint_map
                )
                if all_upstream_have_schema:
                    confidence_map[cj.name] = "inferred"
                else:
                    confidence_map[cj.name] = "partial"
        elif cj.type == "sink":
            # Handle case where sink has no schema
            if cj.output_schema is None:
                # Check if schema merging failed due to conflicts
                upstream_schemas = [
                    joint_map[u].output_schema for u in cj.upstream if u in joint_map
                ]
                non_none_schemas = [s for s in upstream_schemas if s is not None]

                if len(non_none_schemas) > 1:
                    # Multiple schemas exist but sink has None - merging failed, assign "partial"
                    confidence_map[cj.name] = "partial"
                elif len(non_none_schemas) == 1 and len(upstream_schemas) > 1:
                    # One upstream has schema, others have None - assign "partial"
                    confidence_map[cj.name] = "partial"
                else:
                    # All upstreams have None or no upstreams - assign "none"
                    confidence_map[cj.name] = "none"
            else:
                # Sink has a schema - inherit best confidence from upstream
                upstream_confidences = [confidence_map.get(u, "none") for u in cj.upstream]
                rank = {"introspected": 3, "inferred": 2, "partial": 1, "none": 0}
                best: SchemaConfidence = (
                    max(upstream_confidences, key=lambda c: rank.get(c, 0))
                    if upstream_confidences
                    else "none"
                )
                confidence_map[cj.name] = best
        else:
            confidence_map[cj.name] = "none"

    return [
        replace(cj, schema_confidence=confidence_map.get(cj.name, "none")) for cj in compiled_joints
    ]


def _infer_sink_schemas(
    compiled_joints: list[CompiledJoint],
    warnings: list[str],
) -> list[CompiledJoint]:
    """Infer output schemas for sink joints based on upstream schemas.

    For each sink:
    - Single upstream: copy upstream schema
    - Multiple upstreams with identical schemas: use that schema
    - Multiple upstreams with differing schemas: set to None, emit warning
    - Any upstream with None schema: set to None

    Args:
        compiled_joints: List of compiled joints to process
        warnings: List to append warning messages to

    Returns:
        Updated list of CompiledJoints with sink schemas populated
    """
    # Build joint_map for O(1) lookups
    joint_map: dict[str, CompiledJoint] = {cj.name: cj for cj in compiled_joints}

    result: list[CompiledJoint] = []

    for cj in compiled_joints:
        if cj.type not in ("sink", "checkpoint"):
            result.append(cj)
            continue

        # If the sink already has a SQL-inferred schema, keep it
        if cj.output_schema is not None:
            result.append(cj)
            continue

        # Collect upstream schemas
        upstream_schemas: list[Schema | None] = []
        for upstream_name in cj.upstream:
            if upstream_name in joint_map:
                upstream_schemas.append(joint_map[upstream_name].output_schema)

        # Determine sink schema based on upstream schemas
        inferred_schema: Schema | None = None

        if not upstream_schemas:
            # No upstream joints (shouldn't happen for valid sinks, but handle gracefully)
            inferred_schema = None
        elif len(upstream_schemas) == 1:
            # Single upstream: copy schema (even if None)
            inferred_schema = upstream_schemas[0]
        else:
            # Multiple upstreams: merge if identical, None if conflicting
            if any(s is None for s in upstream_schemas):
                # Any upstream has no schema
                inferred_schema = None
            elif _schemas_identical(upstream_schemas):
                # All schemas are identical
                inferred_schema = upstream_schemas[0]
            else:
                # Schemas differ - emit warning
                inferred_schema = None
                upstream_names = ", ".join(f"'{u}'" for u in cj.upstream)
                warnings.append(
                    f"Sink '{cj.name}' has conflicting upstream schemas from joints: {upstream_names}. "
                    f"Schema inference failed. Sink output_schema set to None."
                )

        # Update the compiled joint with inferred schema
        result.append(replace(cj, output_schema=inferred_schema))

    return result


def _schemas_identical(schemas: list[Schema | None]) -> bool:
    """Check if all schemas in the list are identical.

    Returns False if any schema is None or if schemas differ in columns,
    types, nullability, or order.

    Args:
        schemas: List of Schema objects to compare

    Returns:
        True if all schemas are non-None and identical, False otherwise
    """
    if not schemas:
        return True

    # If any schema is None, they're not identical
    if any(s is None for s in schemas):
        return False

    # All schemas are non-None at this point
    first_schema = schemas[0]
    assert first_schema is not None  # Type narrowing

    for schema in schemas[1:]:
        assert schema is not None  # Type narrowing

        # Check if column count differs
        if len(first_schema.columns) != len(schema.columns):
            return False

        # Check each column (order matters)
        for col1, col2 in zip(first_schema.columns, schema.columns):
            if col1.name != col2.name:
                return False
            if col1.type != col2.type:
                return False
            if col1.nullable != col2.nullable:
                return False

    return True


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
        # Determine source joints that need CTE rewrite in this group
        adapter_read_sources = {
            jn for jn in group.joints if cj_map.get(jn) and cj_map[jn].type == "source"
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
