"""Phase 10: Build final CompiledAssembly.

This is a Level 3 module — it may import from ``models``, ``state``, and
``helpers/`` only.  It must NOT import from ``pipeline.py`` or other phase
modules.
"""

from __future__ import annotations

from dataclasses import replace

from rivet_core.compiler.helpers.resolution import (
    _build_compilation_diagnostics,
    _build_compiled_adapters,
    _build_compiled_catalogs,
    _build_compiled_engines,
    _compute_parallel_execution_plan,
)
from rivet_core.compiler.models import (
    PHASE_FINALIZATION,
    CompilationStats,
    CompiledAdapter,
    CompiledAssembly,
)
from rivet_core.compiler.state import PhaseState


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
                if state.cj_map.get(jn)
                and state.cj_map[jn].type == "source"
                and state.cj_map[jn].adapter is not None
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
