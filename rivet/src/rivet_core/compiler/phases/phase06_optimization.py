"""Phase 6: Pushdown optimization passes.

This is a Level 3 module — it may import from ``models``, ``state``, and
``helpers/`` only.  It must NOT import from ``pipeline.py`` or other phase
modules.
"""

from __future__ import annotations

from dataclasses import replace

from rivet_core.compiler.models import PHASE_OPTIMIZATION, PluginAnnotation
from rivet_core.compiler.state import PhaseState
from rivet_core.optimizer import cross_group_pushdown_pass, pushdown_pass
from rivet_core.sql_parser import LogicalPlan


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
