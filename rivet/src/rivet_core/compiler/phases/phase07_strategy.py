"""Phase 7: Strategy resolution, reference resolution, checkpoint CTEs.

This is a Level 3 module — it may import from ``models``, ``state``, and
``helpers/`` only.  It must NOT import from ``pipeline.py`` or other phase
modules.
"""

from __future__ import annotations

from dataclasses import replace

from rivet_core.compiler.helpers.resolution import (
    _build_checkpoint_sources,
    _inject_checkpoint_ctes,
    _resolve_references,
    _resolve_strategy,
)
from rivet_core.compiler.models import PHASE_STRATEGY_RESOLUTION, PluginAnnotation
from rivet_core.compiler.state import PhaseState
from rivet_core.errors import RivetError


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
