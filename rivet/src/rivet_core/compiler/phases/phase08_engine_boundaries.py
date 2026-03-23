"""Phase 8: Detect engine boundaries and resolve materialization strategies.

This is a Level 3 module — it may import from ``models``, ``state``, and
``helpers/`` only.  It must NOT import from ``pipeline.py`` or other phase
modules.
"""

from __future__ import annotations

from dataclasses import replace

from rivet_core.compiler.helpers.resolution import _detect_engine_boundaries
from rivet_core.compiler.models import PHASE_ENGINE_BOUNDARIES, AdapterDecision
from rivet_core.compiler.state import PhaseState


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
