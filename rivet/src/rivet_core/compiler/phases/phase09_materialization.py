"""Phase 9: Determine materialization points.

Runs after ``_EngineBoundaryPhase`` so it can use the validated
``state.engine_boundaries`` to detect engine-instance changes instead
of performing its own ad-hoc engine-name comparison.

This is a Level 3 module — it may import from ``models``, ``state``, and
``helpers/`` only.  It must NOT import from ``pipeline.py`` or other phase
modules.
"""

from __future__ import annotations

from dataclasses import replace

from rivet_core.compiler.helpers.resolution import _determine_materializations
from rivet_core.compiler.models import PHASE_MATERIALIZATION
from rivet_core.compiler.state import PhaseState


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
