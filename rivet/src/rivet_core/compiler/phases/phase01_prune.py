"""Phase 1: Prune the DAG to the target subgraph.

This is a Level 3 module — it may import from ``models``, ``state``, and
``helpers/`` only.  It must NOT import from ``pipeline.py`` or other phase
modules.
"""

from __future__ import annotations

from dataclasses import replace

from rivet_core.compiler.models import PHASE_PRUNE_DAG
from rivet_core.compiler.state import PhaseState
from rivet_core.errors import RivetError


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
