"""Phase 5: Build fused groups from compiled joints.

This is a Level 3 module — it may import from ``models``, ``state``, and
``helpers/`` only.  It must NOT import from ``pipeline.py`` or other phase
modules.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from rivet_core.compiler.models import PHASE_FUSION
from rivet_core.compiler.state import PhaseState
from rivet_core.optimizer import FusionJoint, fusion_pass


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
