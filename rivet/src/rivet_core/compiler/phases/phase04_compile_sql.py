"""Phase 4: Compile SQL, build CompiledJoints, upstream schemas.

This is a Level 3 module — it may import from ``models``, ``state``, and
``helpers/`` only.  It must NOT import from ``pipeline.py`` or other phase
modules.
"""

from __future__ import annotations

from dataclasses import replace

from rivet_core.compiler.helpers.resolution import _do_introspect
from rivet_core.compiler.helpers.sql_helpers import (
    _assign_schema_confidence,
    _compute_source_transform_schema,
    _infer_sink_schemas,
    _warn_unresolved_column_refs,
)
from rivet_core.compiler.helpers.validation import _compile_joint_with_context
from rivet_core.compiler.models import (
    PHASE_COMPILE_SQL,
    CompilationContext,
    CompiledJoint,
)
from rivet_core.compiler.state import PhaseState
from rivet_core.errors import RivetError
from rivet_core.models import Schema
from rivet_core.sql_parser import SQLParser


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
