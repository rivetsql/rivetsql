"""Phase 2: Resolve metadata (catalog, engine, adapter) for each joint.

This is a Level 3 module — it may import from ``models``, ``state``, and
``helpers/`` only.  It must NOT import from ``pipeline.py`` or other phase
modules.
"""

from __future__ import annotations

from dataclasses import replace

from rivet_core.compiler.helpers.resolution import (
    _list_adapter_catalog_types,
    _list_adapter_engine_types,
    _resolve_joint_metadata,
)
from rivet_core.compiler.models import (
    PHASE_RESOLVE_METADATA,
    AdapterDecision,
    CompilationContext,
    PluginAnnotation,
    ResolvedJointMetadata,
)
from rivet_core.compiler.state import PhaseState
from rivet_core.errors import RivetError
from rivet_core.sql_parser import SQLParser


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
