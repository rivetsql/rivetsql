"""Phase 3: Introspect source joints for schema and metadata.

This is a Level 3 module — it may import from ``models``, ``state``, and
``helpers/`` only.  It must NOT import from ``pipeline.py`` or other phase
modules.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from dataclasses import replace
from typing import Any

from rivet_core.compiler.helpers.resolution import _do_introspect
from rivet_core.compiler.models import (
    PHASE_INTROSPECT_SOURCES,
    IntrospectionRecord,
    PluginAnnotation,
)
from rivet_core.compiler.state import PhaseState


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
