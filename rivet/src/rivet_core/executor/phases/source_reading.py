"""Source reading functions for the executor.

Extracted from ``Executor._read_source_via_adapter``,
``Executor._read_source_fallback``, and ``Executor._read_sources_into``.
These functions receive ``registry: PluginRegistry`` as an explicit
parameter instead of accessing ``self._registry``.

This is Level 3 of the executor package dependency hierarchy — it imports
from ``models`` (Level 1) and ``helpers/`` (Level 2) only within the
executor package.  It does NOT import from ``pipeline.py`` or other phase
modules.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import replace

import pyarrow

from rivet_core.compiler import CompiledCatalog, CompiledJoint
from rivet_core.errors import ExecutionError, RivetError
from rivet_core.executor.helpers.arrow_helpers import (
    _apply_source_expressions,
    _apply_source_inline_residuals,
)
from rivet_core.executor.helpers.pushdown import (
    _merge_cross_group_limits,
    _merge_cross_group_predicates,
    _merge_cross_group_projections,
    _merge_residuals,
    _merge_source_limit_into_pushdown,
    _merge_source_predicates_into_pushdown,
    _merge_source_projections_into_pushdown,
)
from rivet_core.models import Catalog, Joint
from rivet_core.optimizer import (
    AdapterPushdownResult,
    FusedGroup,
    ResidualPlan,
)
from rivet_core.plugins import PluginRegistry
from rivet_core.stats import StatsCollector
from rivet_core.strategies import DeferredRef, MaterializedRef

_logger = logging.getLogger(__name__)


async def read_source_via_adapter(
    jn: str,
    cj: CompiledJoint,
    cat: Catalog,
    joint: Joint,
    group: FusedGroup,
    registry: PluginRegistry,
) -> tuple[bool, pyarrow.Table | None, ResidualPlan | None]:
    """Dispatch a source read through the adapter. Returns (found, table_or_none, residual_or_none)."""
    assert cj.adapter is not None, "adapter must not be None for adapter dispatch"
    parts = cj.adapter.split(":", 1)
    engine_type = parts[0]
    catalog_type = parts[1] if len(parts) > 1 else ""
    adapter = registry.get_adapter(engine_type, catalog_type)
    if not adapter:
        return False, None, None
    try:
        engine_instance = registry.get_compute_engine(cj.engine)
        effective_pushdown = _merge_source_limit_into_pushdown(group.pushdown, cj)
        effective_pushdown = _merge_source_predicates_into_pushdown(effective_pushdown, cj)
        effective_pushdown = _merge_source_projections_into_pushdown(effective_pushdown, cj)
        effective_pushdown = _merge_cross_group_predicates(effective_pushdown, group, jn)
        effective_pushdown = _merge_cross_group_projections(effective_pushdown, group, jn)
        effective_pushdown = _merge_cross_group_limits(effective_pushdown, group, jn)
        result = await asyncio.to_thread(
            adapter.read_dispatch, engine_instance, cat, joint, effective_pushdown
        )
        if isinstance(result, AdapterPushdownResult):
            mat = result.material
            tbl = mat.to_arrow() if mat and mat.materialized_ref is not None else None
            return True, tbl, result.residual
        mat = result
        tbl = mat.to_arrow() if mat and mat.materialized_ref is not None else None
        return True, tbl, None
    except Exception as exc:
        raise ExecutionError(  # noqa: B904
            RivetError(
                code="RVT-501",
                message=f"Adapter read_dispatch failed for source '{jn}' from catalog '{cj.catalog}': {exc}",
                context={"joint": jn, "catalog": cj.catalog, "adapter": cj.adapter},
                remediation="Check adapter configuration and catalog connectivity.",
            )
        )


async def read_source_fallback(
    jn: str,
    cj: CompiledJoint,
    cat: Catalog,
    joint: Joint,
    registry: PluginRegistry,
) -> pyarrow.Table | None:
    """Read a source joint via the fallback source registry."""
    source = registry.get_source_plugin(cj.catalog_type or "")
    if source:
        try:
            mat = await asyncio.to_thread(source.read, cat, joint, None)
            if mat.materialized_ref is not None:
                return mat.to_arrow()
        except Exception as exc:
            catalog_type = cj.catalog_type or "unknown"
            raise ExecutionError(  # noqa: B904
                RivetError(
                    code="RVT-501",
                    message=(
                        f"Source '{jn}' from catalog '{cj.catalog}' (type '{catalog_type}') failed: "
                        f"no adapter registered. Register a DuckDB adapter for {catalog_type} catalogs."
                    )
                    if cj.adapter
                    else f"Failed to read source '{jn}' from catalog '{cj.catalog}': {exc}",
                    context={"joint": jn, "catalog": cj.catalog},
                    remediation=(
                        f"Register an adapter for {catalog_type} catalogs "
                        f"(e.g. {catalog_type.title()}DuckDBAdapter)."
                    )
                    if cj.adapter
                    else "Check that the catalog path and table reference are correct.",
                )
            )
        return None
    if cj.adapter:
        catalog_type = cj.catalog_type or "unknown"
        raise ExecutionError(
            RivetError(
                code="RVT-501",
                message=(
                    f"Source '{jn}' from catalog '{cj.catalog}' (type '{catalog_type}') failed: "
                    f"no adapter registered. Register a DuckDB adapter for {catalog_type} catalogs."
                ),
                context={"joint": jn, "catalog": cj.catalog, "adapter": cj.adapter},
                remediation=f"Register an adapter for {catalog_type} catalogs (e.g. {catalog_type.title()}DuckDBAdapter).",
            )
        )
    return None


async def read_sources_into(
    input_tables: dict[str, pyarrow.Table],
    group: FusedGroup,
    joint_map: dict[str, CompiledJoint],
    catalog_map: dict[str, CompiledCatalog] | None,
    registry: PluginRegistry | None,
    stats_collector: StatsCollector | None = None,
    skip_fused_sources: bool = False,
    ref_materials: dict[str, MaterializedRef] | None = None,
) -> ResidualPlan | None:
    """Read source joints from their catalogs into input_tables dict.

    Returns the merged adapter residual if any adapter returned one,
    or None if no adapter residuals were produced.

    When *skip_fused_sources* is True, source joints are skipped because
    the fused SQL already contains fully-qualified table references
    (rewritten by a reference resolver) and does not depend on
    input_tables for these sources.

    When *ref_materials* is provided, checkpoint upstream refs that are
    ``DeferredRef`` instances are resolved through the same adapter /
    source-plugin / fallback path used for source joints, using the
    pre-resolved ``group.checkpoint_sources`` metadata.
    """
    merged_adapter_residual: ResidualPlan | None = None
    if not registry or not catalog_map:
        return None
    for jn in group.entry_joints or group.joints:
        cj = joint_map.get(jn)
        if not cj or cj.type != "source" or not cj.catalog:
            continue
        if jn in input_tables:
            continue
        if skip_fused_sources:
            continue

        cc = catalog_map.get(cj.catalog)
        if not cc:
            continue
        cat = Catalog(name=cc.name, type=cc.type, options=cc.options)
        joint = Joint(
            name=cj.name,
            joint_type="source",
            catalog=cj.catalog,
            table=cj.table,
        )

        adapter_name = cj.adapter or ""
        catalog_type = cj.catalog_type or cc.type or ""
        read_start = time.monotonic()

        if cj.adapter:
            try:
                found, tbl, residual = await read_source_via_adapter(
                    jn, cj, cat, joint, group, registry
                )
            except Exception as exc:
                read_ms = (time.monotonic() - read_start) * 1000
                if stats_collector is not None:
                    err = (
                        exc.error
                        if isinstance(exc, ExecutionError)
                        else RivetError(
                            code="RVT-501",
                            message=str(exc),
                        )
                    )
                    stats_collector.record_source_read(
                        jn,
                        adapter_name,
                        catalog_type,
                        row_count=None,
                        read_ms=read_ms,
                        error=err,
                    )
                raise
            read_ms = (time.monotonic() - read_start) * 1000
            if found:
                row_count = tbl.num_rows if tbl is not None else None
                has_residual = residual is not None
                if stats_collector is not None:
                    stats_collector.record_source_read(
                        jn,
                        adapter_name,
                        catalog_type,
                        row_count=row_count,
                        read_ms=read_ms,
                        has_residual=has_residual,
                    )
                if tbl is not None:
                    tbl = _apply_source_expressions(tbl, cj)
                    input_tables[jn] = tbl
                    if residual is not None:
                        merged_adapter_residual = _merge_residuals(
                            merged_adapter_residual, residual
                        )
                continue

        try:
            tbl = await read_source_fallback(jn, cj, cat, joint, registry)
        except Exception as exc:
            read_ms = (time.monotonic() - read_start) * 1000
            if stats_collector is not None:
                err = (
                    exc.error
                    if isinstance(exc, ExecutionError)
                    else RivetError(
                        code="RVT-501",
                        message=str(exc),
                    )
                )
                stats_collector.record_source_read(
                    jn,
                    adapter_name,
                    catalog_type,
                    row_count=None,
                    read_ms=read_ms,
                    error=err,
                )
            raise
        read_ms = (time.monotonic() - read_start) * 1000
        if tbl is not None:
            if stats_collector is not None:
                stats_collector.record_source_read(
                    jn,
                    adapter_name,
                    catalog_type,
                    row_count=tbl.num_rows,
                    read_ms=read_ms,
                )
            input_tables[jn] = _apply_source_expressions(
                _apply_source_inline_residuals(tbl, cj),
                cj,
            )
    # --- Checkpoint upstream resolution ---
    # Resolve DeferredRef entries from upstream checkpoints using the same
    # adapter/fallback pattern as source joints.
    # When skip_fused_sources is True, the reference resolver has already
    # rewritten the SQL to include fully-qualified checkpoint table names,
    # so the engine reads them natively — skip adapter/fallback reads.
    if ref_materials and group.checkpoint_sources and not skip_fused_sources:
        for cp_name, cp_info in group.checkpoint_sources.items():
            if cp_name in input_tables:
                continue  # already resolved
            ref = ref_materials.get(cp_name)
            if not isinstance(ref, DeferredRef):
                continue  # _ArrowMaterializedRef or already materialized

            cp_cj = joint_map.get(cp_name)
            if not cp_cj:
                # Fallback: materialize via .to_arrow()
                input_tables[cp_name] = ref.to_arrow()
                continue

            cc = catalog_map.get(cp_info.catalog) if catalog_map else None
            if not cc:
                # Fallback: materialize via .to_arrow()
                input_tables[cp_name] = ref.to_arrow()
                continue

            cat = Catalog(name=cc.name, type=cc.type, options=cc.options)
            joint = Joint(
                name=cp_name,
                joint_type="source",
                catalog=cp_info.catalog,
                table=cp_info.table,
            )

            if cp_info.adapter:
                # Build a synthetic CompiledJoint for read_source_via_adapter
                # using the downstream group's engine and pre-resolved adapter.
                synthetic_cj = replace(
                    cp_cj,
                    adapter=cp_info.adapter,
                    engine=group.engine,
                    catalog_type=cp_info.catalog_type,
                )
                try:
                    found, tbl, residual = await read_source_via_adapter(
                        cp_name, synthetic_cj, cat, joint, group, registry
                    )
                    if found and tbl is not None:
                        input_tables[cp_name] = tbl
                        if residual is not None:
                            merged_adapter_residual = _merge_residuals(
                                merged_adapter_residual, residual
                            )
                        continue
                except Exception:
                    _logger.debug("Adapter read of cross-group checkpoint failed; falling back to source plugin", exc_info=True)  # noqa: BLE001

            # Source plugin fallback
            try:
                source = registry.get_source_plugin(cp_info.catalog_type) if registry else None
                if source:
                    mat = await asyncio.to_thread(source.read, cat, joint, None)
                    if mat.materialized_ref is not None:
                        input_tables[cp_name] = mat.to_arrow()
                        continue
            except Exception:
                _logger.debug("SourcePlugin fallback for checkpoint failed; using DeferredRef.to_arrow()", exc_info=True)  # noqa: BLE001

            # Last resort: DeferredRef.to_arrow()
            input_tables[cp_name] = ref.to_arrow()

    return merged_adapter_residual
