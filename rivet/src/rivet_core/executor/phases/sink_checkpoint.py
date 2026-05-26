"""Sink/checkpoint handling functions for the executor.

Extracted from ``Executor._dispatch_sink_write``,
``Executor._execute_checkpoint``, ``Executor._checkpoint_read_back``,
``Executor._try_native_sql_write``, ``Executor._run_sink_audits``, and
``Executor._run_audits``.

This is Level 3 of the executor package dependency hierarchy — it imports
from ``models`` (Level 1) and ``helpers/`` (Level 2) only within the
executor package.  It does NOT import from ``pipeline.py`` or other phase
modules.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pyarrow

from rivet_core.compiler import CompiledCatalog, CompiledJoint
from rivet_core.errors import ExecutionError, RivetError
from rivet_core.executor.helpers.arrow_helpers import (
    _normalize_arrow_type,
    _schema_to_dict,
    _schemas_are_compatible,
)
from rivet_core.executor.helpers.checks import _execute_check
from rivet_core.executor.helpers.utils import (
    _cached_arrow_materials,
    _resolve_fused_sql,
)
from rivet_core.executor.models import (
    CheckExecutionResult,
    JointExecutionResult,
)
from rivet_core.models import Catalog, Joint, Material
from rivet_core.optimizer import FusedGroup
from rivet_core.plugins import NativeSqlWriteContext, PluginRegistry
from rivet_core.strategies import DeferredRef, MaterializedRef

log = logging.getLogger("rivet_core.executor")


async def dispatch_sink_write(
    cj: CompiledJoint,
    ref: MaterializedRef,
    catalog_map: dict[str, CompiledCatalog],
    registry: PluginRegistry | None,
) -> None:
    """Dispatch sink write via registry SinkPlugin.

    Uses CompiledJoint.engine and catalog without re-resolution.
    Passes MaterializedRef in Material so sink can inspect storage_type
    without forcing .to_arrow().
    """
    if not registry or not cj.catalog:
        return
    cc = catalog_map.get(cj.catalog)
    if not cc:
        return
    sink = registry.get_sink_plugin(cc.type)
    if not sink:
        return
    try:
        cat = Catalog(name=cc.name, type=cc.type, options=cc.options)

        # Attach inferred schema to Material if available
        inferred_schema_dict = _schema_to_dict(cj.output_schema) if cj.output_schema else None

        # Check for schema mismatch and emit warning if needed
        if inferred_schema_dict:
            try:
                arrow_table = ref.to_arrow()
                actual_schema = {field.name: str(field.type) for field in arrow_table.schema}

                if not _schemas_are_compatible(inferred_schema_dict, actual_schema):
                    # Only report columns that actually differ after normalization
                    mismatched: list[str] = []
                    for col in inferred_schema_dict:
                        if col not in actual_schema:
                            mismatched.append(
                                f"{col}: expected={inferred_schema_dict[col]}, got=<missing>"
                            )
                        elif _normalize_arrow_type(
                            inferred_schema_dict[col]
                        ) != _normalize_arrow_type(actual_schema[col]):
                            mismatched.append(
                                f"{col}: expected={inferred_schema_dict[col]}, got={actual_schema[col]}"
                            )
                    for col in actual_schema:
                        if col not in inferred_schema_dict:
                            mismatched.append(
                                f"{col}: expected=<missing>, got={actual_schema[col]}"
                            )

                    logging.getLogger("rivet_core.executor").warning(
                        "Sink '%s' Material schema differs from inferred schema. "
                        "Mismatched columns: [%s]",
                        cj.name,
                        "; ".join(mismatched),
                    )
            except Exception:
                # If we can't get the Arrow table or compare schemas, continue without warning
                log.debug('Schema mismatch warning', exc_info=True)  # best-effort: see RVT logs at debug level

        mat = Material(
            name=cj.name,
            catalog=cj.catalog,
            table=cj.table,
            schema=inferred_schema_dict,
            materialized_ref=ref,
            state="materialized",
        )
        joint = Joint(name=cj.name, joint_type=cj.type, catalog=cj.catalog, table=cj.table)
        await asyncio.to_thread(sink.write, cat, joint, mat, cj.write_strategy or "replace")
    except Exception:
        if cj.type == "checkpoint":
            raise  # Checkpoint writes must succeed; read-back depends on it
        logging.getLogger("rivet_core.executor").warning(
            "Sink write failed for '%s'; continuing (audit phase may report).",
            cj.name,
            exc_info=True,
        )


async def execute_checkpoint(
    cj: CompiledJoint,
    result_ref: MaterializedRef,
    catalog_map: dict[str, CompiledCatalog],
    registry: PluginRegistry | None,
    engine_type: str | None = None,
    engine_name: str | None = None,
) -> MaterializedRef:
    """Execute checkpoint write-then-read: persist data, read it back.

    Delegates the write step to dispatch_sink_write (shared with sink joints),
    then reads the written data back via adapter or SourcePlugin and returns
    it as a new MaterializedRef for downstream joints.

    When *engine_type* and *engine_name* are provided, the read-back
    attempts to use the adapter first (required for Databricks and similar
    backends where the raw SourcePlugin returns a deferred ref).

    Raises ExecutionError on write failure or read-back failure.
    Write errors propagate immediately for checkpoints (unlike sinks).

    Note: ``incremental_append`` semantics are implemented entirely inside the
    sink plugins (key-based dedup via INSERT…ON CONFLICT / WHERE NOT EXISTS).
    The watermark store (``rivet_core.watermark.WatermarkBackend``) is
    advisory metadata maintained by the ``rivet watermark`` CLI commands and
    is intentionally orthogonal to the sink write path. Do not add executor
    logic here that depends on watermark state — it would silently desync
    from the sink-side dedup.
    """
    # Step 1: Write via shared sink write path (catalog resolution, SinkPlugin
    # dispatch, schema validation — identical to sink joints).
    await dispatch_sink_write(cj, result_ref, catalog_map, registry)

    # Step 2: Read back — pass the Arrow table so DeferredRef caches it
    return await checkpoint_read_back(
        cj,
        catalog_map,
        registry,
        engine_type=engine_type,
        engine_name=engine_name,
        result_table=result_ref.to_arrow(),
    )


async def checkpoint_read_back(
    cj: CompiledJoint,
    catalog_map: dict[str, CompiledCatalog],
    registry: PluginRegistry | None,
    engine_type: str | None = None,
    engine_name: str | None = None,
    result_table: pyarrow.Table | None = None,
) -> MaterializedRef:
    """Return a DeferredRef for checkpoint read-back. No data is read eagerly.

    When *result_table* is provided (Arrow fallback write path), the
    DeferredRef carries it as a pre-computed cached table so that
    ``.to_arrow()`` returns it without re-reading from the catalog.

    When *result_table* is ``None`` (native SQL write path), the DeferredRef
    has no cached table — ``.to_arrow()`` reads from the catalog on first access.

    *engine_type* and *engine_name* are kept for signature compatibility
    but are no longer used — adapter resolution happens in
    ``read_sources_into`` at downstream execution time.
    """
    if not registry or not cj.catalog:
        raise ExecutionError(
            RivetError(
                code="RVT-501",
                message=f"Checkpoint joint '{cj.name}' has no catalog configured.",
                context={"joint": cj.name},
                remediation="Ensure the checkpoint joint has a valid catalog.",
            )
        )

    cc = catalog_map.get(cj.catalog)
    if not cc:
        raise ExecutionError(
            RivetError(
                code="RVT-501",
                message=f"Checkpoint joint '{cj.name}': catalog '{cj.catalog}' not found.",
                context={"joint": cj.name, "catalog": cj.catalog},
                remediation="Check that the catalog is defined in your project configuration.",
            )
        )

    return DeferredRef(
        catalog_name=cc.name,
        catalog_type=cc.type,
        table_name=cj.table or cj.name,
        catalog_options=cc.options,
        registry=registry,
        cached_table=result_table,
    )


async def try_native_sql_write(
    group: FusedGroup,
    exit_cj: CompiledJoint,
    catalog_map: dict[str, CompiledCatalog],
    materials: dict[str, MaterializedRef],
    registry: PluginRegistry | None,
) -> bool:
    """Attempt native SQL write for the exit joint.

    When the group has fused SQL (e.g. from an upstream transform or
    explicit sink SQL), that SQL is used directly.  When fused SQL is
    unavailable — typically because the sink is in its own group with
    no SQL — the method constructs ``SELECT * FROM {upstream}`` from
    the single upstream materialized table.  If there are multiple
    upstreams and no fused SQL, native write is skipped (the sink
    needs explicit SQL to combine them).

    Returns True if native SQL write was dispatched successfully,
    False if the method fell back (caller should use the Arrow path).
    """
    # 1. Guard: non-empty residuals present → fallback
    if group.residual is not None and (
        group.residual.predicates or group.residual.limit is not None or group.residual.casts
    ):
        log.debug("native_sql_write skipped for '%s': residual plan present", exit_cj.name)
        return False

    # 2. Guard: no catalog → fallback
    if not exit_cj.catalog:
        return False
    cc = catalog_map.get(exit_cj.catalog)
    if not cc:
        return False

    # 3. Lookup adapter
    if not registry:
        return False
    adapter = registry.get_adapter(group.engine_type, cc.type)
    if adapter is None:
        log.debug(
            "native_sql_write skipped for '%s': no adapter for (%s, %s)",
            exit_cj.name,
            group.engine_type,
            cc.type,
        )
        return False

    # 4. Check native SQL write support for the strategy
    write_strategy = exit_cj.write_strategy or "replace"
    if not adapter.supports_native_sql_write(write_strategy):
        log.debug(
            "native_sql_write skipped for '%s': adapter does not support strategy '%s'",
            exit_cj.name,
            write_strategy,
        )
        return False

    # 5. Resolve fused SQL
    sql = _resolve_fused_sql(group)
    if not sql:
        # No fused SQL — try to construct from the exit joint's direct
        # upstream.  ``materials`` may contain entries from earlier waves
        # that are not relevant to this sink, so we filter to only the
        # upstream joints declared by the exit joint.
        upstream_names = [u for u in exit_cj.upstream if u in materials]
        if len(upstream_names) == 1:
            upstream_name = upstream_names[0]
            sql = f"SELECT * FROM {upstream_name}"
            log.debug(
                "native_sql_write for '%s': constructed SQL from upstream '%s'",
                exit_cj.name,
                upstream_name,
            )
        else:
            # Multiple upstreams require explicit SQL to combine them
            log.debug(
                "native_sql_write skipped for '%s': no fused SQL and %d upstream(s)",
                exit_cj.name,
                len(upstream_names),
                len(materials),
            )
            return False

    # 6. Build input tables (all upstream materialized Arrow tables)
    # Skip DeferredRef entries with no cached table — the engine wrote
    # directly to the catalog and there is no Arrow table to pass.
    input_tables = _cached_arrow_materials(materials)

    # 7. Build context and dispatch
    cat = Catalog(name=cc.name, type=cc.type, options=cc.options)
    target_table = exit_cj.table or exit_cj.name
    joint = Joint(
        name=exit_cj.name,
        joint_type=exit_cj.type,
        catalog=exit_cj.catalog,
        table=target_table,
    )
    engine_instance = registry.get_compute_engine(group.engine)

    ctx = NativeSqlWriteContext(
        fused_sql=sql,
        target_table=target_table,
        write_strategy=write_strategy,
        input_tables=input_tables,
        engine=engine_instance,
        catalog=cat,
        joint=joint,
    )
    await asyncio.to_thread(adapter.write_dispatch, engine_instance, cat, joint, ctx)
    log.debug("native_sql_write succeeded for '%s' (strategy=%s)", exit_cj.name, write_strategy)
    return True


async def run_sink_audits(
    group: FusedGroup,
    joint_map: dict[str, CompiledJoint],
    result_ref: MaterializedRef,
    catalog_map: dict[str, CompiledCatalog],
    assertion_error: bool,
    joint_results: list[JointExecutionResult],
    registry: PluginRegistry | None,
    skip_write: bool = False,
) -> tuple[int, int]:
    """Execute sink writes and audit checks. Returns (error_count, warning_count).

    When *skip_write* is True the sink write step is skipped (the data was
    already persisted by the native SQL write path) but audit checks still run.
    """
    check_failures = 0
    check_warnings = 0
    for jn in group.joints:
        cj = joint_map.get(jn)
        if not cj or cj.type != "sink" or assertion_error:
            continue
        if not skip_write:
            await dispatch_sink_write(cj, result_ref, catalog_map, registry)
        audit_checks = [c for c in cj.checks if c.phase == "audit"]
        if not audit_checks:
            continue
        audit_results = await run_audits(
            cj, audit_checks, result_ref.to_arrow(), catalog_map, registry
        )
        for i, jr in enumerate(joint_results):
            if jr.name == jn and jr.fused_group_id == group.id:
                merged = list(jr.check_results) + audit_results
                joint_results[i] = JointExecutionResult(
                    name=jr.name,
                    success=jr.success,
                    rows_in=jr.rows_in,
                    rows_out=jr.rows_out,
                    timing=jr.timing,
                    fused_group_id=jr.fused_group_id,
                    materialized=jr.materialized,
                    materialization_trigger=jr.materialization_trigger,
                    materialization_stats=jr.materialization_stats,
                    check_results=merged,
                    plugin_metrics=jr.plugin_metrics,
                    error=jr.error,
                )
                break
        for ar in audit_results:
            if not ar.passed:
                if ar.severity == "error":
                    check_failures += 1
                else:
                    check_warnings += 1
    return check_failures, check_warnings


async def run_audits(
    cj: CompiledJoint,
    audit_checks: list[Any],
    written_table: pyarrow.Table,
    catalog_map: dict[str, CompiledCatalog],
    registry: PluginRegistry | None,
) -> list[CheckExecutionResult]:
    """Run audit checks after sink write by reading back from target catalog.

    Audit read-back failure produces RVT-670. Audit failures are reported
    but do not rollback the write.
    """
    results: list[CheckExecutionResult] = []

    # Attempt to read back from catalog via registry
    read_back_table: pyarrow.Table | None = None
    if registry and cj.catalog:
        cc = catalog_map.get(cj.catalog)
        if cc:
            source = registry.get_source_plugin(cc.type)
            if source:
                try:
                    cat = Catalog(name=cc.name, type=cc.type, options=cc.options)
                    joint = Joint(
                        name=cj.name,
                        joint_type="source",
                        catalog=cj.catalog,
                        table=cj.table,
                    )
                    mat = await asyncio.to_thread(source.read, cat, joint, None)
                    if mat.materialized_ref is not None:
                        read_back_table = mat.to_arrow()
                except Exception:
                    log.debug('Audit read-back via SourcePlugin', exc_info=True)  # best-effort: see RVT logs at debug level

    if read_back_table is None:
        # Fallback: use the written table as best-effort read-back
        # or report RVT-670 if no data available
        if written_table is not None:
            read_back_table = written_table
        else:
            for chk in audit_checks:
                results.append(
                    CheckExecutionResult(
                        type=chk.type,
                        severity=chk.severity,
                        passed=False,
                        message="Audit read-back failed: could not read from target catalog (RVT-670)",
                        phase="audit",
                        read_back_rows=None,
                    )
                )
            return results

    read_back_rows = read_back_table.num_rows

    # Run all audit checks without short-circuiting
    for chk in audit_checks:
        cr = _execute_check(chk, read_back_table)
        results.append(
            CheckExecutionResult(
                type=cr.type,
                severity=cr.severity,
                passed=cr.passed,
                message=cr.message,
                phase="audit",
                read_back_rows=read_back_rows,
            )
        )

    return results
