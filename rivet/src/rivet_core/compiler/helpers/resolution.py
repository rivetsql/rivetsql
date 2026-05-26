"""Metadata resolution helpers: engines, adapters, strategies, introspection.

This is a Level 2 module — it may import from ``models`` and ``state``
within the compiler package, but must NOT import from ``phases/`` or
``pipeline.py``.
"""

from __future__ import annotations

import logging
from collections import deque
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from dataclasses import replace
from typing import cast

from rivet_core.compiler.models import (
    CompilationContext,
    CompilationDiagnostics,
    CompilationStats,
    CompilationWarning,
    CompiledAdapter,
    CompiledCatalog,
    CompiledEngine,
    CompiledJoint,
    EngineBoundary,
    EngineResolutionSource,
    ExecutionWave,
    Materialization,
    MaterializationStrategyName,
    MaterializationTrigger,
    OptimizationResult,
    ResolvedJointMetadata,
    SourceStats,
)
from rivet_core.errors import RivetError
from rivet_core.models import Catalog, ComputeEngine, Joint, Schema
from rivet_core.optimizer import (
    CheckpointSourceInfo,
    FusedGroup,
    _compose_cte,
    _compose_temp_view,
)
from rivet_core.plugins import CatalogPlugin, PluginRegistry, ReferenceResolver

logger = logging.getLogger("rivet_core.compiler")


def _resolve_engine(
    joint: Joint,
    engines: dict[str, ComputeEngine],
    default_engine: str | None,
) -> tuple[str, str, EngineResolutionSource | None]:
    """Resolve engine for a joint. Returns (engine_name, engine_type, resolution_path) or raises."""
    # Joint-level override
    if joint.engine:
        engine = engines.get(joint.engine)
        if engine:
            return engine.name, engine.engine_type, "joint_override"
        return joint.engine, "", "joint_override"  # will error on adapter lookup

    # Profile-level default
    if default_engine:
        engine = engines.get(default_engine)
        if engine:
            return engine.name, engine.engine_type, "project_default"
        return default_engine, "", "project_default"

    return "", "", ""


def _resolve_adapter(
    engine_type: str,
    catalog_type: str | None,
    engine_name: str,
    joint_name: str,
    registry: PluginRegistry,
    errors: list[RivetError],
    adapter_cache: dict[tuple[str, str], str | None] | None = None,
) -> str | None:
    """Resolve adapter for an engine/catalog pair. Returns adapter key or None."""
    if not engine_type or not catalog_type:
        return None

    key = (engine_type, catalog_type)
    if adapter_cache is not None and key in adapter_cache:
        return adapter_cache[key]

    adapter = registry.get_adapter(engine_type, catalog_type)
    if adapter:
        result = f"{engine_type}:{catalog_type}"
        if adapter_cache is not None:
            adapter_cache[key] = result
        return result

    caps = registry.resolve_capabilities(engine_type, catalog_type)
    if caps is None and engine_name:
        errors.append(
            RivetError(
                code="RVT-402",
                message=f"Engine '{engine_name}' (type '{engine_type}') does not support "
                f"catalog type '{catalog_type}' for joint '{joint_name}'.",
                context={
                    "joint": joint_name,
                    "engine": engine_name,
                    "engine_type": engine_type,
                    "catalog_type": catalog_type,
                },
                remediation=f"Register an adapter for ({engine_type}, {catalog_type}) "
                f"or use an engine that supports this catalog type.",
            )
        )

    if adapter_cache is not None:
        adapter_cache[key] = None
    return None


def _resolve_joint_metadata(
    joint: Joint,
    ctx: CompilationContext,
) -> ResolvedJointMetadata:
    """Resolve catalog, engine, and adapter metadata for a joint."""
    catalog = ctx.catalog_map.get(joint.catalog) if joint.catalog else None
    catalog_type = catalog.type if catalog else None
    catalog_plugin = ctx.registry.get_catalog_plugin(catalog_type) if catalog_type else None

    engine_name, engine_type, resolution = _resolve_engine(
        joint, ctx.engine_map, ctx.default_engine
    )
    if not engine_name:
        ctx.errors.append(
            RivetError(
                code="RVT-401",
                message=f"No compute engine resolved for joint '{joint.name}'. "
                f"Specify an engine on the joint or provide a default engine.",
                context={"joint": joint.name},
                remediation="Set engine on the joint or pass engines to compile().",
            )
        )
        engine_name = ""
        engine_type = ""
        resolution = ""

    if engine_name and not engine_type:
        eng = ctx.engine_map.get(engine_name)
        if eng:
            engine_type = eng.engine_type

    adapter_name = _resolve_adapter(
        engine_type,
        catalog_type,
        engine_name,
        joint.name,
        ctx.registry,
        ctx.errors,
        adapter_cache=ctx.adapter_cache,
    )

    return ResolvedJointMetadata(
        catalog=catalog,
        catalog_type=catalog_type,
        catalog_plugin=catalog_plugin,
        engine_name=engine_name,
        engine_type=engine_type,
        resolution=resolution,
        adapter_name=adapter_name,
    )


def _resolve_table_map_name(
    joint: Joint,
    catalog: Catalog | None,
    catalog_map: dict[str, Catalog],
) -> str:
    """Resolve the physical table name via ``table_map``.

    Applies the same ``table_map`` alias resolution used during compilation
    (Phase 4) so that introspection, compilation, and execution all see the
    same physical name.

    Falls back to ``joint.table or joint.path or joint.name`` when no
    ``table_map`` entry matches.
    """
    lookup_key = joint.table or joint.name
    cat_obj = catalog

    # When the joint's catalog name doesn't match any profile catalog,
    # search all catalogs for a table_map entry that matches.
    if cat_obj is None and joint.catalog:
        for candidate in catalog_map.values():
            candidate_map: dict[str, str] = candidate.options.get("table_map", {})
            if lookup_key in candidate_map:
                cat_obj = candidate
                break

    if cat_obj is not None:
        table_map: dict[str, str] = cat_obj.options.get("table_map", {})
        if lookup_key in table_map:
            return table_map[lookup_key]

    return joint.table or joint.path or joint.name


def _resolve_strategy(
    fused_groups: list[FusedGroup],
    cj_map: dict[str, CompiledJoint],
    default_fusion_strategy: str,
    errors: list[RivetError],
) -> list[FusedGroup]:
    """Resolve fusion and materialization strategies for each group."""
    VALID_FUSION = {"cte", "temp_view"}
    VALID_MATERIALIZATION = {"arrow", "temp_table"}

    result = list(fused_groups)
    for idx, group in enumerate(result):
        overrides: set[str] = set()
        for jn in group.joints:
            cj = cj_map[jn]
            if cj.fusion_strategy_override:
                overrides.add(cj.fusion_strategy_override)

        if len(overrides) > 1:
            errors.append(
                RivetError(
                    code="RVT-603",
                    message=f"Conflicting fusion strategy overrides in group '{group.id}': {sorted(overrides)}.",
                    context={"group_id": group.id, "overrides": sorted(overrides)},
                    remediation="Ensure all joints in a fused group use the same fusion strategy override.",
                )
            )
        resolved_fusion = overrides.pop() if len(overrides) == 1 else default_fusion_strategy
        if resolved_fusion not in VALID_FUSION:
            errors.append(
                RivetError(
                    code="RVT-601",
                    message=f"Invalid fusion strategy '{resolved_fusion}'. Valid options: {sorted(VALID_FUSION)}.",
                    context={"strategy": resolved_fusion},
                    remediation=f"Use one of: {sorted(VALID_FUSION)}.",
                )
            )
            resolved_fusion = default_fusion_strategy

        if resolved_fusion != group.fusion_strategy:
            joint_sql: dict[str, str | None] = {jn: cj_map[jn].sql for jn in group.joints}
            if resolved_fusion == "cte":
                new_result = _compose_cte(group.joints, joint_sql)
            else:
                new_result = _compose_temp_view(group.joints, joint_sql)
            group = replace(
                group,
                fusion_strategy=resolved_fusion,
                fusion_result=new_result,
                fused_sql=new_result.fused_sql if new_result else None,
            )

        for jn in group.joints:
            cj = cj_map[jn]
            mat_override = cj.materialization_strategy_override
            if mat_override and mat_override not in VALID_MATERIALIZATION:
                errors.append(
                    RivetError(
                        code="RVT-602",
                        message=f"Invalid materialization strategy '{mat_override}' on joint '{jn}'. "
                        f"Valid options: {sorted(VALID_MATERIALIZATION)}.",
                        context={"joint": jn, "strategy": mat_override},
                        remediation=f"Use one of: {sorted(VALID_MATERIALIZATION)}.",
                    )
                )

        result[idx] = group
    return result


def _get_resolver_for_engine_type(
    engine_type: str,
    registry: PluginRegistry,
) -> ReferenceResolver | None:
    """Return the reference resolver for a specific engine type, or None."""
    plugin = registry.get_engine_plugin(engine_type)
    if plugin:
        return plugin.get_reference_resolver()
    return None


def _resolve_references(
    fused_groups: list[FusedGroup],
    cj_map: dict[str, CompiledJoint],
    compiled_joints: list[CompiledJoint],
    engine_map: dict[str, ComputeEngine],
    catalog_map: dict[str, Catalog],
    registry: PluginRegistry,
    resolve_references: ReferenceResolver | None,
    warnings: list[str],
) -> list[FusedGroup]:
    """Resolve SQL references in fused groups.

    Each group is resolved using the reference resolver from its own engine
    plugin.  An explicitly provided *resolve_references* overrides auto-
    discovery and is applied to all groups (for backward compatibility with
    tests and single-engine projects).

    In multi-engine plans, this prevents a resolver from one engine type
    (e.g. postgres) from rewriting SQL in groups belonging to a different
    engine type (e.g. duckdb).
    """
    result = list(fused_groups)
    resolver_cache: dict[str, ReferenceResolver | None] = {}
    compiled_catalog_cache: dict[str, CompiledCatalog | None] = {}
    for idx, group in enumerate(result):
        # Per-group resolver: use the explicit override if provided,
        # otherwise look up the resolver for this group's engine type.
        resolver: ReferenceResolver | None
        if resolve_references is not None:
            resolver = resolve_references
        else:
            if group.engine_type not in resolver_cache:
                resolver_cache[group.engine_type] = _get_resolver_for_engine_type(
                    group.engine_type,
                    registry,
                )
            resolver = resolver_cache[group.engine_type]
        if resolver is None:
            continue

        any_resolved = False
        group_joints = list(group.joints)
        for jn in group.joints:
            cj = cj_map[jn]
            if not (cj.sql_translated or cj.sql):
                continue
            # Source joints only need resolution when fused with other joints
            # (to avoid self-referencing CTEs like `x AS (SELECT * FROM x)`).
            if cj.type == "source" and len(group.joints) < 2:
                continue
            if cj.type not in ("sql", "sink", "source"):
                continue
            input_sql = cj.sql_translated or cj.sql
            assert input_sql is not None
            compiled_cat = None
            if cj.catalog:
                if cj.catalog not in compiled_catalog_cache:
                    cat = catalog_map.get(cj.catalog)
                    compiled_catalog_cache[cj.catalog] = (
                        CompiledCatalog(name=cat.name, type=cat.type, options=dict(cat.options))
                        if cat
                        else None
                    )
                compiled_cat = compiled_catalog_cache[cj.catalog]
            try:
                resolved = resolver.resolve_references(
                    input_sql,
                    cj,
                    compiled_cat,
                    compiled_joints=cj_map,
                    catalog_map=catalog_map,
                    fused_group_joints=group_joints,
                )
                if resolved and resolved != input_sql:
                    cj_map[jn] = replace(cj, sql_resolved=resolved)
                    any_resolved = True
            except Exception as e:
                warnings.append(f"Reference resolution failed for joint '{jn}': {e}")

        if any_resolved:
            resolved_joint_sql: dict[str, str | None] = {}
            for jn in group.joints:
                cj = cj_map[jn]
                resolved_joint_sql[jn] = cj.sql_resolved or cj.sql_translated or cj.sql
            if group.fusion_strategy == "cte":
                resolved_result = _compose_cte(group.joints, resolved_joint_sql)
            else:
                resolved_result = _compose_temp_view(group.joints, resolved_joint_sql)
            if resolved_result:
                new_fusion_result = group.fusion_result
                if new_fusion_result:
                    new_fusion_result = replace(
                        new_fusion_result,
                        resolved_fused_sql=resolved_result.fused_sql,
                        resolved_statements=resolved_result.statements,
                        resolved_final_select=resolved_result.final_select,
                    )
                result[idx] = replace(
                    group,
                    resolved_sql=resolved_result.fused_sql,
                    fusion_result=new_fusion_result,
                )
    return result


def _build_compiled_catalogs(
    compiled_joints: list[CompiledJoint],
    catalogs: list[Catalog],
) -> list[CompiledCatalog]:
    """Build compiled catalog list from used catalogs."""
    used = {cj.catalog for cj in compiled_joints if cj.catalog}
    return [
        CompiledCatalog(name=c.name, type=c.type, options=dict(c.options))
        for c in catalogs
        if c.name in used
    ]


def _build_compiled_engines(
    compiled_joints: list[CompiledJoint],
    engines: list[ComputeEngine],
    registry: PluginRegistry,
) -> list[CompiledEngine]:
    """Build compiled engine list from used engines."""
    used = {cj.engine for cj in compiled_joints if cj.engine}
    result: list[CompiledEngine] = []
    for e in engines:
        if e.name in used:
            plugin = registry.get_engine_plugin(e.engine_type)
            native = list(plugin.supported_catalog_types.keys()) if plugin else []
            result.append(
                CompiledEngine(name=e.name, engine_type=e.engine_type, native_catalog_types=native)
            )
    return result


def _build_compiled_adapters(
    compiled_joints: list[CompiledJoint],
    engine_map: dict[str, ComputeEngine],
    registry: PluginRegistry,
) -> list[CompiledAdapter]:
    """Build compiled adapter list from used adapters."""
    used_keys: set[tuple[str, str]] = set()
    for cj in compiled_joints:
        if cj.adapter and cj.catalog_type:
            used_keys.add((cj.engine, cj.catalog_type))
    result: list[CompiledAdapter] = []
    for et, ct in used_keys:
        eng = engine_map.get(et)
        e_type = eng.engine_type if eng else et
        adapter = registry.get_adapter(e_type, ct)
        if adapter:
            result.append(
                CompiledAdapter(
                    engine_type=adapter.target_engine_type,
                    catalog_type=adapter.catalog_type,
                    source=adapter.source,
                )
            )
    return result


def _build_downstream_map(cj_map: dict[str, CompiledJoint]) -> dict[str, list[str]]:
    """Build a downstream dependency map in O(V+E).

    For each joint, collect the list of joints that depend on it by iterating
    each joint's upstream list once.
    """
    downstream: dict[str, list[str]] = {jn: [] for jn in cj_map}
    for cj in cj_map.values():
        for up in cj.upstream:
            if up in downstream:
                downstream[up].append(cj.name)
    return downstream


def _determine_materializations(
    cj_map: dict[str, CompiledJoint],
    joint_to_group: dict[str, str],
    engine_map: dict[str, ComputeEngine],
    registry: PluginRegistry,
    default_materialization_strategy: MaterializationStrategyName,
    boundary_joints: set[str] | None = None,
) -> list[Materialization]:
    """Determine materialization points between joints.

    When a joint has assertion-phase checks and the engine plugin declares
    ``supports_native_assertions = True`` and all assertion-phase checks are
    SQL-translatable, the ``"assertion_boundary"`` trigger is suppressed.
    An ``OptimizationResult`` is recorded on the joint for observability.

    Parameters
    ----------
    boundary_joints:
        Joint names that sit on an engine-type boundary, as computed by
        ``_detect_engine_boundaries``.  When provided, the
        ``"engine_instance_change"`` trigger is derived from this set
        instead of performing an ad-hoc engine-name comparison.
    """
    from rivet_core.executor import _is_sql_translatable

    VALID_MATERIALIZATION = {"arrow", "temp_table"}
    downstream_map = _build_downstream_map(cj_map)

    materializations: list[Materialization] = []
    for cj in cj_map.values():
        # Record assertion boundary optimization results for joints with checks
        has_assertion_checks = any(c.phase == "assertion" for c in cj.checks)
        if has_assertion_checks:
            eng = engine_map.get(cj.engine)
            engine_type = eng.engine_type if eng else ""
            plugin = registry.get_engine_plugin(engine_type) if engine_type else None
            assertion_checks = [c for c in cj.checks if c.phase == "assertion"]
            native_ok = (
                plugin is not None
                and plugin.supports_native_assertions
                and all(_is_sql_translatable(c) for c in assertion_checks)
            )
            if native_ok:
                cj_map[cj.name] = replace(
                    cj,
                    optimizations=[
                        *cj.optimizations,
                        OptimizationResult(
                            rule="assertion_boundary_suppressed",
                            status="applied",
                            detail=f"Joint '{cj.name}' assertions executed engine-natively on {engine_type}",
                        ),
                    ],
                )
            else:
                reason = (
                    f"Engine '{engine_type}' does not support native assertions"
                    if plugin is None or not plugin.supports_native_assertions
                    else f"Joint '{cj.name}' has non-SQL-translatable checks: "
                    + ", ".join(
                        sorted({c.type for c in assertion_checks if not _is_sql_translatable(c)})
                    )
                )
                cj_map[cj.name] = replace(
                    cj,
                    optimizations=[
                        *cj.optimizations,
                        OptimizationResult(
                            rule="assertion_boundary_suppressed",
                            status="not_applicable",
                            detail=reason,
                        ),
                    ],
                )
        else:
            native_ok = False

        for ds_name in downstream_map.get(cj.name, []):
            ds = cj_map[ds_name]
            trigger: MaterializationTrigger | None = None
            detail = ""

            if cj.type == "checkpoint":
                trigger = "checkpoint_boundary"
                detail = f"Joint '{cj.name}' is a checkpoint exit joint"
            elif cj.eager:
                trigger = "eager"
                detail = f"Joint '{cj.name}' declares eager=true"
            elif ds.type == "python":
                trigger = "python_boundary"
                detail = f"Downstream joint '{ds_name}' is a PythonJoint"
            elif bool(cj.checks) and not native_ok:
                trigger = "assertion_boundary"
                detail = f"Joint '{cj.name}' has assertions"
            elif len(downstream_map.get(cj.name, [])) > 1:
                trigger = "multi_consumer"
                detail = (
                    f"Joint '{cj.name}' has {len(downstream_map[cj.name])} downstream consumers"
                )
            else:
                _boundary_joints = boundary_joints or set()
                if cj.name in _boundary_joints:
                    eng_from = engine_map.get(cj.engine)
                    eng_to = engine_map.get(ds.engine)
                    from_name = eng_from.name if eng_from else cj.engine
                    to_name = eng_to.name if eng_to else ds.engine
                    trigger = "engine_instance_change"
                    detail = f"Engine changes from '{from_name}' to '{to_name}'"
                elif joint_to_group.get(cj.name) != joint_to_group.get(ds_name):
                    trigger = "capability_gap"
                    detail = f"Joints '{cj.name}' and '{ds_name}' are in different fused groups"

            if trigger:
                if trigger == "checkpoint_boundary":
                    mat_strategy: MaterializationStrategyName = "arrow"
                else:
                    candidate_strategy = (
                        cj.materialization_strategy_override or default_materialization_strategy
                    )
                    if candidate_strategy not in VALID_MATERIALIZATION:
                        candidate_strategy = default_materialization_strategy
                    mat_strategy = cast(MaterializationStrategyName, candidate_strategy)
                materializations.append(
                    Materialization(
                        from_joint=cj.name,
                        to_joint=ds_name,
                        trigger=trigger,
                        detail=detail,
                        strategy=mat_strategy,
                    )
                )
    return materializations


def _detect_engine_boundaries(
    fused_groups: list[FusedGroup],
    cj_map: dict[str, CompiledJoint],
    joint_to_group: dict[str, str],
    registry: PluginRegistry,
    warnings: list[str],
) -> list[EngineBoundary]:
    """Detect engine type changes between adjacent fused groups."""
    group_map: dict[str, FusedGroup] = {g.id: g for g in fused_groups}
    boundary_joints_map: dict[tuple[str, str], list[str]] = {}

    for group in fused_groups:
        group_et = group.engine_type
        for jn in group.entry_joints or group.joints:
            cj = cj_map.get(jn)
            if not cj:
                continue
            for up in cj.upstream:
                up_gid = joint_to_group.get(up)
                if not up_gid or up_gid == group.id:
                    continue
                # up_gid is guaranteed to be in group_map since it came from
                # joint_to_group which is built from the same fused_groups list.
                up_et = group_map[up_gid].engine_type
                if up_et == group_et:
                    continue
                key = (up_gid, group.id)
                bj = boundary_joints_map.setdefault(key, [])
                if up not in bj:
                    bj.append(up)

    boundaries: list[EngineBoundary] = []
    for (prod_gid, cons_gid), joints in boundary_joints_map.items():
        prod_et = group_map[prod_gid].engine_type
        cons_et = group_map[cons_gid].engine_type
        adapter = registry.get_cross_joint_adapter(cons_et, prod_et)
        if adapter is None:
            warnings.append(
                f"RVT-504: No CrossJointAdapter registered for "
                f"({cons_et}, {prod_et}) boundary "
                f"at joints {joints}. Default arrow passthrough will be used."
            )
            strategy = "default: arrow_passthrough"
        else:
            strategy = type(adapter).__qualname__
        boundaries.append(
            EngineBoundary(
                producer_group_id=prod_gid,
                consumer_group_id=cons_gid,
                producer_engine_type=prod_et,
                consumer_engine_type=cons_et,
                boundary_joints=joints,
                adapter_strategy=strategy,
            )
        )
    return boundaries


def _build_checkpoint_sources(
    fused_groups: list[FusedGroup],
    cj_map: dict[str, CompiledJoint],
    joint_to_group: dict[str, str],
    registry: PluginRegistry,
    warnings: list[str],
) -> list[FusedGroup]:
    """Pre-resolve checkpoint-to-downstream adapter metadata on each FusedGroup.

    For each group, finds upstream joints that are checkpoints (type == "checkpoint")
    and resolves the adapter for (group.engine_type, checkpoint.catalog_type).
    Stores the result in group.checkpoint_sources.

    This makes checkpoint resolution visible at compile time:
    - Missing adapters produce warnings
    - The compiled output shows which adapters will be used
    """
    updated: list[FusedGroup] = []
    for group in fused_groups:
        cp_sources: dict[str, CheckpointSourceInfo] = {}
        # Iterate ALL joints in the group, not just entry_joints.
        # A joint can have upstream both inside and outside the group,
        # so it won't be an entry joint but still needs checkpoint resolution.
        for jn in group.joints:
            cj = cj_map.get(jn)
            if not cj:
                continue
            for up in cj.upstream:
                up_gid = joint_to_group.get(up)
                up_cj = cj_map.get(up)
                if not up_gid or up_gid == group.id:
                    continue
                if not up_cj or up_cj.type != "checkpoint":
                    continue
                if up in cp_sources:
                    continue
                catalog_type = up_cj.catalog_type
                catalog_name = up_cj.catalog or ""
                table_name = up_cj.table or up_cj.name

                adapter_name: str | None = None
                if catalog_type:
                    adapter = registry.get_adapter(group.engine_type, catalog_type)
                    if adapter:
                        adapter_name = f"{group.engine_type}:{catalog_type}"
                    else:
                        warnings.append(
                            f"No adapter for ({group.engine_type}, {catalog_type}) "
                            f"to read checkpoint '{up}' in group '{group.id}'. "
                            f"Will fall back to SourcePlugin or Arrow passthrough."
                        )

                cp_sources[up] = CheckpointSourceInfo(
                    checkpoint_joint=up,
                    catalog=catalog_name,
                    catalog_type=catalog_type or "",
                    table=table_name,
                    adapter=adapter_name,
                )
        if cp_sources:
            updated.append(replace(group, checkpoint_sources=cp_sources))
        else:
            updated.append(group)
    return updated


def _build_checkpoint_fq_name(
    cj: CompiledJoint,
    catalog: Catalog | None,
) -> str:
    """Build fully-qualified table name for a checkpoint CTE.

    Uses catalog options (catalog_name/catalog, schema) + checkpoint's table field.
    Consistent with DatabricksReferenceResolver FQ name logic.
    """
    table = cj.table or cj.name

    if not catalog:
        return table

    opts = catalog.options
    db_catalog = opts.get("catalog_name") or opts.get("catalog")
    if not db_catalog:
        return table

    parts = table.split(".")
    if len(parts) == 3:
        return table

    db_schema = opts.get("schema", "default")
    if len(parts) == 2:
        return f"{db_catalog}.{table}"

    return f"{db_catalog}.{db_schema}.{table}"


def _resolve_checkpoint_cte_body(
    cp_name: str,
    cp_cj: CompiledJoint,
    catalog_map: dict[str, Catalog],
    cj_map: dict[str, CompiledJoint],
    resolver: ReferenceResolver,
    group_joints: list[str],
) -> str | None:
    """Ask the engine's resolver to produce an engine-native SELECT for a checkpoint CTE.

    Constructs ``SELECT * FROM <cp_name>`` and passes it through the resolver
    with a synthetic joint whose upstream is ``[cp_name]``.  The resolver
    rewrites ``<cp_name>`` to the engine-native expression (e.g.
    ``read_parquet(...)`` for DuckDB filesystem catalogs).

    Returns the resolved SQL string, or ``None`` if the resolver cannot
    resolve the reference.
    """
    from types import SimpleNamespace

    synthetic_sql = f"SELECT * FROM {cp_name}"
    synthetic_joint = SimpleNamespace(
        name="__checkpoint_cte_synthetic__",
        type="sql",
        upstream=[cp_name],
        sql=synthetic_sql,
        sql_translated=None,
        catalog=None,
        table=None,
    )
    try:
        resolved = resolver.resolve_references(
            synthetic_sql,
            synthetic_joint,
            None,
            compiled_joints=cj_map,
            catalog_map=catalog_map,
            fused_group_joints=group_joints,
        )
        if resolved and resolved != synthetic_sql:
            return resolved
    except Exception:
        logger.debug('Synthetic SQL re-resolution', exc_info=True)  # best-effort: see RVT logs at debug level
    return None


def _inject_checkpoint_ctes(
    fused_groups: list[FusedGroup],
    cj_map: dict[str, CompiledJoint],
    catalog_map: dict[str, Catalog],
    registry: PluginRegistry | None = None,
) -> list[FusedGroup]:
    """Prepend cross-group checkpoint CTEs to downstream fused groups.

    For each group with checkpoint_sources, builds CTE entries and prepends
    them to the group's fused SQL.  When a *registry* is provided, the
    engine's :class:`ReferenceResolver` is used to produce engine-native
    table references (e.g. ``read_parquet(...)`` for DuckDB filesystem
    catalogs).  Falls back to :func:`_build_checkpoint_fq_name` when no
    resolver is available or when the resolver cannot resolve the reference.
    """
    updated: list[FusedGroup] = []
    resolver_cache: dict[str, ReferenceResolver | None] = {}
    for group in fused_groups:
        if not group.checkpoint_sources:
            updated.append(group)
            continue

        # Obtain the resolver for this group's engine type (cached).
        resolver: ReferenceResolver | None = None
        if registry is not None:
            if group.engine_type not in resolver_cache:
                resolver_cache[group.engine_type] = _get_resolver_for_engine_type(
                    group.engine_type, registry
                )
            resolver = resolver_cache[group.engine_type]

        cte_parts: list[str] = []
        for cp_name, _cp_info in group.checkpoint_sources.items():
            cp_cj = cj_map.get(cp_name)
            if not cp_cj:
                continue

            # Try engine-native resolution via the resolver first.
            resolved_body: str | None = None
            if resolver is not None:
                resolved_body = _resolve_checkpoint_cte_body(
                    cp_name, cp_cj, catalog_map, cj_map, resolver, list(group.joints)
                )

            if resolved_body is not None:
                cte_parts.append(f"{cp_name} AS (\n    {resolved_body}\n)")
            else:
                # No resolver available — the engine will receive checkpoint
                # data via input_tables registered under the joint name, so
                # reference the joint name directly.  The database-style FQ
                # name (via _build_checkpoint_fq_name) only works for engines
                # with native catalog access (Databricks / Unity) which always
                # have a resolver.
                cte_parts.append(f"{cp_name} AS (\n    SELECT * FROM {cp_name}\n)")

        if not cte_parts:
            updated.append(group)
            continue

        # Update group-level fused SQL
        new_fused_sql = (
            _prepend_ctes(group.fused_sql, cte_parts) if group.fused_sql else group.fused_sql
        )
        new_resolved_sql = (
            _prepend_ctes(group.resolved_sql, cte_parts)
            if group.resolved_sql
            else group.resolved_sql
        )

        # Update fusion_result fields
        new_fusion_result = group.fusion_result
        if new_fusion_result:
            new_fr_fused_sql = _prepend_ctes(new_fusion_result.fused_sql, cte_parts)
            new_fr_resolved_fused_sql = (
                _prepend_ctes(new_fusion_result.resolved_fused_sql, cte_parts)
                if new_fusion_result.resolved_fused_sql
                else None
            )
            new_statements = cte_parts + list(new_fusion_result.statements)
            new_resolved_statements = (
                cte_parts + list(new_fusion_result.resolved_statements)
                if new_fusion_result.resolved_statements is not None
                else None
            )
            new_fusion_result = replace(
                new_fusion_result,
                fused_sql=new_fr_fused_sql,
                resolved_fused_sql=new_fr_resolved_fused_sql,
                statements=new_statements,
                resolved_statements=new_resolved_statements,
            )

        updated.append(
            replace(
                group,
                fused_sql=new_fused_sql,
                resolved_sql=new_resolved_sql,
                fusion_result=new_fusion_result,
            )
        )
    return updated


def _prepend_ctes(sql: str, cte_parts: list[str]) -> str:
    """Prepend checkpoint CTE definitions to a SQL string.

    If the SQL already starts with WITH, the checkpoint CTEs are inserted
    before the existing CTEs. Otherwise a new WITH clause is created.
    """
    joined = ",\n".join(cte_parts)
    if sql.upper().startswith("WITH "):
        # Strip "WITH " and prepend checkpoint CTEs before existing ones
        rest = sql[5:]  # everything after "WITH "
        return f"WITH {joined},\n{rest}"
    else:
        # No existing WITH clause — wrap checkpoint CTEs + original SQL
        return f"WITH {joined}\n{sql}"


def _build_compilation_diagnostics(
    errors: list[RivetError],
    warnings: list[str],
    stats: CompilationStats | None = None,
) -> CompilationDiagnostics:
    return CompilationDiagnostics(
        errors=errors,
        warnings=[CompilationWarning(message=warning) for warning in warnings],
        stats=stats,
    )


def _do_introspect(
    joint: Joint,
    catalog: Catalog,
    catalog_plugin: CatalogPlugin,
    catalog_map: dict[str, Catalog],
) -> tuple[Schema | None, SourceStats | None, list[str]]:
    """Inner introspection logic — runs inside thread for timeout."""
    if not catalog or not catalog_plugin:
        return None, None, []

    from rivet_core.models import Column

    schema: Schema | None = None
    source_stats: SourceStats | None = None
    local_warnings: list[str] = []
    table_name = _resolve_table_map_name(joint, catalog, catalog_map)

    # Schema
    try:
        obj_schema = catalog_plugin.get_schema(catalog, table_name)
        schema = Schema(
            columns=[
                Column(name=c.name, type=c.type, nullable=c.nullable) for c in obj_schema.columns
            ]
        )
    except NotImplementedError:
        pass
    except Exception as e:
        local_warnings.append(f"Introspection failed for source '{joint.name}': {e}")

    # Metadata
    try:
        meta = catalog_plugin.get_metadata(catalog, table_name)
        if meta is not None:
            source_stats = SourceStats(
                row_count=meta.row_count,
                size_bytes=meta.size_bytes,
                last_modified=meta.last_modified,
                partition_count=(len(meta.partitioning.partitions) if meta.partitioning else None),
            )
    except NotImplementedError:
        pass
    except Exception as e:
        local_warnings.append(f"Introspection failed for source '{joint.name}': {e}")

    return schema, source_stats, local_warnings


def _introspect_source(
    joint: Joint,
    catalog: Catalog | None,
    catalog_plugin: CatalogPlugin | None,
    warnings: list[str],
    timeout_seconds: float = 5.0,
    catalog_map: dict[str, Catalog] | None = None,
) -> tuple[Schema | None, SourceStats | None]:
    """Attempt introspection for source joints. Returns (schema, source_stats).

    Enforces per-source timeout. Never raises.
    """
    if not catalog or not catalog_plugin:
        return None, None
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            _do_introspect,
            joint,
            catalog,
            catalog_plugin,
            catalog_map or {},
        )
        try:
            schema, source_stats, local_warnings = future.result(timeout=timeout_seconds)
            warnings.extend(local_warnings)
            return schema, source_stats
        except TimeoutError:
            warnings.append(
                f"Introspection timed out for source '{joint.name}' after {timeout_seconds}s"
            )
            return None, None


def _list_adapter_catalog_types(registry: PluginRegistry, engine_type: str) -> list[str]:
    """List catalog types that have adapters registered for the given engine type."""
    # Access the internal adapter dict — keys are (engine_type, catalog_type) tuples
    result: list[str] = []
    for et, ct in registry._adapters:  # type: ignore[attr-defined]
        if et == engine_type:
            result.append(ct)
    return result


def _list_adapter_engine_types(registry: PluginRegistry, catalog_type: str) -> list[str]:
    """List engine types that have adapters registered for the given catalog type."""
    result: list[str] = []
    for et, ct in registry._adapters:  # type: ignore[attr-defined]
        if ct == catalog_type:
            result.append(et)
    return result


def _compute_parallel_execution_plan(
    fused_groups: list[FusedGroup],
    cj_map: dict[str, CompiledJoint],
    warnings: list[str] | None = None,
) -> list[ExecutionWave]:
    """Compute the parallel execution plan using wavefront analysis.

    Locally reimplements the DependencyGraph edge-building logic to avoid
    circular imports (compiler → executor).

    Algorithm:
    1. Build upstream/in-degree maps from fused groups and compiled joints.
    2. Groups with in-degree 0 → wave 1.
    3. Remove wave 1 groups, find new in-degree 0 groups → wave 2.
    4. Repeat until all groups are assigned.
    """
    if not fused_groups:
        return []

    # Map each joint name to its owning fused group ID
    joint_to_group: dict[str, str] = {}
    group_by_id: dict[str, FusedGroup] = {}
    for group in fused_groups:
        group_by_id[group.id] = group
        for joint_name in group.joints:
            joint_to_group[joint_name] = group.id

    # Build upstream and downstream edges
    upstream: dict[str, set[str]] = {g.id: set() for g in fused_groups}
    downstream: dict[str, set[str]] = {g.id: set() for g in fused_groups}

    for group in fused_groups:
        for joint_name in group.joints:
            compiled_joint = cj_map.get(joint_name)
            if compiled_joint is None:
                continue
            for up_name in compiled_joint.upstream:
                up_group_id = joint_to_group.get(up_name)
                if up_group_id is None or up_group_id == group.id:
                    continue
                upstream[group.id].add(up_group_id)
                downstream[up_group_id].add(group.id)

    in_degree: dict[str, int] = {gid: len(ups) for gid, ups in upstream.items()}
    group_order = [group.id for group in fused_groups]
    order_index = {gid: idx for idx, gid in enumerate(group_order)}

    # Wavefront assignment
    ready = deque(gid for gid in group_order if in_degree[gid] == 0)
    waves: list[ExecutionWave] = []
    wave_number = 0
    assigned: set[str] = set()

    while ready:
        wave_number += 1
        current_wave = [ready.popleft() for _ in range(len(ready))]
        assigned.update(current_wave)

        # Build engine mapping for this wave
        engines: dict[str, list[str]] = {}
        for gid in current_wave:
            engine_name = group_by_id[gid].engine
            engines.setdefault(engine_name, []).append(gid)

        waves.append(
            ExecutionWave(
                wave_number=wave_number,
                groups=current_wave,
                engines=engines,
            )
        )

        next_ready: set[str] = set()
        for gid in current_wave:
            for ds_id in downstream.get(gid, set()):
                in_degree[ds_id] -= 1
                if in_degree[ds_id] == 0 and ds_id not in assigned:
                    next_ready.add(ds_id)

        for gid in sorted(next_ready, key=order_index.__getitem__):
            ready.append(gid)

    unresolved = [gid for gid in group_order if gid not in assigned]
    if unresolved and warnings is not None:
        warnings.append(
            "Parallel execution plan could not be fully computed because fused group "
            "dependencies are cyclic or inconsistent. Remaining groups: "
            f"{', '.join(unresolved)}."
        )

    return waves
