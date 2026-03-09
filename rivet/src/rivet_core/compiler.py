"""Compilation data models and compile() function for rivet-core.

CompiledAssembly is the single source of truth produced by compile() and
consumed by the Executor. All models are immutable frozen dataclasses.
"""

from __future__ import annotations

import importlib
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any

from rivet_core.assembly import Assembly
from rivet_core.checks import CompiledCheck
from rivet_core.errors import RivetError, SQLParseError
from rivet_core.lineage import ColumnLineage, ColumnOrigin
from rivet_core.models import Catalog, ComputeEngine, Joint, Schema
from rivet_core.optimizer import (
    FusedGroup,
    FusionJoint,
    _compose_cte,
    _compose_temp_view,
    cross_group_pushdown_pass,
    fusion_pass,
    pushdown_pass,
)
from rivet_core.plugins import CatalogPlugin, PluginRegistry, ReferenceResolver
from rivet_core.sql_parser import LogicalPlan, SQLParser

# ---------------------------------------------------------------------------
# Data models (task 12.1)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SourceStats:
    """Cheap table-level metadata from catalog introspection.

    All fields optional — catalogs report what they can.
    """

    row_count: int | None = None
    size_bytes: int | None = None
    last_modified: datetime | None = None
    partition_count: int | None = None


@dataclass(frozen=True)
class EngineBoundary:
    """Records an engine type change between adjacent fused groups."""

    producer_group_id: str
    consumer_group_id: str
    producer_engine_type: str
    consumer_engine_type: str
    boundary_joints: list[str]
    adapter_strategy: str | None = None


@dataclass(frozen=True)
class OptimizationResult:
    rule: str
    status: str  # "applied", "not_applicable", "capability_gap"
    detail: str
    pushed: str | None = None
    residual: str | None = None


@dataclass(frozen=True)
class Materialization:
    from_joint: str
    to_joint: str
    trigger: str  # "eager", "engine_instance_change", "capability_gap",
    #               "python_boundary", "assertion_boundary", "multi_consumer"
    detail: str
    strategy: str  # "arrow" or "temp_table"


@dataclass(frozen=True)
class CompiledCatalog:
    name: str
    type: str
    options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CompiledEngine:
    name: str
    engine_type: str
    native_catalog_types: list[str]


@dataclass(frozen=True)
class CompiledAdapter:
    engine_type: str
    catalog_type: str
    source: str


@dataclass(frozen=True)
class CompiledJoint:
    name: str
    type: str  # "source", "sql", "sink", "python"
    catalog: str | None
    catalog_type: str | None
    engine: str
    engine_resolution: str | None  # "joint_override", "catalog_default", "project_default"
    adapter: str | None
    sql: str | None
    sql_translated: str | None
    sql_resolved: str | None
    sql_dialect: str | None
    engine_dialect: str | None
    upstream: list[str]
    eager: bool
    table: str | None
    write_strategy: str | None
    function: str | None
    source_file: str | None
    logical_plan: LogicalPlan | None
    output_schema: Schema | None
    column_lineage: list[ColumnLineage]
    optimizations: list[OptimizationResult]
    checks: list[CompiledCheck]
    fused_group_id: str | None
    tags: list[str]
    description: str | None
    fusion_strategy_override: str | None
    materialization_strategy_override: str | None
    source_stats: SourceStats | None = None
    schema_confidence: str = "none"


@dataclass(frozen=True)
class CompilationStats:
    """Metrics about the compilation process itself."""

    compile_duration_ms: int
    joints_with_schema: int
    joints_total: int
    introspection_attempted: int
    introspection_succeeded: int
    introspection_failed: int
    introspection_skipped: int


@dataclass(frozen=True)
class ExecutionWave:
    """A set of fused groups that can execute concurrently."""

    wave_number: int
    groups: list[str]  # fused group IDs
    engines: dict[str, list[str]]  # engine_name → group_ids on that engine


@dataclass(frozen=True)
class CompiledAssembly:
    success: bool
    profile_name: str
    catalogs: list[CompiledCatalog]
    engines: list[CompiledEngine]
    adapters: list[CompiledAdapter]
    joints: list[CompiledJoint]
    fused_groups: list[FusedGroup]
    materializations: list[Materialization]
    execution_order: list[str]  # fused group IDs + standalone joint names
    errors: list[RivetError]
    warnings: list[str]
    engine_boundaries: list[EngineBoundary] = field(default_factory=list)
    compilation_stats: CompilationStats | None = None
    parallel_execution_plan: list[ExecutionWave] = field(default_factory=list)


# ---------------------------------------------------------------------------
# compile() — resolution steps (task 12.2)
# ---------------------------------------------------------------------------


def _resolve_engine(
    joint: Joint,
    engines: dict[str, ComputeEngine],
    default_engine: str | None,
) -> tuple[str, str, str | None]:
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


def _verify_callable(function_path: str) -> bool:
    """Check if a colon-separated function path (module:func) is importable."""
    parts = function_path.rsplit(":", 1)
    if len(parts) != 2:
        return False
    module_path, func_name = parts
    try:
        mod = importlib.import_module(module_path)
        return callable(getattr(mod, func_name, None))
    except Exception:
        return False


def _do_introspect(
    joint: Joint,
    catalog: Catalog,
    catalog_plugin: CatalogPlugin,
    warnings: list[str],
) -> tuple[Schema | None, SourceStats | None]:
    """Inner introspection logic — runs inside thread for timeout."""
    if not catalog or not catalog_plugin:
        return None, None

    from rivet_core.models import Column

    schema: Schema | None = None
    source_stats: SourceStats | None = None
    table_name = joint.table or joint.path or joint.name

    # Schema
    try:
        obj_schema = catalog_plugin.get_schema(catalog, table_name)
        schema = Schema(
            columns=[
                Column(name=c.name, type=c.type, nullable=c.nullable)
                for c in obj_schema.columns
            ]
        )
    except NotImplementedError:
        pass
    except Exception as e:
        warnings.append(f"Introspection failed for source '{joint.name}': {e}")

    # Metadata
    try:
        meta = catalog_plugin.get_metadata(catalog, table_name)
        if meta is not None:
            source_stats = SourceStats(
                row_count=meta.row_count,
                size_bytes=meta.size_bytes,
                last_modified=meta.last_modified,
                partition_count=(
                    len(meta.partitioning.partitions) if meta.partitioning else None
                ),
            )
    except NotImplementedError:
        pass
    except Exception as e:
        warnings.append(f"Introspection failed for source '{joint.name}': {e}")

    return schema, source_stats


def _introspect_source(
    joint: Joint,
    catalog: Catalog | None,
    catalog_plugin: CatalogPlugin | None,
    warnings: list[str],
    timeout_seconds: float = 5.0,
) -> tuple[Schema | None, SourceStats | None]:
    """Attempt introspection for source joints. Returns (schema, source_stats).

    Enforces per-source timeout. Never raises.
    """
    if not catalog or not catalog_plugin:
        return None, None
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(_do_introspect, joint, catalog, catalog_plugin, warnings)
        try:
            return future.result(timeout=timeout_seconds)
        except TimeoutError:
            warnings.append(
                f"Introspection timed out for source '{joint.name}' after {timeout_seconds}s"
            )
            return None, None


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


def _compile_sql_joint(
    joint: Joint,
    engine_type: str,
    registry: PluginRegistry,
    parser: SQLParser,
    upstream_schemas: dict[str, Schema],
    errors: list[RivetError],
    warnings: list[str],
) -> tuple[LogicalPlan | None, list[ColumnLineage], str | None, str | None, Schema | None]:
    """Compile SQL parsing, lineage, and translation for a SQL joint.

    Returns (logical_plan, column_lineage, sql_translated, engine_dialect, output_schema).
    """
    logical_plan: LogicalPlan | None = None
    column_lineage: list[ColumnLineage] = []
    sql_translated: str | None = None
    output_schema: Schema | None = None
    sql_dialect = joint.dialect

    engine_plugin = registry.get_engine_plugin(engine_type) if engine_type else None
    engine_dialect = getattr(engine_plugin, "dialect", None) if engine_plugin else None

    try:
        assert joint.sql is not None, f"SQL must not be None for joint '{joint.name}'"
        ast = parser.parse(joint.sql, dialect=sql_dialect)
        ast = parser.normalize(ast)
        parser.extract_table_references(ast, dialect=sql_dialect)

        joint_upstream_schemas: dict[str, Schema] = {
            up: upstream_schemas[up] for up in joint.upstream if up in upstream_schemas
        }

        logical_plan = parser.extract_logical_plan(ast)

        inferred_schema, schema_warnings = parser.infer_schema(
            ast, joint_upstream_schemas, dialect=sql_dialect
        )
        warnings.extend(schema_warnings)
        if inferred_schema:
            output_schema = inferred_schema

        column_lineage = parser.extract_lineage(
            ast, joint_upstream_schemas, joint_name=joint.name
        )

        target_dialect = sql_dialect or engine_dialect or "duckdb"
        if sql_dialect and target_dialect != sql_dialect:
            try:
                sql_translated = parser.translate(ast, sql_dialect, target_dialect)
            except SQLParseError as e:
                errors.append(e.error)
        elif engine_dialect and engine_dialect != (sql_dialect or ""):
            try:
                source = sql_dialect or "duckdb"
                sql_translated = parser.translate(ast, source, engine_dialect)
            except SQLParseError as e:
                errors.append(e.error)

    except SQLParseError as e:
        errors.append(e.error)

    return logical_plan, column_lineage, sql_translated, engine_dialect, output_schema


def _compile_python_joint(
    joint: Joint,
    errors: list[RivetError],
) -> list[ColumnLineage]:
    """Validate and produce lineage for a PythonJoint."""
    if joint.function and not _verify_callable(joint.function):
        errors.append(
            RivetError(
                code="RVT-753",
                message=f"PythonJoint '{joint.name}' references non-importable "
                f"callable '{joint.function}'.",
                context={"joint": joint.name, "function": joint.function},
                remediation="Ensure the function path is a valid dotted path "
                "to an importable callable (e.g., 'mymodule.my_function').",
            )
        )
    return [
        ColumnLineage(
            output_column="*",
            transform="opaque",
            origins=[ColumnOrigin(joint=up, column="*") for up in joint.upstream],
            expression=None,
        )
    ]


def _compile_checks(
    joint: Joint,
    errors: list[RivetError],
) -> list[CompiledCheck]:
    """Compile assertion/audit checks for a joint."""
    checks: list[CompiledCheck] = []
    for assertion in joint.assertions:
        if assertion.phase == "audit" and joint.joint_type != "sink":
            errors.append(
                RivetError(
                    code="RVT-651",
                    message=f"Audit assertion on non-sink joint '{joint.name}' is not allowed.",
                    context={"joint": joint.name, "assertion_type": assertion.type},
                    remediation="Move audit assertions to sink joints only, "
                    "or change the phase to 'assertion'.",
                )
            )
        checks.append(
            CompiledCheck(
                type=assertion.type,
                severity=assertion.severity,
                config=assertion.config,
                phase=assertion.phase,
            )
        )
    return checks


def _compile_joint(
    joint: Joint,
    catalog_map: dict[str, Catalog],
    engine_map: dict[str, ComputeEngine],
    registry: PluginRegistry,
    default_engine: str | None,
    parser: SQLParser,
    upstream_schemas: dict[str, Schema],
    errors: list[RivetError],
    warnings: list[str],
    introspect: bool = True,
    introspect_timeout: float = 5.0,
    adapter_cache: dict[tuple[str, str], str | None] | None = None,
) -> CompiledJoint:
    """Compile a single joint: resolve engine, adapter, parse SQL, validate."""
    catalog = catalog_map.get(joint.catalog) if joint.catalog else None
    catalog_type = catalog.type if catalog else None
    catalog_plugin = registry.get_catalog_plugin(catalog_type) if catalog_type else None

    # Engine resolution
    engine_name, engine_type, resolution = _resolve_engine(
        joint, engine_map, default_engine
    )
    if not engine_name:
        errors.append(
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
        eng = engine_map.get(engine_name)
        if eng:
            engine_type = eng.engine_type

    # Adapter lookup
    adapter_name = _resolve_adapter(
        engine_type, catalog_type, engine_name, joint.name, registry, errors,
        adapter_cache=adapter_cache,
    )

    # Introspection for sources
    output_schema: Schema | None = None
    source_stats = None
    if joint.joint_type == "source" and introspect:
        output_schema, source_stats = _introspect_source(
            joint, catalog, catalog_plugin, warnings,
            timeout_seconds=introspect_timeout,
        )

    # SQL parsing
    logical_plan: LogicalPlan | None = None
    column_lineage: list[ColumnLineage] = []
    sql_translated: str | None = None
    engine_dialect: str | None = None

    if joint.joint_type == "sql" and joint.sql:
        logical_plan, column_lineage, sql_translated, engine_dialect, sql_schema = _compile_sql_joint(
            joint, engine_type, registry, parser, upstream_schemas, errors, warnings
        )
        if sql_schema:
            output_schema = sql_schema

    # Source joints: parse SQL for logical plan (limit, predicates) when present.
    # The adapter uses this to apply pushdown (e.g. LIMIT) at the storage level.
    if joint.joint_type == "source" and joint.sql:
        try:
            ast = parser.parse(joint.sql, dialect=joint.dialect)
            ast = parser.normalize(ast)
            logical_plan = parser.extract_logical_plan(ast)
        except Exception:
            pass  # Best-effort: source SQL parsing failure is non-fatal

    # PythonJoint handling
    if joint.joint_type == "python":
        column_lineage = _compile_python_joint(joint, errors)

    # Checks
    checks = _compile_checks(joint, errors)

    if output_schema:
        upstream_schemas[joint.name] = output_schema

    return CompiledJoint(
        name=joint.name,
        type=joint.joint_type,
        catalog=joint.catalog,
        catalog_type=catalog_type,
        engine=engine_name,
        engine_resolution=resolution,
        adapter=adapter_name,
        sql=joint.sql,
        sql_translated=sql_translated,
        sql_resolved=None,
        sql_dialect=joint.dialect,
        engine_dialect=engine_dialect,
        upstream=list(joint.upstream),
        eager=joint.eager,
        table=joint.table,
        write_strategy=joint.write_strategy,
        function=joint.function,
        source_file=joint.source_file,
        logical_plan=logical_plan,
        output_schema=output_schema,
        column_lineage=column_lineage,
        optimizations=[],
        checks=checks,
        fused_group_id=None,
        tags=list(joint.tags),
        description=joint.description,
        fusion_strategy_override=joint.fusion_strategy_override,
        materialization_strategy_override=joint.materialization_strategy_override,
        source_stats=source_stats,
    )


def _build_compiled_catalogs(
    compiled_joints: list[CompiledJoint],
    catalogs: list[Catalog],
) -> list[CompiledCatalog]:
    """Build compiled catalog list from used catalogs."""
    used = {cj.catalog for cj in compiled_joints if cj.catalog}
    return [
        CompiledCatalog(name=c.name, type=c.type, options=dict(c.options))
        for c in catalogs if c.name in used
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
            result.append(CompiledEngine(name=e.name, engine_type=e.engine_type, native_catalog_types=native))
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
            result.append(CompiledAdapter(
                engine_type=adapter.target_engine_type,
                catalog_type=adapter.catalog_type,
                source=adapter.source,
            ))
    return result


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
            composer = _compose_cte if resolved_fusion == "cte" else _compose_temp_view
            new_result = composer(group.joints, joint_sql)
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


def _discover_resolver(
    compiled_joints: list[CompiledJoint],
    engine_map: dict[str, ComputeEngine],
    registry: PluginRegistry,
) -> ReferenceResolver | None:
    """Find the first available reference resolver from engine plugins."""
    for cj in compiled_joints:
        eng = engine_map.get(cj.engine)
        if eng:
            plugin = registry.get_engine_plugin(eng.engine_type)
            if plugin:
                r = plugin.get_reference_resolver()
                if r is not None:
                    return r
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
    """Resolve SQL references in fused groups."""
    resolver = resolve_references or _discover_resolver(compiled_joints, engine_map, registry)

    if resolver is None:
        return fused_groups

    result = list(fused_groups)
    for idx, group in enumerate(result):
        any_resolved = False
        for jn in group.joints:
            cj = cj_map[jn]
            if cj.type not in ("sql", "sink") or not (cj.sql_translated or cj.sql):
                continue
            input_sql = cj.sql_translated or cj.sql
            assert input_sql is not None
            cat = catalog_map.get(cj.catalog) if cj.catalog else None
            compiled_cat = CompiledCatalog(name=cat.name, type=cat.type, options=dict(cat.options)) if cat else None
            try:
                resolved = resolver.resolve_references(
                    input_sql, cj, compiled_cat,
                    compiled_joints=cj_map,
                    catalog_map=catalog_map,
                    fused_group_joints=list(group.joints),
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
            composer = _compose_cte if group.fusion_strategy == "cte" else _compose_temp_view
            resolved_result = composer(group.joints, resolved_joint_sql)
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
    default_materialization_strategy: str,
) -> list[Materialization]:
    """Determine materialization points between joints."""
    VALID_MATERIALIZATION = {"arrow", "temp_table"}
    downstream_map = _build_downstream_map(cj_map)

    materializations: list[Materialization] = []
    for cj in cj_map.values():
        for ds_name in downstream_map.get(cj.name, []):
            ds = cj_map[ds_name]
            trigger: str | None = None
            detail = ""

            if cj.eager:
                trigger = "eager"
                detail = f"Joint '{cj.name}' declares eager=true"
            elif ds.type == "python":
                trigger = "python_boundary"
                detail = f"Downstream joint '{ds_name}' is a PythonJoint"
            elif bool(cj.checks):
                trigger = "assertion_boundary"
                detail = f"Joint '{cj.name}' has assertions"
            elif len(downstream_map.get(cj.name, [])) > 1:
                trigger = "multi_consumer"
                detail = f"Joint '{cj.name}' has {len(downstream_map[cj.name])} downstream consumers"
            else:
                eng_from = engine_map.get(cj.engine)
                eng_to = engine_map.get(ds.engine)
                if eng_from and eng_to and eng_from.name != eng_to.name:
                    trigger = "engine_instance_change"
                    detail = f"Engine changes from '{eng_from.name}' to '{eng_to.name}'"
                elif joint_to_group.get(cj.name) != joint_to_group.get(ds_name):
                    trigger = "capability_gap"
                    detail = f"Joints '{cj.name}' and '{ds_name}' are in different fused groups"

            if trigger:
                mat_strategy = cj.materialization_strategy_override or default_materialization_strategy
                if mat_strategy not in VALID_MATERIALIZATION:
                    mat_strategy = default_materialization_strategy
                materializations.append(
                    Materialization(
                        from_joint=cj.name, to_joint=ds_name,
                        trigger=trigger, detail=detail, strategy=mat_strategy,
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
                producer_group_id=prod_gid, consumer_group_id=cons_gid,
                producer_engine_type=prod_et, consumer_engine_type=cons_et,
                boundary_joints=joints, adapter_strategy=strategy,
            )
        )
    return boundaries


def _assign_schema_confidence(
    compiled_joints: list[CompiledJoint],
    introspected_sources: set[str],
) -> list[CompiledJoint]:
    """Assign schema_confidence to each joint based on how its schema was determined."""
    confidence_map: dict[str, str] = {}
    joint_map = {cj.name: cj for cj in compiled_joints}

    for cj in compiled_joints:
        if cj.type == "source":
            if cj.name in introspected_sources:
                confidence_map[cj.name] = "introspected"
            else:
                confidence_map[cj.name] = "none"
        elif cj.type == "python":
            confidence_map[cj.name] = "none"
        elif cj.type == "sql":
            if cj.output_schema is None:
                # Check if some upstream had schemas (partial) or none at all
                upstream_have_schema = any(
                    joint_map[u].output_schema is not None
                    for u in cj.upstream if u in joint_map
                )
                confidence_map[cj.name] = "partial" if upstream_have_schema else "none"
            else:
                all_upstream_have_schema = all(
                    joint_map[u].output_schema is not None
                    for u in cj.upstream if u in joint_map
                )
                if all_upstream_have_schema:
                    confidence_map[cj.name] = "inferred"
                else:
                    confidence_map[cj.name] = "partial"
        elif cj.type == "sink":
            # Inherit best confidence from upstream
            upstream_confidences = [
                confidence_map.get(u, "none") for u in cj.upstream
            ]
            rank = {"introspected": 3, "inferred": 2, "partial": 1, "none": 0}
            best = max(upstream_confidences, key=lambda c: rank.get(c, 0)) if upstream_confidences else "none"
            confidence_map[cj.name] = best
        else:
            confidence_map[cj.name] = "none"

    return [replace(cj, schema_confidence=confidence_map.get(cj.name, "none")) for cj in compiled_joints]


def _prune_dag(
    assembly: Assembly,
    target_sink: str | None,
    tags: list[str] | None,
    tag_mode: str,
    profile_name: str,
    errors: list[RivetError],
    warnings: list[str],
) -> Assembly | CompiledAssembly:
    """Step 1: Prune the DAG to the target subgraph.

    Returns the pruned Assembly on success, or a failed CompiledAssembly on error.
    """
    try:
        return assembly.subgraph(
            target_sink=target_sink, tags=tags, tag_mode=tag_mode
        )
    except Exception as e:
        errors.append(
            RivetError(
                code="RVT-306",
                message=str(e),
                context={"target_sink": target_sink, "tags": tags},
                remediation="Check target_sink and tags parameters.",
            )
        )
        return CompiledAssembly(
            success=False, profile_name=profile_name,
            catalogs=[], engines=[], adapters=[], joints=[],
            fused_groups=[], materializations=[], execution_order=[],
            errors=errors, warnings=warnings,
        )


def _compile_all_joints(
    pruned: Assembly,
    catalog_map: dict[str, Catalog],
    engine_map: dict[str, ComputeEngine],
    registry: PluginRegistry,
    default_engine: str | None,
    errors: list[RivetError],
    warnings: list[str],
    introspect: bool,
    introspect_timeout: float,
    catalogs: list[Catalog],
    engines: list[ComputeEngine],
) -> tuple[
    list[str],
    list[CompiledJoint],
    dict[str, CompiledJoint],
    list[CompiledCatalog],
    list[CompiledEngine],
    list[CompiledAdapter],
    int, int, int, int,
]:
    """Steps 2–3b: Topological ordering, per-joint compilation, schema confidence."""
    topo_order = pruned.topological_order()
    parser = SQLParser()
    upstream_schemas: dict[str, Schema] = {}

    introspection_attempted = 0
    introspection_succeeded = 0
    introspection_failed = 0
    introspection_skipped = 0

    # ── Submit all source introspections concurrently via a shared pool ──
    introspection_futures: dict[str, Any] = {}
    source_joints: list[str] = []
    if introspect:
        for jn in topo_order:
            joint = pruned.joints[jn]
            if joint.joint_type == "source":
                source_joints.append(jn)

    pool: ThreadPoolExecutor | None = None
    if introspect and source_joints:
        pool = ThreadPoolExecutor(max_workers=min(8, len(source_joints)))
        for jn in source_joints:
            joint = pruned.joints[jn]
            catalog = catalog_map.get(joint.catalog) if joint.catalog else None
            catalog_type = catalog.type if catalog else None
            catalog_plugin = registry.get_catalog_plugin(catalog_type) if catalog_type else None
            if catalog and catalog_plugin:
                future = pool.submit(
                    _do_introspect, joint, catalog, catalog_plugin, warnings
                )
                introspection_futures[jn] = future

    # ── Compile joints (introspection disabled inline; results attached after) ──
    introspected_sources: set[str] = set()
    compiled_joints: list[CompiledJoint] = []
    adapter_cache: dict[tuple[str, str], str | None] = {}
    try:
        for jn in topo_order:
            joint = pruned.joints[jn]
            is_source = joint.joint_type == "source"

            if is_source and not introspect:
                introspection_skipped += 1
            elif is_source and introspect:
                introspection_attempted += 1

            # Compile without inline introspection — we handle it from the pool
            cj = _compile_joint(
                joint, catalog_map, engine_map, registry,
                default_engine, parser, upstream_schemas, errors, warnings,
                introspect=False, introspect_timeout=introspect_timeout,
                adapter_cache=adapter_cache,
            )

            # Attach introspection results from the shared pool
            if is_source and introspect and jn in introspection_futures:
                try:
                    schema, stats = introspection_futures[jn].result(
                        timeout=introspect_timeout
                    )
                    if schema is not None or stats is not None:
                        cj = replace(
                            cj,
                            output_schema=schema or cj.output_schema,
                            source_stats=stats if stats is not None else cj.source_stats,
                        )
                except TimeoutError:
                    warnings.append(
                        f"Introspection timed out for source '{jn}' "
                        f"after {introspect_timeout}s"
                    )
                except Exception as exc:
                    warnings.append(
                        f"Introspection failed for source '{jn}': {exc}"
                    )

            if is_source and introspect:
                if cj.output_schema is not None:
                    introspection_succeeded += 1
                    introspected_sources.add(cj.name)
                    # Propagate schema for downstream SQL inference
                    upstream_schemas[cj.name] = cj.output_schema
                else:
                    introspection_failed += 1

            compiled_joints.append(cj)
    finally:
        if pool is not None:
            pool.shutdown(wait=False)

    compiled_joints = _assign_schema_confidence(compiled_joints, introspected_sources)
    cj_map: dict[str, CompiledJoint] = {cj.name: cj for cj in compiled_joints}

    compiled_catalogs = _build_compiled_catalogs(compiled_joints, catalogs)
    compiled_engines = _build_compiled_engines(compiled_joints, engines, registry)
    compiled_adapters = _build_compiled_adapters(compiled_joints, engine_map, registry)

    return (
        topo_order, compiled_joints, cj_map,
        compiled_catalogs, compiled_engines, compiled_adapters,
        introspection_attempted, introspection_succeeded,
        introspection_failed, introspection_skipped,
    )


def _run_optimizer_passes(
    compiled_joints: list[CompiledJoint],
    cj_map: dict[str, CompiledJoint],
    engine_map: dict[str, ComputeEngine],
    registry: PluginRegistry,
    catalog_map: dict[str, Catalog],
    default_fusion_strategy: str,
    resolve_references_fn: ReferenceResolver | None,
    errors: list[RivetError],
    warnings: list[str],
) -> tuple[list[FusedGroup], dict[str, str]]:
    """Steps 4–7: Fusion, pushdown, strategy resolution, reference resolution."""
    # Fusion pass
    fusion_joints: list[FusionJoint] = []
    for cj in compiled_joints:
        eng = engine_map.get(cj.engine)
        et = eng.engine_type if eng else ""
        fusion_joints.append(
            FusionJoint(
                name=cj.name, joint_type=cj.type, upstream=cj.upstream,
                engine=cj.engine, engine_type=et, adapter=cj.adapter,
                eager=cj.eager, has_assertions=bool(cj.checks), sql=cj.sql,
            )
        )

    fused_groups = fusion_pass(fusion_joints, fusion_strategy=default_fusion_strategy)

    joint_to_group: dict[str, str] = {}
    for group in fused_groups:
        for jn in group.joints:
            joint_to_group[jn] = group.id

    # Pushdown pass
    logical_plans: dict[str, LogicalPlan | None] = {
        cj.name: cj.logical_plan for cj in compiled_joints
    }
    catalog_types_map: dict[str, str | None] = {
        cj.name: cj.catalog_type for cj in compiled_joints
    }
    cap_map: dict[str, list[str]] = {}
    for cj in compiled_joints:
        eng = engine_map.get(cj.engine)
        et = eng.engine_type if eng else ""
        if et and cj.catalog_type:
            key = f"{et}:{cj.catalog_type}"
            if key not in cap_map:
                caps = registry.resolve_capabilities(et, cj.catalog_type)
                if caps is not None:
                    cap_map[key] = caps

    fused_groups = pushdown_pass(fused_groups, logical_plans, cap_map, catalog_types_map)

    # Cross-group predicate pushdown
    fused_groups, xgroup_results = cross_group_pushdown_pass(
        fused_groups, cj_map, cap_map, catalog_types_map,
    )
    # Attach cross-group optimization results to the relevant compiled joints
    for result in xgroup_results:
        # Extract the exit joint name from the detail string for consumer-side results,
        # or the target joint name for applied/not_applicable results.
        # Results reference joints in their detail — attach to all joints in cj_map
        # that are mentioned. For simplicity, find the joint name after "source joint '"
        # or "exit joint '" in the detail.
        for jn, cj in cj_map.items():
            if f"'{jn}'" in result.detail:
                cj_map[jn] = replace(
                    cj, optimizations=[*cj.optimizations, result]
                )
                break

    # Strategy + reference resolution
    fused_groups = _resolve_strategy(fused_groups, cj_map, default_fusion_strategy, errors)
    fused_groups = _resolve_references(
        fused_groups, cj_map, compiled_joints, engine_map, catalog_map,
        registry, resolve_references_fn, warnings,
    )

    return fused_groups, joint_to_group


def _determine_materializations_and_boundaries(
    cj_map: dict[str, CompiledJoint],
    joint_to_group: dict[str, str],
    engine_map: dict[str, ComputeEngine],
    registry: PluginRegistry,
    default_materialization_strategy: str,
    fused_groups: list[FusedGroup],
    warnings: list[str],
) -> tuple[list[Materialization], list[EngineBoundary], list[FusedGroup]]:
    """Steps 8–9b: Materializations, engine boundaries, group mat-strategy."""
    materializations = _determine_materializations(
        cj_map, joint_to_group, engine_map, default_materialization_strategy,
    )
    engine_boundaries = _detect_engine_boundaries(
        fused_groups, cj_map, joint_to_group, registry, warnings,
    )

    # Resolve materialization_strategy_name per group
    for group in fused_groups:
        resolved_mat_name: str | None = None
        for jn in group.joints:
            cj = cj_map.get(jn)
            if cj and cj.materialization_strategy_override:
                resolved_mat_name = cj.materialization_strategy_override
                break
        if resolved_mat_name is None:
            engine_plugin = registry.get_engine_plugin(group.engine_type)
            if engine_plugin:
                resolved_mat_name = engine_plugin.materialization_strategy_name
        if not resolved_mat_name:
            resolved_mat_name = "arrow"
        if resolved_mat_name != group.materialization_strategy_name:
            fused_groups = [
                replace(group, materialization_strategy_name=resolved_mat_name)
                if g.id == group.id else g
                for g in fused_groups
            ]

    return materializations, engine_boundaries, fused_groups


def _finalize_assembly(
    cj_map: dict[str, CompiledJoint],
    topo_order: list[str],
    joint_to_group: dict[str, str],
    profile_name: str,
    compiled_catalogs: list[CompiledCatalog],
    compiled_engines: list[CompiledEngine],
    compiled_adapters: list[CompiledAdapter],
    fused_groups: list[FusedGroup],
    materializations: list[Materialization],
    engine_boundaries: list[EngineBoundary],
    errors: list[RivetError],
    warnings: list[str],
    compile_duration_ms: int,
    introspection_attempted: int,
    introspection_succeeded: int,
    introspection_failed: int,
    introspection_skipped: int,
) -> CompiledAssembly:
    """Step 10: Build final joint list, execution order, and CompiledAssembly."""
    final_joints = [
        replace(cj_map[jn], fused_group_id=joint_to_group.get(jn))
        for jn in topo_order
    ]

    execution_order: list[str] = []
    seen_groups: set[str] = set()
    for jn in topo_order:
        gid = joint_to_group.get(jn)
        if gid and gid not in seen_groups:
            seen_groups.add(gid)
            execution_order.append(gid)

    # Compute parallel execution plan (wave assignment)
    parallel_execution_plan = _compute_parallel_execution_plan(fused_groups, cj_map)

    return CompiledAssembly(
        success=len(errors) == 0,
        profile_name=profile_name,
        catalogs=compiled_catalogs,
        engines=compiled_engines,
        adapters=compiled_adapters,
        joints=final_joints,
        fused_groups=fused_groups,
        materializations=materializations,
        execution_order=execution_order,
        errors=errors,
        warnings=warnings,
        engine_boundaries=engine_boundaries,
        compilation_stats=CompilationStats(
            compile_duration_ms=compile_duration_ms,
            joints_with_schema=sum(1 for j in final_joints if j.output_schema is not None),
            joints_total=len(final_joints),
            introspection_attempted=introspection_attempted,
            introspection_succeeded=introspection_succeeded,
            introspection_failed=introspection_failed,
            introspection_skipped=introspection_skipped,
        ),
        parallel_execution_plan=parallel_execution_plan,
    )

def _compute_parallel_execution_plan(
    fused_groups: list[FusedGroup],
    cj_map: dict[str, CompiledJoint],
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

    # Wavefront assignment
    remaining = set(in_degree.keys())
    waves: list[ExecutionWave] = []
    wave_number = 0

    while remaining:
        wave_number += 1
        ready = [gid for gid in remaining if in_degree[gid] == 0]
        if not ready:
            # Safety: break if no progress (shouldn't happen with a valid DAG)
            break

        # Build engine mapping for this wave
        engines: dict[str, list[str]] = {}
        for gid in ready:
            engine_name = group_by_id[gid].engine
            engines.setdefault(engine_name, []).append(gid)

        waves.append(ExecutionWave(
            wave_number=wave_number,
            groups=ready,
            engines=engines,
        ))

        # Remove ready groups and decrement downstream in-degrees
        for gid in ready:
            remaining.discard(gid)
            for ds_id in downstream.get(gid, set()):
                in_degree[ds_id] -= 1

    return waves




def compile(
    assembly: Assembly,
    catalogs: list[Catalog],
    engines: list[ComputeEngine],
    registry: PluginRegistry,
    profile_name: str = "default",
    target_sink: str | None = None,
    tags: list[str] | None = None,
    tag_mode: str = "or",
    default_fusion_strategy: str = "cte",
    default_materialization_strategy: str = "arrow",
    resolve_references: ReferenceResolver | None = None,
    default_engine: str | None = None,
    introspect: bool = True,
    introspect_timeout: float = 5.0,
) -> CompiledAssembly:
    """Compile an Assembly into an immutable CompiledAssembly.

    Pure function — performs no data operations. Collects all errors
    collectively rather than stopping at the first error.
    """
    _t0 = time.monotonic()
    errors: list[RivetError] = []
    warnings: list[str] = []

    catalog_map: dict[str, Catalog] = {c.name: c for c in catalogs}
    engine_map: dict[str, ComputeEngine] = {e.name: e for e in engines}

    # Build unified engine lookup map: merge registry engines for all names
    # referenced in the assembly that aren't already in the provided engines.
    for name in {j.engine for j in assembly.joints.values() if j.engine}:
        if name not in engine_map:
            eng = registry.get_compute_engine(name)
            if eng:
                engine_map[name] = eng
    if default_engine and default_engine not in engine_map:
        eng = registry.get_compute_engine(default_engine)
        if eng:
            engine_map[default_engine] = eng

    if default_engine is None and engines:
        default_engine = engines[0].name

    # ── Step 1: DAG pruning ───────────────────────────────────────────
    result = _prune_dag(assembly, target_sink, tags, tag_mode, profile_name, errors, warnings)
    if isinstance(result, CompiledAssembly):
        return result
    pruned = result

    # ── Steps 2–3b: Per-joint compilation ─────────────────────────────
    (
        topo_order, compiled_joints, cj_map,
        compiled_catalogs, compiled_engines, compiled_adapters,
        introspection_attempted, introspection_succeeded,
        introspection_failed, introspection_skipped,
    ) = _compile_all_joints(
        pruned, catalog_map, engine_map, registry, default_engine,
        errors, warnings, introspect, introspect_timeout, catalogs, engines,
    )

    # ── Steps 4–7: Optimizer passes ───────────────────────────────────
    fused_groups, joint_to_group = _run_optimizer_passes(
        compiled_joints, cj_map, engine_map, registry, catalog_map,
        default_fusion_strategy, resolve_references, errors, warnings,
    )

    # ── Steps 8–9b: Materializations and boundaries ──────────────────
    materializations, engine_boundaries, fused_groups = _determine_materializations_and_boundaries(
        cj_map, joint_to_group, engine_map, registry,
        default_materialization_strategy, fused_groups, warnings,
    )

    # ── Step 10: Finalize ─────────────────────────────────────────────
    return _finalize_assembly(
        cj_map, topo_order, joint_to_group, profile_name,
        compiled_catalogs, compiled_engines, compiled_adapters,
        fused_groups, materializations, engine_boundaries,
        errors, warnings,
        compile_duration_ms=int((time.monotonic() - _t0) * 1000),
        introspection_attempted=introspection_attempted,
        introspection_succeeded=introspection_succeeded,
        introspection_failed=introspection_failed,
        introspection_skipped=introspection_skipped,
    )
