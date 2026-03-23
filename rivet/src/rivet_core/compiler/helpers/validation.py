"""General validation and joint compilation helpers.

This is a Level 2 module — it may import from ``models`` and ``state``
within the compiler package, but must NOT import from ``phases/`` or
``pipeline.py``.
"""

from __future__ import annotations

import importlib
import logging
import sys
from pathlib import Path

from rivet_core.checks import CompiledCheck
from rivet_core.compiler.helpers.resolution import (
    _introspect_source,
    _resolve_joint_metadata,
    _resolve_table_map_name,
)
from rivet_core.compiler.helpers.sql_helpers import (
    _analyze_source_sql,
    _compile_sql_like_joint,
    _validate_source_inline_transforms,
)
from rivet_core.compiler.models import (
    CompilationContext,
    CompiledJoint,
    ResolvedJointMetadata,
    SourceSQLAnalysis,
    SourceStats,
)
from rivet_core.errors import RivetError
from rivet_core.lineage import ColumnLineage, ColumnOrigin
from rivet_core.models import Catalog, ComputeEngine, Joint, Schema
from rivet_core.plugins import PluginRegistry
from rivet_core.sql_parser import LogicalPlan, SQLParser

logger = logging.getLogger("rivet_core.compiler")


def _verify_callable(function_path: str, project_root: Path | None = None) -> bool:
    """Check if a colon-separated function path (module:func) is importable.

    When *project_root* is provided it is temporarily prepended to
    ``sys.path`` so that project-local modules (e.g. ``joints/``) are
    importable without the user having to set ``PYTHONPATH``.
    """
    parts = function_path.rsplit(":", 1)
    if len(parts) != 2:
        return False
    module_path, func_name = parts
    root_str = str(project_root) if project_root else None
    added = False
    try:
        if root_str and root_str not in sys.path:
            sys.path.insert(0, root_str)
            added = True
        mod = importlib.import_module(module_path)
        return callable(getattr(mod, func_name, None))
    except Exception:
        return False
    finally:
        if added and root_str:
            try:
                sys.path.remove(root_str)
            except ValueError:
                pass


def _validate_checkpoint_joint(joint: Joint, errors: list[RivetError]) -> None:
    """Validate checkpoint-specific required fields."""
    if joint.joint_type == "checkpoint":
        if not joint.catalog:
            errors.append(
                RivetError(
                    code="RVT-401",
                    message=f"Checkpoint joint '{joint.name}' requires a 'catalog' field.",
                    context={"joint": joint.name},
                    remediation="Add a 'catalog' field to the joint configuration.",
                )
            )
        if not joint.table:
            errors.append(
                RivetError(
                    code="RVT-401",
                    message=f"Checkpoint joint '{joint.name}' requires a 'table' field.",
                    context={"joint": joint.name},
                    remediation="Add a 'table' field to the joint configuration.",
                )
            )


def _default_write_strategy(joint: Joint) -> str | None:
    """Resolve the default write strategy for sink-like joints."""
    if joint.joint_type in ("sink", "checkpoint"):
        return joint.write_strategy or "replace"
    return joint.write_strategy


def _compile_python_joint(
    joint: Joint,
    errors: list[RivetError],
    project_root: Path | None = None,
) -> list[ColumnLineage]:
    """Validate and produce lineage for a PythonJoint."""
    if joint.function and not _verify_callable(joint.function, project_root):
        errors.append(
            RivetError(
                code="RVT-753",
                message=f"PythonJoint '{joint.name}' references non-importable "
                f"callable '{joint.function}'.",
                context={"joint": joint.name, "function": joint.function},
                remediation="Ensure the function path is importable and uses the "
                "form 'module:callable' (e.g., 'mymodule:my_function').",
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


def _compile_joint_with_context(
    joint: Joint,
    ctx: CompilationContext,
    *,
    introspect: bool = True,
    introspect_timeout: float = 5.0,
) -> CompiledJoint:
    """Compile a single joint using a shared compilation context."""
    resolved = _resolve_joint_metadata(joint, ctx)

    logical_plan: LogicalPlan | None = None
    column_lineage: list[ColumnLineage] = []
    sql_translated: str | None = None
    engine_dialect: str | None = None
    output_schema: Schema | None = None
    source_stats: SourceStats | None = None

    # Resolve table_map aliases early so that __self substitution in source
    # joints and all downstream adapters see the mapped (physical) name.
    if joint.catalog:
        resolved_table = _resolve_table_map_name(
            joint,
            ctx.catalog_map.get(joint.catalog),
            ctx.catalog_map,
        )
        if resolved_table != (joint.table or joint.path or joint.name):
            joint.table = resolved_table

    if joint.joint_type == "source":
        joint, logical_plan, output_schema, source_stats = _compile_source_joint(
            joint,
            resolved,
            ctx,
            introspect=introspect,
            introspect_timeout=introspect_timeout,
        )
    else:
        logical_plan, column_lineage, sql_translated, engine_dialect, sql_schema = (
            _compile_sql_like_joint(joint, resolved.engine_type, ctx)
        )
        if sql_schema:
            output_schema = sql_schema

    if joint.joint_type == "python":
        column_lineage = _compile_python_joint(joint, ctx.errors, ctx.project_root)

    write_strategy = _default_write_strategy(joint)
    _validate_checkpoint_joint(joint, ctx.errors)
    checks = _compile_checks(joint, ctx.errors)

    if output_schema:
        ctx.upstream_schemas[joint.name] = output_schema

    return CompiledJoint(
        name=joint.name,
        type=joint.joint_type,
        catalog=joint.catalog,
        catalog_type=resolved.catalog_type,
        engine=resolved.engine_name,
        engine_resolution=resolved.resolution,
        adapter=resolved.adapter_name,
        sql=joint.sql,
        sql_translated=sql_translated,
        sql_resolved=None,
        sql_dialect=joint.dialect,
        engine_dialect=engine_dialect,
        upstream=list(joint.upstream),
        eager=joint.eager,
        table=joint.table,
        write_strategy=write_strategy,
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
    project_root: Path | None = None,
) -> CompiledJoint:
    """Backward-compatible wrapper for compiling a single joint."""
    ctx = CompilationContext(
        catalog_map=catalog_map,
        engine_map=engine_map,
        registry=registry,
        default_engine=default_engine,
        parser=parser,
        upstream_schemas=upstream_schemas,
        errors=errors,
        warnings=warnings,
        adapter_cache=adapter_cache if adapter_cache is not None else {},
        project_root=project_root,
    )
    return _compile_joint_with_context(
        joint,
        ctx,
        introspect=introspect,
        introspect_timeout=introspect_timeout,
    )


def _compile_source_joint(
    joint: Joint,
    resolved: ResolvedJointMetadata,
    ctx: CompilationContext,
    *,
    introspect: bool,
    introspect_timeout: float,
) -> tuple[Joint, LogicalPlan | None, Schema | None, SourceStats | None]:
    """Compile source-specific SQL analysis, introspection, and validation."""
    output_schema: Schema | None = None
    source_stats: SourceStats | None = None
    if joint.joint_type == "source" and introspect:
        output_schema, source_stats = _introspect_source(
            joint,
            resolved.catalog,
            resolved.catalog_plugin,
            ctx.warnings,
            timeout_seconds=introspect_timeout,
        )

    source_sql_analysis: SourceSQLAnalysis | None = None
    if joint.joint_type == "source" and joint.sql:
        source_sql_analysis = _analyze_source_sql(joint, ctx.parser)
        joint = source_sql_analysis.joint
        logical_plan = source_sql_analysis.logical_plan
    else:
        logical_plan = None

    output_schema = _validate_source_inline_transforms(
        joint.name,
        logical_plan,
        output_schema,
        ctx.errors,
        ctx.warnings,
        sql=joint.sql,
        parsed_ast=(source_sql_analysis.parsed_ast if source_sql_analysis else None),
    )
    return joint, logical_plan, output_schema, source_stats
