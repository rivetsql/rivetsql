"""SQL analysis and schema inference helpers.

This is a Level 2 module — it may import from ``models`` and ``state``
within the compiler package, but must NOT import from ``phases/`` or
``pipeline.py``.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any

from rivet_core.compiler.models import (
    CompilationContext,
    CompiledJoint,
    SchemaConfidence,
    SourceSQLAnalysis,
)
from rivet_core.errors import RivetError, SQLParseError
from rivet_core.lineage import ColumnLineage
from rivet_core.models import Joint, Schema
from rivet_core.plugins import PluginRegistry
from rivet_core.sql_parser import LogicalPlan, Projection, SQLParser

logger = logging.getLogger("rivet_core.compiler")


def _analyze_source_sql(
    joint: Joint,
    parser: SQLParser,
) -> SourceSQLAnalysis:
    """Parse source SQL once and reuse the AST for rewrite and logical-plan extraction.

    Source SQL parsing remains best-effort: failures produce no logical plan and
    leave the original SQL untouched.
    """
    if joint.joint_type != "source" or not joint.sql:
        return SourceSQLAnalysis(joint=joint, logical_plan=None, parsed_ast=None)

    try:
        from sqlglot import exp as sg_exp

        parsed_ast = parser.parse(joint.sql, dialect=joint.dialect)

        if joint.table:
            source_table = sg_exp.to_table(joint.table)
            for table_node in parsed_ast.find_all(sg_exp.Table):
                if table_node.name in ("__self", joint.name):
                    table_node.set("this", source_table.this)
                    table_node.set("db", source_table.args.get("db"))
                    table_node.set("catalog", source_table.args.get("catalog"))
            rewritten_sql = parsed_ast.sql()
            if rewritten_sql != joint.sql:
                joint = replace(joint, sql=rewritten_sql)

        normalized_ast = parser.normalize(parsed_ast)
        logical_plan = parser.extract_logical_plan(normalized_ast)
        return SourceSQLAnalysis(joint=joint, logical_plan=logical_plan, parsed_ast=parsed_ast)
    except Exception:
        return SourceSQLAnalysis(joint=joint, logical_plan=None, parsed_ast=None)


def _validate_source_inline_transforms(
    joint_name: str,
    logical_plan: LogicalPlan | None,
    output_schema: Schema | None,
    errors: list[RivetError],
    warnings: list[str],
    sql: str | None = None,
    parsed_ast: Any | None = None,
) -> Schema | None:
    """Validate source inline transforms and compute transformed output schema.

    Checks:
    1. Single-table constraint (no joins, CTEs, subqueries) → RVT-760, RVT-761, RVT-762
    2. Column/filter reference resolution against introspected schema (warnings)
    3. Transformed output schema computation from LogicalPlan projections

    Returns the transformed output schema, or the original if no transforms apply.
    """
    if logical_plan is None:
        return output_schema

    # --- Single-table constraint checks ---

    # RVT-760: Reject JOINs (also catches comma-separated FROM which parses as implicit join)
    if logical_plan.joins:
        errors.append(
            RivetError(
                code="RVT-760",
                message=(
                    f"Source joint '{joint_name}' violates single-table constraint: "
                    f"JOINs are not allowed in source SQL."
                ),
                context={"joint": joint_name},
                remediation="Remove JOINs from the source SQL. Source joints must reference a single table.",
            )
        )

    # RVT-761 / RVT-762: Detect CTEs and subqueries via sqlglot AST.
    # The LogicalPlan's source_tables don't reliably surface these, so we
    # parse the SQL directly when available.
    if parsed_ast is not None:
        try:
            from sqlglot import exp as sg_exp

            if parsed_ast.find(sg_exp.With):
                errors.append(
                    RivetError(
                        code="RVT-761",
                        message=(
                            f"Source joint '{joint_name}' violates single-table constraint: "
                            f"CTEs are not allowed in source SQL."
                        ),
                        context={"joint": joint_name},
                        remediation="Remove CTEs from the source SQL. Source joints must reference a single table.",
                    )
                )

            if parsed_ast.find(sg_exp.Subquery):
                errors.append(
                    RivetError(
                        code="RVT-762",
                        message=(
                            f"Source joint '{joint_name}' violates single-table constraint: "
                            f"subqueries are not allowed in source SQL."
                        ),
                        context={"joint": joint_name},
                        remediation="Remove subqueries from the source SQL. Use simple WHERE conditions only.",
                    )
                )
        except Exception:
            logger.debug('Inline-transform SQL re-parse', exc_info=True)  # best-effort: see RVT logs at debug level
    elif sql:
        try:
            import sqlglot
            from sqlglot import exp as sg_exp

            parsed = sqlglot.parse_one(sql)

            # RVT-761: Reject CTEs (WITH clause)
            if parsed.find(sg_exp.With):
                errors.append(
                    RivetError(
                        code="RVT-761",
                        message=(
                            f"Source joint '{joint_name}' violates single-table constraint: "
                            f"CTEs are not allowed in source SQL."
                        ),
                        context={"joint": joint_name},
                        remediation="Remove CTEs from the source SQL. Source joints must reference a single table.",
                    )
                )

            # RVT-762: Reject subqueries
            if parsed.find(sg_exp.Subquery):
                errors.append(
                    RivetError(
                        code="RVT-762",
                        message=(
                            f"Source joint '{joint_name}' violates single-table constraint: "
                            f"subqueries are not allowed in source SQL."
                        ),
                        context={"joint": joint_name},
                        remediation="Remove subqueries from the source SQL. Use simple WHERE conditions only.",
                    )
                )
        except Exception:
            logger.debug("Column-warning SQL re-parse failed; skipping warnings", exc_info=True)  # noqa: BLE001

    # --- Column reference warnings (only when introspected schema is available) ---

    _warn_unresolved_column_refs(joint_name, logical_plan, output_schema, warnings)

    # --- Compute transformed output schema ---

    transformed_schema = _compute_source_transform_schema(
        joint_name, logical_plan.projections, output_schema, warnings
    )
    return transformed_schema if transformed_schema is not None else output_schema


def _compute_source_transform_schema(
    joint_name: str,
    projections: list[Projection],
    catalog_schema: Schema | None,
    warnings: list[str],
) -> Schema | None:
    """Compute the output schema for a source joint with inline transform projections.

    For SELECT * (no explicit projections), returns None (use catalog schema as-is).
    For explicit projections, builds a schema from the projected columns using the
    catalog schema for type information.
    """
    from rivet_core.models import Column

    if not projections:
        return None

    # Check for SELECT * (single Star projection)
    if len(projections) == 1 and projections[0].expression == "*":
        return None

    if catalog_schema is None:
        # No introspected schema — can't compute types, but can compute column names
        columns: list[Column] = []
        for proj in projections:
            col_name = proj.alias if proj.alias else proj.expression
            columns.append(Column(name=col_name, type="large_binary", nullable=True))
            warnings.append(
                f"Source joint '{joint_name}' column '{col_name}': "
                f"cannot infer output type for expression '{proj.expression}' "
                f"(no catalog schema available)."
            )
        return Schema(columns=columns)

    # Build lookup from catalog schema
    catalog_col_map = {col.name.lower(): col for col in catalog_schema.columns}

    columns = []
    for proj in projections:
        col_name = proj.alias if proj.alias else proj.expression
        col_type = _infer_projection_type(proj, catalog_col_map, joint_name, warnings)
        columns.append(Column(name=col_name, type=col_type, nullable=True))

    return Schema(columns=columns)


def _infer_projection_type(
    proj: Projection,
    catalog_col_map: dict[str, Any],
    joint_name: str,
    warnings: list[str],
) -> str:
    """Infer the output type of a single projection expression.

    Returns the Arrow type string. Falls back to 'large_binary' with a warning
    when the type cannot be determined.
    """
    expr = proj.expression
    alias = proj.alias

    # Simple column reference (no alias, or alias with expression = column name)
    if not proj.alias and len(proj.source_columns) == 1:
        col_name = proj.source_columns[0].rsplit(".", 1)[-1].lower()
        cat_col = catalog_col_map.get(col_name)
        if cat_col is not None:
            return str(cat_col.type)
    elif (
        proj.alias
        and len(proj.source_columns) == 1
        and proj.expression.lower() == proj.source_columns[0].rsplit(".", 1)[-1].lower()
    ):
        # Simple rename: alias for a single column reference
        col_name = proj.source_columns[0].rsplit(".", 1)[-1].lower()
        cat_col = catalog_col_map.get(col_name)
        if cat_col is not None:
            return str(cat_col.type)

    # CAST expression: try to extract target type from the expression string
    expr_upper = expr.strip().upper()
    if expr_upper.startswith("CAST("):
        # Parse "CAST(x AS TYPE)" to extract TYPE
        try:
            import sqlglot
            from sqlglot import exp as sg_exp

            parsed = sqlglot.parse_one(expr)
            if isinstance(parsed, sg_exp.Cast):
                from rivet_core.sql_parser import SQLParser

                return SQLParser._normalize_sqlglot_type(parsed.to)
        except Exception:
            logger.debug('sqlglot type inference fallback', exc_info=True)  # best-effort: see RVT logs at debug level

    # Cannot determine type — emit warning
    col_label = alias or expr
    warnings.append(
        f"Source joint '{joint_name}' column '{col_label}': "
        f"cannot infer output type for expression '{expr}'."
    )
    return "large_binary"


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

        column_lineage = parser.extract_lineage(ast, joint_upstream_schemas, joint_name=joint.name)

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


def _compile_sql_like_joint(
    joint: Joint,
    engine_type: str,
    ctx: CompilationContext,
) -> tuple[LogicalPlan | None, list[ColumnLineage], str | None, str | None, Schema | None]:
    """Compile SQL-bearing SQL, sink, and checkpoint joints."""
    if joint.joint_type not in ("sql", "sink", "checkpoint") or not joint.sql:
        return None, [], None, None, None
    return _compile_sql_joint(
        joint,
        engine_type,
        ctx.registry,
        ctx.parser,
        ctx.upstream_schemas,
        ctx.errors,
        ctx.warnings,
    )


def _warn_unresolved_column_refs(
    joint_name: str,
    logical_plan: LogicalPlan,
    output_schema: Schema | None,
    warnings: list[str],
) -> None:
    """Emit warnings for column references not found in the introspected catalog schema."""
    if output_schema is None:
        return

    known_columns = {col.name.lower() for col in output_schema.columns}

    # Warn about filter references to unknown columns
    for pred in logical_plan.predicates:
        for col_ref in pred.columns:
            col_name = col_ref.rsplit(".", 1)[-1].lower()
            if col_name not in known_columns:
                warnings.append(
                    f"Source joint '{joint_name}' filter references column "
                    f"'{col_ref}' not found in catalog schema."
                )

    # Warn about column expression references to unknown columns
    for proj in logical_plan.projections:
        for col_ref in proj.source_columns:
            col_name = col_ref.rsplit(".", 1)[-1].lower()
            if col_name not in known_columns:
                alias_label = proj.alias or proj.expression
                warnings.append(
                    f"Source joint '{joint_name}' column '{alias_label}' "
                    f"expression references '{col_ref}' not found in catalog schema."
                )


def _assign_schema_confidence(
    compiled_joints: list[CompiledJoint],
    introspected_sources: set[str],
) -> list[CompiledJoint]:
    """Assign schema_confidence to each joint based on how its schema was determined."""
    confidence_map: dict[str, SchemaConfidence] = {}
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
                    joint_map[u].output_schema is not None for u in cj.upstream if u in joint_map
                )
                confidence_map[cj.name] = "partial" if upstream_have_schema else "none"
            else:
                all_upstream_have_schema = all(
                    joint_map[u].output_schema is not None for u in cj.upstream if u in joint_map
                )
                if all_upstream_have_schema:
                    confidence_map[cj.name] = "inferred"
                else:
                    confidence_map[cj.name] = "partial"
        elif cj.type == "sink":
            # Handle case where sink has no schema
            if cj.output_schema is None:
                # Check if schema merging failed due to conflicts
                upstream_schemas = [
                    joint_map[u].output_schema for u in cj.upstream if u in joint_map
                ]
                non_none_schemas = [s for s in upstream_schemas if s is not None]

                if len(non_none_schemas) > 1:
                    # Multiple schemas exist but sink has None - merging failed, assign "partial"
                    confidence_map[cj.name] = "partial"
                elif len(non_none_schemas) == 1 and len(upstream_schemas) > 1:
                    # One upstream has schema, others have None - assign "partial"
                    confidence_map[cj.name] = "partial"
                else:
                    # All upstreams have None or no upstreams - assign "none"
                    confidence_map[cj.name] = "none"
            else:
                # Sink has a schema - inherit best confidence from upstream
                upstream_confidences = [confidence_map.get(u, "none") for u in cj.upstream]
                rank = {"introspected": 3, "inferred": 2, "partial": 1, "none": 0}
                best: SchemaConfidence = (
                    max(upstream_confidences, key=lambda c: rank.get(c, 0))
                    if upstream_confidences
                    else "none"
                )
                confidence_map[cj.name] = best
        else:
            confidence_map[cj.name] = "none"

    return [
        replace(cj, schema_confidence=confidence_map.get(cj.name, "none")) for cj in compiled_joints
    ]


def _infer_sink_schemas(
    compiled_joints: list[CompiledJoint],
    warnings: list[str],
) -> list[CompiledJoint]:
    """Infer output schemas for sink joints based on upstream schemas.

    For each sink:
    - Single upstream: copy upstream schema
    - Multiple upstreams with identical schemas: use that schema
    - Multiple upstreams with differing schemas: set to None, emit warning
    - Any upstream with None schema: set to None

    Args:
        compiled_joints: List of compiled joints to process
        warnings: List to append warning messages to

    Returns:
        Updated list of CompiledJoints with sink schemas populated
    """
    # Build joint_map for O(1) lookups
    joint_map: dict[str, CompiledJoint] = {cj.name: cj for cj in compiled_joints}

    result: list[CompiledJoint] = []

    for cj in compiled_joints:
        if cj.type not in ("sink", "checkpoint"):
            result.append(cj)
            continue

        # If the sink already has a SQL-inferred schema, keep it
        if cj.output_schema is not None:
            result.append(cj)
            continue

        # Collect upstream schemas
        upstream_schemas: list[Schema | None] = []
        for upstream_name in cj.upstream:
            if upstream_name in joint_map:
                upstream_schemas.append(joint_map[upstream_name].output_schema)

        # Determine sink schema based on upstream schemas
        inferred_schema: Schema | None = None

        if not upstream_schemas:
            # No upstream joints (shouldn't happen for valid sinks, but handle gracefully)
            inferred_schema = None
        elif len(upstream_schemas) == 1:
            # Single upstream: copy schema (even if None)
            inferred_schema = upstream_schemas[0]
        else:
            # Multiple upstreams: merge if identical, None if conflicting
            if any(s is None for s in upstream_schemas):
                # Any upstream has no schema
                inferred_schema = None
            elif _schemas_identical(upstream_schemas):
                # All schemas are identical
                inferred_schema = upstream_schemas[0]
            else:
                # Schemas differ - emit warning
                inferred_schema = None
                upstream_names = ", ".join(f"'{u}'" for u in cj.upstream)
                warnings.append(
                    f"Sink '{cj.name}' has conflicting upstream schemas from joints: {upstream_names}. "
                    f"Schema inference failed. Sink output_schema set to None."
                )

        # Update the compiled joint with inferred schema
        result.append(replace(cj, output_schema=inferred_schema))

    return result


def _schemas_identical(schemas: list[Schema | None]) -> bool:
    """Check if all schemas in the list are identical.

    Returns False if any schema is None or if schemas differ in columns,
    types, nullability, or order.

    Args:
        schemas: List of Schema objects to compare

    Returns:
        True if all schemas are non-None and identical, False otherwise
    """
    if not schemas:
        return True

    # If any schema is None, they're not identical
    if any(s is None for s in schemas):
        return False

    # All schemas are non-None at this point
    first_schema = schemas[0]
    assert first_schema is not None  # Type narrowing

    for schema in schemas[1:]:
        assert schema is not None  # Type narrowing

        # Check if column count differs
        if len(first_schema.columns) != len(schema.columns):
            return False

        # Check each column (order matters)
        for col1, col2 in zip(first_schema.columns, schema.columns):
            if col1.name != col2.name:
                return False
            if col1.type != col2.type:
                return False
            if col1.nullable != col2.nullable:
                return False

    return True
