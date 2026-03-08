"""SQL parsing, validation, logical plan extraction, and AST normalization."""

from __future__ import annotations

from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as exp
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.simplify import simplify

from rivet_core.errors import RivetError, SQLParseError
from rivet_core.lineage import ColumnLineage, ColumnOrigin
from rivet_core.models import Column, Schema


@dataclass(frozen=True)
class TableReference:
    name: str
    schema: str | None
    alias: str | None
    source_type: str  # "from", "join", "subquery"


@dataclass(frozen=True)
class Projection:
    expression: str
    alias: str | None
    source_columns: list[str]


@dataclass(frozen=True)
class Predicate:
    expression: str
    columns: list[str]
    location: str  # "where" or "having"


@dataclass(frozen=True)
class Join:
    type: str  # "inner", "left", "right", "full", "cross"
    left_table: str
    right_table: str
    condition: str | None
    columns: list[str]


@dataclass(frozen=True)
class Aggregation:
    group_by: list[str]
    functions: list[str]


@dataclass(frozen=True)
class Limit:
    count: int | None
    offset: int | None


@dataclass(frozen=True)
class Ordering:
    columns: list[tuple[str, str]]  # (column, "asc"/"desc")


@dataclass(frozen=True)
class LogicalPlan:
    projections: list[Projection]
    predicates: list[Predicate]
    joins: list[Join]
    aggregations: Aggregation | None
    limit: Limit | None
    ordering: Ordering | None
    distinct: bool
    source_tables: list[TableReference]


@dataclass(frozen=True)
class ParsedSQL:
    original_sql: str
    parse_dialect: str
    ast: exp.Expression  # sqlglot AST (normalized)
    table_references: list[TableReference]
    logical_plan: LogicalPlan
    output_schema: Schema | None
    column_lineage: list[ColumnLineage]
    translated_sql: str | None  # None if no translation needed
    engine_dialect: str | None
    warnings: list[str]


# DDL/DML types that must be rejected
_NON_SELECT_TYPES = (
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Command,
)

# sqlglot DataType.Type → Arrow type name
_SQLGLOT_TO_ARROW: dict[exp.DataType.Type, str] = {
    exp.DataType.Type.INT: "int32",
    exp.DataType.Type.BIGINT: "int64",
    exp.DataType.Type.SMALLINT: "int16",
    exp.DataType.Type.FLOAT: "float32",
    exp.DataType.Type.DOUBLE: "float64",
    exp.DataType.Type.VARCHAR: "utf8",
    exp.DataType.Type.TEXT: "utf8",
    exp.DataType.Type.BOOLEAN: "bool",
    exp.DataType.Type.DATE: "date32",
    exp.DataType.Type.TIMESTAMP: "timestamp[us]",
    exp.DataType.Type.DECIMAL: "decimal128",
}

# Arrow type name → sqlglot type string (for upstream schema conversion)
_ARROW_TO_SQLGLOT: dict[str, str] = {
    "int16": "SMALLINT",
    "int32": "INT",
    "int64": "BIGINT",
    "float32": "FLOAT",
    "float64": "DOUBLE",
    "utf8": "TEXT",
    "bool": "BOOLEAN",
    "date32": "DATE",
    "timestamp[us]": "TIMESTAMP",
    "large_binary": "VARBINARY",
}


class SQLParser:
    """SQL parser using sqlglot for validation and table reference extraction."""

    def parse(self, sql: str, dialect: str | None = None) -> exp.Expression:
        """Parse SQL string into a sqlglot AST.

        Validates it is a single read-only SELECT (CTEs allowed).
        Raises SQLParseError with RVT-701 on parse failure, RVT-702 on invalid statement type.
        """
        try:
            statements = sqlglot.parse(sql, dialect=dialect)
        except (sqlglot.errors.ParseError, sqlglot.errors.TokenError) as e:
            raise SQLParseError(
                RivetError(
                    code="RVT-701",
                    message=f"SQL parse failure: {e}",
                    context={"sql": sql, "dialect": dialect or "generic"},
                    remediation="Check SQL syntax and ensure it is valid for the target dialect.",
                    original_sql=sql,
                    dialect=dialect,
                    failing_construct=str(e),
                )
            ) from e

        # Filter out None entries (empty statements from trailing semicolons)
        statements = [s for s in statements if s is not None]

        if len(statements) == 0:
            raise SQLParseError(
                RivetError(
                    code="RVT-701",
                    message="Empty SQL statement.",
                    context={"sql": sql, "dialect": dialect or "generic"},
                    remediation="Provide a valid SELECT statement.",
                    original_sql=sql,
                    dialect=dialect,
                )
            )

        if len(statements) > 1:
            raise SQLParseError(
                RivetError(
                    code="RVT-702",
                    message="Multiple SQL statements are not allowed. Only a single SELECT is permitted.",
                    context={"sql": sql, "dialect": dialect or "generic"},
                    remediation="Provide exactly one SELECT statement. Use CTEs instead of multiple statements.",
                    original_sql=sql,
                    dialect=dialect,
                    failing_construct="multi-statement",
                )
            )

        ast = statements[0]
        if ast is None:
            raise SQLParseError(
                RivetError(
                    code="RVT-301",
                    message="SQL parsing produced a None AST.",
                    context={"sql": sql[:200]},
                    remediation="Check the SQL syntax.",
                )
            )
        self._validate_select(ast, sql, dialect)
        return ast

    def _validate_select(
        self, ast: exp.Expression, sql: str, dialect: str | None
    ) -> None:
        """Validate that the AST is a single read-only SELECT (CTEs allowed)."""
        if isinstance(ast, _NON_SELECT_TYPES):
            kind = type(ast).__name__.upper()
            raise SQLParseError(
                RivetError(
                    code="RVT-702",
                    message=f"{kind} statements are not allowed. Only SELECT statements are permitted.",
                    context={"sql": sql, "dialect": dialect or "generic"},
                    remediation="Rewrite as a SELECT statement. DDL and DML are not supported in SQL joints.",
                    original_sql=sql,
                    dialect=dialect,
                    failing_construct=kind,
                )
            )

        if not isinstance(ast, exp.Select) and not isinstance(ast, exp.Union):
            # Check if it's a subqueryable (e.g. CTE wrapping a select)
            select = ast.find(exp.Select)
            if select is None:
                raise SQLParseError(
                    RivetError(
                        code="RVT-702",
                        message="Only SELECT statements are permitted in SQL joints.",
                        context={"sql": sql, "dialect": dialect or "generic"},
                        remediation="Provide a SELECT statement, optionally with CTEs.",
                        original_sql=sql,
                        dialect=dialect,
                        failing_construct=type(ast).__name__,
                    )
                )

    def extract_table_references(
        self, ast: exp.Expression, dialect: str | None = None
    ) -> list[TableReference]:
        """Extract table references from FROM, JOIN, and subqueries, excluding CTE names."""
        cte_names: set[str] = set()
        for cte in ast.find_all(exp.CTE):
            alias_node = cte.args.get("alias")
            if alias_node:
                cte_names.add(alias_node.name)

        refs: list[TableReference] = []
        seen: set[tuple[str, str | None, str]] = set()

        for table in ast.find_all(exp.Table):
            name = table.name
            if not name:
                continue
            if name in cte_names:
                continue

            schema_name = table.db or None
            alias = table.alias or None

            # Determine source_type by walking up the tree
            source_type = self._classify_table_source(table)

            key = (name, schema_name, source_type)
            if key not in seen:
                seen.add(key)
                refs.append(
                    TableReference(
                        name=name,
                        schema=schema_name,
                        alias=alias,
                        source_type=source_type,
                    )
                )

        return refs

    def _classify_table_source(self, table: exp.Table) -> str:
        """Classify whether a table reference comes from FROM, JOIN, or subquery."""
        parent = table.parent
        while parent is not None:
            if isinstance(parent, exp.Join):
                return "join"
            if isinstance(parent, exp.From):
                return "from"
            if isinstance(parent, exp.Subquery):
                return "subquery"
            parent = parent.parent
        return "from"

    def validate(self, ast: exp.Expression) -> list[RivetError]:
        """Validate SQL AST for rivet-specific rules.

        Returns a list of validation errors (empty if valid).
        """
        errors: list[RivetError] = []

        # Check for DDL/DML nested inside (e.g. INSERT ... SELECT)
        for node in ast.walk():
            if isinstance(node, _NON_SELECT_TYPES):
                errors.append(
                    RivetError(
                        code="RVT-702",
                        message=f"{type(node).__name__.upper()} is not allowed inside a SQL joint.",
                        context={},
                        remediation="Remove DDL/DML statements. Only SELECT is permitted.",
                        failing_construct=type(node).__name__.upper(),
                    )
                )

        return errors

    def normalize(self, ast: exp.Expression) -> exp.Expression:
        """Normalize the AST.

        Applies: constant-left to column-left, AND ordering, double negation
        elimination, boolean simplification, COALESCE simplification, alias resolution.
        """
        # Deep copy to avoid mutating the original
        ast = ast.copy()

        # Use sqlglot's simplify for double negation elimination, boolean simplification,
        # and constant-left to column-left rewriting
        ast = simplify(ast)

        # COALESCE simplification: COALESCE(a, a) -> a, remove NULL args
        for node in list(ast.walk()):
            if isinstance(node, exp.Coalesce):
                all_args = [node.this] + list(node.expressions) if node.this else list(node.expressions)
                # Remove NULL literals
                filtered = [a for a in all_args if not isinstance(a, exp.Null)]
                if not filtered:
                    continue
                # If all remaining args are identical SQL, simplify to first
                if len(set(a.sql() for a in filtered)) == 1:
                    node.replace(filtered[0].copy())
                elif len(filtered) < len(all_args):
                    # Rebuild with NULLs removed
                    node.set("this", filtered[0].copy())
                    node.set("expressions", [a.copy() for a in filtered[1:]])

        # AND ordering: sort AND conjuncts alphabetically for determinism
        for node in list(ast.walk()):
            if isinstance(node, exp.And):
                conjuncts = self._flatten_and(node)
                conjuncts.sort(key=lambda c: c.sql())
                rebuilt = self._build_and(conjuncts)
                node.replace(rebuilt)

        return ast

    @staticmethod
    def _flatten_and(node: exp.Expression) -> list[exp.Expression]:
        """Flatten nested AND expressions into a list of conjuncts."""
        if isinstance(node, exp.And):
            return SQLParser._flatten_and(node.left) + SQLParser._flatten_and(node.right)
        return [node]

    @staticmethod
    def _build_and(conjuncts: list[exp.Expression]) -> exp.Expression:
        """Build a left-associative AND tree from a list of conjuncts."""
        result = conjuncts[0].copy()
        for c in conjuncts[1:]:
            result = exp.And(this=result, expression=c.copy())
        return result

    def extract_logical_plan(
        self, ast: exp.Expression, dialect: str | None = None
    ) -> LogicalPlan:
        """Extract a LogicalPlan from a parsed SQL AST."""
        source_tables = self.extract_table_references(ast, dialect)

        # Find the outermost SELECT (may be wrapped in a CTE)
        select = ast if isinstance(ast, exp.Select) else ast.find(exp.Select)

        projections = self._extract_projections(select) if select else []
        predicates = self._extract_predicates(ast)
        joins = self._extract_joins(ast)
        aggregations = self._extract_aggregations(ast)
        limit = self._extract_limit(ast)
        ordering = self._extract_ordering(ast)
        distinct = bool(select.args.get("distinct")) if select else False

        return LogicalPlan(
            projections=projections,
            predicates=predicates,
            joins=joins,
            aggregations=aggregations,
            limit=limit,
            ordering=ordering,
            distinct=distinct,
            source_tables=source_tables,
        )

    @staticmethod
    def _extract_projections(select: exp.Select) -> list[Projection]:
        """Extract projection list from SELECT expressions."""
        projections: list[Projection] = []
        for expr in select.expressions:
            alias: str | None = None
            inner = expr
            if isinstance(expr, exp.Alias):
                alias = expr.alias
                inner = expr.this
            source_cols = [c.sql() for c in inner.find_all(exp.Column)]
            projections.append(
                Projection(expression=inner.sql(), alias=alias, source_columns=source_cols)
            )
        return projections

    @staticmethod
    def _extract_predicates(ast: exp.Expression) -> list[Predicate]:
        """Extract predicates from WHERE and HAVING clauses."""
        predicates: list[Predicate] = []
        where = ast.find(exp.Where)
        if where:
            for conjunct in SQLParser._flatten_and(where.this):
                cols = [c.sql() for c in conjunct.find_all(exp.Column)]
                predicates.append(
                    Predicate(expression=conjunct.sql(), columns=cols, location="where")
                )
        having = ast.find(exp.Having)
        if having:
            for conjunct in SQLParser._flatten_and(having.this):
                cols = [c.sql() for c in conjunct.find_all(exp.Column)]
                predicates.append(
                    Predicate(expression=conjunct.sql(), columns=cols, location="having")
                )
        return predicates

    @staticmethod
    def _resolve_join_type(join: exp.Join) -> str:
        """Resolve the join type string from a sqlglot Join node."""
        kind = (join.kind or "").lower()
        side = (join.side or "").lower()
        if kind == "cross":
            return "cross"
        if side in ("left", "right", "full"):
            return side
        if kind == "inner" or (not side and not kind):
            return "inner"
        return side or kind or "inner"

    def _extract_joins(self, ast: exp.Expression) -> list[Join]:
        """Extract join information from the AST."""
        joins: list[Join] = []
        # Get the FROM table as the left side for the first join
        from_clause = ast.find(exp.From)
        left_table = ""
        if from_clause:
            ft = from_clause.find(exp.Table)
            if ft:
                left_table = ft.alias or ft.name

        for j in ast.find_all(exp.Join):
            right_t = j.find(exp.Table)
            right_table = (right_t.alias or right_t.name) if right_t else ""
            on_cond = j.args.get("on")
            condition = on_cond.sql() if on_cond else None
            cols = [c.sql() for c in on_cond.find_all(exp.Column)] if on_cond else []
            jtype = self._resolve_join_type(j)
            joins.append(
                Join(
                    type=jtype,
                    left_table=left_table,
                    right_table=right_table,
                    condition=condition,
                    columns=cols,
                )
            )
            # For chained joins, the right table becomes the left for the next
            left_table = right_table

        return joins

    @staticmethod
    def _extract_aggregations(ast: exp.Expression) -> Aggregation | None:
        """Extract aggregation info (GROUP BY and aggregate functions)."""
        group = ast.find(exp.Group)
        agg_funcs = [f.sql() for f in ast.find_all(exp.AggFunc)]
        if not group and not agg_funcs:
            return None
        group_by = [e.sql() for e in group.expressions] if group else []
        return Aggregation(group_by=group_by, functions=agg_funcs)

    @staticmethod
    def _extract_limit(ast: exp.Expression) -> Limit | None:
        """Extract LIMIT and OFFSET."""
        limit_node = ast.find(exp.Limit)
        offset_node = ast.find(exp.Offset)
        if not limit_node and not offset_node:
            return None
        count: int | None = None
        offset: int | None = None
        if limit_node and limit_node.expression:
            try:
                count = int(limit_node.expression.this)
            except (ValueError, AttributeError):
                count = None
        if offset_node and offset_node.expression:
            try:
                offset = int(offset_node.expression.this)
            except (ValueError, AttributeError):
                offset = None
        return Limit(count=count, offset=offset)

    @staticmethod
    def _extract_ordering(ast: exp.Expression) -> Ordering | None:
        """Extract ORDER BY columns and directions."""
        order = ast.find(exp.Order)
        if not order:
            return None
        columns: list[tuple[str, str]] = []
        for o in order.expressions:
            if isinstance(o, exp.Ordered):
                desc = o.args.get("desc", False)
                columns.append((o.this.sql(), "desc" if desc else "asc"))
            else:
                columns.append((o.sql(), "asc"))
        return Ordering(columns=columns)

    def translate(
        self, ast: exp.Expression, source_dialect: str, target_dialect: str
    ) -> str:
        """Translate SQL from source_dialect to target_dialect using sqlglot transpile.

        Raises SQLParseError with RVT-703 on translation failure.
        Supports any dialect sqlglot supports: duckdb, postgres, mysql, bigquery,
        snowflake, sqlite, trino, spark, hive, tsql, etc.
        """
        sql = ast.sql(dialect=source_dialect)
        try:
            results = sqlglot.transpile(sql, read=source_dialect, write=target_dialect)
        except Exception as e:
            raise SQLParseError(
                RivetError(
                    code="RVT-703",
                    message=f"Dialect translation failed from '{source_dialect}' to '{target_dialect}': {e}",
                    context={
                        "source_dialect": source_dialect,
                        "target_dialect": target_dialect,
                        "sql": sql,
                    },
                    remediation=(
                        f"Check that the SQL is valid for the '{source_dialect}' dialect "
                        f"and that '{target_dialect}' is a supported sqlglot dialect."
                    ),
                    original_sql=sql,
                    dialect=source_dialect,
                    failing_construct=str(e),
                )
            ) from e

        if not results:
            raise SQLParseError(
                RivetError(
                    code="RVT-703",
                    message=f"Dialect translation produced no output from '{source_dialect}' to '{target_dialect}'.",
                    context={
                        "source_dialect": source_dialect,
                        "target_dialect": target_dialect,
                        "sql": sql,
                    },
                    remediation=(
                        f"Ensure the SQL is a valid SELECT statement for the '{source_dialect}' dialect."
                    ),
                    original_sql=sql,
                    dialect=source_dialect,
                )
            )

        return results[0]

    # ------------------------------------------------------------------
    # Schema inference (task 9.4)
    # ------------------------------------------------------------------

    def infer_schema(
        self,
        ast: exp.Expression,
        upstream_schemas: dict[str, Schema],
        dialect: str | None = None,
    ) -> tuple[Schema | None, list[str]]:
        """Infer output schema using sqlglot type annotation with upstream schemas.

        Returns (Schema | None, warnings). Failure produces (None, warnings),
        never raises.
        """
        warnings: list[str] = []
        try:
            # Build sqlglot schema dict from upstream schemas
            sg_schema = self._build_sqlglot_schema(upstream_schemas)

            # Qualify and annotate types
            qualified = qualify(
                ast.copy(),
                schema=sg_schema,
                validate_qualify_columns=False,
            )
            annotated = annotate_types(qualified, schema=sg_schema)

            # Find the outermost SELECT
            select = (
                annotated
                if isinstance(annotated, exp.Select)
                else annotated.find(exp.Select)
            )
            if select is None:
                return None, warnings

            columns: list[Column] = []
            for expr in select.expressions:
                col_name = expr.alias if isinstance(expr, exp.Alias) else expr.sql()
                inner = expr.this if isinstance(expr, exp.Alias) else expr
                dt = inner.type
                arrow_type = self._sqlglot_type_to_arrow(dt, warnings)
                columns.append(Column(name=col_name, type=arrow_type, nullable=True))

            return Schema(columns=columns) if columns else None, warnings
        except Exception:
            return None, warnings

    # ------------------------------------------------------------------
    # Column-level lineage extraction (task 9.5)
    # ------------------------------------------------------------------

    def extract_lineage(
        self,
        ast: exp.Expression,
        upstream_schemas: dict[str, Schema],
        joint_name: str = "",
    ) -> list[ColumnLineage]:
        """Extract column-level lineage from SQL projections.

        Derives lineage by analyzing each projection's source column references.
        Classifies transforms as: source, direct, renamed, expression, aggregation,
        window, literal, multi_column, opaque.

        Returns a list of ColumnLineage records, one per output column.
        """
        select = ast if isinstance(ast, exp.Select) else ast.find(exp.Select)
        if select is None:
            return []

        # Build alias → joint name map from FROM/JOIN table references
        alias_map: dict[str, str] = {}
        for table in ast.find_all(exp.Table):
            tname = table.name
            talias = table.alias
            if talias and tname and tname in upstream_schemas:
                alias_map[talias] = tname

        # Expand SELECT * if upstream schemas available
        expressions = self._resolve_star(select, upstream_schemas)

        lineage: list[ColumnLineage] = []
        for expr in expressions:
            alias: str | None = None
            inner = expr
            if isinstance(expr, exp.Alias):
                alias = expr.alias
                inner = expr.this

            output_col = alias or inner.sql()
            source_cols = list(inner.find_all(exp.Column))
            transform, origins, expr_str = self._classify_transform(
                inner, source_cols, alias, upstream_schemas, joint_name,
                alias_map=alias_map,
            )
            lineage.append(
                ColumnLineage(
                    output_column=output_col,
                    transform=transform,
                    origins=origins,
                    expression=expr_str,
                )
            )
        return lineage

    def _resolve_star(
        self,
        select: exp.Select,
        upstream_schemas: dict[str, Schema],
    ) -> list[exp.Expression]:
        """Expand SELECT * into explicit columns when upstream schemas are available."""
        expressions = list(select.expressions)
        if not expressions:
            return expressions

        has_star = any(isinstance(e, exp.Star) for e in expressions)
        if not has_star:
            return expressions

        if not upstream_schemas:
            return expressions

        # Determine which tables are referenced in FROM/JOIN
        referenced = self.extract_table_references(select)
        ref_names = {r.name for r in referenced}

        expanded: list[exp.Expression] = []
        for expr in expressions:
            if isinstance(expr, exp.Star):
                for table_name, schema in upstream_schemas.items():
                    if ref_names and table_name not in ref_names:
                        continue
                    for col in schema.columns:
                        expanded.append(exp.Column(this=exp.to_identifier(col.name)))
            else:
                expanded.append(expr)
        return expanded

    @staticmethod
    def _classify_transform(
        inner: exp.Expression,
        source_cols: list[exp.Column],
        alias: str | None,
        upstream_schemas: dict[str, Schema],
        joint_name: str,
        alias_map: dict[str, str] | None = None,
    ) -> tuple[str, list[ColumnOrigin], str | None]:
        """Classify a projection expression's transform type and extract origins."""
        # Window function
        if inner.find(exp.Window):
            origins = SQLParser._cols_to_origins(source_cols, upstream_schemas, joint_name, alias_map)
            return "window", origins, inner.sql()

        # Aggregate function
        if inner.find(exp.AggFunc):
            origins = SQLParser._cols_to_origins(source_cols, upstream_schemas, joint_name, alias_map)
            return "aggregation", origins, inner.sql()

        # Literal (no column references)
        if not source_cols:
            return "literal", [], inner.sql()

        origins = SQLParser._cols_to_origins(source_cols, upstream_schemas, joint_name, alias_map)

        # Single column reference — direct or renamed
        if len(source_cols) == 1 and isinstance(inner, exp.Column):
            col_name = source_cols[0].name
            if alias is not None and alias != col_name:
                return "renamed", origins, None
            return "direct", origins, None

        # Multiple source columns
        if len(source_cols) > 1:
            return "multi_column", origins, inner.sql()

        # Single column inside an expression (e.g. UPPER(col))
        return "expression", origins, inner.sql()

    @staticmethod
    def _cols_to_origins(
        cols: list[exp.Column],
        upstream_schemas: dict[str, Schema],
        joint_name: str,
        alias_map: dict[str, str] | None = None,
    ) -> list[ColumnOrigin]:
        """Convert sqlglot Column nodes to ColumnOrigin records."""
        origins: list[ColumnOrigin] = []
        seen: set[tuple[str, str]] = set()
        _alias_map = alias_map or {}
        for col in cols:
            table = col.table or ""
            col_name = col.name
            # Resolve table alias to joint name, then check upstream_schemas
            resolved = _alias_map.get(table, table)
            if resolved and resolved in upstream_schemas:
                origin_joint = resolved
            else:
                origin_joint = joint_name
            key = (origin_joint, col_name)
            if key not in seen:
                seen.add(key)
                origins.append(ColumnOrigin(joint=origin_joint, column=col_name))
        return origins

    @staticmethod
    def _build_sqlglot_schema(
        upstream_schemas: dict[str, Schema],
    ) -> dict[str, dict[str, str]]:
        """Convert upstream Schema objects to sqlglot schema format."""
        sg: dict[str, dict[str, str]] = {}
        for table_name, schema in upstream_schemas.items():
            cols: dict[str, str] = {}
            for col in schema.columns:
                # Handle decimal128(p,s) → DECIMAL(p,s)
                if col.type.startswith("decimal128"):
                    params = col.type[len("decimal128"):]
                    cols[col.name] = f"DECIMAL{params}"
                else:
                    cols[col.name] = _ARROW_TO_SQLGLOT.get(col.type, "TEXT")
            sg[table_name] = cols
        return sg

    @staticmethod
    def _sqlglot_type_to_arrow(
        dt: exp.DataType, warnings: list[str]
    ) -> str:
        """Map a sqlglot DataType to an Arrow type name."""
        type_kind = dt.this if isinstance(dt, exp.DataType) else None
        if type_kind is None or type_kind == exp.DataType.Type.UNKNOWN:
            return "large_binary"

        if type_kind == exp.DataType.Type.DECIMAL:
            # Extract precision and scale from expressions
            params = dt.expressions
            if len(params) >= 2:
                p = params[0].this.this
                s = params[1].this.this
                return f"decimal128({p},{s})"
            return "decimal128(38,18)"

        arrow = _SQLGLOT_TO_ARROW.get(type_kind)
        if arrow is not None:
            return arrow

        # Unmapped type → large_binary + RVT-706 warning
        warnings.append(
            f"RVT-706: Unmapped SQL type '{dt}' defaulting to large_binary."
        )
        return "large_binary"
