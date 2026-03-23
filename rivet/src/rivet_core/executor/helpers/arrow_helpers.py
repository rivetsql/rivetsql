"""Arrow/residual operations for the executor.

Functions that apply residual operations, build filter expressions, parse
literals, handle Arrow type conversions, and compute materialization statistics.
"""

from __future__ import annotations

import re
from typing import Any

import pyarrow
import pyarrow.compute as pc

from rivet_core.compiler import CompiledJoint
from rivet_core.errors import ExecutionError, RivetError
from rivet_core.metrics import ColumnExecutionStats, MaterializationStats
from rivet_core.optimizer import ResidualPlan


def _apply_residuals(
    table: pyarrow.Table,
    residual: ResidualPlan,
    projected_columns: list[str] | None = None,
) -> pyarrow.Table:
    """Apply residual predicates, limits, casts, and projections post-materialization.

    When *projected_columns* is provided and the adapter could not push
    projections, only the listed columns are kept from the materialized table.
    """
    # Residual predicates
    for pred in residual.predicates:
        try:
            # Build filter expression from predicate columns
            # Use pyarrow.compute.filter with expression evaluation
            expr = _build_filter_expr(pred.expression, table)
            if expr is not None:
                table = table.filter(expr)
        except Exception:
            pass  # Best-effort: skip unparseable residual predicates

    # Residual limit
    if residual.limit is not None:
        table = table.slice(0, residual.limit)

    # Residual casts
    if residual.casts:
        fields: list[pyarrow.Field] = []
        needs_cast = False
        for _i, col_field in enumerate(table.schema):
            cast_match = next((c for c in residual.casts if c.column == col_field.name), None)
            if cast_match:
                target_type = _arrow_type_from_str(cast_match.to_type)
                if target_type is not None and target_type != col_field.type:
                    fields.append(pyarrow.field(col_field.name, target_type, col_field.nullable))
                    needs_cast = True
                else:
                    fields.append(col_field)
            else:
                fields.append(col_field)
        if needs_cast:
            target_schema = pyarrow.schema(fields)
            table = table.cast(target_schema)

    # Residual projections — select only the needed columns
    if projected_columns is not None:
        available = {f.name for f in table.schema}
        keep = [c for c in projected_columns if c in available]
        if keep:
            table = table.select(keep)

    return table


# ---------------------------------------------------------------------------
# SQL type → pyarrow type mapping for CAST expression evaluation
# ---------------------------------------------------------------------------

_SQL_TYPE_TO_ARROW: dict[str, pyarrow.DataType] = {
    "BOOLEAN": pyarrow.bool_(),
    "BOOL": pyarrow.bool_(),
    "TINYINT": pyarrow.int8(),
    "SMALLINT": pyarrow.int16(),
    "INT": pyarrow.int32(),
    "INTEGER": pyarrow.int32(),
    "BIGINT": pyarrow.int64(),
    "FLOAT": pyarrow.float32(),
    "REAL": pyarrow.float32(),
    "DOUBLE": pyarrow.float64(),
    "VARCHAR": pyarrow.utf8(),
    "TEXT": pyarrow.utf8(),
    "STRING": pyarrow.utf8(),
    "DATE": pyarrow.date32(),
    "TIMESTAMP": pyarrow.timestamp("us"),
}


def _parse_cast_expression(expression: str) -> tuple[str, pyarrow.DataType] | None:
    """Parse a CAST(col AS TYPE) expression, returning (source_col, arrow_type) or None."""
    m = re.match(r"^CAST\((.+)\s+AS\s+(\w+)\)$", expression.strip(), re.IGNORECASE)
    if not m:
        return None
    source_expr = m.group(1).strip()
    type_str = m.group(2).strip().upper()
    arrow_type = _SQL_TYPE_TO_ARROW.get(type_str)
    if arrow_type is None:
        return None
    return source_expr, arrow_type


def _apply_source_inline_residuals(
    table: pyarrow.Table,
    source_joint: CompiledJoint,
) -> pyarrow.Table:
    """Apply source inline transform predicates and limit from the LogicalPlan.

    Called in the fallback source read path where the source plugin reads all
    data without pushdown support.  Predicates are applied via
    ``_build_filter_expr`` and limit via ``table.slice``.
    """
    lp = source_joint.logical_plan
    if lp is None:
        return table

    for pred in lp.predicates:
        try:
            expr = _build_filter_expr(pred.expression, table)
            if expr is not None:
                table = table.filter(expr)
        except Exception:
            pass  # Best-effort: skip unparseable predicates

    if lp.limit is not None and lp.limit.count is not None:
        table = table.slice(0, lp.limit.count)

    return table


def _apply_source_expressions(
    table: pyarrow.Table,
    source_joint: CompiledJoint,
) -> pyarrow.Table:
    """Apply aliased column expressions from the source joint's LogicalPlan.

    Processes projections in declaration order:
    1. Simple column references with aliases → rename column
    2. CAST expressions → type cast via pyarrow
    3. Computed expressions → evaluate via DuckDB in-process

    Raises ExecutionError (RVT-501) if a referenced column is missing.
    Returns the table with only the declared output columns in declaration order.
    """
    lp = source_joint.logical_plan
    if lp is None or not lp.projections:
        return table

    # Detect SELECT * — single projection with expression "*" and no alias
    if (
        len(lp.projections) == 1
        and lp.projections[0].expression == "*"
        and lp.projections[0].alias is None
    ):
        return table

    output_columns: list[str] = []
    col_names = {f.name for f in table.schema}

    for proj in lp.projections:
        alias = proj.alias
        expr = proj.expression

        # Case 1: Simple column reference (no alias) — pass through
        if alias is None and not proj.source_columns:
            # Expression is the column name itself
            if expr not in col_names:
                raise ExecutionError(
                    RivetError(
                        code="RVT-501",
                        message=(
                            f"Source joint '{source_joint.name}' expression for column "
                            f"'{expr}' references '{expr}' which is not present in the "
                            f"materialized data"
                        ),
                        context={"joint": source_joint.name, "column": expr},
                        remediation="Check that the column exists in the source table.",
                    )
                )
            output_columns.append(expr)
            continue

        if alias is None:
            # Simple column reference with source_columns but no alias
            col = proj.source_columns[0] if proj.source_columns else expr
            if col not in col_names:
                raise ExecutionError(
                    RivetError(
                        code="RVT-501",
                        message=(
                            f"Source joint '{source_joint.name}' expression for column "
                            f"'{col}' references '{col}' which is not present in the "
                            f"materialized data"
                        ),
                        context={"joint": source_joint.name, "column": col},
                        remediation="Check that the column exists in the source table.",
                    )
                )
            output_columns.append(col)
            continue

        # Has an alias — check what kind of expression

        # Case 2: Simple rename (alias for a single column reference)
        if (
            len(proj.source_columns) == 1
            and expr.lower() == proj.source_columns[0].rsplit(".", 1)[-1].lower()
        ):
            src_col = proj.source_columns[0]
            if src_col not in col_names:
                raise ExecutionError(
                    RivetError(
                        code="RVT-501",
                        message=(
                            f"Source joint '{source_joint.name}' expression for column "
                            f"'{alias}' references '{src_col}' which is not present in "
                            f"the materialized data"
                        ),
                        context={"joint": source_joint.name, "column": alias},
                        remediation="Check that the column exists in the source table.",
                    )
                )
            idx = table.schema.get_field_index(src_col)
            table = table.rename_columns(
                [alias if i == idx else f.name for i, f in enumerate(table.schema)]
            )
            col_names.discard(src_col)
            col_names.add(alias)
            output_columns.append(alias)
            continue

        # Case 3: CAST expression
        cast_result = _parse_cast_expression(expr)
        if cast_result is not None:
            src_expr, arrow_type = cast_result
            if src_expr not in col_names:
                raise ExecutionError(
                    RivetError(
                        code="RVT-501",
                        message=(
                            f"Source joint '{source_joint.name}' expression for column "
                            f"'{alias}' references '{src_expr}' which is not present in "
                            f"the materialized data"
                        ),
                        context={"joint": source_joint.name, "column": alias},
                        remediation="Check that the column exists in the source table.",
                    )
                )
            col_array = table.column(src_expr).cast(arrow_type)
            table = table.append_column(alias, col_array)
            col_names.add(alias)
            output_columns.append(alias)
            continue

        # Case 4: Computed expression — evaluate via DuckDB in-process
        # Validate that all referenced source columns exist
        for src_col in proj.source_columns:
            if src_col not in col_names:
                raise ExecutionError(
                    RivetError(
                        code="RVT-501",
                        message=(
                            f"Source joint '{source_joint.name}' expression for column "
                            f"'{alias}' references '{src_col}' which is not present in "
                            f"the materialized data"
                        ),
                        context={"joint": source_joint.name, "column": alias},
                        remediation="Check that the column exists in the source table.",
                    )
                )

        try:
            import duckdb

            con = duckdb.connect(":memory:")
            con.register("__src", table)
            result = con.execute(
                f"SELECT {expr} AS {alias} FROM __src"  # noqa: S608
            ).fetch_arrow_table()
            col_array = result.column(alias)
            table = table.append_column(alias, col_array)
            col_names.add(alias)
            output_columns.append(alias)
        except Exception as exc:
            raise ExecutionError(  # noqa: B904
                RivetError(
                    code="RVT-501",
                    message=(
                        f"Source joint '{source_joint.name}' failed to evaluate "
                        f"expression '{expr}' for column '{alias}': {exc}"
                    ),
                    context={"joint": source_joint.name, "column": alias, "expression": expr},
                    remediation="Check that the expression is valid SQL.",
                )
            )

    # Return table with only the declared output columns in declaration order
    return table.select(output_columns)


def _build_filter_expr(expression: str, table: pyarrow.Table) -> Any:
    """Best-effort: build a pyarrow compute expression from a SQL-like predicate string.

    Returns a pyarrow Expression or None if unparseable.
    """
    expr = expression.strip()
    col_names = {f.name for f in table.schema}

    # Handle simple comparisons: column op value
    m = re.match(r"^(\w+)\s*(=|!=|<>|>=|<=|>|<)\s*(.+)$", expr)
    if m:
        col, op, val = m.group(1), m.group(2), m.group(3).strip()
        if col not in col_names:
            return None
        literal = _parse_literal(val)
        field_expr = pc.field(col)
        match op:
            case "=":
                return field_expr == literal
            case "!=" | "<>":
                return field_expr != literal
            case ">":
                return field_expr > literal
            case "<":
                return field_expr < literal
            case ">=":
                return field_expr >= literal
            case "<=":
                return field_expr <= literal

    # IS NOT NULL
    m = re.match(r"^(\w+)\s+IS\s+NOT\s+NULL$", expr, re.IGNORECASE)
    if m and m.group(1) in col_names:
        return pc.field(m.group(1)).is_valid()

    # IS NULL
    m = re.match(r"^(\w+)\s+IS\s+NULL$", expr, re.IGNORECASE)
    if m and m.group(1) in col_names:
        return ~pc.field(m.group(1)).is_valid()

    return None


def _parse_literal(val: str) -> Any:
    """Parse a SQL literal value into a Python value."""
    # String literal
    if (val.startswith("'") and val.endswith("'")) or (val.startswith('"') and val.endswith('"')):
        return val[1:-1]
    # Boolean
    if val.upper() == "TRUE":
        return True
    if val.upper() == "FALSE":
        return False
    # NULL
    if val.upper() == "NULL":
        return None
    # Numeric
    try:
        return int(val)
    except ValueError:
        pass
    try:
        return float(val)
    except ValueError:
        pass
    return val


_ARROW_TYPE_MAP: dict[str, pyarrow.DataType] = {
    "int8": pyarrow.int8(),
    "int16": pyarrow.int16(),
    "int32": pyarrow.int32(),
    "int64": pyarrow.int64(),
    "uint8": pyarrow.uint8(),
    "uint16": pyarrow.uint16(),
    "uint32": pyarrow.uint32(),
    "uint64": pyarrow.uint64(),
    "float16": pyarrow.float16(),
    "float32": pyarrow.float32(),
    "float64": pyarrow.float64(),
    "bool": pyarrow.bool_(),
    "utf8": pyarrow.utf8(),
    "large_utf8": pyarrow.large_utf8(),
    "string": pyarrow.utf8(),
    "date32": pyarrow.date32(),
    "date64": pyarrow.date64(),
    "timestamp[us]": pyarrow.timestamp("us"),
    "timestamp[ns]": pyarrow.timestamp("ns"),
    "timestamp[ms]": pyarrow.timestamp("ms"),
    "timestamp[s]": pyarrow.timestamp("s"),
    "binary": pyarrow.binary(),
    "large_binary": pyarrow.large_binary(),
}


def _arrow_type_from_str(type_str: str) -> pyarrow.DataType | None:
    """Convert an Arrow type name string to a pyarrow DataType."""
    return _ARROW_TYPE_MAP.get(type_str)


SAMPLE_THRESHOLD = 1_000_000
SAMPLE_SIZE = 100_000


def _compute_materialization_stats(table: pyarrow.Table) -> MaterializationStats:
    """Compute MaterializationStats for a materialized pyarrow.Table."""
    use_sample = table.num_rows > SAMPLE_THRESHOLD
    if use_sample:
        indices = pc.random(SAMPLE_SIZE).cast(pyarrow.float64()).to_pylist()
        scaled = [int(v * table.num_rows) % table.num_rows for v in indices]
        sample = table.take(scaled)
    else:
        sample = table

    col_stats: list[ColumnExecutionStats] = []
    for col_name in table.column_names:
        full_arr = table.column(col_name)
        sample_arr = sample.column(col_name) if use_sample else full_arr
        null_count = full_arr.null_count
        try:
            distinct_est = pc.count_distinct(sample_arr).as_py()
        except Exception:
            distinct_est = 0
        try:
            min_val = pc.min(full_arr).as_py()
            min_str = str(min_val) if min_val is not None else None
        except Exception:
            min_str = None
        try:
            max_val = pc.max(full_arr).as_py()
            max_str = str(max_val) if max_val is not None else None
        except Exception:
            max_str = None
        col_stats.append(
            ColumnExecutionStats(
                column=col_name,
                null_count=null_count,
                distinct_count_estimate=distinct_est,
                min_value=min_str,
                max_value=max_str,
            )
        )
    return MaterializationStats(
        row_count=table.num_rows,
        byte_size=table.nbytes,
        column_stats=col_stats,
        sampled=use_sample,
    )


def _schemas_are_compatible(expected: dict[str, str], actual: dict[str, str]) -> bool:
    """Check if two schemas are compatible for sink writes.

    Returns True if schemas match after normalization, or if they differ only
    in ways that don't affect data correctness (e.g., string vs timestamp for
    date columns, which can be cast automatically by most sinks).

    Args:
        expected: Expected schema from compiler inference
        actual: Actual schema from execution engine

    Returns:
        True if schemas are compatible, False if there's a real mismatch
    """
    # Column names must match exactly
    if set(expected.keys()) != set(actual.keys()):
        return False

    # Check each column type
    for col_name in expected:
        expected_type = _normalize_arrow_type(expected[col_name])
        actual_type = _normalize_arrow_type(actual[col_name])

        # After normalization, if types match, they're compatible
        if expected_type == actual_type:
            continue

        # Allow string/timestamp interchangeability (common for date columns)
        # Sinks can typically handle this conversion automatically
        if {expected_type, actual_type} <= {
            "utf8",
            "timestamp[s]",
            "timestamp[ms]",
            "timestamp[us]",
            "timestamp[ns]",
        }:
            continue

        # If we get here, there's a real incompatibility
        return False

    return True


def _normalize_arrow_type(type_str: str) -> str:
    """Normalize Arrow type string to canonical form for comparison.

    Handles semantic equivalents:
    - utf8 ↔ string
    - float64 ↔ double
    - date32 ↔ date32[day]
    - decimal128(38, 0) → int64 (for integer aggregations)
    - timestamp[s] with timezone info stripped

    Args:
        type_str: Arrow type string (e.g., "utf8", "string", "float64")

    Returns:
        Normalized type string for comparison
    """
    # Strip whitespace
    normalized = type_str.strip()

    # Handle type aliases
    type_aliases = {
        "string": "utf8",
        "large_string": "large_utf8",
        "double": "float64",
        "float": "float32",
    }
    if normalized in type_aliases:
        return type_aliases[normalized]

    # Handle date32 with unit specifier
    if normalized.startswith("date32["):
        return "date32"

    # Handle decimal128(38, 0) as int64 (common from SQL SUM/COUNT)
    if normalized.startswith("decimal128(38, 0)"):
        return "int64"

    # Handle timestamp with timezone - normalize to base timestamp
    if normalized.startswith("timestamp["):
        # Extract unit: timestamp[s], timestamp[ms], etc.
        # Strip timezone info if present (e.g., "timestamp[s, tz=UTC]" -> "timestamp[s]")
        if "]" in normalized:
            bracket_content = normalized[len("timestamp[") : normalized.index("]")]
            # If there's a comma, take only the unit part before it
            if "," in bracket_content:
                unit = bracket_content.split(",")[0].strip()
                return f"timestamp[{unit}]"
            # Otherwise return as-is
            return normalized.split("]")[0] + "]"

    return normalized


def _schema_to_dict(schema: Any) -> dict[str, str]:
    """Convert Schema object to dict[column_name, arrow_type_str].

    Args:
        schema: Schema object with columns attribute, or None

    Returns:
        Dictionary mapping column names to Arrow type strings.
        Returns empty dict if schema is None.
    """
    if schema is None:
        return {}
    return {col.name: col.type for col in schema.columns}
