"""Check execution functions for the executor.

Functions related to quality check execution, SQL generation, and
interpretation of check results.
"""

from __future__ import annotations

import importlib
from typing import Any

import pyarrow
import pyarrow.compute as pc

from rivet_core.checks import CompiledCheck
from rivet_core.executor.helpers.arrow_helpers import _build_filter_expr
from rivet_core.executor.models import CheckExecutionResult


def _get_check_columns(cfg: dict[str, Any]) -> list[str]:
    """Extract column list from check config (handles both 'column' and 'columns')."""
    columns = cfg.get("columns", [])
    if isinstance(columns, str):
        columns = [columns]
    col = cfg.get("column", "")
    if col and not columns:
        columns = [col]
    return columns  # type: ignore[no-any-return]


def _check_not_null(cfg: dict[str, Any], table: pyarrow.Table) -> tuple[bool, str]:
    columns = _get_check_columns(cfg)
    passed = True
    messages = []
    for c in columns:
        if c in table.column_names:
            null_count = table.column(c).null_count
            if null_count > 0:
                passed = False
                messages.append(f"'{c}': {null_count} null(s)")
            else:
                messages.append(f"'{c}': passed")
        else:
            passed = False
            messages.append(f"'{c}': not found")
    return passed, f"not_null: {'; '.join(messages)}"


def _check_unique(cfg: dict[str, Any], table: pyarrow.Table) -> tuple[bool, str]:
    columns = _get_check_columns(cfg)
    passed = True
    messages = []
    for c in columns:
        if c in table.column_names:
            arr = table.column(c)
            n_unique = pc.count_distinct(arr).as_py()
            n_total = len(arr) - arr.null_count
            if n_unique != n_total:
                passed = False
                messages.append(f"'{c}': {n_total - n_unique} duplicate(s)")
            else:
                messages.append(f"'{c}': passed")
        else:
            passed = False
            messages.append(f"'{c}': not found")
    return passed, f"unique: {'; '.join(messages)}"


def _check_row_count(cfg: dict[str, Any], table: pyarrow.Table) -> tuple[bool, str]:
    min_count = cfg.get("min", 0)
    max_count = cfg.get("max")
    actual = table.num_rows
    passed = actual >= min_count and (max_count is None or actual <= max_count)
    return (
        passed,
        f"row_count: {actual} rows (min={min_count}, max={max_count}): {'passed' if passed else 'failed'}",
    )


def _check_accepted_values(cfg: dict[str, Any], table: pyarrow.Table) -> tuple[bool, str]:
    col = cfg.get("column", "")
    values = cfg.get("values", [])
    if col in table.column_names:
        arr = table.column(col)
        unique_vals = pc.unique(arr).to_pylist()
        invalid = [v for v in unique_vals if v is not None and v not in values]
        passed = len(invalid) == 0
        return (
            passed,
            f"accepted_values on '{col}': {'passed' if passed else f'invalid values {invalid}'}",
        )
    return False, f"accepted_values: column '{col}' not found"


def _check_expression(cfg: dict[str, Any], table: pyarrow.Table) -> tuple[bool, str]:
    expr_str = cfg.get("expression", "")
    expr = _build_filter_expr(expr_str, table)
    if expr is not None:
        filtered = table.filter(expr)
        failing = table.num_rows - filtered.num_rows
        passed = failing == 0
        return (
            passed,
            f"expression '{expr_str}': {'passed' if passed else f'{failing} failing row(s)'}",
        )
    return False, f"expression: could not parse '{expr_str}'"


def _check_custom(cfg: dict[str, Any], table: pyarrow.Table) -> tuple[bool, str]:
    func_path = cfg.get("function", "")
    try:
        mod_path, func_name = func_path.rsplit(":", 1)
        mod = importlib.import_module(mod_path)
        func = getattr(mod, func_name)
        result = func(table)
        return result.passed, result.message
    except Exception as exc:
        return False, f"custom check '{func_path}' failed: {exc}"


def _check_schema(cfg: dict[str, Any], table: pyarrow.Table) -> tuple[bool, str]:
    expected_cols = cfg.get("columns", {})
    for col_name, expected_type in expected_cols.items():
        if col_name not in table.column_names:
            return False, f"schema: column '{col_name}' not found"
        actual_type = str(table.schema.field(col_name).type)
        if actual_type != expected_type:
            return (
                False,
                f"schema: column '{col_name}' expected type '{expected_type}', got '{actual_type}'",
            )
    return True, "schema: passed"


def _check_freshness(cfg: dict[str, Any], table: pyarrow.Table) -> tuple[bool, str]:
    col = cfg.get("column", "")
    max_age_seconds = cfg.get("max_age_seconds", 86400)
    if col not in table.column_names:
        return False, f"freshness: column '{col}' not found"
    arr = table.column(col)
    max_val = pc.max(arr).as_py()
    if max_val is None:
        return False, f"freshness on '{col}': no non-null values"
    import datetime

    now = datetime.datetime.now(tz=datetime.UTC)
    if hasattr(max_val, "timestamp"):
        age = (now - max_val.replace(tzinfo=datetime.UTC)).total_seconds()
    else:
        age = float("inf")
    passed = age <= max_age_seconds
    return (
        passed,
        f"freshness on '{col}': age={age:.0f}s, max={max_age_seconds}s: {'passed' if passed else 'stale'}",
    )


_CHECK_HANDLERS: dict[str, Any] = {
    "not_null": _check_not_null,
    "unique": _check_unique,
    "row_count": _check_row_count,
    "accepted_values": _check_accepted_values,
    "expression": _check_expression,
    "custom": _check_custom,
    "schema": _check_schema,
    "freshness": _check_freshness,
}

# ---------------------------------------------------------------------------
# Check SQL generation for engine-native assertion execution
# ---------------------------------------------------------------------------

_SQL_TRANSLATABLE_CHECKS: frozenset[str] = frozenset(
    {"not_null", "unique", "row_count", "accepted_values", "expression"}
)


def _is_sql_translatable(check: CompiledCheck) -> bool:
    """Return True if the check type can be translated to SQL for engine-native execution."""
    return check.type in _SQL_TRANSLATABLE_CHECKS


def _generate_check_sql(check: CompiledCheck, table_ref: str) -> list[str]:
    """Generate SQL for a compiled check against the given table reference.

    Returns a list of SQL strings. Most check types produce a single SQL string,
    but multi-column ``not_null`` and ``unique`` checks produce one SQL per column.

    Each SQL is a SELECT that produces a single-row result with a ``result_value``
    column containing the metric needed to evaluate pass/fail.

    Raises ``ValueError`` for missing required config fields or unsupported types.
    """
    cfg = check.config
    check_type = check.type

    if check_type == "not_null":
        columns = _get_check_columns(cfg)
        if not columns:
            raise ValueError("not_null check requires 'column' or 'columns' in config")
        return [
            f"SELECT COUNT(*) AS result_value FROM {table_ref} WHERE {col} IS NULL"
            for col in columns
        ]

    if check_type == "unique":
        columns = _get_check_columns(cfg)
        if not columns:
            raise ValueError("unique check requires 'column' or 'columns' in config")
        return [
            f"SELECT COUNT(*) - COUNT(DISTINCT {col}) AS result_value FROM {table_ref} WHERE {col} IS NOT NULL"
            for col in columns
        ]

    if check_type == "row_count":
        return [f"SELECT COUNT(*) AS result_value FROM {table_ref}"]

    if check_type == "accepted_values":
        col = cfg.get("column", "")
        values = cfg.get("values", [])
        if not col:
            raise ValueError("accepted_values check requires 'column' in config")
        if not values:
            raise ValueError("accepted_values check requires 'values' in config")
        quoted = ", ".join(f"'{v}'" for v in values)
        return [
            f"SELECT COUNT(*) AS result_value FROM {table_ref} WHERE {col} IS NOT NULL AND {col} NOT IN ({quoted})"
        ]

    if check_type == "expression":
        expr = cfg.get("expression", "")
        if not expr:
            raise ValueError("expression check requires 'expression' in config")
        return [f"SELECT COUNT(*) AS result_value FROM {table_ref} WHERE NOT ({expr})"]

    raise ValueError(f"Cannot generate SQL for check type: {check_type!r}")


def _interpret_check_sql_result(
    check: CompiledCheck,
    result_table: pyarrow.Table,
) -> CheckExecutionResult:
    """Interpret the result of a check SQL query into a CheckExecutionResult.

    The ``result_table`` is expected to have a single row with a ``result_value``
    column produced by ``_generate_check_sql``.
    """
    result_value: int = result_table.column("result_value")[0].as_py()
    check_type = check.type
    cfg = check.config

    if check_type == "row_count":
        min_count = cfg.get("min", 0)
        max_count = cfg.get("max")
        passed = result_value >= min_count and (max_count is None or result_value <= max_count)
        message = (
            f"row_count: {result_value} rows (min={min_count}, max={max_count}): "
            f"{'passed' if passed else 'failed'}"
        )
    else:
        # not_null, unique, accepted_values, expression: result_value == 0 means passed
        passed = result_value == 0
        message = f"{check_type}: {'passed' if passed else f'{result_value} failing row(s)'}"

    return CheckExecutionResult(
        type=check.type,
        severity=check.severity,
        passed=passed,
        message=message,
        phase=check.phase,
        execution_method="engine_native",
    )


def _execute_check(check: CompiledCheck, table: pyarrow.Table) -> CheckExecutionResult:
    """Execute a single compiled check against a pyarrow.Table."""
    handler = _CHECK_HANDLERS.get(check.type)
    if check.type == "relationship":
        passed, message = True, "relationship: skipped (requires external reference data)"
    elif handler:
        passed, message = handler(check.config, table)
    else:
        passed, message = False, f"Unknown check type: {check.type}"

    return CheckExecutionResult(
        type=check.type,
        severity=check.severity,
        passed=passed,
        message=message,
        phase=check.phase,
    )
