"""Arrow-based comparison engine for test fixtures."""

from __future__ import annotations

import importlib
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from rivet_core.testing.models import ComparisonResult


def compare_tables(
    actual: pa.Table,
    expected: pa.Table,
    mode: str = "exact",
    options: dict[str, Any] | None = None,
    compare_function: str | None = None,
) -> ComparisonResult:
    """Compare actual vs expected Arrow tables.

    Modes: exact, unordered, schema_only, custom.
    """
    options = options or {}

    if mode == "schema_only":
        return _compare_schema_only(actual, expected)
    if mode == "custom":
        return _compare_custom(actual, expected, compare_function)

    # Apply ignore_columns before comparison
    ignore_cols = options.get("ignore_columns", [])
    if ignore_cols:
        expected = expected.drop_columns(
            [c for c in ignore_cols if c in expected.column_names]
        )
        actual = actual.drop_columns(
            [c for c in ignore_cols if c in actual.column_names]
        )

    # Check for missing columns in actual (RVT-904)
    missing = [c for c in expected.column_names if c not in actual.column_names]
    if missing:
        return ComparisonResult(
            passed=False,
            message=f"[RVT-904] Missing columns in actual: {missing}",
        )

    # Select only expected columns from actual, in expected's column order
    actual = actual.select(expected.column_names)

    # Empty table cases
    if expected.num_rows == 0 and actual.num_rows == 0:
        return ComparisonResult(passed=True, message="Both tables empty")
    if expected.num_rows == 0:
        return ComparisonResult(
            passed=False,
            message=f"Expected 0 rows, got {actual.num_rows}",
        )
    if actual.num_rows == 0:
        return ComparisonResult(
            passed=False,
            message=f"Expected {expected.num_rows} rows, got 0",
        )

    if mode == "unordered":
        return _compare_unordered(actual, expected, options)
    return _compare_exact(actual, expected, options)


def _values_equal(
    actual_val: Any, expected_val: Any, options: dict[str, Any],
) -> bool:
    """Compare two scalar values respecting options."""
    a_null = actual_val is None
    e_null = expected_val is None
    if a_null and e_null:
        return options.get("null_equals_null", True)  # type: ignore[no-any-return]
    if a_null or e_null:
        return False

    # Float tolerance
    if isinstance(actual_val, float) or isinstance(expected_val, float):
        try:
            a_f = float(actual_val)
            e_f = float(expected_val)
        except (TypeError, ValueError):
            return actual_val == expected_val  # type: ignore[no-any-return]
        atol = options.get("float_atol", options.get("tolerance", 1e-9))
        rtol = options.get("float_rtol", options.get("relative_tolerance", 1e-6))
        abs_diff = abs(a_f - e_f)
        if abs_diff <= atol:
            return True
        if abs_diff <= rtol * abs(e_f):
            return True
        return False

    # Timestamp tolerance
    ts_tol = options.get("timestamp_tolerance")
    if ts_tol is not None:
        from datetime import datetime

        if isinstance(actual_val, datetime) and isinstance(expected_val, datetime):
            return abs((actual_val - expected_val).total_seconds()) <= _parse_duration(ts_tol).total_seconds()  # type: ignore[no-any-return]

    return actual_val == expected_val  # type: ignore[no-any-return]


def _parse_duration(s: str) -> Any:
    """Parse a duration string like '1s', '100ms', '1us' into timedelta."""
    from datetime import timedelta

    s = s.strip()
    if s.endswith("ms"):
        return timedelta(milliseconds=float(s[:-2]))
    if s.endswith("us"):
        return timedelta(microseconds=float(s[:-2]))
    if s.endswith("s"):
        return timedelta(seconds=float(s[:-1]))
    return timedelta(seconds=float(s))


def _compare_exact(
    actual: pa.Table, expected: pa.Table, options: dict[str, Any],
) -> ComparisonResult:
    """Row-by-row, column-by-column comparison. Row order matters."""
    if actual.num_rows != expected.num_rows:
        return ComparisonResult(
            passed=False,
            message=f"Row count mismatch: expected {expected.num_rows}, got {actual.num_rows}",
        )

    diff = _build_diff(actual, expected, options)
    if diff:
        return ComparisonResult(
            passed=False,
            message=f"[RVT-905] {len(diff)} row(s) differ (showing first {len(diff)})",
            diff=diff,
        )
    return ComparisonResult(passed=True, message="Tables match")


def _compare_unordered(
    actual: pa.Table, expected: pa.Table, options: dict[str, Any],
) -> ComparisonResult:
    """Sort both tables then compare row-by-row. NULLS LAST."""
    if actual.num_rows != expected.num_rows:
        return ComparisonResult(
            passed=False,
            message=f"Row count mismatch: expected {expected.num_rows}, got {actual.num_rows}",
        )

    sort_by = options.get("sort_by", expected.column_names)
    sort_keys = [(col, "ascending") for col in sort_by]

    actual_indices = pc.sort_indices(actual, sort_keys=sort_keys, null_placement="at_end")
    expected_indices = pc.sort_indices(expected, sort_keys=sort_keys, null_placement="at_end")
    actual = actual.take(actual_indices)
    expected = expected.take(expected_indices)

    diff = _build_diff(actual, expected, options)
    if diff:
        return ComparisonResult(
            passed=False,
            message=f"[RVT-905] {len(diff)} row(s) differ (showing first {len(diff)})",
            diff=diff,
        )
    return ComparisonResult(passed=True, message="Tables match (unordered)")


def _compare_schema_only(
    actual: pa.Table, expected: pa.Table,
) -> ComparisonResult:
    """Compare column names and types only."""
    exp_fields = {f.name: f.type for f in expected.schema}
    act_fields = {f.name: f.type for f in actual.schema}

    missing = [n for n in exp_fields if n not in act_fields]
    if missing:
        return ComparisonResult(
            passed=False,
            message=f"[RVT-904] Missing columns in actual: {missing}",
        )

    type_mismatches = []
    for name, exp_type in exp_fields.items():
        act_type = act_fields[name]
        if act_type != exp_type:
            type_mismatches.append(
                {"column": name, "expected_type": str(exp_type), "actual_type": str(act_type)}
            )

    if type_mismatches:
        return ComparisonResult(
            passed=False,
            message=f"[RVT-904] Schema mismatch: {len(type_mismatches)} column(s) differ",
            diff=type_mismatches,
        )
    return ComparisonResult(passed=True, message="Schemas match")


def _compare_custom(
    actual: pa.Table,
    expected: pa.Table,
    function_path: str | None,
) -> ComparisonResult:
    """Import and call a custom comparison function."""
    if not function_path:
        return ComparisonResult(
            passed=False,
            message="[RVT-906] No compare_function specified for custom mode",
        )
    try:
        module_path, func_name = function_path.rsplit(".", 1)
        mod = importlib.import_module(module_path)
        func = getattr(mod, func_name)
    except Exception as exc:
        return ComparisonResult(
            passed=False,
            message=f"[RVT-906] Cannot import compare_function '{function_path}': {exc}",
        )
    return func(actual, expected)  # type: ignore[no-any-return]


def _build_diff(
    actual: pa.Table,
    expected: pa.Table,
    options: dict[str, Any],
    max_rows: int = 5,
) -> list[dict[str, Any]]:
    """Produce first max_rows differing rows with column name, expected, actual."""
    diffs: list[dict[str, Any]] = []
    for row_idx in range(min(actual.num_rows, expected.num_rows)):
        for col in expected.column_names:
            a_val = actual.column(col)[row_idx].as_py()
            e_val = expected.column(col)[row_idx].as_py()
            if not _values_equal(a_val, e_val, options):
                diffs.append({
                    "row": row_idx,
                    "column": col,
                    "expected": e_val,
                    "actual": a_val,
                })
                break  # one diff entry per row
        if len(diffs) >= max_rows:
            break
    return diffs
