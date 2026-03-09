"""Executor: runs a CompiledAssembly deterministically.

The Executor follows execution_order exactly, dispatches fused groups,
applies residual operations post-materialization, and materializes where
the CompiledAssembly specifies.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import time
import traceback
from collections import deque
from dataclasses import dataclass
from typing import Any

import pyarrow
import pyarrow.compute as pc

from rivet_core.checks import CompiledCheck
from rivet_core.compiler import (
    CompiledAssembly,
    CompiledCatalog,
    CompiledJoint,
    Materialization,
)
from rivet_core.context import RivetContext
from rivet_core.errors import CompilationError, ExecutionError, RivetError
from rivet_core.metrics import (
    ColumnExecutionStats,
    MaterializationStats,
    PhasedTiming,
    PluginMetrics,
)
from rivet_core.models import Catalog, ComputeEngine, Joint, Material
from rivet_core.optimizer import (
    AdapterPushdownResult,
    CastPushdownResult,
    FusedGroup,
    LimitPushdownResult,
    PredicatePushdownResult,
    ProjectionPushdownResult,
    PushdownPlan,
    ResidualPlan,
)
from rivet_core.plugins import (
    ComputeEnginePlugin,
    CrossJointContext,
    PluginRegistry,
    UpstreamResolution,
)
from rivet_core.stats import RunStats, StatsCollector
from rivet_core.strategies import (
    ArrowMaterialization,
    MaterializationContext,
    MaterializationStrategy,
    MaterializedRef,
    _ArrowMaterializedRef,
)

# ---------------------------------------------------------------------------
# Residual merging
# ---------------------------------------------------------------------------


def _merge_residuals(group_residual: ResidualPlan | None, adapter_residual: ResidualPlan) -> ResidualPlan:
    """Merge compiler-computed residuals with adapter-returned residuals."""
    if group_residual is None:
        return adapter_residual
    return ResidualPlan(
        predicates=group_residual.predicates + adapter_residual.predicates,
        limit=adapter_residual.limit if adapter_residual.limit is not None else group_residual.limit,
        casts=group_residual.casts + adapter_residual.casts,
    )


def _merge_source_limit_into_pushdown(
    pushdown: PushdownPlan | None,
    source_joint: CompiledJoint,
) -> PushdownPlan | None:
    """If the source joint's SQL has a LIMIT, ensure it's in the pushdown for the adapter.

    When a source joint like ``SELECT * FROM table LIMIT 100`` is read via an adapter,
    the adapter ignores the SQL body and reads directly from storage. The LIMIT must be
    forwarded through the pushdown so the adapter can apply it at the storage level.
    """
    lp = source_joint.logical_plan
    if lp is None or lp.limit is None or lp.limit.count is None:
        return pushdown

    source_limit = lp.limit.count

    if pushdown is not None and pushdown.limit.pushed_limit is not None:
        # Already has a pushed limit — use the smaller of the two
        effective = min(pushdown.limit.pushed_limit, source_limit)
        if effective == pushdown.limit.pushed_limit:
            return pushdown
        return PushdownPlan(
            predicates=pushdown.predicates,
            projections=pushdown.projections,
            limit=LimitPushdownResult(pushed_limit=effective, residual_limit=None, reason=None),
            casts=pushdown.casts,
        )

    # No existing pushdown or no limit in it — create/augment with source limit
    if pushdown is None:
        return PushdownPlan(
            predicates=PredicatePushdownResult(pushed=[], residual=[]),
            projections=ProjectionPushdownResult(pushed_columns=None, reason=None),
            limit=LimitPushdownResult(pushed_limit=source_limit, residual_limit=None, reason=None),
            casts=CastPushdownResult(pushed=[], residual=[]),
        )

    return PushdownPlan(
        predicates=pushdown.predicates,
        projections=pushdown.projections,
        limit=LimitPushdownResult(pushed_limit=source_limit, residual_limit=None, reason=None),
        casts=pushdown.casts,
    )


def _merge_cross_group_predicates(
    pushdown: PushdownPlan | None,
    group: FusedGroup,
    joint_name: str,
) -> PushdownPlan | None:
    """Merge cross-group predicates from ``group.per_joint_predicates`` into *pushdown*.

    If the group has cross-group predicates targeting *joint_name*, they are
    appended to the pushed predicate list.  If *pushdown* is ``None`` and there
    are cross-group predicates, a new ``PushdownPlan`` is created with those
    predicates as the only pushed entries.  When no cross-group predicates
    exist for the joint, *pushdown* is returned unchanged.
    """
    xg_preds = group.per_joint_predicates.get(joint_name)
    if not xg_preds:
        return pushdown

    if pushdown is None:
        return PushdownPlan(
            predicates=PredicatePushdownResult(pushed=list(xg_preds), residual=[]),
            projections=ProjectionPushdownResult(pushed_columns=None, reason=None),
            limit=LimitPushdownResult(pushed_limit=None, residual_limit=None, reason=None),
            casts=CastPushdownResult(pushed=[], residual=[]),
        )

    merged_pushed = list(pushdown.predicates.pushed) + list(xg_preds)
    return PushdownPlan(
        predicates=PredicatePushdownResult(pushed=merged_pushed, residual=pushdown.predicates.residual),
        projections=pushdown.projections,
        limit=pushdown.limit,
        casts=pushdown.casts,
    )

def _merge_cross_group_projections(
    pushdown: PushdownPlan | None,
    group: FusedGroup,
    joint_name: str,
) -> PushdownPlan | None:
    """Merge cross-group projections from ``group.per_joint_projections`` into *pushdown*.

    If the group has cross-group projections targeting *joint_name*, they are
    merged with any existing intra-group projections.  When both exist, the
    intersection is used (only columns needed by both).  When only cross-group
    projections exist, they are used directly.  If *pushdown* is ``None`` and
    there are cross-group projections, a new ``PushdownPlan`` is created.
    When no cross-group projections exist for the joint, *pushdown* is returned
    unchanged.
    """
    xg_cols = group.per_joint_projections.get(joint_name)
    if not xg_cols:
        return pushdown

    if pushdown is None:
        return PushdownPlan(
            predicates=PredicatePushdownResult(pushed=[], residual=[]),
            projections=ProjectionPushdownResult(pushed_columns=list(xg_cols), reason=None),
            limit=LimitPushdownResult(pushed_limit=None, residual_limit=None, reason=None),
            casts=CastPushdownResult(pushed=[], residual=[]),
        )

    existing = pushdown.projections.pushed_columns
    if existing is not None:
        merged = sorted(set(existing) & set(xg_cols))
    else:
        merged = list(xg_cols)

    return PushdownPlan(
        predicates=pushdown.predicates,
        projections=ProjectionPushdownResult(pushed_columns=merged, reason=pushdown.projections.reason),
        limit=pushdown.limit,
        casts=pushdown.casts,
    )


def _merge_cross_group_limits(
    pushdown: PushdownPlan | None,
    group: FusedGroup,
    joint_name: str,
) -> PushdownPlan | None:
    """Merge cross-group limits from ``group.per_joint_limits`` into *pushdown*.

    If the group has a cross-group limit targeting *joint_name*, it is merged
    with any existing pushed limit.  When both exist, the minimum is used (the
    tighter constraint wins).  When only a cross-group limit exists, it is used
    directly.  If *pushdown* is ``None`` and there is a cross-group limit, a
    new ``PushdownPlan`` is created.  When no cross-group limit exists for the
    joint, *pushdown* is returned unchanged.
    """
    xg_limit = group.per_joint_limits.get(joint_name)
    if xg_limit is None:
        return pushdown

    if pushdown is None:
        return PushdownPlan(
            predicates=PredicatePushdownResult(pushed=[], residual=[]),
            projections=ProjectionPushdownResult(pushed_columns=None, reason=None),
            limit=LimitPushdownResult(pushed_limit=xg_limit, residual_limit=None, reason=None),
            casts=CastPushdownResult(pushed=[], residual=[]),
        )

    existing = pushdown.limit.pushed_limit
    if existing is not None:
        effective = min(existing, xg_limit)
    else:
        effective = xg_limit

    return PushdownPlan(
        predicates=pushdown.predicates,
        projections=pushdown.projections,
        limit=LimitPushdownResult(
            pushed_limit=effective,
            residual_limit=pushdown.limit.residual_limit,
            reason=pushdown.limit.reason,
        ),
        casts=pushdown.casts,
    )



# ---------------------------------------------------------------------------
# Execution result data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CheckExecutionResult:
    type: str
    severity: str
    passed: bool
    message: str
    phase: str  # "assertion" or "audit"
    read_back_rows: int | None = None


@dataclass(frozen=True)
class JointExecutionResult:
    name: str
    success: bool
    rows_in: int | None
    rows_out: int | None
    timing: PhasedTiming | None
    fused_group_id: str | None
    materialized: bool
    materialization_trigger: str | None
    materialization_stats: MaterializationStats | None
    check_results: list[CheckExecutionResult]
    plugin_metrics: PluginMetrics | None
    error: RivetError | None


@dataclass(frozen=True)
class FusedGroupExecutionResult:
    group_id: str
    joints: list[str]
    success: bool
    rows_in: int
    rows_out: int
    timing: PhasedTiming
    materialization_stats: MaterializationStats | None
    plugin_metrics: PluginMetrics | None
    error: RivetError | None


@dataclass(frozen=True)
class ExecutionResult:
    success: bool
    status: str  # "success", "failure", "partial_failure"
    joint_results: list[JointExecutionResult]
    group_results: list[FusedGroupExecutionResult]
    total_time_ms: float
    total_materializations: int
    total_failures: int
    total_check_failures: int
    total_check_warnings: int
    run_stats: RunStats | None = None


# ---------------------------------------------------------------------------
# Residual application helpers
# ---------------------------------------------------------------------------


def _apply_residuals(table: pyarrow.Table, residual: ResidualPlan) -> pyarrow.Table:
    """Apply residual predicates, limits, and casts post-materialization."""
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
            cast_match = next(
                (c for c in residual.casts if c.column == col_field.name), None
            )
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

    return table


def _build_filter_expr(expression: str, table: pyarrow.Table) -> Any:
    """Best-effort: build a pyarrow compute expression from a SQL-like predicate string.

    Returns a pyarrow Expression or None if unparseable.
    """
    expr = expression.strip()
    col_names = {f.name for f in table.schema}

    # Handle simple comparisons: column op value
    import re

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
    if (val.startswith("'") and val.endswith("'")) or (
        val.startswith('"') and val.endswith('"')
    ):
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
        col_stats.append(ColumnExecutionStats(
            column=col_name,
            null_count=null_count,
            distinct_count_estimate=distinct_est,
            min_value=min_str,
            max_value=max_str,
        ))
    return MaterializationStats(
        row_count=table.num_rows,
        byte_size=table.nbytes,
        column_stats=col_stats,
        sampled=use_sample,
    )


def _normalize_python_result(joint_name: str, func_path: str, result: Any) -> Material:
    """Normalize a PythonJoint return value to a Material.

    Accepts: Material, MaterializedRef, pyarrow.Table, pandas.DataFrame,
    polars.DataFrame, pyspark.sql.DataFrame.
    Raises RVT-752 on None or unsupported type.
    """
    _remediation = (
        "Return a Material, MaterializedRef, pyarrow.Table, pandas.DataFrame, "
        "polars.DataFrame, or pyspark DataFrame."
    )

    if result is None:
        raise ExecutionError(
            RivetError(
                code="RVT-752",
                message=f"PythonJoint '{joint_name}' returned None.",
                context={"joint": joint_name, "function": func_path, "return_type": "NoneType"},
                remediation=_remediation,
            )
        )

    # Material passthrough
    if isinstance(result, Material):
        if result.materialized_ref is not None:
            return result
        raise ExecutionError(
            RivetError(
                code="RVT-752",
                message=f"PythonJoint '{joint_name}' returned a Material with no MaterializedRef.",
                context={"joint": joint_name, "function": func_path, "return_type": "Material"},
                remediation=_remediation,
            )
        )

    # MaterializedRef wrapping
    if isinstance(result, MaterializedRef):
        return Material(name=joint_name, catalog="", materialized_ref=result, state="materialized")

    # DataFrame branches — convert to Arrow then wrap
    table: pyarrow.Table | None = None

    if isinstance(result, pyarrow.Table):
        table = result
    else:
        # pandas DataFrame
        try:
            import pandas
            if isinstance(result, pandas.DataFrame):
                table = pyarrow.Table.from_pandas(result)
        except ImportError:
            pass

        # polars DataFrame
        if table is None:
            try:
                import polars
                if isinstance(result, polars.DataFrame):
                    table = result.to_arrow()
            except ImportError:
                pass

        # pyspark DataFrame
        if table is None:
            try:
                import pyspark.sql
                if isinstance(result, pyspark.sql.DataFrame):
                    table = pyarrow.Table.from_pandas(result.toPandas())
            except ImportError:
                pass

    if table is not None:
        ref = _ArrowMaterializedRef(table)
        return Material(name=joint_name, catalog="", materialized_ref=ref, state="materialized")

    raise ExecutionError(
        RivetError(
            code="RVT-752",
            message=f"PythonJoint '{joint_name}' returned unsupported type '{type(result).__name__}'.",
            context={"joint": joint_name, "function": func_path, "return_type": type(result).__name__},
            remediation=_remediation,
        )
    )


# ---------------------------------------------------------------------------
# ErrorMaterial — placeholder for failed joint outputs (Req 40.2)
# ---------------------------------------------------------------------------


def _make_error_material(joint_name: str, error: RivetError) -> Material:
    """Create an ErrorMaterial for a failed joint's output."""
    return Material(
        name=joint_name,
        catalog="",
        state="error",
        schema=None,
    )


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------


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
    return passed, f"row_count: {actual} rows (min={min_count}, max={max_count}): {'passed' if passed else 'failed'}"


def _check_accepted_values(cfg: dict[str, Any], table: pyarrow.Table) -> tuple[bool, str]:
    col = cfg.get("column", "")
    values = cfg.get("values", [])
    if col in table.column_names:
        arr = table.column(col)
        unique_vals = pc.unique(arr).to_pylist()
        invalid = [v for v in unique_vals if v is not None and v not in values]
        passed = len(invalid) == 0
        return passed, f"accepted_values on '{col}': {'passed' if passed else f'invalid values {invalid}'}"
    return False, f"accepted_values: column '{col}' not found"


def _check_expression(cfg: dict[str, Any], table: pyarrow.Table) -> tuple[bool, str]:
    expr_str = cfg.get("expression", "")
    expr = _build_filter_expr(expr_str, table)
    if expr is not None:
        filtered = table.filter(expr)
        failing = table.num_rows - filtered.num_rows
        passed = failing == 0
        return passed, f"expression '{expr_str}': {'passed' if passed else f'{failing} failing row(s)'}"
    return False, f"expression: could not parse '{expr_str}'"


def _check_custom(cfg: dict[str, Any], table: pyarrow.Table) -> tuple[bool, str]:
    func_path = cfg.get("function", "")
    try:
        mod_path, func_name = func_path.rsplit(".", 1)
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
            return False, f"schema: column '{col_name}' expected type '{expected_type}', got '{actual_type}'"
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
    return passed, f"freshness on '{col}': age={age:.0f}s, max={max_age_seconds}s: {'passed' if passed else 'stale'}"


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


class DependencyGraph:
    """DAG of fused groups derived from upstream joint references."""

    _upstream: dict[str, set[str]]
    _downstream: dict[str, set[str]]
    _in_degree: dict[str, int]
    _submitted: set[str]
    _completed: set[str]

    def __init__(
        self,
        upstream: dict[str, set[str]],
        downstream: dict[str, set[str]],
        in_degree: dict[str, int],
    ) -> None:
        self._upstream = upstream
        self._downstream = downstream
        self._in_degree = in_degree
        self._submitted: set[str] = set()
        self._completed: set[str] = set()

    @staticmethod
    def build(
        fused_groups: list[FusedGroup],
        joint_map: dict[str, CompiledJoint],
    ) -> DependencyGraph:
        """Construct the graph from compiled assembly data.

        An edge A -> B exists iff any joint in B has an upstream joint
        whose output is produced by group A.
        """
        # Map each joint name to its owning fused group ID
        joint_to_group: dict[str, str] = {}
        for group in fused_groups:
            for joint_name in group.joints:
                joint_to_group[joint_name] = group.id

        upstream: dict[str, set[str]] = {g.id: set() for g in fused_groups}
        downstream: dict[str, set[str]] = {g.id: set() for g in fused_groups}

        for group in fused_groups:
            for joint_name in group.joints:
                compiled_joint = joint_map.get(joint_name)
                if compiled_joint is None:
                    continue
                for up_name in compiled_joint.upstream:
                    up_group_id = joint_to_group.get(up_name)
                    # Skip upstream refs not belonging to any group (Req 1.3)
                    if up_group_id is None:
                        continue
                    # Skip self-references (joints within the same group)
                    if up_group_id == group.id:
                        continue
                    upstream[group.id].add(up_group_id)
                    downstream[up_group_id].add(group.id)

        in_degree: dict[str, int] = {
            gid: len(ups) for gid, ups in upstream.items()
        }

        return DependencyGraph(
            upstream=upstream,
            downstream=downstream,
            in_degree=in_degree,
        )

    def ready_groups(self) -> list[str]:
        """Return group IDs with in-degree 0 that haven't been submitted."""
        return [
            gid
            for gid, deg in self._in_degree.items()
            if deg == 0 and gid not in self._submitted
        ]

    def mark_complete(self, group_id: str) -> list[str]:
        """Add to completed, decrement downstream in-degrees.

        Returns newly ready group IDs (in-degree just became 0 and not yet
        submitted).
        """
        self._completed.add(group_id)
        newly_ready: list[str] = []
        for ds_id in self._downstream.get(group_id, set()):
            self._in_degree[ds_id] -= 1
            if self._in_degree[ds_id] == 0 and ds_id not in self._submitted:
                newly_ready.append(ds_id)
        return newly_ready

    def mark_failed(self, group_id: str) -> list[str]:
        """BFS to collect all transitive downstream group IDs."""
        visited: set[str] = set()
        queue: deque[str] = deque()
        # Seed with direct downstream of the failed group
        for ds_id in self._downstream.get(group_id, set()):
            if ds_id not in visited:
                visited.add(ds_id)
                queue.append(ds_id)
        while queue:
            current = queue.popleft()
            for ds_id in self._downstream.get(current, set()):
                if ds_id not in visited:
                    visited.add(ds_id)
                    queue.append(ds_id)
        return list(visited)


class EngineConcurrencyPool:
    """Manages concurrency for a single engine instance."""

    def __init__(self, engine_name: str, concurrency_limit: int) -> None:
        self._semaphore = asyncio.Semaphore(concurrency_limit)
        self.engine_name = engine_name
        self.concurrency_limit = concurrency_limit

    async def __aenter__(self) -> EngineConcurrencyPool:
        """Acquire a slot (suspends coroutine if pool is full)."""
        await self._semaphore.acquire()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        """Release a slot."""
        self._semaphore.release()


def _resolve_concurrency_limits(
    engines: list[ComputeEngine],
    plugin_registry: PluginRegistry,
) -> dict[str, int]:
    """Resolve concurrency_limit for each engine.

    Priority: config["concurrency_limit"] (user override) > plugin.default_concurrency_limit > 1.
    Returns engine_name → concurrency_limit mapping.
    Raises ExecutionError if any resolved limit is invalid (< 1 or non-integer).
    """
    limits: dict[str, int] = {}
    for engine in engines:
        limit: Any = engine.config.get("concurrency_limit")
        if limit is None:
            plugin = plugin_registry.get_engine_plugin(engine.engine_type)
            if plugin is not None and hasattr(plugin, "default_concurrency_limit"):
                limit = plugin.default_concurrency_limit
            else:
                limit = 1
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ExecutionError(
                RivetError(
                    code="RVT-501",
                    message=(
                        f"Invalid concurrency_limit for engine '{engine.name}': "
                        f"{limit!r}. Must be a positive integer (>= 1)."
                    ),
                    context={"engine": engine.name, "concurrency_limit": limit},
                    remediation=(
                        "Set 'concurrency_limit' in the engine config to a positive integer, "
                        "or remove it to use the plugin default."
                    ),
                )
            )
        limits[engine.name] = limit
    return limits


class _nullcontext:
    """Minimal async context manager that does nothing (fallback when no pool)."""

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *exc: Any) -> None:
        pass


class Executor:
    """Executes a CompiledAssembly deterministically.

    Follows execution_order exactly. No re-resolution or re-optimization.
    """

    def __init__(self, registry: PluginRegistry | None = None) -> None:
        self._materialization_strategies: dict[str, MaterializationStrategy] = {
            "arrow": ArrowMaterialization(),
        }
        if registry is None:
            # Create a minimal default registry with the built-in arrow engine plugin
            from rivet_core.builtins.arrow_catalog import ArrowComputeEnginePlugin
            registry = PluginRegistry()
            registry.register_engine_plugin(ArrowComputeEnginePlugin())
        self._registry = registry

    def _get_materialization_strategy(self, name: str) -> MaterializationStrategy:
        return self._materialization_strategies.get(name) or self._materialization_strategies["arrow"]

    @staticmethod
    def _has_upstream_failure(
        group: FusedGroup,
        joint_map: dict[str, CompiledJoint],
        failed_joints: set[str],
    ) -> bool:
        """Check if any upstream joint of this group has failed."""
        for jn in group.joints:
            cj = joint_map.get(jn)
            if cj:
                for up in cj.upstream:
                    if up in failed_joints:
                        return True
        return False

    @staticmethod
    def _record_group_failure(
        group: FusedGroup,
        error: RivetError,
        failed_joints: set[str],
        joint_results: list[JointExecutionResult],
        group_results: list[FusedGroupExecutionResult],
        step_ms: float = 0.0,
        stats_collector: StatsCollector | None = None,
    ) -> None:
        """Record failure results for all joints in a group."""
        timing = PhasedTiming(
            total_ms=step_ms, engine_ms=0.0, materialize_ms=0.0,
            residual_ms=0.0, check_ms=0.0,
        )
        for jn in group.joints:
            failed_joints.add(jn)
            joint_results.append(
                JointExecutionResult(
                    name=jn, success=False, rows_in=None, rows_out=None,
                    timing=timing, fused_group_id=group.id,
                    materialized=False, materialization_trigger=None,
                    materialization_stats=None, check_results=[],
                    plugin_metrics=None, error=error,
                )
            )
        group_results.append(
            FusedGroupExecutionResult(
                group_id=group.id, joints=list(group.joints),
                success=False, rows_in=0, rows_out=0, timing=timing,
                materialization_stats=None, plugin_metrics=None,
                error=error,
            )
        )
        if stats_collector is not None:
            stats_collector.record_group_timing(
                group.id, list(group.joints), timing, success=False, error=error,
            )
            for jn in group.joints:
                stats_collector.record_joint_stats(
                    jn, rows_in=None, rows_out=None, timing=timing,
                    materialization_stats=None, skipped=True,
                    skip_reason=error.message if error else "upstream failure",
                )

    @staticmethod
    async def _run_assertion_checks(
        group: FusedGroup,
        joint_map: dict[str, CompiledJoint],
        result_ref: MaterializedRef,
    ) -> tuple[dict[str, list[CheckExecutionResult]], bool, int, int]:
        """Run assertion-phase checks for all joints in a group.

        Returns (check_results_by_joint, has_error, error_count, warning_count).
        """
        all_check_results: dict[str, list[CheckExecutionResult]] = {
            jn: [] for jn in group.joints
        }
        assertion_error = False
        check_failures = 0
        check_warnings = 0
        assertion_table: pyarrow.Table | None = None

        for jn in group.joints:
            cj = joint_map.get(jn)
            if not cj or not cj.checks:
                continue
            assertion_checks = [c for c in cj.checks if c.phase == "assertion"]
            if not assertion_checks:
                continue
            if assertion_table is None:
                assertion_table = result_ref.to_arrow()
            for chk in assertion_checks:
                cr = _execute_check(chk, assertion_table)
                all_check_results[jn].append(cr)
                if not cr.passed:
                    if cr.severity == "error":
                        check_failures += 1
                        assertion_error = True
                    else:
                        check_warnings += 1

        return all_check_results, assertion_error, check_failures, check_warnings

    async def _run_sink_audits(
        self,
        group: FusedGroup,
        joint_map: dict[str, CompiledJoint],
        result_ref: MaterializedRef,
        catalog_map: dict[str, CompiledCatalog],
        assertion_error: bool,
        joint_results: list[JointExecutionResult],
    ) -> tuple[int, int]:
        """Execute sink writes and audit checks. Returns (error_count, warning_count)."""
        check_failures = 0
        check_warnings = 0
        for jn in group.joints:
            cj = joint_map.get(jn)
            if not cj or cj.type != "sink" or assertion_error:
                continue
            await self._dispatch_sink_write(cj, result_ref, catalog_map)
            audit_checks = [c for c in cj.checks if c.phase == "audit"]
            if not audit_checks:
                continue
            audit_results = await self._run_audits(
                cj, audit_checks, result_ref.to_arrow(), catalog_map
            )
            for i, jr in enumerate(joint_results):
                if jr.name == jn and jr.fused_group_id == group.id:
                    merged = list(jr.check_results) + audit_results
                    joint_results[i] = JointExecutionResult(
                        name=jr.name, success=jr.success,
                        rows_in=jr.rows_in, rows_out=jr.rows_out,
                        timing=jr.timing, fused_group_id=jr.fused_group_id,
                        materialized=jr.materialized,
                        materialization_trigger=jr.materialization_trigger,
                        materialization_stats=jr.materialization_stats,
                        check_results=merged,
                        plugin_metrics=jr.plugin_metrics, error=jr.error,
                    )
                    break
            for ar in audit_results:
                if not ar.passed:
                    if ar.severity == "error":
                        check_failures += 1
                    else:
                        check_warnings += 1
        return check_failures, check_warnings

    async def _execute_group_success(
        self,
        group: FusedGroup,
        joint_map: dict[str, CompiledJoint],
        catalog_map: dict[str, CompiledCatalog],
        mat_map: dict[str, list[Materialization]],
        materials: dict[str, MaterializedRef],
        failed_joints: set[str],
        joint_results: list[JointExecutionResult],
        group_results: list[FusedGroupExecutionResult],
        fail_fast: bool,
        step_start: float,
        engine_start: float,
        stats_collector: StatsCollector | None = None,
    ) -> tuple[int, int, int, int, bool]:
        """Execute a single group successfully and record results.

        Returns (materializations, failures, check_failures, check_warnings, stop).
        """
        needed_keys: set[str] = set()
        for jn in group.entry_joints or group.joints:
            cj = joint_map.get(jn)
            if cj:
                needed_keys.update(cj.upstream)

        arrow_materials: dict[str, pyarrow.Table] = {
            k: v.to_arrow() for k, v in materials.items() if k in needed_keys
        }
        result_ref, adapter_residual = await self._execute_fused_group(
            group, arrow_materials, joint_map, catalog_map, ref_materials=materials,
            stats_collector=stats_collector,
        )
        engine_ms = (time.monotonic() - engine_start) * 1000

        # Collect engine metrics via plugin.collect_metrics
        if stats_collector is not None:
            plugin = self._registry.get_engine_plugin(group.engine_type) if self._registry else None
            if plugin is not None:
                # Build execution context
                sql = group.resolved_sql or group.fused_sql or ""
                if group.fusion_result:
                    sql = group.fusion_result.resolved_fused_sql or group.fusion_result.fused_sql or sql
                execution_context: dict[str, Any] = {
                    "sql": sql[:1000],
                    "group_id": group.id,
                    "engine_type": group.engine_type,
                    "engine_ms": engine_ms,
                }
                # Merge response metadata if available from plugin
                response_metadata = getattr(plugin, "_last_response_metadata", None)
                if isinstance(response_metadata, dict):
                    for k, v in response_metadata.items():
                        if k not in execution_context:
                            execution_context[k] = v
                try:
                    metrics = plugin.collect_metrics(execution_context)
                    if metrics is None:
                        metrics = PluginMetrics()
                    stats_collector.record_engine_metrics(group.id, metrics)
                except Exception:
                    import logging
                    logging.getLogger("rivet_core.executor").warning(
                        "collect_metrics failed for group '%s'; continuing without engine metrics",
                        group.id,
                    )
                    stats_collector.record_engine_metrics(group.id, PluginMetrics())

        residual_start = time.monotonic()
        merged_residual = _merge_residuals(group.residual, adapter_residual) if adapter_residual else group.residual
        if merged_residual is not None:
            result_table = result_ref.to_arrow()
            result_table = _apply_residuals(result_table, merged_residual)
            result_ref = self._materialize_result(result_table, group)
        residual_ms = (time.monotonic() - residual_start) * 1000

        mat_start = time.monotonic()
        exit_joint = group.exit_joints[-1] if group.exit_joints else group.joints[-1]
        materialized = False
        mat_trigger: str | None = None
        mat_stats: MaterializationStats | None = None

        for jn in group.joints:
            if jn in mat_map:
                materialized = True
                mat_trigger = mat_map[jn][0].trigger
                break

        total_materializations = 0
        if materialized:
            total_materializations = 1
            mat_stats = _compute_materialization_stats(result_ref.to_arrow())

        for jn in group.joints:
            materials[jn] = result_ref

        materialize_ms = (time.monotonic() - mat_start) * 1000

        check_start = time.monotonic()
        all_check_results, assertion_error, check_failures, check_warnings = await self._run_assertion_checks(
            group, joint_map, result_ref,
        )
        check_ms = (time.monotonic() - check_start) * 1000

        # Record assertion check stats
        if stats_collector is not None:
            for jn in group.joints:
                jn_checks = all_check_results.get(jn, [])
                if jn_checks:
                    passed = sum(1 for c in jn_checks if c.passed)
                    failed = sum(1 for c in jn_checks if not c.passed and c.severity == "error")
                    warned = sum(1 for c in jn_checks if not c.passed and c.severity != "error")
                    stats_collector.record_check_results(jn, "assertion", passed, failed, warned)
        step_ms = (time.monotonic() - step_start) * 1000

        timing = PhasedTiming(
            total_ms=step_ms, engine_ms=engine_ms,
            materialize_ms=materialize_ms, residual_ms=residual_ms,
            check_ms=check_ms,
        )

        rows_out = result_ref.row_count
        rows_in = 0
        for jn in group.entry_joints or group.joints:
            cj = joint_map.get(jn)
            if cj:
                for up in cj.upstream:
                    if up in materials:
                        rows_in += materials[up].row_count
        group_success = not assertion_error

        for jn in group.joints:
            jn_mat = materialized and jn == exit_joint
            joint_results.append(
                JointExecutionResult(
                    name=jn, success=group_success,
                    rows_in=rows_in if jn == (group.entry_joints or group.joints)[0] else None,
                    rows_out=rows_out, timing=timing,
                    fused_group_id=group.id,
                    materialized=jn_mat,
                    materialization_trigger=mat_trigger if jn_mat else None,
                    materialization_stats=mat_stats if jn_mat else None,
                    check_results=all_check_results.get(jn, []),
                    plugin_metrics=None, error=None,
                )
            )

        group_results.append(
            FusedGroupExecutionResult(
                group_id=group.id, joints=list(group.joints),
                success=group_success, rows_in=rows_in,
                rows_out=rows_out, timing=timing,
                materialization_stats=mat_stats,
                plugin_metrics=None, error=None,
            )
        )

        # Record stats via StatsCollector
        if stats_collector is not None:
            stats_collector.record_group_timing(
                group.id, list(group.joints), timing, success=group_success,
            )
            for jn in group.joints:
                jn_mat = materialized and jn == exit_joint
                jn_rows_in = rows_in if jn == (group.entry_joints or group.joints)[0] else None
                stats_collector.record_joint_stats(
                    jn,
                    rows_in=jn_rows_in,
                    rows_out=rows_out,
                    timing=timing,
                    materialization_stats=mat_stats if jn_mat else None,
                )

        total_failures = 0
        stop = False
        if assertion_error:
            total_failures = 1
            for jn in group.joints:
                failed_joints.add(jn)
            if fail_fast:
                stop = True
                return total_materializations, total_failures, check_failures, check_warnings, stop

        af, aw = await self._run_sink_audits(
            group, joint_map, result_ref, catalog_map,
            assertion_error, joint_results,
        )
        check_failures += af
        check_warnings += aw

        # Record audit check stats
        if stats_collector is not None:
            for jr in joint_results:
                if jr.fused_group_id == group.id:
                    audit_checks = [c for c in jr.check_results if c.phase == "audit"]
                    if audit_checks:
                        passed = sum(1 for c in audit_checks if c.passed)
                        failed = sum(1 for c in audit_checks if not c.passed and c.severity == "error")
                        warned = sum(1 for c in audit_checks if not c.passed and c.severity != "error")
                        read_back_rows = next((c.read_back_rows for c in audit_checks if c.read_back_rows is not None), None)
                        stats_collector.record_check_results(jr.name, "audit", passed, failed, warned, read_back_rows=read_back_rows)

        return total_materializations, total_failures, check_failures, check_warnings, stop

    @staticmethod
    def _build_execution_result(
        joint_results: list[JointExecutionResult],
        group_results: list[FusedGroupExecutionResult],
        total_ms: float,
        total_materializations: int,
        total_failures: int,
        total_check_failures: int,
        total_check_warnings: int,
        fail_fast: bool,
        run_stats: RunStats | None = None,
    ) -> ExecutionResult:
        """Build the final ExecutionResult from accumulated state."""
        if total_failures == 0:
            status = "success"
        elif fail_fast:
            status = "failure"
        else:
            any_success = any(jr.success for jr in joint_results)
            status = "partial_failure" if any_success else "failure"

        return ExecutionResult(
            success=total_failures == 0,
            status=status,
            joint_results=joint_results,
            group_results=group_results,
            total_time_ms=total_ms,
            total_materializations=total_materializations,
            total_failures=total_failures,
            total_check_failures=total_check_failures,
            total_check_warnings=total_check_warnings,
            run_stats=run_stats,
        )

    async def run(
        self, compiled: CompiledAssembly, fail_fast: bool = True
    ) -> ExecutionResult:
        """Execute the compiled assembly using a wavefront parallel scheduler.

        Builds a dependency graph from fused groups, creates per-engine
        concurrency pools, and schedules groups for concurrent execution.
        Groups with all upstream dependencies satisfied are submitted in
        parallel, constrained by engine concurrency limits.

        Raises CompilationError if compiled.success is False.

        fail_fast=True: stop scheduling new groups on first failure,
            let running groups complete.
        fail_fast=False: continue independent branches, skip downstream of
            failed joints, produce ErrorMaterial, accumulate all errors,
            set "partial_failure" status.
        """
        if not compiled.success:
            raise CompilationError(compiled.errors)

        start_time = time.monotonic()
        stats_collector = StatsCollector()

        joint_map: dict[str, CompiledJoint] = {cj.name: cj for cj in compiled.joints}
        group_map: dict[str, FusedGroup] = {g.id: g for g in compiled.fused_groups}
        mat_map: dict[str, list[Materialization]] = {}
        for m in compiled.materializations:
            mat_map.setdefault(m.from_joint, []).append(m)
        catalog_map: dict[str, CompiledCatalog] = {
            cc.name: cc for cc in compiled.catalogs
        }

        materials: dict[str, MaterializedRef] = {}
        failed_joints: set[str] = set()
        joint_results: list[JointExecutionResult] = []
        group_results: list[FusedGroupExecutionResult] = []
        total_materializations = 0
        total_failures = 0
        total_check_failures = 0
        total_check_warnings = 0
        pipeline_stopped = False

        # --- Build dependency graph ---
        dep_graph = DependencyGraph.build(compiled.fused_groups, joint_map)

        # --- Resolve concurrency limits and create engine pools ---
        # Collect unique engine names from fused groups, look up ComputeEngine
        # instances from the registry.
        unique_engines: dict[str, ComputeEngine] = {}
        for g in compiled.fused_groups:
            if g.engine not in unique_engines and self._registry:
                engine_instance = self._registry.get_compute_engine(g.engine)
                if engine_instance is not None:
                    unique_engines[g.engine] = engine_instance

        # Build list of ComputeEngine objects for concurrency limit resolution.
        # For engines not found in the registry, create a minimal ComputeEngine
        # so _resolve_concurrency_limits can still apply defaults.
        engine_list: list[ComputeEngine] = []
        for g in compiled.fused_groups:
            if g.engine not in {e.name for e in engine_list}:
                if g.engine in unique_engines:
                    engine_list.append(unique_engines[g.engine])
                else:
                    engine_list.append(
                        ComputeEngine(name=g.engine, engine_type=g.engine_type, config={})
                    )

        if self._registry:
            concurrency_limits = _resolve_concurrency_limits(engine_list, self._registry)
        else:
            concurrency_limits = {e.name: 1 for e in engine_list}

        engine_pools: dict[str, EngineConcurrencyPool] = {
            engine_name: EngineConcurrencyPool(engine_name, limit)
            for engine_name, limit in concurrency_limits.items()
        }

        # --- Per-group coroutine ---
        async def _run_group(
            group_id: str,
        ) -> tuple[str, bool, int, int, int, int]:
            """Execute a single group within its engine's concurrency pool.

            Returns (group_id, success, materializations, failures,
                     check_failures, check_warnings).
            """
            group = group_map[group_id]
            pool = engine_pools.get(group.engine)

            async with pool if pool is not None else _nullcontext():
                # Check for upstream failures (between awaits, safe)
                if self._has_upstream_failure(group, joint_map, failed_joints):
                    error = RivetError(
                        code="RVT-501",
                        message="Skipped: upstream dependency failed.",
                        context={"group_id": group.id, "joints": group.joints},
                        remediation="Fix the upstream failure first.",
                    )
                    self._record_group_failure(
                        group, error, failed_joints, joint_results, group_results,
                        stats_collector=stats_collector,
                    )
                    return group_id, False, 0, 1, 0, 0

                step_start = time.monotonic()
                engine_start = time.monotonic()

                try:
                    mats, fails, cf, cw, stop = await self._execute_group_success(
                        group, joint_map, catalog_map, mat_map, materials,
                        failed_joints, joint_results, group_results, fail_fast,
                        step_start, engine_start,
                        stats_collector=stats_collector,
                    )
                    return group_id, (fails == 0), mats, fails, cf, cw

                except Exception as e:
                    step_ms = (time.monotonic() - step_start) * 1000
                    error = RivetError(
                        code="RVT-501",
                        message=f"Execution failed for group '{group.id}': {e}",
                        context={"group_id": group.id, "joints": group.joints},
                        remediation="Check the SQL and upstream data.",
                    )
                    self._record_group_failure(
                        group, error, failed_joints, joint_results, group_results,
                        step_ms=step_ms,
                        stats_collector=stats_collector,
                    )
                    return group_id, False, 0, 1, 0, 0

        # --- Wavefront scheduling loop ---
        pending_tasks: dict[asyncio.Task[tuple[str, bool, int, int, int, int]], str] = {}

        # Seed with initially ready groups
        for gid in dep_graph.ready_groups():
            dep_graph._submitted.add(gid)
            task = asyncio.create_task(_run_group(gid))
            pending_tasks[task] = gid

        while pending_tasks:
            # Wait for at least one task to complete
            done, _ = await asyncio.wait(
                pending_tasks.keys(), return_when=asyncio.FIRST_COMPLETED
            )

            # Process ALL completed tasks before checking pipeline_stopped.
            # This ensures running tasks that finished in the same batch
            # have their results recorded.
            for task in done:
                completed_gid = pending_tasks.pop(task)

                try:
                    _gid, success, mats, fails, cf, cw = task.result()
                except asyncio.CancelledError:
                    # Task was cancelled (fail-fast); don't record
                    continue
                except Exception:
                    # Unexpected error in the coroutine wrapper itself
                    success = False
                    mats, fails, cf, cw = 0, 1, 0, 0

                total_materializations += mats
                total_failures += fails
                total_check_failures += cf
                total_check_warnings += cw

                newly_ready = dep_graph.mark_complete(completed_gid)

                if not success and fail_fast:
                    pipeline_stopped = True
                elif not success and not fail_fast:
                    # Non-fail-fast: skip transitive downstream dependents,
                    # continue independent branches.
                    downstream_ids = dep_graph.mark_failed(completed_gid)
                    for ds_gid in downstream_ids:
                        ds_group = group_map.get(ds_gid)
                        if ds_group is not None:
                            error = RivetError(
                                code="RVT-501",
                                message=(
                                    f"Skipped: upstream dependency '{completed_gid}' failed."
                                ),
                                context={
                                    "group_id": ds_gid,
                                    "joints": list(ds_group.joints),
                                    "failed_upstream": completed_gid,
                                },
                                remediation="Fix the upstream failure first.",
                            )
                            self._record_group_failure(
                                ds_group, error, failed_joints,
                                joint_results, group_results,
                                stats_collector=stats_collector,
                            )
                            total_failures += 1
                        # Mark as submitted so ready_groups() won't return them
                        dep_graph._submitted.add(ds_gid)

                # When pipeline is stopped, don't schedule new groups
                # but continue processing remaining done tasks
                if not pipeline_stopped:
                    # Schedule newly ready groups
                    for new_gid in newly_ready:
                        if new_gid not in dep_graph._submitted:
                            dep_graph._submitted.add(new_gid)
                            new_task = asyncio.create_task(_run_group(new_gid))
                            pending_tasks[new_task] = new_gid

            if pipeline_stopped:
                # Cancel all pending tasks (not yet running or waiting
                # for semaphore). cancel() is a no-op on already-done tasks.
                for t in list(pending_tasks.keys()):
                    t.cancel()
                # Wait for cancelled/running tasks to settle, then collect
                # results from tasks that completed (were already running).
                if pending_tasks:
                    await asyncio.wait(pending_tasks.keys())
                    for t in list(pending_tasks.keys()):
                        try:
                            _gid, success, mats, fails, cf, cw = t.result()
                        except asyncio.CancelledError:
                            # Cancelled task — don't record in results
                            continue
                        except Exception:
                            success = False
                            mats, fails, cf, cw = 0, 1, 0, 0
                        total_materializations += mats
                        total_failures += fails
                        total_check_failures += cf
                        total_check_warnings += cw
                    pending_tasks.clear()
                break

        total_ms = (time.monotonic() - start_time) * 1000
        run_stats = stats_collector.build_run_stats(total_ms)

        return self._build_execution_result(
            joint_results, group_results, total_ms,
            total_materializations, total_failures,
            total_check_failures, total_check_warnings, fail_fast,
            run_stats=run_stats,
        )
    async def run_query(
        self, compiled: CompiledAssembly, target_joint: str = "__query"
    ) -> pyarrow.Table:
        """Execute a compiled assembly and return the target joint's result table.

        Runs the execution pipeline without sink writes or audits, capturing
        intermediate materials to return the target joint's output.

        Raises:
            CompilationError: If compiled.success is False.
            ExecutionError: If execution fails or target joint not found.
        """
        if not compiled.success:
            raise CompilationError(compiled.errors)

        joint_map = {cj.name: cj for cj in compiled.joints}
        group_map = {g.id: g for g in compiled.fused_groups}
        catalog_map = {cc.name: cc for cc in compiled.catalogs}
        materials: dict[str, MaterializedRef] = {}

        for step in compiled.execution_order:
            group = group_map.get(step)
            if group is None:
                continue
            arrow_materials: dict[str, pyarrow.Table] = {
                k: v.to_arrow() for k, v in materials.items()
            }
            result_ref, adapter_residual = await self._execute_fused_group(
                group, arrow_materials, joint_map, catalog_map, ref_materials=materials
            )
            merged_residual = _merge_residuals(group.residual, adapter_residual) if adapter_residual else group.residual
            if merged_residual is not None:
                result_table = _apply_residuals(result_ref.to_arrow(), merged_residual)
                result_ref = self._materialize_result(result_table, group)
            for jn in group.joints:
                materials[jn] = result_ref

        if target_joint in materials:
            return materials[target_joint].to_arrow()

        raise ExecutionError(
            RivetError(
                code="RVT-501",
                message=f"Target joint '{target_joint}' not found in execution results.",
                context={"target_joint": target_joint, "available": list(materials.keys())},
                remediation="Check that the target joint name is correct.",
            )
        )

    async def run_query_with_stats(
        self, compiled: CompiledAssembly, target_joint: str = "__query"
    ) -> tuple[pyarrow.Table, RunStats]:
        """Like run_query but also returns RunStats with timing breakdown.

        Raises:
            CompilationError: If compiled.success is False.
            ExecutionError: If execution fails or target joint not found.
        """
        if not compiled.success:
            raise CompilationError(compiled.errors)

        start_time = time.monotonic()
        stats_collector = StatsCollector()

        joint_map = {cj.name: cj for cj in compiled.joints}
        group_map = {g.id: g for g in compiled.fused_groups}
        catalog_map = {cc.name: cc for cc in compiled.catalogs}
        materials: dict[str, MaterializedRef] = {}

        for step in compiled.execution_order:
            group = group_map.get(step)
            if group is None:
                continue

            step_start = time.monotonic()
            arrow_materials: dict[str, pyarrow.Table] = {
                k: v.to_arrow() for k, v in materials.items()
            }

            engine_start = time.monotonic()
            result_ref, adapter_residual = await self._execute_fused_group(
                group, arrow_materials, joint_map, catalog_map,
                ref_materials=materials, stats_collector=stats_collector,
            )
            engine_ms = (time.monotonic() - engine_start) * 1000

            merged_residual = _merge_residuals(group.residual, adapter_residual) if adapter_residual else group.residual
            if merged_residual is not None:
                result_table = _apply_residuals(result_ref.to_arrow(), merged_residual)
                result_ref = self._materialize_result(result_table, group)

            step_ms = (time.monotonic() - step_start) * 1000
            timing = PhasedTiming(
                total_ms=step_ms, engine_ms=engine_ms,
                materialize_ms=0.0, residual_ms=0.0, check_ms=0.0,
            )
            stats_collector.record_group_timing(
                group.id, list(group.joints), timing, success=True,
            )

            for jn in group.joints:
                materials[jn] = result_ref
                rows_out = result_ref.to_arrow().num_rows if result_ref else None
                stats_collector.record_joint_stats(
                    jn, rows_in=None, rows_out=rows_out, timing=timing,
                    materialization_stats=None,
                )

        total_ms = (time.monotonic() - start_time) * 1000
        run_stats = stats_collector.build_run_stats(total_ms)

        if target_joint in materials:
            return materials[target_joint].to_arrow(), run_stats

        raise ExecutionError(
            RivetError(
                code="RVT-501",
                message=f"Target joint '{target_joint}' not found in execution results.",
                context={"target_joint": target_joint, "available": list(materials.keys())},
                remediation="Check that the target joint name is correct.",
            )
        )

    def run_sync(
        self, compiled: CompiledAssembly, fail_fast: bool = True
    ) -> ExecutionResult:
        """Synchronous entry point — creates an event loop and runs the async scheduler."""
        return asyncio.run(self.run(compiled, fail_fast=fail_fast))

    def run_query_sync(
        self, compiled: CompiledAssembly, target_joint: str = "__query"
    ) -> pyarrow.Table:
        """Synchronous wrapper for run_query."""
        return asyncio.run(self.run_query(compiled, target_joint))

    def run_query_with_stats_sync(
        self, compiled: CompiledAssembly, target_joint: str = "__query"
    ) -> tuple[pyarrow.Table, RunStats]:
        """Synchronous wrapper for run_query_with_stats."""
        return asyncio.run(self.run_query_with_stats(compiled, target_joint))


    async def _run_audits(
        self,
        cj: CompiledJoint,
        audit_checks: list[CompiledCheck],
        written_table: pyarrow.Table,
        catalog_map: dict[str, CompiledCatalog],
    ) -> list[CheckExecutionResult]:
        """Run audit checks after sink write by reading back from target catalog.

        Audit read-back failure produces RVT-670. Audit failures are reported
        but do not rollback the write.
        """
        results: list[CheckExecutionResult] = []

        # Attempt to read back from catalog via registry
        read_back_table: pyarrow.Table | None = None
        if self._registry and cj.catalog:
            cc = catalog_map.get(cj.catalog)
            if cc:
                source = self._registry._sources.get(cc.type)
                if source:
                    try:
                        from rivet_core.models import Catalog, Joint

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
                        pass

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

    async def _execute_fused_group(
        self,
        group: FusedGroup,
        materials: dict[str, pyarrow.Table],
        joint_map: dict[str, CompiledJoint],
        catalog_map: dict[str, CompiledCatalog] | None = None,
        ref_materials: dict[str, MaterializedRef] | None = None,
        stats_collector: StatsCollector | None = None,
    ) -> tuple[MaterializedRef, ResidualPlan | None]:
        """Dispatch a fused group for execution and materialize the result.

        Single dispatch path: all non-Python groups go through plugin.execute_sql.
        Python joints are dispatched to _execute_python_joint.
        Returns a (MaterializedRef, adapter_residual) tuple.
        """
        # PythonJoint groups are always standalone (single joint)
        if len(group.joints) == 1:
            cj = joint_map.get(group.joints[0])
            if cj and cj.type == "python":
                return await self._execute_python_joint(cj, ref_materials or {}), None

        # Look up engine plugin — required for all engine types
        plugin = self._registry.get_engine_plugin(group.engine_type) if self._registry else None
        if plugin is None:
            raise ExecutionError(
                RivetError(
                    code="RVT-501",
                    message=f"No plugin registered for engine type '{group.engine_type}'",
                    context={
                        "engine_type": group.engine_type,
                        "group_id": group.id,
                        "joints": group.joints,
                    },
                    remediation=f"Register a ComputeEnginePlugin for '{group.engine_type}'.",
                )
            )

        # Single dispatch path: resolve upstream, build input_tables, call execute_sql
        result_table, adapter_residual = await self._execute_via_plugin(group, materials, joint_map, catalog_map, plugin, stats_collector=stats_collector)
        return self._materialize_result(result_table, group), adapter_residual

    def _materialize_result(self, table: pyarrow.Table, group: FusedGroup) -> MaterializedRef:
        """Materialize a pyarrow.Table using the group's materialization strategy."""
        exit_joint = group.exit_joints[-1] if group.exit_joints else group.joints[-1]
        strategy = self._get_materialization_strategy(group.materialization_strategy_name)
        return strategy.materialize(
            table,
            MaterializationContext(
                joint_name=exit_joint,
                strategy_name=group.materialization_strategy_name,
                options={},
            ),
        )

    # CLEANUP-RISK: _execute_via_plugin (complexity 13) — plugin dispatch with error handling; refactoring risks changing error messages/types
    @staticmethod
    def _resolve_sql_for_execution(
        group: FusedGroup,
        joint_map: dict[str, CompiledJoint],
        adapter_read_sources: set[str],
        has_materialized_inputs: bool = False,
    ) -> str | None:
        """Resolve the SQL string for CTE-strategy execution.

        Rewrites adapter-backed source CTE bodies to ``SELECT * FROM <name>``
        so the engine reads from the registered Arrow table (input_tables).
        When a reference resolver has produced resolved SQL, that is returned
        directly since it already contains fully-qualified table references.
        Falls back through fusion_result and group-level SQL attributes.

        When *has_materialized_inputs* is True (upstream data was materialized
        across an engine boundary), resolved SQL is skipped because the
        receiving engine works with in-memory tables registered by joint name,
        not catalog-qualified references.
        """
        # Prefer resolved SQL (reference-resolver output) when available
        # AND the engine can natively resolve catalog references (no
        # materialized inputs from an engine boundary).
        if not has_materialized_inputs:
            resolved = None
            if group.fusion_result:
                resolved = group.fusion_result.resolved_fused_sql
            if resolved is None:
                resolved = group.resolved_sql
            if resolved is not None:
                return resolved

        # No reference resolver ran — rewrite adapter-read sources as CTEs
        # so the engine can resolve them from input_tables.
        sql: str | None = None
        if adapter_read_sources and len(group.joints) > 1:
            from rivet_core.optimizer import _compose_cte

            rewritten_joint_sql: dict[str, str | None] = {}
            for jn in group.joints:
                cj = joint_map.get(jn)
                if jn in adapter_read_sources:
                    rewritten_joint_sql[jn] = f"SELECT * FROM {jn}"
                elif cj:
                    rewritten_joint_sql[jn] = cj.sql_resolved or cj.sql_translated or cj.sql
                else:
                    rewritten_joint_sql[jn] = None
            rewritten = _compose_cte(group.joints, rewritten_joint_sql)
            if rewritten:
                sql = rewritten.fused_sql
        if sql is None and group.fusion_result:
            sql = group.fusion_result.fused_sql
        if sql is None:
            sql = group.fused_sql
        return sql

    @staticmethod
    async def _dispatch_to_engine(
        plugin: ComputeEnginePlugin,
        engine_instance: ComputeEngine | None,
        sql: str,
        input_tables: dict[str, pyarrow.Table],
        group: FusedGroup,
    ) -> pyarrow.Table:
        """Call plugin.execute_sql, wrapping non-ExecutionError failures as RVT-503."""
        try:
            return await asyncio.to_thread(plugin.execute_sql, engine_instance, sql, input_tables)  # type: ignore[arg-type]
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(  # noqa: B904
                RivetError(
                    code="RVT-503",
                    message=f"Plugin execute_sql failed for group '{group.id}': {exc}",
                    context={
                        "engine_type": group.engine_type,
                        "group_id": group.id,
                        "sql": (sql or "")[:200],
                    },
                    remediation="Check engine connectivity and SQL syntax.",
                )
            )

    async def _execute_via_plugin(
        self,
        group: FusedGroup,
        materials: dict[str, pyarrow.Table],
        joint_map: dict[str, CompiledJoint],
        catalog_map: dict[str, CompiledCatalog] | None,
        plugin: ComputeEnginePlugin,
        stats_collector: StatsCollector | None = None,
    ) -> tuple[pyarrow.Table, ResidualPlan | None]:
        """Execute via plugin's execute_sql with cross-joint adapter resolution.

        Handles both CTE and temp_view fusion strategies.
        Resolves upstream references, builds input_tables, reads sources,
        and delegates SQL execution to the engine plugin.
        Returns (result_table, adapter_residual).
        Raises RVT-503 if plugin.execute_sql fails.
        """
        engine_instance = (
            self._registry.get_compute_engine(group.engine)
            if self._registry
            else None
        )

        # Build input_tables from upstream materials and source reads
        input_tables: dict[str, pyarrow.Table] = {}
        has_materialized_inputs = False

        for jn in group.joints:
            cj = joint_map.get(jn)
            if not cj:
                continue
            for up in cj.upstream:
                if up in materials:
                    input_tables[up] = materials[up]
                    has_materialized_inputs = True

        # Read source joints into input_tables.
        # When a reference resolver has rewritten the fused SQL (resolved_sql
        # is set), table references are fully-qualified catalog names and the
        # engine does not need adapter-read data in input_tables.  Skip adapter
        # reads for fused sources to avoid redundant queries.
        group_has_resolved_sql = (
            group.resolved_sql is not None
            or (group.fusion_result is not None and group.fusion_result.resolved_fused_sql is not None)
        )
        skip_source_reads = group_has_resolved_sql and len(group.joints) > 1
        adapter_residual = await self._read_sources_into(
            input_tables, group, joint_map, catalog_map,
            stats_collector=stats_collector,
            skip_fused_sources=skip_source_reads,
        )

        adapter_read_sources = {
            jn for jn in group.joints
            if jn in input_tables and joint_map.get(jn) and joint_map[jn].type == "source" and joint_map[jn].adapter
        }

        # Handle temp_view strategy: execute intermediate statements, then final select
        if group.fusion_strategy == "temp_view" and group.fusion_result:
            return await self._execute_temp_view_via_plugin(
                group, input_tables, plugin, engine_instance, adapter_read_sources,
            ), adapter_residual

        # CTE strategy
        sql = self._resolve_sql_for_execution(
            group, joint_map, adapter_read_sources,
            has_materialized_inputs=has_materialized_inputs,
        )

        if not sql:
            if input_tables:
                return next(iter(input_tables.values())), adapter_residual
            return pyarrow.table({}), adapter_residual

        # Source-only groups with no input tables: return empty table
        if not input_tables and all(
            joint_map.get(jn) is not None and joint_map[jn].type == "source"
            for jn in group.joints
            if jn in joint_map
        ):
            return pyarrow.table({}), adapter_residual

        return await self._dispatch_to_engine(plugin, engine_instance, sql, input_tables, group), adapter_residual

    # CLEANUP-RISK: _execute_temp_view_via_plugin (complexity 13) — plugin dispatch with error handling; refactoring risks changing error messages/types
    async def _execute_temp_view_via_plugin(
        self,
        group: FusedGroup,
        input_tables: dict[str, pyarrow.Table],
        plugin: ComputeEnginePlugin,
        engine_instance: ComputeEngine | None,
        adapter_read_sources: set[str] | None = None,
    ) -> pyarrow.Table:
        """Execute temp_view strategy by running intermediate statements through plugin."""
        import re

        fr = group.fusion_result
        assert fr is not None, f"fusion_result must not be None for group '{group.id}'"
        statements = fr.resolved_statements or fr.statements
        final_select = fr.resolved_final_select or fr.final_select

        # Rewrite statements for adapter-backed sources: replace their view SQL
        # with SELECT * FROM <joint_name> so the engine reads from the registered table.
        if adapter_read_sources and statements:
            rewritten_stmts: list[str] = []
            for stmt in statements:
                m = re.match(
                    r"CREATE\s+TEMPORARY\s+VIEW\s+(\w+)\s+AS\s*\((.+)\)",
                    stmt,
                    re.IGNORECASE | re.DOTALL,
                )
                if m and m.group(1) in adapter_read_sources:
                    view_name = m.group(1)
                    rewritten_stmts.append(
                        f"CREATE TEMPORARY VIEW {view_name} AS (SELECT * FROM {view_name})"
                    )
                else:
                    rewritten_stmts.append(stmt)
            statements = rewritten_stmts

        if not statements:
            if not final_select:
                if input_tables:
                    return next(iter(input_tables.values()))
                return pyarrow.table({})
            try:
                return await asyncio.to_thread(plugin.execute_sql, engine_instance, final_select, input_tables)  # type: ignore[arg-type]
            except ExecutionError:
                raise
            except Exception as exc:
                raise ExecutionError(  # noqa: B904
                    RivetError(
                        code="RVT-503",
                        message=f"Plugin execute_sql failed for group '{group.id}': {exc}",
                        context={
                            "engine_type": group.engine_type,
                            "group_id": group.id,
                            "sql": (final_select or "")[:200],
                        },
                        remediation="Check engine connectivity and SQL syntax.",
                    )
                )

        # Execute intermediate view statements, storing results as input tables
        created_views: list[str] = []
        try:
            for stmt in statements:
                m = re.match(
                    r"CREATE\s+TEMPORARY\s+VIEW\s+(\w+)\s+AS\s*\((.+)\)",
                    stmt,
                    re.IGNORECASE | re.DOTALL,
                )
                if m:
                    view_name = m.group(1)
                    view_sql = m.group(2)
                    try:
                        result = await asyncio.to_thread(plugin.execute_sql, engine_instance, view_sql, input_tables)  # type: ignore[arg-type]
                    except ExecutionError:
                        raise
                    except Exception as exc:
                        raise ExecutionError(  # noqa: B904
                            RivetError(
                                code="RVT-503",
                                message=f"Plugin execute_sql failed for group '{group.id}': {exc}",
                                context={
                                    "engine_type": group.engine_type,
                                    "group_id": group.id,
                                    "sql": view_sql[:200],
                                },
                                remediation="Check engine connectivity and SQL syntax.",
                            )
                        )
                    input_tables[view_name] = result
                    created_views.append(view_name)

            try:
                return await asyncio.to_thread(plugin.execute_sql, engine_instance, final_select, input_tables)  # type: ignore[arg-type]
            except ExecutionError:
                raise
            except Exception as exc:
                raise ExecutionError(  # noqa: B904
                    RivetError(
                        code="RVT-503",
                        message=f"Plugin execute_sql failed for group '{group.id}': {exc}",
                        context={
                            "engine_type": group.engine_type,
                            "group_id": group.id,
                            "sql": (final_select or "")[:200],
                        },
                        remediation="Check engine connectivity and SQL syntax.",
                    )
                )
        finally:
            for view_name in created_views:
                input_tables.pop(view_name, None)

    def _resolve_cross_joint(
        self,
        upstream_ref: MaterializedRef,
        consumer_group: FusedGroup,
        producer_joint_name: str,
        consumer_joint_name: str,
        joint_map: dict[str, CompiledJoint],
        group_map: dict[str, FusedGroup],
    ) -> UpstreamResolution:
        """Resolve how the consumer engine accesses upstream data."""
        producer_cj = joint_map.get(producer_joint_name)
        consumer_cj = joint_map.get(consumer_joint_name)

        # Determine producer engine type via its fused group
        producer_engine_type: str | None = None
        if producer_cj and producer_cj.fused_group_id:
            producer_group = group_map.get(producer_cj.fused_group_id)
            if producer_group:
                producer_engine_type = producer_group.engine_type

        consumer_engine_type = consumer_group.engine_type

        # Same engine type or unknown producer → default arrow passthrough
        if producer_engine_type is None or producer_engine_type == consumer_engine_type:
            return UpstreamResolution(strategy="arrow_passthrough")

        # Look up registered cross-joint adapter
        adapter = (
            self._registry.get_cross_joint_adapter(consumer_engine_type, producer_engine_type)
            if self._registry
            else None
        )

        if adapter is None:
            return UpstreamResolution(strategy="arrow_passthrough")

        # Adapter found → delegate resolution
        engine_instance = (
            self._registry.get_compute_engine(consumer_group.engine)
            if self._registry
            else None
        )
        ctx = CrossJointContext(
            producer_joint_name=producer_joint_name,
            consumer_joint_name=consumer_joint_name,
            producer_catalog_type=producer_cj.catalog_type if producer_cj else None,
            producer_table=producer_cj.table if producer_cj else None,
            consumer_catalog_type=consumer_cj.catalog_type if consumer_cj else None,
        )
        return adapter.resolve_upstream(upstream_ref, engine_instance, ctx)

    async def _read_source_via_adapter(
        self,
        jn: str,
        cj: CompiledJoint,
        cat: Catalog,
        joint: Joint,
        group: FusedGroup,
    ) -> tuple[bool, pyarrow.Table | None, ResidualPlan | None]:
        """Dispatch a source read through the adapter. Returns (found, table_or_none, residual_or_none)."""
        assert cj.adapter is not None, "adapter must not be None for adapter dispatch"
        parts = cj.adapter.split(":", 1)
        engine_type = parts[0]
        catalog_type = parts[1] if len(parts) > 1 else ""
        adapter = self._registry.get_adapter(engine_type, catalog_type)
        if not adapter:
            return False, None, None
        try:
            engine_instance = self._registry.get_compute_engine(cj.engine)
            effective_pushdown = _merge_source_limit_into_pushdown(group.pushdown, cj)
            effective_pushdown = _merge_cross_group_predicates(effective_pushdown, group, jn)
            effective_pushdown = _merge_cross_group_projections(effective_pushdown, group, jn)
            effective_pushdown = _merge_cross_group_limits(effective_pushdown, group, jn)
            result = await asyncio.to_thread(adapter.read_dispatch, engine_instance, cat, joint, effective_pushdown)
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

    async def _read_source_fallback(
        self,
        jn: str,
        cj: CompiledJoint,
        cat: Catalog,
        joint: Joint,
    ) -> pyarrow.Table | None:
        """Read a source joint via the fallback source registry."""
        source = self._registry._sources.get(cj.catalog_type or "")
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

    async def _read_sources_into(
        self,
        input_tables: dict[str, pyarrow.Table],
        group: FusedGroup,
        joint_map: dict[str, CompiledJoint],
        catalog_map: dict[str, CompiledCatalog] | None,
        stats_collector: StatsCollector | None = None,
        skip_fused_sources: bool = False,
    ) -> ResidualPlan | None:
        """Read source joints from their catalogs into input_tables dict.

        Returns the merged adapter residual if any adapter returned one,
        or None if no adapter residuals were produced.

        When *skip_fused_sources* is True, source joints are skipped because
        the fused SQL already contains fully-qualified table references
        (rewritten by a reference resolver) and does not depend on
        input_tables for these sources.
        """
        merged_adapter_residual: ResidualPlan | None = None
        if not self._registry or not catalog_map:
            return None
        for jn in group.entry_joints or group.joints:
            cj = joint_map.get(jn)
            if not cj or cj.type != "source" or not cj.catalog:
                continue
            if jn in input_tables:
                continue
            if skip_fused_sources:
                continue

            from rivet_core.models import Catalog, Joint

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
                    found, tbl, residual = await self._read_source_via_adapter(jn, cj, cat, joint, group)
                except Exception as exc:
                    read_ms = (time.monotonic() - read_start) * 1000
                    if stats_collector is not None:
                        err = exc.error if isinstance(exc, ExecutionError) else RivetError(
                            code="RVT-501", message=str(exc),
                        )
                        stats_collector.record_source_read(
                            jn, adapter_name, catalog_type,
                            row_count=None, read_ms=read_ms, error=err,
                        )
                    raise
                read_ms = (time.monotonic() - read_start) * 1000
                if found:
                    row_count = tbl.num_rows if tbl is not None else None
                    has_residual = residual is not None
                    if stats_collector is not None:
                        stats_collector.record_source_read(
                            jn, adapter_name, catalog_type,
                            row_count=row_count, read_ms=read_ms,
                            has_residual=has_residual,
                        )
                    if tbl is not None:
                        input_tables[jn] = tbl
                    if residual is not None:
                        merged_adapter_residual = _merge_residuals(merged_adapter_residual, residual)
                    continue

            try:
                tbl = await self._read_source_fallback(jn, cj, cat, joint)
            except Exception as exc:
                read_ms = (time.monotonic() - read_start) * 1000
                if stats_collector is not None:
                    err = exc.error if isinstance(exc, ExecutionError) else RivetError(
                        code="RVT-501", message=str(exc),
                    )
                    stats_collector.record_source_read(
                        jn, adapter_name, catalog_type,
                        row_count=None, read_ms=read_ms, error=err,
                    )
                raise
            read_ms = (time.monotonic() - read_start) * 1000
            if tbl is not None:
                if stats_collector is not None:
                    stats_collector.record_source_read(
                        jn, adapter_name, catalog_type,
                        row_count=tbl.num_rows, read_ms=read_ms,
                    )
                input_tables[jn] = tbl
        return merged_adapter_residual

    async def _execute_python_joint(
        self,
        cj: CompiledJoint,
        materials: dict[str, MaterializedRef],
    ) -> MaterializedRef:
        """Execute a PythonJoint by calling its referenced function.

        Builds Material inputs from upstream MaterializedRef directly (no
        materialize() round-trip), supports single-input shorthand,
        optional RivetContext, async functions, and normalizes return to Arrow.
        Returns a MaterializedRef.
        """
        # Import the callable
        func_path = cj.function or ""
        mod_path, func_name = func_path.rsplit(".", 1)
        try:
            mod = importlib.import_module(mod_path)
            func = getattr(mod, func_name)
        except Exception as exc:
            raise ExecutionError(  # noqa: B904
                RivetError(
                    code="RVT-751",
                    message=f"PythonJoint '{cj.name}' failed to import '{func_path}': {exc}",
                    context={"joint": cj.name, "function": func_path,
                             "traceback": traceback.format_exc()},
                    remediation="Ensure the function path is importable.",
                )
            )

        # Build Material inputs from upstream — use MaterializedRef directly
        inputs: dict[str, Material] = {}
        for up in cj.upstream:
            ref = materials.get(up)
            if ref is not None:
                inputs[up] = Material(name=up, catalog=cj.catalog or "", materialized_ref=ref, state="materialized")

        # Determine if function accepts RivetContext
        sig = inspect.signature(func)
        params = list(sig.parameters.values())
        wants_context = any(
            p.annotation is RivetContext
            or (isinstance(p.annotation, str) and "RivetContext" in p.annotation)
            for p in params
        )

        # Build call args
        try:
            if len(cj.upstream) == 1 and len(inputs) == 1:
                # Single-input shorthand
                single_material = next(iter(inputs.values()))
                args: tuple[Any, ...] = (single_material,)
            else:
                args = (inputs,)

            kwargs: dict[str, Any] = {}
            if wants_context:
                kwargs["context"] = RivetContext(joint_name=cj.name)

            # Support async functions
            if inspect.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = await asyncio.to_thread(func, *args, **kwargs)
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(  # noqa: B904
                RivetError(
                    code="RVT-751",
                    message=f"PythonJoint '{cj.name}' raised an exception: {exc}",
                    context={"joint": cj.name, "function": func_path,
                             "upstream": cj.upstream,
                             "traceback": traceback.format_exc()},
                    remediation="Fix the Python function or check upstream data.",
                )
            )

        # Normalize return to Arrow and materialize
        table = _normalize_python_result(cj.name, func_path, result)
        return self._get_materialization_strategy("arrow").materialize(
            table, MaterializationContext(joint_name=cj.name, strategy_name="arrow", options={})
        )

    async def _dispatch_sink_write(
        self,
        cj: CompiledJoint,
        ref: MaterializedRef,
        catalog_map: dict[str, CompiledCatalog],
    ) -> None:
        """Dispatch sink write via registry SinkPlugin.

        Uses CompiledJoint.engine and catalog without re-resolution.
        Passes MaterializedRef in Material so sink can inspect storage_type
        without forcing .to_arrow().
        """
        if not self._registry or not cj.catalog:
            return
        cc = catalog_map.get(cj.catalog)
        if not cc:
            return
        sink = self._registry._sinks.get(cc.type)
        if not sink:
            return
        try:
            from rivet_core.models import Catalog, Joint

            cat = Catalog(name=cc.name, type=cc.type, options=cc.options)
            mat = Material(
                name=cj.name, catalog=cj.catalog,
                table=cj.table, materialized_ref=ref, state="materialized",
            )
            joint = Joint(name=cj.name, joint_type="sink", catalog=cj.catalog, table=cj.table)
            await asyncio.to_thread(sink.write, cat, joint, mat, cj.write_strategy or "replace")
        except Exception:
            pass  # Write failures handled by audit phase
