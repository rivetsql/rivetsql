"""Shared Polars pushdown helper for all Polars adapters."""

from __future__ import annotations

import polars as pl

from rivet_core.optimizer import Cast, PushdownPlan, ResidualPlan
from rivet_core.sql_parser import Predicate


def _apply_polars_pushdown(
    df: pl.DataFrame | pl.LazyFrame,
    pushdown: PushdownPlan,
) -> tuple[pl.DataFrame | pl.LazyFrame, ResidualPlan]:
    """Apply pushdown operations to a Polars DataFrame/LazyFrame.

    Each operation is wrapped in try/except; failures move to residual.
    Returns (modified_df, residual_plan).
    """
    residual_predicates: list[Predicate] = []
    residual_casts: list[Cast] = []
    residual_limit: int | None = None

    # Projection
    if pushdown.projections.pushed_columns is not None:
        try:
            df = df.select(pushdown.projections.pushed_columns)
        except Exception:
            pass  # full columns already present, no residual needed

    # Predicates
    for pred in pushdown.predicates.pushed:
        try:
            df = df.filter(pl.Expr.deserialize(pred.expression.encode(), format="json"))
        except Exception:
            try:
                df = df.filter(pl.sql_expr(pred.expression))
            except Exception:
                residual_predicates.append(pred)

    # Limit
    if pushdown.limit.pushed_limit is not None:
        try:
            df = df.head(pushdown.limit.pushed_limit)
        except Exception:
            residual_limit = pushdown.limit.pushed_limit

    # Casts
    for cast in pushdown.casts.pushed:
        try:
            pl_type = _map_type(cast.to_type)
            df = df.with_columns(pl.col(cast.column).cast(pl_type))
        except Exception:
            residual_casts.append(cast)

    residual = ResidualPlan(
        predicates=residual_predicates + list(pushdown.predicates.residual),
        limit=residual_limit if residual_limit is not None else pushdown.limit.residual_limit,
        casts=residual_casts + list(pushdown.casts.residual),
    )
    return df, residual


_TYPE_MAP: dict[str, pl.DataType] = {
    "int8": pl.Int8(),
    "int16": pl.Int16(),
    "int32": pl.Int32(),
    "int64": pl.Int64(),
    "uint8": pl.UInt8(),
    "uint16": pl.UInt16(),
    "uint32": pl.UInt32(),
    "uint64": pl.UInt64(),
    "float32": pl.Float32(),
    "float64": pl.Float64(),
    "boolean": pl.Boolean(),
    "bool": pl.Boolean(),
    "utf8": pl.Utf8(),
    "string": pl.Utf8(),
    "date": pl.Date(),
    "datetime": pl.Datetime(),
    "timestamp": pl.Datetime(),
    "time": pl.Time(),
    "duration": pl.Duration(),
    "binary": pl.Binary(),
}


def _map_type(type_str: str) -> pl.DataType:
    """Map a type string to a Polars DataType. Raises KeyError on unknown types."""
    key = type_str.strip().lower()
    if key in _TYPE_MAP:
        return _TYPE_MAP[key]
    # Try polars eval as fallback
    return getattr(pl, type_str)()  # type: ignore[no-any-return]
