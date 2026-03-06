"""Shared PySpark pushdown helper: DataFrame API operations from PushdownPlan."""

from __future__ import annotations

from typing import Any

from rivet_core.optimizer import Cast, PushdownPlan, ResidualPlan
from rivet_core.sql_parser import Predicate

EMPTY_RESIDUAL = ResidualPlan(predicates=[], limit=None, casts=[])


def _apply_pyspark_pushdown(
    df: Any,
    pushdown: PushdownPlan | None,
) -> tuple[Any, ResidualPlan]:
    """Apply pushdown operations to a PySpark DataFrame.

    Returns (modified_df, residual) where residual contains any operations
    that could not be applied.
    """
    if pushdown is None:
        return df, EMPTY_RESIDUAL

    residual_predicates: list[Predicate] = list(pushdown.predicates.residual)
    residual_casts: list[Cast] = list(pushdown.casts.residual)
    residual_limit: int | None = pushdown.limit.residual_limit

    # Projection: df.select(*columns)
    if pushdown.projections.pushed_columns is not None:
        try:
            df = df.select(*pushdown.projections.pushed_columns)
        except Exception:
            pass  # full columns already present, no residual needed

    # Predicates: df.filter(expression)
    for pred in pushdown.predicates.pushed:
        try:
            df = df.filter(pred.expression)
        except Exception:
            residual_predicates.append(pred)

    # Limit: df.limit(n)
    if pushdown.limit.pushed_limit is not None:
        try:
            df = df.limit(pushdown.limit.pushed_limit)
        except Exception:
            residual_limit = pushdown.limit.pushed_limit

    # Casts: df.withColumn(col, F.col(col).cast(type))
    if pushdown.casts.pushed:
        from pyspark.sql import functions as F

        for cast in pushdown.casts.pushed:
            try:
                df = df.withColumn(cast.column, F.col(cast.column).cast(cast.to_type))
            except Exception:
                residual_casts.append(cast)

    residual = ResidualPlan(
        predicates=residual_predicates,
        limit=residual_limit,
        casts=residual_casts,
    )
    return df, residual
