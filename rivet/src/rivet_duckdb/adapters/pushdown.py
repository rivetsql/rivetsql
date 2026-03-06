"""Shared DuckDB pushdown helper: SQL clause injection from PushdownPlan."""

from __future__ import annotations

from rivet_core.optimizer import (
    Cast,
    PushdownPlan,
    ResidualPlan,
)
from rivet_core.sql_parser import Predicate

EMPTY_RESIDUAL = ResidualPlan(predicates=[], limit=None, casts=[])


def _apply_duckdb_pushdown(
    base_sql: str,
    pushdown: PushdownPlan | None,
) -> tuple[str, ResidualPlan]:
    """Apply pushdown operations to a DuckDB SQL query.

    Returns (modified_sql, residual) where residual contains any operations
    that could not be applied.
    """
    if pushdown is None:
        return base_sql, EMPTY_RESIDUAL

    sql = base_sql
    residual_predicates: list[Predicate] = list(pushdown.predicates.residual)
    residual_casts: list[Cast] = list(pushdown.casts.residual)
    residual_limit: int | None = pushdown.limit.residual_limit

    # Projection: replace SELECT * with column list
    if pushdown.projections.pushed_columns is not None:
        try:
            cols = ", ".join(pushdown.projections.pushed_columns)
            sql = sql.replace("SELECT *", f"SELECT {cols}", 1)
        except Exception:
            pass  # full columns already present, no residual needed

    # Predicates: wrap in subquery with WHERE clause
    if pushdown.predicates.pushed:
        where_parts: list[str] = []
        for pred in pushdown.predicates.pushed:
            try:
                where_parts.append(pred.expression)
            except Exception:
                residual_predicates.append(pred)
        if where_parts:
            where_clause = " AND ".join(where_parts)
            sql = f"SELECT * FROM ({sql}) AS __pd WHERE {where_clause}"

    # Limit
    if pushdown.limit.pushed_limit is not None:
        try:
            sql = f"{sql} LIMIT {pushdown.limit.pushed_limit}"
        except Exception:
            residual_limit = pushdown.limit.pushed_limit

    # Casts: wrap columns with CAST(col AS type)
    for cast in pushdown.casts.pushed:
        try:
            sql = sql.replace(cast.column, f"CAST({cast.column} AS {cast.to_type})")
        except Exception:
            residual_casts.append(cast)

    residual = ResidualPlan(
        predicates=residual_predicates,
        limit=residual_limit,
        casts=residual_casts,
    )
    return sql, residual
