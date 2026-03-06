"""Unit tests for _apply_duckdb_pushdown SQL generation."""

from __future__ import annotations

from rivet_core.optimizer import (
    Cast,
    CastPushdownResult,
    LimitPushdownResult,
    PredicatePushdownResult,
    ProjectionPushdownResult,
    PushdownPlan,
)
from rivet_core.sql_parser import Predicate
from rivet_duckdb.adapters.pushdown import EMPTY_RESIDUAL, _apply_duckdb_pushdown


def _make_pushdown(
    *,
    columns: list[str] | None = None,
    predicates: list[Predicate] | None = None,
    residual_predicates: list[Predicate] | None = None,
    limit: int | None = None,
    residual_limit: int | None = None,
    casts: list[Cast] | None = None,
    residual_casts: list[Cast] | None = None,
) -> PushdownPlan:
    return PushdownPlan(
        projections=ProjectionPushdownResult(pushed_columns=columns, reason=None),
        predicates=PredicatePushdownResult(
            pushed=predicates or [], residual=residual_predicates or [],
        ),
        limit=LimitPushdownResult(
            pushed_limit=limit, residual_limit=residual_limit, reason=None,
        ),
        casts=CastPushdownResult(
            pushed=casts or [], residual=residual_casts or [],
        ),
    )


BASE_SQL = "SELECT * FROM read_parquet('s3://bucket/data.parquet')"


class TestNonePushdown:
    def test_returns_original_sql_and_empty_residual(self):
        sql, residual = _apply_duckdb_pushdown(BASE_SQL, None)
        assert sql == BASE_SQL
        assert residual == EMPTY_RESIDUAL


class TestEmptyPushdown:
    def test_returns_original_sql_and_empty_residual(self):
        pushdown = _make_pushdown()
        sql, residual = _apply_duckdb_pushdown(BASE_SQL, pushdown)
        assert sql == BASE_SQL
        assert residual == EMPTY_RESIDUAL


class TestProjectionPushdown:
    def test_replaces_select_star_with_columns(self):
        pushdown = _make_pushdown(columns=["id", "name"])
        sql, residual = _apply_duckdb_pushdown(BASE_SQL, pushdown)
        assert sql == "SELECT id, name FROM read_parquet('s3://bucket/data.parquet')"
        assert residual == EMPTY_RESIDUAL

    def test_replaces_only_first_select_star(self):
        base = "SELECT * FROM (SELECT * FROM t)"
        pushdown = _make_pushdown(columns=["a"])
        sql, _ = _apply_duckdb_pushdown(base, pushdown)
        assert sql == "SELECT a FROM (SELECT * FROM t)"


class TestPredicatePushdown:
    def test_single_predicate(self):
        pred = Predicate(expression="age > 18", columns=["age"], location="where")
        pushdown = _make_pushdown(predicates=[pred])
        sql, residual = _apply_duckdb_pushdown(BASE_SQL, pushdown)
        assert sql == f"SELECT * FROM ({BASE_SQL}) AS __pd WHERE age > 18"
        assert residual.predicates == []

    def test_multiple_predicates(self):
        p1 = Predicate(expression="age > 18", columns=["age"], location="where")
        p2 = Predicate(expression="status = 'active'", columns=["status"], location="where")
        pushdown = _make_pushdown(predicates=[p1, p2])
        sql, residual = _apply_duckdb_pushdown(BASE_SQL, pushdown)
        assert "WHERE age > 18 AND status = 'active'" in sql
        assert residual.predicates == []

    def test_residual_predicates_preserved(self):
        pushed = Predicate(expression="x = 1", columns=["x"], location="where")
        residual_pred = Predicate(expression="y IN (SELECT ...)", columns=["y"], location="where")
        pushdown = _make_pushdown(predicates=[pushed], residual_predicates=[residual_pred])
        _, residual = _apply_duckdb_pushdown(BASE_SQL, pushdown)
        assert residual_pred in residual.predicates


class TestLimitPushdown:
    def test_appends_limit(self):
        pushdown = _make_pushdown(limit=100)
        sql, residual = _apply_duckdb_pushdown(BASE_SQL, pushdown)
        assert sql.endswith("LIMIT 100")
        assert residual.limit is None

    def test_residual_limit_preserved(self):
        pushdown = _make_pushdown(residual_limit=50)
        _, residual = _apply_duckdb_pushdown(BASE_SQL, pushdown)
        assert residual.limit == 50


class TestCastPushdown:
    def test_wraps_column_with_cast(self):
        base = "SELECT id, price FROM t"
        cast = Cast(column="price", from_type="int32", to_type="float64")
        pushdown = _make_pushdown(casts=[cast])
        sql, residual = _apply_duckdb_pushdown(base, pushdown)
        assert "CAST(price AS float64)" in sql
        assert residual.casts == []

    def test_residual_casts_preserved(self):
        residual_cast = Cast(column="x", from_type="string", to_type="int32")
        pushdown = _make_pushdown(residual_casts=[residual_cast])
        _, residual = _apply_duckdb_pushdown(BASE_SQL, pushdown)
        assert residual_cast in residual.casts


class TestCombinedPushdown:
    def test_projection_predicate_limit(self):
        pred = Predicate(expression="id > 0", columns=["id"], location="where")
        pushdown = _make_pushdown(columns=["id", "name"], predicates=[pred], limit=10)
        sql, residual = _apply_duckdb_pushdown(BASE_SQL, pushdown)
        # Projection applied first, then predicate wraps, then limit appended
        assert "SELECT id, name" in sql
        assert "WHERE id > 0" in sql
        assert sql.endswith("LIMIT 10")
        assert residual == EMPTY_RESIDUAL
