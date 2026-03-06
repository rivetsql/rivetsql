"""Unit tests for _apply_pyspark_pushdown DataFrame operations."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from rivet_core.optimizer import (
    Cast,
    CastPushdownResult,
    LimitPushdownResult,
    PredicatePushdownResult,
    ProjectionPushdownResult,
    PushdownPlan,
)
from rivet_core.sql_parser import Predicate
from rivet_pyspark.adapters.pushdown import EMPTY_RESIDUAL, _apply_pyspark_pushdown


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


def _mock_df():
    """Create a mock Spark DataFrame that chains method calls."""
    df = MagicMock()
    df.select.return_value = df
    df.filter.return_value = df
    df.limit.return_value = df
    df.withColumn.return_value = df
    return df


class TestNonePushdown:
    def test_returns_original_df_and_empty_residual(self):
        df = _mock_df()
        result_df, residual = _apply_pyspark_pushdown(df, None)
        assert result_df is df
        assert residual == EMPTY_RESIDUAL


class TestEmptyPushdown:
    def test_returns_original_df_and_empty_residual(self):
        df = _mock_df()
        pushdown = _make_pushdown()
        result_df, residual = _apply_pyspark_pushdown(df, pushdown)
        assert result_df is df
        assert residual == EMPTY_RESIDUAL
        df.select.assert_not_called()
        df.filter.assert_not_called()
        df.limit.assert_not_called()


class TestProjectionPushdown:
    def test_calls_select_with_columns(self):
        df = _mock_df()
        pushdown = _make_pushdown(columns=["id", "name"])
        _apply_pyspark_pushdown(df, pushdown)
        df.select.assert_called_once_with("id", "name")

    def test_projection_failure_degrades_gracefully(self):
        df = _mock_df()
        df.select.side_effect = Exception("bad column")
        pushdown = _make_pushdown(columns=["nonexistent"])
        result_df, residual = _apply_pyspark_pushdown(df, pushdown)
        # No residual for projection — full columns already present
        assert residual == EMPTY_RESIDUAL


class TestPredicatePushdown:
    def test_single_predicate(self):
        df = _mock_df()
        pred = Predicate(expression="age > 18", columns=["age"], location="where")
        pushdown = _make_pushdown(predicates=[pred])
        _apply_pyspark_pushdown(df, pushdown)
        df.filter.assert_called_once_with("age > 18")

    def test_multiple_predicates_applied_sequentially(self):
        df = _mock_df()
        p1 = Predicate(expression="age > 18", columns=["age"], location="where")
        p2 = Predicate(expression="status = 'active'", columns=["status"], location="where")
        pushdown = _make_pushdown(predicates=[p1, p2])
        _apply_pyspark_pushdown(df, pushdown)
        assert df.filter.call_count == 2

    def test_predicate_failure_moves_to_residual(self):
        df = _mock_df()
        df.filter.side_effect = Exception("parse error")
        pred = Predicate(expression="bad expr", columns=["x"], location="where")
        pushdown = _make_pushdown(predicates=[pred])
        _, residual = _apply_pyspark_pushdown(df, pushdown)
        assert pred in residual.predicates

    def test_residual_predicates_preserved(self):
        df = _mock_df()
        pushed = Predicate(expression="x = 1", columns=["x"], location="where")
        residual_pred = Predicate(expression="y IN (SELECT ...)", columns=["y"], location="where")
        pushdown = _make_pushdown(predicates=[pushed], residual_predicates=[residual_pred])
        _, residual = _apply_pyspark_pushdown(df, pushdown)
        assert residual_pred in residual.predicates


class TestLimitPushdown:
    def test_calls_limit(self):
        df = _mock_df()
        pushdown = _make_pushdown(limit=100)
        _apply_pyspark_pushdown(df, pushdown)
        df.limit.assert_called_once_with(100)

    def test_limit_failure_moves_to_residual(self):
        df = _mock_df()
        df.limit.side_effect = Exception("limit error")
        pushdown = _make_pushdown(limit=50)
        _, residual = _apply_pyspark_pushdown(df, pushdown)
        assert residual.limit == 50

    def test_residual_limit_preserved(self):
        df = _mock_df()
        pushdown = _make_pushdown(residual_limit=50)
        _, residual = _apply_pyspark_pushdown(df, pushdown)
        assert residual.limit == 50


class TestCastPushdown:
    @patch("pyspark.sql.functions.col")
    def test_calls_withColumn_and_cast(self, mock_col_fn):
        df = _mock_df()
        mock_col_obj = MagicMock()
        mock_col_fn.return_value = mock_col_obj
        cast = Cast(column="price", from_type="int32", to_type="float64")
        pushdown = _make_pushdown(casts=[cast])
        _apply_pyspark_pushdown(df, pushdown)
        mock_col_fn.assert_called_with("price")
        mock_col_obj.cast.assert_called_with("float64")
        df.withColumn.assert_called_once()

    def test_cast_failure_moves_to_residual(self):
        df = _mock_df()
        df.withColumn.side_effect = Exception("cast error")
        cast = Cast(column="x", from_type="string", to_type="int32")
        pushdown = _make_pushdown(casts=[cast])
        _, residual = _apply_pyspark_pushdown(df, pushdown)
        assert cast in residual.casts

    def test_residual_casts_preserved(self):
        df = _mock_df()
        residual_cast = Cast(column="x", from_type="string", to_type="int32")
        pushdown = _make_pushdown(residual_casts=[residual_cast])
        _, residual = _apply_pyspark_pushdown(df, pushdown)
        assert residual_cast in residual.casts


class TestCombinedPushdown:
    def test_projection_predicate_limit(self):
        df = _mock_df()
        pred = Predicate(expression="id > 0", columns=["id"], location="where")
        pushdown = _make_pushdown(columns=["id", "name"], predicates=[pred], limit=10)
        _, residual = _apply_pyspark_pushdown(df, pushdown)
        df.select.assert_called_once_with("id", "name")
        df.filter.assert_called_once_with("id > 0")
        df.limit.assert_called_once_with(10)
        assert residual == EMPTY_RESIDUAL
