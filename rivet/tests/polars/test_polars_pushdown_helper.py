"""Tests for _apply_polars_pushdown helper."""

from __future__ import annotations

import polars as pl

from rivet_core.optimizer import (
    Cast,
    CastPushdownResult,
    LimitPushdownResult,
    PredicatePushdownResult,
    ProjectionPushdownResult,
    PushdownPlan,
    ResidualPlan,
)
from rivet_core.sql_parser import Predicate
from rivet_polars.adapters.pushdown import _apply_polars_pushdown


def _empty_pushdown() -> PushdownPlan:
    return PushdownPlan(
        predicates=PredicatePushdownResult(pushed=[], residual=[]),
        projections=ProjectionPushdownResult(pushed_columns=None, reason=None),
        limit=LimitPushdownResult(pushed_limit=None, residual_limit=None, reason=None),
        casts=CastPushdownResult(pushed=[], residual=[]),
    )


def _sample_df() -> pl.DataFrame:
    return pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"], "c": [10.0, 20.0, 30.0]})


class TestEmptyPushdown:
    def test_returns_original_data_and_empty_residual(self):
        df = _sample_df()
        result_df, residual = _apply_polars_pushdown(df, _empty_pushdown())
        assert result_df.shape == df.shape
        assert residual == ResidualPlan(predicates=[], limit=None, casts=[])


class TestProjectionPushdown:
    def test_selects_specified_columns(self):
        df = _sample_df()
        pushdown = PushdownPlan(
            predicates=PredicatePushdownResult(pushed=[], residual=[]),
            projections=ProjectionPushdownResult(pushed_columns=["a", "c"], reason=None),
            limit=LimitPushdownResult(pushed_limit=None, residual_limit=None, reason=None),
            casts=CastPushdownResult(pushed=[], residual=[]),
        )
        result_df, residual = _apply_polars_pushdown(df, pushdown)
        assert result_df.columns == ["a", "c"]
        assert result_df.shape[0] == 3

    def test_invalid_column_does_not_raise(self):
        df = _sample_df()
        pushdown = PushdownPlan(
            predicates=PredicatePushdownResult(pushed=[], residual=[]),
            projections=ProjectionPushdownResult(pushed_columns=["nonexistent"], reason=None),
            limit=LimitPushdownResult(pushed_limit=None, residual_limit=None, reason=None),
            casts=CastPushdownResult(pushed=[], residual=[]),
        )
        # Should not raise — graceful degradation, keeps original columns
        result_df, residual = _apply_polars_pushdown(df, pushdown)
        assert result_df.columns == ["a", "b", "c"]


class TestLimitPushdown:
    def test_limits_rows(self):
        df = _sample_df()
        pushdown = PushdownPlan(
            predicates=PredicatePushdownResult(pushed=[], residual=[]),
            projections=ProjectionPushdownResult(pushed_columns=None, reason=None),
            limit=LimitPushdownResult(pushed_limit=2, residual_limit=None, reason=None),
            casts=CastPushdownResult(pushed=[], residual=[]),
        )
        result_df, residual = _apply_polars_pushdown(df, pushdown)
        assert result_df.shape[0] == 2

    def test_limit_larger_than_table(self):
        df = _sample_df()
        pushdown = PushdownPlan(
            predicates=PredicatePushdownResult(pushed=[], residual=[]),
            projections=ProjectionPushdownResult(pushed_columns=None, reason=None),
            limit=LimitPushdownResult(pushed_limit=100, residual_limit=None, reason=None),
            casts=CastPushdownResult(pushed=[], residual=[]),
        )
        result_df, residual = _apply_polars_pushdown(df, pushdown)
        assert result_df.shape[0] == 3


class TestCastPushdown:
    def test_casts_column_type(self):
        df = _sample_df()
        pushdown = PushdownPlan(
            predicates=PredicatePushdownResult(pushed=[], residual=[]),
            projections=ProjectionPushdownResult(pushed_columns=None, reason=None),
            limit=LimitPushdownResult(pushed_limit=None, residual_limit=None, reason=None),
            casts=CastPushdownResult(
                pushed=[Cast(column="a", from_type="int64", to_type="float64")],
                residual=[],
            ),
        )
        result_df, residual = _apply_polars_pushdown(df, pushdown)
        assert result_df["a"].dtype == pl.Float64

    def test_invalid_cast_becomes_residual(self):
        df = _sample_df()
        bad_cast = Cast(column="b", from_type="utf8", to_type="nonexistent_type_xyz")
        pushdown = PushdownPlan(
            predicates=PredicatePushdownResult(pushed=[], residual=[]),
            projections=ProjectionPushdownResult(pushed_columns=None, reason=None),
            limit=LimitPushdownResult(pushed_limit=None, residual_limit=None, reason=None),
            casts=CastPushdownResult(pushed=[bad_cast], residual=[]),
        )
        result_df, residual = _apply_polars_pushdown(df, pushdown)
        assert bad_cast in residual.casts


class TestPredicatePushdown:
    def test_sql_expr_predicate(self):
        df = _sample_df()
        pred = Predicate(expression="a > 1", columns=["a"], location="where")
        pushdown = PushdownPlan(
            predicates=PredicatePushdownResult(pushed=[pred], residual=[]),
            projections=ProjectionPushdownResult(pushed_columns=None, reason=None),
            limit=LimitPushdownResult(pushed_limit=None, residual_limit=None, reason=None),
            casts=CastPushdownResult(pushed=[], residual=[]),
        )
        result_df, residual = _apply_polars_pushdown(df, pushdown)
        assert result_df.shape[0] == 2
        assert list(result_df["a"]) == [2, 3]

    def test_invalid_predicate_becomes_residual(self):
        df = _sample_df()
        bad_pred = Predicate(expression="INVALID!!!", columns=["a"], location="where")
        pushdown = PushdownPlan(
            predicates=PredicatePushdownResult(pushed=[bad_pred], residual=[]),
            projections=ProjectionPushdownResult(pushed_columns=None, reason=None),
            limit=LimitPushdownResult(pushed_limit=None, residual_limit=None, reason=None),
            casts=CastPushdownResult(pushed=[], residual=[]),
        )
        result_df, residual = _apply_polars_pushdown(df, pushdown)
        assert bad_pred in residual.predicates


class TestResidualPassthrough:
    def test_residuals_from_pushdown_plan_are_preserved(self):
        df = _sample_df()
        residual_pred = Predicate(expression="x > 5", columns=["x"], location="where")
        residual_cast = Cast(column="z", from_type="int", to_type="string")
        pushdown = PushdownPlan(
            predicates=PredicatePushdownResult(pushed=[], residual=[residual_pred]),
            projections=ProjectionPushdownResult(pushed_columns=None, reason=None),
            limit=LimitPushdownResult(pushed_limit=None, residual_limit=7, reason=None),
            casts=CastPushdownResult(pushed=[], residual=[residual_cast]),
        )
        result_df, residual = _apply_polars_pushdown(df, pushdown)
        assert residual_pred in residual.predicates
        assert residual_cast in residual.casts
        assert residual.limit == 7


class TestLazyFrame:
    def test_works_with_lazyframe(self):
        df = _sample_df().lazy()
        pushdown = PushdownPlan(
            predicates=PredicatePushdownResult(pushed=[], residual=[]),
            projections=ProjectionPushdownResult(pushed_columns=["a", "b"], reason=None),
            limit=LimitPushdownResult(pushed_limit=2, residual_limit=None, reason=None),
            casts=CastPushdownResult(pushed=[], residual=[]),
        )
        result_df, residual = _apply_polars_pushdown(df, pushdown)
        assert isinstance(result_df, pl.LazyFrame)
        collected = result_df.collect()
        assert collected.columns == ["a", "b"]
        assert collected.shape[0] == 2


class TestCombinedPushdown:
    def test_projection_limit_and_cast_together(self):
        df = _sample_df()
        pushdown = PushdownPlan(
            predicates=PredicatePushdownResult(pushed=[], residual=[]),
            projections=ProjectionPushdownResult(pushed_columns=["a", "c"], reason=None),
            limit=LimitPushdownResult(pushed_limit=2, residual_limit=None, reason=None),
            casts=CastPushdownResult(
                pushed=[Cast(column="a", from_type="int64", to_type="float32")],
                residual=[],
            ),
        )
        result_df, residual = _apply_polars_pushdown(df, pushdown)
        assert result_df.columns == ["a", "c"]
        assert result_df.shape[0] == 2
        assert result_df["a"].dtype == pl.Float32
