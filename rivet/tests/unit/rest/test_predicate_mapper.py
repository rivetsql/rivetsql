"""Unit tests for predicate mapper.

Covers equality pushdown, comparison operators, unsupported operators,
undeclared columns, and mixed pushable/residual predicates.
"""

from __future__ import annotations

from rivet_core.optimizer import (
    EMPTY_RESIDUAL,
    CastPushdownResult,
    LimitPushdownResult,
    PredicatePushdownResult,
    ProjectionPushdownResult,
    PushdownPlan,
)
from rivet_core.sql_parser import Predicate
from rivet_rest.predicate_mapper import map_predicates

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _plan(
    pushed: list[Predicate] | None = None,
    residual: list[Predicate] | None = None,
) -> PushdownPlan:
    return PushdownPlan(
        predicates=PredicatePushdownResult(
            pushed=pushed or [],
            residual=residual or [],
        ),
        projections=ProjectionPushdownResult(pushed_columns=None, reason=None),
        limit=LimitPushdownResult(pushed_limit=None, residual_limit=None, reason=None),
        casts=CastPushdownResult(pushed=[], residual=[]),
    )


# ---------------------------------------------------------------------------
# Equality pushdown
# ---------------------------------------------------------------------------


class TestEqualityPushdown:
    def test_equality_becomes_query_param(self) -> None:
        pred = Predicate(expression="status = 'active'", columns=["status"], location="where")
        params, residual = map_predicates(
            _plan(pushed=[pred]),
            {"status": "status"},
        )
        assert params == {"status": "active"}
        assert residual == EMPTY_RESIDUAL

    def test_equality_with_mapped_param_name(self) -> None:
        pred = Predicate(
            expression="created_after = '2024-01-01'", columns=["created_after"], location="where"
        )
        params, residual = map_predicates(
            _plan(pushed=[pred]),
            {"created_after": "since"},
        )
        assert params == {"since": "2024-01-01"}
        assert residual == EMPTY_RESIDUAL

    def test_equality_integer_value(self) -> None:
        pred = Predicate(expression="age = 25", columns=["age"], location="where")
        params, residual = map_predicates(
            _plan(pushed=[pred]),
            {"age": "age"},
        )
        assert params == {"age": "25"}
        assert residual == EMPTY_RESIDUAL


# ---------------------------------------------------------------------------
# Comparison operators pushdown
# ---------------------------------------------------------------------------


class TestComparisonPushdown:
    def test_less_than(self) -> None:
        pred = Predicate(expression="price < 100", columns=["price"], location="where")
        params, residual = map_predicates(
            _plan(pushed=[pred]),
            {"price": "max_price"},
        )
        assert params == {"max_price": "<100"}
        assert residual == EMPTY_RESIDUAL

    def test_greater_than(self) -> None:
        pred = Predicate(expression="score > 50", columns=["score"], location="where")
        params, residual = map_predicates(
            _plan(pushed=[pred]),
            {"score": "min_score"},
        )
        assert params == {"min_score": ">50"}
        assert residual == EMPTY_RESIDUAL

    def test_less_than_or_equal(self) -> None:
        pred = Predicate(expression="count <= 10", columns=["count"], location="where")
        params, residual = map_predicates(
            _plan(pushed=[pred]),
            {"count": "count"},
        )
        assert params == {"count": "<=10"}
        assert residual == EMPTY_RESIDUAL

    def test_greater_than_or_equal(self) -> None:
        pred = Predicate(expression="rating >= 4", columns=["rating"], location="where")
        params, residual = map_predicates(
            _plan(pushed=[pred]),
            {"rating": "min_rating"},
        )
        assert params == {"min_rating": ">=4"}
        assert residual == EMPTY_RESIDUAL


# ---------------------------------------------------------------------------
# Unsupported operators become residuals
# ---------------------------------------------------------------------------


class TestUnsupportedOperators:
    def test_like_becomes_residual(self) -> None:
        pred = Predicate(expression="name LIKE '%test%'", columns=["name"], location="where")
        params, residual = map_predicates(
            _plan(pushed=[pred]),
            {"name": "name"},
        )
        assert params == {}
        assert len(residual.predicates) == 1
        assert residual.predicates[0].expression == "name LIKE '%test%'"

    def test_in_becomes_residual(self) -> None:
        pred = Predicate(expression="status IN ('a', 'b')", columns=["status"], location="where")
        params, residual = map_predicates(
            _plan(pushed=[pred]),
            {"status": "status"},
        )
        assert params == {}
        assert len(residual.predicates) == 1

    def test_between_becomes_residual(self) -> None:
        pred = Predicate(expression="age BETWEEN 18 AND 65", columns=["age"], location="where")
        params, residual = map_predicates(
            _plan(pushed=[pred]),
            {"age": "age"},
        )
        assert params == {}
        assert len(residual.predicates) == 1


# ---------------------------------------------------------------------------
# Undeclared columns become residuals
# ---------------------------------------------------------------------------


class TestUndeclaredColumns:
    def test_undeclared_column_becomes_residual(self) -> None:
        pred = Predicate(expression="secret = 'x'", columns=["secret"], location="where")
        params, residual = map_predicates(
            _plan(pushed=[pred]),
            {"status": "status"},
        )
        assert params == {}
        assert len(residual.predicates) == 1
        assert residual.predicates[0] is pred

    def test_empty_filter_params(self) -> None:
        pred = Predicate(expression="col = 1", columns=["col"], location="where")
        params, residual = map_predicates(
            _plan(pushed=[pred]),
            {},
        )
        assert params == {}
        assert len(residual.predicates) == 1

    def test_none_filter_params(self) -> None:
        pred = Predicate(expression="col = 1", columns=["col"], location="where")
        params, residual = map_predicates(
            _plan(pushed=[pred]),
            None,
        )
        assert params == {}
        assert len(residual.predicates) == 1


# ---------------------------------------------------------------------------
# Mixed pushable and residual predicates
# ---------------------------------------------------------------------------


class TestMixedPredicates:
    def test_mixed_pushable_and_residual(self) -> None:
        pushable = Predicate(expression="status = 'active'", columns=["status"], location="where")
        residual_pred = Predicate(
            expression="name LIKE '%test%'", columns=["name"], location="where"
        )
        params, residual = map_predicates(
            _plan(pushed=[pushable, residual_pred]),
            {"status": "status", "name": "name"},
        )
        assert params == {"status": "active"}
        assert len(residual.predicates) == 1
        assert residual.predicates[0].expression == "name LIKE '%test%'"

    def test_mixed_declared_and_undeclared(self) -> None:
        declared = Predicate(expression="status = 'ok'", columns=["status"], location="where")
        undeclared = Predicate(expression="secret = 'x'", columns=["secret"], location="where")
        params, residual = map_predicates(
            _plan(pushed=[declared, undeclared]),
            {"status": "status"},
        )
        assert params == {"status": "ok"}
        assert len(residual.predicates) == 1
        assert residual.predicates[0] is undeclared

    def test_existing_residuals_preserved(self) -> None:
        """Predicates already in the residual list are preserved."""
        pushed = Predicate(expression="status = 'ok'", columns=["status"], location="where")
        existing_residual = Predicate(expression="x > 1", columns=["x"], location="where")
        params, residual = map_predicates(
            _plan(pushed=[pushed], residual=[existing_residual]),
            {"status": "status"},
        )
        assert params == {"status": "ok"}
        assert len(residual.predicates) == 1
        assert residual.predicates[0] is existing_residual

    def test_none_pushdown_returns_empty(self) -> None:
        params, residual = map_predicates(None, {"status": "status"})
        assert params == {}
        assert residual == EMPTY_RESIDUAL
