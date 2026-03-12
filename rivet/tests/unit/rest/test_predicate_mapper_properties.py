"""Property-based tests for predicate mapper.

- Property 3: Predicate pushdown correctness
  Validates: Requirements 7.2, 7.5
- Property 4: Residual predicate correctness
  Validates: Requirements 7.3, 7.4
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.optimizer import (
    CastPushdownResult,
    LimitPushdownResult,
    PredicatePushdownResult,
    ProjectionPushdownResult,
    PushdownPlan,
)
from rivet_core.sql_parser import Predicate
from rivet_rest.predicate_mapper import map_predicates

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Column names: simple identifiers
_column_name = st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True)

# Supported comparison operators
_supported_op = st.sampled_from(["=", "<", ">", "<=", ">="])

# Unsupported operators
_unsupported_op = st.sampled_from(["LIKE", "IN", "BETWEEN", "<>", "!="])

# Simple scalar literal values for the right-hand side of comparisons
_literal_value = st.one_of(
    st.integers(min_value=-10000, max_value=10000).map(str),
    st.from_regex(r"'[a-z]{1,8}'", fullmatch=True),
)


def _empty_pushdown_plan(pushed: list[Predicate]) -> PushdownPlan:
    """Build a PushdownPlan with the given pushed predicates."""
    return PushdownPlan(
        predicates=PredicatePushdownResult(pushed=pushed, residual=[]),
        projections=ProjectionPushdownResult(pushed_columns=None, reason=None),
        limit=LimitPushdownResult(pushed_limit=None, residual_limit=None, reason=None),
        casts=CastPushdownResult(pushed=[], residual=[]),
    )


# ---------------------------------------------------------------------------
# Feature: rest-api-catalog, Property 3: Predicate pushdown correctness
# ---------------------------------------------------------------------------


@settings(max_examples=100)
@given(
    col=_column_name,
    param=_column_name,
    op=_supported_op,
    value=_literal_value,
)
def test_property3_predicate_pushdown_correctness(
    col: str,
    param: str,
    op: str,
    value: str,
) -> None:
    """**Validates: Requirements 7.2, 7.5**

    For any predicate on a column declared in filter_params with a supported
    operator, map_predicates shall include the predicate as a query parameter
    using the mapped parameter name.
    """
    expression = f"{col} {op} {value}"
    pred = Predicate(expression=expression, columns=[col], location="where")
    pushdown = _empty_pushdown_plan([pred])
    filter_params = {col: param}

    query_params, residual = map_predicates(pushdown, filter_params)

    # The predicate must appear as a query parameter
    assert param in query_params, (
        f"Expected param {param!r} in query_params for expression {expression!r}, "
        f"got {query_params}"
    )

    # The predicate must NOT be in the residual
    residual_exprs = [p.expression for p in residual.predicates]
    assert expression not in residual_exprs, (
        f"Pushed predicate {expression!r} should not be in residuals"
    )


# ---------------------------------------------------------------------------
# Feature: rest-api-catalog, Property 4: Residual predicate correctness
# ---------------------------------------------------------------------------


@settings(max_examples=100)
@given(
    col=_column_name,
    op=_unsupported_op,
    value=_literal_value,
    declared_col=_column_name,
    param=_column_name,
)
def test_property4_residual_unsupported_operator(
    col: str,
    op: str,
    value: str,
    declared_col: str,
    param: str,
) -> None:
    """**Validates: Requirements 7.3, 7.4**

    For any predicate with an unsupported operator, map_predicates shall
    include the predicate in the residual list and not as a query parameter.
    """
    expression = f"{col} {op} {value}"
    pred = Predicate(expression=expression, columns=[col], location="where")
    pushdown = _empty_pushdown_plan([pred])
    # Even if the column is declared, unsupported ops must be residual
    filter_params = {col: param, declared_col: param}

    query_params, residual = map_predicates(pushdown, filter_params)

    # The predicate must be in the residual
    residual_exprs = [p.expression for p in residual.predicates]
    assert expression in residual_exprs, (
        f"Predicate {expression!r} with unsupported op {op!r} should be residual"
    )

    # No query param should have been generated from this predicate's column
    # (unless the declared_col happened to match — but the op is unsupported
    # so it should not be pushed regardless)
    # We check that the param value doesn't contain the literal value with the op
    for v in query_params.values():
        assert v != value.strip("'") or op == "=", (
            f"Unsupported op {op!r} should not produce query param"
        )


@settings(max_examples=100)
@given(
    col=_column_name,
    op=_supported_op,
    value=_literal_value,
    param=_column_name,
)
def test_property4_residual_undeclared_column(
    col: str,
    op: str,
    value: str,
    param: str,
) -> None:
    """**Validates: Requirements 7.3, 7.4**

    For any predicate on a column NOT declared in filter_params,
    map_predicates shall include the predicate in the residual list.
    """
    expression = f"{col} {op} {value}"
    pred = Predicate(expression=expression, columns=[col], location="where")
    pushdown = _empty_pushdown_plan([pred])
    # Use a different column name in filter_params so col is undeclared
    other_col = col + "_other"
    filter_params = {other_col: param}

    query_params, residual = map_predicates(pushdown, filter_params)

    # The predicate must be in the residual
    residual_exprs = [p.expression for p in residual.predicates]
    assert expression in residual_exprs, (
        f"Predicate on undeclared column {col!r} should be residual"
    )

    # No query param should have been generated
    assert len(query_params) == 0, (
        f"Undeclared column should not produce query params, got {query_params}"
    )
