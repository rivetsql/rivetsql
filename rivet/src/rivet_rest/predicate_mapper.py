"""Predicate mapper: translate Rivet predicates to REST API query parameters.

Only equality and comparison operators on columns declared in ``filter_params``
are pushed down as query parameters.  Everything else becomes a residual
predicate for the engine to apply post-fetch.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from rivet_core.optimizer import EMPTY_RESIDUAL, ResidualPlan
from rivet_core.sql_parser import Predicate

if TYPE_CHECKING:
    from rivet_core.optimizer import PushdownPlan

# Operators that can be expressed as a single query parameter value.
_SUPPORTED_OPS = frozenset({"=", "<", ">", "<=", ">="})

# Pattern to match simple binary comparison expressions:
#   <column> <op> <value>   or   <value> <op> <column>
# Captures: (lhs, operator, rhs)
_COMPARISON_RE = re.compile(
    r"^\s*(.+?)\s*(<=|>=|<>|!=|<|>|=)\s*(.+?)\s*$",
    re.DOTALL,
)


def _strip_quotes(value: str) -> str:
    """Remove surrounding single quotes from a SQL literal."""
    if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
        return value[1:-1]
    return value


def _parse_simple_comparison(
    expression: str,
    columns: list[str],
) -> tuple[str, str, str] | None:
    """Try to parse a simple ``column op value`` expression.

    Returns ``(column, operator, value)`` if the expression is a simple
    binary comparison involving exactly one of the predicate's declared
    columns, or ``None`` if it cannot be parsed.
    """
    m = _COMPARISON_RE.match(expression)
    if m is None:
        return None

    lhs, op, rhs = m.group(1), m.group(2), m.group(3)

    if op not in _SUPPORTED_OPS:
        return None

    # Determine which side is the column reference.
    col_lower = {c.lower() for c in columns}

    lhs_bare = lhs.strip()
    rhs_bare = rhs.strip()

    if lhs_bare.lower() in col_lower:
        return lhs_bare, op, _strip_quotes(rhs_bare)
    if rhs_bare.lower() in col_lower:
        # Flip the operator when the column is on the right:
        #   5 < age  →  age > 5
        flipped = {"<": ">", ">": "<", "<=": ">=", ">=": "<=", "=": "="}
        return rhs_bare, flipped[op], _strip_quotes(lhs_bare)

    return None


def map_predicates(
    pushdown: PushdownPlan | None,
    filter_params: dict[str, str] | None,
) -> tuple[dict[str, str], ResidualPlan]:
    """Map pushed predicates to query parameters, returning residuals.

    Only predicates on columns declared in *filter_params* with a supported
    operator (``=``, ``<``, ``>``, ``<=``, ``>=``) are translated to query
    parameters.  All other predicates — unsupported operators, undeclared
    columns, complex expressions — become residual predicates for the engine
    to evaluate post-fetch.

    Args:
        pushdown: The pushdown plan from the optimizer, or ``None``.
        filter_params: Mapping of column name → query parameter name, or
            ``None`` if the endpoint declares no filterable columns.

    Returns:
        A tuple of ``(query_params, residual_plan)`` where *query_params*
        is a dict of query parameter name → value, and *residual_plan*
        contains all predicates that could not be pushed down.
    """
    if pushdown is None or filter_params is None:
        residual_preds = (
            list(pushdown.predicates.pushed) + list(pushdown.predicates.residual)
            if pushdown is not None
            else []
        )
        if not residual_preds:
            return {}, EMPTY_RESIDUAL
        return {}, ResidualPlan(
            predicates=residual_preds,
            limit=None,
            casts=[],
        )

    query_params: dict[str, str] = {}
    residual_predicates: list[Predicate] = list(pushdown.predicates.residual)

    # Normalise filter_params keys to lower-case for case-insensitive lookup.
    filter_lower: dict[str, str] = {k.lower(): v for k, v in filter_params.items()}

    for pred in pushdown.predicates.pushed:
        parsed = _parse_simple_comparison(pred.expression, pred.columns)
        if parsed is None:
            residual_predicates.append(pred)
            continue

        col_name, op, value = parsed

        param_name = filter_lower.get(col_name.lower())
        if param_name is None:
            residual_predicates.append(pred)
            continue

        # Build the query parameter value.  For equality we use the bare
        # value; for comparisons we prefix with the operator so the caller
        # (or API) can interpret it.
        if op == "=":
            query_params[param_name] = value
        else:
            query_params[param_name] = f"{op}{value}"

        # Successfully pushed — do NOT add to residuals.

    if not residual_predicates:
        return query_params, EMPTY_RESIDUAL

    return query_params, ResidualPlan(
        predicates=residual_predicates,
        limit=None,
        casts=[],
    )
