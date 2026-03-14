"""Unit tests for source inline transform merge and expression functions in the executor.

Tests _merge_source_predicates_into_pushdown, _merge_source_projections_into_pushdown,
and _apply_source_expressions with specific examples and edge cases.
"""

from __future__ import annotations

import pyarrow
import pytest

from rivet_core.compiler import CompiledJoint
from rivet_core.errors import ExecutionError
from rivet_core.executor import (
    _apply_source_expressions,
    _merge_source_predicates_into_pushdown,
    _merge_source_projections_into_pushdown,
)
from rivet_core.optimizer import (
    CastPushdownResult,
    LimitPushdownResult,
    PredicatePushdownResult,
    ProjectionPushdownResult,
    PushdownPlan,
)
from rivet_core.sql_parser import LogicalPlan, Predicate, Projection, TableReference

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cj(logical_plan: LogicalPlan | None, name: str = "test_source") -> CompiledJoint:
    """Create a minimal CompiledJoint with the given logical plan."""
    return CompiledJoint(
        name=name,
        type="source",
        catalog="test_catalog",
        catalog_type="filesystem",
        engine="duckdb",
        engine_resolution=None,
        adapter=None,
        sql=None,
        sql_translated=None,
        sql_resolved=None,
        sql_dialect=None,
        engine_dialect=None,
        upstream=[],
        eager=False,
        table="test_table",
        write_strategy=None,
        function=None,
        source_file=None,
        logical_plan=logical_plan,
        output_schema=None,
        column_lineage=[],
        optimizations=[],
        checks=[],
        fused_group_id=None,
        tags=[],
        description=None,
        fusion_strategy_override=None,
        materialization_strategy_override=None,
    )


def _pushdown(
    predicates: list[Predicate] | None = None,
    columns: list[str] | None = None,
    limit: int | None = None,
) -> PushdownPlan:
    return PushdownPlan(
        predicates=PredicatePushdownResult(pushed=predicates or [], residual=[]),
        projections=ProjectionPushdownResult(pushed_columns=columns, reason=None),
        limit=LimitPushdownResult(pushed_limit=limit, residual_limit=None, reason=None),
        casts=CastPushdownResult(pushed=[], residual=[]),
    )


def _lp(
    predicates: list[Predicate] | None = None,
    projections: list[Projection] | None = None,
) -> LogicalPlan:
    return LogicalPlan(
        projections=projections or [],
        predicates=predicates or [],
        joins=[],
        aggregations=None,
        limit=None,
        ordering=None,
        distinct=False,
        source_tables=[TableReference(name="t", schema=None, alias=None, source_type="from")],
    )


# ===================================================================
# 9.6 — _merge_source_predicates_into_pushdown
# ===================================================================


class TestMergeSourcePredicatesWithExistingPushdown:
    """Predicates appended to existing pushdown."""

    def test_predicates_appended(self) -> None:
        existing = _pushdown(
            predicates=[Predicate(expression="x > 0", columns=["x"], location="where")]
        )
        lp = _lp(
            predicates=[
                Predicate(expression="status = 'active'", columns=["status"], location="where"),
            ]
        )
        result = _merge_source_predicates_into_pushdown(existing, _cj(lp))
        assert result is not None
        pushed_exprs = [p.expression for p in result.predicates.pushed]
        assert "x > 0" in pushed_exprs
        assert "status = 'active'" in pushed_exprs

    def test_multiple_predicates_appended(self) -> None:
        existing = _pushdown(
            predicates=[Predicate(expression="a = 1", columns=["a"], location="where")]
        )
        lp = _lp(
            predicates=[
                Predicate(expression="b > 2", columns=["b"], location="where"),
                Predicate(expression="c < 10", columns=["c"], location="where"),
            ]
        )
        result = _merge_source_predicates_into_pushdown(existing, _cj(lp))
        assert result is not None
        assert len(result.predicates.pushed) == 3

    def test_preserves_projections_and_limit(self) -> None:
        existing = _pushdown(
            predicates=[Predicate(expression="x > 0", columns=["x"], location="where")],
            columns=["id", "name"],
            limit=50,
        )
        lp = _lp(
            predicates=[
                Predicate(expression="y = 1", columns=["y"], location="where"),
            ]
        )
        result = _merge_source_predicates_into_pushdown(existing, _cj(lp))
        assert result is not None
        assert result.projections.pushed_columns == ["id", "name"]
        assert result.limit.pushed_limit == 50


class TestMergeSourcePredicatesFromNone:
    """New PushdownPlan created when pushdown is None."""

    def test_creates_new_pushdown(self) -> None:
        lp = _lp(
            predicates=[
                Predicate(expression="id > 0", columns=["id"], location="where"),
            ]
        )
        result = _merge_source_predicates_into_pushdown(None, _cj(lp))
        assert result is not None
        pushed_exprs = [p.expression for p in result.predicates.pushed]
        assert "id > 0" in pushed_exprs
        assert result.projections.pushed_columns is None
        assert result.limit.pushed_limit is None


class TestMergeSourcePredicatesNoPredicates:
    """Pushdown unchanged when no predicates."""

    def test_no_predicates_returns_same(self) -> None:
        existing = _pushdown(
            predicates=[Predicate(expression="x > 0", columns=["x"], location="where")]
        )
        lp = _lp(predicates=[])
        result = _merge_source_predicates_into_pushdown(existing, _cj(lp))
        assert result is existing

    def test_none_logical_plan_returns_same(self) -> None:
        existing = _pushdown(
            predicates=[Predicate(expression="x > 0", columns=["x"], location="where")]
        )
        result = _merge_source_predicates_into_pushdown(existing, _cj(None))
        assert result is existing

    def test_none_pushdown_no_predicates_returns_none(self) -> None:
        lp = _lp(predicates=[])
        result = _merge_source_predicates_into_pushdown(None, _cj(lp))
        assert result is None


# ===================================================================
# 9.7 — _merge_source_projections_into_pushdown
# ===================================================================


class TestMergeSourceProjectionsSimple:
    """Simple column projections → column names in pushed_columns."""

    def test_simple_columns_pushed(self) -> None:
        lp = _lp(
            projections=[
                Projection(expression="id", alias=None, source_columns=["id"]),
                Projection(expression="name", alias=None, source_columns=["name"]),
            ]
        )
        result = _merge_source_projections_into_pushdown(None, _cj(lp))
        assert result is not None
        assert set(result.projections.pushed_columns or []) == {"id", "name"}

    def test_simple_columns_intersect_with_existing(self) -> None:
        existing = _pushdown(columns=["id", "name", "email"])
        lp = _lp(
            projections=[
                Projection(expression="id", alias=None, source_columns=["id"]),
                Projection(expression="name", alias=None, source_columns=["name"]),
            ]
        )
        result = _merge_source_projections_into_pushdown(existing, _cj(lp))
        assert result is not None
        pushed = set(result.projections.pushed_columns or [])
        assert pushed == {"id", "name"}


class TestMergeSourceProjectionsAliased:
    """Aliased expressions → base columns in pushed_columns."""

    def test_aliased_pushes_base_columns(self) -> None:
        lp = _lp(
            projections=[
                Projection(
                    expression="price * quantity",
                    alias="revenue",
                    source_columns=["price", "quantity"],
                ),
            ]
        )
        result = _merge_source_projections_into_pushdown(None, _cj(lp))
        assert result is not None
        pushed = set(result.projections.pushed_columns or [])
        assert "price" in pushed
        assert "quantity" in pushed
        assert "revenue" not in pushed

    def test_mixed_simple_and_aliased(self) -> None:
        lp = _lp(
            projections=[
                Projection(expression="id", alias=None, source_columns=["id"]),
                Projection(
                    expression="price * quantity",
                    alias="revenue",
                    source_columns=["price", "quantity"],
                ),
            ]
        )
        result = _merge_source_projections_into_pushdown(None, _cj(lp))
        assert result is not None
        pushed = set(result.projections.pushed_columns or [])
        assert pushed == {"id", "price", "quantity"}


class TestMergeSourceProjectionsSelectStar:
    """SELECT * → no projection pushdown."""

    def test_select_star_returns_unchanged(self) -> None:
        existing = _pushdown(columns=["id"])
        lp = _lp(
            projections=[
                Projection(expression="*", alias=None, source_columns=[]),
            ]
        )
        result = _merge_source_projections_into_pushdown(existing, _cj(lp))
        assert result is existing

    def test_select_star_none_pushdown_returns_none(self) -> None:
        lp = _lp(
            projections=[
                Projection(expression="*", alias=None, source_columns=[]),
            ]
        )
        result = _merge_source_projections_into_pushdown(None, _cj(lp))
        assert result is None


class TestMergeSourceProjectionsNonePushdown:
    """New PushdownPlan created when pushdown is None."""

    def test_creates_new_pushdown(self) -> None:
        lp = _lp(
            projections=[
                Projection(expression="id", alias=None, source_columns=["id"]),
            ]
        )
        result = _merge_source_projections_into_pushdown(None, _cj(lp))
        assert result is not None
        assert result.projections.pushed_columns == ["id"]
        assert result.predicates.pushed == []
        assert result.limit.pushed_limit is None


class TestMergeSourceProjectionsNoProjections:
    """Pushdown unchanged when no projections."""

    def test_empty_projections_returns_same(self) -> None:
        existing = _pushdown(columns=["id"])
        lp = _lp(projections=[])
        result = _merge_source_projections_into_pushdown(existing, _cj(lp))
        assert result is existing

    def test_none_logical_plan_returns_same(self) -> None:
        existing = _pushdown(columns=["id"])
        result = _merge_source_projections_into_pushdown(existing, _cj(None))
        assert result is existing


# ===================================================================
# 9.8 — _apply_source_expressions
# ===================================================================


def _table(*cols: tuple[str, list]) -> pyarrow.Table:
    """Create a pyarrow Table from (name, values) pairs."""
    arrays = {name: vals for name, vals in cols}
    return pyarrow.table(arrays)


class TestApplySourceExpressionsRename:
    """Column rename → column renamed in output."""

    def test_simple_rename(self) -> None:
        table = _table(("id", [1, 2, 3]))
        lp = _lp(
            projections=[
                Projection(expression="id", alias="order_id", source_columns=["id"]),
            ]
        )
        result = _apply_source_expressions(table, _cj(lp))
        assert "order_id" in result.column_names
        assert "id" not in result.column_names
        assert result.column("order_id").to_pylist() == [1, 2, 3]

    def test_rename_preserves_values(self) -> None:
        table = _table(("name", ["alice", "bob"]))
        lp = _lp(
            projections=[
                Projection(expression="name", alias="label", source_columns=["name"]),
            ]
        )
        result = _apply_source_expressions(table, _cj(lp))
        assert result.column("label").to_pylist() == ["alice", "bob"]


class TestApplySourceExpressionsCast:
    """CAST expression → column has target type."""

    def test_cast_to_double(self) -> None:
        table = _table(("amount", [1, 2, 3]))
        lp = _lp(
            projections=[
                Projection(
                    expression="CAST(amount AS DOUBLE)", alias="amount_f", source_columns=["amount"]
                ),
            ]
        )
        result = _apply_source_expressions(table, _cj(lp))
        assert result.column("amount_f").type == pyarrow.float64()
        assert result.column("amount_f").to_pylist() == [1.0, 2.0, 3.0]

    def test_cast_to_int(self) -> None:
        table = _table(("score", [1.0, 2.0, 3.0]))
        lp = _lp(
            projections=[
                Projection(
                    expression="CAST(score AS INT)", alias="score_i", source_columns=["score"]
                ),
            ]
        )
        result = _apply_source_expressions(table, _cj(lp))
        assert result.column("score_i").type == pyarrow.int32()
        assert result.column("score_i").to_pylist() == [1, 2, 3]


class TestApplySourceExpressionsComputed:
    """Computed expression → column has correct values."""

    def test_multiplication(self) -> None:
        table = _table(("price", [10, 20]), ("quantity", [2, 3]))
        lp = _lp(
            projections=[
                Projection(
                    expression="price * quantity",
                    alias="revenue",
                    source_columns=["price", "quantity"],
                ),
            ]
        )
        result = _apply_source_expressions(table, _cj(lp))
        assert "revenue" in result.column_names
        assert result.column("revenue").to_pylist() == [20, 60]

    def test_string_concatenation(self) -> None:
        table = _table(("first", ["a", "c"]), ("last", ["b", "d"]))
        lp = _lp(
            projections=[
                Projection(
                    expression="first || last", alias="full", source_columns=["first", "last"]
                ),
            ]
        )
        result = _apply_source_expressions(table, _cj(lp))
        assert result.column("full").to_pylist() == ["ab", "cd"]


class TestApplySourceExpressionsMissingColumn:
    """Missing column reference → ExecutionError RVT-501."""

    def test_missing_simple_column(self) -> None:
        table = _table(("id", [1, 2]))
        lp = _lp(
            projections=[
                Projection(expression="bogus", alias=None, source_columns=[]),
            ]
        )
        with pytest.raises(ExecutionError) as exc_info:
            _apply_source_expressions(table, _cj(lp))
        assert exc_info.value.error.code == "RVT-501"

    def test_missing_rename_source(self) -> None:
        table = _table(("id", [1, 2]))
        lp = _lp(
            projections=[
                Projection(expression="missing", alias="out", source_columns=["missing"]),
            ]
        )
        with pytest.raises(ExecutionError) as exc_info:
            _apply_source_expressions(table, _cj(lp))
        assert exc_info.value.error.code == "RVT-501"

    def test_missing_cast_source(self) -> None:
        table = _table(("id", [1, 2]))
        lp = _lp(
            projections=[
                Projection(
                    expression="CAST(missing AS DOUBLE)", alias="out", source_columns=["missing"]
                ),
            ]
        )
        with pytest.raises(ExecutionError) as exc_info:
            _apply_source_expressions(table, _cj(lp))
        assert exc_info.value.error.code == "RVT-501"

    def test_missing_computed_source(self) -> None:
        table = _table(("id", [1, 2]))
        lp = _lp(
            projections=[
                Projection(
                    expression="missing_a + missing_b",
                    alias="out",
                    source_columns=["missing_a", "missing_b"],
                ),
            ]
        )
        with pytest.raises(ExecutionError) as exc_info:
            _apply_source_expressions(table, _cj(lp))
        assert exc_info.value.error.code == "RVT-501"

    def test_error_mentions_joint_name(self) -> None:
        table = _table(("id", [1]))
        lp = _lp(
            projections=[
                Projection(expression="nope", alias=None, source_columns=[]),
            ]
        )
        with pytest.raises(ExecutionError) as exc_info:
            _apply_source_expressions(table, _cj(lp, name="my_src"))
        assert "my_src" in exc_info.value.error.message


class TestApplySourceExpressionsDeclarationOrder:
    """Declaration order: later expression references earlier → succeeds."""

    def test_chained_expressions(self) -> None:
        table = _table(("price", [10, 20]), ("quantity", [2, 3]))
        lp = _lp(
            projections=[
                Projection(
                    expression="price * quantity",
                    alias="total",
                    source_columns=["price", "quantity"],
                ),
                Projection(expression="total * 0.1", alias="tax", source_columns=["total"]),
            ]
        )
        result = _apply_source_expressions(table, _cj(lp))
        assert result.column("total").to_pylist() == [20, 60]
        assert result.column("tax").to_pylist() == [2.0, 6.0]


class TestApplySourceExpressionsOutputColumns:
    """Output has only declared columns in declaration order."""

    def test_output_only_declared_columns(self) -> None:
        table = _table(("id", [1]), ("name", ["a"]), ("extra", [99]))
        lp = _lp(
            projections=[
                Projection(expression="name", alias=None, source_columns=["name"]),
                Projection(expression="id", alias=None, source_columns=["id"]),
            ]
        )
        result = _apply_source_expressions(table, _cj(lp))
        assert result.column_names == ["name", "id"]

    def test_select_star_passthrough(self) -> None:
        table = _table(("id", [1]), ("name", ["a"]))
        lp = _lp(
            projections=[
                Projection(expression="*", alias=None, source_columns=[]),
            ]
        )
        result = _apply_source_expressions(table, _cj(lp))
        assert result.column_names == ["id", "name"]

    def test_no_logical_plan_passthrough(self) -> None:
        table = _table(("id", [1]), ("name", ["a"]))
        result = _apply_source_expressions(table, _cj(None))
        assert result.column_names == ["id", "name"]
