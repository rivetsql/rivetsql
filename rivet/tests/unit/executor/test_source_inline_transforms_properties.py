"""Property tests for source inline transform merge functions in the executor.

Tests _merge_source_predicates_into_pushdown and _merge_source_projections_into_pushdown
using Hypothesis to generate random LogicalPlans and verify pushdown assembly.
"""

from __future__ import annotations

import uuid

import pyarrow
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.compiler import CompiledJoint
from rivet_core.executor import (
    _apply_residuals,
    _apply_source_expressions,
    _merge_cross_group_limits,
    _merge_cross_group_predicates,
    _merge_cross_group_projections,
    _merge_source_limit_into_pushdown,
    _merge_source_predicates_into_pushdown,
    _merge_source_projections_into_pushdown,
)
from rivet_core.optimizer import (
    CastPushdownResult,
    FusedGroup,
    LimitPushdownResult,
    PredicatePushdownResult,
    ProjectionPushdownResult,
    PushdownPlan,
    ResidualPlan,
)
from rivet_core.sql_parser import LogicalPlan, Predicate, Projection, TableReference

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_COLUMN_NAMES = st.sampled_from(
    [
        "id",
        "name",
        "email",
        "amount",
        "price",
        "quantity",
        "status",
        "created_at",
        "updated_at",
        "is_active",
        "country",
        "age",
        "score",
    ]
)

_OPERATORS = st.sampled_from(["> 0", "< 100", "= 'active'", "IS NOT NULL", ">= 10"])


def _make_compiled_joint(logical_plan: LogicalPlan | None) -> CompiledJoint:
    """Create a minimal CompiledJoint with the given logical plan."""
    return CompiledJoint(
        name="test_source",
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


def _make_pushdown(
    predicates: list[Predicate] | None = None,
    columns: list[str] | None = None,
    limit: int | None = None,
) -> PushdownPlan:
    """Create a PushdownPlan with optional pre-existing entries."""
    return PushdownPlan(
        predicates=PredicatePushdownResult(pushed=predicates or [], residual=[]),
        projections=ProjectionPushdownResult(pushed_columns=columns, reason=None),
        limit=LimitPushdownResult(pushed_limit=limit, residual_limit=None, reason=None),
        casts=CastPushdownResult(pushed=[], residual=[]),
    )


@st.composite
def predicate_list(draw: st.DrawFn) -> list[Predicate]:
    """Generate a list of 1-5 random WHERE predicates."""
    n = draw(st.integers(min_value=1, max_value=5))
    preds = []
    for _ in range(n):
        col = draw(_COLUMN_NAMES)
        op = draw(_OPERATORS)
        preds.append(Predicate(expression=f"{col} {op}", columns=[col], location="where"))
    return preds


@st.composite
def simple_projection_list(draw: st.DrawFn) -> list[Projection]:
    """Generate a list of 1-5 simple column reference projections."""
    n = draw(st.integers(min_value=1, max_value=5))
    used: set[str] = set()
    projs = []
    for _ in range(n):
        col = draw(_COLUMN_NAMES.filter(lambda x: x not in used))
        used.add(col)
        projs.append(Projection(expression=col, alias=None, source_columns=[col]))
    return projs


@st.composite
def aliased_expression_list(draw: st.DrawFn) -> list[Projection]:
    """Generate a list of 1-4 aliased expression projections."""
    n = draw(st.integers(min_value=1, max_value=4))
    projs = []
    for i in range(n):
        col_a = draw(_COLUMN_NAMES)
        col_b = draw(_COLUMN_NAMES)
        alias = f"computed_{i}"
        projs.append(
            Projection(
                expression=f"{col_a} + {col_b}",
                alias=alias,
                source_columns=[col_a, col_b],
            )
        )
    return projs


def _make_logical_plan(
    predicates: list[Predicate] | None = None,
    projections: list[Projection] | None = None,
) -> LogicalPlan:
    """Create a minimal LogicalPlan with given predicates/projections."""
    return LogicalPlan(
        projections=projections or [],
        predicates=predicates or [],
        joins=[],
        aggregations=None,
        limit=None,
        ordering=None,
        distinct=False,
        source_tables=[
            TableReference(name="test_table", schema=None, alias=None, source_type="from")
        ],
    )


# ---------------------------------------------------------------------------
# Property 1: Source predicates appear in PushdownPlan
# ---------------------------------------------------------------------------


# Feature: source-inline-transforms, Property 1: Source predicates appear in PushdownPlan
@given(preds=predicate_list())
@settings(max_examples=100)
def test_source_predicates_appear_in_pushdown_from_none(preds: list[Predicate]) -> None:
    """When pushdown is None, all source predicates appear in the new PushdownPlan."""
    lp = _make_logical_plan(predicates=preds)
    cj = _make_compiled_joint(lp)

    result = _merge_source_predicates_into_pushdown(None, cj)

    assert result is not None
    assert result.predicates.pushed == preds


# Feature: source-inline-transforms, Property 1: Source predicates appear in PushdownPlan
@given(preds=predicate_list(), existing=predicate_list())
@settings(max_examples=100)
def test_source_predicates_appended_to_existing_pushdown(
    preds: list[Predicate],
    existing: list[Predicate],
) -> None:
    """When pushdown already has predicates, source predicates are appended."""
    lp = _make_logical_plan(predicates=preds)
    cj = _make_compiled_joint(lp)
    pushdown = _make_pushdown(predicates=existing)

    result = _merge_source_predicates_into_pushdown(pushdown, cj)

    assert result is not None
    assert result.predicates.pushed == existing + preds


# Feature: source-inline-transforms, Property 1: Source predicates appear in PushdownPlan
@given(preds=predicate_list())
@settings(max_examples=100)
def test_source_predicates_preserve_other_pushdown_fields(preds: list[Predicate]) -> None:
    """Merging source predicates does not alter projections, limit, or casts."""
    lp = _make_logical_plan(predicates=preds)
    cj = _make_compiled_joint(lp)
    pushdown = _make_pushdown(columns=["id", "name"], limit=50)

    result = _merge_source_predicates_into_pushdown(pushdown, cj)

    assert result is not None
    assert result.projections.pushed_columns == ["id", "name"]
    assert result.limit.pushed_limit == 50


def test_no_predicates_returns_pushdown_unchanged() -> None:
    """When source has no predicates, pushdown is returned as-is."""
    lp = _make_logical_plan(predicates=[])
    cj = _make_compiled_joint(lp)
    pushdown = _make_pushdown(predicates=["x > 0"])

    result = _merge_source_predicates_into_pushdown(pushdown, cj)

    assert result is pushdown  # Same object


def test_no_logical_plan_returns_pushdown_unchanged() -> None:
    """When source has no logical plan, pushdown is returned as-is."""
    cj = _make_compiled_joint(None)

    result = _merge_source_predicates_into_pushdown(None, cj)

    assert result is None


# ---------------------------------------------------------------------------
# Property 3: Source projections appear in PushdownPlan
# ---------------------------------------------------------------------------


# Feature: source-inline-transforms, Property 3: Source projections appear in PushdownPlan
@given(projs=simple_projection_list())
@settings(max_examples=100)
def test_source_projections_appear_in_pushdown_from_none(projs: list[Projection]) -> None:
    """When pushdown is None, all source column names appear in pushed_columns."""
    lp = _make_logical_plan(projections=projs)
    cj = _make_compiled_joint(lp)

    result = _merge_source_projections_into_pushdown(None, cj)

    assert result is not None
    expected = [p.expression for p in projs]
    assert result.projections.pushed_columns == expected


# Feature: source-inline-transforms, Property 3: Source projections appear in PushdownPlan
@given(projs=simple_projection_list())
@settings(max_examples=100)
def test_source_projections_intersect_with_existing(projs: list[Projection]) -> None:
    """When pushdown already has projections, result is the intersection."""
    lp = _make_logical_plan(projections=projs)
    cj = _make_compiled_joint(lp)
    source_cols = [p.expression for p in projs]
    # Existing has all source cols plus an extra one
    existing_cols = source_cols + ["extra_col"]
    pushdown = _make_pushdown(columns=existing_cols)

    result = _merge_source_projections_into_pushdown(pushdown, cj)

    assert result is not None
    assert result.projections.pushed_columns is not None
    # Intersection should be exactly the source cols (sorted)
    assert set(result.projections.pushed_columns) == set(source_cols)


def test_select_star_no_projection_pushdown() -> None:
    """SELECT * produces no projection pushdown."""
    star = Projection(expression="*", alias=None, source_columns=[])
    lp = _make_logical_plan(projections=[star])
    cj = _make_compiled_joint(lp)

    result = _merge_source_projections_into_pushdown(None, cj)

    assert result is None


def test_no_projections_returns_pushdown_unchanged() -> None:
    """When source has no projections, pushdown is returned as-is."""
    lp = _make_logical_plan(projections=[])
    cj = _make_compiled_joint(lp)
    pushdown = _make_pushdown(columns=["id"])

    result = _merge_source_projections_into_pushdown(pushdown, cj)

    assert result is pushdown


# ---------------------------------------------------------------------------
# Property 5: Aliased expressions split into base columns pushed
# ---------------------------------------------------------------------------


# Feature: source-inline-transforms, Property 5: Aliased expressions split into base columns pushed and expression residual
@given(projs=aliased_expression_list())
@settings(max_examples=100)
def test_aliased_expressions_push_base_columns(projs: list[Projection]) -> None:
    """Aliased expressions push their source_columns, not the alias."""
    lp = _make_logical_plan(projections=projs)
    cj = _make_compiled_joint(lp)

    result = _merge_source_projections_into_pushdown(None, cj)

    assert result is not None
    assert result.projections.pushed_columns is not None
    pushed = set(result.projections.pushed_columns)
    # All source_columns from all projections must be in pushed
    for proj in projs:
        for col in proj.source_columns:
            assert col in pushed, f"Base column '{col}' not in pushed: {pushed}"


# Feature: source-inline-transforms, Property 5: Aliased expressions split into base columns pushed and expression residual
@given(projs=aliased_expression_list())
@settings(max_examples=100)
def test_aliased_expressions_do_not_push_alias_names(projs: list[Projection]) -> None:
    """Alias names (computed_0, computed_1, ...) are NOT in pushed_columns."""
    lp = _make_logical_plan(projections=projs)
    cj = _make_compiled_joint(lp)

    result = _merge_source_projections_into_pushdown(None, cj)

    assert result is not None
    assert result.projections.pushed_columns is not None
    pushed = set(result.projections.pushed_columns)
    for proj in projs:
        if proj.alias:
            assert proj.alias not in pushed, (
                f"Alias '{proj.alias}' should not be in pushed: {pushed}"
            )


# Feature: source-inline-transforms, Property 5: Aliased expressions split into base columns pushed and expression residual
@given(projs=aliased_expression_list())
@settings(max_examples=100)
def test_aliased_expressions_no_duplicate_base_columns(projs: list[Projection]) -> None:
    """Base columns are deduplicated in pushed_columns."""
    lp = _make_logical_plan(projections=projs)
    cj = _make_compiled_joint(lp)

    result = _merge_source_projections_into_pushdown(None, cj)

    assert result is not None
    assert result.projections.pushed_columns is not None
    pushed = result.projections.pushed_columns
    assert len(pushed) == len(set(pushed)), f"Duplicates in pushed_columns: {pushed}"


# ---------------------------------------------------------------------------
# Property 6: Source expressions produce correct output columns and values
# ---------------------------------------------------------------------------


@st.composite
def arrow_table_with_int_columns(draw: st.DrawFn) -> tuple[pyarrow.Table, list[str]]:
    """Generate a small Arrow table with 2-5 int64 columns and 1-10 rows."""
    n_cols = draw(st.integers(min_value=2, max_value=5))
    n_rows = draw(st.integers(min_value=1, max_value=10))
    col_names = [f"col_{i}" for i in range(n_cols)]
    arrays = [
        pyarrow.array(
            draw(
                st.lists(st.integers(min_value=0, max_value=100), min_size=n_rows, max_size=n_rows)
            )
        )
        for _ in range(n_cols)
    ]
    table = pyarrow.table(dict(zip(col_names, arrays)))
    return table, col_names


# Feature: source-inline-transforms, Property 6: Source expressions produce correct output columns and values
@given(data=arrow_table_with_int_columns())
@settings(max_examples=100)
def test_source_expressions_rename_produces_correct_columns(
    data: tuple[pyarrow.Table, list[str]],
) -> None:
    """Simple column renames produce correct output column names."""
    table, col_names = data
    # Rename first column to "renamed_col"
    src_col = col_names[0]
    projs = [
        Projection(expression=src_col, alias="renamed_col", source_columns=[src_col]),
    ]
    lp = _make_logical_plan(projections=projs)
    cj = _make_compiled_joint(lp)

    result = _apply_source_expressions(table, cj)

    assert result.num_columns == 1
    assert result.column_names == ["renamed_col"]
    assert result.column("renamed_col").to_pylist() == table.column(src_col).to_pylist()


# Feature: source-inline-transforms, Property 6: Source expressions produce correct output columns and values
@given(data=arrow_table_with_int_columns())
@settings(max_examples=100)
def test_source_expressions_cast_produces_correct_type(
    data: tuple[pyarrow.Table, list[str]],
) -> None:
    """CAST expressions produce columns with the target Arrow type."""
    table, col_names = data
    src_col = col_names[0]
    projs = [
        Projection(
            expression=f"CAST({src_col} AS DOUBLE)",
            alias="casted_col",
            source_columns=[src_col],
        ),
    ]
    lp = _make_logical_plan(projections=projs)
    cj = _make_compiled_joint(lp)

    result = _apply_source_expressions(table, cj)

    assert result.num_columns == 1
    assert result.column_names == ["casted_col"]
    assert result.schema.field("casted_col").type == pyarrow.float64()


# Feature: source-inline-transforms, Property 6: Source expressions produce correct output columns and values
@given(data=arrow_table_with_int_columns())
@settings(max_examples=100)
def test_source_expressions_computed_produces_correct_values(
    data: tuple[pyarrow.Table, list[str]],
) -> None:
    """Computed expressions (col_0 + col_1) produce correct values."""
    table, col_names = data
    if len(col_names) < 2:
        return
    col_a, col_b = col_names[0], col_names[1]
    projs = [
        Projection(
            expression=f"{col_a} + {col_b}",
            alias="total",
            source_columns=[col_a, col_b],
        ),
    ]
    lp = _make_logical_plan(projections=projs)
    cj = _make_compiled_joint(lp)

    result = _apply_source_expressions(table, cj)

    assert result.num_columns == 1
    assert result.column_names == ["total"]
    expected = [
        a + b for a, b in zip(table.column(col_a).to_pylist(), table.column(col_b).to_pylist())
    ]
    assert result.column("total").to_pylist() == expected


# ---------------------------------------------------------------------------
# Property 7: Expression declaration order is respected
# ---------------------------------------------------------------------------


# Feature: source-inline-transforms, Property 7: Expression declaration order is respected
@given(data=arrow_table_with_int_columns())
@settings(max_examples=100)
def test_expression_declaration_order_respected(
    data: tuple[pyarrow.Table, list[str]],
) -> None:
    """Later expressions can reference columns produced by earlier expressions."""
    table, col_names = data
    if len(col_names) < 2:
        return
    col_a, col_b = col_names[0], col_names[1]
    # First expression: total = col_a + col_b
    # Second expression: doubled = total * 2 (references the earlier "total")
    projs = [
        Projection(
            expression=f"{col_a} + {col_b}",
            alias="total",
            source_columns=[col_a, col_b],
        ),
        Projection(
            expression="total * 2",
            alias="doubled",
            source_columns=["total"],
        ),
    ]
    lp = _make_logical_plan(projections=projs)
    cj = _make_compiled_joint(lp)

    result = _apply_source_expressions(table, cj)

    assert result.num_columns == 2
    assert result.column_names == ["total", "doubled"]
    expected_total = [
        a + b for a, b in zip(table.column(col_a).to_pylist(), table.column(col_b).to_pylist())
    ]
    expected_doubled = [t * 2 for t in expected_total]
    assert result.column("total").to_pylist() == expected_total
    assert result.column("doubled").to_pylist() == expected_doubled


# ---------------------------------------------------------------------------
# Property 2: Residual predicates filter the output correctly
# ---------------------------------------------------------------------------


# Feature: source-inline-transforms, Property 2: Residual predicates filter the output correctly
@given(
    n_rows=st.integers(min_value=1, max_value=50),
    threshold=st.integers(min_value=0, max_value=100),
)
@settings(max_examples=100)
def test_residual_predicates_filter_output(n_rows: int, threshold: int) -> None:
    """Residual predicates correctly filter rows from the Arrow table."""
    values = list(range(n_rows))
    table = pyarrow.table({"score": pyarrow.array(values)})
    pred = Predicate(expression=f"score > {threshold}", columns=["score"], location="where")
    residual = ResidualPlan(predicates=[pred], limit=None, casts=[])

    result = _apply_residuals(table, residual)

    # All remaining rows must satisfy the predicate
    for val in result.column("score").to_pylist():
        assert val > threshold, (
            f"Row with score={val} should have been filtered (threshold={threshold})"
        )


# Feature: source-inline-transforms, Property 2: Residual predicates filter the output correctly
@given(
    n_rows=st.integers(min_value=1, max_value=50),
    threshold=st.integers(min_value=0, max_value=100),
)
@settings(max_examples=100)
def test_residual_predicates_preserve_qualifying_rows(n_rows: int, threshold: int) -> None:
    """Residual predicate filtering preserves all rows that satisfy the condition."""
    values = list(range(n_rows))
    table = pyarrow.table({"score": pyarrow.array(values)})
    pred = Predicate(expression=f"score > {threshold}", columns=["score"], location="where")
    residual = ResidualPlan(predicates=[pred], limit=None, casts=[])

    result = _apply_residuals(table, residual)

    expected_count = sum(1 for v in values if v > threshold)
    assert result.num_rows == expected_count


# ---------------------------------------------------------------------------
# Property 4: Residual projections subset the output columns
# ---------------------------------------------------------------------------


# Feature: source-inline-transforms, Property 4: Residual projections subset the output columns
@given(data=arrow_table_with_int_columns())
@settings(max_examples=100)
def test_residual_projections_subset_output_columns(
    data: tuple[pyarrow.Table, list[str]],
) -> None:
    """Residual projection selects only the declared columns from the table."""
    table, col_names = data
    if len(col_names) < 2:
        return
    # Keep only the first column
    keep = [col_names[0]]
    residual = ResidualPlan(predicates=[], limit=None, casts=[])

    result = _apply_residuals(table, residual, projected_columns=keep)

    assert result.column_names == keep
    assert result.num_rows == table.num_rows


# Feature: source-inline-transforms, Property 4: Residual projections subset the output columns
@given(data=arrow_table_with_int_columns())
@settings(max_examples=100)
def test_residual_projections_preserve_row_data(
    data: tuple[pyarrow.Table, list[str]],
) -> None:
    """Residual projection preserves the data in the kept columns."""
    table, col_names = data
    if len(col_names) < 2:
        return
    keep = col_names[:2]
    residual = ResidualPlan(predicates=[], limit=None, casts=[])

    result = _apply_residuals(table, residual, projected_columns=keep)

    for col in keep:
        assert result.column(col).to_pylist() == table.column(col).to_pylist()


# Feature: source-inline-transforms, Property 4: Residual projections subset the output columns
def test_residual_projections_none_keeps_all_columns() -> None:
    """When projected_columns is None, all columns are preserved."""
    table = pyarrow.table({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
    residual = ResidualPlan(predicates=[], limit=None, casts=[])

    result = _apply_residuals(table, residual, projected_columns=None)

    assert result.column_names == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# Property 11: Source and cross-group predicates merge with AND semantics
# ---------------------------------------------------------------------------


def _make_fused_group(
    *,
    per_joint_predicates: dict[str, list[Predicate]] | None = None,
    per_joint_projections: dict[str, list[str]] | None = None,
    per_joint_limits: dict[str, int] | None = None,
) -> FusedGroup:
    """Create a minimal FusedGroup with optional cross-group pushdown fields."""
    return FusedGroup(
        id=str(uuid.uuid4()),
        joints=["test_source"],
        engine="duckdb",
        engine_type="duckdb",
        adapters={"test_source": None},
        fused_sql=None,
        entry_joints=["test_source"],
        exit_joints=["test_source"],
        per_joint_predicates=per_joint_predicates or {},
        per_joint_projections=per_joint_projections or {},
        per_joint_limits=per_joint_limits or {},
    )


# Feature: source-inline-transforms, Property 11: Source and cross-group predicates merge with AND semantics
@given(source_preds=predicate_list(), xg_preds=predicate_list())
@settings(max_examples=100)
def test_source_and_cross_group_predicates_merge_with_and(
    source_preds: list[Predicate],
    xg_preds: list[Predicate],
) -> None:
    """Both source and cross-group predicates end up in the pushed list (AND semantics)."""
    lp = _make_logical_plan(predicates=source_preds)
    cj = _make_compiled_joint(lp)
    group = _make_fused_group(per_joint_predicates={"test_source": xg_preds})

    # Simulate the merge pipeline ordering from _read_source_via_adapter
    pushdown = _merge_source_predicates_into_pushdown(None, cj)
    pushdown = _merge_cross_group_predicates(pushdown, group, "test_source")

    assert pushdown is not None
    pushed = pushdown.predicates.pushed
    # Both source and cross-group predicates are Predicate objects in the pushed list
    assert pushed[: len(source_preds)] == source_preds
    assert pushed[len(source_preds) :] == xg_preds


# Feature: source-inline-transforms, Property 11: Source and cross-group predicates merge with AND semantics
@given(source_preds=predicate_list(), xg_preds=predicate_list())
@settings(max_examples=100)
def test_source_and_cross_group_predicates_total_count(
    source_preds: list[Predicate],
    xg_preds: list[Predicate],
) -> None:
    """The total number of pushed predicates equals source + cross-group count."""
    lp = _make_logical_plan(predicates=source_preds)
    cj = _make_compiled_joint(lp)
    group = _make_fused_group(per_joint_predicates={"test_source": xg_preds})

    pushdown = _merge_source_predicates_into_pushdown(None, cj)
    pushdown = _merge_cross_group_predicates(pushdown, group, "test_source")

    assert pushdown is not None
    assert len(pushdown.predicates.pushed) == len(source_preds) + len(xg_preds)


# ---------------------------------------------------------------------------
# Property 12: Source and cross-group projections intersect
# ---------------------------------------------------------------------------


# Feature: source-inline-transforms, Property 12: Source and cross-group projections intersect
@given(projs=simple_projection_list())
@settings(max_examples=100)
def test_source_and_cross_group_projections_intersect(projs: list[Projection]) -> None:
    """When both source and cross-group projections exist, the result is their intersection."""
    lp = _make_logical_plan(projections=projs)
    cj = _make_compiled_joint(lp)
    source_cols = [p.expression for p in projs]
    # Cross-group requests a subset plus an extra column not in source
    xg_cols = source_cols[:1] + ["extra_xg_col"]
    group = _make_fused_group(per_joint_projections={"test_source": xg_cols})

    pushdown = _merge_source_projections_into_pushdown(None, cj)
    pushdown = _merge_cross_group_projections(pushdown, group, "test_source")

    assert pushdown is not None
    assert pushdown.projections.pushed_columns is not None
    result_set = set(pushdown.projections.pushed_columns)
    expected = set(source_cols) & set(xg_cols)
    assert result_set == expected


# Feature: source-inline-transforms, Property 12: Source and cross-group projections intersect
@given(projs=simple_projection_list())
@settings(max_examples=100)
def test_source_and_cross_group_projections_subset_of_both(projs: list[Projection]) -> None:
    """The intersection is a subset of both source and cross-group projections."""
    lp = _make_logical_plan(projections=projs)
    cj = _make_compiled_joint(lp)
    source_cols = [p.expression for p in projs]
    xg_cols = source_cols + ["extra_xg_col"]
    group = _make_fused_group(per_joint_projections={"test_source": xg_cols})

    pushdown = _merge_source_projections_into_pushdown(None, cj)
    pushdown = _merge_cross_group_projections(pushdown, group, "test_source")

    assert pushdown is not None
    assert pushdown.projections.pushed_columns is not None
    result_set = set(pushdown.projections.pushed_columns)
    assert result_set <= set(source_cols)
    assert result_set <= set(xg_cols)


# Feature: source-inline-transforms, Property 12: Source and cross-group projections intersect
@given(projs=simple_projection_list())
@settings(max_examples=100)
def test_cross_group_projections_without_source_uses_cross_group(projs: list[Projection]) -> None:
    """When source has no projections, cross-group projections are used directly."""
    lp = _make_logical_plan(projections=[])  # No source projections
    cj = _make_compiled_joint(lp)
    xg_cols = ["id", "name"]
    group = _make_fused_group(per_joint_projections={"test_source": xg_cols})

    pushdown = _merge_source_projections_into_pushdown(None, cj)
    pushdown = _merge_cross_group_projections(pushdown, group, "test_source")

    assert pushdown is not None
    assert pushdown.projections.pushed_columns is not None
    assert set(pushdown.projections.pushed_columns) == set(xg_cols)


# ---------------------------------------------------------------------------
# Property 13: Source and cross-group limits take the minimum
# ---------------------------------------------------------------------------


# Feature: source-inline-transforms, Property 13: Source and cross-group limits take the minimum
@given(
    source_limit=st.integers(min_value=1, max_value=10000),
    xg_limit=st.integers(min_value=1, max_value=10000),
)
@settings(max_examples=100)
def test_source_and_cross_group_limits_take_minimum(
    source_limit: int,
    xg_limit: int,
) -> None:
    """The effective limit is the minimum of source and cross-group limits."""
    from rivet_core.sql_parser import Limit

    lp = LogicalPlan(
        projections=[],
        predicates=[],
        joins=[],
        aggregations=None,
        limit=Limit(count=source_limit, offset=None),
        ordering=None,
        distinct=False,
        source_tables=[
            TableReference(name="test_table", schema=None, alias=None, source_type="from")
        ],
    )
    cj = _make_compiled_joint(lp)
    group = _make_fused_group(per_joint_limits={"test_source": xg_limit})

    pushdown = _merge_source_limit_into_pushdown(None, cj)
    pushdown = _merge_cross_group_limits(pushdown, group, "test_source")

    assert pushdown is not None
    assert pushdown.limit.pushed_limit == min(source_limit, xg_limit)


# Feature: source-inline-transforms, Property 13: Source and cross-group limits take the minimum
@given(source_limit=st.integers(min_value=1, max_value=10000))
@settings(max_examples=100)
def test_source_limit_without_cross_group_uses_source(source_limit: int) -> None:
    """When only source limit exists, it is used as-is."""
    from rivet_core.sql_parser import Limit

    lp = LogicalPlan(
        projections=[],
        predicates=[],
        joins=[],
        aggregations=None,
        limit=Limit(count=source_limit, offset=None),
        ordering=None,
        distinct=False,
        source_tables=[
            TableReference(name="test_table", schema=None, alias=None, source_type="from")
        ],
    )
    cj = _make_compiled_joint(lp)
    group = _make_fused_group()  # No cross-group limit

    pushdown = _merge_source_limit_into_pushdown(None, cj)
    pushdown = _merge_cross_group_limits(pushdown, group, "test_source")

    assert pushdown is not None
    assert pushdown.limit.pushed_limit == source_limit


# Feature: source-inline-transforms, Property 13: Source and cross-group limits take the minimum
@given(xg_limit=st.integers(min_value=1, max_value=10000))
@settings(max_examples=100)
def test_cross_group_limit_without_source_uses_cross_group(xg_limit: int) -> None:
    """When only cross-group limit exists, it is used as-is."""
    lp = _make_logical_plan()  # No source limit
    cj = _make_compiled_joint(lp)
    group = _make_fused_group(per_joint_limits={"test_source": xg_limit})

    pushdown = _merge_source_limit_into_pushdown(None, cj)
    pushdown = _merge_cross_group_limits(pushdown, group, "test_source")

    assert pushdown is not None
    assert pushdown.limit.pushed_limit == xg_limit
