"""Property-based tests for cross-group predicate pushdown in the optimizer.

Covers Properties 1, 2, and 3 from the cross-group predicate pushdown design document.

- Property 1: Single-origin pushable conjuncts reach the correct source group
  Validates: Requirements 1.1, 2.1, 2.2, 4.1, 4.2, 6.1
- Property 2: Non-pushable conjuncts are never propagated
  Validates: Requirements 1.2, 1.3, 2.3, 2.4, 2.5, 6.2, 7.1, 7.2, 7.3, 7.4
- Property 3: Predicate expression rewriting preserves column semantics
  Validates: Requirements 3.1, 3.2, 3.3
"""

from __future__ import annotations

import uuid

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.compiler import CompiledJoint
from rivet_core.lineage import ColumnLineage, ColumnOrigin
from rivet_core.optimizer import (
    FusedGroup,
    cross_group_pushdown_pass,
)
from rivet_core.sql_parser import (
    Join,
    LogicalPlan,
    Ordering,
    Predicate,
)

# ---------------------------------------------------------------------------
# Helpers — build minimal CompiledJoint stubs
# ---------------------------------------------------------------------------

_EMPTY_LOGICAL_PLAN = LogicalPlan(
    projections=[],
    predicates=[],
    joins=[],
    aggregations=None,
    limit=None,
    ordering=None,
    distinct=False,
    source_tables=[],
)


def _make_compiled_joint(
    name: str,
    *,
    joint_type: str = "source",
    upstream: list[str] | None = None,
    engine: str = "eng1",
    engine_type: str = "databricks",
    logical_plan: LogicalPlan | None = None,
    column_lineage: list[ColumnLineage] | None = None,
    catalog: str | None = None,
    catalog_type: str | None = None,
    adapter: str | None = None,
) -> CompiledJoint:
    return CompiledJoint(
        name=name,
        type=joint_type,
        catalog=catalog,
        catalog_type=catalog_type,
        engine=engine,
        engine_resolution=None,
        adapter=adapter,
        sql=None,
        sql_translated=None,
        sql_resolved=None,
        sql_dialect=None,
        engine_dialect=None,
        upstream=upstream or [],
        eager=False,
        table=None,
        write_strategy=None,
        function=None,
        source_file=None,
        logical_plan=logical_plan,
        output_schema=None,
        column_lineage=column_lineage or [],
        optimizations=[],
        checks=[],
        fused_group_id=None,
        tags=[],
        description=None,
        fusion_strategy_override=None,
        materialization_strategy_override=None,
    )


def _make_group(
    joints: list[str],
    *,
    engine: str = "eng1",
    engine_type: str = "databricks",
    entry_joints: list[str] | None = None,
    exit_joints: list[str] | None = None,
    adapters: dict[str, str | None] | None = None,
) -> FusedGroup:
    return FusedGroup(
        id=str(uuid.uuid4()),
        joints=joints,
        engine=engine,
        engine_type=engine_type,
        adapters=adapters or {j: None for j in joints},
        fused_sql=None,
        entry_joints=entry_joints or joints[:1],
        exit_joints=exit_joints or joints[-1:],
    )


# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------

# Column names: simple lowercase identifiers
_col_name_st = st.from_regex(r"[a-z]{3,8}", fullmatch=True)


@st.composite
def _single_origin_pushable_scenario(draw: st.DrawFn) -> dict:
    """Generate a scenario with a consumer group whose WHERE predicate traces
    through direct/renamed lineage to a single upstream source joint.

    Returns a dict with keys:
        groups, compiled_joints, capabilities, catalog_types,
        source_joint_name, consumer_col, source_col, transform
    """
    # Generate column names
    source_col = draw(_col_name_st)
    transform = draw(st.sampled_from(["direct", "renamed"]))
    consumer_col = draw(_col_name_st.filter(lambda c: c != source_col)) if transform == "renamed" else source_col

    # Source joint
    source_name = "src_joint"
    source_cj = _make_compiled_joint(
        source_name,
        joint_type="source",
        engine_type="databricks",
        column_lineage=[
            ColumnLineage(
                output_column=source_col,
                transform="source",
                origins=[],
                expression=None,
            ),
        ],
    )

    # Consumer joint — its lineage traces consumer_col back to source_col on source_name
    consumer_name = "consumer_joint"
    pred_expr = f"{consumer_col} = 'test_value'"
    consumer_plan = LogicalPlan(
        projections=[],
        predicates=[
            Predicate(expression=pred_expr, columns=[consumer_col], location="where"),
        ],
        joins=[],
        aggregations=None,
        limit=None,
        ordering=None,
        distinct=False,
        source_tables=[],
    )
    consumer_cj = _make_compiled_joint(
        consumer_name,
        joint_type="sql",
        upstream=[source_name],
        engine="eng2",
        engine_type="polars",
        logical_plan=consumer_plan,
        column_lineage=[
            ColumnLineage(
                output_column=consumer_col,
                transform=transform,
                origins=[ColumnOrigin(joint=source_name, column=source_col)],
                expression=None,
            ),
        ],
    )

    # Groups
    source_group = _make_group(
        [source_name],
        engine_type="databricks",
        entry_joints=[source_name],
        exit_joints=[source_name],
    )
    consumer_group = _make_group(
        [consumer_name],
        engine="eng2",
        engine_type="polars",
        entry_joints=[consumer_name],
        exit_joints=[consumer_name],
    )

    compiled_joints = {source_name: source_cj, consumer_name: consumer_cj}
    capabilities = {"databricks": ["predicate_pushdown"]}
    catalog_types: dict[str, str | None] = {source_name: None, consumer_name: None}

    return {
        "groups": [source_group, consumer_group],
        "compiled_joints": compiled_joints,
        "capabilities": capabilities,
        "catalog_types": catalog_types,
        "source_joint_name": source_name,
        "source_group_id": source_group.id,
        "consumer_col": consumer_col,
        "source_col": source_col,
        "transform": transform,
    }


@st.composite
def _non_pushable_scenario(draw: st.DrawFn) -> dict:
    """Generate a scenario with a consumer group whose predicate is NOT pushable.

    The reason for non-pushability is drawn from:
    - HAVING location
    - subquery in expression
    - no lineage for the column
    - aggregation/window/expression/multi_column/opaque transform
    - multi-source origins
    """
    reason = draw(st.sampled_from([
        "having", "subquery", "no_lineage",
        "aggregation", "window", "expression", "multi_column", "opaque",
        "multi_source",
    ]))

    source_a = "src_a"
    source_b = "src_b"
    consumer_name = "consumer_joint"
    col = "test_col"

    # Build predicate based on reason
    if reason == "having":
        pred = Predicate(expression=f"{col} > 5", columns=[col], location="having")
        lineage = [ColumnLineage(
            output_column=col, transform="direct",
            origins=[ColumnOrigin(joint=source_a, column=col)], expression=None,
        )]
    elif reason == "subquery":
        pred = Predicate(
            expression=f"{col} IN (SELECT id FROM other)",
            columns=[col], location="where",
        )
        lineage = [ColumnLineage(
            output_column=col, transform="direct",
            origins=[ColumnOrigin(joint=source_a, column=col)], expression=None,
        )]
    elif reason == "no_lineage":
        pred = Predicate(expression=f"{col} = 'x'", columns=[col], location="where")
        lineage = []  # No lineage for the column
    elif reason == "multi_source":
        pred = Predicate(
            expression=f"{col} = other_col",
            columns=[col, "other_col"], location="where",
        )
        lineage = [
            ColumnLineage(
                output_column=col, transform="direct",
                origins=[ColumnOrigin(joint=source_a, column=col)], expression=None,
            ),
            ColumnLineage(
                output_column="other_col", transform="direct",
                origins=[ColumnOrigin(joint=source_b, column="other_col")], expression=None,
            ),
        ]
    else:
        # aggregation, window, expression, multi_column, opaque
        pred = Predicate(expression=f"{col} > 5", columns=[col], location="where")
        lineage = [ColumnLineage(
            output_column=col, transform=reason,
            origins=[ColumnOrigin(joint=source_a, column=col)], expression=None,
        )]

    consumer_plan = LogicalPlan(
        projections=[], predicates=[pred], joins=[],
        aggregations=None, limit=None, ordering=None,
        distinct=False, source_tables=[],
    )
    consumer_cj = _make_compiled_joint(
        consumer_name, joint_type="sql", upstream=[source_a],
        engine="eng2", engine_type="polars",
        logical_plan=consumer_plan, column_lineage=lineage,
    )
    source_a_cj = _make_compiled_joint(source_a, joint_type="source", engine_type="databricks")
    source_b_cj = _make_compiled_joint(source_b, joint_type="source", engine_type="databricks")

    source_group_a = _make_group([source_a], engine_type="databricks")
    source_group_b = _make_group([source_b], engine_type="databricks")
    consumer_group = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        entry_joints=[consumer_name], exit_joints=[consumer_name],
    )

    compiled_joints = {
        source_a: source_a_cj, source_b: source_b_cj, consumer_name: consumer_cj,
    }
    capabilities = {"databricks": ["predicate_pushdown"]}
    catalog_types: dict[str, str | None] = {
        source_a: None, source_b: None, consumer_name: None,
    }

    return {
        "groups": [source_group_a, source_group_b, consumer_group],
        "compiled_joints": compiled_joints,
        "capabilities": capabilities,
        "catalog_types": catalog_types,
        "reason": reason,
    }


@st.composite
def _rewrite_scenario(draw: st.DrawFn) -> dict:
    """Generate a scenario with a single-origin predicate that has a known
    column rename and/or table alias, for testing expression rewriting.
    """
    source_col = draw(_col_name_st)
    consumer_col = draw(_col_name_st.filter(lambda c: c != source_col and c not in source_col and source_col not in c))
    use_alias = draw(st.booleans())

    source_name = "src_joint"
    consumer_name = "consumer_joint"

    # Build predicate expression — optionally with table alias
    if use_alias:
        alias = draw(st.from_regex(r"t[0-9]", fullmatch=True))
        pred_expr = f"{alias}.{consumer_col} = 'val'"
    else:
        pred_expr = f"{consumer_col} = 'val'"

    pred = Predicate(expression=pred_expr, columns=[consumer_col], location="where")

    consumer_plan = LogicalPlan(
        projections=[], predicates=[pred], joins=[],
        aggregations=None, limit=None, ordering=None,
        distinct=False, source_tables=[],
    )
    consumer_cj = _make_compiled_joint(
        consumer_name, joint_type="sql", upstream=[source_name],
        engine="eng2", engine_type="polars",
        logical_plan=consumer_plan,
        column_lineage=[
            ColumnLineage(
                output_column=consumer_col, transform="renamed",
                origins=[ColumnOrigin(joint=source_name, column=source_col)],
                expression=None,
            ),
        ],
    )
    source_cj = _make_compiled_joint(
        source_name, joint_type="source", engine_type="databricks",
        column_lineage=[
            ColumnLineage(
                output_column=source_col, transform="source",
                origins=[], expression=None,
            ),
        ],
    )

    source_group = _make_group(
        [source_name], engine_type="databricks",
        entry_joints=[source_name], exit_joints=[source_name],
    )
    consumer_group = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        entry_joints=[consumer_name], exit_joints=[consumer_name],
    )

    compiled_joints = {source_name: source_cj, consumer_name: consumer_cj}
    capabilities = {"databricks": ["predicate_pushdown"]}
    catalog_types: dict[str, str | None] = {source_name: None, consumer_name: None}

    return {
        "groups": [source_group, consumer_group],
        "compiled_joints": compiled_joints,
        "capabilities": capabilities,
        "catalog_types": catalog_types,
        "source_joint_name": source_name,
        "source_group_id": source_group.id,
        "consumer_col": consumer_col,
        "source_col": source_col,
        "use_alias": use_alias,
    }


# ---------------------------------------------------------------------------
# Property Tests
# ---------------------------------------------------------------------------


# Feature: cross-group-predicate-pushdown
# Property 1: Single-origin pushable conjuncts reach the correct source group
@given(scenario=_single_origin_pushable_scenario())
@settings(max_examples=100)
def test_property1_single_origin_pushable_reaches_correct_source(
    scenario: dict,
) -> None:
    """For any consumer group with WHERE predicates whose columns all trace
    through direct/renamed lineage to a single upstream source joint with
    predicate_pushdown capability, the source group's per_joint_predicates
    shall contain a predicate for the target joint with source column names.

    Validates: Requirements 1.1, 2.1, 2.2, 4.1, 4.2, 6.1
    """
    new_groups, results = cross_group_pushdown_pass(
        scenario["groups"],
        scenario["compiled_joints"],
        scenario["capabilities"],
        scenario["catalog_types"],
    )

    source_joint = scenario["source_joint_name"]
    source_col = scenario["source_col"]
    source_gid = scenario["source_group_id"]

    # Find the updated source group
    source_group = next(g for g in new_groups if g.id == source_gid)

    # The source group must have per_joint_predicates for the source joint
    assert source_joint in source_group.per_joint_predicates, (
        f"Expected per_joint_predicates for '{source_joint}' but got "
        f"{source_group.per_joint_predicates}"
    )

    pushed_preds = source_group.per_joint_predicates[source_joint]
    assert len(pushed_preds) >= 1, "Expected at least one pushed predicate"

    # The pushed predicate should reference the source column name
    pushed = pushed_preds[0]
    assert source_col in pushed.columns, (
        f"Expected source column '{source_col}' in pushed predicate columns "
        f"{pushed.columns}"
    )
    assert source_col in pushed.expression, (
        f"Expected source column '{source_col}' in pushed expression "
        f"'{pushed.expression}'"
    )

    # There should be an 'applied' OptimizationResult
    applied = [r for r in results if r.status == "applied"]
    assert len(applied) >= 1, (
        f"Expected at least one 'applied' result but got {results}"
    )


# Feature: cross-group-predicate-pushdown
# Property 2: Non-pushable conjuncts are never propagated
@given(scenario=_non_pushable_scenario())
@settings(max_examples=100)
def test_property2_non_pushable_conjuncts_never_propagated(
    scenario: dict,
) -> None:
    """For any conjunct that has location='having', contains a subquery,
    references a column with no lineage, references a column with a
    non-direct/non-renamed transform, or traces to multiple source joints,
    the cross-group pushdown pass shall not add that conjunct to any source
    group's per_joint_predicates.

    Validates: Requirements 1.2, 1.3, 2.3, 2.4, 2.5, 6.2, 7.1, 7.2, 7.3, 7.4
    """
    new_groups, results = cross_group_pushdown_pass(
        scenario["groups"],
        scenario["compiled_joints"],
        scenario["capabilities"],
        scenario["catalog_types"],
    )

    # No source group should have any per_joint_predicates
    for group in new_groups:
        assert not group.per_joint_predicates, (
            f"Non-pushable predicate (reason={scenario['reason']}) was "
            f"propagated to group '{group.id}': {group.per_joint_predicates}"
        )

    # There should be at least one 'skipped' result (no 'applied')
    applied = [r for r in results if r.status == "applied"]
    assert len(applied) == 0, (
        f"Non-pushable predicate (reason={scenario['reason']}) produced "
        f"'applied' results: {applied}"
    )


# Feature: cross-group-predicate-pushdown
# Property 3: Predicate expression rewriting preserves column semantics
@given(scenario=_rewrite_scenario())
@settings(max_examples=100)
def test_property3_rewriting_preserves_column_semantics(
    scenario: dict,
) -> None:
    """For any single-origin pushable conjunct where the lineage maps consumer
    column c to source column s, the rewritten predicate expression shall
    contain s in place of c, and the columns list shall contain only source
    column names.

    Validates: Requirements 3.1, 3.2, 3.3
    """
    new_groups, results = cross_group_pushdown_pass(
        scenario["groups"],
        scenario["compiled_joints"],
        scenario["capabilities"],
        scenario["catalog_types"],
    )

    source_joint = scenario["source_joint_name"]
    source_col = scenario["source_col"]
    consumer_col = scenario["consumer_col"]
    source_gid = scenario["source_group_id"]

    # Find the updated source group
    source_group = next(g for g in new_groups if g.id == source_gid)

    assert source_joint in source_group.per_joint_predicates, (
        f"Expected per_joint_predicates for '{source_joint}'"
    )

    pushed = source_group.per_joint_predicates[source_joint][0]

    # The rewritten expression must contain the source column, not the consumer column
    assert source_col in pushed.expression, (
        f"Expected source column '{source_col}' in rewritten expression "
        f"'{pushed.expression}'"
    )
    # Consumer column should NOT appear (since it was renamed)
    assert consumer_col not in pushed.expression, (
        f"Consumer column '{consumer_col}' should not appear in rewritten "
        f"expression '{pushed.expression}'"
    )

    # The columns list must contain only source column names
    assert pushed.columns == [source_col], (
        f"Expected columns=['{source_col}'] but got {pushed.columns}"
    )

    # Table alias should be stripped
    assert "." not in pushed.expression.split("=")[0].strip(), (
        f"Table alias not stripped from rewritten expression '{pushed.expression}'"
    )


# ---------------------------------------------------------------------------
# Additional Hypothesis strategies for Properties 4–11
# ---------------------------------------------------------------------------


@st.composite
def _incapable_adapter_scenario(draw: st.DrawFn) -> dict:
    """Generate a scenario with a single-origin pushable conjunct targeting a
    source joint whose adapter lacks predicate_pushdown capability.

    Returns a dict with keys:
        groups, compiled_joints, capabilities, catalog_types,
        source_joint_name, source_group_id
    """
    source_col = draw(_col_name_st)
    consumer_col = source_col  # direct lineage for simplicity

    source_name = "src_joint"
    consumer_name = "consumer_joint"

    source_cj = _make_compiled_joint(
        source_name, joint_type="source", engine_type="duckdb",
        column_lineage=[
            ColumnLineage(
                output_column=source_col, transform="source",
                origins=[], expression=None,
            ),
        ],
    )

    consumer_plan = LogicalPlan(
        projections=[], joins=[],
        predicates=[
            Predicate(expression=f"{consumer_col} = 'v'", columns=[consumer_col], location="where"),
        ],
        aggregations=None, limit=None, ordering=None,
        distinct=False, source_tables=[],
    )
    consumer_cj = _make_compiled_joint(
        consumer_name, joint_type="sql", upstream=[source_name],
        engine="eng2", engine_type="polars",
        logical_plan=consumer_plan,
        column_lineage=[
            ColumnLineage(
                output_column=consumer_col, transform="direct",
                origins=[ColumnOrigin(joint=source_name, column=source_col)],
                expression=None,
            ),
        ],
    )

    source_group = _make_group(
        [source_name], engine_type="duckdb",
        entry_joints=[source_name], exit_joints=[source_name],
    )
    consumer_group = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        entry_joints=[consumer_name], exit_joints=[consumer_name],
    )

    compiled_joints = {source_name: source_cj, consumer_name: consumer_cj}
    # duckdb does NOT have predicate_pushdown
    capabilities: dict[str, list[str]] = {"duckdb": [], "polars": []}
    catalog_types: dict[str, str | None] = {source_name: None, consumer_name: None}

    return {
        "groups": [source_group, consumer_group],
        "compiled_joints": compiled_joints,
        "capabilities": capabilities,
        "catalog_types": catalog_types,
        "source_joint_name": source_name,
        "source_group_id": source_group.id,
    }


@st.composite
def _existing_intra_group_predicates_scenario(draw: st.DrawFn) -> dict:
    """Generate a scenario where the source group already has per_joint_predicates
    from a prior intra-group pass, and a cross-group pushable predicate targets it.

    Returns a dict with keys:
        groups, compiled_joints, capabilities, catalog_types,
        source_joint_name, source_group_id, existing_predicates
    """
    source_col = draw(_col_name_st)
    consumer_col = source_col

    source_name = "src_joint"
    consumer_name = "consumer_joint"

    source_cj = _make_compiled_joint(
        source_name, joint_type="source", engine_type="databricks",
        column_lineage=[
            ColumnLineage(
                output_column=source_col, transform="source",
                origins=[], expression=None,
            ),
        ],
    )

    consumer_plan = LogicalPlan(
        projections=[], joins=[],
        predicates=[
            Predicate(expression=f"{consumer_col} = 'new'", columns=[consumer_col], location="where"),
        ],
        aggregations=None, limit=None, ordering=None,
        distinct=False, source_tables=[],
    )
    consumer_cj = _make_compiled_joint(
        consumer_name, joint_type="sql", upstream=[source_name],
        engine="eng2", engine_type="polars",
        logical_plan=consumer_plan,
        column_lineage=[
            ColumnLineage(
                output_column=consumer_col, transform="direct",
                origins=[ColumnOrigin(joint=source_name, column=source_col)],
                expression=None,
            ),
        ],
    )

    # Existing intra-group predicates on the source group
    existing_pred = Predicate(
        expression=f"{source_col} > 0", columns=[source_col], location="where",
    )
    existing_predicates = [existing_pred]

    source_group = _make_group(
        [source_name], engine_type="databricks",
        entry_joints=[source_name], exit_joints=[source_name],
    )
    # Pre-populate per_joint_predicates to simulate intra-group pass output
    from dataclasses import replace
    source_group = replace(
        source_group,
        per_joint_predicates={source_name: list(existing_predicates)},
    )
    consumer_group = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        entry_joints=[consumer_name], exit_joints=[consumer_name],
    )

    compiled_joints = {source_name: source_cj, consumer_name: consumer_cj}
    capabilities = {"databricks": ["predicate_pushdown"]}
    catalog_types: dict[str, str | None] = {source_name: None, consumer_name: None}

    return {
        "groups": [source_group, consumer_group],
        "compiled_joints": compiled_joints,
        "capabilities": capabilities,
        "catalog_types": catalog_types,
        "source_joint_name": source_name,
        "source_group_id": source_group.id,
        "existing_predicates": existing_predicates,
    }


@st.composite
def _consumer_predicates_unchanged_scenario(draw: st.DrawFn) -> dict:
    """Generate a consumer group with WHERE predicates and verify they are
    unchanged after the cross-group pass.

    Returns a dict with keys:
        groups, compiled_joints, capabilities, catalog_types,
        consumer_group_id, original_predicates
    """
    source_col = draw(_col_name_st)
    consumer_col = source_col

    source_name = "src_joint"
    consumer_name = "consumer_joint"

    pred = Predicate(
        expression=f"{consumer_col} = 'keep'", columns=[consumer_col], location="where",
    )

    source_cj = _make_compiled_joint(
        source_name, joint_type="source", engine_type="databricks",
        column_lineage=[
            ColumnLineage(
                output_column=source_col, transform="source",
                origins=[], expression=None,
            ),
        ],
    )

    consumer_plan = LogicalPlan(
        projections=[], joins=[], predicates=[pred],
        aggregations=None, limit=None, ordering=None,
        distinct=False, source_tables=[],
    )
    consumer_cj = _make_compiled_joint(
        consumer_name, joint_type="sql", upstream=[source_name],
        engine="eng2", engine_type="polars",
        logical_plan=consumer_plan,
        column_lineage=[
            ColumnLineage(
                output_column=consumer_col, transform="direct",
                origins=[ColumnOrigin(joint=source_name, column=source_col)],
                expression=None,
            ),
        ],
    )

    source_group = _make_group(
        [source_name], engine_type="databricks",
        entry_joints=[source_name], exit_joints=[source_name],
    )
    consumer_group = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        entry_joints=[consumer_name], exit_joints=[consumer_name],
    )

    compiled_joints = {source_name: source_cj, consumer_name: consumer_cj}
    capabilities = {"databricks": ["predicate_pushdown"]}
    catalog_types: dict[str, str | None] = {source_name: None, consumer_name: None}

    return {
        "groups": [source_group, consumer_group],
        "compiled_joints": compiled_joints,
        "capabilities": capabilities,
        "catalog_types": catalog_types,
        "consumer_group_id": consumer_group.id,
        "consumer_joint_name": consumer_name,
        "original_predicates": [pred],
    }


@st.composite
def _distinct_orderby_scenario(draw: st.DrawFn) -> dict:
    """Generate a consumer group with DISTINCT and/or ORDER BY and an eligible
    single-origin predicate.

    Returns a dict with keys:
        groups, compiled_joints, capabilities, catalog_types,
        source_joint_name, source_group_id, has_distinct, has_ordering
    """
    source_col = draw(_col_name_st)
    consumer_col = source_col

    source_name = "src_joint"
    consumer_name = "consumer_joint"

    has_distinct = draw(st.booleans())
    has_ordering = draw(st.booleans())
    # Ensure at least one is True
    if not has_distinct and not has_ordering:
        has_distinct = True

    ordering = Ordering(columns=[(consumer_col, "asc")]) if has_ordering else None

    source_cj = _make_compiled_joint(
        source_name, joint_type="source", engine_type="databricks",
        column_lineage=[
            ColumnLineage(
                output_column=source_col, transform="source",
                origins=[], expression=None,
            ),
        ],
    )

    consumer_plan = LogicalPlan(
        projections=[], joins=[],
        predicates=[
            Predicate(expression=f"{consumer_col} = 'v'", columns=[consumer_col], location="where"),
        ],
        aggregations=None, limit=None,
        ordering=ordering,
        distinct=has_distinct,
        source_tables=[],
    )
    consumer_cj = _make_compiled_joint(
        consumer_name, joint_type="sql", upstream=[source_name],
        engine="eng2", engine_type="polars",
        logical_plan=consumer_plan,
        column_lineage=[
            ColumnLineage(
                output_column=consumer_col, transform="direct",
                origins=[ColumnOrigin(joint=source_name, column=source_col)],
                expression=None,
            ),
        ],
    )

    source_group = _make_group(
        [source_name], engine_type="databricks",
        entry_joints=[source_name], exit_joints=[source_name],
    )
    consumer_group = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        entry_joints=[consumer_name], exit_joints=[consumer_name],
    )

    compiled_joints = {source_name: source_cj, consumer_name: consumer_cj}
    capabilities = {"databricks": ["predicate_pushdown"]}
    catalog_types: dict[str, str | None] = {source_name: None, consumer_name: None}

    return {
        "groups": [source_group, consumer_group],
        "compiled_joints": compiled_joints,
        "capabilities": capabilities,
        "catalog_types": catalog_types,
        "source_joint_name": source_name,
        "source_group_id": source_group.id,
        "has_distinct": has_distinct,
        "has_ordering": has_ordering,
    }


@st.composite
def _optimization_result_per_conjunct_scenario(draw: st.DrawFn) -> dict:
    """Generate a consumer group with 1–3 WHERE conjuncts (mix of pushable and
    non-pushable) and verify each produces exactly one OptimizationResult.

    Returns a dict with keys:
        groups, compiled_joints, capabilities, catalog_types, num_conjuncts
    """
    source_name = "src_joint"
    consumer_name = "consumer_joint"

    # Generate 1–3 conjuncts
    num_pushable = draw(st.integers(min_value=0, max_value=2))
    num_non_pushable = draw(st.integers(min_value=0, max_value=2))
    if num_pushable == 0 and num_non_pushable == 0:
        num_pushable = 1

    predicates: list[Predicate] = []
    lineage: list[ColumnLineage] = []

    for i in range(num_pushable):
        col = f"pcol{i}"
        predicates.append(
            Predicate(expression=f"{col} = 'v{i}'", columns=[col], location="where"),
        )
        lineage.append(ColumnLineage(
            output_column=col, transform="direct",
            origins=[ColumnOrigin(joint=source_name, column=col)],
            expression=None,
        ))

    for i in range(num_non_pushable):
        col = f"npcol{i}"
        predicates.append(
            Predicate(expression=f"{col} > {i}", columns=[col], location="having"),
        )
        lineage.append(ColumnLineage(
            output_column=col, transform="direct",
            origins=[ColumnOrigin(joint=source_name, column=col)],
            expression=None,
        ))

    source_cj = _make_compiled_joint(
        source_name, joint_type="source", engine_type="databricks",
    )

    consumer_plan = LogicalPlan(
        projections=[], joins=[], predicates=predicates,
        aggregations=None, limit=None, ordering=None,
        distinct=False, source_tables=[],
    )
    consumer_cj = _make_compiled_joint(
        consumer_name, joint_type="sql", upstream=[source_name],
        engine="eng2", engine_type="polars",
        logical_plan=consumer_plan, column_lineage=lineage,
    )

    source_group = _make_group(
        [source_name], engine_type="databricks",
        entry_joints=[source_name], exit_joints=[source_name],
    )
    consumer_group = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        entry_joints=[consumer_name], exit_joints=[consumer_name],
    )

    compiled_joints = {source_name: source_cj, consumer_name: consumer_cj}
    capabilities = {"databricks": ["predicate_pushdown"]}
    catalog_types: dict[str, str | None] = {source_name: None, consumer_name: None}

    return {
        "groups": [source_group, consumer_group],
        "compiled_joints": compiled_joints,
        "capabilities": capabilities,
        "catalog_types": catalog_types,
        "num_conjuncts": num_pushable + num_non_pushable,
    }


@st.composite
def _join_equality_scenario(draw: st.DrawFn) -> dict:
    """Generate a consumer group with an INNER JOIN equality condition (A = B)
    and a single-origin pushable predicate on column A.

    Optionally generates a second INNER JOIN equality (A = C) to test multiple
    derived predicates.

    Returns a dict with keys:
        groups, compiled_joints, capabilities, catalog_types,
        source_a_name, source_b_name, source_a_group_id, source_b_group_id,
        col_a, col_b, has_second_join, source_c_name, source_c_group_id, col_c
    """
    col_a = draw(_col_name_st)
    # col_b must differ from col_a so lineage lookup resolves to the correct source
    col_b = draw(_col_name_st.filter(lambda c: c != col_a))
    has_second_join = draw(st.booleans())
    col_c = draw(_col_name_st.filter(lambda c: c not in (col_a, col_b))) if has_second_join else None

    source_a = "src_a"
    source_b = "src_b"
    source_c = "src_c"
    consumer_name = "consumer_joint"

    source_a_cj = _make_compiled_joint(
        source_a, joint_type="source", engine_type="databricks",
        column_lineage=[
            ColumnLineage(output_column=col_a, transform="source", origins=[], expression=None),
        ],
    )
    source_b_cj = _make_compiled_joint(
        source_b, joint_type="source", engine_type="databricks",
        column_lineage=[
            ColumnLineage(output_column=col_b, transform="source", origins=[], expression=None),
        ],
    )

    joins = [
        Join(type="inner", left_table="a", right_table="b",
             condition=f"a.{col_a} = b.{col_b}", columns=[col_a, col_b]),
    ]
    consumer_lineage = [
        ColumnLineage(
            output_column=col_a, transform="direct",
            origins=[ColumnOrigin(joint=source_a, column=col_a)], expression=None,
        ),
        ColumnLineage(
            output_column=col_b, transform="direct",
            origins=[ColumnOrigin(joint=source_b, column=col_b)], expression=None,
        ),
    ]

    source_c_cj = None
    source_c_group = None
    if has_second_join:
        source_c_cj = _make_compiled_joint(
            source_c, joint_type="source", engine_type="databricks",
            column_lineage=[
                ColumnLineage(output_column=col_c, transform="source", origins=[], expression=None),
            ],
        )
        joins.append(
            Join(type="inner", left_table="a", right_table="c",
                 condition=f"a.{col_a} = c.{col_c}", columns=[col_a, col_c]),
        )
        consumer_lineage.append(
            ColumnLineage(
                output_column=col_c, transform="direct",
                origins=[ColumnOrigin(joint=source_c, column=col_c)], expression=None,
            ),
        )

    consumer_plan = LogicalPlan(
        projections=[], joins=joins,
        predicates=[
            Predicate(expression=f"{col_a} = 'val'", columns=[col_a], location="where"),
        ],
        aggregations=None, limit=None, ordering=None,
        distinct=False, source_tables=[],
    )
    upstream = [source_a, source_b] + ([source_c] if has_second_join else [])
    consumer_cj = _make_compiled_joint(
        consumer_name, joint_type="sql", upstream=upstream,
        engine="eng2", engine_type="polars",
        logical_plan=consumer_plan, column_lineage=consumer_lineage,
    )

    source_a_group = _make_group(
        [source_a], engine_type="databricks",
        entry_joints=[source_a], exit_joints=[source_a],
    )
    source_b_group = _make_group(
        [source_b], engine_type="databricks",
        entry_joints=[source_b], exit_joints=[source_b],
    )
    consumer_group = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        entry_joints=[consumer_name], exit_joints=[consumer_name],
    )

    groups = [source_a_group, source_b_group, consumer_group]
    compiled_joints: dict[str, CompiledJoint] = {
        source_a: source_a_cj, source_b: source_b_cj, consumer_name: consumer_cj,
    }
    catalog_types: dict[str, str | None] = {
        source_a: None, source_b: None, consumer_name: None,
    }

    if has_second_join and source_c_cj is not None:
        source_c_group = _make_group(
            [source_c], engine_type="databricks",
            entry_joints=[source_c], exit_joints=[source_c],
        )
        groups.insert(2, source_c_group)
        compiled_joints[source_c] = source_c_cj
        catalog_types[source_c] = None

    capabilities = {"databricks": ["predicate_pushdown"]}

    return {
        "groups": groups,
        "compiled_joints": compiled_joints,
        "capabilities": capabilities,
        "catalog_types": catalog_types,
        "source_a_name": source_a,
        "source_b_name": source_b,
        "source_a_group_id": source_a_group.id,
        "source_b_group_id": source_b_group.id,
        "col_a": col_a,
        "col_b": col_b,
        "has_second_join": has_second_join,
        "source_c_name": source_c if has_second_join else None,
        "source_c_group_id": source_c_group.id if source_c_group else None,
        "col_c": col_c,
    }


@st.composite
def _non_inner_join_scenario(draw: st.DrawFn) -> dict:
    """Generate a consumer group with a non-INNER join or expression-based
    INNER JOIN condition, plus a single-origin pushable predicate.

    Returns a dict with keys:
        groups, compiled_joints, capabilities, catalog_types,
        source_a_name, source_b_name, source_b_group_id, reason
    """
    reason = draw(st.sampled_from([
        "left", "right", "full", "cross", "expression_inner",
    ]))

    col_a = "cola"
    col_b = "colb"
    source_a = "src_a"
    source_b = "src_b"
    consumer_name = "consumer_joint"

    if reason == "cross":
        join = Join(type="cross", left_table="a", right_table="b",
                    condition=None, columns=[])
    elif reason == "expression_inner":
        join = Join(type="inner", left_table="a", right_table="b",
                    condition=f"UPPER(a.{col_a}) = b.{col_b}", columns=[col_a, col_b])
    else:
        join = Join(type=reason, left_table="a", right_table="b",
                    condition=f"a.{col_a} = b.{col_b}", columns=[col_a, col_b])

    source_a_cj = _make_compiled_joint(
        source_a, joint_type="source", engine_type="databricks",
        column_lineage=[
            ColumnLineage(output_column=col_a, transform="source", origins=[], expression=None),
        ],
    )
    source_b_cj = _make_compiled_joint(
        source_b, joint_type="source", engine_type="databricks",
        column_lineage=[
            ColumnLineage(output_column=col_b, transform="source", origins=[], expression=None),
        ],
    )

    consumer_plan = LogicalPlan(
        projections=[], joins=[join],
        predicates=[
            Predicate(expression=f"{col_a} = 'val'", columns=[col_a], location="where"),
        ],
        aggregations=None, limit=None, ordering=None,
        distinct=False, source_tables=[],
    )
    consumer_cj = _make_compiled_joint(
        consumer_name, joint_type="sql", upstream=[source_a, source_b],
        engine="eng2", engine_type="polars",
        logical_plan=consumer_plan,
        column_lineage=[
            ColumnLineage(
                output_column=col_a, transform="direct",
                origins=[ColumnOrigin(joint=source_a, column=col_a)], expression=None,
            ),
            ColumnLineage(
                output_column=col_b, transform="direct",
                origins=[ColumnOrigin(joint=source_b, column=col_b)], expression=None,
            ),
        ],
    )

    source_a_group = _make_group(
        [source_a], engine_type="databricks",
        entry_joints=[source_a], exit_joints=[source_a],
    )
    source_b_group = _make_group(
        [source_b], engine_type="databricks",
        entry_joints=[source_b], exit_joints=[source_b],
    )
    consumer_group = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        entry_joints=[consumer_name], exit_joints=[consumer_name],
    )

    compiled_joints = {
        source_a: source_a_cj, source_b: source_b_cj, consumer_name: consumer_cj,
    }
    capabilities = {"databricks": ["predicate_pushdown"]}
    catalog_types: dict[str, str | None] = {
        source_a: None, source_b: None, consumer_name: None,
    }

    return {
        "groups": [source_a_group, source_b_group, consumer_group],
        "compiled_joints": compiled_joints,
        "capabilities": capabilities,
        "catalog_types": catalog_types,
        "source_a_name": source_a,
        "source_b_name": source_b,
        "source_b_group_id": source_b_group.id,
        "reason": reason,
    }


# ---------------------------------------------------------------------------
# Property Tests 4–11
# ---------------------------------------------------------------------------


# Feature: cross-group-predicate-pushdown
# Property 4: Capability gate prevents pushdown to incapable adapters
@given(scenario=_incapable_adapter_scenario())
@settings(max_examples=100)
def test_property4_capability_gate_prevents_pushdown(
    scenario: dict,
) -> None:
    """For any single-origin pushable conjunct targeting a source joint whose
    adapter lacks predicate_pushdown capability, no predicates shall be added
    to that source group and an OptimizationResult with status not_applicable
    shall be recorded.

    Validates: Requirements 4.3, 9.3
    """
    new_groups, results = cross_group_pushdown_pass(
        scenario["groups"],
        scenario["compiled_joints"],
        scenario["capabilities"],
        scenario["catalog_types"],
    )

    source_gid = scenario["source_group_id"]
    source_group = next(g for g in new_groups if g.id == source_gid)

    # No predicates should be pushed to the incapable source
    assert not source_group.per_joint_predicates, (
        f"Predicates were pushed to incapable source group: "
        f"{source_group.per_joint_predicates}"
    )

    # There should be no 'applied' results
    applied = [r for r in results if r.status == "applied"]
    assert len(applied) == 0, f"Unexpected 'applied' results: {applied}"

    # There should be at least one 'not_applicable' result
    not_applicable = [r for r in results if r.status == "not_applicable"]
    assert len(not_applicable) >= 1, (
        f"Expected 'not_applicable' result but got: {results}"
    )


# Feature: cross-group-predicate-pushdown
# Property 5: Cross-group pushdown preserves existing intra-group predicates
@given(scenario=_existing_intra_group_predicates_scenario())
@settings(max_examples=100)
def test_property5_preserves_existing_intra_group_predicates(
    scenario: dict,
) -> None:
    """For any source group that already has per_joint_predicates from a prior
    pass, after the cross-group pushdown pass, all original predicates shall
    still be present.

    Validates: Requirements 4.4, 8.2
    """
    new_groups, results = cross_group_pushdown_pass(
        scenario["groups"],
        scenario["compiled_joints"],
        scenario["capabilities"],
        scenario["catalog_types"],
    )

    source_joint = scenario["source_joint_name"]
    source_gid = scenario["source_group_id"]
    existing = scenario["existing_predicates"]

    source_group = next(g for g in new_groups if g.id == source_gid)

    assert source_joint in source_group.per_joint_predicates, (
        f"Expected per_joint_predicates for '{source_joint}'"
    )

    pushed = source_group.per_joint_predicates[source_joint]

    # All existing predicates must still be present
    for ep in existing:
        assert any(
            p.expression == ep.expression and p.columns == ep.columns
            for p in pushed
        ), (
            f"Existing predicate '{ep.expression}' was lost after cross-group "
            f"pass. Current predicates: {[p.expression for p in pushed]}"
        )

    # There should also be at least one new predicate (the cross-group one)
    assert len(pushed) > len(existing), (
        f"Expected new cross-group predicates to be appended. "
        f"Existing: {len(existing)}, current: {len(pushed)}"
    )


# Feature: cross-group-predicate-pushdown
# Property 6: Consumer group predicates are never modified
@given(scenario=_consumer_predicates_unchanged_scenario())
@settings(max_examples=100)
def test_property6_consumer_predicates_never_modified(
    scenario: dict,
) -> None:
    """For any consumer group, after the cross-group pushdown pass, the
    consumer group's exit joint logical plan predicates shall be identical
    to the predicates before the pass.

    Validates: Requirements 5.1
    """
    consumer_joint_name = scenario["consumer_joint_name"]
    original_preds = scenario["original_predicates"]

    new_groups, _results = cross_group_pushdown_pass(
        scenario["groups"],
        scenario["compiled_joints"],
        scenario["capabilities"],
        scenario["catalog_types"],
    )

    # The compiled_joints dict is passed by reference — verify the consumer
    # joint's logical plan predicates are unchanged
    cj = scenario["compiled_joints"][consumer_joint_name]
    assert cj.logical_plan is not None
    current_preds = cj.logical_plan.predicates

    assert len(current_preds) == len(original_preds), (
        f"Consumer predicate count changed: {len(original_preds)} -> {len(current_preds)}"
    )
    for orig, curr in zip(original_preds, current_preds):
        assert orig.expression == curr.expression, (
            f"Consumer predicate expression changed: '{orig.expression}' -> '{curr.expression}'"
        )
        assert orig.columns == curr.columns, (
            f"Consumer predicate columns changed: {orig.columns} -> {curr.columns}"
        )
        assert orig.location == curr.location, (
            f"Consumer predicate location changed: '{orig.location}' -> '{curr.location}'"
        )


# Feature: cross-group-predicate-pushdown
# Property 7: DISTINCT and ORDER BY do not block cross-group pushdown
@given(scenario=_distinct_orderby_scenario())
@settings(max_examples=100)
def test_property7_distinct_orderby_do_not_block_pushdown(
    scenario: dict,
) -> None:
    """For any consumer group with DISTINCT and/or ORDER BY, the cross-group
    pushdown pass shall still push eligible single-origin predicates.

    Validates: Requirements 7.5, 7.6
    """
    new_groups, results = cross_group_pushdown_pass(
        scenario["groups"],
        scenario["compiled_joints"],
        scenario["capabilities"],
        scenario["catalog_types"],
    )

    source_joint = scenario["source_joint_name"]
    source_gid = scenario["source_group_id"]

    source_group = next(g for g in new_groups if g.id == source_gid)

    # The predicate should still be pushed despite DISTINCT/ORDER BY
    assert source_joint in source_group.per_joint_predicates, (
        f"Predicate not pushed despite DISTINCT={scenario['has_distinct']}, "
        f"ORDER BY={scenario['has_ordering']}"
    )

    pushed = source_group.per_joint_predicates[source_joint]
    assert len(pushed) >= 1, "Expected at least one pushed predicate"

    applied = [r for r in results if r.status == "applied"]
    assert len(applied) >= 1, (
        f"Expected 'applied' result but got: {results}"
    )


# Feature: cross-group-predicate-pushdown
# Property 8: Every processed conjunct produces exactly one OptimizationResult
@given(scenario=_optimization_result_per_conjunct_scenario())
@settings(max_examples=100)
def test_property8_one_optimization_result_per_conjunct(
    scenario: dict,
) -> None:
    """For any consumer group with WHERE predicates, exactly one
    OptimizationResult with rule cross_group_predicate_pushdown shall be
    recorded per conjunct.

    Validates: Requirements 9.1, 9.2, 9.3
    """
    _new_groups, results = cross_group_pushdown_pass(
        scenario["groups"],
        scenario["compiled_joints"],
        scenario["capabilities"],
        scenario["catalog_types"],
    )

    num_conjuncts = scenario["num_conjuncts"]

    # Filter to only cross_group_predicate_pushdown results
    xgroup_results = [
        r for r in results if r.rule == "cross_group_predicate_pushdown"
    ]

    # Each conjunct should produce exactly one result
    assert len(xgroup_results) == num_conjuncts, (
        f"Expected {num_conjuncts} OptimizationResult(s) but got "
        f"{len(xgroup_results)}: {[(r.status, r.detail) for r in xgroup_results]}"
    )

    # Each result should have a valid status
    for r in xgroup_results:
        assert r.status in ("applied", "skipped", "not_applicable"), (
            f"Unexpected status '{r.status}' in result: {r}"
        )


# Feature: cross-group-predicate-pushdown
# Property 9: Input groups are not mutated
@given(scenario=_single_origin_pushable_scenario())
@settings(max_examples=100)
def test_property9_input_groups_not_mutated(
    scenario: dict,
) -> None:
    """For any list of FusedGroup objects passed to cross_group_pushdown_pass,
    the original group objects shall not be modified.

    Validates: Requirements 8.4
    """
    import copy

    groups = scenario["groups"]
    # Deep-copy the groups before the pass
    groups_before = copy.deepcopy(groups)

    _new_groups, _results = cross_group_pushdown_pass(
        groups,
        scenario["compiled_joints"],
        scenario["capabilities"],
        scenario["catalog_types"],
    )

    # Verify original groups are unchanged
    for orig, before in zip(groups, groups_before):
        assert orig.id == before.id
        assert orig.joints == before.joints
        assert orig.engine == before.engine
        assert orig.engine_type == before.engine_type
        assert orig.per_joint_predicates == before.per_joint_predicates, (
            f"Original group '{orig.id}' was mutated: "
            f"per_joint_predicates changed from {before.per_joint_predicates} "
            f"to {orig.per_joint_predicates}"
        )
        assert orig.pushdown == before.pushdown
        assert orig.residual == before.residual


# Feature: cross-group-predicate-pushdown
# Property 10: Join-equality derived predicates reach the correct other-side source group
@given(scenario=_join_equality_scenario())
@settings(max_examples=100)
def test_property10_join_equality_derived_predicates_reach_correct_group(
    scenario: dict,
) -> None:
    """For any consumer group with an INNER JOIN condition A = B and a
    single-origin pushable predicate on column A, the other-side source
    group's per_joint_predicates shall contain a derived predicate for
    column B with source column names.

    Validates: Requirements 10.1, 10.2, 10.5
    """
    new_groups, results = cross_group_pushdown_pass(
        scenario["groups"],
        scenario["compiled_joints"],
        scenario["capabilities"],
        scenario["catalog_types"],
    )

    source_b = scenario["source_b_name"]
    source_b_gid = scenario["source_b_group_id"]
    col_b = scenario["col_b"]

    # Source B should have a derived predicate
    source_b_group = next(g for g in new_groups if g.id == source_b_gid)
    assert source_b in source_b_group.per_joint_predicates, (
        f"Expected derived predicate for '{source_b}' via join equality "
        f"but got {source_b_group.per_joint_predicates}"
    )

    derived_preds = source_b_group.per_joint_predicates[source_b]
    assert len(derived_preds) >= 1, "Expected at least one derived predicate"
    assert any(col_b in p.columns for p in derived_preds), (
        f"Expected column '{col_b}' in derived predicate columns but got "
        f"{[p.columns for p in derived_preds]}"
    )

    # If there's a second join (A = C), source C should also have a derived predicate
    if scenario["has_second_join"]:
        source_c = scenario["source_c_name"]
        source_c_gid = scenario["source_c_group_id"]
        col_c = scenario["col_c"]

        source_c_group = next(g for g in new_groups if g.id == source_c_gid)
        assert source_c in source_c_group.per_joint_predicates, (
            f"Expected derived predicate for '{source_c}' via second join "
            f"equality but got {source_c_group.per_joint_predicates}"
        )

        derived_c = source_c_group.per_joint_predicates[source_c]
        assert any(col_c in p.columns for p in derived_c), (
            f"Expected column '{col_c}' in derived predicate columns for "
            f"source_c but got {[p.columns for p in derived_c]}"
        )

    # Verify applied results include join-equality inferred entries
    applied = [r for r in results if r.status == "applied"]
    join_eq_applied = [r for r in applied if "join-equality" in (r.detail or "")]
    assert len(join_eq_applied) >= 1, (
        f"Expected at least one join-equality applied result but got: "
        f"{[(r.status, r.detail) for r in results]}"
    )


# Feature: cross-group-predicate-pushdown
# Property 11: Non-inner joins and expression-based conditions block join-equality derivation
@given(scenario=_non_inner_join_scenario())
@settings(max_examples=100)
def test_property11_non_inner_joins_block_join_equality_derivation(
    scenario: dict,
) -> None:
    """For any consumer group with a non-INNER join or expression-based INNER
    JOIN condition, no derived predicates shall be produced through that join
    condition.

    Validates: Requirements 10.3, 10.4
    """
    new_groups, results = cross_group_pushdown_pass(
        scenario["groups"],
        scenario["compiled_joints"],
        scenario["capabilities"],
        scenario["catalog_types"],
    )

    _source_b = scenario["source_b_name"]
    source_b_gid = scenario["source_b_group_id"]

    # Source B should NOT have any derived predicates
    source_b_group = next(g for g in new_groups if g.id == source_b_gid)
    assert not source_b_group.per_joint_predicates, (
        f"Derived predicates were produced through {scenario['reason']} join "
        f"for source_b: {source_b_group.per_joint_predicates}"
    )

    # No join-equality applied results should exist
    join_eq_applied = [
        r for r in results
        if r.status == "applied" and "join-equality" in (r.detail or "")
    ]
    assert len(join_eq_applied) == 0, (
        f"Unexpected join-equality applied results for {scenario['reason']} "
        f"join: {[(r.status, r.detail) for r in join_eq_applied]}"
    )
