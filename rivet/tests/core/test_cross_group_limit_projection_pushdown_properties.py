"""Property-based tests for cross-group projection & limit pushdown.

Covers Properties 1–4 from the cross-group limit & projection pushdown design document.

- Property 1: Projection columns are mapped correctly via lineage to source joints
  Validates: Requirements 1.1, 2.1, 2.2, 2.4, 3.1
- Property 2: SELECT * skips projection pushdown
  Validates: Requirements 1.2, 9.3
- Property 3: Missing lineage falls back to no projection pushdown
  Validates: Requirements 2.3
- Property 4: Capability gate prevents pushdown to incapable adapters
  Validates: Requirements 3.2, 5.2, 9.5
"""

from __future__ import annotations

import uuid

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.compiler import CompiledJoint, OptimizationResult
from rivet_core.lineage import ColumnLineage, ColumnOrigin
from rivet_core.optimizer import (
    FusedGroup,
    cross_group_pushdown_pass,
)
from rivet_core.sql_parser import (
    Aggregation,
    Join,
    Limit,
    LogicalPlan,
    Ordering,
    Predicate,
    Projection,
)

# ---------------------------------------------------------------------------
# Helpers — reuse the same pattern as test_cross_group_pushdown_properties.py
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
# Capability dicts
# ---------------------------------------------------------------------------

_FULL_CAPS: dict[str, list[str]] = {
    "databricks": ["predicate_pushdown", "projection_pushdown", "limit_pushdown"],
}
_PRED_ONLY_CAPS: dict[str, list[str]] = {
    "databricks": ["predicate_pushdown"],
}
_NO_CAPS: dict[str, list[str]] = {}

# Column name strategy: simple lowercase identifiers
_col_name_st = st.from_regex(r"[a-z]{3,8}", fullmatch=True)


# ---------------------------------------------------------------------------
# Strategy: Property 1 — Projection mapping scenario
# ---------------------------------------------------------------------------


@st.composite
def _projection_mapping_scenario(draw: st.DrawFn) -> dict:
    """Generate a consumer with non-star projections and traced lineage.

    Returns dict with keys:
        groups, compiled_joints, capabilities, catalog_types,
        source_cols, consumer_cols, source_name
    """
    # Generate 1–4 unique source columns
    source_cols = draw(
        st.lists(_col_name_st, min_size=1, max_size=4, unique=True),
    )
    transform = draw(st.sampled_from(["direct", "renamed"]))

    # Consumer columns: same as source for direct, different for renamed
    if transform == "renamed":
        consumer_cols: list[str] = []
        used = set(source_cols)
        for _ in source_cols:
            c = draw(_col_name_st.filter(lambda x, _u=frozenset(used): x not in _u))
            consumer_cols.append(c)
            used.add(c)
    else:
        consumer_cols = list(source_cols)

    source_name = "src_joint"
    consumer_name = "consumer_joint"

    # Build lineage on consumer: each consumer_col traces to source_col on source_name
    consumer_lineage = [
        ColumnLineage(
            output_column=cc,
            transform=transform,
            origins=[ColumnOrigin(joint=source_name, column=sc)],
            expression=None,
        )
        for cc, sc in zip(consumer_cols, source_cols)
    ]

    # Build LogicalPlan with projections referencing consumer columns
    projections = [
        Projection(expression=cc, alias=None, source_columns=[cc])
        for cc in consumer_cols
    ]
    lp = LogicalPlan(
        projections=projections,
        predicates=[],
        joins=[],
        aggregations=None,
        limit=None,
        ordering=None,
        distinct=False,
        source_tables=[],
    )

    source_cj = _make_compiled_joint(
        source_name, joint_type="source", engine_type="databricks",
    )
    consumer_cj = _make_compiled_joint(
        consumer_name,
        joint_type="sql",
        upstream=[source_name],
        engine="eng2",
        engine_type="polars",
        logical_plan=lp,
        column_lineage=consumer_lineage,
    )

    grp_src = _make_group([source_name], engine_type="databricks")
    grp_con = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        exit_joints=[consumer_name],
    )

    return {
        "groups": [grp_src, grp_con],
        "compiled_joints": {source_name: source_cj, consumer_name: consumer_cj},
        "capabilities": _FULL_CAPS,
        "catalog_types": {},
        "source_cols": source_cols,
        "consumer_cols": consumer_cols,
        "source_name": source_name,
    }


# ---------------------------------------------------------------------------
# Strategy: Property 2 — SELECT * scenario
# ---------------------------------------------------------------------------


@st.composite
def _select_star_scenario(draw: st.DrawFn) -> dict:
    """Generate a consumer with SELECT * projection."""
    source_name = "src_joint"
    consumer_name = "consumer_joint"

    # SELECT * projection
    projections = [Projection(expression="*", alias=None, source_columns=[])]
    lp = LogicalPlan(
        projections=projections,
        predicates=[],
        joins=[],
        aggregations=None,
        limit=None,
        ordering=None,
        distinct=False,
        source_tables=[],
    )

    # Even with lineage, SELECT * should skip
    some_col = draw(_col_name_st)
    consumer_lineage = [
        ColumnLineage(
            output_column=some_col,
            transform="direct",
            origins=[ColumnOrigin(joint=source_name, column=some_col)],
            expression=None,
        ),
    ]

    source_cj = _make_compiled_joint(
        source_name, joint_type="source", engine_type="databricks",
    )
    consumer_cj = _make_compiled_joint(
        consumer_name,
        joint_type="sql",
        upstream=[source_name],
        engine="eng2",
        engine_type="polars",
        logical_plan=lp,
        column_lineage=consumer_lineage,
    )

    grp_src = _make_group([source_name], engine_type="databricks")
    grp_con = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        exit_joints=[consumer_name],
    )

    return {
        "groups": [grp_src, grp_con],
        "compiled_joints": {source_name: source_cj, consumer_name: consumer_cj},
        "capabilities": _FULL_CAPS,
        "catalog_types": {},
        "source_name": source_name,
    }


# ---------------------------------------------------------------------------
# Strategy: Property 3 — Missing lineage scenario
# ---------------------------------------------------------------------------


@st.composite
def _missing_lineage_scenario(draw: st.DrawFn) -> dict:
    """Generate a consumer with columns that have no ColumnLineage record."""
    source_name = "src_joint"
    consumer_name = "consumer_joint"

    # Generate a column that the consumer references but has NO lineage for
    col_with_lineage = draw(_col_name_st)
    col_without_lineage = draw(
        _col_name_st.filter(lambda c: c != col_with_lineage),
    )

    # Only provide lineage for col_with_lineage, not col_without_lineage
    consumer_lineage = [
        ColumnLineage(
            output_column=col_with_lineage,
            transform="direct",
            origins=[ColumnOrigin(joint=source_name, column=col_with_lineage)],
            expression=None,
        ),
    ]

    # LogicalPlan references BOTH columns
    projections = [
        Projection(expression=col_with_lineage, alias=None, source_columns=[col_with_lineage]),
        Projection(expression=col_without_lineage, alias=None, source_columns=[col_without_lineage]),
    ]
    lp = LogicalPlan(
        projections=projections,
        predicates=[],
        joins=[],
        aggregations=None,
        limit=None,
        ordering=None,
        distinct=False,
        source_tables=[],
    )

    source_cj = _make_compiled_joint(
        source_name, joint_type="source", engine_type="databricks",
    )
    consumer_cj = _make_compiled_joint(
        consumer_name,
        joint_type="sql",
        upstream=[source_name],
        engine="eng2",
        engine_type="polars",
        logical_plan=lp,
        column_lineage=consumer_lineage,
    )

    grp_src = _make_group([source_name], engine_type="databricks")
    grp_con = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        exit_joints=[consumer_name],
    )

    return {
        "groups": [grp_src, grp_con],
        "compiled_joints": {source_name: source_cj, consumer_name: consumer_cj},
        "capabilities": _FULL_CAPS,
        "catalog_types": {},
        "source_name": source_name,
        "col_without_lineage": col_without_lineage,
    }


# ---------------------------------------------------------------------------
# Strategy: Property 4 — Capability gate scenario
# ---------------------------------------------------------------------------


@st.composite
def _capability_gate_scenario(draw: st.DrawFn) -> dict:
    """Generate source groups without projection_pushdown or limit_pushdown capability."""
    source_name = "src_joint"
    consumer_name = "consumer_joint"

    # Which capability to withhold
    cap_mode = draw(st.sampled_from(["no_projection", "no_limit", "no_caps"]))

    if cap_mode == "no_projection":
        # Has predicate + limit but NOT projection
        caps: dict[str, list[str]] = {
            "databricks": ["predicate_pushdown", "limit_pushdown"],
        }
    elif cap_mode == "no_limit":
        # Has predicate + projection but NOT limit
        caps = {
            "databricks": ["predicate_pushdown", "projection_pushdown"],
        }
    else:
        # No capabilities at all
        caps = _NO_CAPS

    source_col = draw(_col_name_st)

    consumer_lineage = [
        ColumnLineage(
            output_column=source_col,
            transform="direct",
            origins=[ColumnOrigin(joint=source_name, column=source_col)],
            expression=None,
        ),
    ]

    projections = [
        Projection(expression=source_col, alias=None, source_columns=[source_col]),
    ]
    lp = LogicalPlan(
        projections=projections,
        predicates=[],
        joins=[],
        aggregations=None,
        limit=Limit(count=draw(st.integers(min_value=1, max_value=1000)), offset=None),
        ordering=None,
        distinct=False,
        source_tables=[],
    )

    source_cj = _make_compiled_joint(
        source_name, joint_type="source", engine_type="databricks",
    )
    consumer_cj = _make_compiled_joint(
        consumer_name,
        joint_type="sql",
        upstream=[source_name],
        engine="eng2",
        engine_type="polars",
        logical_plan=lp,
        column_lineage=consumer_lineage,
    )

    grp_src = _make_group([source_name], engine_type="databricks")
    grp_con = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        exit_joints=[consumer_name],
    )

    return {
        "groups": [grp_src, grp_con],
        "compiled_joints": {source_name: source_cj, consumer_name: consumer_cj},
        "capabilities": caps,
        "catalog_types": {},
        "source_name": source_name,
        "cap_mode": cap_mode,
        "source_col": source_col,
    }


# ---------------------------------------------------------------------------
# Property 1: Projection columns are mapped correctly via lineage to source joints
# Feature: cross-group-limit-projection-pushdown, Property 1
# **Validates: Requirements 1.1, 2.1, 2.2, 2.4, 3.1**
# ---------------------------------------------------------------------------


@given(scenario=_projection_mapping_scenario())
@settings(max_examples=100)
def test_property1_projection_columns_mapped_correctly_via_lineage(
    scenario: dict,
) -> None:
    """For any consumer with non-star projections and traced lineage,
    per_joint_projections on the source group contains the correct
    mapped source column names for every traced origin."""
    groups = scenario["groups"]
    compiled_joints = scenario["compiled_joints"]
    capabilities = scenario["capabilities"]
    catalog_types = scenario["catalog_types"]
    source_cols = scenario["source_cols"]
    source_name = scenario["source_name"]

    new_groups, results = cross_group_pushdown_pass(
        groups, compiled_joints, capabilities, catalog_types,
    )

    # Find the source group in the output
    src_group = [g for g in new_groups if source_name in g.joints][0]

    # per_joint_projections must contain the source joint
    assert source_name in src_group.per_joint_projections, (
        f"Expected source joint '{source_name}' in per_joint_projections, "
        f"got {src_group.per_joint_projections}"
    )

    pushed_cols = set(src_group.per_joint_projections[source_name])

    # Every source column must be present in the pushed projections
    for sc in source_cols:
        assert sc in pushed_cols, (
            f"Expected source column '{sc}' in pushed projections, "
            f"got {pushed_cols}"
        )

    # There should be at least one 'applied' OptimizationResult for projection
    applied_proj = [
        r for r in results
        if r.rule == "cross_group_projection_pushdown" and r.status == "applied"
    ]
    assert len(applied_proj) >= 1, (
        f"Expected at least one applied projection result, got {results}"
    )


# ---------------------------------------------------------------------------
# Property 2: SELECT * skips projection pushdown
# Feature: cross-group-limit-projection-pushdown, Property 2
# **Validates: Requirements 1.2, 9.3**
# ---------------------------------------------------------------------------


@given(scenario=_select_star_scenario())
@settings(max_examples=100)
def test_property2_select_star_skips_projection_pushdown(
    scenario: dict,
) -> None:
    """For any consumer with SELECT * projection, no per_joint_projections
    are added to any source group, and a skipped OptimizationResult is
    recorded with rule cross_group_projection_pushdown."""
    groups = scenario["groups"]
    compiled_joints = scenario["compiled_joints"]
    capabilities = scenario["capabilities"]
    catalog_types = scenario["catalog_types"]
    source_name = scenario["source_name"]

    new_groups, results = cross_group_pushdown_pass(
        groups, compiled_joints, capabilities, catalog_types,
    )

    # No source group should have per_joint_projections
    for g in new_groups:
        assert not g.per_joint_projections, (
            f"Expected no per_joint_projections on any group, "
            f"but group '{g.id}' has {g.per_joint_projections}"
        )

    # There must be a skipped result for projection pushdown
    skipped_proj = [
        r for r in results
        if r.rule == "cross_group_projection_pushdown" and r.status == "skipped"
    ]
    assert len(skipped_proj) >= 1, (
        f"Expected at least one skipped projection result for SELECT *, "
        f"got {[r for r in results if r.rule == 'cross_group_projection_pushdown']}"
    )


# ---------------------------------------------------------------------------
# Property 3: Missing lineage falls back to no projection pushdown
# Feature: cross-group-limit-projection-pushdown, Property 3
# **Validates: Requirements 2.3**
# ---------------------------------------------------------------------------


@given(scenario=_missing_lineage_scenario())
@settings(max_examples=100)
def test_property3_missing_lineage_skips_projection_pushdown(
    scenario: dict,
) -> None:
    """For any consumer whose exit joint references a column with no
    ColumnLineage record, projection pushdown is skipped for that
    consumer group entirely — no per_joint_projections entries added."""
    groups = scenario["groups"]
    compiled_joints = scenario["compiled_joints"]
    capabilities = scenario["capabilities"]
    catalog_types = scenario["catalog_types"]
    source_name = scenario["source_name"]

    new_groups, results = cross_group_pushdown_pass(
        groups, compiled_joints, capabilities, catalog_types,
    )

    # Source group should NOT have per_joint_projections because one
    # consumer column had no lineage → entire projection pushdown skipped
    src_group = [g for g in new_groups if source_name in g.joints][0]
    assert not src_group.per_joint_projections, (
        f"Expected no per_joint_projections when lineage is missing, "
        f"but got {src_group.per_joint_projections}"
    )


# ---------------------------------------------------------------------------
# Property 4: Capability gate prevents pushdown to incapable adapters
# Feature: cross-group-limit-projection-pushdown, Property 4
# **Validates: Requirements 3.2, 5.2, 9.5**
# ---------------------------------------------------------------------------


@given(scenario=_capability_gate_scenario())
@settings(max_examples=100)
def test_property4_capability_gate_prevents_pushdown(
    scenario: dict,
) -> None:
    """For any source joint whose adapter lacks projection_pushdown capability,
    no cross-group projections are stored. Similarly for limit_pushdown.
    In both cases, a not_applicable OptimizationResult is recorded."""
    groups = scenario["groups"]
    compiled_joints = scenario["compiled_joints"]
    capabilities = scenario["capabilities"]
    catalog_types = scenario["catalog_types"]
    source_name = scenario["source_name"]
    cap_mode = scenario["cap_mode"]

    new_groups, results = cross_group_pushdown_pass(
        groups, compiled_joints, capabilities, catalog_types,
    )

    src_group = [g for g in new_groups if source_name in g.joints][0]

    if cap_mode in ("no_projection", "no_caps"):
        # No projection pushdown should occur
        assert not src_group.per_joint_projections, (
            f"Expected no per_joint_projections without projection_pushdown "
            f"capability, but got {src_group.per_joint_projections}"
        )
        # Should have a not_applicable result for projection
        not_applicable_proj = [
            r for r in results
            if r.rule == "cross_group_projection_pushdown"
            and r.status == "not_applicable"
        ]
        assert len(not_applicable_proj) >= 1, (
            f"Expected not_applicable result for projection pushdown, "
            f"got {[r for r in results if 'projection' in r.rule]}"
        )

    if cap_mode in ("no_limit", "no_caps"):
        # No limit pushdown should occur
        assert not src_group.per_joint_limits, (
            f"Expected no per_joint_limits without limit_pushdown "
            f"capability, but got {src_group.per_joint_limits}"
        )


# ---------------------------------------------------------------------------
# Import ResidualPlan for Property 8
# ---------------------------------------------------------------------------
from rivet_core.optimizer import ResidualPlan


# ---------------------------------------------------------------------------
# Strategy: Property 5 — Limit extraction without blocking constructs
# ---------------------------------------------------------------------------


@st.composite
def _limit_extraction_scenario(draw: st.DrawFn) -> dict:
    """Generate a consumer with LIMIT, no aggregations/joins/DISTINCT,
    single upstream source group, and no residual predicates.

    ORDER BY may or may not be present — it should NOT block limit pushdown.
    """
    source_col = draw(_col_name_st)
    limit_val = draw(st.integers(min_value=1, max_value=10000))
    has_ordering = draw(st.booleans())

    ordering = Ordering(columns=[(source_col, "asc")]) if has_ordering else None

    lp = LogicalPlan(
        projections=[Projection(expression=source_col, alias=None, source_columns=[source_col])],
        predicates=[],
        joins=[],
        aggregations=None,
        limit=Limit(count=limit_val, offset=None),
        ordering=ordering,
        distinct=False,
        source_tables=[],
    )

    source_name = "src_joint"
    consumer_name = "consumer_joint"

    consumer_lineage = [
        ColumnLineage(
            output_column=source_col,
            transform="direct",
            origins=[ColumnOrigin(joint=source_name, column=source_col)],
            expression=None,
        ),
    ]

    source_cj = _make_compiled_joint(
        source_name, joint_type="source", engine_type="databricks",
    )
    consumer_cj = _make_compiled_joint(
        consumer_name,
        joint_type="sql",
        upstream=[source_name],
        engine="eng2",
        engine_type="polars",
        logical_plan=lp,
        column_lineage=consumer_lineage,
    )

    grp_src = _make_group([source_name], engine_type="databricks")
    grp_con = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        exit_joints=[consumer_name],
    )

    return {
        "groups": [grp_src, grp_con],
        "compiled_joints": {source_name: source_cj, consumer_name: consumer_cj},
        "capabilities": _FULL_CAPS,
        "catalog_types": {},
        "source_name": source_name,
        "limit_val": limit_val,
        "has_ordering": has_ordering,
    }


# ---------------------------------------------------------------------------
# Strategy: Property 6 — Blocking constructs scenario
# ---------------------------------------------------------------------------


@st.composite
def _blocking_construct_scenario(draw: st.DrawFn) -> dict:
    """Generate a consumer with LIMIT AND one of: aggregation, join, or DISTINCT."""
    blocker = draw(st.sampled_from(["aggregation", "join", "distinct"]))
    source_col = draw(_col_name_st)
    limit_val = draw(st.integers(min_value=1, max_value=10000))

    aggregations = None
    joins: list[Join] = []
    distinct = False

    if blocker == "aggregation":
        aggregations = Aggregation(group_by=[source_col], functions=[])
    elif blocker == "join":
        joins = [Join(type="inner", left_table="src", right_table="other", condition=f"{source_col} = other.{source_col}", columns=[source_col])]
    else:
        distinct = True

    lp = LogicalPlan(
        projections=[Projection(expression=source_col, alias=None, source_columns=[source_col])],
        predicates=[],
        joins=joins,
        aggregations=aggregations,
        limit=Limit(count=limit_val, offset=None),
        ordering=None,
        distinct=distinct,
        source_tables=[],
    )

    source_name = "src_joint"
    consumer_name = "consumer_joint"

    consumer_lineage = [
        ColumnLineage(
            output_column=source_col,
            transform="direct",
            origins=[ColumnOrigin(joint=source_name, column=source_col)],
            expression=None,
        ),
    ]

    source_cj = _make_compiled_joint(
        source_name, joint_type="source", engine_type="databricks",
    )
    consumer_cj = _make_compiled_joint(
        consumer_name,
        joint_type="sql",
        upstream=[source_name],
        engine="eng2",
        engine_type="polars",
        logical_plan=lp,
        column_lineage=consumer_lineage,
    )

    grp_src = _make_group([source_name], engine_type="databricks")
    grp_con = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        exit_joints=[consumer_name],
    )

    return {
        "groups": [grp_src, grp_con],
        "compiled_joints": {source_name: source_cj, consumer_name: consumer_cj},
        "capabilities": _FULL_CAPS,
        "catalog_types": {},
        "source_name": source_name,
        "blocker": blocker,
    }


# ---------------------------------------------------------------------------
# Strategy: Property 7 — Multiple upstream source groups
# ---------------------------------------------------------------------------


@st.composite
def _multiple_upstream_sources_scenario(draw: st.DrawFn) -> dict:
    """Generate a consumer referencing source joints in TWO different groups."""
    col_a = draw(_col_name_st)
    col_b = draw(_col_name_st.filter(lambda c, _a=col_a: c != _a))
    limit_val = draw(st.integers(min_value=1, max_value=10000))

    source_a = "src_joint_a"
    source_b = "src_joint_b"
    consumer_name = "consumer_joint"

    lp = LogicalPlan(
        projections=[
            Projection(expression=col_a, alias=None, source_columns=[col_a]),
            Projection(expression=col_b, alias=None, source_columns=[col_b]),
        ],
        predicates=[],
        joins=[],
        aggregations=None,
        limit=Limit(count=limit_val, offset=None),
        ordering=None,
        distinct=False,
        source_tables=[],
    )

    consumer_lineage = [
        ColumnLineage(
            output_column=col_a,
            transform="direct",
            origins=[ColumnOrigin(joint=source_a, column=col_a)],
            expression=None,
        ),
        ColumnLineage(
            output_column=col_b,
            transform="direct",
            origins=[ColumnOrigin(joint=source_b, column=col_b)],
            expression=None,
        ),
    ]

    source_a_cj = _make_compiled_joint(
        source_a, joint_type="source", engine_type="databricks",
    )
    source_b_cj = _make_compiled_joint(
        source_b, joint_type="source", engine_type="databricks",
    )
    consumer_cj = _make_compiled_joint(
        consumer_name,
        joint_type="sql",
        upstream=[source_a, source_b],
        engine="eng2",
        engine_type="polars",
        logical_plan=lp,
        column_lineage=consumer_lineage,
    )

    grp_src_a = _make_group([source_a], engine_type="databricks")
    grp_src_b = _make_group([source_b], engine_type="databricks")
    grp_con = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        exit_joints=[consumer_name],
    )

    return {
        "groups": [grp_src_a, grp_src_b, grp_con],
        "compiled_joints": {
            source_a: source_a_cj,
            source_b: source_b_cj,
            consumer_name: consumer_cj,
        },
        "capabilities": _FULL_CAPS,
        "catalog_types": {},
        "source_a": source_a,
        "source_b": source_b,
    }


# ---------------------------------------------------------------------------
# Strategy: Property 8 — Residual predicates block limit pushdown
# ---------------------------------------------------------------------------


@st.composite
def _residual_predicates_scenario(draw: st.DrawFn) -> dict:
    """Generate a consumer group with residual predicates and a LIMIT."""
    source_col = draw(_col_name_st)
    limit_val = draw(st.integers(min_value=1, max_value=10000))

    lp = LogicalPlan(
        projections=[Projection(expression=source_col, alias=None, source_columns=[source_col])],
        predicates=[],
        joins=[],
        aggregations=None,
        limit=Limit(count=limit_val, offset=None),
        ordering=None,
        distinct=False,
        source_tables=[],
    )

    source_name = "src_joint"
    consumer_name = "consumer_joint"

    consumer_lineage = [
        ColumnLineage(
            output_column=source_col,
            transform="direct",
            origins=[ColumnOrigin(joint=source_name, column=source_col)],
            expression=None,
        ),
    ]

    source_cj = _make_compiled_joint(
        source_name, joint_type="source", engine_type="databricks",
    )
    consumer_cj = _make_compiled_joint(
        consumer_name,
        joint_type="sql",
        upstream=[source_name],
        engine="eng2",
        engine_type="polars",
        logical_plan=lp,
        column_lineage=consumer_lineage,
    )

    grp_src = _make_group([source_name], engine_type="databricks")
    # Consumer group WITH residual predicates
    grp_con = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        exit_joints=[consumer_name],
    )
    # Add residual predicates to the consumer group
    residual_pred = Predicate(
        expression=f"{source_col} > 10",
        columns=[source_col],
        location="where",
    )
    grp_con = grp_con.__class__(
        id=grp_con.id,
        joints=grp_con.joints,
        engine=grp_con.engine,
        engine_type=grp_con.engine_type,
        adapters=grp_con.adapters,
        fused_sql=grp_con.fused_sql,
        entry_joints=grp_con.entry_joints,
        exit_joints=grp_con.exit_joints,
        residual=ResidualPlan(predicates=[residual_pred], limit=None, casts=[]),
    )

    return {
        "groups": [grp_src, grp_con],
        "compiled_joints": {source_name: source_cj, consumer_name: consumer_cj},
        "capabilities": _FULL_CAPS,
        "catalog_types": {},
        "source_name": source_name,
    }


# ---------------------------------------------------------------------------
# Strategy: Property 9 — Multi-consumer projection union and limit maximum
# ---------------------------------------------------------------------------


@st.composite
def _multi_consumer_scenario(draw: st.DrawFn) -> dict:
    """Generate TWO consumer groups referencing the same source joint
    with different columns and different limits."""
    # Generate 2-4 unique column names
    all_cols = draw(st.lists(_col_name_st, min_size=4, max_size=6, unique=True))
    # Split into two overlapping sets
    mid = len(all_cols) // 2
    cols_a = all_cols[:mid + 1]  # first consumer's columns (includes overlap)
    cols_b = all_cols[mid:]      # second consumer's columns (includes overlap)

    limit_a = draw(st.integers(min_value=1, max_value=5000))
    limit_b = draw(st.integers(min_value=1, max_value=5000).filter(lambda x, _a=limit_a: x != _a))

    source_name = "src_joint"
    consumer_a = "consumer_a"
    consumer_b = "consumer_b"

    # Build lineage and LogicalPlan for consumer A
    lineage_a = [
        ColumnLineage(
            output_column=c,
            transform="direct",
            origins=[ColumnOrigin(joint=source_name, column=c)],
            expression=None,
        )
        for c in cols_a
    ]
    lp_a = LogicalPlan(
        projections=[Projection(expression=c, alias=None, source_columns=[c]) for c in cols_a],
        predicates=[],
        joins=[],
        aggregations=None,
        limit=Limit(count=limit_a, offset=None),
        ordering=None,
        distinct=False,
        source_tables=[],
    )

    # Build lineage and LogicalPlan for consumer B
    lineage_b = [
        ColumnLineage(
            output_column=c,
            transform="direct",
            origins=[ColumnOrigin(joint=source_name, column=c)],
            expression=None,
        )
        for c in cols_b
    ]
    lp_b = LogicalPlan(
        projections=[Projection(expression=c, alias=None, source_columns=[c]) for c in cols_b],
        predicates=[],
        joins=[],
        aggregations=None,
        limit=Limit(count=limit_b, offset=None),
        ordering=None,
        distinct=False,
        source_tables=[],
    )

    source_cj = _make_compiled_joint(
        source_name, joint_type="source", engine_type="databricks",
    )
    consumer_a_cj = _make_compiled_joint(
        consumer_a,
        joint_type="sql",
        upstream=[source_name],
        engine="eng2",
        engine_type="polars",
        logical_plan=lp_a,
        column_lineage=lineage_a,
    )
    consumer_b_cj = _make_compiled_joint(
        consumer_b,
        joint_type="sql",
        upstream=[source_name],
        engine="eng3",
        engine_type="polars",
        logical_plan=lp_b,
        column_lineage=lineage_b,
    )

    grp_src = _make_group([source_name], engine_type="databricks")
    grp_con_a = _make_group(
        [consumer_a], engine="eng2", engine_type="polars",
        exit_joints=[consumer_a],
    )
    grp_con_b = _make_group(
        [consumer_b], engine="eng3", engine_type="polars",
        exit_joints=[consumer_b],
    )

    return {
        "groups": [grp_src, grp_con_a, grp_con_b],
        "compiled_joints": {
            source_name: source_cj,
            consumer_a: consumer_a_cj,
            consumer_b: consumer_b_cj,
        },
        "capabilities": _FULL_CAPS,
        "catalog_types": {},
        "source_name": source_name,
        "cols_a": cols_a,
        "cols_b": cols_b,
        "limit_a": limit_a,
        "limit_b": limit_b,
    }


# ---------------------------------------------------------------------------
# Property 5: Limit is extracted when no blocking constructs are present
# Feature: cross-group-limit-projection-pushdown, Property 5
# **Validates: Requirements 4.1, 5.1, 7.1**
# ---------------------------------------------------------------------------


@given(scenario=_limit_extraction_scenario())
@settings(max_examples=100)
def test_property5_limit_extracted_when_no_blocking_constructs(
    scenario: dict,
) -> None:
    """For any consumer with LIMIT, no aggregations/joins/DISTINCT, single
    upstream source group, and no residual predicates, per_joint_limits on
    the source group contains the limit value. ORDER BY does NOT block."""
    groups = scenario["groups"]
    compiled_joints = scenario["compiled_joints"]
    capabilities = scenario["capabilities"]
    catalog_types = scenario["catalog_types"]
    source_name = scenario["source_name"]
    limit_val = scenario["limit_val"]

    new_groups, results = cross_group_pushdown_pass(
        groups, compiled_joints, capabilities, catalog_types,
    )

    src_group = [g for g in new_groups if source_name in g.joints][0]

    # per_joint_limits must contain the source joint with the correct limit
    assert source_name in src_group.per_joint_limits, (
        f"Expected source joint '{source_name}' in per_joint_limits, "
        f"got {src_group.per_joint_limits}"
    )
    assert src_group.per_joint_limits[source_name] == limit_val, (
        f"Expected limit {limit_val}, got {src_group.per_joint_limits[source_name]}"
    )

    # There should be at least one 'applied' OptimizationResult for limit
    applied_lim = [
        r for r in results
        if r.rule == "cross_group_limit_pushdown" and r.status == "applied"
    ]
    assert len(applied_lim) >= 1, (
        f"Expected at least one applied limit result, got {results}"
    )


# ---------------------------------------------------------------------------
# Property 6: Aggregations, joins, and DISTINCT block limit pushdown
# Feature: cross-group-limit-projection-pushdown, Property 6
# **Validates: Requirements 4.2, 4.3, 4.4, 9.4**
# ---------------------------------------------------------------------------


@given(scenario=_blocking_construct_scenario())
@settings(max_examples=100)
def test_property6_blocking_constructs_prevent_limit_pushdown(
    scenario: dict,
) -> None:
    """For any consumer with LIMIT and aggregations, joins, or DISTINCT,
    no per_joint_limits are stored and a skipped OptimizationResult is
    recorded with rule cross_group_limit_pushdown."""
    groups = scenario["groups"]
    compiled_joints = scenario["compiled_joints"]
    capabilities = scenario["capabilities"]
    catalog_types = scenario["catalog_types"]
    source_name = scenario["source_name"]
    blocker = scenario["blocker"]

    new_groups, results = cross_group_pushdown_pass(
        groups, compiled_joints, capabilities, catalog_types,
    )

    # No source group should have per_joint_limits
    for g in new_groups:
        assert not g.per_joint_limits, (
            f"Expected no per_joint_limits when {blocker} is present, "
            f"but group '{g.id}' has {g.per_joint_limits}"
        )

    # There must be a skipped result for limit pushdown
    skipped_lim = [
        r for r in results
        if r.rule == "cross_group_limit_pushdown" and r.status == "skipped"
    ]
    assert len(skipped_lim) >= 1, (
        f"Expected at least one skipped limit result for {blocker}, "
        f"got {[r for r in results if r.rule == 'cross_group_limit_pushdown']}"
    )


# ---------------------------------------------------------------------------
# Property 7: Multiple upstream source groups block limit pushdown
# Feature: cross-group-limit-projection-pushdown, Property 7
# **Validates: Requirements 7.2**
# ---------------------------------------------------------------------------


@given(scenario=_multiple_upstream_sources_scenario())
@settings(max_examples=100)
def test_property7_multiple_upstream_sources_block_limit_pushdown(
    scenario: dict,
) -> None:
    """For any consumer referencing source joints in more than one upstream
    source group, no per_joint_limits are stored on any source group."""
    groups = scenario["groups"]
    compiled_joints = scenario["compiled_joints"]
    capabilities = scenario["capabilities"]
    catalog_types = scenario["catalog_types"]

    new_groups, results = cross_group_pushdown_pass(
        groups, compiled_joints, capabilities, catalog_types,
    )

    # No source group should have per_joint_limits
    for g in new_groups:
        assert not g.per_joint_limits, (
            f"Expected no per_joint_limits when multiple upstream sources, "
            f"but group '{g.id}' has {g.per_joint_limits}"
        )


# ---------------------------------------------------------------------------
# Property 8: Residual predicates block limit pushdown
# Feature: cross-group-limit-projection-pushdown, Property 8
# **Validates: Requirements 7.3**
# ---------------------------------------------------------------------------


@given(scenario=_residual_predicates_scenario())
@settings(max_examples=100)
def test_property8_residual_predicates_block_limit_pushdown(
    scenario: dict,
) -> None:
    """For any consumer group with residual predicates, no per_joint_limits
    are stored on any source group."""
    groups = scenario["groups"]
    compiled_joints = scenario["compiled_joints"]
    capabilities = scenario["capabilities"]
    catalog_types = scenario["catalog_types"]
    source_name = scenario["source_name"]

    new_groups, results = cross_group_pushdown_pass(
        groups, compiled_joints, capabilities, catalog_types,
    )

    src_group = [g for g in new_groups if source_name in g.joints][0]

    assert not src_group.per_joint_limits, (
        f"Expected no per_joint_limits when residual predicates present, "
        f"but got {src_group.per_joint_limits}"
    )


# ---------------------------------------------------------------------------
# Property 9: Multi-consumer projection union and limit maximum
# Feature: cross-group-limit-projection-pushdown, Property 9
# **Validates: Requirements 3.4, 5.4**
# ---------------------------------------------------------------------------


@given(scenario=_multi_consumer_scenario())
@settings(max_examples=100)
def test_property9_multi_consumer_projection_union_and_limit_maximum(
    scenario: dict,
) -> None:
    """For any two consumer groups referencing the same source joint,
    per_joint_projections contains the UNION of both consumers' column lists,
    and per_joint_limits contains the MAXIMUM of both consumers' limit values."""
    groups = scenario["groups"]
    compiled_joints = scenario["compiled_joints"]
    capabilities = scenario["capabilities"]
    catalog_types = scenario["catalog_types"]
    source_name = scenario["source_name"]
    cols_a = scenario["cols_a"]
    cols_b = scenario["cols_b"]
    limit_a = scenario["limit_a"]
    limit_b = scenario["limit_b"]

    new_groups, results = cross_group_pushdown_pass(
        groups, compiled_joints, capabilities, catalog_types,
    )

    src_group = [g for g in new_groups if source_name in g.joints][0]

    # Projections: union of both consumers' columns
    expected_proj_cols = sorted(set(cols_a) | set(cols_b))
    assert source_name in src_group.per_joint_projections, (
        f"Expected source joint '{source_name}' in per_joint_projections, "
        f"got {src_group.per_joint_projections}"
    )
    actual_proj_cols = sorted(src_group.per_joint_projections[source_name])
    assert actual_proj_cols == expected_proj_cols, (
        f"Expected projection union {expected_proj_cols}, got {actual_proj_cols}"
    )

    # Limits: maximum of both consumers' limits
    expected_limit = max(limit_a, limit_b)
    assert source_name in src_group.per_joint_limits, (
        f"Expected source joint '{source_name}' in per_joint_limits, "
        f"got {src_group.per_joint_limits}"
    )
    assert src_group.per_joint_limits[source_name] == expected_limit, (
        f"Expected limit max({limit_a}, {limit_b}) = {expected_limit}, "
        f"got {src_group.per_joint_limits[source_name]}"
    )


# ---------------------------------------------------------------------------
# Imports for executor merge function tests (Properties 10 & 11)
# ---------------------------------------------------------------------------
from dataclasses import replace

from rivet_core.executor import _merge_cross_group_projections, _merge_cross_group_limits
from rivet_core.optimizer import (
    PushdownPlan,
    PredicatePushdownResult,
    ProjectionPushdownResult,
    LimitPushdownResult,
    CastPushdownResult,
)


# ---------------------------------------------------------------------------
# Strategy: Property 10 — Executor projection merge scenario
# ---------------------------------------------------------------------------


@st.composite
def _executor_projection_merge_scenario(draw: st.DrawFn) -> dict:
    """Generate a source group with both intra-group projections
    (PushdownPlan.projections.pushed_columns) and cross-group projections
    (per_joint_projections).

    Returns dict with keys:
        pushdown, group, joint_name, intra_cols, cross_cols, has_intra
    """
    # Generate two sets of columns with some overlap
    all_cols = draw(st.lists(_col_name_st, min_size=2, max_size=8, unique=True))
    mid = len(all_cols) // 2
    intra_cols = all_cols[: mid + 1]  # some overlap
    cross_cols = all_cols[mid:]  # some overlap

    has_intra = draw(st.booleans())  # whether intra-group projections exist

    joint_name = "src_joint"

    # Build a FusedGroup with per_joint_projections
    group = _make_group([joint_name], engine_type="databricks")
    group = replace(group, per_joint_projections={joint_name: sorted(cross_cols)})

    # Build a PushdownPlan with or without intra-group projections
    if has_intra:
        pushdown = PushdownPlan(
            predicates=PredicatePushdownResult(pushed=[], residual=[]),
            projections=ProjectionPushdownResult(
                pushed_columns=sorted(intra_cols), reason=None,
            ),
            limit=LimitPushdownResult(
                pushed_limit=None, residual_limit=None, reason=None,
            ),
            casts=CastPushdownResult(pushed=[], residual=[]),
        )
    else:
        pushdown = PushdownPlan(
            predicates=PredicatePushdownResult(pushed=[], residual=[]),
            projections=ProjectionPushdownResult(
                pushed_columns=None, reason=None,
            ),
            limit=LimitPushdownResult(
                pushed_limit=None, residual_limit=None, reason=None,
            ),
            casts=CastPushdownResult(pushed=[], residual=[]),
        )

    return {
        "pushdown": pushdown,
        "group": group,
        "joint_name": joint_name,
        "intra_cols": intra_cols if has_intra else None,
        "cross_cols": cross_cols,
        "has_intra": has_intra,
    }


# ---------------------------------------------------------------------------
# Property 10: Executor projection merge computes intersection
# Feature: cross-group-limit-projection-pushdown, Property 10
# **Validates: Requirements 3.3, 6.1, 6.3**
# ---------------------------------------------------------------------------


@given(scenario=_executor_projection_merge_scenario())
@settings(max_examples=100)
def test_property10_executor_projection_merge_computes_intersection(
    scenario: dict,
) -> None:
    """For any source group that has both intra-group projections and
    cross-group projections, _merge_cross_group_projections returns a
    PushdownPlan whose pushed_columns is the intersection of both column
    lists. When only cross-group projections exist (intra-group is None),
    the cross-group columns are used directly."""
    pushdown = scenario["pushdown"]
    group = scenario["group"]
    joint_name = scenario["joint_name"]
    intra_cols = scenario["intra_cols"]
    cross_cols = scenario["cross_cols"]
    has_intra = scenario["has_intra"]

    result = _merge_cross_group_projections(pushdown, group, joint_name)

    assert result is not None, "Expected a non-None PushdownPlan from merge"
    assert result.projections.pushed_columns is not None, (
        "Expected pushed_columns to be set after merge"
    )

    actual_cols = sorted(result.projections.pushed_columns)

    if has_intra:
        # Intersection of intra-group and cross-group columns
        expected = sorted(set(intra_cols) & set(cross_cols))
        assert actual_cols == expected, (
            f"Expected intersection {expected}, got {actual_cols} "
            f"(intra={sorted(intra_cols)}, cross={sorted(cross_cols)})"
        )
    else:
        # No intra-group projections → cross-group columns used directly
        expected = sorted(cross_cols)
        assert actual_cols == expected, (
            f"Expected cross-group columns {expected}, got {actual_cols}"
        )


# ---------------------------------------------------------------------------
# Strategy: Property 11 — Executor limit merge scenario
# ---------------------------------------------------------------------------


@st.composite
def _executor_limit_merge_scenario(draw: st.DrawFn) -> dict:
    """Generate a source group with both an existing pushed limit and a
    cross-group limit.

    Returns dict with keys:
        pushdown, group, joint_name, intra_limit, cross_limit, has_intra
    """
    cross_limit = draw(st.integers(min_value=1, max_value=10000))
    has_intra = draw(st.booleans())  # whether intra-group limit exists

    if has_intra:
        intra_limit = draw(st.integers(min_value=1, max_value=10000))
    else:
        intra_limit = None

    joint_name = "src_joint"

    # Build a FusedGroup with per_joint_limits
    group = _make_group([joint_name], engine_type="databricks")
    group = replace(group, per_joint_limits={joint_name: cross_limit})

    # Build a PushdownPlan with or without an existing pushed limit
    pushdown = PushdownPlan(
        predicates=PredicatePushdownResult(pushed=[], residual=[]),
        projections=ProjectionPushdownResult(pushed_columns=None, reason=None),
        limit=LimitPushdownResult(
            pushed_limit=intra_limit, residual_limit=None, reason=None,
        ),
        casts=CastPushdownResult(pushed=[], residual=[]),
    )

    return {
        "pushdown": pushdown,
        "group": group,
        "joint_name": joint_name,
        "intra_limit": intra_limit,
        "cross_limit": cross_limit,
        "has_intra": has_intra,
    }


# ---------------------------------------------------------------------------
# Property 11: Executor limit merge computes minimum
# Feature: cross-group-limit-projection-pushdown, Property 11
# **Validates: Requirements 5.3, 6.2, 6.4, 6.5**
# ---------------------------------------------------------------------------


@given(scenario=_executor_limit_merge_scenario())
@settings(max_examples=100)
def test_property11_executor_limit_merge_computes_minimum(
    scenario: dict,
) -> None:
    """For any source group that has both an existing pushed limit and a
    cross-group limit, _merge_cross_group_limits returns a PushdownPlan
    whose pushed_limit is the minimum of both values. When only a
    cross-group limit exists, it is used directly."""
    pushdown = scenario["pushdown"]
    group = scenario["group"]
    joint_name = scenario["joint_name"]
    intra_limit = scenario["intra_limit"]
    cross_limit = scenario["cross_limit"]
    has_intra = scenario["has_intra"]

    result = _merge_cross_group_limits(pushdown, group, joint_name)

    assert result is not None, "Expected a non-None PushdownPlan from merge"
    assert result.limit.pushed_limit is not None, (
        "Expected pushed_limit to be set after merge"
    )

    if has_intra:
        # Minimum of intra-group and cross-group limits
        expected = min(intra_limit, cross_limit)
        assert result.limit.pushed_limit == expected, (
            f"Expected min({intra_limit}, {cross_limit}) = {expected}, "
            f"got {result.limit.pushed_limit}"
        )
    else:
        # No intra-group limit → cross-group limit used directly
        assert result.limit.pushed_limit == cross_limit, (
            f"Expected cross-group limit {cross_limit}, "
            f"got {result.limit.pushed_limit}"
        )


# ---------------------------------------------------------------------------
# Strategy: Property 12 — Consumer retains limit in residual plan
# ---------------------------------------------------------------------------


@st.composite
def _consumer_retains_limit_scenario(draw: st.DrawFn) -> dict:
    """Generate a consumer with a LIMIT that gets pushed to an upstream source.

    The consumer group has a ResidualPlan with a limit value. After the pass,
    the consumer's residual plan should still contain the original LIMIT.
    """
    source_col = draw(_col_name_st)
    limit_val = draw(st.integers(min_value=1, max_value=10000))

    lp = LogicalPlan(
        projections=[Projection(expression=source_col, alias=None, source_columns=[source_col])],
        predicates=[],
        joins=[],
        aggregations=None,
        limit=Limit(count=limit_val, offset=None),
        ordering=None,
        distinct=False,
        source_tables=[],
    )

    source_name = "src_joint"
    consumer_name = "consumer_joint"

    consumer_lineage = [
        ColumnLineage(
            output_column=source_col,
            transform="direct",
            origins=[ColumnOrigin(joint=source_name, column=source_col)],
            expression=None,
        ),
    ]

    source_cj = _make_compiled_joint(
        source_name, joint_type="source", engine_type="databricks",
    )
    consumer_cj = _make_compiled_joint(
        consumer_name,
        joint_type="sql",
        upstream=[source_name],
        engine="eng2",
        engine_type="polars",
        logical_plan=lp,
        column_lineage=consumer_lineage,
    )

    grp_src = _make_group([source_name], engine_type="databricks")
    grp_con = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        exit_joints=[consumer_name],
    )
    # Give the consumer a residual plan with the limit
    grp_con = replace(
        grp_con,
        residual=ResidualPlan(predicates=[], limit=limit_val, casts=[]),
    )

    return {
        "groups": [grp_src, grp_con],
        "compiled_joints": {source_name: source_cj, consumer_name: consumer_cj},
        "capabilities": _FULL_CAPS,
        "catalog_types": {},
        "source_name": source_name,
        "consumer_name": consumer_name,
        "limit_val": limit_val,
    }


# ---------------------------------------------------------------------------
# Property 12: Consumer retains limit in residual plan
# Feature: cross-group-limit-projection-pushdown, Property 12
# **Validates: Requirements 5.5**
# ---------------------------------------------------------------------------


@given(scenario=_consumer_retains_limit_scenario())
@settings(max_examples=100)
def test_property12_consumer_retains_limit_in_residual(
    scenario: dict,
) -> None:
    """For any consumer group whose limit is pushed to an upstream source
    group, the consumer group's own ResidualPlan shall still contain the
    original LIMIT value, ensuring correctness regardless of whether the
    source returns more rows than needed."""
    groups = scenario["groups"]
    compiled_joints = scenario["compiled_joints"]
    capabilities = scenario["capabilities"]
    catalog_types = scenario["catalog_types"]
    source_name = scenario["source_name"]
    consumer_name = scenario["consumer_name"]
    limit_val = scenario["limit_val"]

    new_groups, results = cross_group_pushdown_pass(
        groups, compiled_joints, capabilities, catalog_types,
    )

    # Verify the limit was actually pushed to the source
    src_group = [g for g in new_groups if source_name in g.joints][0]
    assert source_name in src_group.per_joint_limits, (
        f"Expected limit to be pushed to source '{source_name}', "
        f"but per_joint_limits = {src_group.per_joint_limits}"
    )

    # Verify the consumer's residual plan still has the original limit
    con_group = [g for g in new_groups if consumer_name in g.joints][0]
    assert con_group.residual is not None, (
        "Expected consumer group to retain its ResidualPlan"
    )
    assert con_group.residual.limit == limit_val, (
        f"Expected consumer residual limit to be {limit_val}, "
        f"got {con_group.residual.limit}"
    )


# ---------------------------------------------------------------------------
# Strategy: Property 13 — Predicate columns included in projection list
# ---------------------------------------------------------------------------


@st.composite
def _predicate_columns_in_projection_scenario(draw: st.DrawFn) -> dict:
    """Generate a consumer where cross-group predicates reference columns
    NOT in the consumer's SELECT list, and verify those predicate columns
    appear in per_joint_projections.

    The consumer has SELECT col_select FROM src WHERE col_pred = 'x'.
    col_pred is NOT in the SELECT list but should be included in projections
    because it's needed for predicate evaluation.
    """
    col_select = draw(_col_name_st)
    col_pred = draw(_col_name_st.filter(lambda c, _s=col_select: c != _s))

    source_name = "src_joint"
    consumer_name = "consumer_joint"

    # LogicalPlan: SELECT col_select WHERE col_pred = 'x'
    lp = LogicalPlan(
        projections=[Projection(expression=col_select, alias=None, source_columns=[col_select])],
        predicates=[Predicate(expression=f"{col_pred} = 'x'", columns=[col_pred], location="where")],
        joins=[],
        aggregations=None,
        limit=None,
        ordering=None,
        distinct=False,
        source_tables=[],
    )

    # Lineage: both columns trace directly to source
    consumer_lineage = [
        ColumnLineage(
            output_column=col_select,
            transform="direct",
            origins=[ColumnOrigin(joint=source_name, column=col_select)],
            expression=None,
        ),
        ColumnLineage(
            output_column=col_pred,
            transform="direct",
            origins=[ColumnOrigin(joint=source_name, column=col_pred)],
            expression=None,
        ),
    ]

    source_cj = _make_compiled_joint(
        source_name, joint_type="source", engine_type="databricks",
    )
    consumer_cj = _make_compiled_joint(
        consumer_name,
        joint_type="sql",
        upstream=[source_name],
        engine="eng2",
        engine_type="polars",
        logical_plan=lp,
        column_lineage=consumer_lineage,
    )

    grp_src = _make_group([source_name], engine_type="databricks")
    grp_con = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        exit_joints=[consumer_name],
    )

    return {
        "groups": [grp_src, grp_con],
        "compiled_joints": {source_name: source_cj, consumer_name: consumer_cj},
        "capabilities": _FULL_CAPS,
        "catalog_types": {},
        "source_name": source_name,
        "col_select": col_select,
        "col_pred": col_pred,
    }


# ---------------------------------------------------------------------------
# Property 13: Predicate columns are included in projection list
# Feature: cross-group-limit-projection-pushdown, Property 13
# **Validates: Requirements 10.2**
# ---------------------------------------------------------------------------


@given(scenario=_predicate_columns_in_projection_scenario())
@settings(max_examples=100)
def test_property13_predicate_columns_included_in_projection_list(
    scenario: dict,
) -> None:
    """For any consumer group where cross-group predicates are pushed to a
    source and cross-group projections are also pushed to the same source,
    the projection column list shall include all columns referenced by the
    pushed predicates, even if those columns are not in the consumer's
    SELECT list."""
    groups = scenario["groups"]
    compiled_joints = scenario["compiled_joints"]
    capabilities = scenario["capabilities"]
    catalog_types = scenario["catalog_types"]
    source_name = scenario["source_name"]
    col_select = scenario["col_select"]
    col_pred = scenario["col_pred"]

    new_groups, results = cross_group_pushdown_pass(
        groups, compiled_joints, capabilities, catalog_types,
    )

    src_group = [g for g in new_groups if source_name in g.joints][0]

    # Projections should be pushed
    assert source_name in src_group.per_joint_projections, (
        f"Expected projections pushed to source '{source_name}', "
        f"but per_joint_projections = {src_group.per_joint_projections}"
    )

    pushed_cols = set(src_group.per_joint_projections[source_name])

    # Both the SELECT column and the predicate column must be in projections
    assert col_select in pushed_cols, (
        f"Expected SELECT column '{col_select}' in pushed projections, "
        f"got {pushed_cols}"
    )
    assert col_pred in pushed_cols, (
        f"Expected predicate column '{col_pred}' in pushed projections "
        f"(even though it's not in SELECT), got {pushed_cols}"
    )


# ---------------------------------------------------------------------------
# Strategy: Property 14 — Predicates and limits compose on same source
# ---------------------------------------------------------------------------


@st.composite
def _predicates_and_limits_compose_scenario(draw: st.DrawFn) -> dict:
    """Generate a consumer with both a WHERE clause and a LIMIT, single
    upstream source, no blocking constructs, no residual predicates.

    Both predicates and limit should be pushed to the same source.
    """
    col_select = draw(_col_name_st)
    col_pred = draw(_col_name_st.filter(lambda c, _s=col_select: c != _s))
    limit_val = draw(st.integers(min_value=1, max_value=10000))

    source_name = "src_joint"
    consumer_name = "consumer_joint"

    # LogicalPlan: SELECT col_select WHERE col_pred = 'x' LIMIT N
    lp = LogicalPlan(
        projections=[Projection(expression=col_select, alias=None, source_columns=[col_select])],
        predicates=[Predicate(expression=f"{col_pred} = 'x'", columns=[col_pred], location="where")],
        joins=[],
        aggregations=None,
        limit=Limit(count=limit_val, offset=None),
        ordering=None,
        distinct=False,
        source_tables=[],
    )

    # Lineage: both columns trace directly to source
    consumer_lineage = [
        ColumnLineage(
            output_column=col_select,
            transform="direct",
            origins=[ColumnOrigin(joint=source_name, column=col_select)],
            expression=None,
        ),
        ColumnLineage(
            output_column=col_pred,
            transform="direct",
            origins=[ColumnOrigin(joint=source_name, column=col_pred)],
            expression=None,
        ),
    ]

    source_cj = _make_compiled_joint(
        source_name, joint_type="source", engine_type="databricks",
    )
    consumer_cj = _make_compiled_joint(
        consumer_name,
        joint_type="sql",
        upstream=[source_name],
        engine="eng2",
        engine_type="polars",
        logical_plan=lp,
        column_lineage=consumer_lineage,
    )

    grp_src = _make_group([source_name], engine_type="databricks")
    grp_con = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        exit_joints=[consumer_name],
    )

    return {
        "groups": [grp_src, grp_con],
        "compiled_joints": {source_name: source_cj, consumer_name: consumer_cj},
        "capabilities": _FULL_CAPS,
        "catalog_types": {},
        "source_name": source_name,
        "limit_val": limit_val,
    }


# ---------------------------------------------------------------------------
# Property 14: Predicates and limits compose on the same source
# Feature: cross-group-limit-projection-pushdown, Property 14
# **Validates: Requirements 10.3**
# ---------------------------------------------------------------------------


@given(scenario=_predicates_and_limits_compose_scenario())
@settings(max_examples=100)
def test_property14_predicates_and_limits_compose_on_same_source(
    scenario: dict,
) -> None:
    """For any consumer group where both cross-group predicates and a
    cross-group limit are applicable to the same source joint, the
    cross-group pushdown pass shall push both the predicates (in
    per_joint_predicates) and the limit (in per_joint_limits) to that
    source group."""
    groups = scenario["groups"]
    compiled_joints = scenario["compiled_joints"]
    capabilities = scenario["capabilities"]
    catalog_types = scenario["catalog_types"]
    source_name = scenario["source_name"]
    limit_val = scenario["limit_val"]

    new_groups, results = cross_group_pushdown_pass(
        groups, compiled_joints, capabilities, catalog_types,
    )

    src_group = [g for g in new_groups if source_name in g.joints][0]

    # Both predicates and limits should be pushed to the same source
    assert source_name in src_group.per_joint_predicates, (
        f"Expected predicates pushed to source '{source_name}', "
        f"but per_joint_predicates = {src_group.per_joint_predicates}"
    )
    assert len(src_group.per_joint_predicates[source_name]) >= 1, (
        f"Expected at least one predicate pushed, "
        f"got {src_group.per_joint_predicates[source_name]}"
    )

    assert source_name in src_group.per_joint_limits, (
        f"Expected limit pushed to source '{source_name}', "
        f"but per_joint_limits = {src_group.per_joint_limits}"
    )
    assert src_group.per_joint_limits[source_name] == limit_val, (
        f"Expected limit {limit_val}, got {src_group.per_joint_limits[source_name]}"
    )


# ---------------------------------------------------------------------------
# Strategy: Property 15 — Input groups are not mutated
# ---------------------------------------------------------------------------

import copy


@st.composite
def _immutability_scenario(draw: st.DrawFn) -> dict:
    """Generate groups that will trigger projection and limit pushdown,
    so we can verify the originals are not mutated."""
    source_col = draw(_col_name_st)
    limit_val = draw(st.integers(min_value=1, max_value=10000))

    lp = LogicalPlan(
        projections=[Projection(expression=source_col, alias=None, source_columns=[source_col])],
        predicates=[],
        joins=[],
        aggregations=None,
        limit=Limit(count=limit_val, offset=None),
        ordering=None,
        distinct=False,
        source_tables=[],
    )

    source_name = "src_joint"
    consumer_name = "consumer_joint"

    consumer_lineage = [
        ColumnLineage(
            output_column=source_col,
            transform="direct",
            origins=[ColumnOrigin(joint=source_name, column=source_col)],
            expression=None,
        ),
    ]

    source_cj = _make_compiled_joint(
        source_name, joint_type="source", engine_type="databricks",
    )
    consumer_cj = _make_compiled_joint(
        consumer_name,
        joint_type="sql",
        upstream=[source_name],
        engine="eng2",
        engine_type="polars",
        logical_plan=lp,
        column_lineage=consumer_lineage,
    )

    grp_src = _make_group([source_name], engine_type="databricks")
    grp_con = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        exit_joints=[consumer_name],
    )

    return {
        "groups": [grp_src, grp_con],
        "compiled_joints": {source_name: source_cj, consumer_name: consumer_cj},
        "capabilities": _FULL_CAPS,
        "catalog_types": {},
    }


# ---------------------------------------------------------------------------
# Property 15: Input groups are not mutated
# Feature: cross-group-limit-projection-pushdown, Property 15
# **Validates: Requirements 8.3**
# ---------------------------------------------------------------------------


@given(scenario=_immutability_scenario())
@settings(max_examples=100)
def test_property15_input_groups_not_mutated(
    scenario: dict,
) -> None:
    """For any list of FusedGroup objects passed to cross_group_pushdown_pass,
    the original group objects shall not be modified. The pass shall return
    new FusedGroup instances via replace(), following the immutable-replace
    pattern."""
    groups = scenario["groups"]
    compiled_joints = scenario["compiled_joints"]
    capabilities = scenario["capabilities"]
    catalog_types = scenario["catalog_types"]

    # Deep-copy the input groups before calling the pass
    original_snapshot = copy.deepcopy(groups)

    new_groups, results = cross_group_pushdown_pass(
        groups, compiled_joints, capabilities, catalog_types,
    )

    # Verify the originals are unchanged after the pass
    assert len(groups) == len(original_snapshot), (
        f"Original groups list length changed: {len(groups)} vs {len(original_snapshot)}"
    )
    for orig, snap in zip(groups, original_snapshot):
        assert orig.id == snap.id, (
            f"Group id changed: {orig.id} vs {snap.id}"
        )
        assert orig.per_joint_predicates == snap.per_joint_predicates, (
            f"Group '{orig.id}' per_joint_predicates mutated: "
            f"{orig.per_joint_predicates} vs {snap.per_joint_predicates}"
        )
        assert orig.per_joint_projections == snap.per_joint_projections, (
            f"Group '{orig.id}' per_joint_projections mutated: "
            f"{orig.per_joint_projections} vs {snap.per_joint_projections}"
        )
        assert orig.per_joint_limits == snap.per_joint_limits, (
            f"Group '{orig.id}' per_joint_limits mutated: "
            f"{orig.per_joint_limits} vs {snap.per_joint_limits}"
        )
        assert orig.residual == snap.residual, (
            f"Group '{orig.id}' residual mutated: "
            f"{orig.residual} vs {snap.residual}"
        )
        assert orig.pushdown == snap.pushdown, (
            f"Group '{orig.id}' pushdown mutated: "
            f"{orig.pushdown} vs {snap.pushdown}"
        )


# ---------------------------------------------------------------------------
# Strategy: Property 16 — Every pushdown decision produces an OptimizationResult
# ---------------------------------------------------------------------------


@st.composite
def _observability_scenario(draw: st.DrawFn) -> dict:
    """Generate a consumer that triggers projection and limit pushdown
    decisions, so we can verify OptimizationResult entries are produced.

    Randomly chooses between scenarios that produce applied, skipped,
    or not_applicable results.
    """
    mode = draw(st.sampled_from([
        "applied_both",
        "select_star",
        "limit_blocked_agg",
        "no_proj_cap",
        "no_limit_cap",
    ]))

    source_col = draw(_col_name_st)
    limit_val = draw(st.integers(min_value=1, max_value=10000))

    source_name = "src_joint"
    consumer_name = "consumer_joint"

    if mode == "select_star":
        projections = [Projection(expression="*", alias=None, source_columns=[])]
        aggregations = None
        limit = Limit(count=limit_val, offset=None)
    elif mode == "limit_blocked_agg":
        projections = [Projection(expression=source_col, alias=None, source_columns=[source_col])]
        aggregations = Aggregation(group_by=[source_col], functions=[])
        limit = Limit(count=limit_val, offset=None)
    else:
        projections = [Projection(expression=source_col, alias=None, source_columns=[source_col])]
        aggregations = None
        limit = Limit(count=limit_val, offset=None)

    lp = LogicalPlan(
        projections=projections,
        predicates=[],
        joins=[],
        aggregations=aggregations,
        limit=limit,
        ordering=None,
        distinct=False,
        source_tables=[],
    )

    consumer_lineage = [
        ColumnLineage(
            output_column=source_col,
            transform="direct",
            origins=[ColumnOrigin(joint=source_name, column=source_col)],
            expression=None,
        ),
    ]

    source_cj = _make_compiled_joint(
        source_name, joint_type="source", engine_type="databricks",
    )
    consumer_cj = _make_compiled_joint(
        consumer_name,
        joint_type="sql",
        upstream=[source_name],
        engine="eng2",
        engine_type="polars",
        logical_plan=lp,
        column_lineage=consumer_lineage,
    )

    if mode == "no_proj_cap":
        caps: dict[str, list[str]] = {"databricks": ["predicate_pushdown", "limit_pushdown"]}
    elif mode == "no_limit_cap":
        caps = {"databricks": ["predicate_pushdown", "projection_pushdown"]}
    else:
        caps = dict(_FULL_CAPS)

    grp_src = _make_group([source_name], engine_type="databricks")
    grp_con = _make_group(
        [consumer_name], engine="eng2", engine_type="polars",
        exit_joints=[consumer_name],
    )

    return {
        "groups": [grp_src, grp_con],
        "compiled_joints": {source_name: source_cj, consumer_name: consumer_cj},
        "capabilities": caps,
        "catalog_types": {},
        "mode": mode,
    }


# ---------------------------------------------------------------------------
# Property 16: Every pushdown decision produces an OptimizationResult
# Feature: cross-group-limit-projection-pushdown, Property 16
# **Validates: Requirements 9.1, 9.2, 9.3, 9.4, 9.5**
# ---------------------------------------------------------------------------


@given(scenario=_observability_scenario())
@settings(max_examples=100)
def test_property16_every_pushdown_decision_produces_optimization_result(
    scenario: dict,
) -> None:
    """For any consumer group processed by the cross-group pushdown pass,
    each projection pushdown decision (applied, skipped, or not_applicable)
    shall produce an OptimizationResult with rule cross_group_projection_pushdown,
    and each limit pushdown decision shall produce an OptimizationResult with
    rule cross_group_limit_pushdown."""
    groups = scenario["groups"]
    compiled_joints = scenario["compiled_joints"]
    capabilities = scenario["capabilities"]
    catalog_types = scenario["catalog_types"]
    mode = scenario["mode"]

    new_groups, results = cross_group_pushdown_pass(
        groups, compiled_joints, capabilities, catalog_types,
    )

    proj_results = [r for r in results if r.rule == "cross_group_projection_pushdown"]
    lim_results = [r for r in results if r.rule == "cross_group_limit_pushdown"]

    if mode == "applied_both":
        # Both projection and limit should have 'applied' results
        assert any(r.status == "applied" for r in proj_results), (
            f"Expected 'applied' projection result in mode '{mode}', "
            f"got {proj_results}"
        )
        assert any(r.status == "applied" for r in lim_results), (
            f"Expected 'applied' limit result in mode '{mode}', "
            f"got {lim_results}"
        )

    elif mode == "select_star":
        # Projection should be 'skipped', limit may be applied
        assert any(r.status == "skipped" for r in proj_results), (
            f"Expected 'skipped' projection result for SELECT * in mode '{mode}', "
            f"got {proj_results}"
        )
        # Limit should still have a result (applied since no blocking constructs)
        assert len(lim_results) >= 1, (
            f"Expected at least one limit result in mode '{mode}', "
            f"got {lim_results}"
        )

    elif mode == "limit_blocked_agg":
        # Projection should be 'applied', limit should be 'skipped'
        assert any(r.status == "applied" for r in proj_results), (
            f"Expected 'applied' projection result in mode '{mode}', "
            f"got {proj_results}"
        )
        assert any(r.status == "skipped" for r in lim_results), (
            f"Expected 'skipped' limit result for aggregation in mode '{mode}', "
            f"got {lim_results}"
        )

    elif mode == "no_proj_cap":
        # Projection should be 'not_applicable', limit should be 'applied'
        assert any(r.status == "not_applicable" for r in proj_results), (
            f"Expected 'not_applicable' projection result in mode '{mode}', "
            f"got {proj_results}"
        )
        assert any(r.status == "applied" for r in lim_results), (
            f"Expected 'applied' limit result in mode '{mode}', "
            f"got {lim_results}"
        )

    elif mode == "no_limit_cap":
        # Projection should be 'applied', limit should be 'not_applicable'
        assert any(r.status == "applied" for r in proj_results), (
            f"Expected 'applied' projection result in mode '{mode}', "
            f"got {proj_results}"
        )
        assert any(r.status == "not_applicable" for r in lim_results), (
            f"Expected 'not_applicable' limit result in mode '{mode}', "
            f"got {lim_results}"
        )

    # Universal: every mode should produce at least one projection result
    # and at least one limit result
    assert len(proj_results) >= 1, (
        f"Expected at least one projection OptimizationResult, got none. "
        f"All results: {results}"
    )
    assert len(lim_results) >= 1, (
        f"Expected at least one limit OptimizationResult, got none. "
        f"All results: {results}"
    )
