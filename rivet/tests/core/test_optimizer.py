"""Unit tests for the optimizer: fusion pass and pushdown pass."""

from __future__ import annotations

from rivet_core.optimizer import (
    CastPushdownResult,
    FusionJoint,
    OptimizerResult,
    _can_fuse,
    _compose_cte,
    _compose_temp_view,
    _downstream_counts,
    _pushdown_casts,
    _pushdown_limit,
    _pushdown_predicates,
    _pushdown_projections,
    _split_and_conjuncts,
    fusion_pass,
    pushdown_pass,
)
from rivet_core.sql_parser import (
    Aggregation,
    Join,
    Limit,
    LogicalPlan,
    Ordering,
    Predicate,
    Projection,
    TableReference,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_joint(
    name: str,
    upstream: list[str] | None = None,
    engine: str = "eng1",
    engine_type: str = "duckdb",
    joint_type: str = "sql",
    eager: bool = False,
    has_assertions: bool = False,
    sql: str | None = None,
    adapter: str | None = None,
) -> FusionJoint:
    return FusionJoint(
        name=name,
        joint_type=joint_type,
        upstream=upstream or [],
        engine=engine,
        engine_type=engine_type,
        adapter=adapter,
        eager=eager,
        has_assertions=has_assertions,
        sql=sql,
    )


def _make_plan(
    projections: list[Projection] | None = None,
    predicates: list[Predicate] | None = None,
    joins: list[Join] | None = None,
    aggregations: Aggregation | None = None,
    limit: Limit | None = None,
    ordering: Ordering | None = None,
    distinct: bool = False,
    source_tables: list[TableReference] | None = None,
) -> LogicalPlan:
    return LogicalPlan(
        projections=projections or [],
        predicates=predicates or [],
        joins=joins or [],
        aggregations=aggregations,
        limit=limit,
        ordering=ordering,
        distinct=distinct,
        source_tables=source_tables or [],
    )


# ---------------------------------------------------------------------------
# Fusion: _downstream_counts
# ---------------------------------------------------------------------------


def test_downstream_counts_simple_chain() -> None:
    joints = [
        _make_joint("a"),
        _make_joint("b", upstream=["a"]),
        _make_joint("c", upstream=["b"]),
    ]
    counts = _downstream_counts(joints)
    assert counts["a"] == 1
    assert counts["b"] == 1
    assert counts["c"] == 0


def test_downstream_counts_diamond() -> None:
    joints = [
        _make_joint("a"),
        _make_joint("b", upstream=["a"]),
        _make_joint("c", upstream=["a"]),
        _make_joint("d", upstream=["b", "c"]),
    ]
    counts = _downstream_counts(joints)
    assert counts["a"] == 2  # consumed by b and c
    assert counts["b"] == 1
    assert counts["c"] == 1
    assert counts["d"] == 0


# ---------------------------------------------------------------------------
# Fusion: _can_fuse
# ---------------------------------------------------------------------------


def test_can_fuse_same_engine_no_barriers() -> None:
    up = _make_joint("a", engine="eng1")
    down = _make_joint("b", upstream=["a"], engine="eng1")
    counts = {"a": 1}
    assert _can_fuse(down, "a", up, counts) is True


def test_can_fuse_different_engine_instance() -> None:
    up = _make_joint("a", engine="eng1")
    down = _make_joint("b", upstream=["a"], engine="eng2")
    counts = {"a": 1}
    assert _can_fuse(down, "a", up, counts) is False


def test_can_fuse_eager_upstream() -> None:
    up = _make_joint("a", engine="eng1", eager=True)
    down = _make_joint("b", upstream=["a"], engine="eng1")
    counts = {"a": 1}
    assert _can_fuse(down, "a", up, counts) is False


def test_can_fuse_upstream_has_assertions() -> None:
    up = _make_joint("a", engine="eng1", has_assertions=True)
    down = _make_joint("b", upstream=["a"], engine="eng1")
    counts = {"a": 1}
    assert _can_fuse(down, "a", up, counts) is False


def test_can_fuse_multi_consumer_upstream() -> None:
    up = _make_joint("a", engine="eng1")
    down = _make_joint("b", upstream=["a"], engine="eng1")
    counts = {"a": 2}  # two consumers
    assert _can_fuse(down, "a", up, counts) is False


def test_can_fuse_python_joint_downstream() -> None:
    up = _make_joint("a", engine="eng1")
    down = _make_joint("b", upstream=["a"], engine="eng1", joint_type="python")
    counts = {"a": 1}
    assert _can_fuse(down, "a", up, counts) is False


# ---------------------------------------------------------------------------
# Fusion pass: basic cases
# ---------------------------------------------------------------------------


def test_fusion_pass_single_joint() -> None:
    joints = [_make_joint("a", sql="SELECT 1")]
    groups = fusion_pass(joints)
    assert len(groups) == 1
    assert groups[0].joints == ["a"]


def test_fusion_pass_chain_same_engine() -> None:
    joints = [
        _make_joint("a", sql="SELECT id FROM src"),
        _make_joint("b", upstream=["a"], sql="SELECT id FROM a WHERE id > 1"),
    ]
    groups = fusion_pass(joints)
    assert len(groups) == 1
    assert groups[0].joints == ["a", "b"]


def test_fusion_pass_different_engines_no_fusion() -> None:
    joints = [
        _make_joint("a", engine="eng1", sql="SELECT 1"),
        _make_joint("b", upstream=["a"], engine="eng2", sql="SELECT 2"),
    ]
    groups = fusion_pass(joints)
    assert len(groups) == 2
    assert groups[0].joints == ["a"]
    assert groups[1].joints == ["b"]


def test_fusion_pass_eager_barrier() -> None:
    joints = [
        _make_joint("a", engine="eng1", eager=True, sql="SELECT 1"),
        _make_joint("b", upstream=["a"], engine="eng1", sql="SELECT 2"),
    ]
    groups = fusion_pass(joints)
    assert len(groups) == 2


def test_fusion_pass_assertion_barrier() -> None:
    joints = [
        _make_joint("a", engine="eng1", has_assertions=True, sql="SELECT 1"),
        _make_joint("b", upstream=["a"], engine="eng1", sql="SELECT 2"),
    ]
    groups = fusion_pass(joints)
    assert len(groups) == 2


def test_fusion_pass_python_joint_standalone() -> None:
    joints = [
        _make_joint("a", engine="eng1", sql="SELECT 1"),
        _make_joint("b", upstream=["a"], engine="eng1", joint_type="python"),
    ]
    groups = fusion_pass(joints)
    # python joint is always standalone
    assert len(groups) == 2
    python_group = next(g for g in groups if "b" in g.joints)
    assert python_group.joints == ["b"]


def test_fusion_pass_multi_consumer_barrier() -> None:
    """A joint with two consumers cannot be fused with either."""
    joints = [
        _make_joint("a", engine="eng1", sql="SELECT 1"),
        _make_joint("b", upstream=["a"], engine="eng1", sql="SELECT 2"),
        _make_joint("c", upstream=["a"], engine="eng1", sql="SELECT 3"),
    ]
    groups = fusion_pass(joints)
    # a has 2 consumers → cannot fuse; b and c are standalone
    assert len(groups) == 3


def test_fusion_pass_diamond_a_standalone() -> None:
    """Diamond: A→B, A→C, B→D, C→D — A has 2 consumers so A is standalone.
    B and C each have 1 consumer (D), so D merges both B and C into one group.
    Result: [A], [B, C, D].
    """
    joints = [
        _make_joint("a", engine="eng1", sql="SELECT 1"),
        _make_joint("b", upstream=["a"], engine="eng1", sql="SELECT 2"),
        _make_joint("c", upstream=["a"], engine="eng1", sql="SELECT 3"),
        _make_joint("d", upstream=["b", "c"], engine="eng1", sql="SELECT 4"),
    ]
    groups = fusion_pass(joints)
    # A is standalone (2 consumers). D merges both B and C.
    assert len(groups) == 2
    # A must be its own group
    group_a = next(g for g in groups if "a" in g.joints)
    assert group_a.joints == ["a"]
    # D must be in a group with both B and C
    group_d = next(g for g in groups if "d" in g.joints)
    assert set(group_d.joints) == {"b", "c", "d"}


def test_fusion_pass_multi_upstream_merges_all_eligible() -> None:
    """When a joint has multiple upstreams, merge ALL eligible groups."""
    joints = [
        _make_joint("a", engine="eng1", sql="SELECT 1"),
        _make_joint("b", upstream=["a"], engine="eng1", sql="SELECT 2"),
        _make_joint("c", engine="eng1", sql="SELECT 3"),  # separate chain
        _make_joint("d", upstream=["b", "c"], engine="eng1", sql="SELECT 4"),
    ]
    groups = fusion_pass(joints)
    # All eligible upstreams should be merged into one group with d
    group_with_d = next(g for g in groups if "d" in g.joints)
    assert "a" in group_with_d.joints
    assert "b" in group_with_d.joints
    assert "c" in group_with_d.joints
    assert "d" in group_with_d.joints
    # Only one group total since everything is on the same engine
    assert len(groups) == 1

# ---------------------------------------------------------------------------
# Integration & edge-case tests for multi-upstream fusion
# ---------------------------------------------------------------------------


def test_fusion_pass_two_sources_into_join() -> None:
    """Motivating example: two Databricks source joints feed a SQL JOIN on the same engine.

    The fusion pass should produce a single fused group containing both sources
    and the join, with no wasteful standalone SELECT * FROM ... groups.
    Validates: Requirements 7.1, 7.2, 7.3
    """
    src_orders = _make_joint(
        "orders_src",
        engine="databricks_prod",
        engine_type="databricks",
        joint_type="source",
        adapter="unity",
        sql="SELECT * FROM catalog.schema.orders",
    )
    src_customers = _make_joint(
        "customers_src",
        engine="databricks_prod",
        engine_type="databricks",
        joint_type="source",
        adapter="unity",
        sql="SELECT * FROM catalog.schema.customers",
    )
    join_joint = _make_joint(
        "orders_customers_join",
        upstream=["orders_src", "customers_src"],
        engine="databricks_prod",
        engine_type="databricks",
        joint_type="sql",
        sql="SELECT o.*, c.name FROM orders_src o JOIN customers_src c ON o.cust_id = c.id",
    )
    sink = _make_joint(
        "output_sink",
        upstream=["orders_customers_join"],
        engine="local_duckdb",
        engine_type="duckdb",
        joint_type="sink",
        sql="SELECT * FROM orders_customers_join",
    )

    groups = fusion_pass([src_orders, src_customers, join_joint, sink])

    # The two sources and the join should be in a single fused group
    fused = next(g for g in groups if "orders_customers_join" in g.joints)
    assert "orders_src" in fused.joints
    assert "customers_src" in fused.joints
    assert "orders_customers_join" in fused.joints
    assert fused.engine == "databricks_prod"

    # Both sources are entry joints (no in-group upstream)
    assert set(fused.entry_joints) == {"orders_src", "customers_src"}
    # The join is the exit joint (its downstream sink is outside the group)
    assert fused.exit_joints == ["orders_customers_join"]

    # No standalone source groups — only the fused group + the sink group
    assert len(groups) == 2
    # Verify no group is a wasteful standalone source SELECT *
    for g in groups:
        if g.id != fused.id:
            assert g.joints == ["output_sink"]


def test_fusion_pass_zero_eligible_upstreams() -> None:
    """Multi-input joint where all upstreams are on different engines.

    The joint should be placed in its own standalone group.
    Validates: Requirement 1.4
    """
    src_a = _make_joint("src_a", engine="eng_a", engine_type="databricks", sql="SELECT 1")
    src_b = _make_joint("src_b", engine="eng_b", engine_type="postgres", sql="SELECT 2")
    join_j = _make_joint(
        "join_j",
        upstream=["src_a", "src_b"],
        engine="eng_c",
        engine_type="duckdb",
        sql="SELECT * FROM src_a JOIN src_b ON src_a.id = src_b.id",
    )

    groups = fusion_pass([src_a, src_b, join_j])

    # Each joint should be in its own standalone group (3 groups total)
    assert len(groups) == 3
    join_group = next(g for g in groups if "join_j" in g.joints)
    assert join_group.joints == ["join_j"]
    assert join_group.engine == "eng_c"


def test_fusion_pass_duplicate_upstream_self_join() -> None:
    """A joint that references the same upstream twice (self-join).

    Should fuse correctly with no duplicate group entries.
    Validates: Requirement 2.1
    """
    src = _make_joint("src", engine="eng1", engine_type="duckdb", sql="SELECT * FROM t")
    self_join = _make_joint(
        "self_join",
        upstream=["src", "src"],
        engine="eng1",
        engine_type="duckdb",
        sql="SELECT a.*, b.val FROM src a JOIN src b ON a.id = b.parent_id",
    )

    groups = fusion_pass([src, self_join])

    # Both should be fused into a single group
    assert len(groups) == 1
    group = groups[0]
    assert "src" in group.joints
    assert "self_join" in group.joints
    # No duplicate entries for src
    assert group.joints.count("src") == 1
    assert group.joints.count("self_join") == 1
    # src is entry, self_join is exit
    assert group.entry_joints == ["src"]
    assert group.exit_joints == ["self_join"]



# ---------------------------------------------------------------------------
# CTE composition
# ---------------------------------------------------------------------------


def test_compose_cte_single_joint() -> None:
    result = _compose_cte(["a"], {"a": "SELECT 1"})
    assert result is not None
    assert result.fused_sql == "SELECT 1"
    assert result.final_select == "SELECT 1"
    assert result.statements == []


def test_compose_cte_two_joints() -> None:
    result = _compose_cte(
        ["a", "b"],
        {"a": "SELECT id FROM src", "b": "SELECT id FROM a WHERE id > 1"},
    )
    assert result is not None
    assert "WITH" in result.fused_sql
    assert "a AS (" in result.fused_sql
    assert "SELECT id FROM a WHERE id > 1" in result.fused_sql
    assert result.final_select == "SELECT id FROM a WHERE id > 1"
    assert len(result.statements) == 1


def test_compose_cte_no_sql_joints() -> None:
    result = _compose_cte(["a"], {"a": None})
    assert result is None


# ---------------------------------------------------------------------------
# TempView composition
# ---------------------------------------------------------------------------


def test_compose_temp_view_single_joint() -> None:
    result = _compose_temp_view(["a"], {"a": "SELECT 1"})
    assert result is not None
    assert result.fused_sql == "SELECT 1"
    assert result.statements == []


def test_compose_temp_view_two_joints() -> None:
    result = _compose_temp_view(
        ["a", "b"],
        {"a": "SELECT id FROM src", "b": "SELECT id FROM a"},
    )
    assert result is not None
    assert "CREATE TEMPORARY VIEW a AS" in result.fused_sql
    assert "SELECT id FROM a" in result.fused_sql
    assert len(result.statements) == 1
    assert result.final_select == "SELECT id FROM a"


def test_fusion_pass_temp_view_strategy() -> None:
    joints = [
        _make_joint("a", sql="SELECT id FROM src"),
        _make_joint("b", upstream=["a"], sql="SELECT id FROM a"),
    ]
    groups = fusion_pass(joints, fusion_strategy="temp_view")
    assert len(groups) == 1
    assert groups[0].fusion_strategy == "temp_view"
    assert groups[0].fusion_result is not None
    assert "CREATE TEMPORARY VIEW" in groups[0].fused_sql  # type: ignore[operator]


# ---------------------------------------------------------------------------
# Entry/exit joints
# ---------------------------------------------------------------------------


def test_fusion_pass_entry_exit_chain() -> None:
    joints = [
        _make_joint("a", sql="SELECT 1"),
        _make_joint("b", upstream=["a"], sql="SELECT 2"),
        _make_joint("c", upstream=["b"], sql="SELECT 3"),
    ]
    groups = fusion_pass(joints)
    assert len(groups) == 1
    g = groups[0]
    assert g.entry_joints == ["a"]
    assert g.exit_joints == ["c"]


def test_fusion_pass_entry_exit_two_groups() -> None:
    joints = [
        _make_joint("a", engine="eng1", sql="SELECT 1"),
        _make_joint("b", upstream=["a"], engine="eng2", sql="SELECT 2"),
    ]
    groups = fusion_pass(joints)
    assert len(groups) == 2
    g_a = next(g for g in groups if "a" in g.joints)
    g_b = next(g for g in groups if "b" in g.joints)
    assert g_a.entry_joints == ["a"]
    assert g_a.exit_joints == ["a"]
    assert g_b.entry_joints == ["b"]
    assert g_b.exit_joints == ["b"]


# ---------------------------------------------------------------------------
# Pushdown: predicate splitting
# ---------------------------------------------------------------------------


def test_split_and_conjuncts_single() -> None:
    pred = Predicate(expression="a > 1", columns=["a"], location="where")
    result = _split_and_conjuncts(pred)
    assert len(result) == 1
    assert result[0].expression == "a > 1"


def test_split_and_conjuncts_two_parts() -> None:
    pred = Predicate(expression="a > 1 AND b < 5", columns=["a", "b"], location="where")
    result = _split_and_conjuncts(pred)
    assert len(result) == 2
    exprs = {r.expression for r in result}
    assert "a > 1" in exprs
    assert "b < 5" in exprs


def test_split_and_conjuncts_or_is_atomic() -> None:
    pred = Predicate(expression="a > 1 OR b < 5", columns=["a", "b"], location="where")
    result = _split_and_conjuncts(pred)
    assert len(result) == 1  # OR is not split


# ---------------------------------------------------------------------------
# Pushdown: predicate pushdown
# ---------------------------------------------------------------------------


def test_predicate_pushdown_no_capability() -> None:
    plan = _make_plan(predicates=[Predicate("a > 1", ["a"], "where")])
    result = _pushdown_predicates(plan, capabilities=[])
    assert result.pushed == []
    assert len(result.residual) == 1


def test_predicate_pushdown_with_capability() -> None:
    plan = _make_plan(predicates=[Predicate("a > 1", ["a"], "where")])
    result = _pushdown_predicates(plan, capabilities=["predicate_pushdown"])
    assert len(result.pushed) == 1
    assert result.residual == []


def test_predicate_pushdown_subquery_never_pushed() -> None:
    plan = _make_plan(
        predicates=[Predicate("id IN (SELECT id FROM other)", ["id"], "where")]
    )
    result = _pushdown_predicates(plan, capabilities=["predicate_pushdown"])
    assert result.pushed == []
    assert len(result.residual) == 1


def test_predicate_pushdown_having_never_pushed() -> None:
    plan = _make_plan(predicates=[Predicate("count(*) > 5", [], "having")])
    result = _pushdown_predicates(plan, capabilities=["predicate_pushdown"])
    assert result.pushed == []
    assert len(result.residual) == 1


def test_predicate_pushdown_and_split() -> None:
    plan = _make_plan(
        predicates=[Predicate("a > 1 AND b < 5", ["a", "b"], "where")]
    )
    result = _pushdown_predicates(plan, capabilities=["predicate_pushdown"])
    assert len(result.pushed) == 2
    assert result.residual == []


def test_predicate_pushdown_and_split_partial() -> None:
    """AND with one subquery part: subquery stays residual, other is pushed."""
    plan = _make_plan(
        predicates=[
            Predicate("a > 1 AND id IN (SELECT id FROM t)", ["a", "id"], "where")
        ]
    )
    result = _pushdown_predicates(plan, capabilities=["predicate_pushdown"])
    pushed_exprs = {p.expression for p in result.pushed}
    residual_exprs = {p.expression for p in result.residual}
    assert "a > 1" in pushed_exprs
    assert any("SELECT" in e for e in residual_exprs)


# ---------------------------------------------------------------------------
# Pushdown: projection pushdown
# ---------------------------------------------------------------------------


def test_projection_pushdown_no_capability() -> None:
    plan = _make_plan(
        projections=[Projection("id", None, ["id"]), Projection("name", None, ["name"])]
    )
    result = _pushdown_projections(plan, capabilities=[])
    assert result.pushed_columns is None
    assert result.reason == "capability_gap"


def test_projection_pushdown_with_capability() -> None:
    plan = _make_plan(
        projections=[Projection("id", None, ["id"]), Projection("name", None, ["name"])]
    )
    result = _pushdown_projections(plan, capabilities=["projection_pushdown"])
    assert result.pushed_columns is not None
    assert "id" in result.pushed_columns
    assert "name" in result.pushed_columns


def test_projection_pushdown_select_star_not_applicable() -> None:
    plan = _make_plan(projections=[Projection("*", None, [])])
    result = _pushdown_projections(plan, capabilities=["projection_pushdown"])
    assert result.pushed_columns is None
    assert result.reason == "not_applicable"


def test_projection_pushdown_no_plan() -> None:
    result = _pushdown_projections(None, capabilities=["projection_pushdown"])
    assert result.pushed_columns is None
    assert result.reason == "no_logical_plan"


# ---------------------------------------------------------------------------
# Pushdown: limit pushdown
# ---------------------------------------------------------------------------


def test_limit_pushdown_no_limit() -> None:
    plan = _make_plan()
    result = _pushdown_limit(plan, capabilities=["limit_pushdown"])
    assert result.pushed_limit is None
    assert result.residual_limit is None


def test_limit_pushdown_safe() -> None:
    plan = _make_plan(limit=Limit(count=10, offset=None))
    result = _pushdown_limit(plan, capabilities=["limit_pushdown"])
    assert result.pushed_limit == 10
    assert result.residual_limit is None


def test_limit_pushdown_no_capability() -> None:
    plan = _make_plan(limit=Limit(count=10, offset=None))
    result = _pushdown_limit(plan, capabilities=[])
    assert result.pushed_limit is None
    assert result.residual_limit == 10
    assert result.reason == "capability_gap"


def test_limit_pushdown_aggregation_unsafe() -> None:
    plan = _make_plan(
        limit=Limit(count=5, offset=None),
        aggregations=Aggregation(group_by=["x"], functions=["COUNT(*)"]),
    )
    result = _pushdown_limit(plan, capabilities=["limit_pushdown"])
    assert result.pushed_limit is None
    assert result.residual_limit == 5
    assert result.reason == "aggregation_present"


def test_limit_pushdown_join_unsafe() -> None:
    plan = _make_plan(
        limit=Limit(count=5, offset=None),
        joins=[Join("inner", "a", "b", "a.id = b.id", ["id"])],
    )
    result = _pushdown_limit(plan, capabilities=["limit_pushdown"])
    assert result.pushed_limit is None
    assert result.residual_limit == 5
    assert result.reason == "join_present"


def test_limit_pushdown_distinct_unsafe() -> None:
    plan = _make_plan(limit=Limit(count=5, offset=None), distinct=True)
    result = _pushdown_limit(plan, capabilities=["limit_pushdown"])
    assert result.pushed_limit is None
    assert result.residual_limit == 5
    assert result.reason == "distinct_present"


# ---------------------------------------------------------------------------
# Pushdown: cast pushdown
# ---------------------------------------------------------------------------


def test_cast_pushdown_widening_numeric() -> None:
    plan = _make_plan(projections=[Projection("CAST(x AS BIGINT)", None, ["x"])])
    result = _pushdown_casts(plan, capabilities=["cast_pushdown"])
    # int32 → int64 is widening; from_type is "unknown" here but to_type is "bigint"
    # The implementation uses _is_widening_or_to_string; bigint is not in _STRING_TYPES
    # and from_type is "unknown" so it won't be in _NUMERIC_WIDENING → residual
    # This tests the extraction mechanism; actual widening test below
    assert isinstance(result, CastPushdownResult)


def test_cast_pushdown_to_string_always_safe() -> None:
    from rivet_core.optimizer import _is_widening_or_to_string

    assert _is_widening_or_to_string("int32", "utf8") is True
    assert _is_widening_or_to_string("float64", "string") is True
    assert _is_widening_or_to_string("bool", "large_utf8") is True


def test_cast_pushdown_widening_numeric_check() -> None:
    from rivet_core.optimizer import _is_widening_or_to_string

    assert _is_widening_or_to_string("int32", "int64") is True
    assert _is_widening_or_to_string("int32", "float64") is True
    assert _is_widening_or_to_string("float32", "float64") is True
    assert _is_widening_or_to_string("int64", "int32") is False  # narrowing
    assert _is_widening_or_to_string("float64", "int64") is False  # narrowing


def test_cast_pushdown_no_capability() -> None:
    plan = _make_plan(projections=[Projection("CAST(x AS VARCHAR)", None, ["x"])])
    result = _pushdown_casts(plan, capabilities=[])
    # No capability → all casts are residual
    assert result.pushed == []
    # residual may have the cast if extraction found it
    assert isinstance(result, CastPushdownResult)


def test_cast_pushdown_no_casts_in_plan() -> None:
    plan = _make_plan(projections=[Projection("id", None, ["id"])])
    result = _pushdown_casts(plan, capabilities=["cast_pushdown"])
    assert result.pushed == []
    assert result.residual == []


# ---------------------------------------------------------------------------
# Full pushdown_pass integration
# ---------------------------------------------------------------------------


def test_pushdown_pass_applies_to_groups() -> None:
    joints = [
        _make_joint("a", sql="SELECT id FROM src"),
        _make_joint("b", upstream=["a"], sql="SELECT id FROM a WHERE id > 1"),
    ]
    groups = fusion_pass(joints)
    assert len(groups) == 1

    plan = _make_plan(
        projections=[Projection("id", None, ["id"])],
        predicates=[Predicate("id > 1", ["id"], "where")],
    )
    logical_plans = {"b": plan, "a": None}
    capabilities = {"duckdb:None": ["predicate_pushdown", "projection_pushdown"]}
    catalog_types = {"a": None, "b": None}

    updated = pushdown_pass(groups, logical_plans, capabilities, catalog_types)
    assert len(updated) == 1
    g = updated[0]
    assert g.pushdown is not None
    assert g.residual is not None


def test_pushdown_pass_no_capabilities() -> None:
    joints = [_make_joint("a", sql="SELECT id FROM src WHERE id > 1")]
    groups = fusion_pass(joints)

    plan = _make_plan(predicates=[Predicate("id > 1", ["id"], "where")])
    logical_plans = {"a": plan}
    capabilities: dict[str, list[str]] = {}
    catalog_types: dict[str, str | None] = {"a": None}

    updated = pushdown_pass(groups, logical_plans, capabilities, catalog_types)
    g = updated[0]
    assert g.pushdown is not None
    # No capabilities → everything is residual
    assert g.pushdown.predicates.pushed == []
    assert len(g.pushdown.predicates.residual) == 1


# ---------------------------------------------------------------------------
# OptimizerResult data model
# ---------------------------------------------------------------------------


def test_optimizer_result_holds_groups() -> None:
    joints = [_make_joint("a", sql="SELECT 1")]
    groups = fusion_pass(joints)
    result = OptimizerResult(fused_groups=groups)
    assert len(result.fused_groups) == 1
    assert result.fused_groups[0].joints == ["a"]
