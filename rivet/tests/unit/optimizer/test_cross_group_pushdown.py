"""Unit tests for cross-group predicate pushdown in the optimizer.

Exhaustive examples covering all scenarios from task 7 (subtasks 7.1–7.36).
"""

from __future__ import annotations

import uuid

from rivet_core.compiler import CompiledJoint
from rivet_core.executor import _merge_cross_group_predicates
from rivet_core.lineage import ColumnLineage, ColumnOrigin
from rivet_core.optimizer import (
    CastPushdownResult,
    FusedGroup,
    LimitPushdownResult,
    PredicatePushdownResult,
    ProjectionPushdownResult,
    PushdownPlan,
    cross_group_pushdown_pass,
)
from rivet_core.sql_parser import (
    Join,
    LogicalPlan,
    Ordering,
    Predicate,
)

# ---------------------------------------------------------------------------
# Helpers — reuse the same patterns as the property tests
# ---------------------------------------------------------------------------

_EMPTY_PLAN = LogicalPlan(
    projections=[],
    predicates=[],
    joins=[],
    aggregations=None,
    limit=None,
    ordering=None,
    distinct=False,
    source_tables=[],
)


def _make_cj(
    name: str,
    *,
    joint_type: str = "source",
    upstream: list[str] | None = None,
    engine: str = "eng1",
    engine_type: str = "databricks",
    logical_plan: LogicalPlan | None = None,
    column_lineage: list[ColumnLineage] | None = None,
    catalog_type: str | None = None,
) -> CompiledJoint:
    return CompiledJoint(
        name=name,
        type=joint_type,
        catalog=None,
        catalog_type=catalog_type,
        engine=engine,
        engine_resolution=None,
        adapter=None,
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
    group_id: str | None = None,
    engine: str = "eng1",
    engine_type: str = "databricks",
    entry_joints: list[str] | None = None,
    exit_joints: list[str] | None = None,
    adapters: dict[str, str | None] | None = None,
    per_joint_predicates: dict[str, list[Predicate]] | None = None,
) -> FusedGroup:
    return FusedGroup(
        id=group_id or str(uuid.uuid4()),
        joints=joints,
        engine=engine,
        engine_type=engine_type,
        adapters=adapters or {j: None for j in joints},
        fused_sql=None,
        entry_joints=entry_joints or joints[:1],
        exit_joints=exit_joints or joints[-1:],
        per_joint_predicates=per_joint_predicates or {},
    )


def _source_lineage(col: str) -> ColumnLineage:
    """Lineage for a source joint column (no upstream origins)."""
    return ColumnLineage(output_column=col, transform="source", origins=[], expression=None)


def _direct_lineage(col: str, source_joint: str) -> ColumnLineage:
    """Direct lineage: consumer col traces to same-named col on source joint."""
    return ColumnLineage(
        output_column=col,
        transform="direct",
        origins=[ColumnOrigin(joint=source_joint, column=col)],
        expression=None,
    )


def _renamed_lineage(consumer_col: str, source_joint: str, source_col: str) -> ColumnLineage:
    """Renamed lineage: consumer col traces to differently-named col on source joint."""
    return ColumnLineage(
        output_column=consumer_col,
        transform="renamed",
        origins=[ColumnOrigin(joint=source_joint, column=source_col)],
        expression=None,
    )


_CAPABLE = {"databricks": ["predicate_pushdown"]}
_CAPABLE_BOTH = {"databricks": ["predicate_pushdown"], "polars": ["predicate_pushdown"]}
_INCAPABLE: dict[str, list[str]] = {"databricks": []}
_EMPTY_CATALOG_TYPES: dict[str, str | None] = {}


def _make_pushdown_plan(pushed: list[Predicate] | None = None) -> PushdownPlan:
    return PushdownPlan(
        predicates=PredicatePushdownResult(pushed=pushed or [], residual=[]),
        projections=ProjectionPushdownResult(pushed_columns=None, reason=None),
        limit=LimitPushdownResult(pushed_limit=None, residual_limit=None, reason=None),
        casts=CastPushdownResult(pushed=[], residual=[]),
    )


# ===================================================================
# 7.1 Motivating example — Polars SQL joining two Databricks sources
# ===================================================================

class TestMotivatingExample:
    """7.1: Polars consumer joins two Databricks sources with WHERE filter."""

    def test_predicate_pushed_to_correct_source(self):
        src_a = _make_cj("db_src_a", engine_type="databricks",
                         column_lineage=[_source_lineage("correlation_id")])
        src_b = _make_cj("db_src_b", engine_type="databricks",
                         column_lineage=[_source_lineage("amount")])

        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="correlation_id = 'abc'",
                                  columns=["correlation_id"], location="where")],
        )
        consumer = _make_cj(
            "polars_consumer", joint_type="sql", upstream=["db_src_a", "db_src_b"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("correlation_id", "db_src_a"),
                            _direct_lineage("amount", "db_src_b")],
        )

        grp_a = _make_group(["db_src_a"], engine_type="databricks")
        grp_b = _make_group(["db_src_b"], engine_type="databricks")
        grp_c = _make_group(["polars_consumer"], engine="eng2", engine_type="polars",
                            entry_joints=["polars_consumer"], exit_joints=["polars_consumer"])

        cj_map = {c.name: c for c in [src_a, src_b, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_a, grp_b, grp_c], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        # Predicate should be pushed to source_a, not source_b
        updated_a = next(g for g in new_groups if "db_src_a" in g.joints)
        updated_b = next(g for g in new_groups if "db_src_b" in g.joints)

        assert "db_src_a" in updated_a.per_joint_predicates
        pushed = updated_a.per_joint_predicates["db_src_a"]
        assert len(pushed) == 1
        assert "correlation_id" in pushed[0].expression
        assert "'abc'" in pushed[0].expression

        assert not updated_b.per_joint_predicates


# ===================================================================
# 7.2 Renamed column pushdown
# ===================================================================

class TestRenamedColumnPushdown:
    """7.2: Source has corr_id, consumer has correlation_id via renamed lineage."""

    def test_rewritten_predicate_uses_source_column_name(self):
        src = _make_cj("src", column_lineage=[_source_lineage("corr_id")])

        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="correlation_id = 'abc'",
                                  columns=["correlation_id"], location="where")],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_renamed_lineage("correlation_id", "src", "corr_id")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        pushed = updated_src.per_joint_predicates["src"]
        assert len(pushed) == 1
        assert "corr_id" in pushed[0].expression
        assert "correlation_id" not in pushed[0].expression
        assert pushed[0].columns == ["corr_id"]


# ===================================================================
# 7.3 Table-qualified predicate alias stripping
# ===================================================================

class TestTableQualifiedAliasStripping:
    """7.3: WHERE t1.status = 'active' — alias stripped in rewritten predicate."""

    def test_alias_stripped(self):
        src = _make_cj("src", column_lineage=[_source_lineage("status")])

        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="t1.status = 'active'",
                                  columns=["status"], location="where")],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("status", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        pushed = next(g for g in new_groups if "src" in g.joints).per_joint_predicates["src"]
        assert len(pushed) == 1
        # t1. prefix should be stripped
        assert "t1." not in pushed[0].expression
        assert "status" in pushed[0].expression


# ===================================================================
# 7.4 Multiple predicates to same source
# ===================================================================

class TestMultiplePredicatesSameSource:
    """7.4: WHERE a.col1 = 'x' AND a.col2 = 'y' both targeting source_a."""

    def test_both_pushed_to_same_source(self):
        src = _make_cj("src_a", column_lineage=[
            _source_lineage("col1"), _source_lineage("col2"),
        ])

        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="col1 = 'x' AND col2 = 'y'",
                                  columns=["col1", "col2"], location="where")],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src_a"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col1", "src_a"),
                            _direct_lineage("col2", "src_a")],
        )

        grp_src = _make_group(["src_a"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        pushed = next(g for g in new_groups if "src_a" in g.joints).per_joint_predicates["src_a"]
        assert len(pushed) == 2
        exprs = {p.expression for p in pushed}
        assert any("col1" in e and "'x'" in e for e in exprs)
        assert any("col2" in e and "'y'" in e for e in exprs)


# ===================================================================
# 7.5 Multiple predicates to different sources
# ===================================================================

class TestMultiplePredicatesDifferentSources:
    """7.5: WHERE a.col1 = 'x' AND b.col2 = 'y' targeting source_a and source_b."""

    def test_each_predicate_goes_to_correct_source(self):
        src_a = _make_cj("src_a", column_lineage=[_source_lineage("col1")])
        src_b = _make_cj("src_b", column_lineage=[_source_lineage("col2")])

        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="col1 = 'x' AND col2 = 'y'",
                                  columns=["col1", "col2"], location="where")],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src_a", "src_b"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col1", "src_a"),
                            _direct_lineage("col2", "src_b")],
        )

        grp_a = _make_group(["src_a"], engine_type="databricks")
        grp_b = _make_group(["src_b"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src_a, src_b, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_a, grp_b, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        updated_a = next(g for g in new_groups if "src_a" in g.joints)
        updated_b = next(g for g in new_groups if "src_b" in g.joints)

        pushed_a = updated_a.per_joint_predicates["src_a"]
        pushed_b = updated_b.per_joint_predicates["src_b"]

        assert len(pushed_a) == 1
        assert "col1" in pushed_a[0].expression
        assert len(pushed_b) == 1
        assert "col2" in pushed_b[0].expression


# ===================================================================
# 7.6 Mixed pushability — partial conjunct split
# ===================================================================

class TestMixedPushability:
    """7.6: WHERE a.col1 = 'x' AND a.col1 = b.col2 — first pushable, second not."""

    def test_first_pushed_second_skipped(self):
        src_a = _make_cj("src_a", column_lineage=[_source_lineage("col1")])
        src_b = _make_cj("src_b", column_lineage=[_source_lineage("col2")])

        # Two separate predicates: one single-origin, one cross-source
        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[
                Predicate(expression="col1 = 'x'", columns=["col1"], location="where"),
                Predicate(expression="col1 = col2", columns=["col1", "col2"], location="where"),
            ],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src_a", "src_b"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col1", "src_a"),
                            _direct_lineage("col2", "src_b")],
        )

        grp_a = _make_group(["src_a"], engine_type="databricks")
        grp_b = _make_group(["src_b"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src_a, src_b, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_a, grp_b, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        # First predicate pushed to src_a
        updated_a = next(g for g in new_groups if "src_a" in g.joints)
        assert "src_a" in updated_a.per_joint_predicates
        pushed = updated_a.per_joint_predicates["src_a"]
        assert any("col1" in p.expression and "'x'" in p.expression for p in pushed)

        # Second predicate skipped (multi-source)
        skipped = [r for r in results if r.status == "skipped"]
        assert len(skipped) >= 1


# ===================================================================
# 7.7 HAVING predicate not pushed
# ===================================================================

class TestHavingNotPushed:
    """7.7: Consumer with only HAVING predicates — none pushed."""

    def test_having_predicate_skipped(self):
        src = _make_cj("src", column_lineage=[_source_lineage("cnt")])

        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="cnt > 5", columns=["cnt"], location="having")],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("cnt", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert not updated_src.per_joint_predicates
        assert any(r.status == "skipped" for r in results)


# ===================================================================
# 7.8 Subquery predicate not pushed
# ===================================================================

class TestSubqueryNotPushed:
    """7.8: WHERE a.col IN (SELECT ...) — subquery not pushed."""

    def test_subquery_predicate_skipped(self):
        src = _make_cj("src", column_lineage=[_source_lineage("col")])

        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="col IN (SELECT id FROM other)",
                                  columns=["col"], location="where")],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert not updated_src.per_joint_predicates
        assert any(r.status == "skipped" for r in results)


# ===================================================================
# 7.9 Aggregation-derived column not pushed
# ===================================================================

class TestAggregationDerivedNotPushed:
    """7.9: WHERE count_col > 5 with aggregation transform — not pushed."""

    def test_aggregation_column_skipped(self):
        src = _make_cj("src", column_lineage=[_source_lineage("id")])

        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="count_col > 5",
                                  columns=["count_col"], location="where")],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[
                ColumnLineage(output_column="count_col", transform="aggregation",
                              origins=[ColumnOrigin(joint="src", column="id")],
                              expression="COUNT(id)"),
            ],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert not updated_src.per_joint_predicates
        assert any(r.status == "skipped" for r in results)


# ===================================================================
# 7.10 Window-function-derived column not pushed
# ===================================================================

class TestWindowDerivedNotPushed:
    """7.10: WHERE rank_col = 1 with window transform — not pushed."""

    def test_window_column_skipped(self):
        src = _make_cj("src", column_lineage=[_source_lineage("id")])

        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="rank_col = 1",
                                  columns=["rank_col"], location="where")],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[
                ColumnLineage(output_column="rank_col", transform="window",
                              origins=[ColumnOrigin(joint="src", column="id")],
                              expression="ROW_NUMBER() OVER (ORDER BY id)"),
            ],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert not updated_src.per_joint_predicates
        assert any(r.status == "skipped" for r in results)


# ===================================================================
# 7.11 Multi-origin expression column not pushed
# ===================================================================

class TestMultiOriginNotPushed:
    """7.11: WHERE combined = 'x' tracing to two sources — not pushed."""

    def test_multi_origin_skipped(self):
        src_a = _make_cj("src_a", column_lineage=[_source_lineage("first")])
        src_b = _make_cj("src_b", column_lineage=[_source_lineage("last")])

        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="combined = 'x'",
                                  columns=["combined"], location="where")],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src_a", "src_b"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[
                ColumnLineage(output_column="combined", transform="expression",
                              origins=[ColumnOrigin(joint="src_a", column="first"),
                                       ColumnOrigin(joint="src_b", column="last")],
                              expression="CONCAT(first, last)"),
            ],
        )

        grp_a = _make_group(["src_a"], engine_type="databricks")
        grp_b = _make_group(["src_b"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src_a, src_b, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_a, grp_b, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        updated_a = next(g for g in new_groups if "src_a" in g.joints)
        updated_b = next(g for g in new_groups if "src_b" in g.joints)
        assert not updated_a.per_joint_predicates
        assert not updated_b.per_joint_predicates
        assert any(r.status == "skipped" for r in results)


# ===================================================================
# 7.12 No lineage column not pushed
# ===================================================================

class TestNoLineageNotPushed:
    """7.12: Predicate on a column with no ColumnLineage — not pushed."""

    def test_no_lineage_skipped(self):
        src = _make_cj("src", column_lineage=[_source_lineage("id")])

        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="mystery_col = 42",
                                  columns=["mystery_col"], location="where")],
        )
        # No lineage for mystery_col
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("id", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert not updated_src.per_joint_predicates
        assert any(r.status == "skipped" for r in results)


# ===================================================================
# 7.13 No-op — consumer with no predicates
# ===================================================================

class TestNoOpNoPredicates:
    """7.13: Exit joint has no WHERE clause — pass is a no-op."""

    def test_no_predicates_no_changes(self):
        src = _make_cj("src", column_lineage=[_source_lineage("id")])

        consumer_plan = LogicalPlan(
            projections=[], predicates=[], joins=[], aggregations=None,
            limit=None, ordering=None, distinct=False, source_tables=[],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("id", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        for g in new_groups:
            assert not g.per_joint_predicates
        assert len(results) == 0


# ===================================================================
# 7.14 No-op — consumer with no logical plan
# ===================================================================

class TestNoOpNoLogicalPlan:
    """7.14: Exit joint has logical_plan = None — pass is a no-op."""

    def test_no_logical_plan_no_changes(self):
        src = _make_cj("src", column_lineage=[_source_lineage("id")])
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src"],
            engine="eng2", engine_type="polars", logical_plan=None,
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        for g in new_groups:
            assert not g.per_joint_predicates
        assert len(results) == 0


# ===================================================================
# 7.15 Source without predicate_pushdown capability
# ===================================================================

class TestSourceWithoutCapability:
    """7.15: Source adapter lacks predicate_pushdown — not pushed, not_applicable."""

    def test_incapable_adapter_not_applicable(self):
        src = _make_cj("src", column_lineage=[_source_lineage("col")])

        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="col = 'x'",
                                  columns=["col"], location="where")],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _INCAPABLE, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert not updated_src.per_joint_predicates
        not_applicable = [
            r for r in results
            if r.status == "not_applicable"
            and r.rule == "cross_group_predicate_pushdown"
        ]
        assert len(not_applicable) == 1
        assert "predicate_pushdown" in not_applicable[0].detail


# ===================================================================
# 7.16 Existing intra-group predicates preserved
# ===================================================================

class TestExistingIntraGroupPreserved:
    """7.16: Source group already has pushed predicates — cross-group appended."""

    def test_existing_predicates_preserved(self):
        existing_pred = Predicate(expression="intra_col = 1", columns=["intra_col"], location="where")
        src = _make_cj("src", column_lineage=[_source_lineage("col"), _source_lineage("intra_col")])

        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="col = 'x'",
                                  columns=["col"], location="where")],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col", "src")],
        )

        # Source group already has per_joint_predicates from a previous pass
        grp_src = _make_group(["src"], engine_type="databricks",
                              per_joint_predicates={"src": [existing_pred]})
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        pushed = updated_src.per_joint_predicates["src"]
        # Both existing and new predicates should be present
        assert len(pushed) == 2
        exprs = [p.expression for p in pushed]
        assert "intra_col = 1" in exprs
        assert any("col" in e and "'x'" in e for e in exprs)


# ===================================================================
# 7.17 DISTINCT does not block pushdown
# ===================================================================

class TestDistinctDoesNotBlock:
    """7.17: Consumer with DISTINCT and eligible predicates — pushdown occurs."""

    def test_distinct_allows_pushdown(self):
        src = _make_cj("src", column_lineage=[_source_lineage("col")])

        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=None, distinct=True, source_tables=[],
            predicates=[Predicate(expression="col = 'x'",
                                  columns=["col"], location="where")],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert "src" in updated_src.per_joint_predicates
        assert any(r.status == "applied" for r in results)


# ===================================================================
# 7.18 ORDER BY does not block pushdown
# ===================================================================

class TestOrderByDoesNotBlock:
    """7.18: Consumer with ORDER BY and eligible predicates — pushdown occurs."""

    def test_orderby_allows_pushdown(self):
        src = _make_cj("src", column_lineage=[_source_lineage("col")])

        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=Ordering(columns=[("col", "asc")]),
            distinct=False, source_tables=[],
            predicates=[Predicate(expression="col = 'x'",
                                  columns=["col"], location="where")],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert "src" in updated_src.per_joint_predicates
        assert any(r.status == "applied" for r in results)


# ===================================================================
# 7.19 DISTINCT + ORDER BY combined
# ===================================================================

class TestDistinctAndOrderByCombined:
    """7.19: Consumer with both DISTINCT and ORDER BY — pushdown still occurs."""

    def test_distinct_orderby_allows_pushdown(self):
        src = _make_cj("src", column_lineage=[_source_lineage("col")])

        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=Ordering(columns=[("col", "desc")]),
            distinct=True, source_tables=[],
            predicates=[Predicate(expression="col = 'x'",
                                  columns=["col"], location="where")],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert "src" in updated_src.per_joint_predicates
        assert any(r.status == "applied" for r in results)


# ===================================================================
# 7.20 Basic INNER JOIN equality propagation
# ===================================================================

class TestInnerJoinEqualityPropagation:
    """7.20: INNER JOIN ON a.id = b.id WHERE a.id = 'value' — pushed to BOTH."""

    def test_predicate_pushed_to_both_sources(self):
        _src_a = _make_cj("src_a", column_lineage=[_source_lineage("id")])
        _src_b = _make_cj("src_b", column_lineage=[_source_lineage("id")])

        consumer_plan = LogicalPlan(
            projections=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="id = 'value'",
                                  columns=["id"], location="where")],
            joins=[Join(type="inner", left_table="src_a", right_table="src_b",
                        condition="a.id = b.id", columns=["id"])],
        )
        _consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src_a", "src_b"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[
                _direct_lineage("id", "src_a"),
                # b.id also traces to src_b
                ColumnLineage(output_column="id", transform="direct",
                              origins=[ColumnOrigin(joint="src_a", column="id")],
                              expression=None),
            ],
        )
        # We need lineage for the "other side" column (b.id -> src_b.id)
        # The join condition references b.id which needs lineage on the consumer
        # Let's set up lineage so that "id" traces to src_a (the predicate column)
        # and the join equality "a.id = b.id" means b.id also needs lineage
        # The _derive function creates a derived pred with column "id" (from b.id)
        # and then resolves it through lineage. We need a second lineage entry.
        # Since both sides use "id", we need to handle this carefully.
        # The join condition is "a.id = b.id" — the predicate col is "id" matching left side "a.id"
        # The other side is "b.id" with bare name "id"
        # The derived pred will have column "id" and try to resolve through lineage
        # Since the first lineage match for "id" points to src_a, the derived pred
        # would also resolve to src_a — which is the same source. So we need
        # different column names or aliases.

        # Let's use a more realistic setup with different column names on each side
        src_a2 = _make_cj("src_a", column_lineage=[_source_lineage("aid")])
        src_b2 = _make_cj("src_b", column_lineage=[_source_lineage("bid")])

        consumer_plan2 = LogicalPlan(
            projections=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="aid = 'value'",
                                  columns=["aid"], location="where")],
            joins=[Join(type="inner", left_table="src_a", right_table="src_b",
                        condition="aid = bid", columns=["aid", "bid"])],
        )
        consumer2 = _make_cj(
            "consumer", joint_type="sql", upstream=["src_a", "src_b"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan2,
            column_lineage=[
                _direct_lineage("aid", "src_a"),
                _direct_lineage("bid", "src_b"),
            ],
        )

        grp_a = _make_group(["src_a"], engine_type="databricks")
        grp_b = _make_group(["src_b"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src_a2, src_b2, consumer2]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_a, grp_b, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        updated_a = next(g for g in new_groups if "src_a" in g.joints)
        updated_b = next(g for g in new_groups if "src_b" in g.joints)

        # Direct pushdown to src_a
        assert "src_a" in updated_a.per_joint_predicates
        pushed_a = updated_a.per_joint_predicates["src_a"]
        assert any("aid" in p.expression and "'value'" in p.expression for p in pushed_a)

        # Derived pushdown to src_b via join equality
        assert "src_b" in updated_b.per_joint_predicates
        pushed_b = updated_b.per_joint_predicates["src_b"]
        assert any("bid" in p.expression and "'value'" in p.expression for p in pushed_b)

        # Should have applied results for both
        applied = [r for r in results if r.status == "applied"]
        assert len(applied) >= 2


# ===================================================================
# 7.20b INNER JOIN same-column-name with table-qualified aliases
# ===================================================================

class TestInnerJoinSameColumnNameWithAliases:
    """JOIN where both sides share the same column name, disambiguated by table aliases.

    Reproduces the real-world scenario:
        FROM raw_ingestion_status ris
        INNER JOIN raw_ingestion_events rie ON ris.correlation_id = rie.correlation_id
        WHERE ris.correlation_id = '0000498e-...'

    The predicate should be pushed to BOTH sources via join-equality inference.
    """

    def test_predicate_pushed_to_both_sources_same_column_name(self):
        from rivet_core.sql_parser import TableReference

        src_status = _make_cj(
            "raw_ingestion_status",
            column_lineage=[_source_lineage("correlation_id")],
        )
        src_events = _make_cj(
            "raw_ingestion_events",
            column_lineage=[_source_lineage("correlation_id")],
        )

        consumer_plan = LogicalPlan(
            projections=[],
            aggregations=None,
            limit=None,
            ordering=None,
            distinct=False,
            source_tables=[
                TableReference(
                    name="raw_ingestion_status", schema=None,
                    alias="ris", source_type="from",
                ),
                TableReference(
                    name="raw_ingestion_events", schema=None,
                    alias="rie", source_type="join",
                ),
            ],
            predicates=[
                Predicate(
                    expression="ris.correlation_id = '0000498e'",
                    columns=["ris.correlation_id"],
                    location="where",
                ),
            ],
            joins=[
                Join(
                    type="inner",
                    left_table="ris",
                    right_table="rie",
                    condition="rie.correlation_id = ris.correlation_id",
                    columns=["rie.correlation_id", "ris.correlation_id"],
                ),
            ],
        )
        consumer = _make_cj(
            "transform", joint_type="sql",
            upstream=["raw_ingestion_status", "raw_ingestion_events"],
            engine="eng2", engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[
                ColumnLineage(
                    output_column="correlation_id",
                    transform="direct",
                    origins=[
                        ColumnOrigin(joint="raw_ingestion_status", column="correlation_id"),
                    ],
                    expression=None,
                ),
            ],
        )

        grp_status = _make_group(["raw_ingestion_status"], engine_type="databricks")
        grp_events = _make_group(["raw_ingestion_events"], engine_type="databricks")
        grp_consumer = _make_group(["transform"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src_status, src_events, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_status, grp_events, grp_consumer],
            cj_map,
            _CAPABLE,
            _EMPTY_CATALOG_TYPES,
        )

        updated_status = next(
            g for g in new_groups if "raw_ingestion_status" in g.joints
        )
        updated_events = next(
            g for g in new_groups if "raw_ingestion_events" in g.joints
        )

        # Direct pushdown to raw_ingestion_status
        assert "raw_ingestion_status" in updated_status.per_joint_predicates
        pushed_status = updated_status.per_joint_predicates["raw_ingestion_status"]
        assert any(
            "correlation_id" in p.expression and "'0000498e'" in p.expression
            for p in pushed_status
        )

        # Derived pushdown to raw_ingestion_events via join equality
        assert "raw_ingestion_events" in updated_events.per_joint_predicates
        pushed_events = updated_events.per_joint_predicates["raw_ingestion_events"]
        assert any(
            "correlation_id" in p.expression and "'0000498e'" in p.expression
            for p in pushed_events
        )

        applied = [r for r in results if r.status == "applied"]
        assert len(applied) >= 2


# ===================================================================
# 7.21 INNER JOIN propagation with renamed columns
# ===================================================================

class TestInnerJoinRenamedColumns:
    """7.21: Source_a has user_id, source_b has uid. ON a.user_id = b.uid WHERE a.user_id = 42."""

    def test_derived_predicate_uses_source_column_name(self):
        src_a = _make_cj("src_a", column_lineage=[_source_lineage("user_id")])
        src_b = _make_cj("src_b", column_lineage=[_source_lineage("uid")])

        consumer_plan = LogicalPlan(
            projections=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="user_id = 42",
                                  columns=["user_id"], location="where")],
            joins=[Join(type="inner", left_table="src_a", right_table="src_b",
                        condition="user_id = user_uid", columns=["user_id", "user_uid"])],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src_a", "src_b"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[
                _direct_lineage("user_id", "src_a"),
                _renamed_lineage("user_uid", "src_b", "uid"),
            ],
        )

        grp_a = _make_group(["src_a"], engine_type="databricks")
        grp_b = _make_group(["src_b"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src_a, src_b, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_a, grp_b, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        updated_b = next(g for g in new_groups if "src_b" in g.joints)
        assert "src_b" in updated_b.per_joint_predicates
        pushed_b = updated_b.per_joint_predicates["src_b"]
        # The derived predicate should use the source column name "uid"
        assert any("uid" in p.expression for p in pushed_b)


# ===================================================================
# 7.22 Multiple INNER JOIN equalities on same column
# ===================================================================

class TestMultipleInnerJoinEqualities:
    """7.22: ON a.id = b.id AND a.id = c.id WHERE a.id = 'value' — derived to b AND c."""

    def test_derived_predicates_to_multiple_sources(self):
        src_a = _make_cj("src_a", column_lineage=[_source_lineage("aid")])
        src_b = _make_cj("src_b", column_lineage=[_source_lineage("bid")])
        src_c = _make_cj("src_c", column_lineage=[_source_lineage("cid")])

        consumer_plan = LogicalPlan(
            projections=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="aid = 'value'",
                                  columns=["aid"], location="where")],
            joins=[
                Join(type="inner", left_table="src_a", right_table="src_b",
                     condition="aid = bid", columns=["aid", "bid"]),
                Join(type="inner", left_table="src_a", right_table="src_c",
                     condition="aid = cid", columns=["aid", "cid"]),
            ],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src_a", "src_b", "src_c"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[
                _direct_lineage("aid", "src_a"),
                _direct_lineage("bid", "src_b"),
                _direct_lineage("cid", "src_c"),
            ],
        )

        grp_a = _make_group(["src_a"], engine_type="databricks")
        grp_b = _make_group(["src_b"], engine_type="databricks")
        grp_c = _make_group(["src_c"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src_a, src_b, src_c, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_a, grp_b, grp_c, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        updated_b = next(g for g in new_groups if "src_b" in g.joints)
        updated_c = next(g for g in new_groups if "src_c" in g.joints)

        assert "src_b" in updated_b.per_joint_predicates
        assert "src_c" in updated_c.per_joint_predicates

        pushed_b = updated_b.per_joint_predicates["src_b"]
        pushed_c = updated_c.per_joint_predicates["src_c"]

        assert any("bid" in p.expression and "'value'" in p.expression for p in pushed_b)
        assert any("cid" in p.expression and "'value'" in p.expression for p in pushed_c)


# ===================================================================
# 7.23 LEFT JOIN blocks join-equality propagation
# ===================================================================

class TestLeftJoinBlocksPropagation:
    """7.23: LEFT JOIN — predicate pushed to source_a only, NOT derived for source_b."""

    def test_left_join_no_derived_predicate(self):
        src_a = _make_cj("src_a", column_lineage=[_source_lineage("aid")])
        src_b = _make_cj("src_b", column_lineage=[_source_lineage("bid")])

        consumer_plan = LogicalPlan(
            projections=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="aid = 'value'",
                                  columns=["aid"], location="where")],
            joins=[Join(type="left", left_table="src_a", right_table="src_b",
                        condition="aid = bid", columns=["aid", "bid"])],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src_a", "src_b"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("aid", "src_a"),
                            _direct_lineage("bid", "src_b")],
        )

        grp_a = _make_group(["src_a"], engine_type="databricks")
        grp_b = _make_group(["src_b"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src_a, src_b, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_a, grp_b, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        updated_a = next(g for g in new_groups if "src_a" in g.joints)
        updated_b = next(g for g in new_groups if "src_b" in g.joints)

        # Direct pushdown to src_a
        assert "src_a" in updated_a.per_joint_predicates
        # No derived pushdown to src_b
        assert not updated_b.per_joint_predicates


# ===================================================================
# 7.24 RIGHT JOIN blocks join-equality propagation
# ===================================================================

class TestRightJoinBlocksPropagation:
    """7.24: RIGHT JOIN — no derived predicate for source_b."""

    def test_right_join_no_derived_predicate(self):
        src_a = _make_cj("src_a", column_lineage=[_source_lineage("aid")])
        src_b = _make_cj("src_b", column_lineage=[_source_lineage("bid")])

        consumer_plan = LogicalPlan(
            projections=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="aid = 'value'",
                                  columns=["aid"], location="where")],
            joins=[Join(type="right", left_table="src_a", right_table="src_b",
                        condition="aid = bid", columns=["aid", "bid"])],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src_a", "src_b"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("aid", "src_a"),
                            _direct_lineage("bid", "src_b")],
        )

        grp_a = _make_group(["src_a"], engine_type="databricks")
        grp_b = _make_group(["src_b"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src_a, src_b, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_a, grp_b, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        updated_b = next(g for g in new_groups if "src_b" in g.joints)
        assert not updated_b.per_joint_predicates


# ===================================================================
# 7.25 FULL OUTER JOIN blocks join-equality propagation
# ===================================================================

class TestFullOuterJoinBlocksPropagation:
    """7.25: FULL OUTER JOIN — no derived predicates."""

    def test_full_join_no_derived_predicate(self):
        src_a = _make_cj("src_a", column_lineage=[_source_lineage("aid")])
        src_b = _make_cj("src_b", column_lineage=[_source_lineage("bid")])

        consumer_plan = LogicalPlan(
            projections=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="aid = 'value'",
                                  columns=["aid"], location="where")],
            joins=[Join(type="full", left_table="src_a", right_table="src_b",
                        condition="aid = bid", columns=["aid", "bid"])],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src_a", "src_b"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("aid", "src_a"),
                            _direct_lineage("bid", "src_b")],
        )

        grp_a = _make_group(["src_a"], engine_type="databricks")
        grp_b = _make_group(["src_b"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src_a, src_b, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_a, grp_b, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        updated_b = next(g for g in new_groups if "src_b" in g.joints)
        assert not updated_b.per_joint_predicates


# ===================================================================
# 7.26 CROSS JOIN — no join condition, no derivation
# ===================================================================

class TestCrossJoinNoDerived:
    """7.26: CROSS JOIN with no ON condition — no derived predicates."""

    def test_cross_join_no_derived(self):
        src_a = _make_cj("src_a", column_lineage=[_source_lineage("aid")])
        src_b = _make_cj("src_b", column_lineage=[_source_lineage("bid")])

        consumer_plan = LogicalPlan(
            projections=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="aid = 'value'",
                                  columns=["aid"], location="where")],
            joins=[Join(type="cross", left_table="src_a", right_table="src_b",
                        condition=None, columns=[])],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src_a", "src_b"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("aid", "src_a"),
                            _direct_lineage("bid", "src_b")],
        )

        grp_a = _make_group(["src_a"], engine_type="databricks")
        grp_b = _make_group(["src_b"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src_a, src_b, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_a, grp_b, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        # Direct pushdown to src_a should still work
        updated_a = next(g for g in new_groups if "src_a" in g.joints)
        assert "src_a" in updated_a.per_joint_predicates

        # No derived pushdown to src_b
        updated_b = next(g for g in new_groups if "src_b" in g.joints)
        assert not updated_b.per_joint_predicates


# ===================================================================
# 7.27 Expression-based join condition blocks propagation
# ===================================================================

class TestExpressionJoinBlocksPropagation:
    """7.27: ON UPPER(a.name) = b.name WHERE a.name = 'Alice' — no derived predicate."""

    def test_expression_join_no_derived(self):
        src_a = _make_cj("src_a", column_lineage=[_source_lineage("name")])
        src_b = _make_cj("src_b", column_lineage=[_source_lineage("bname")])

        consumer_plan = LogicalPlan(
            projections=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="name = 'Alice'",
                                  columns=["name"], location="where")],
            joins=[Join(type="inner", left_table="src_a", right_table="src_b",
                        condition="UPPER(a.name) = b.bname", columns=["name", "bname"])],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src_a", "src_b"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("name", "src_a"),
                            _direct_lineage("bname", "src_b")],
        )

        grp_a = _make_group(["src_a"], engine_type="databricks")
        grp_b = _make_group(["src_b"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src_a, src_b, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_a, grp_b, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        # Direct pushdown to src_a
        updated_a = next(g for g in new_groups if "src_a" in g.joints)
        assert "src_a" in updated_a.per_joint_predicates

        # No derived pushdown to src_b (expression-based condition)
        updated_b = next(g for g in new_groups if "src_b" in g.joints)
        assert not updated_b.per_joint_predicates


# ===================================================================
# 7.28 Mixed join types — INNER and LEFT on same consumer
# ===================================================================

class TestMixedJoinTypes:
    """7.28: INNER JOIN src_b, LEFT JOIN src_c — derived to src_b only."""

    def test_inner_derived_left_blocked(self):
        src_a = _make_cj("src_a", column_lineage=[_source_lineage("aid")])
        src_b = _make_cj("src_b", column_lineage=[_source_lineage("bid")])
        src_c = _make_cj("src_c", column_lineage=[_source_lineage("cid")])

        consumer_plan = LogicalPlan(
            projections=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="aid = 'value'",
                                  columns=["aid"], location="where")],
            joins=[
                Join(type="inner", left_table="src_a", right_table="src_b",
                     condition="aid = bid", columns=["aid", "bid"]),
                Join(type="left", left_table="src_a", right_table="src_c",
                     condition="aid = cid", columns=["aid", "cid"]),
            ],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src_a", "src_b", "src_c"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[
                _direct_lineage("aid", "src_a"),
                _direct_lineage("bid", "src_b"),
                _direct_lineage("cid", "src_c"),
            ],
        )

        grp_a = _make_group(["src_a"], engine_type="databricks")
        grp_b = _make_group(["src_b"], engine_type="databricks")
        grp_c = _make_group(["src_c"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src_a, src_b, src_c, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_a, grp_b, grp_c, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        updated_b = next(g for g in new_groups if "src_b" in g.joints)
        updated_c = next(g for g in new_groups if "src_c" in g.joints)

        # Derived to src_b (INNER)
        assert "src_b" in updated_b.per_joint_predicates
        # NOT derived to src_c (LEFT)
        assert not updated_c.per_joint_predicates


# ===================================================================
# 7.29 Derived predicate target lacks capability
# ===================================================================

class TestDerivedPredicateTargetLacksCapability:
    """7.29: INNER JOIN propagation where source_b lacks predicate_pushdown."""

    def test_derived_not_pushed_to_incapable(self):
        src_a = _make_cj("src_a", engine_type="databricks",
                         column_lineage=[_source_lineage("aid")])
        src_b = _make_cj("src_b", engine_type="polars",
                         column_lineage=[_source_lineage("bid")])

        consumer_plan = LogicalPlan(
            projections=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="aid = 'value'",
                                  columns=["aid"], location="where")],
            joins=[Join(type="inner", left_table="src_a", right_table="src_b",
                        condition="aid = bid", columns=["aid", "bid"])],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src_a", "src_b"],
            engine="eng3", engine_type="duckdb", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("aid", "src_a"),
                            _direct_lineage("bid", "src_b")],
        )

        grp_a = _make_group(["src_a"], engine_type="databricks")
        grp_b = _make_group(["src_b"], engine="eng2", engine_type="polars")
        grp_con = _make_group(["consumer"], engine="eng3", engine_type="duckdb")

        # Only databricks has capability, polars does not
        caps = {"databricks": ["predicate_pushdown"], "polars": []}
        cj_map = {c.name: c for c in [src_a, src_b, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_a, grp_b, grp_con], cj_map, caps, _EMPTY_CATALOG_TYPES,
        )

        # Direct pushdown to src_a should work
        updated_a = next(g for g in new_groups if "src_a" in g.joints)
        assert "src_a" in updated_a.per_joint_predicates

        # Derived predicate NOT pushed to src_b (incapable)
        updated_b = next(g for g in new_groups if "src_b" in g.joints)
        assert not updated_b.per_joint_predicates


# ===================================================================
# 7.30 _merge_cross_group_predicates with existing pushdown
# ===================================================================

class TestMergeCrossGroupWithExisting:
    """7.30: Cross-group predicates appended to existing pushed list."""

    def test_merge_appends_to_existing(self):
        existing_pred = Predicate(expression="intra = 1", columns=["intra"], location="where")
        xg_pred = Predicate(expression="cross = 2", columns=["cross"], location="where")

        pushdown = _make_pushdown_plan(pushed=[existing_pred])
        group = _make_group(["src"], per_joint_predicates={"src": [xg_pred]})

        result = _merge_cross_group_predicates(pushdown, group, "src")

        assert result is not None
        assert len(result.predicates.pushed) == 2
        exprs = [p.expression for p in result.predicates.pushed]
        assert "intra = 1" in exprs
        assert "cross = 2" in exprs


# ===================================================================
# 7.31 _merge_cross_group_predicates with None pushdown
# ===================================================================

class TestMergeCrossGroupWithNone:
    """7.31: None pushdown — new PushdownPlan created with cross-group predicates."""

    def test_merge_creates_new_plan(self):
        xg_pred = Predicate(expression="cross = 2", columns=["cross"], location="where")
        group = _make_group(["src"], per_joint_predicates={"src": [xg_pred]})

        result = _merge_cross_group_predicates(None, group, "src")

        assert result is not None
        assert len(result.predicates.pushed) == 1
        assert result.predicates.pushed[0].expression == "cross = 2"
        assert result.predicates.residual == []
        assert result.projections.pushed_columns is None
        assert result.limit.pushed_limit is None


# ===================================================================
# 7.32 _merge_cross_group_predicates no-op
# ===================================================================

class TestMergeCrossGroupNoOp:
    """7.32: No per-joint predicates for the joint — pushdown returned unchanged."""

    def test_merge_noop_returns_unchanged(self):
        pushdown = _make_pushdown_plan(pushed=[
            Predicate(expression="x = 1", columns=["x"], location="where"),
        ])
        group = _make_group(["src"])  # no per_joint_predicates

        result = _merge_cross_group_predicates(pushdown, group, "src")
        assert result is pushdown  # same object

    def test_merge_noop_none_returns_none(self):
        group = _make_group(["src"])
        result = _merge_cross_group_predicates(None, group, "src")
        assert result is None


# ===================================================================
# 7.33 OptimizationResult — applied status recorded
# ===================================================================

class TestOptimizationResultApplied:
    """7.33: OptimizationResult with status 'applied' for each pushed predicate."""

    def test_applied_result_recorded(self):
        src = _make_cj("src", column_lineage=[_source_lineage("col")])

        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="col = 'x'",
                                  columns=["col"], location="where")],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        _, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        applied = [r for r in results if r.status == "applied"]
        assert len(applied) == 1
        assert applied[0].rule == "cross_group_predicate_pushdown"
        assert "col" in applied[0].detail
        assert applied[0].pushed is not None


# ===================================================================
# 7.34 OptimizationResult — skipped status recorded
# ===================================================================

class TestOptimizationResultSkipped:
    """7.34: OptimizationResult with status 'skipped' for non-pushable predicates."""

    def test_skipped_result_for_having(self):
        src = _make_cj("src", column_lineage=[_source_lineage("cnt")])

        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="cnt > 5",
                                  columns=["cnt"], location="having")],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("cnt", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        _, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        skipped = [r for r in results if r.status == "skipped"]
        assert len(skipped) == 1
        assert skipped[0].rule == "cross_group_predicate_pushdown"
        assert "non-pushable" in skipped[0].detail.lower() or "HAVING" in skipped[0].detail

    def test_skipped_result_for_multi_source(self):
        src_a = _make_cj("src_a", column_lineage=[_source_lineage("col1")])
        src_b = _make_cj("src_b", column_lineage=[_source_lineage("col2")])

        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="col1 = col2",
                                  columns=["col1", "col2"], location="where")],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src_a", "src_b"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col1", "src_a"),
                            _direct_lineage("col2", "src_b")],
        )

        grp_a = _make_group(["src_a"], engine_type="databricks")
        grp_b = _make_group(["src_b"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src_a, src_b, consumer]}
        _, results = cross_group_pushdown_pass(
            [grp_a, grp_b, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        skipped = [r for r in results if r.status == "skipped"]
        assert len(skipped) == 1
        assert "multiple" in skipped[0].detail.lower()


# ===================================================================
# 7.35 OptimizationResult — not_applicable status recorded
# ===================================================================

class TestOptimizationResultNotApplicable:
    """7.35: OptimizationResult with status 'not_applicable' for incapable adapters."""

    def test_not_applicable_result_recorded(self):
        src = _make_cj("src", column_lineage=[_source_lineage("col")])

        consumer_plan = LogicalPlan(
            projections=[], joins=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="col = 'x'",
                                  columns=["col"], location="where")],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        _, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _INCAPABLE, _EMPTY_CATALOG_TYPES,
        )

        na = [
            r for r in results
            if r.status == "not_applicable"
            and r.rule == "cross_group_predicate_pushdown"
        ]
        assert len(na) == 1
        assert na[0].rule == "cross_group_predicate_pushdown"
        assert "predicate_pushdown" in na[0].detail


# ===================================================================
# 7.36 OptimizationResult — join-equality derived applied
# ===================================================================

class TestOptimizationResultJoinEqualityDerived:
    """7.36: OptimizationResult with status 'applied' and join-equality detail."""

    def test_join_equality_derived_result(self):
        src_a = _make_cj("src_a", column_lineage=[_source_lineage("aid")])
        src_b = _make_cj("src_b", column_lineage=[_source_lineage("bid")])

        consumer_plan = LogicalPlan(
            projections=[], aggregations=None, limit=None,
            ordering=None, distinct=False, source_tables=[],
            predicates=[Predicate(expression="aid = 'value'",
                                  columns=["aid"], location="where")],
            joins=[Join(type="inner", left_table="src_a", right_table="src_b",
                        condition="aid = bid", columns=["aid", "bid"])],
        )
        consumer = _make_cj(
            "consumer", joint_type="sql", upstream=["src_a", "src_b"],
            engine="eng2", engine_type="polars", logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("aid", "src_a"),
                            _direct_lineage("bid", "src_b")],
        )

        grp_a = _make_group(["src_a"], engine_type="databricks")
        grp_b = _make_group(["src_b"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src_a, src_b, consumer]}
        _, results = cross_group_pushdown_pass(
            [grp_a, grp_b, grp_con], cj_map, _CAPABLE, _EMPTY_CATALOG_TYPES,
        )

        applied = [r for r in results if r.status == "applied"]
        # Should have at least 2: one direct, one derived
        assert len(applied) >= 2

        # Find the join-equality derived result
        derived_results = [r for r in applied if "join-equality" in r.detail.lower()
                           or "join equality" in r.detail.lower()]
        assert len(derived_results) >= 1
        assert derived_results[0].rule == "cross_group_predicate_pushdown"
        assert derived_results[0].pushed is not None
