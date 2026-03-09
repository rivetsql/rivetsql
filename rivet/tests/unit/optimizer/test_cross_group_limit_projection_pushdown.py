"""Unit tests for cross-group projection and limit pushdown in the optimizer.

Exhaustive examples covering projection pushdown scenarios (task 10),
limit pushdown scenarios (task 11), executor merge scenarios (task 12),
and observability scenarios (task 13).
"""

from __future__ import annotations

import uuid

from rivet_core.compiler import CompiledJoint
from rivet_core.lineage import ColumnLineage, ColumnOrigin
from rivet_core.optimizer import (
    FusedGroup,
    ResidualPlan,
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
# Helpers
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

_FULL_CAPS = {"databricks": ["predicate_pushdown", "projection_pushdown", "limit_pushdown"]}
_NO_PROJ_CAPS = {"databricks": ["predicate_pushdown", "limit_pushdown"]}
_EMPTY_CATALOG_TYPES: dict[str, str | None] = {}


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


# ===================================================================
# 10.1 Motivating example — Polars SQL SELECT correlation_id, status
# ===================================================================


class TestProjectionMotivatingExample:
    """10.1: Polars SQL SELECT correlation_id, status FROM databricks_source.

    Verify per_joint_projections contains ["correlation_id", "status"] on the
    Databricks source group.
    Requirements: 1.1, 2.1, 3.1
    """

    def test_projection_pushed_to_source(self):
        src = _make_cj(
            "db_src",
            engine_type="databricks",
            column_lineage=[
                _source_lineage("correlation_id"),
                _source_lineage("status"),
                _source_lineage("extra_col"),
            ],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="correlation_id", alias=None, source_columns=["correlation_id"]),
                Projection(expression="status", alias=None, source_columns=["status"]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=None,
            ordering=None,
            distinct=False,
            source_tables=["db_src"],
        )
        consumer = _make_cj(
            "polars_consumer",
            joint_type="sql",
            upstream=["db_src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[
                _direct_lineage("correlation_id", "db_src"),
                _direct_lineage("status", "db_src"),
            ],
        )

        grp_src = _make_group(["db_src"], engine_type="databricks")
        grp_con = _make_group(
            ["polars_consumer"],
            engine="eng2",
            engine_type="polars",
            entry_joints=["polars_consumer"],
            exit_joints=["polars_consumer"],
        )

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "db_src" in g.joints)
        assert "db_src" in updated_src.per_joint_projections
        assert updated_src.per_joint_projections["db_src"] == ["correlation_id", "status"]


# ===================================================================
# 10.2 Renamed column projection
# ===================================================================


class TestProjectionRenamedColumn:
    """10.2: Source has corr_id, consumer has correlation_id via renamed lineage.

    Verify per_joint_projections contains corr_id.
    Requirements: 2.1
    """

    def test_renamed_column_mapped_to_source_name(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[_source_lineage("corr_id")],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="correlation_id", alias=None, source_columns=["correlation_id"]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=None,
            ordering=None,
            distinct=False,
            source_tables=["src"],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[_renamed_lineage("correlation_id", "src", "corr_id")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert "src" in updated_src.per_joint_projections
        assert updated_src.per_joint_projections["src"] == ["corr_id"]


# ===================================================================
# 10.3 Expression column includes all contributing source columns
# ===================================================================


class TestProjectionExpressionColumn:
    """10.3: Consumer has CONCAT(a.first, a.last) tracing to first and last on source.

    Verify both columns in projection list.
    Requirements: 2.4
    """

    def test_expression_includes_all_source_columns(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[
                _source_lineage("first"),
                _source_lineage("last"),
            ],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(
                    expression="CONCAT(first, last)",
                    alias="full_name",
                    source_columns=["full_name"],
                ),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=None,
            ordering=None,
            distinct=False,
            source_tables=["src"],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[
                ColumnLineage(
                    output_column="full_name",
                    transform="expression",
                    origins=[
                        ColumnOrigin(joint="src", column="first"),
                        ColumnOrigin(joint="src", column="last"),
                    ],
                    expression="CONCAT(first, last)",
                ),
            ],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert "src" in updated_src.per_joint_projections
        assert updated_src.per_joint_projections["src"] == ["first", "last"]


# ===================================================================
# 10.4 Multi-source join projection — each source gets its own list
# ===================================================================


class TestProjectionMultiSourceJoin:
    """10.4: Consumer joins source_a and source_b, referencing columns from both.

    Verify each source gets its own projection list.
    Requirements: 2.2
    """

    def test_each_source_gets_own_projection_list(self):
        src_a = _make_cj(
            "src_a",
            engine_type="databricks",
            column_lineage=[
                _source_lineage("id"),
                _source_lineage("name"),
            ],
        )
        src_b = _make_cj(
            "src_b",
            engine_type="databricks",
            column_lineage=[
                _source_lineage("amount"),
                _source_lineage("currency"),
            ],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="id", alias=None, source_columns=["id"]),
                Projection(expression="amount", alias=None, source_columns=["amount"]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=None,
            ordering=None,
            distinct=False,
            source_tables=["src_a", "src_b"],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src_a", "src_b"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[
                _direct_lineage("id", "src_a"),
                _direct_lineage("amount", "src_b"),
            ],
        )

        grp_a = _make_group(["src_a"], engine_type="databricks")
        grp_b = _make_group(["src_b"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src_a, src_b, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_a, grp_b, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_a = next(g for g in new_groups if "src_a" in g.joints)
        updated_b = next(g for g in new_groups if "src_b" in g.joints)

        assert updated_a.per_joint_projections["src_a"] == ["id"]
        assert updated_b.per_joint_projections["src_b"] == ["amount"]


# ===================================================================
# 10.5 SELECT * skips projection pushdown
# ===================================================================


class TestProjectionSelectStarSkips:
    """10.5: Consumer with SELECT *.

    Verify no per_joint_projections set and skipped OptimizationResult recorded.
    Requirements: 1.2, 9.3
    """

    def test_select_star_skips_projection(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[_source_lineage("col1")],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="*", alias=None, source_columns=[]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=None,
            ordering=None,
            distinct=False,
            source_tables=["src"],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col1", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert not updated_src.per_joint_projections

        proj_results = [r for r in results if r.rule == "cross_group_projection_pushdown"]
        assert any(r.status == "skipped" for r in proj_results)


# ===================================================================
# 10.6 No logical plan skips projection pushdown
# ===================================================================


class TestProjectionNoLogicalPlanSkips:
    """10.6: Consumer with logical_plan = None.

    Verify no per_joint_projections set.
    Requirements: 1.3
    """

    def test_no_logical_plan_skips_projection(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[_source_lineage("col1")],
        )

        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=None,
            column_lineage=[_direct_lineage("col1", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert not updated_src.per_joint_projections


# ===================================================================
# 10.7 No lineage column skips projection pushdown for that consumer
# ===================================================================


class TestProjectionNoLineageSkips:
    """10.7: Consumer references a column with no lineage.

    Verify projection pushdown skipped for that consumer group.
    Requirements: 2.3
    """

    def test_no_lineage_skips_projection(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[_source_lineage("col1")],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="col1", alias=None, source_columns=["col1"]),
                Projection(expression="unknown_col", alias=None, source_columns=["unknown_col"]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=None,
            ordering=None,
            distinct=False,
            source_tables=["src"],
        )
        # Consumer has lineage for col1 but NOT for unknown_col
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col1", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert not updated_src.per_joint_projections


# ===================================================================
# 10.8 Adapter lacks projection_pushdown capability
# ===================================================================


class TestProjectionAdapterLacksCapability:
    """10.8: Source adapter without projection_pushdown capability.

    Verify not pushed and not_applicable OptimizationResult recorded.
    Requirements: 3.2, 9.5
    """

    def test_incapable_adapter_not_applicable(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[_source_lineage("col1")],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="col1", alias=None, source_columns=["col1"]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=None,
            ordering=None,
            distinct=False,
            source_tables=["src"],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col1", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _NO_PROJ_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert not updated_src.per_joint_projections

        proj_results = [r for r in results if r.rule == "cross_group_projection_pushdown"]
        assert any(r.status == "not_applicable" for r in proj_results)


# ===================================================================
# 10.9 Multiple consumers same source — union of projection columns
# ===================================================================


class TestProjectionMultiConsumerUnion:
    """10.9: Two consumers referencing same source with different columns.

    Verify union of column lists in per_joint_projections.
    Requirements: 3.4
    """

    def test_union_of_projection_columns(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[
                _source_lineage("col_a"),
                _source_lineage("col_b"),
                _source_lineage("col_c"),
            ],
        )

        consumer1_plan = LogicalPlan(
            projections=[
                Projection(expression="col_a", alias=None, source_columns=["col_a"]),
                Projection(expression="col_b", alias=None, source_columns=["col_b"]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=None,
            ordering=None,
            distinct=False,
            source_tables=["src"],
        )
        consumer1 = _make_cj(
            "consumer1",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer1_plan,
            column_lineage=[
                _direct_lineage("col_a", "src"),
                _direct_lineage("col_b", "src"),
            ],
        )

        consumer2_plan = LogicalPlan(
            projections=[
                Projection(expression="col_b", alias=None, source_columns=["col_b"]),
                Projection(expression="col_c", alias=None, source_columns=["col_c"]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=None,
            ordering=None,
            distinct=False,
            source_tables=["src"],
        )
        consumer2 = _make_cj(
            "consumer2",
            joint_type="sql",
            upstream=["src"],
            engine="eng3",
            engine_type="polars",
            logical_plan=consumer2_plan,
            column_lineage=[
                _direct_lineage("col_b", "src"),
                _direct_lineage("col_c", "src"),
            ],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con1 = _make_group(["consumer1"], engine="eng2", engine_type="polars")
        grp_con2 = _make_group(["consumer2"], engine="eng3", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer1, consumer2]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con1, grp_con2], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert "src" in updated_src.per_joint_projections
        # Union of {col_a, col_b} and {col_b, col_c} = {col_a, col_b, col_c}
        assert updated_src.per_joint_projections["src"] == ["col_a", "col_b", "col_c"]


# ===================================================================
# 10.10 Predicate columns included in projection list
# ===================================================================


class TestProjectionPredicateColumnsIncluded:
    """10.10: Consumer has SELECT col1 FROM src WHERE col2 = 'x' with cross-group
    predicate on col2.

    Verify per_joint_projections includes both col1 and col2.
    Requirements: 10.2
    """

    def test_predicate_columns_in_projection(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[
                _source_lineage("col1"),
                _source_lineage("col2"),
            ],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="col1", alias=None, source_columns=["col1"]),
            ],
            predicates=[
                Predicate(expression="col2 = 'x'", columns=["col2"], location="where"),
            ],
            joins=[],
            aggregations=None,
            limit=None,
            ordering=None,
            distinct=False,
            source_tables=[],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[
                _direct_lineage("col1", "src"),
                _direct_lineage("col2", "src"),
            ],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert "src" in updated_src.per_joint_projections
        # Both col1 (from SELECT) and col2 (from WHERE predicate) should be included
        assert updated_src.per_joint_projections["src"] == ["col1", "col2"]


# ===================================================================
# ===================================================================
# 10.11 Join-equality derived predicate columns included in projection
# ===================================================================


class TestProjectionJoinEqualityDerivedColumns:
    """10.11: Consumer joins src_a and src_b on sa.cid = sb.cid.

    A WHERE predicate on sa.cid pushes to src_a directly and derives a
    predicate on src_b.cid via join-equality propagation.  The projection for
    src_b must include 'cid' even though it only appears in the JOIN ON clause
    (not in SELECT).

    This is the bug scenario from the ingestion_sink pipeline where
    correlation_id was missing from raw_ingestion_events' projection.
    """

    def test_derived_predicate_columns_in_projection(self):
        from rivet_core.sql_parser import TableReference

        src_a = _make_cj(
            "src_a",
            engine_type="databricks",
            column_lineage=[
                _source_lineage("cid"),
                _source_lineage("status"),
            ],
        )
        src_b = _make_cj(
            "src_b",
            engine_type="databricks",
            column_lineage=[
                _source_lineage("cid"),
                _source_lineage("created_at"),
                _source_lineage("event_type"),
            ],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="sa.status", alias=None, source_columns=["sa.status"]),
                Projection(expression="sb.created_at", alias=None, source_columns=["sb.created_at"]),
                Projection(expression="sb.event_type", alias=None, source_columns=["sb.event_type"]),
            ],
            predicates=[
                Predicate(expression="sa.cid = 'abc'", columns=["sa.cid"], location="where"),
            ],
            joins=[
                Join(
                    type="inner",
                    left_table="sa",
                    right_table="sb",
                    condition="sa.cid = sb.cid",
                    columns=["sa.cid", "sb.cid"],
                ),
            ],
            aggregations=None,
            limit=None,
            ordering=None,
            distinct=False,
            source_tables=[
                TableReference(name="src_a", schema=None, alias="sa", source_type="from"),
                TableReference(name="src_b", schema=None, alias="sb", source_type="join"),
            ],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src_a", "src_b"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[
                _direct_lineage("sa.cid", "src_a"),
                _direct_lineage("sa.status", "src_a"),
                _direct_lineage("sb.cid", "src_b"),
                _direct_lineage("sb.created_at", "src_b"),
                _direct_lineage("sb.event_type", "src_b"),
            ],
        )

        grp_a = _make_group(["src_a"], engine_type="databricks")
        grp_b = _make_group(["src_b"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src_a, src_b, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_a, grp_b, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_b = next(g for g in new_groups if "src_b" in g.joints)

        # src_b must include 'cid' (from the derived predicate) alongside
        # 'created_at' and 'event_type' (from SELECT)
        assert "src_b" in updated_b.per_joint_projections
        proj_b = updated_b.per_joint_projections["src_b"]
        assert "cid" in proj_b, (
            f"Expected 'cid' in src_b projection (from join-equality derived "
            f"predicate), got {proj_b}"
        )
        assert any("created_at" in c for c in proj_b)
        assert any("event_type" in c for c in proj_b)


# ===================================================================
# Limit pushdown capability dict (no limit_pushdown)
# ===================================================================

_NO_LIMIT_CAPS = {"databricks": ["predicate_pushdown", "projection_pushdown"]}


# ===================================================================
# 11.1 Simple LIMIT pushdown
# ===================================================================


class TestLimitSimplePushdown:
    """11.1: Consumer with LIMIT 100 and no blocking constructs.

    Verify per_joint_limits contains 100.
    Requirements: 4.1, 5.1
    """

    def test_limit_pushed_to_source(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[_source_lineage("col1")],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="col1", alias=None, source_columns=["col1"]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=Limit(count=100, offset=None),
            ordering=None,
            distinct=False,
            source_tables=["src"],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col1", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert "src" in updated_src.per_joint_limits
        assert updated_src.per_joint_limits["src"] == 100


# ===================================================================
# 11.2 LIMIT with aggregation blocks
# ===================================================================


class TestLimitAggregationBlocks:
    """11.2: Consumer with LIMIT 100 and GROUP BY.

    Verify no per_joint_limits.
    Requirements: 4.2, 9.4
    """

    def test_aggregation_blocks_limit_pushdown(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[_source_lineage("col1")],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="col1", alias=None, source_columns=["col1"]),
            ],
            predicates=[],
            joins=[],
            aggregations=Aggregation(group_by=["col1"], functions=["COUNT(*)"]),
            limit=Limit(count=100, offset=None),
            ordering=None,
            distinct=False,
            source_tables=["src"],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col1", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert not updated_src.per_joint_limits


# ===================================================================
# 11.3 LIMIT with join blocks
# ===================================================================


class TestLimitJoinBlocks:
    """11.3: Consumer with LIMIT 100 and a JOIN.

    Verify no per_joint_limits.
    Requirements: 4.3, 9.4
    """

    def test_join_blocks_limit_pushdown(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[_source_lineage("col1")],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="col1", alias=None, source_columns=["col1"]),
            ],
            predicates=[],
            joins=[
                Join(
                    type="inner",
                    left_table="src",
                    right_table="other",
                    condition="src.id = other.id",
                    columns=["id"],
                ),
            ],
            aggregations=None,
            limit=Limit(count=100, offset=None),
            ordering=None,
            distinct=False,
            source_tables=["src", "other"],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col1", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert not updated_src.per_joint_limits


# ===================================================================
# 11.4 LIMIT with DISTINCT blocks
# ===================================================================


class TestLimitDistinctBlocks:
    """11.4: Consumer with LIMIT 100 and DISTINCT.

    Verify no per_joint_limits.
    Requirements: 4.4, 9.4
    """

    def test_distinct_blocks_limit_pushdown(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[_source_lineage("col1")],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="col1", alias=None, source_columns=["col1"]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=Limit(count=100, offset=None),
            ordering=None,
            distinct=True,
            source_tables=["src"],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col1", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert not updated_src.per_joint_limits


# ===================================================================
# 11.5 No LIMIT skips
# ===================================================================


class TestLimitNoLimitSkips:
    """11.5: Consumer with no LIMIT clause.

    Verify no per_joint_limits.
    Requirements: 4.5
    """

    def test_no_limit_skips(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[_source_lineage("col1")],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="col1", alias=None, source_columns=["col1"]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=None,
            ordering=None,
            distinct=False,
            source_tables=["src"],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col1", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert not updated_src.per_joint_limits


# ===================================================================
# 11.6 ORDER BY does not block limit pushdown
# ===================================================================


class TestLimitOrderByDoesNotBlock:
    """11.6: Consumer with ORDER BY col LIMIT 100.

    Verify per_joint_limits contains 100.
    Requirements: 7.1
    """

    def test_order_by_does_not_block_limit(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[_source_lineage("col1")],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="col1", alias=None, source_columns=["col1"]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=Limit(count=100, offset=None),
            ordering=Ordering(columns=[("col1", "asc")]),
            distinct=False,
            source_tables=["src"],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col1", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert "src" in updated_src.per_joint_limits
        assert updated_src.per_joint_limits["src"] == 100


# ===================================================================
# 11.7 Multiple upstream sources block limit pushdown
# ===================================================================


class TestLimitMultipleUpstreamSourcesBlock:
    """11.7: Consumer referencing two source groups.

    Verify no per_joint_limits.
    Requirements: 7.2
    """

    def test_multiple_upstream_sources_block_limit(self):
        src_a = _make_cj(
            "src_a",
            engine_type="databricks",
            column_lineage=[_source_lineage("col_a")],
        )
        src_b = _make_cj(
            "src_b",
            engine_type="databricks",
            column_lineage=[_source_lineage("col_b")],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="col_a", alias=None, source_columns=["col_a"]),
                Projection(expression="col_b", alias=None, source_columns=["col_b"]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=Limit(count=100, offset=None),
            ordering=None,
            distinct=False,
            source_tables=["src_a", "src_b"],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src_a", "src_b"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[
                _direct_lineage("col_a", "src_a"),
                _direct_lineage("col_b", "src_b"),
            ],
        )

        grp_a = _make_group(["src_a"], engine_type="databricks")
        grp_b = _make_group(["src_b"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src_a, src_b, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_a, grp_b, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_a = next(g for g in new_groups if "src_a" in g.joints)
        updated_b = next(g for g in new_groups if "src_b" in g.joints)
        assert not updated_a.per_joint_limits
        assert not updated_b.per_joint_limits


# ===================================================================
# 11.8 Residual predicates block limit pushdown
# ===================================================================


class TestLimitResidualPredicatesBlock:
    """11.8: Consumer with residual predicates.

    Verify no per_joint_limits.
    Requirements: 7.3
    """

    def test_residual_predicates_block_limit(self):
        from dataclasses import replace

        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[_source_lineage("col1")],
        )

        pred = Predicate(expression="col1 > 10", columns=["col1"], location="where")

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="col1", alias=None, source_columns=["col1"]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=Limit(count=100, offset=None),
            ordering=None,
            distinct=False,
            source_tables=["src"],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col1", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")
        grp_con = replace(grp_con, residual=ResidualPlan(predicates=[pred], limit=None, casts=[]))

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert not updated_src.per_joint_limits


# ===================================================================
# 11.9 Adapter lacks limit_pushdown capability
# ===================================================================


class TestLimitAdapterLacksCapability:
    """11.9: Source adapter without limit_pushdown capability.

    Verify not pushed and not_applicable OptimizationResult recorded.
    Requirements: 5.2, 9.5
    """

    def test_incapable_adapter_not_applicable(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[_source_lineage("col1")],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="col1", alias=None, source_columns=["col1"]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=Limit(count=100, offset=None),
            ordering=None,
            distinct=False,
            source_tables=["src"],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col1", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _NO_LIMIT_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert not updated_src.per_joint_limits

        limit_results = [r for r in results if r.rule == "cross_group_limit_pushdown"]
        assert any(r.status == "not_applicable" for r in limit_results)


# ===================================================================
# 11.10 Multiple consumers same source different limits — max wins
# ===================================================================


class TestLimitMultiConsumerMaxWins:
    """11.10: Two consumers with LIMIT 50 and LIMIT 200.

    Verify per_joint_limits contains 200 (max).
    Requirements: 5.4
    """

    def test_max_limit_wins(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[_source_lineage("col1")],
        )

        consumer1_plan = LogicalPlan(
            projections=[
                Projection(expression="col1", alias=None, source_columns=["col1"]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=Limit(count=50, offset=None),
            ordering=None,
            distinct=False,
            source_tables=["src"],
        )
        consumer1 = _make_cj(
            "consumer1",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer1_plan,
            column_lineage=[_direct_lineage("col1", "src")],
        )

        consumer2_plan = LogicalPlan(
            projections=[
                Projection(expression="col1", alias=None, source_columns=["col1"]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=Limit(count=200, offset=None),
            ordering=None,
            distinct=False,
            source_tables=["src"],
        )
        consumer2 = _make_cj(
            "consumer2",
            joint_type="sql",
            upstream=["src"],
            engine="eng3",
            engine_type="polars",
            logical_plan=consumer2_plan,
            column_lineage=[_direct_lineage("col1", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con1 = _make_group(["consumer1"], engine="eng2", engine_type="polars")
        grp_con2 = _make_group(["consumer2"], engine="eng3", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer1, consumer2]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con1, grp_con2], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert "src" in updated_src.per_joint_limits
        assert updated_src.per_joint_limits["src"] == 200


# ===================================================================
# 11.11 Consumer retains limit in residual plan
# ===================================================================


class TestLimitConsumerRetainsResidual:
    """11.11: After limit pushdown, verify consumer's ResidualPlan.limit still
    has the original value.

    Requirements: 5.5
    """

    def test_consumer_retains_limit_in_residual(self):
        from dataclasses import replace

        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[_source_lineage("col1")],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="col1", alias=None, source_columns=["col1"]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=Limit(count=100, offset=None),
            ordering=None,
            distinct=False,
            source_tables=["src"],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col1", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")
        grp_con = replace(grp_con, residual=ResidualPlan(predicates=[], limit=100, casts=[]))

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        # Verify the limit was pushed to the source
        updated_src = next(g for g in new_groups if "src" in g.joints)
        assert "src" in updated_src.per_joint_limits
        assert updated_src.per_joint_limits["src"] == 100

        # Verify the consumer still retains its limit in the residual plan
        updated_con = next(g for g in new_groups if "consumer" in g.joints)
        assert updated_con.residual is not None
        assert updated_con.residual.limit == 100


# ===================================================================
# 12. Executor merge and composition scenarios
# ===================================================================

from dataclasses import replace

from rivet_core.executor import (
    _merge_cross_group_limits,
    _merge_cross_group_projections,
    _merge_source_limit_into_pushdown,
)
from rivet_core.optimizer import (
    CastPushdownResult,
    LimitPushdownResult,
    PredicatePushdownResult,
    ProjectionPushdownResult,
    PushdownPlan,
)


def _make_pushdown(*, pushed_columns=None, pushed_limit=None, pushed_predicates=None):
    return PushdownPlan(
        predicates=PredicatePushdownResult(pushed=pushed_predicates or [], residual=[]),
        projections=ProjectionPushdownResult(pushed_columns=pushed_columns, reason=None),
        limit=LimitPushdownResult(pushed_limit=pushed_limit, residual_limit=None, reason=None),
        casts=CastPushdownResult(pushed=[], residual=[]),
    )


# ===================================================================
# 12.1 _merge_cross_group_projections with existing intra-group — intersection
# ===================================================================


class TestMergeProjectionsIntersection:
    """12.1: Intra-group has [a, b, c], cross-group has [b, c, d].
    Verify result is [b, c] (intersection).

    Requirements: 3.3, 6.3
    """

    def test_intersection_of_intra_and_cross_group(self):
        pushdown = _make_pushdown(pushed_columns=["a", "b", "c"])
        group = _make_group(
            ["src"],
            engine_type="databricks",
        )
        group = replace(group, per_joint_projections={"src": ["b", "c", "d"]})

        result = _merge_cross_group_projections(pushdown, group, "src")

        assert result is not None
        assert sorted(result.projections.pushed_columns) == ["b", "c"]


# ===================================================================
# 12.2 _merge_cross_group_projections with no intra-group projections
# ===================================================================


class TestMergeProjectionsNoIntraGroup:
    """12.2: Cross-group has [a, b]. No intra-group projections.
    Verify result is [a, b].

    Requirements: 6.1
    """

    def test_cross_group_used_directly(self):
        pushdown = _make_pushdown(pushed_columns=None)
        group = _make_group(
            ["src"],
            engine_type="databricks",
        )
        group = replace(group, per_joint_projections={"src": ["a", "b"]})

        result = _merge_cross_group_projections(pushdown, group, "src")

        assert result is not None
        assert sorted(result.projections.pushed_columns) == ["a", "b"]


# ===================================================================
# 12.3 _merge_cross_group_projections no-op
# ===================================================================


class TestMergeProjectionsNoOp:
    """12.3: No per_joint_projections for joint. Verify pushdown unchanged.

    Requirements: 6.1
    """

    def test_no_cross_group_projections_returns_unchanged(self):
        pushdown = _make_pushdown(pushed_columns=["x", "y"])
        group = _make_group(
            ["src"],
            engine_type="databricks",
        )
        # No per_joint_projections set (empty dict by default)

        result = _merge_cross_group_projections(pushdown, group, "src")

        assert result is pushdown  # exact same object, unchanged


# ===================================================================
# 12.4 _merge_cross_group_limits with existing limit — min wins
# ===================================================================


class TestMergeLimitsMinWins:
    """12.4: Existing limit 50, cross-group limit 100.
    Verify result is 50 (min).

    Requirements: 6.4
    """

    def test_min_of_existing_and_cross_group(self):
        pushdown = _make_pushdown(pushed_limit=50)
        group = _make_group(
            ["src"],
            engine_type="databricks",
        )
        group = replace(group, per_joint_limits={"src": 100})

        result = _merge_cross_group_limits(pushdown, group, "src")

        assert result is not None
        assert result.limit.pushed_limit == 50


# ===================================================================
# 12.5 _merge_cross_group_limits with no existing limit
# ===================================================================


class TestMergeLimitsNoExisting:
    """12.5: Cross-group limit 100, no existing limit.
    Verify result is 100.

    Requirements: 6.2
    """

    def test_cross_group_limit_used_directly(self):
        pushdown = _make_pushdown(pushed_limit=None)
        group = _make_group(
            ["src"],
            engine_type="databricks",
        )
        group = replace(group, per_joint_limits={"src": 100})

        result = _merge_cross_group_limits(pushdown, group, "src")

        assert result is not None
        assert result.limit.pushed_limit == 100


# ===================================================================
# 12.6 _merge_cross_group_limits no-op
# ===================================================================


class TestMergeLimitsNoOp:
    """12.6: No per_joint_limits for joint. Verify pushdown unchanged.

    Requirements: 6.2
    """

    def test_no_cross_group_limits_returns_unchanged(self):
        pushdown = _make_pushdown(pushed_limit=50)
        group = _make_group(
            ["src"],
            engine_type="databricks",
        )
        # No per_joint_limits set (empty dict by default)

        result = _merge_cross_group_limits(pushdown, group, "src")

        assert result is pushdown  # exact same object, unchanged


# ===================================================================
# 12.7 _merge_source_limit_into_pushdown + cross-group limit — min of both
# ===================================================================


class TestSourceLimitPlusCrossGroupLimit:
    """12.7: Source SQL has LIMIT 200, cross-group limit is 100.
    Verify effective limit is 100 (min of both).

    Requirements: 6.5
    """

    def test_min_of_source_and_cross_group_limit(self):
        # Source joint with LIMIT 200 in its SQL
        source_plan = LogicalPlan(
            projections=[],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=Limit(count=200, offset=None),
            ordering=None,
            distinct=False,
            source_tables=[],
        )
        src_cj = _make_cj("src", engine_type="databricks", logical_plan=source_plan)

        # Start with no pushdown, apply source limit merge first
        after_source = _merge_source_limit_into_pushdown(None, src_cj)
        assert after_source is not None
        assert after_source.limit.pushed_limit == 200

        # Now apply cross-group limit of 100
        group = _make_group(["src"], engine_type="databricks")
        group = replace(group, per_joint_limits={"src": 100})

        result = _merge_cross_group_limits(after_source, group, "src")

        assert result is not None
        assert result.limit.pushed_limit == 100  # min(200, 100) = 100


# ===================================================================
# 12.8 Predicates + projections + limit all pushed to same source
# ===================================================================


class TestAllThreePushedToSameSource:
    """12.8: Consumer with WHERE, specific columns, and LIMIT.
    Verify all three are pushed to the same source.

    Requirements: 10.1, 10.3
    """

    def test_predicates_projections_limit_all_pushed(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[
                _source_lineage("col1"),
                _source_lineage("col2"),
                _source_lineage("col3"),
            ],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="col1", alias=None, source_columns=["col1"]),
                Projection(expression="col2", alias=None, source_columns=["col2"]),
            ],
            predicates=[
                Predicate(expression="col3 = 'x'", columns=["col3"], location="where"),
            ],
            joins=[],
            aggregations=None,
            limit=Limit(count=50, offset=None),
            ordering=None,
            distinct=False,
            source_tables=[],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[
                _direct_lineage("col1", "src"),
                _direct_lineage("col2", "src"),
                _direct_lineage("col3", "src"),
            ],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)

        # Predicates pushed
        assert "src" in updated_src.per_joint_predicates
        assert len(updated_src.per_joint_predicates["src"]) > 0

        # Projections pushed (col1, col2 from SELECT + col3 from WHERE)
        assert "src" in updated_src.per_joint_projections
        assert sorted(updated_src.per_joint_projections["src"]) == ["col1", "col2", "col3"]

        # Limit pushed
        assert "src" in updated_src.per_joint_limits
        assert updated_src.per_joint_limits["src"] == 50


# ===================================================================
# 12.9 Predicates pushed but limit blocked by aggregation
# ===================================================================


class TestPredicatesPushedLimitBlockedByAggregation:
    """12.9: Consumer with WHERE and LIMIT but also has aggregation.
    Verify predicates pushed, limit not pushed.

    Requirements: 4.2, 10.1
    """

    def test_predicates_pushed_limit_blocked(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[
                _source_lineage("col1"),
                _source_lineage("col2"),
            ],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="col1", alias=None, source_columns=["col1"]),
            ],
            predicates=[
                Predicate(expression="col2 > 10", columns=["col2"], location="where"),
            ],
            joins=[],
            aggregations=Aggregation(group_by=["col1"], functions=["COUNT(*)"]),
            limit=Limit(count=100, offset=None),
            ordering=None,
            distinct=False,
            source_tables=[],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[
                _direct_lineage("col1", "src"),
                _direct_lineage("col2", "src"),
            ],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        new_groups, _ = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        updated_src = next(g for g in new_groups if "src" in g.joints)

        # Predicates should be pushed
        assert "src" in updated_src.per_joint_predicates
        assert len(updated_src.per_joint_predicates["src"]) > 0

        # Limit should NOT be pushed (blocked by aggregation)
        assert "src" not in updated_src.per_joint_limits


# ===================================================================
# 13. Observability scenarios
# ===================================================================


# ===================================================================
# 13.1 Applied projection OptimizationResult
# ===================================================================


class TestAppliedProjectionOptimizationResult:
    """13.1: Verify OptimizationResult with rule cross_group_projection_pushdown,
    status applied.

    Requirements: 9.1
    """

    def test_applied_projection_result(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[
                _source_lineage("col1"),
                _source_lineage("col2"),
            ],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="col1", alias=None, source_columns=["col1"]),
                Projection(expression="col2", alias=None, source_columns=["col2"]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=None,
            ordering=None,
            distinct=False,
            source_tables=["src"],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[
                _direct_lineage("col1", "src"),
                _direct_lineage("col2", "src"),
            ],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        _, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        proj_results = [r for r in results if r.rule == "cross_group_projection_pushdown"]
        assert len(proj_results) >= 1
        applied = [r for r in proj_results if r.status == "applied"]
        assert len(applied) >= 1
        # Detail should mention the target source joint
        assert any("src" in r.detail for r in applied)


# ===================================================================
# 13.2 Applied limit OptimizationResult
# ===================================================================


class TestAppliedLimitOptimizationResult:
    """13.2: Verify OptimizationResult with rule cross_group_limit_pushdown,
    status applied.

    Requirements: 9.2
    """

    def test_applied_limit_result(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[_source_lineage("col1")],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="col1", alias=None, source_columns=["col1"]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=Limit(count=50, offset=None),
            ordering=None,
            distinct=False,
            source_tables=["src"],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col1", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        _, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        limit_results = [r for r in results if r.rule == "cross_group_limit_pushdown"]
        assert len(limit_results) >= 1
        applied = [r for r in limit_results if r.status == "applied"]
        assert len(applied) >= 1
        # Detail should mention the target source joint
        assert any("src" in r.detail for r in applied)


# ===================================================================
# 13.3 Skipped projection (SELECT *) OptimizationResult
# ===================================================================


class TestSkippedProjectionSelectStarOptimizationResult:
    """13.3: Verify OptimizationResult with rule cross_group_projection_pushdown,
    status skipped and detail mentioning SELECT *.

    Requirements: 9.3
    """

    def test_skipped_projection_select_star_result(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[_source_lineage("col1")],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="*", alias=None, source_columns=[]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=None,
            ordering=None,
            distinct=False,
            source_tables=["src"],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col1", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        _, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        proj_results = [r for r in results if r.rule == "cross_group_projection_pushdown"]
        assert len(proj_results) >= 1
        skipped = [r for r in proj_results if r.status == "skipped"]
        assert len(skipped) >= 1
        # Detail should mention SELECT *
        assert any("SELECT *" in r.detail or "select *" in r.detail.lower() for r in skipped)


# ===================================================================
# 13.4 Skipped limit (aggregation) OptimizationResult
# ===================================================================


class TestSkippedLimitAggregationOptimizationResult:
    """13.4: Verify OptimizationResult with rule cross_group_limit_pushdown,
    status skipped and detail mentioning aggregation.

    Requirements: 9.4
    """

    def test_skipped_limit_aggregation_result(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[_source_lineage("col1")],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="col1", alias=None, source_columns=["col1"]),
            ],
            predicates=[],
            joins=[],
            aggregations=Aggregation(group_by=["col1"], functions=["COUNT(*)"]),
            limit=Limit(count=100, offset=None),
            ordering=None,
            distinct=False,
            source_tables=["src"],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col1", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        _, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _FULL_CAPS, _EMPTY_CATALOG_TYPES,
        )

        limit_results = [r for r in results if r.rule == "cross_group_limit_pushdown"]
        assert len(limit_results) >= 1
        skipped = [r for r in limit_results if r.status == "skipped"]
        assert len(skipped) >= 1
        # Detail should mention aggregation
        assert any("aggregat" in r.detail.lower() for r in skipped)


# ===================================================================
# 13.5 Not applicable (capability gap) OptimizationResult
# ===================================================================


class TestNotApplicableCapabilityGapOptimizationResult:
    """13.5: Verify OptimizationResult with status not_applicable and detail
    naming the missing capability when source adapter lacks projection_pushdown.

    Requirements: 9.5
    """

    def test_not_applicable_capability_gap_result(self):
        src = _make_cj(
            "src",
            engine_type="databricks",
            column_lineage=[_source_lineage("col1")],
        )

        consumer_plan = LogicalPlan(
            projections=[
                Projection(expression="col1", alias=None, source_columns=["col1"]),
            ],
            predicates=[],
            joins=[],
            aggregations=None,
            limit=None,
            ordering=None,
            distinct=False,
            source_tables=["src"],
        )
        consumer = _make_cj(
            "consumer",
            joint_type="sql",
            upstream=["src"],
            engine="eng2",
            engine_type="polars",
            logical_plan=consumer_plan,
            column_lineage=[_direct_lineage("col1", "src")],
        )

        grp_src = _make_group(["src"], engine_type="databricks")
        grp_con = _make_group(["consumer"], engine="eng2", engine_type="polars")

        cj_map = {c.name: c for c in [src, consumer]}
        _, results = cross_group_pushdown_pass(
            [grp_src, grp_con], cj_map, _NO_PROJ_CAPS, _EMPTY_CATALOG_TYPES,
        )

        proj_results = [r for r in results if r.rule == "cross_group_projection_pushdown"]
        assert len(proj_results) >= 1
        not_applicable = [r for r in proj_results if r.status == "not_applicable"]
        assert len(not_applicable) >= 1
        # Detail should name the missing capability
        assert any(
            "projection" in r.detail.lower() or "capability" in r.detail.lower()
            for r in not_applicable
        )
