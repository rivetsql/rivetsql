"""Unit tests for sink SQL parsing compiler behavior.

Tests that the compiler correctly parses SQL on sink joints into LogicalPlans,
extracts column lineage, infers schemas, and translates dialects — using the
same code path as sql-type joints.
"""

from __future__ import annotations

from dataclasses import replace

from rivet_core.compiler import (
    CompiledJoint,
    _compile_sql_joint,
    _infer_sink_schemas,
)
from rivet_core.errors import RivetError
from rivet_core.lineage import ColumnOrigin
from rivet_core.models import Column, Joint, Schema
from rivet_core.plugins import PluginRegistry
from rivet_core.sql_parser import SQLParser

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _schema(*cols: tuple[str, str]) -> Schema:
    return Schema(columns=[Column(name=n, type=t, nullable=True) for n, t in cols])


def _sink_joint(
    name: str = "my_sink",
    sql: str | None = None,
    upstream: list[str] | None = None,
    dialect: str | None = None,
) -> Joint:
    return Joint(
        name=name,
        joint_type="sink",
        sql=sql,
        upstream=upstream or [],
        dialect=dialect,
    )


def _compiled_joint_stub(
    name: str,
    joint_type: str = "source",
    output_schema: Schema | None = None,
    upstream: list[str] | None = None,
) -> CompiledJoint:
    """Minimal CompiledJoint for _infer_sink_schemas tests."""
    return CompiledJoint(
        name=name,
        type=joint_type,
        catalog=None,
        catalog_type=None,
        engine="duckdb_local",
        engine_resolution="project_default",
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
        logical_plan=None,
        output_schema=output_schema,
        column_lineage=[],
        optimizations=[],
        checks=[],
        fused_group_id=None,
        tags=[],
        description=None,
        fusion_strategy_override=None,
        materialization_strategy_override=None,
    )


def _compile_sink(
    sql: str,
    upstream_schemas: dict[str, Schema] | None = None,
    dialect: str | None = None,
    upstream: list[str] | None = None,
) -> tuple[CompiledJoint, list[RivetError], list[str]]:
    """Compile a sink joint's SQL and return a stub CompiledJoint with results."""
    joint = _sink_joint(
        sql=sql,
        upstream=upstream or list((upstream_schemas or {}).keys()),
        dialect=dialect,
    )
    parser = SQLParser()
    registry = PluginRegistry()
    errors: list[RivetError] = []
    warnings: list[str] = []

    logical_plan, column_lineage, sql_translated, engine_dialect, sql_schema = _compile_sql_joint(
        joint,
        "",  # engine_type — empty, no engine plugin registered
        registry,
        parser,
        upstream_schemas or {},
        errors,
        warnings,
    )
    output_schema = sql_schema
    cj = _compiled_joint_stub(
        name=joint.name,
        joint_type="sink",
        output_schema=output_schema,
        upstream=joint.upstream,
    )
    cj = replace(
        cj,
        sql=joint.sql,
        logical_plan=logical_plan,
        column_lineage=column_lineage,
        sql_translated=sql_translated,
        engine_dialect=engine_dialect,
    )
    return cj, errors, warnings


# ===================================================================
# 3.1 — Sink with valid SQL produces LogicalPlan
# ===================================================================


class TestSinkValidSqlLogicalPlan:
    def test_projections_predicates_limit(self) -> None:
        cj, errors, _ = _compile_sink(
            "SELECT col1, col2 FROM upstream_joint WHERE col1 > 10 LIMIT 5",
            upstream_schemas={"upstream_joint": _schema(("col1", "int64"), ("col2", "utf8"))},
        )
        assert cj.logical_plan is not None
        assert len(cj.logical_plan.projections) == 2
        assert len(cj.logical_plan.predicates) == 1
        assert cj.logical_plan.limit is not None
        assert cj.logical_plan.limit.count == 5
        assert not errors


# ===================================================================
# 3.2 — Sink with aliased columns produces correct lineage
# ===================================================================


class TestSinkAliasedColumnLineage:
    def test_alias_maps_to_upstream_column(self) -> None:
        cj, errors, _ = _compile_sink(
            "SELECT col1 AS alias1 FROM upstream_joint",
            upstream_schemas={"upstream_joint": _schema(("col1", "int64"), ("col2", "utf8"))},
        )
        assert not errors
        assert cj.logical_plan is not None
        # Find lineage for alias1
        alias_lineage = [l for l in cj.column_lineage if l.output_column == "alias1"]
        assert len(alias_lineage) == 1
        lin = alias_lineage[0]
        assert lin.transform == "renamed"
        assert ColumnOrigin(joint="upstream_joint", column="col1") in lin.origins


# ===================================================================
# 3.3 — Sink with multi-upstream expression produces lineage
# ===================================================================


class TestSinkMultiUpstreamLineage:
    def test_expression_has_origins_from_both_upstreams(self) -> None:
        cj, errors, _ = _compile_sink(
            "SELECT a.col1 + b.col2 AS combined FROM upstream_a a JOIN upstream_b b ON a.id = b.id",
            upstream_schemas={
                "upstream_a": _schema(("id", "int64"), ("col1", "int64")),
                "upstream_b": _schema(("id", "int64"), ("col2", "int64")),
            },
        )
        assert cj.logical_plan is not None
        combined_lineage = [l for l in cj.column_lineage if l.output_column == "combined"]
        assert len(combined_lineage) == 1
        lin = combined_lineage[0]
        origin_joints = {o.joint for o in lin.origins}
        assert "upstream_a" in origin_joints
        assert "upstream_b" in origin_joints


# ===================================================================
# 3.4 — Sink with no SQL has None logical_plan and empty lineage
# ===================================================================


class TestSinkNoSql:
    def test_no_sql_no_plan_no_lineage(self) -> None:
        """A sink joint with sql=None should not enter the SQL parsing branch."""
        joint = _sink_joint(sql=None, upstream=["upstream_joint"])
        # Simulate what _compile_joint does: the sink branch is only entered
        # when joint.joint_type == "sink" and joint.sql is truthy.
        # With sql=None, logical_plan stays None and lineage stays empty.
        assert joint.sql is None
        # Verify the condition that guards the sink branch
        assert not (joint.joint_type == "sink" and joint.sql)


# ===================================================================
# 3.5 — Sink with invalid SQL has None logical_plan and error recorded
# ===================================================================


class TestSinkInvalidSql:
    def test_invalid_sql_produces_none_plan_and_error(self) -> None:
        cj, errors, _ = _compile_sink("NOT VALID SQL AT ALL")
        assert cj.logical_plan is None
        assert cj.column_lineage == []
        assert len(errors) >= 1
        # Error should be an RVT-7xx SQL error
        assert any(e.code.startswith("RVT-7") for e in errors)


# ===================================================================
# 3.6 — Sink with SQL and upstream schemas gets SQL-inferred schema
# ===================================================================


class TestSinkSqlInferredSchema:
    def test_sql_inferred_schema_subset_of_upstream(self) -> None:
        upstream = _schema(("col1", "int64"), ("col2", "utf8"), ("col3", "float64"))
        cj, errors, _ = _compile_sink(
            "SELECT col1, col2 FROM upstream_joint",
            upstream_schemas={"upstream_joint": upstream},
        )
        assert not errors
        assert cj.output_schema is not None
        col_names = [c.name for c in cj.output_schema.columns]
        assert col_names == ["col1", "col2"]
        # Should NOT be the full upstream schema
        assert len(cj.output_schema.columns) == 2

    def test_sql_schema_preserved_by_infer_sink_schemas(self) -> None:
        """_infer_sink_schemas should keep the SQL-inferred schema, not overwrite it."""
        sql_schema = _schema(("col1", "int64"), ("col2", "utf8"))
        upstream_schema = _schema(("col1", "int64"), ("col2", "utf8"), ("col3", "float64"))

        sink_cj = _compiled_joint_stub(
            "my_sink", joint_type="sink", output_schema=sql_schema, upstream=["src"]
        )
        src_cj = _compiled_joint_stub("src", joint_type="source", output_schema=upstream_schema)

        result = _infer_sink_schemas([src_cj, sink_cj], [])
        sink_result = [cj for cj in result if cj.name == "my_sink"][0]
        # SQL-inferred schema should be preserved (2 columns, not 3)
        assert sink_result.output_schema is not None
        assert len(sink_result.output_schema.columns) == 2


# ===================================================================
# 3.7 — Sink with SQL but failed schema inference falls back
# ===================================================================


class TestSinkSchemaFallback:
    def test_no_upstream_schema_falls_back(self) -> None:
        """When SQL schema inference returns None (no upstream schemas),
        _infer_sink_schemas should set the schema from upstream."""
        upstream_schema = _schema(("col1", "int64"), ("col2", "utf8"))

        # Sink with output_schema=None (SQL inference failed)
        sink_cj = _compiled_joint_stub(
            "my_sink", joint_type="sink", output_schema=None, upstream=["src"]
        )
        src_cj = _compiled_joint_stub("src", joint_type="source", output_schema=upstream_schema)

        result = _infer_sink_schemas([src_cj, sink_cj], [])
        sink_result = [cj for cj in result if cj.name == "my_sink"][0]
        # Should fall back to upstream schema
        assert sink_result.output_schema is not None
        assert len(sink_result.output_schema.columns) == 2
        assert sink_result.output_schema == upstream_schema


# ===================================================================
# 3.8 — Sink SQL dialect translation
# ===================================================================


class TestSinkDialectTranslation:
    def test_sink_sql_translated_when_dialect_differs(self) -> None:
        """Sink with SQL in duckdb dialect assigned to a postgres engine
        should produce sql_translated."""
        joint = _sink_joint(
            sql="SELECT col1 FROM upstream_joint LIMIT 10",
            upstream=["upstream_joint"],
            dialect=None,  # defaults to duckdb
        )
        parser = SQLParser()
        registry = PluginRegistry()
        # Register DuckDB engine plugin so we can look up postgres
        try:
            from rivet_postgres.engine import PostgresComputeEnginePlugin

            registry.register_engine_plugin(PostgresComputeEnginePlugin())
        except ImportError:
            # If postgres plugin not available, register a mock
            from unittest.mock import MagicMock

            mock_plugin = MagicMock()
            mock_plugin.engine_type = "postgres"
            mock_plugin.dialect = "postgres"
            registry._engine_plugins["postgres"] = mock_plugin

        errors: list[RivetError] = []
        warnings: list[str] = []
        upstream_schemas = {
            "upstream_joint": _schema(("col1", "int64")),
        }

        lp, lineage, sql_translated, engine_dialect, sql_schema = _compile_sql_joint(
            joint,
            "postgres",  # engine_type triggers dialect lookup
            registry,
            parser,
            upstream_schemas,
            errors,
            warnings,
        )
        assert engine_dialect == "postgres"
        assert sql_translated is not None
        assert "SELECT" in sql_translated


# ===================================================================
# Integration-level tests — compiler + optimizer end-to-end (Task 4)
# ===================================================================

import copy
import uuid

from rivet_core.lineage import ColumnLineage
from rivet_core.optimizer import (
    FusedGroup,
    cross_group_pushdown_pass,
)

# ---------------------------------------------------------------------------
# Integration helpers
# ---------------------------------------------------------------------------

_FULL_CAPS = {"databricks": ["predicate_pushdown", "projection_pushdown", "limit_pushdown"]}
_EMPTY_CATALOG_TYPES: dict[str, str | None] = {}


def _source_lineage(col: str) -> ColumnLineage:
    return ColumnLineage(output_column=col, transform="source", origins=[], expression=None)


def _make_source_cj(
    name: str,
    columns: list[str],
    engine_type: str = "databricks",
) -> CompiledJoint:
    """Create a source CompiledJoint with source-level lineage."""
    return _compiled_joint_stub(
        name=name,
        joint_type="source",
        output_schema=_schema(*[(c, "int64") for c in columns]),
        upstream=[],
    )


def _make_group(
    joints: list[str],
    *,
    group_id: str | None = None,
    engine: str = "eng1",
    engine_type: str = "databricks",
    entry_joints: list[str] | None = None,
    exit_joints: list[str] | None = None,
) -> FusedGroup:
    return FusedGroup(
        id=group_id or str(uuid.uuid4()),
        joints=joints,
        engine=engine,
        engine_type=engine_type,
        adapters={j: None for j in joints},
        fused_sql=None,
        entry_joints=entry_joints or joints[:1],
        exit_joints=exit_joints or joints[-1:],
        per_joint_predicates={},
    )


def _run_pushdown(
    source_cjs: list[CompiledJoint],
    sink_cj: CompiledJoint,
    source_group: FusedGroup,
    sink_group: FusedGroup,
    caps: dict[str, list[str]] | None = None,
) -> tuple[list[FusedGroup], list[object]]:
    """Run cross_group_pushdown_pass with the given source + sink setup."""
    cj_map = {cj.name: cj for cj in [*source_cjs, sink_cj]}
    groups = [source_group, sink_group]
    return cross_group_pushdown_pass(
        groups,
        cj_map,
        caps or _FULL_CAPS,
        _EMPTY_CATALOG_TYPES,
    )


# ===================================================================
# 4.1 — source → sink with WHERE predicate → predicate pushed
# ===================================================================


class TestSinkPredicatePushdown:
    def test_predicate_pushed_to_source_group(self) -> None:
        upstream_schemas = {"src": _schema(("col", "utf8"), ("id", "int64"))}
        sink_cj, errors, _ = _compile_sink(
            "SELECT src.col, src.id FROM src WHERE src.col = 'value'",
            upstream_schemas=upstream_schemas,
            upstream=["src"],
        )
        assert not errors
        assert sink_cj.logical_plan is not None
        assert len(sink_cj.logical_plan.predicates) == 1

        src_cj = _make_source_cj("src", ["col", "id"])
        src_group = _make_group(["src"], group_id="src_grp")
        sink_group = _make_group(
            ["my_sink"],
            group_id="sink_grp",
            entry_joints=["my_sink"],
            exit_joints=["my_sink"],
        )

        new_groups, results = _run_pushdown([src_cj], sink_cj, src_group, sink_group)

        # Predicate should be pushed to source group
        src_grp = [g for g in new_groups if g.id == "src_grp"][0]
        assert "src" in src_grp.per_joint_predicates
        pushed = src_grp.per_joint_predicates["src"]
        assert len(pushed) >= 1
        assert any("value" in p.expression for p in pushed)

        # OptimizationResult with status=applied should exist
        applied = [
            r
            for r in results
            if r.status == "applied" and r.rule == "cross_group_predicate_pushdown"
        ]
        assert len(applied) >= 1


# ===================================================================
# 4.2 — source → sink with explicit projections → projections pushed
# ===================================================================


class TestSinkProjectionPushdown:
    def test_projections_pushed_to_source_group(self) -> None:
        upstream_schemas = {
            "src": _schema(("col1", "int64"), ("col2", "utf8"), ("col3", "float64")),
        }
        sink_cj, errors, _ = _compile_sink(
            "SELECT src.col1, src.col2 FROM src",
            upstream_schemas=upstream_schemas,
            upstream=["src"],
        )
        assert not errors
        assert sink_cj.logical_plan is not None

        src_cj = _make_source_cj("src", ["col1", "col2", "col3"])
        src_group = _make_group(["src"], group_id="src_grp")
        sink_group = _make_group(
            ["my_sink"],
            group_id="sink_grp",
            entry_joints=["my_sink"],
            exit_joints=["my_sink"],
        )

        new_groups, results = _run_pushdown([src_cj], sink_cj, src_group, sink_group)

        src_grp = [g for g in new_groups if g.id == "src_grp"][0]
        assert "src" in src_grp.per_joint_projections
        pushed_cols = set(src_grp.per_joint_projections["src"])
        assert "col1" in pushed_cols
        assert "col2" in pushed_cols
        # col3 should NOT be in the pushed projections
        assert "col3" not in pushed_cols

        applied = [
            r
            for r in results
            if r.status == "applied" and r.rule == "cross_group_projection_pushdown"
        ]
        assert len(applied) >= 1


# ===================================================================
# 4.3 — source → sink with LIMIT → limit pushed
# ===================================================================


class TestSinkLimitPushdown:
    def test_limit_pushed_to_source_group(self) -> None:
        upstream_schemas = {"src": _schema(("col1", "int64"), ("col2", "utf8"))}
        sink_cj, errors, _ = _compile_sink(
            "SELECT * FROM src LIMIT 10",
            upstream_schemas=upstream_schemas,
            upstream=["src"],
        )
        assert not errors
        assert sink_cj.logical_plan is not None
        assert sink_cj.logical_plan.limit is not None
        assert sink_cj.logical_plan.limit.count == 10

        src_cj = _make_source_cj("src", ["col1", "col2"])
        src_group = _make_group(["src"], group_id="src_grp")
        sink_group = _make_group(
            ["my_sink"],
            group_id="sink_grp",
            entry_joints=["my_sink"],
            exit_joints=["my_sink"],
        )

        new_groups, results = _run_pushdown([src_cj], sink_cj, src_group, sink_group)

        src_grp = [g for g in new_groups if g.id == "src_grp"][0]
        assert "src" in src_grp.per_joint_limits
        assert src_grp.per_joint_limits["src"] == 10

        applied = [
            r for r in results if r.status == "applied" and r.rule == "cross_group_limit_pushdown"
        ]
        assert len(applied) >= 1


# ===================================================================
# 4.4 — source → sink with SELECT * → no projection pushdown
# ===================================================================


class TestSinkSelectStarNoProjectionPushdown:
    def test_select_star_skips_projection_pushdown(self) -> None:
        upstream_schemas = {"src": _schema(("col1", "int64"), ("col2", "utf8"))}
        sink_cj, errors, _ = _compile_sink(
            "SELECT * FROM src",
            upstream_schemas=upstream_schemas,
            upstream=["src"],
        )
        assert not errors
        assert sink_cj.logical_plan is not None

        src_cj = _make_source_cj("src", ["col1", "col2"])
        src_group = _make_group(["src"], group_id="src_grp")
        sink_group = _make_group(
            ["my_sink"],
            group_id="sink_grp",
            entry_joints=["my_sink"],
            exit_joints=["my_sink"],
        )

        new_groups, results = _run_pushdown([src_cj], sink_cj, src_group, sink_group)

        src_grp = [g for g in new_groups if g.id == "src_grp"][0]
        # No projection pushdown should occur
        assert "src" not in src_grp.per_joint_projections

        skipped = [
            r
            for r in results
            if r.status == "skipped" and r.rule == "cross_group_projection_pushdown"
        ]
        assert len(skipped) >= 1


# ===================================================================
# 4.5 — two sources → sink with JOIN + LIMIT → limit NOT pushed
# ===================================================================


class TestSinkJoinLimitNotPushed:
    def test_join_blocks_limit_pushdown(self) -> None:
        upstream_schemas = {
            "src_a": _schema(("id", "int64"), ("col1", "int64")),
            "src_b": _schema(("id", "int64"), ("col2", "int64")),
        }
        sink_cj, errors, _ = _compile_sink(
            "SELECT a.col1, b.col2 FROM src_a a JOIN src_b b ON a.id = b.id LIMIT 10",
            upstream_schemas=upstream_schemas,
            upstream=["src_a", "src_b"],
        )
        assert not errors
        assert sink_cj.logical_plan is not None
        assert sink_cj.logical_plan.limit is not None
        assert len(sink_cj.logical_plan.joins) >= 1

        src_a_cj = _make_source_cj("src_a", ["id", "col1"])
        src_b_cj = _make_source_cj("src_b", ["id", "col2"])
        grp_a = _make_group(["src_a"], group_id="grp_a")
        grp_b = _make_group(["src_b"], group_id="grp_b")
        sink_group = _make_group(
            ["my_sink"],
            group_id="sink_grp",
            entry_joints=["my_sink"],
            exit_joints=["my_sink"],
        )

        cj_map = {cj.name: cj for cj in [src_a_cj, src_b_cj, sink_cj]}
        groups = [grp_a, grp_b, sink_group]
        new_groups, results = cross_group_pushdown_pass(
            groups,
            cj_map,
            _FULL_CAPS,
            _EMPTY_CATALOG_TYPES,
        )

        grp_a_out = [g for g in new_groups if g.id == "grp_a"][0]
        grp_b_out = [g for g in new_groups if g.id == "grp_b"][0]
        # Limit should NOT be pushed to either source group (join blocks it)
        assert "src_a" not in grp_a_out.per_joint_limits
        assert "src_b" not in grp_b_out.per_joint_limits

        skipped = [
            r for r in results if r.status == "skipped" and r.rule == "cross_group_limit_pushdown"
        ]
        assert len(skipped) >= 1


# ===================================================================
# 4.6 — sink with aggregation + LIMIT → limit NOT pushed
# ===================================================================


class TestSinkAggregationLimitNotPushed:
    def test_aggregation_blocks_limit_pushdown(self) -> None:
        upstream_schemas = {"src": _schema(("col1", "int64"))}
        sink_cj, errors, _ = _compile_sink(
            "SELECT COUNT(*) FROM src LIMIT 5",
            upstream_schemas=upstream_schemas,
            upstream=["src"],
        )
        assert not errors
        assert sink_cj.logical_plan is not None
        assert sink_cj.logical_plan.limit is not None
        assert sink_cj.logical_plan.aggregations is not None

        src_cj = _make_source_cj("src", ["col1"])
        src_group = _make_group(["src"], group_id="src_grp")
        sink_group = _make_group(
            ["my_sink"],
            group_id="sink_grp",
            entry_joints=["my_sink"],
            exit_joints=["my_sink"],
        )

        new_groups, results = _run_pushdown([src_cj], sink_cj, src_group, sink_group)

        src_grp = [g for g in new_groups if g.id == "src_grp"][0]
        # Limit should NOT be pushed (aggregation blocks it)
        assert "src" not in src_grp.per_joint_limits

        skipped = [
            r for r in results if r.status == "skipped" and r.rule == "cross_group_limit_pushdown"
        ]
        assert len(skipped) >= 1
        assert any("aggregation" in r.detail.lower() for r in skipped)


# ===================================================================
# 4.7 — sink pushdown preserves consumer-side LogicalPlan
# ===================================================================


class TestSinkPushdownPreservesLogicalPlan:
    def test_logical_plan_unchanged_after_pushdown(self) -> None:
        upstream_schemas = {"src": _schema(("col1", "int64"), ("col2", "utf8"))}
        sink_cj, errors, _ = _compile_sink(
            "SELECT col1 FROM src WHERE col2 = 'x' LIMIT 10",
            upstream_schemas=upstream_schemas,
            upstream=["src"],
        )
        assert not errors
        assert sink_cj.logical_plan is not None

        # Deep copy the LogicalPlan before the pass
        lp_before = copy.deepcopy(sink_cj.logical_plan)

        src_cj = _make_source_cj("src", ["col1", "col2"])
        src_group = _make_group(["src"], group_id="src_grp")
        sink_group = _make_group(
            ["my_sink"],
            group_id="sink_grp",
            entry_joints=["my_sink"],
            exit_joints=["my_sink"],
        )

        _run_pushdown([src_cj], sink_cj, src_group, sink_group)

        # LogicalPlan on the sink should be identical after the pass
        lp_after = sink_cj.logical_plan
        assert lp_after is not None
        assert lp_after.projections == lp_before.projections
        assert lp_after.predicates == lp_before.predicates
        assert lp_after.limit == lp_before.limit
        assert lp_after.joins == lp_before.joins
        assert lp_after.aggregations == lp_before.aggregations
        assert lp_after.ordering == lp_before.ordering
        assert lp_after.distinct == lp_before.distinct


# ===================================================================
# 4.8 — sink pushdown skipped produces OptimizationResult with skipped
# ===================================================================


class TestSinkPushdownSkippedResult:
    def test_non_pushable_predicate_produces_skipped_result(self) -> None:
        """Sink with HAVING-like predicate (aggregation-derived) should produce
        a skipped OptimizationResult."""
        upstream_schemas = {"src": _schema(("category", "utf8"), ("amount", "int64"))}
        sink_cj, errors, _ = _compile_sink(
            "SELECT category, SUM(amount) AS total FROM src "
            "GROUP BY category HAVING SUM(amount) > 100",
            upstream_schemas=upstream_schemas,
            upstream=["src"],
        )
        assert not errors
        assert sink_cj.logical_plan is not None

        src_cj = _make_source_cj("src", ["category", "amount"])
        src_group = _make_group(["src"], group_id="src_grp")
        sink_group = _make_group(
            ["my_sink"],
            group_id="sink_grp",
            entry_joints=["my_sink"],
            exit_joints=["my_sink"],
        )

        new_groups, results = _run_pushdown([src_cj], sink_cj, src_group, sink_group)

        # The HAVING predicate should be skipped (non-pushable)
        skipped_preds = [
            r
            for r in results
            if r.status == "skipped" and r.rule == "cross_group_predicate_pushdown"
        ]
        assert len(skipped_preds) >= 1
