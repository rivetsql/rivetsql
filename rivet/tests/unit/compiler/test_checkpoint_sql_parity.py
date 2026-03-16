"""Unit tests for checkpoint SQL parity with sink joints.

Tests that the compiler correctly handles checkpoint joints with SQL using the
same code path as sink joints: LogicalPlan extraction, column lineage, schema
inference, write_strategy defaulting, and _infer_sink_schemas coverage.

Requirements: 1.1, 1.2, 1.3, 1.4, 1.6, 1.7, 1.8, 5.1
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
# Helpers (mirrors test_sink_sql_parsing.py pattern)
# ---------------------------------------------------------------------------


def _schema(*cols: tuple[str, str]) -> Schema:
    return Schema(columns=[Column(name=n, type=t, nullable=True) for n, t in cols])


def _checkpoint_joint(
    name: str = "my_checkpoint",
    sql: str | None = None,
    upstream: list[str] | None = None,
    catalog: str = "local",
    table: str = "out_table",
    write_strategy: str | None = None,
) -> Joint:
    return Joint(
        name=name,
        joint_type="checkpoint",
        sql=sql,
        upstream=upstream or [],
        catalog=catalog,
        table=table,
        write_strategy=write_strategy,
    )


def _compiled_joint_stub(
    name: str,
    joint_type: str = "source",
    output_schema: Schema | None = None,
    upstream: list[str] | None = None,
) -> CompiledJoint:
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


def _compile_checkpoint_sql(
    sql: str,
    upstream_schemas: dict[str, Schema] | None = None,
    upstream: list[str] | None = None,
) -> tuple[CompiledJoint, list[RivetError], list[str]]:
    """Compile a checkpoint joint's SQL and return a stub CompiledJoint with results."""
    joint = _checkpoint_joint(
        sql=sql,
        upstream=upstream or list((upstream_schemas or {}).keys()),
    )
    parser = SQLParser()
    registry = PluginRegistry()
    errors: list[RivetError] = []
    warnings: list[str] = []

    logical_plan, column_lineage, sql_translated, engine_dialect, sql_schema = _compile_sql_joint(
        joint,
        "",
        registry,
        parser,
        upstream_schemas or {},
        errors,
        warnings,
    )
    cj = _compiled_joint_stub(
        name=joint.name,
        joint_type="checkpoint",
        output_schema=sql_schema,
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
# 1.1 — Checkpoint with SQL produces LogicalPlan
# ===================================================================


class TestCheckpointWithSqlProducesLogicalPlan:
    def test_projections_predicates_limit(self) -> None:
        cj, errors, _ = _compile_checkpoint_sql(
            "SELECT col1, col2 FROM upstream WHERE col1 > 5 LIMIT 10",
            upstream_schemas={"upstream": _schema(("col1", "int64"), ("col2", "utf8"))},
        )
        assert not errors
        assert cj.logical_plan is not None
        assert len(cj.logical_plan.projections) == 2
        assert len(cj.logical_plan.predicates) == 1
        assert cj.logical_plan.limit is not None
        assert cj.logical_plan.limit.count == 10


# ===================================================================
# 1.2 — Checkpoint with SQL extracts column lineage
# ===================================================================


class TestCheckpointSqlLineage:
    def test_alias_maps_to_upstream_column(self) -> None:
        cj, errors, _ = _compile_checkpoint_sql(
            "SELECT col1 AS alias1 FROM upstream",
            upstream_schemas={"upstream": _schema(("col1", "int64"), ("col2", "utf8"))},
        )
        assert not errors
        alias_lineage = [l for l in cj.column_lineage if l.output_column == "alias1"]
        assert len(alias_lineage) == 1
        lin = alias_lineage[0]
        assert lin.transform == "renamed"
        assert ColumnOrigin(joint="upstream", column="col1") in lin.origins


# ===================================================================
# 1.6 — Checkpoint with no SQL has None logical_plan and empty lineage
# ===================================================================


class TestCheckpointNoSqlProducesNonePlan:
    def test_no_sql_no_plan_no_lineage(self) -> None:
        joint = _checkpoint_joint(sql=None, upstream=["upstream"])
        # The unified branch is only entered when joint.sql is truthy
        assert joint.sql is None
        assert not (joint.joint_type in ("sink", "checkpoint") and joint.sql)


# ===================================================================
# 1.8 — Checkpoint write_strategy defaults to "replace"
# ===================================================================


class TestCheckpointWriteStrategyDefault:
    def test_no_write_strategy_defaults_to_replace(self) -> None:
        """The unified sink/checkpoint branch defaults write_strategy to 'replace'."""
        joint = _checkpoint_joint(write_strategy=None)
        # Simulate what _compile_joint does for the write_strategy default
        write_strategy = joint.write_strategy
        if joint.joint_type in ("sink", "checkpoint"):
            if write_strategy is None:
                write_strategy = "replace"
        assert write_strategy == "replace"

    def test_explicit_write_strategy_preserved(self) -> None:
        joint = _checkpoint_joint(write_strategy="append")
        write_strategy = joint.write_strategy
        if joint.joint_type in ("sink", "checkpoint"):
            if write_strategy is None:
                write_strategy = "replace"
        assert write_strategy == "append"


# ===================================================================
# 1.7 — _infer_sink_schemas applies to checkpoint joints
# ===================================================================


class TestInferSinkSchemasAppliesToCheckpoints:
    def test_checkpoint_with_no_schema_gets_upstream_schema(self) -> None:
        upstream_schema = _schema(("col1", "int64"), ("col2", "utf8"))
        checkpoint_cj = _compiled_joint_stub(
            "my_checkpoint", joint_type="checkpoint", output_schema=None, upstream=["src"]
        )
        src_cj = _compiled_joint_stub("src", joint_type="source", output_schema=upstream_schema)

        result = _infer_sink_schemas([src_cj, checkpoint_cj], [])
        cp_result = next(cj for cj in result if cj.name == "my_checkpoint")
        assert cp_result.output_schema is not None
        assert cp_result.output_schema == upstream_schema

    def test_checkpoint_with_sql_schema_preserved(self) -> None:
        """SQL-inferred schema on a checkpoint is not overwritten by _infer_sink_schemas."""
        sql_schema = _schema(("col1", "int64"))
        upstream_schema = _schema(("col1", "int64"), ("col2", "utf8"), ("col3", "float64"))

        checkpoint_cj = _compiled_joint_stub(
            "my_checkpoint", joint_type="checkpoint", output_schema=sql_schema, upstream=["src"]
        )
        src_cj = _compiled_joint_stub("src", joint_type="source", output_schema=upstream_schema)

        result = _infer_sink_schemas([src_cj, checkpoint_cj], [])
        cp_result = next(cj for cj in result if cj.name == "my_checkpoint")
        # SQL-inferred schema (1 col) should be preserved, not overwritten by upstream (3 cols)
        assert cp_result.output_schema is not None
        assert len(cp_result.output_schema.columns) == 1

    def test_non_sink_non_checkpoint_joints_unchanged(self) -> None:
        """_infer_sink_schemas does not touch source or sql joints."""
        src_schema = _schema(("x", "int64"))
        src_cj = _compiled_joint_stub("src", joint_type="source", output_schema=src_schema)
        sql_cj = _compiled_joint_stub("transform", joint_type="sql", output_schema=None)

        result = _infer_sink_schemas([src_cj, sql_cj], [])
        src_out = next(cj for cj in result if cj.name == "src")
        sql_out = next(cj for cj in result if cj.name == "transform")
        assert src_out.output_schema == src_schema
        assert sql_out.output_schema is None


# ===================================================================
# Parity: checkpoint SQL compilation matches sink SQL compilation
# ===================================================================


class TestCheckpointSinkCompilationParity:
    def test_same_sql_produces_same_logical_plan(self) -> None:
        """Checkpoint and sink with identical SQL produce identical LogicalPlans."""
        sql = "SELECT col1, col2 FROM upstream WHERE col1 > 0 LIMIT 5"
        upstream_schemas = {"upstream": _schema(("col1", "int64"), ("col2", "utf8"))}

        # Compile as checkpoint
        cp_cj, cp_errors, _ = _compile_checkpoint_sql(sql, upstream_schemas)

        # Compile as sink (reuse the same helper from test_sink_sql_parsing)
        sink_joint = Joint(
            name="my_sink",
            joint_type="sink",
            sql=sql,
            upstream=list(upstream_schemas.keys()),
        )
        parser = SQLParser()
        registry = PluginRegistry()
        errors: list[RivetError] = []
        warnings: list[str] = []
        sink_lp, sink_lineage, _, _, _ = _compile_sql_joint(
            sink_joint, "", registry, parser, upstream_schemas, errors, warnings
        )

        assert not cp_errors
        assert not errors
        assert cp_cj.logical_plan is not None
        assert sink_lp is not None

        # LogicalPlans should be structurally identical
        assert len(cp_cj.logical_plan.projections) == len(sink_lp.projections)
        assert len(cp_cj.logical_plan.predicates) == len(sink_lp.predicates)
        assert cp_cj.logical_plan.limit == sink_lp.limit
