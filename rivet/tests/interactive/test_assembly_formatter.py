"""Tests for AssemblyFormatter — format_joint(), format_joint_text(), and format_assembly_text().

Validates Requirements 9.3 (text rendering) and 12.1 (no ANSI when ansi=False).
"""

from __future__ import annotations

import re

from rivet_core.checks import CompiledCheck
from rivet_core.compiler import (
    CompilationStats,
    CompiledAdapter,
    CompiledAssembly,
    CompiledCatalog,
    CompiledEngine,
    CompiledJoint,
    Materialization,
    OptimizationResult,
    SourceStats,
)
from rivet_core.interactive.assembly_formatter import AssemblyFormatter
from rivet_core.interactive.types import InspectFilter, Verbosity
from rivet_core.models import Column, Schema
from rivet_core.optimizer import FusedGroup

ANSI_RE = re.compile(r"\x1b\[")


def _make_assembly(
    joints: list[CompiledJoint] | None = None,
    fused_groups: list[FusedGroup] | None = None,
    materializations: list[Materialization] | None = None,
    execution_order: list[str] | None = None,
) -> CompiledAssembly:
    if joints is None:
        joints = [
            CompiledJoint(
                name="raw_orders",
                type="source",
                catalog="local",
                catalog_type="filesystem",
                engine="duckdb_main",
                engine_resolution="project_default",
                adapter=None,
                sql=None,
                sql_translated=None,
                sql_resolved=None,
                sql_dialect=None,
                engine_dialect=None,
                upstream=[],
                eager=False,
                table="raw_orders",
                write_strategy=None,
                function=None,
                source_file="sources/raw_orders.sql",
                logical_plan=None,
                output_schema=None,
                column_lineage=[],
                optimizations=[],
                checks=[],
                fused_group_id=None,
                tags=["staging"],
                description="Raw orders source",
                fusion_strategy_override=None,
                materialization_strategy_override=None,
            ),
            CompiledJoint(
                name="cleaned_orders",
                type="sql",
                catalog=None,
                catalog_type=None,
                engine="duckdb_main",
                engine_resolution="project_default",
                adapter=None,
                sql="SELECT * FROM raw_orders WHERE status != 'cancelled'",
                sql_translated=None,
                sql_resolved="SELECT * FROM raw_orders WHERE status != 'cancelled'",
                sql_dialect="duckdb",
                engine_dialect="duckdb",
                upstream=["raw_orders"],
                eager=False,
                table=None,
                write_strategy=None,
                function=None,
                source_file="joints/cleaned_orders.sql",
                logical_plan=None,
                output_schema=None,
                column_lineage=[],
                optimizations=[],
                checks=[],
                fused_group_id="group-1",
                tags=["staging"],
                description=None,
                fusion_strategy_override=None,
                materialization_strategy_override=None,
            ),
            CompiledJoint(
                name="output_orders",
                type="sink",
                catalog="local",
                catalog_type="filesystem",
                engine="duckdb_main",
                engine_resolution="project_default",
                adapter=None,
                sql=None,
                sql_translated=None,
                sql_resolved=None,
                sql_dialect=None,
                engine_dialect=None,
                upstream=["cleaned_orders"],
                eager=False,
                table="output_orders",
                write_strategy="replace",
                function=None,
                source_file="sinks/output_orders.sql",
                logical_plan=None,
                output_schema=None,
                column_lineage=[],
                optimizations=[],
                checks=[],
                fused_group_id=None,
                tags=[],
                description=None,
                fusion_strategy_override=None,
                materialization_strategy_override=None,
            ),
        ]
    if fused_groups is None:
        fused_groups = [
            FusedGroup(
                id="group-1",
                joints=["raw_orders", "cleaned_orders"],
                engine="duckdb_main",
                engine_type="duckdb",
                adapters={"raw_orders": None, "cleaned_orders": None},
                fused_sql="WITH raw_orders AS (...) SELECT * FROM raw_orders WHERE status != 'cancelled'",
                entry_joints=["raw_orders"],
                exit_joints=["cleaned_orders"],
            ),
        ]
    if materializations is None:
        materializations = [
            Materialization(
                from_joint="cleaned_orders",
                to_joint="output_orders",
                trigger="engine_instance_change",
                detail="Different engine instances",
                strategy="arrow",
            ),
        ]
    if execution_order is None:
        execution_order = ["group-1", "output_orders"]

    return CompiledAssembly(
        success=True,
        profile_name="dev",
        catalogs=[CompiledCatalog(name="local", type="filesystem")],
        engines=[CompiledEngine(name="duckdb_main", engine_type="duckdb", native_catalog_types=["filesystem"])],
        adapters=[CompiledAdapter(engine_type="duckdb", catalog_type="postgres", source="rivet_duckdb")],
        joints=joints,
        fused_groups=fused_groups,
        materializations=materializations,
        execution_order=execution_order,
        errors=[],
        warnings=["Unused joint: orphan_joint"],
    )


def _make_joint(**overrides) -> CompiledJoint:
    """Create a CompiledJoint with sensible defaults, overridable."""
    defaults = dict(
        name="my_joint",
        type="sql",
        catalog=None,
        catalog_type=None,
        engine="duckdb",
        engine_resolution="project_default",
        adapter=None,
        sql="SELECT 1",
        sql_translated=None,
        sql_resolved=None,
        sql_dialect=None,
        engine_dialect=None,
        upstream=[],
        eager=False,
        table=None,
        write_strategy=None,
        function=None,
        source_file=None,
        logical_plan=None,
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
    defaults.update(overrides)
    return CompiledJoint(**defaults)


class TestFormatJoint:
    def test_basic_fields(self) -> None:
        joint = _make_joint(name="orders", type="sql", engine="duckdb", engine_resolution="project_default")
        assembly = _make_assembly([joint])
        fmt = AssemblyFormatter()
        result = fmt.format_joint(joint, assembly)

        assert result.name == "orders"
        assert result.type == "sql"
        assert result.engine == "duckdb"
        assert result.engine_resolution == "project_default"

    def test_all_optional_fields_none(self) -> None:
        joint = _make_joint()
        assembly = _make_assembly([joint])
        fmt = AssemblyFormatter()
        result = fmt.format_joint(joint, assembly)

        assert result.source_file is None
        assert result.adapter is None
        assert result.catalog is None
        assert result.table is None
        assert result.fused_group_id is None
        assert result.output_schema is None
        assert result.sql_translated is None
        assert result.sql_resolved is None
        assert result.write_strategy is None
        assert result.description is None

    def test_all_fields_populated(self) -> None:
        schema = Schema(columns=[Column(name="id", type="int64", nullable=False)])
        check = CompiledCheck(type="not_null", severity="error", config={"column": "id"}, phase="assertion")
        opt = OptimizationResult(rule="predicate_pushdown", status="applied", detail="pushed x > 1")
        joint = _make_joint(
            name="sink_orders",
            type="sink",
            source_file="models/orders.sql",
            engine="postgres",
            engine_resolution="joint_override",
            adapter="duckdb_to_postgres",
            catalog="warehouse",
            table="public.orders",
            fused_group_id="group-1",
            upstream=["raw_orders", "cleaned_orders"],
            output_schema=schema,
            sql="SELECT * FROM raw_orders",
            sql_translated="SELECT * FROM raw_orders",
            sql_resolved="SELECT * FROM main.raw_orders",
            write_strategy="append",
            tags=["staging", "daily"],
            description="Order sink joint",
            checks=[check],
            optimizations=[opt],
        )
        assembly = _make_assembly([joint])
        fmt = AssemblyFormatter()
        result = fmt.format_joint(joint, assembly)

        assert result.name == "sink_orders"
        assert result.type == "sink"
        assert result.source_file == "models/orders.sql"
        assert result.engine == "postgres"
        assert result.engine_resolution == "joint_override"
        assert result.adapter == "duckdb_to_postgres"
        assert result.catalog == "warehouse"
        assert result.table == "public.orders"
        assert result.fused_group_id == "group-1"
        assert result.upstream == ["raw_orders", "cleaned_orders"]
        assert result.output_schema is not None
        assert len(result.output_schema) == 1
        assert result.output_schema[0].name == "id"
        assert result.output_schema[0].type == "int64"
        assert result.sql_original == "SELECT * FROM raw_orders"
        assert result.sql_translated == "SELECT * FROM raw_orders"
        assert result.sql_resolved == "SELECT * FROM main.raw_orders"
        assert result.write_strategy == "append"
        assert result.tags == ["staging", "daily"]
        assert result.description == "Order sink joint"
        assert len(result.checks) == 1
        assert "not_null" in result.checks[0]
        assert len(result.optimizations) == 1
        assert "predicate_pushdown" in result.optimizations[0]

    def test_upstream_is_copy(self) -> None:
        """Upstream list should be a copy, not a reference."""
        original = ["a", "b"]
        joint = _make_joint(upstream=original)
        assembly = _make_assembly([joint])
        fmt = AssemblyFormatter()
        result = fmt.format_joint(joint, assembly)
        assert result.upstream == ["a", "b"]
        assert result.upstream is not original


class TestFormatJointText:
    def test_contains_joint_name(self) -> None:
        joint = _make_joint(name="orders")
        assembly = _make_assembly([joint])
        fmt = AssemblyFormatter()
        text = fmt.format_joint_text(joint, assembly)
        assert "orders" in text

    def test_ansi_false_no_escape_codes(self) -> None:
        joint = _make_joint(
            name="orders",
            source_file="models/orders.sql",
            sql="SELECT 1",
            tags=["staging"],
            description="test",
        )
        assembly = _make_assembly([joint])
        fmt = AssemblyFormatter()
        text = fmt.format_joint_text(joint, assembly, ansi=False)
        assert "\x1b[" not in text

    def test_ansi_true_has_escape_codes(self) -> None:
        joint = _make_joint(name="orders")
        assembly = _make_assembly([joint])
        fmt = AssemblyFormatter()
        text = fmt.format_joint_text(joint, assembly, ansi=True)
        assert "\x1b[" in text

    def test_optional_sections_present_when_populated(self) -> None:
        schema = Schema(columns=[Column(name="id", type="int64", nullable=False)])
        joint = _make_joint(
            name="orders",
            source_file="models/orders.sql",
            adapter="duckdb_to_pg",
            catalog="warehouse",
            table="public.orders",
            fused_group_id="g1",
            write_strategy="append",
            upstream=["raw"],
            tags=["staging"],
            description="desc",
            output_schema=schema,
            sql="SELECT 1",
            sql_translated="SELECT 1",
            sql_resolved="SELECT 1",
            checks=[CompiledCheck(type="not_null", severity="error", config={}, phase="assertion")],
            optimizations=[OptimizationResult(rule="pushdown", status="applied", detail="ok")],
            schema_confidence="inferred",
        )
        assembly = _make_assembly([joint])
        fmt = AssemblyFormatter()
        text = fmt.format_joint_text(joint, assembly, ansi=False)

        assert "Source File:" in text
        assert "Adapter:" in text
        assert "Catalog:" in text
        assert "Table:" in text
        assert "Fused Group:" in text
        assert "Write Strategy:" in text
        assert "Upstream:" in text
        assert "Tags:" in text
        assert "Description:" in text
        assert "Schema Confidence:" in text
        assert "Schema:" in text
        assert "SQL (original):" in text
        assert "SQL (translated):" in text
        assert "SQL (resolved):" in text
        assert "Checks:" in text
        assert "Optimizations:" in text

    def test_optional_sections_absent_when_empty(self) -> None:
        joint = _make_joint()
        assembly = _make_assembly([joint])
        fmt = AssemblyFormatter()
        text = fmt.format_joint_text(joint, assembly, ansi=False)

        assert "Source File:" not in text
        assert "Adapter:" not in text
        assert "Catalog:" not in text
        assert "Table:" not in text
        assert "Fused group:" not in text
        assert "Write strategy:" not in text
        assert "Upstream:" not in text
        assert "Tags:" not in text
        assert "Description:" not in text


class TestFormatAssemblyTextNoAnsi:
    """Property 11: Text export contains no ANSI escape codes when ansi=False."""

    def test_no_ansi_summary(self) -> None:
        assembly = _make_assembly()
        fmt = AssemblyFormatter()
        text = fmt.format_assembly_text(assembly, verbosity=Verbosity.SUMMARY, ansi=False)
        assert not ANSI_RE.search(text), f"Found ANSI codes in:\n{text}"

    def test_no_ansi_normal(self) -> None:
        assembly = _make_assembly()
        fmt = AssemblyFormatter()
        text = fmt.format_assembly_text(assembly, verbosity=Verbosity.NORMAL, ansi=False)
        assert not ANSI_RE.search(text), f"Found ANSI codes in:\n{text}"

    def test_no_ansi_full(self) -> None:
        assembly = _make_assembly()
        fmt = AssemblyFormatter()
        text = fmt.format_assembly_text(assembly, verbosity=Verbosity.FULL, ansi=False)
        assert not ANSI_RE.search(text), f"Found ANSI codes in:\n{text}"


class TestFormatAssemblyTextAnsi:
    """When ansi=True, output should contain ANSI codes."""

    def test_has_ansi_codes(self) -> None:
        assembly = _make_assembly()
        fmt = AssemblyFormatter()
        text = fmt.format_assembly_text(assembly, verbosity=Verbosity.FULL, ansi=True)
        assert ANSI_RE.search(text), "Expected ANSI codes in output"


class TestFormatAssemblyTextSections:
    """Verify section headers and separators appear in text output."""

    def test_summary_has_overview_and_execution(self) -> None:
        assembly = _make_assembly()
        fmt = AssemblyFormatter()
        text = fmt.format_assembly_text(assembly, verbosity=Verbosity.SUMMARY, ansi=False)
        assert "Assembly Overview" in text
        assert "Execution Order" in text
        assert "═══ Fused Groups ═══" not in text
        assert "═══ Materializations ═══" not in text
        assert "═══ DAG ═══" not in text

    def test_normal_has_fused_and_materializations(self) -> None:
        assembly = _make_assembly()
        fmt = AssemblyFormatter()
        text = fmt.format_assembly_text(assembly, verbosity=Verbosity.NORMAL, ansi=False)
        assert "Assembly Overview" in text
        assert "Execution Order" in text
        assert "Fused Groups" in text
        assert "Materializations" in text
        assert "DAG" not in text

    def test_full_has_all_sections(self) -> None:
        assembly = _make_assembly()
        fmt = AssemblyFormatter()
        text = fmt.format_assembly_text(assembly, verbosity=Verbosity.FULL, ansi=False)
        assert "Assembly Overview" in text
        assert "Execution Order" in text
        assert "Fused Groups" in text
        assert "Materializations" in text
        assert "DAG" in text
        assert "Joint:" in text

    def test_section_separators(self) -> None:
        assembly = _make_assembly()
        fmt = AssemblyFormatter()
        text = fmt.format_assembly_text(assembly, verbosity=Verbosity.NORMAL, ansi=False)
        assert "─" * 60 in text

    def test_overview_content(self) -> None:
        assembly = _make_assembly()
        fmt = AssemblyFormatter()
        text = fmt.format_assembly_text(assembly, verbosity=Verbosity.SUMMARY, ansi=False)
        assert "dev" in text  # profile name
        assert "✓ Success" in text
        assert "3 total" in text
        assert "source: 1" in text
        assert "sql: 1" in text
        assert "sink: 1" in text
        assert "Unused joint: orphan_joint" in text

    def test_sql_in_fused_groups(self) -> None:
        assembly = _make_assembly()
        fmt = AssemblyFormatter()
        text = fmt.format_assembly_text(assembly, verbosity=Verbosity.NORMAL, ansi=False)
        assert "WITH raw_orders" in text

    def test_sql_highlighting_with_ansi(self) -> None:
        assembly = _make_assembly()
        fmt = AssemblyFormatter()
        text = fmt.format_assembly_text(assembly, verbosity=Verbosity.FULL, ansi=True)
        # SQL keywords should be highlighted
        assert "\x1b[34m" in text  # blue for SQL keywords

    def test_materialization_details(self) -> None:
        assembly = _make_assembly()
        fmt = AssemblyFormatter()
        text = fmt.format_assembly_text(assembly, verbosity=Verbosity.NORMAL, ansi=False)
        assert "engine_instance_change" in text
        assert "cleaned_orders → output_orders" in text
        assert "arrow" in text

    def test_dag_rendering(self) -> None:
        assembly = _make_assembly()
        fmt = AssemblyFormatter()
        text = fmt.format_assembly_text(assembly, verbosity=Verbosity.FULL, ansi=False)
        assert "⚪" in text  # source icon
        assert "🔵" in text  # sql icon
        assert "🟢" in text  # sink icon
        assert "raw_orders" in text
        assert "cleaned_orders" in text
        assert "output_orders" in text


class TestFormatAssemblyTextFilter:
    """Verify filter info appears in text output."""

    def test_filter_shown_in_text(self) -> None:
        assembly = _make_assembly()
        fmt = AssemblyFormatter()
        text = fmt.format_assembly_text(
            assembly,
            verbosity=Verbosity.SUMMARY,
            filter=InspectFilter(engine="duckdb_main"),
            ansi=False,
        )
        assert "Filter" in text
        assert "engine=duckdb_main" in text


class TestFormatAssemblyTextEmpty:
    """Edge case: empty assembly."""

    def test_empty_assembly(self) -> None:
        assembly = _make_assembly(
            joints=[], fused_groups=[], materializations=[], execution_order=[]
        )
        fmt = AssemblyFormatter()
        text = fmt.format_assembly_text(assembly, verbosity=Verbosity.FULL, ansi=False)
        assert "Assembly Overview" in text
        assert "0 total" in text
        assert not ANSI_RE.search(text)



class TestSchemaConfidenceDisplay:
    """Task 7.1: schema_confidence display in joint inspection."""

    def test_schema_confidence_shown_in_joint_text(self) -> None:
        joint = _make_joint(schema_confidence="introspected")
        assembly = _make_assembly([joint])
        fmt = AssemblyFormatter()
        text = fmt.format_joint_text(joint, assembly, ansi=False)
        assert "Schema Confidence: introspected" in text

    def test_schema_confidence_none_shown(self) -> None:
        joint = _make_joint(schema_confidence="none")
        assembly = _make_assembly([joint])
        fmt = AssemblyFormatter()
        text = fmt.format_joint_text(joint, assembly, ansi=False)
        assert "Schema Confidence: none" in text

    def test_schema_gated_by_confidence_introspected(self) -> None:
        schema = Schema(columns=[Column(name="id", type="int64", nullable=False)])
        joint = _make_joint(output_schema=schema, schema_confidence="introspected")
        assembly = _make_assembly([joint])
        fmt = AssemblyFormatter()
        text = fmt.format_joint_text(joint, assembly, ansi=False)
        assert "Schema:" in text
        assert "id: int64" in text

    def test_schema_gated_by_confidence_inferred(self) -> None:
        schema = Schema(columns=[Column(name="name", type="varchar", nullable=True)])
        joint = _make_joint(output_schema=schema, schema_confidence="inferred")
        assembly = _make_assembly([joint])
        fmt = AssemblyFormatter()
        text = fmt.format_joint_text(joint, assembly, ansi=False)
        assert "Schema:" in text
        assert "name: varchar" in text

    def test_schema_hidden_when_confidence_none(self) -> None:
        schema = Schema(columns=[Column(name="id", type="int64", nullable=False)])
        joint = _make_joint(output_schema=schema, schema_confidence="none")
        assembly = _make_assembly([joint])
        fmt = AssemblyFormatter()
        text = fmt.format_joint_text(joint, assembly, ansi=False)
        assert "  Schema:" not in text

    def test_schema_hidden_when_confidence_partial(self) -> None:
        schema = Schema(columns=[Column(name="id", type="int64", nullable=False)])
        joint = _make_joint(output_schema=schema, schema_confidence="partial")
        assembly = _make_assembly([joint])
        fmt = AssemblyFormatter()
        text = fmt.format_joint_text(joint, assembly, ansi=False)
        assert "  Schema:" not in text

    def test_schema_confidence_in_format_joint_struct(self) -> None:
        joint = _make_joint(schema_confidence="inferred")
        assembly = _make_assembly([joint])
        fmt = AssemblyFormatter()
        result = fmt.format_joint(joint, assembly)
        assert result.schema_confidence == "inferred"


class TestSourceStatsDisplay:
    """Task 7.1: source_stats display in verbose joint inspection."""

    def test_source_stats_shown_when_present(self) -> None:
        from datetime import datetime

        stats = SourceStats(
            row_count=1000, size_bytes=2048, last_modified=datetime(2026, 1, 15), partition_count=4
        )
        joint = _make_joint(type="source", source_stats=stats)
        assembly = _make_assembly([joint])
        fmt = AssemblyFormatter()
        text = fmt.format_joint_text(joint, assembly, ansi=False)
        assert "Source Stats:" in text
        assert "Row Count:" in text
        assert "1,000" in text
        assert "Size:" in text
        assert "2,048 bytes" in text
        assert "Last Modified:" in text
        assert "Partitions:" in text
        assert "4" in text

    def test_source_stats_not_shown_when_none(self) -> None:
        joint = _make_joint(type="source")
        assembly = _make_assembly([joint])
        fmt = AssemblyFormatter()
        text = fmt.format_joint_text(joint, assembly, ansi=False)
        assert "Source Stats:" not in text

    def test_source_stats_partial_fields(self) -> None:
        stats = SourceStats(row_count=500)
        joint = _make_joint(type="source", source_stats=stats)
        assembly = _make_assembly([joint])
        fmt = AssemblyFormatter()
        text = fmt.format_joint_text(joint, assembly, ansi=False)
        assert "Source Stats:" in text
        assert "Row Count:" in text
        assert "500" in text
        assert "Size:" not in text
        assert "Last Modified:" not in text
        assert "Partitions:" not in text

    def test_source_stats_in_format_joint_struct(self) -> None:
        stats = SourceStats(row_count=100)
        joint = _make_joint(type="source", source_stats=stats)
        assembly = _make_assembly([joint])
        fmt = AssemblyFormatter()
        result = fmt.format_joint(joint, assembly)
        assert result.source_stats is not None
        assert result.source_stats.row_count == 100


class TestCompilationStatsDisplay:
    """Task 7.1: compilation_stats display in assembly overview."""

    def test_compilation_stats_shown_in_overview(self) -> None:
        cs = CompilationStats(
            compile_duration_ms=340,
            joints_with_schema=8,
            joints_total=10,
            introspection_attempted=6,
            introspection_succeeded=4,
            introspection_failed=1,
            introspection_skipped=1,
        )
        assembly = _make_assembly()
        # Replace with compilation_stats
        from dataclasses import replace

        assembly = replace(assembly, compilation_stats=cs)
        fmt = AssemblyFormatter()
        text = fmt.format_assembly_text(assembly, verbosity=Verbosity.SUMMARY, ansi=False)
        assert "340ms" in text
        assert "8/10 schemas" in text
        assert "4 ok" in text
        assert "1 failed" in text
        assert "1 skipped" in text

    def test_no_compilation_stats_no_crash(self) -> None:
        assembly = _make_assembly()
        fmt = AssemblyFormatter()
        text = fmt.format_assembly_text(assembly, verbosity=Verbosity.SUMMARY, ansi=False)
        assert "Compilation:" not in text


class TestDagSchemaAnnotations:
    """Task 7.1: schema annotations in DAG gated by confidence."""

    def test_dag_shows_schema_annotation_when_introspected(self) -> None:
        schema = Schema(columns=[Column(name="id", type="int64", nullable=False), Column(name="name", type="varchar", nullable=True)])
        joint = _make_joint(
            name="src",
            type="source",
            output_schema=schema,
            schema_confidence="introspected",
        )
        assembly = _make_assembly(
            joints=[joint], fused_groups=[], materializations=[], execution_order=["src"]
        )
        fmt = AssemblyFormatter()
        text = fmt.format_assembly_text(assembly, verbosity=Verbosity.FULL, ansi=False)
        assert "(2 cols)" in text

    def test_dag_no_schema_annotation_when_none(self) -> None:
        schema = Schema(columns=[Column(name="id", type="int64", nullable=False)])
        joint = _make_joint(
            name="src",
            type="source",
            output_schema=schema,
            schema_confidence="none",
        )
        assembly = _make_assembly(
            joints=[joint], fused_groups=[], materializations=[], execution_order=["src"]
        )
        fmt = AssemblyFormatter()
        text = fmt.format_assembly_text(assembly, verbosity=Verbosity.FULL, ansi=False)
        assert "(1 cols)" not in text
