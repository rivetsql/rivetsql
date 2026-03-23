"""Unit tests for checkpoint CTE injection helpers.

Tests _build_checkpoint_fq_name, _prepend_ctes, and _inject_checkpoint_ctes
from rivet_core.compiler.
"""

from __future__ import annotations

from rivet_core.compiler import (
    CompiledJoint,
    _build_checkpoint_fq_name,
    _inject_checkpoint_ctes,
    _prepend_ctes,
)
from rivet_core.models import Catalog
from rivet_core.optimizer import CheckpointSourceInfo, FusedGroup, FusionResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _checkpoint_cj(
    name: str = "bu_prep",
    catalog: str | None = "unity",
    catalog_type: str | None = "unity",
    table: str | None = "rvt_bu_prep",
) -> CompiledJoint:
    """Minimal CompiledJoint for checkpoint CTE injection tests."""
    return CompiledJoint(
        name=name,
        type="checkpoint",
        catalog=catalog,
        catalog_type=catalog_type,
        engine="databricks",
        engine_resolution="project_default",
        adapter="databricks:unity",
        sql="SELECT * FROM upstream",
        sql_translated=None,
        sql_resolved=None,
        sql_dialect=None,
        engine_dialect=None,
        upstream=["upstream_joint"],
        eager=False,
        table=table,
        write_strategy="replace",
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


def _catalog(
    name: str = "unity",
    type: str = "unity",
    options: dict | None = None,
) -> Catalog:
    return Catalog(name=name, type=type, options=options or {})


def _fused_group(
    group_id: str = "group-1",
    joints: list[str] | None = None,
    fused_sql: str | None = None,
    resolved_sql: str | None = None,
    fusion_result: FusionResult | None = None,
    checkpoint_sources: dict[str, CheckpointSourceInfo] | None = None,
) -> FusedGroup:
    return FusedGroup(
        id=group_id,
        joints=joints or ["sql_joint_1", "sink_1"],
        engine="databricks",
        engine_type="databricks",
        adapters={"sql_joint_1": None},
        fused_sql=fused_sql,
        fusion_result=fusion_result,
        resolved_sql=resolved_sql,
        checkpoint_sources=checkpoint_sources or {},
    )


# ===================================================================
# Task 2.1: _build_checkpoint_fq_name
# ===================================================================


class TestBuildCheckpointFqName:
    """Tests for _build_checkpoint_fq_name with various catalog configurations."""

    def test_unity_catalog_bare_table_produces_3_part_name(self) -> None:
        """catalog_name + schema + bare table → 3-part FQ name."""
        cj = _checkpoint_cj(table="rvt_bu_prep")
        cat = _catalog(options={"catalog_name": "datalake_gold", "schema": "stock"})
        assert _build_checkpoint_fq_name(cj, cat) == "datalake_gold.stock.rvt_bu_prep"

    def test_already_qualified_3_part_table_passes_through(self) -> None:
        """3-part table like cat.sch.tbl → returned as-is."""
        cj = _checkpoint_cj(table="cat.sch.tbl")
        cat = _catalog(options={"catalog_name": "datalake_gold", "schema": "stock"})
        assert _build_checkpoint_fq_name(cj, cat) == "cat.sch.tbl"

    def test_2_part_table_prepends_catalog(self) -> None:
        """2-part table like schema.table → catalog prepended."""
        cj = _checkpoint_cj(table="stock.rvt_bu_prep")
        cat = _catalog(options={"catalog_name": "datalake_gold", "schema": "default"})
        assert _build_checkpoint_fq_name(cj, cat) == "datalake_gold.stock.rvt_bu_prep"

    def test_missing_catalog_none_returns_raw_table(self) -> None:
        """catalog=None → return raw table name."""
        cj = _checkpoint_cj(table="rvt_bu_prep")
        assert _build_checkpoint_fq_name(cj, None) == "rvt_bu_prep"

    def test_missing_catalog_options_returns_raw_table(self) -> None:
        """Catalog with no catalog_name/catalog key → return raw table name."""
        cj = _checkpoint_cj(table="rvt_bu_prep")
        cat = _catalog(options={"schema": "stock"})
        assert _build_checkpoint_fq_name(cj, cat) == "rvt_bu_prep"

    def test_no_table_field_falls_back_to_cj_name(self) -> None:
        """Checkpoint with table=None → uses cj.name as table."""
        cj = _checkpoint_cj(name="bu_prep", table=None)
        cat = _catalog(options={"catalog_name": "datalake_gold", "schema": "stock"})
        assert _build_checkpoint_fq_name(cj, cat) == "datalake_gold.stock.bu_prep"

    def test_catalog_key_instead_of_catalog_name(self) -> None:
        """Catalog with 'catalog' key instead of 'catalog_name' → still works."""
        cj = _checkpoint_cj(table="rvt_bu_prep")
        cat = _catalog(options={"catalog": "datalake_gold", "schema": "stock"})
        assert _build_checkpoint_fq_name(cj, cat) == "datalake_gold.stock.rvt_bu_prep"

    def test_default_schema_when_not_specified(self) -> None:
        """Catalog with catalog_name but no schema → uses 'default' schema."""
        cj = _checkpoint_cj(table="rvt_bu_prep")
        cat = _catalog(options={"catalog_name": "datalake_gold"})
        assert _build_checkpoint_fq_name(cj, cat) == "datalake_gold.default.rvt_bu_prep"


# ===================================================================
# Task 2.2: _prepend_ctes helper
# ===================================================================


class TestPrependCtes:
    """Tests for _prepend_ctes helper function."""

    def test_sql_with_existing_with_clause_prepends_ctes(self) -> None:
        """SQL with existing WITH clause → checkpoint CTEs prepended before existing."""
        sql = "WITH existing AS (\n    SELECT 1\n)\nSELECT * FROM existing"
        cte_parts = ["bu_prep AS (\n    SELECT * FROM datalake_gold.stock.rvt_bu_prep\n)"]
        result = _prepend_ctes(sql, cte_parts)
        expected = (
            "WITH bu_prep AS (\n    SELECT * FROM datalake_gold.stock.rvt_bu_prep\n),\n"
            "existing AS (\n    SELECT 1\n)\nSELECT * FROM existing"
        )
        assert result == expected

    def test_sql_without_with_clause_creates_new_with(self) -> None:
        """SQL without WITH clause → new WITH clause created."""
        sql = "SELECT * FROM some_table"
        cte_parts = ["bu_prep AS (\n    SELECT * FROM datalake_gold.stock.rvt_bu_prep\n)"]
        result = _prepend_ctes(sql, cte_parts)
        expected = (
            "WITH bu_prep AS (\n    SELECT * FROM datalake_gold.stock.rvt_bu_prep\n)\n"
            "SELECT * FROM some_table"
        )
        assert result == expected

    def test_multiple_cte_parts_joined_with_commas(self) -> None:
        """Multiple CTE parts → all joined with commas."""
        sql = "SELECT * FROM some_table"
        cte_parts = [
            "bu_prep AS (\n    SELECT * FROM rvt_bu_prep\n)",
            "product AS (\n    SELECT * FROM rvt_product\n)",
        ]
        result = _prepend_ctes(sql, cte_parts)
        expected = (
            "WITH bu_prep AS (\n    SELECT * FROM rvt_bu_prep\n),\n"
            "product AS (\n    SELECT * FROM rvt_product\n)\n"
            "SELECT * FROM some_table"
        )
        assert result == expected


# ===================================================================
# Task 2.2: _inject_checkpoint_ctes
# ===================================================================


class TestInjectCheckpointCtes:
    """Tests for _inject_checkpoint_ctes with various group configurations."""

    def test_group_with_one_checkpoint_dep_cte_prepended(self) -> None:
        """Group with one checkpoint dep → CTE prepended to fused_sql.

        Without a registry/resolver, the fallback uses the joint name
        (the engine receives data via input_tables keyed by joint name).
        """
        cj_map = {"bu_prep": _checkpoint_cj(name="bu_prep", table="rvt_bu_prep")}
        catalog_map = {
            "unity": _catalog(options={"catalog_name": "datalake_gold", "schema": "stock"})
        }
        checkpoint_sources = {
            "bu_prep": CheckpointSourceInfo(
                checkpoint_joint="bu_prep",
                catalog="unity",
                catalog_type="unity",
                table="rvt_bu_prep",
                adapter="databricks:unity",
            )
        }
        group = _fused_group(
            fused_sql="SELECT * FROM bu_prep",
            checkpoint_sources=checkpoint_sources,
        )

        result = _inject_checkpoint_ctes([group], cj_map, catalog_map)

        assert len(result) == 1
        assert "bu_prep AS (\n    SELECT * FROM bu_prep\n)" in result[0].fused_sql

    def test_group_with_multiple_checkpoint_deps_all_prepended(self) -> None:
        """Group with multiple checkpoint deps → all CTEs prepended in order."""
        cj_map = {
            "bu_prep": _checkpoint_cj(name="bu_prep", table="rvt_bu_prep"),
            "product": _checkpoint_cj(name="product", table="rvt_product"),
        }
        catalog_map = {
            "unity": _catalog(options={"catalog_name": "datalake_gold", "schema": "stock"})
        }
        checkpoint_sources = {
            "bu_prep": CheckpointSourceInfo(
                checkpoint_joint="bu_prep",
                catalog="unity",
                catalog_type="unity",
                table="rvt_bu_prep",
                adapter="databricks:unity",
            ),
            "product": CheckpointSourceInfo(
                checkpoint_joint="product",
                catalog="unity",
                catalog_type="unity",
                table="rvt_product",
                adapter="databricks:unity",
            ),
        }
        group = _fused_group(
            fused_sql="SELECT * FROM bu_prep JOIN product",
            checkpoint_sources=checkpoint_sources,
        )

        result = _inject_checkpoint_ctes([group], cj_map, catalog_map)

        assert len(result) == 1
        fused = result[0].fused_sql
        assert "bu_prep AS (\n    SELECT * FROM bu_prep\n)" in fused
        assert "product AS (\n    SELECT * FROM product\n)" in fused

        # bu_prep should appear before product (dict insertion order)
        assert fused.index("bu_prep AS") < fused.index("product AS")

    def test_group_with_no_checkpoint_deps_unchanged(self) -> None:
        """Group with no checkpoint deps → unchanged."""
        group = _fused_group(fused_sql="SELECT * FROM some_table")

        result = _inject_checkpoint_ctes([group], {}, {})

        assert len(result) == 1
        assert result[0].fused_sql == "SELECT * FROM some_table"

    def test_group_with_existing_with_clause_checkpoint_ctes_inserted_before(self) -> None:
        """Group with existing WITH clause → checkpoint CTEs inserted before existing CTEs."""
        cj_map = {"bu_prep": _checkpoint_cj(name="bu_prep", table="rvt_bu_prep")}
        catalog_map = {
            "unity": _catalog(options={"catalog_name": "datalake_gold", "schema": "stock"})
        }
        checkpoint_sources = {
            "bu_prep": CheckpointSourceInfo(
                checkpoint_joint="bu_prep",
                catalog="unity",
                catalog_type="unity",
                table="rvt_bu_prep",
                adapter="databricks:unity",
            )
        }
        fused_sql = (
            "WITH sql_joint_1 AS (\n    SELECT * FROM some_table\n)\nSELECT * FROM sql_joint_1"
        )
        group = _fused_group(
            fused_sql=fused_sql,
            checkpoint_sources=checkpoint_sources,
        )

        result = _inject_checkpoint_ctes([group], cj_map, catalog_map)

        fused = result[0].fused_sql
        # Checkpoint CTE should appear before existing CTE
        assert fused.index("bu_prep AS") < fused.index("sql_joint_1 AS")
        # Existing CTE content preserved
        assert "SELECT * FROM some_table" in fused
        assert "SELECT * FROM sql_joint_1" in fused

    def test_group_without_with_clause_creates_new_with(self) -> None:
        """Group with no WITH clause (single-joint) → WITH clause created."""
        cj_map = {"bu_prep": _checkpoint_cj(name="bu_prep", table="rvt_bu_prep")}
        catalog_map = {
            "unity": _catalog(options={"catalog_name": "datalake_gold", "schema": "stock"})
        }
        checkpoint_sources = {
            "bu_prep": CheckpointSourceInfo(
                checkpoint_joint="bu_prep",
                catalog="unity",
                catalog_type="unity",
                table="rvt_bu_prep",
                adapter="databricks:unity",
            )
        }
        group = _fused_group(
            fused_sql="SELECT * FROM bu_prep",
            checkpoint_sources=checkpoint_sources,
        )

        result = _inject_checkpoint_ctes([group], cj_map, catalog_map)

        fused = result[0].fused_sql
        assert fused.startswith("WITH ")
        assert "bu_prep AS (\n    SELECT * FROM bu_prep\n)" in fused
        assert "SELECT * FROM bu_prep" in fused

    def test_existing_ctes_and_final_select_preserved(self) -> None:
        """Verify existing CTEs and final SELECT are preserved and unmodified."""
        cj_map = {"bu_prep": _checkpoint_cj(name="bu_prep", table="rvt_bu_prep")}
        catalog_map = {
            "unity": _catalog(options={"catalog_name": "datalake_gold", "schema": "stock"})
        }
        checkpoint_sources = {
            "bu_prep": CheckpointSourceInfo(
                checkpoint_joint="bu_prep",
                catalog="unity",
                catalog_type="unity",
                table="rvt_bu_prep",
                adapter="databricks:unity",
            )
        }
        original_cte_body = "SELECT id, name FROM raw_data WHERE active = 1"
        final_select = "SELECT * FROM sql_joint_1 WHERE region = 'EU'"
        fused_sql = f"WITH sql_joint_1 AS (\n    {original_cte_body}\n)\n{final_select}"
        group = _fused_group(
            fused_sql=fused_sql,
            checkpoint_sources=checkpoint_sources,
        )

        result = _inject_checkpoint_ctes([group], cj_map, catalog_map)

        fused = result[0].fused_sql
        assert original_cte_body in fused
        assert final_select in fused

    def test_resolved_sql_also_updated(self) -> None:
        """resolved_sql is also updated when present."""
        cj_map = {"bu_prep": _checkpoint_cj(name="bu_prep", table="rvt_bu_prep")}
        catalog_map = {
            "unity": _catalog(options={"catalog_name": "datalake_gold", "schema": "stock"})
        }
        checkpoint_sources = {
            "bu_prep": CheckpointSourceInfo(
                checkpoint_joint="bu_prep",
                catalog="unity",
                catalog_type="unity",
                table="rvt_bu_prep",
                adapter="databricks:unity",
            )
        }
        group = _fused_group(
            fused_sql="SELECT * FROM bu_prep",
            resolved_sql="SELECT * FROM bu_prep",
            checkpoint_sources=checkpoint_sources,
        )

        result = _inject_checkpoint_ctes([group], cj_map, catalog_map)

        assert result[0].resolved_sql is not None
        assert "bu_prep AS (\n    SELECT * FROM bu_prep\n)" in result[0].resolved_sql

    def test_fusion_result_fields_updated(self) -> None:
        """fusion_result fields (fused_sql, statements, resolved_statements) are updated."""
        cj_map = {"bu_prep": _checkpoint_cj(name="bu_prep", table="rvt_bu_prep")}
        catalog_map = {
            "unity": _catalog(options={"catalog_name": "datalake_gold", "schema": "stock"})
        }
        checkpoint_sources = {
            "bu_prep": CheckpointSourceInfo(
                checkpoint_joint="bu_prep",
                catalog="unity",
                catalog_type="unity",
                table="rvt_bu_prep",
                adapter="databricks:unity",
            )
        }
        fusion_result = FusionResult(
            fused_sql="WITH sql_joint_1 AS (\n    SELECT * FROM bu_prep\n)\nSELECT * FROM sql_joint_1",
            statements=["sql_joint_1 AS (\n    SELECT * FROM bu_prep\n)"],
            final_select="SELECT * FROM sql_joint_1",
        )
        group = _fused_group(
            fused_sql="WITH sql_joint_1 AS (\n    SELECT * FROM bu_prep\n)\nSELECT * FROM sql_joint_1",
            fusion_result=fusion_result,
            checkpoint_sources=checkpoint_sources,
        )

        result = _inject_checkpoint_ctes([group], cj_map, catalog_map)

        fr = result[0].fusion_result
        assert fr is not None
        # fusion_result.fused_sql updated
        assert "bu_prep AS (\n    SELECT * FROM bu_prep\n)" in fr.fused_sql
        # statements prepended
        assert len(fr.statements) == 2
        assert "bu_prep AS" in fr.statements[0]
        assert "sql_joint_1 AS" in fr.statements[1]
        # final_select unchanged
        assert fr.final_select == "SELECT * FROM sql_joint_1"

    def test_fusion_result_resolved_statements_updated(self) -> None:
        """fusion_result resolved_statements are updated when present."""
        cj_map = {"bu_prep": _checkpoint_cj(name="bu_prep", table="rvt_bu_prep")}
        catalog_map = {
            "unity": _catalog(options={"catalog_name": "datalake_gold", "schema": "stock"})
        }
        checkpoint_sources = {
            "bu_prep": CheckpointSourceInfo(
                checkpoint_joint="bu_prep",
                catalog="unity",
                catalog_type="unity",
                table="rvt_bu_prep",
                adapter="databricks:unity",
            )
        }
        fusion_result = FusionResult(
            fused_sql="WITH j1 AS (\n    SELECT 1\n)\nSELECT * FROM j1",
            statements=["j1 AS (\n    SELECT 1\n)"],
            final_select="SELECT * FROM j1",
            resolved_fused_sql="WITH j1 AS (\n    SELECT 1\n)\nSELECT * FROM j1",
            resolved_statements=["j1 AS (\n    SELECT 1\n)"],
        )
        group = _fused_group(
            fused_sql="WITH j1 AS (\n    SELECT 1\n)\nSELECT * FROM j1",
            fusion_result=fusion_result,
            checkpoint_sources=checkpoint_sources,
        )

        result = _inject_checkpoint_ctes([group], cj_map, catalog_map)

        fr = result[0].fusion_result
        assert fr is not None
        assert fr.resolved_statements is not None
        assert len(fr.resolved_statements) == 2
        assert "bu_prep AS" in fr.resolved_statements[0]
        assert fr.resolved_fused_sql is not None
        assert "bu_prep AS" in fr.resolved_fused_sql
