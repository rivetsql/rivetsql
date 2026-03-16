"""Unit tests for DatabricksReferenceResolver.

Verifies that source joints with SQL (inline transforms) are NOT treated
as CTE siblings, so their references get rewritten to fully-qualified
table names for server-side execution.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from rivet_databricks.engine import DatabricksReferenceResolver


def _joint(
    name: str,
    *,
    type: str = "transform",
    upstream: list[str] | None = None,
    sql: str | None = None,
    sql_translated: str | None = None,
    catalog: str | None = None,
    table: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        type=type,
        upstream=upstream or [],
        sql=sql,
        sql_translated=sql_translated,
        catalog=catalog,
        table=table,
    )


def _catalog(catalog_name: str, schema: str = "default") -> SimpleNamespace:
    return SimpleNamespace(options={"catalog": catalog_name, "schema": schema})


class TestDatabricksReferenceResolver:
    """Tests for DatabricksReferenceResolver.resolve_references."""

    def setup_method(self) -> None:
        self.resolver = DatabricksReferenceResolver()

    def test_source_with_sql_not_treated_as_cte_sibling(self) -> None:
        """Source joints with SQL (inline transforms) must be resolved to FQ names,
        not skipped as CTE siblings."""
        source = _joint(
            "my_source",
            type="source",
            upstream=[],
            sql="select * from my_source",
            catalog="db_cat",
            table="prod.my_source",
        )
        query = _joint(
            "my_query",
            type="transform",
            upstream=["my_source"],
            sql="select * from my_source limit 10",
        )
        compiled: dict[str, Any] = {"my_source": source, "my_query": query}
        catalog_map: dict[str, Any] = {"db_cat": _catalog("unity_catalog", "prod")}
        fused = ["my_source", "my_query"]

        result = self.resolver.resolve_references(
            sql="select * from my_source limit 10",
            joint=query,
            catalog=None,
            compiled_joints=compiled,
            catalog_map=catalog_map,
            fused_group_joints=fused,
        )

        assert result is not None
        assert "unity_catalog.prod.my_source" in result

    def test_transform_with_sql_is_cte_sibling(self) -> None:
        """Transform joints with SQL in the same fused group ARE CTE siblings
        and should NOT be rewritten."""
        source = _joint(
            "raw_data",
            type="source",
            upstream=[],
            catalog="db_cat",
            table="raw_data",
        )
        transform = _joint(
            "clean_data",
            type="transform",
            upstream=["raw_data"],
            sql="select id from raw_data",
        )
        final = _joint(
            "final",
            type="transform",
            upstream=["clean_data"],
            sql="select * from clean_data",
        )
        compiled: dict[str, Any] = {
            "raw_data": source,
            "clean_data": transform,
            "final": final,
        }
        catalog_map: dict[str, Any] = {"db_cat": _catalog("unity", "silver")}
        fused = ["raw_data", "clean_data", "final"]

        result = self.resolver.resolve_references(
            sql="select * from clean_data",
            joint=final,
            catalog=None,
            compiled_joints=compiled,
            catalog_map=catalog_map,
            fused_group_joints=fused,
        )

        # clean_data is a transform with SQL → CTE sibling → not rewritten
        assert result is None

    def test_source_without_sql_resolved_to_fq_name(self) -> None:
        """Source joints without SQL should also be resolved to FQ names."""
        source = _joint(
            "customers",
            type="source",
            upstream=[],
            catalog="db_cat",
            table="customers",
        )
        query = _joint(
            "q",
            type="transform",
            upstream=["customers"],
            sql="select * from customers",
        )
        compiled: dict[str, Any] = {"customers": source, "q": query}
        catalog_map: dict[str, Any] = {"db_cat": _catalog("main", "default")}

        result = self.resolver.resolve_references(
            sql="select * from customers",
            joint=query,
            catalog=None,
            compiled_joints=compiled,
            catalog_map=catalog_map,
            fused_group_joints=["customers", "q"],
        )

        assert result is not None
        assert "main.default.customers" in result

    def test_two_part_table_name_gets_catalog_prefix(self) -> None:
        """A source with schema.table gets only the catalog prepended."""
        source = _joint(
            "orders",
            type="source",
            upstream=[],
            catalog="db_cat",
            table="silver.orders",
        )
        query = _joint(
            "q",
            type="transform",
            upstream=["orders"],
            sql="select * from orders",
        )
        compiled: dict[str, Any] = {"orders": source, "q": query}
        catalog_map: dict[str, Any] = {"db_cat": _catalog("hive_metastore", "default")}

        result = self.resolver.resolve_references(
            sql="select * from orders",
            joint=query,
            catalog=None,
            compiled_joints=compiled,
            catalog_map=catalog_map,
            fused_group_joints=["orders", "q"],
        )

        assert result == "select * from hive_metastore.silver.orders"

    def test_three_part_table_name_unchanged(self) -> None:
        """A source with catalog.schema.table is used as-is."""
        source = _joint(
            "items",
            type="source",
            upstream=[],
            catalog="db_cat",
            table="hive.silver.items",
        )
        query = _joint(
            "q",
            type="transform",
            upstream=["items"],
            sql="select * from items",
        )
        compiled: dict[str, Any] = {"items": source, "q": query}
        catalog_map: dict[str, Any] = {"db_cat": _catalog("hive", "silver")}

        result = self.resolver.resolve_references(
            sql="select * from items",
            joint=query,
            catalog=None,
            compiled_joints=compiled,
            catalog_map=catalog_map,
            fused_group_joints=["items", "q"],
        )

        assert result == "select * from hive.silver.items"

    def test_no_upstream_returns_none(self) -> None:
        """Joint with no upstream returns None (nothing to resolve)."""
        joint = _joint("lonely", type="transform", upstream=[])
        result = self.resolver.resolve_references(
            sql="select 1",
            joint=joint,
            catalog=None,
            compiled_joints={},
            catalog_map={},
        )
        assert result is None

    def test_no_compiled_joints_returns_none(self) -> None:
        """Missing compiled_joints returns None."""
        joint = _joint("q", type="transform", upstream=["src"])
        result = self.resolver.resolve_references(
            sql="select * from src",
            joint=joint,
            catalog=None,
            compiled_joints=None,
            catalog_map=None,
        )
        assert result is None
