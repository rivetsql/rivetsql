"""Built-in Arrow catalog, compute engine, source, and sink plugins."""

from __future__ import annotations

from typing import Any

import pyarrow

from rivet_core.errors import PluginValidationError, RivetError
from rivet_core.introspection import (
    CatalogNode,
    ColumnDetail,
    NodeSummary,
    ObjectMetadata,
    ObjectSchema,
)
from rivet_core.models import Catalog, ComputeEngine, Joint, Material
from rivet_core.plugins import CatalogPlugin, ComputeEnginePlugin, SinkPlugin, SourcePlugin
from rivet_core.strategies import ArrowMaterialization, MaterializationContext


def _arrow_type_name(t: pyarrow.DataType) -> str:
    return str(t)


# Shared in-memory table store — lazily created via _get_shared_store()
_shared_store: dict[tuple[str, str], pyarrow.Table] | None = None


def _get_shared_store() -> dict[tuple[str, str], pyarrow.Table]:
    global _shared_store  # noqa: PLW0603
    if _shared_store is None:
        _shared_store = {}
    return _shared_store


# ── ArrowCatalogPlugin ───────────────────────────────────────────────


class ArrowCatalogPlugin(CatalogPlugin):
    type = "arrow"
    required_options: list[str] = []
    optional_options: dict[str, Any] = {
        "memory_limit": None,
        "spill_to_disk": False,
        "spill_path": None,
    }
    credential_options: list[str] = []

    def __init__(self) -> None:
        self._tables: dict[tuple[str, str], pyarrow.Table] = _get_shared_store()

    def validate(self, options: dict[str, Any]) -> None:
        for key in options:
            if key not in self.optional_options and key != "table_map":
                raise PluginValidationError(
                    RivetError(
                        code="RVT-201",
                        message=f"Unknown option '{key}' for arrow catalog.",
                        context={"plugin": "arrow"},
                        remediation=f"Valid options: {', '.join(self.optional_options)}",
                    )
                )

    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog:
        self.validate(options)
        return Catalog(name=name, type="arrow", options=options)

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        return logical_name

    # ── Introspection ─────────────────────────────────────────────

    def list_tables(self, catalog: Catalog) -> list[CatalogNode]:
        nodes: list[CatalogNode] = []
        for (cat_name, table_name), table in self._tables.items():
            if cat_name != catalog.name:
                continue
            nodes.append(
                CatalogNode(
                    name=table_name,
                    node_type="table",
                    path=[catalog.name, table_name],
                    is_container=False,
                    children_count=None,
                    summary=NodeSummary(
                        row_count=table.num_rows,
                        size_bytes=table.nbytes,
                        format="arrow",
                        last_modified=None,
                        owner=None,
                        comment=None,
                    ),
                )
            )
        return nodes

    def get_schema(self, catalog: Catalog, table: str) -> ObjectSchema:
        t = self._tables.get((catalog.name, table))
        if t is None:
            raise NotImplementedError(f"Table '{table}' not found in arrow catalog '{catalog.name}'.")
        columns = [
            ColumnDetail(
                name=field.name,
                type=_arrow_type_name(field.type),
                native_type=_arrow_type_name(field.type),
                nullable=field.nullable,
                default=None,
                comment=None,
                is_primary_key=False,
                is_partition_key=False,
            )
            for field in t.schema
        ]
        return ObjectSchema(
            path=[catalog.name, table],
            node_type="table",
            columns=columns,
            primary_key=None,
            comment=None,
        )

    def get_metadata(self, catalog: Catalog, table: str) -> ObjectMetadata | None:
        t = self._tables.get((catalog.name, table))
        if t is None:
            return None
        return ObjectMetadata(
            path=[catalog.name, table],
            node_type="table",
            row_count=t.num_rows,
            size_bytes=t.nbytes,
            last_modified=None,
            created_at=None,
            format="arrow",
            compression=None,
            owner=None,
            comment=None,
            location=None,
            column_statistics=[],
            partitioning=None,
            properties={},
        )


# ── ArrowComputeEnginePlugin ─────────────────────────────────────────


class ArrowComputeEnginePlugin(ComputeEnginePlugin):
    engine_type = "arrow"
    supported_catalog_types: dict[str, list[str]] = {
        "arrow": [
            "projection_pushdown",
            "predicate_pushdown",
            "limit_pushdown",
            "cast_pushdown",
        ],
        "filesystem": [
            "projection_pushdown",
            "predicate_pushdown",
            "limit_pushdown",
        ],
    }

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type="arrow", config=config)

    def validate(self, options: dict[str, Any]) -> None:
        pass  # Arrow engine has no required options

    def execute_sql(
        self,
        engine: ComputeEngine,
        sql: str,
        input_tables: dict[str, pyarrow.Table],
    ) -> pyarrow.Table:
        """Execute SQL via DuckDB in-memory (same as current Arrow engine behavior)."""
        import duckdb

        conn = duckdb.connect()
        try:
            for name, table in input_tables.items():
                conn.register(name, table)
            return conn.execute(sql).fetch_arrow_table()
        finally:
            conn.close()


# ── ArrowSource ──────────────────────────────────────────────────────


class ArrowSource(SourcePlugin):
    catalog_type = "arrow"

    def __init__(self, table_store: dict[tuple[str, str], pyarrow.Table] | None = None) -> None:
        self._tables = table_store if table_store is not None else _get_shared_store()

    def read(self, catalog: Catalog, joint: Joint, pushdown: Any | None = None) -> Material:
        table_name = joint.path or joint.name
        table = self._tables.get((catalog.name, table_name))
        if table is None:
            raise KeyError(f"Table '{table_name}' not found in arrow catalog '{catalog.name}'.")

        # Apply pushdown operations
        if pushdown is not None:
            table = _apply_pushdown(table, pushdown)

        ref = ArrowMaterialization().materialize(
            table, MaterializationContext(joint_name=joint.name, strategy_name="arrow", options={})
        )
        schema = {field.name: _arrow_type_name(field.type) for field in table.schema}
        return Material(
            name=joint.name,
            catalog=catalog.name,
            table=table_name,
            schema=schema,
            state="materialized",
            materialized_ref=ref,
        )


def _apply_pushdown(table: pyarrow.Table, pushdown: Any) -> pyarrow.Table:
    """Apply pushdown operations to an Arrow table."""
    # Projection pushdown
    if hasattr(pushdown, "projections") and pushdown.projections is not None:
        cols = pushdown.projections.pushed_columns
        if cols is not None:
            existing = [c for c in cols if c in table.column_names]
            if existing:
                table = table.select(existing)

    # Predicate pushdown — residual predicates handled by executor
    # Limit pushdown
    if hasattr(pushdown, "limit") and pushdown.limit is not None:
        if pushdown.limit.pushed_limit is not None:
            table = table.slice(0, pushdown.limit.pushed_limit)

    return table


# ── ArrowSink ────────────────────────────────────────────────────────


class ArrowSink(SinkPlugin):
    catalog_type = "arrow"

    def __init__(self, table_store: dict[tuple[str, str], pyarrow.Table] | None = None) -> None:
        self._tables = table_store if table_store is not None else _get_shared_store()

    def write(self, catalog: Catalog, joint: Joint, material: Material, strategy: str) -> None:
        table_name = joint.path or joint.name
        arrow_table = material.to_arrow()
        key = (catalog.name, table_name)

        if strategy == "append":
            existing = self._tables.get(key)
            if existing is not None:
                arrow_table = pyarrow.concat_tables([existing, arrow_table])
            self._tables[key] = arrow_table
        elif strategy == "replace":
            self._tables[key] = arrow_table
        else:
            # Default to replace for unsupported strategies
            self._tables[key] = arrow_table
