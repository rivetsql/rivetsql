"""Built-in Arrow catalog, compute engine, source, and sink plugins."""

from __future__ import annotations

from typing import Any

import pyarrow

from rivet_core.errors import ExecutionError, PluginValidationError, RivetError, plugin_error
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
    """Built-in in-memory Arrow catalog plugin.

    Stores tables as ``pyarrow.Table`` objects in a shared dict keyed by
    ``(catalog_name, table_name)``.  Useful for testing and lightweight
    in-process pipelines.
    """

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
        """Validate Arrow catalog options, rejecting unrecognized keys."""
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

    def test_connection(self, catalog: Catalog) -> None:
        """Lightweight connectivity check — always succeeds for in-memory Arrow."""
        pass

    def list_children(self, catalog: Catalog, path: list[str]) -> list[CatalogNode]:
        """Lazy single-level listing for in-memory Arrow catalogs.

        - path=[] → list registered tables
        - path=[table_name] → list columns of that table
        """
        depth = len(path)

        if depth == 0:
            # Level 0: list all registered tables for this catalog
            nodes: list[CatalogNode] = []
            for (cat_name, table_name), table in self._tables.items():
                if cat_name != catalog.name:
                    continue
                nodes.append(
                    CatalogNode(
                        name=table_name,
                        node_type="table",
                        path=[table_name],
                        is_container=False,
                        children_count=table.num_columns,
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

        if depth == 1:
            # Level 1: list columns of a table
            table_name = path[0]
            t = self._tables.get((catalog.name, table_name))
            if t is None:
                return []
            return [
                CatalogNode(
                    name=field.name,
                    node_type="column",
                    path=[table_name, field.name],
                    is_container=False,
                    children_count=None,
                    summary=NodeSummary(
                        row_count=None,
                        size_bytes=None,
                        format=_arrow_type_name(field.type),
                        last_modified=None,
                        owner=None,
                        comment=None,
                    ),
                )
                for field in t.schema
            ]

        return []

    def get_schema(self, catalog: Catalog, table: str) -> ObjectSchema:
        """Return the schema of an in-memory Arrow table.

        Raises:
            ExecutionError: If the table does not exist in this catalog.
        """
        t = self._tables.get((catalog.name, table))
        if t is None:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Table '{table}' not found in arrow catalog '{catalog.name}'.",
                    plugin_name="arrow",
                    plugin_type="catalog",
                    remediation=f"Check that the table '{table}' exists in catalog '{catalog.name}'. "
                    "Use list_tables() to see available tables.",
                    table=table,
                    catalog=catalog.name,
                )
            )
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
    """Lightweight compute engine that delegates SQL execution to an in-process DuckDB instance."""

    engine_type = "arrow"
    required_options: list[str] = []
    optional_options: dict[str, Any] = {}
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
        """Validate Arrow engine options, rejecting unrecognized keys."""
        recognized = set(self.optional_options) | set(self.required_options)
        for key in options:
            if key not in recognized:
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"Unknown option '{key}' for arrow engine.",
                        plugin_name="arrow",
                        plugin_type="engine",
                        remediation="Arrow engine has no configurable options."
                        if not recognized
                        else f"Valid options: {', '.join(sorted(recognized))}",
                        option=key,
                    )
                )

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
    """Source plugin that reads from the shared in-memory Arrow table store."""

    catalog_type = "arrow"

    def __init__(self, table_store: dict[tuple[str, str], pyarrow.Table] | None = None) -> None:
        self._tables = table_store if table_store is not None else _get_shared_store()

    def read(self, catalog: Catalog, joint: Joint, pushdown: Any | None = None) -> Material:
        """Read an in-memory Arrow table, applying optional pushdown.

        Raises:
            ExecutionError: If the table does not exist in this catalog.
        """
        table_name = joint.path or joint.name
        table = self._tables.get((catalog.name, table_name))
        if table is None:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Table '{table_name}' not found in arrow catalog '{catalog.name}'.",
                    plugin_name="arrow",
                    plugin_type="source",
                    remediation=f"Check that the table '{table_name}' exists in catalog '{catalog.name}'. "
                    "Available tables can be listed via the catalog introspection API.",
                    table=table_name,
                    catalog=catalog.name,
                )
            )

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


ARROW_SUPPORTED_STRATEGIES = frozenset({"append", "replace"})


class ArrowSink(SinkPlugin):
    """Sink plugin that writes to the shared in-memory Arrow table store."""

    catalog_type = "arrow"
    supported_strategies = ARROW_SUPPORTED_STRATEGIES

    def __init__(self, table_store: dict[tuple[str, str], pyarrow.Table] | None = None) -> None:
        self._tables = table_store if table_store is not None else _get_shared_store()

    def write(self, catalog: Catalog, joint: Joint, material: Material, strategy: str) -> None:
        """Write an Arrow table to the in-memory store.

        Supports ``append`` (concatenates with existing) and ``replace``.

        Raises:
            ExecutionError: If *strategy* is not in ``supported_strategies``.
        """
        if strategy not in ARROW_SUPPORTED_STRATEGIES:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Unsupported write strategy '{strategy}' for Arrow sink.",
                    plugin_name="arrow",
                    plugin_type="sink",
                    remediation=f"Supported strategies: {', '.join(sorted(ARROW_SUPPORTED_STRATEGIES))}",
                    strategy=strategy,
                    catalog=catalog.name,
                )
            )

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
