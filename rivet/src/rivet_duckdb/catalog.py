"""DuckDB catalog plugin for Rivet."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from rivet_core.errors import ExecutionError, PluginValidationError, plugin_error
from rivet_core.models import Catalog
from rivet_core.plugins import CatalogPlugin
from rivet_core.type_parser import parse_type

if TYPE_CHECKING:
    from rivet_core.introspection import CatalogNode, ObjectMetadata, ObjectSchema

_KNOWN_OPTIONS = {"path", "read_only", "schema", "table_map"}

_DUCKDB_TO_ARROW: dict[str, str] = {
    "bigint": "int64",
    "hugeint": "int64",
    "integer": "int32",
    "int": "int32",
    "int4": "int32",
    "smallint": "int16",
    "int2": "int16",
    "tinyint": "int8",
    "int1": "int8",
    "ubigint": "uint64",
    "uinteger": "uint32",
    "usmallint": "uint16",
    "utinyint": "uint8",
    "float": "float32",
    "float4": "float32",
    "real": "float32",
    "double": "float64",
    "float8": "float64",
    "decimal": "float64",
    "numeric": "float64",
    "boolean": "bool",
    "bool": "bool",
    "varchar": "large_utf8",
    "text": "large_utf8",
    "string": "large_utf8",
    "char": "large_utf8",
    "blob": "large_binary",
    "bytea": "large_binary",
    "date": "date32",
    "time": "time64[us]",
    "timestamp": "timestamp[us]",
    "timestamp with time zone": "timestamp[us, UTC]",
    "timestamptz": "timestamp[us, UTC]",
    "interval": "duration[us]",
    "json": "large_utf8",
    "uuid": "large_utf8",
}


def _open_connection(catalog: Catalog) -> Any:
    import duckdb

    path = catalog.options.get("path", ":memory:")
    read_only = catalog.options.get("read_only", False)
    return duckdb.connect(path, read_only=read_only)


class DuckDBCatalogPlugin(CatalogPlugin):
    type = "duckdb"
    required_options: list[str] = []
    optional_options: dict[str, Any] = {"path": ":memory:", "read_only": False, "schema": None}
    credential_options: list[str] = []

    def validate(self, options: dict[str, Any]) -> None:
        for key in options:
            if key not in _KNOWN_OPTIONS:
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"Unknown option '{key}' for duckdb catalog.",
                        plugin_name="rivet_duckdb",
                        plugin_type="catalog",
                        remediation=f"Valid options: {', '.join(sorted(self.optional_options))}",
                        option=key,
                    )
                )
        path = options.get("path", ":memory:")
        if path != ":memory:":
            parent = Path(path).parent
            if not parent.exists():
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"Parent directory '{parent}' does not exist for duckdb path '{path}'.",
                        plugin_name="rivet_duckdb",
                        plugin_type="catalog",
                        remediation=f"Create the directory first: mkdir -p {parent}",
                        path=path,
                        parent=str(parent),
                    )
                )

    def test_connection(self, catalog: Catalog) -> None:
        """Lightweight DuckDB connectivity check via ``SELECT 1``.

        For ``:memory:`` databases the connection always succeeds without
        file-existence validation.  Raises ``ExecutionError`` with structured
        error info on failure.
        """
        try:
            conn = _open_connection(catalog)
            try:
                conn.execute("SELECT 1")
            finally:
                conn.close()
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"DuckDB connectivity check failed: {exc}",
                    plugin_name="rivet_duckdb",
                    plugin_type="catalog",
                    remediation="Verify the DuckDB database path is correct and accessible.",
                    path=catalog.options.get("path", ":memory:"),
                )
            ) from exc

    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog:
        self.validate(options)
        return Catalog(name=name, type="duckdb", options=options)

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        schema = options.get("schema")
        if schema:
            return f"{schema}.{logical_name}"
        return logical_name

    def list_tables(self, catalog: Catalog) -> list[CatalogNode]:
        from rivet_core.introspection import CatalogNode, NodeSummary

        conn = _open_connection(catalog)
        try:
            rows = conn.execute(
                "SELECT schema_name, table_name, estimated_size "
                "FROM duckdb_tables() "
                "ORDER BY schema_name, table_name"
            ).fetchall()
        finally:
            conn.close()

        nodes = []
        for schema_name, table_name, estimated_size in rows:
            summary = NodeSummary(
                row_count=None,
                size_bytes=estimated_size,
                format="duckdb",
                last_modified=None,
                owner=None,
                comment=None,
            )
            nodes.append(
                CatalogNode(
                    name=table_name,
                    node_type="table",
                    path=[catalog.name, schema_name, table_name],
                    is_container=False,
                    children_count=None,
                    summary=summary,
                )
            )
        return nodes

    def list_children(self, catalog: Catalog, path: list[str]) -> list[CatalogNode]:
        """Lazy single-level listing for DuckDB catalogs.

        - path=[] → list schemas
        - path=[schema] → list tables in that schema
        - path=[schema, table] → columns via get_schema()
        """
        from rivet_core.introspection import CatalogNode, NodeSummary

        depth = len(path)

        if depth == 0:
            # Level 0: list schemas
            conn = _open_connection(catalog)
            try:
                rows = conn.execute(
                    "SELECT DISTINCT schema_name FROM duckdb_tables() ORDER BY schema_name"
                ).fetchall()
            finally:
                conn.close()
            return [
                CatalogNode(
                    name=schema_name,
                    node_type="schema",
                    path=[schema_name],
                    is_container=True,
                    children_count=None,
                    summary=None,
                )
                for (schema_name,) in rows
            ]

        if depth == 1:
            # Level 1: list tables in a schema
            schema_name = path[0]
            conn = _open_connection(catalog)
            try:
                rows = conn.execute(
                    "SELECT table_name, estimated_size "
                    "FROM duckdb_tables() "
                    "WHERE schema_name = ? "
                    "ORDER BY table_name",
                    [schema_name],
                ).fetchall()
            finally:
                conn.close()
            return [
                CatalogNode(
                    name=table_name,
                    node_type="table",
                    path=[schema_name, table_name],
                    is_container=False,
                    children_count=None,
                    summary=NodeSummary(
                        row_count=None,
                        size_bytes=estimated_size,
                        format="duckdb",
                        last_modified=None,
                        owner=None,
                        comment=None,
                    ),
                )
                for table_name, estimated_size in rows
            ]

        if depth == 2:
            # Level 2: list columns of a table
            schema_name, table_name = path[0], path[1]
            qualified = f"{schema_name}.{table_name}"
            try:
                schema = self.get_schema(catalog, qualified)
            except Exception:
                return []
            return [
                CatalogNode(
                    name=col.name,
                    node_type="column",
                    path=[schema_name, table_name, col.name],
                    is_container=False,
                    children_count=None,
                    summary=NodeSummary(
                        row_count=None,
                        size_bytes=None,
                        format=col.type,
                        last_modified=None,
                        owner=None,
                        comment=None,
                    ),
                )
                for col in schema.columns
            ]

        return []

    def get_schema(self, catalog: Catalog, table: str) -> ObjectSchema:
        from rivet_core.introspection import ColumnDetail, ObjectSchema

        conn = _open_connection(catalog)
        try:
            rows = conn.execute(f"DESCRIBE {table}").fetchall()
        finally:
            conn.close()

        # DESCRIBE returns: column_name, column_type, null, key, default, extra
        columns = []
        for row in rows:
            col_name = row[0]
            col_type = row[1]
            nullable_str = row[2] if len(row) > 2 else "YES"
            nullable = nullable_str != "NO" if nullable_str else True
            key = row[3] if len(row) > 3 else None
            default = row[4] if len(row) > 4 else None
            is_pk = key == "PRI" if key else False

            columns.append(
                ColumnDetail(
                    name=col_name,
                    type=parse_type(col_type, _DUCKDB_TO_ARROW),
                    native_type=col_type,
                    nullable=nullable,
                    default=str(default) if default is not None else None,
                    comment=None,
                    is_primary_key=is_pk,
                    is_partition_key=False,
                )
            )

        return ObjectSchema(
            path=[catalog.name, table],
            node_type="table",
            columns=columns,
            primary_key=None,
            comment=None,
        )

    def get_metadata(self, catalog: Catalog, table: str) -> ObjectMetadata | None:
        from rivet_core.introspection import ObjectMetadata

        conn = _open_connection(catalog)
        try:
            # Parse schema and table name
            parts = table.split(".")
            if len(parts) == 2:
                schema_name, table_name = parts[0], parts[1]
            else:
                schema_name = None
                table_name = parts[0]

            if schema_name:
                rows = conn.execute(
                    "SELECT estimated_size, column_count, schema_name, table_name "
                    "FROM duckdb_tables() "
                    "WHERE schema_name = ? AND table_name = ?",
                    [schema_name, table_name],
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT estimated_size, column_count, schema_name, table_name "
                    "FROM duckdb_tables() "
                    "WHERE table_name = ?",
                    [table_name],
                ).fetchall()
        finally:
            conn.close()

        if not rows:
            return None

        row = rows[0]
        estimated_size = row[0]
        resolved_schema = row[2]
        resolved_table = row[3]

        return ObjectMetadata(
            path=[catalog.name, resolved_schema, resolved_table],
            node_type="table",
            row_count=None,
            size_bytes=estimated_size,
            last_modified=None,
            created_at=None,
            format="duckdb",
            compression=None,
            owner=None,
            comment=None,
            location=catalog.options.get("path", ":memory:"),
            column_statistics=[],
            partitioning=None,
        )
