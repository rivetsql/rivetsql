"""Databricks catalog plugin for Rivet."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rivet_core.errors import PluginValidationError, plugin_error
from rivet_core.models import Catalog
from rivet_core.plugins import CatalogPlugin
from rivet_core.type_parser import parse_type
from rivet_databricks.auth import (
    ResolvedCredential,
    _check_partial_azure,
    _check_partial_oauth_m2m,
    resolve_credentials,
)
from rivet_databricks.engine import DatabricksStatementAPI

if TYPE_CHECKING:
    from rivet_core.introspection import CatalogNode, ObjectMetadata, ObjectSchema

_logger = logging.getLogger(__name__)

_REQUIRED_OPTIONS = ["workspace_url", "catalog"]
_CREDENTIAL_OPTIONS = [
    "token",
    "client_id",
    "client_secret",
    "azure_tenant_id",
    "azure_client_id",
    "azure_client_secret",
]
_OPTIONAL_OPTIONS: dict[str, Any] = {
    "schema": None,
    "http_path": None,
    "legacy": False,
    "warehouse_id": None,
}
_KNOWN_OPTIONS = (
    set(_REQUIRED_OPTIONS) | set(_CREDENTIAL_OPTIONS) | set(_OPTIONAL_OPTIONS) | {"table_map"}
)

# Unity Catalog type_text → Arrow type name
_UNITY_TO_ARROW: dict[str, str] = {
    "bigint": "int64",
    "long": "int64",
    "int": "int32",
    "integer": "int32",
    "smallint": "int16",
    "short": "int16",
    "tinyint": "int8",
    "byte": "int8",
    "float": "float32",
    "double": "float64",
    "decimal": "float64",
    "boolean": "bool",
    "string": "large_utf8",
    "varchar": "large_utf8",
    "char": "large_utf8",
    "binary": "large_binary",
    "date": "date32",
    "timestamp": "timestamp[us, UTC]",
    "timestamp_ntz": "timestamp[us]",
    "void": "null",
}


def _parse_ts(value: Any) -> datetime | None:
    """Parse a Unity Catalog timestamp (epoch ms int or ISO string) to datetime."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value / 1000.0, tz=UTC)
        except (OSError, OverflowError, ValueError):
            return None
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _is_truthy(value: Any) -> bool:
    """Check if a value from an Arrow column is truthy (handles bool and string)."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    return bool(value)


class DatabricksCatalogPlugin(CatalogPlugin):
    type = "databricks"
    required_options: list[str] = _REQUIRED_OPTIONS
    optional_options: dict[str, Any] = _OPTIONAL_OPTIONS
    credential_options: list[str] = _CREDENTIAL_OPTIONS

    def validate(self, options: dict[str, Any]) -> None:
        for key in options:
            if key not in _KNOWN_OPTIONS:
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"Unknown option '{key}' for databricks catalog.",
                        plugin_name="rivet_databricks",
                        plugin_type="catalog",
                        remediation=f"Valid options: {', '.join(sorted(_KNOWN_OPTIONS - {'table_map'}))}",
                    )
                )
        for key in _REQUIRED_OPTIONS:
            if key not in options:
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"Missing required option '{key}' for databricks catalog.",
                        plugin_name="rivet_databricks",
                        plugin_type="catalog",
                        remediation=f"Provide '{key}' in the catalog options.",
                        missing_option=key,
                    )
                )
        # Validate workspace_url scheme (RVT-202)
        workspace_url = options.get("workspace_url", "")
        if not str(workspace_url).startswith("https://"):
            raise PluginValidationError(
                plugin_error(
                    "RVT-202",
                    f"workspace_url must start with https:// (got: '{workspace_url}').",
                    plugin_name="rivet_databricks",
                    plugin_type="catalog",
                    remediation="Set workspace_url to a full HTTPS URL, e.g. 'https://my.databricks.com'.",
                    workspace_url=workspace_url,
                )
            )
        # Validate partial credential sets (RVT-205)
        _check_partial_oauth_m2m(options)
        _check_partial_azure(options)
        # Validate legacy catalog requires warehouse_id
        if options.get("legacy"):
            wid = options.get("warehouse_id")
            if not wid:
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        "warehouse_id is required for legacy catalog introspection.",
                        plugin_name="rivet_databricks",
                        plugin_type="catalog",
                        remediation="Add 'warehouse_id' to the catalog options when using 'legacy: true'.",
                        missing_option="warehouse_id",
                    )
                )

    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog:
        self.validate(options)
        return Catalog(name=name, type="databricks", options=options)

    def resolve_credentials(
        self,
        options: dict[str, Any],
        config_path: Path | None = None,
    ) -> ResolvedCredential:
        """Resolve credentials: explicit → env → ~/.databrickscfg."""
        host = options.get("workspace_url")
        return resolve_credentials(options, host=host, config_path=config_path)

    def _create_statement_api(self, catalog: Catalog) -> DatabricksStatementAPI:
        """Instantiate a DatabricksStatementAPI for legacy SQL introspection."""
        host = catalog.options["workspace_url"]
        warehouse_id = catalog.options["warehouse_id"]
        credential = self.resolve_credentials(catalog.options)
        token = credential.token or ""
        return DatabricksStatementAPI(
            workspace_url=host,
            token=token,
            warehouse_id=warehouse_id,
        )

    def _legacy_list_schemas(
        self,
        api: DatabricksStatementAPI,
        catalog_name: str,
    ) -> list[CatalogNode]:
        """List schemas in a legacy catalog via ``SHOW SCHEMAS IN <catalog>``."""
        from rivet_core.errors import ExecutionError
        from rivet_core.introspection import CatalogNode

        sql = f"SHOW SCHEMAS IN {catalog_name}"
        try:
            result = api.execute(sql, catalog=catalog_name)
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Legacy introspection failed for catalog '{catalog_name}': {exc}",
                    plugin_name="rivet_databricks",
                    plugin_type="catalog",
                    remediation="Check warehouse_id, credentials, and that the catalog exists.",
                )
            ) from exc

        col_name = "databaseName" if "databaseName" in result.column_names else "namespace"
        names = result.column(col_name).to_pylist()
        return [
            CatalogNode(
                name=n,
                node_type="schema",
                path=[n],
                is_container=True,
                children_count=None,
                summary=None,
            )
            for n in names
        ]

    def _legacy_list_tables(
        self,
        api: DatabricksStatementAPI,
        catalog_name: str,
        schema_name: str,
    ) -> list[CatalogNode]:
        """List tables in a legacy schema via ``SHOW TABLES IN <catalog>.<schema>``."""
        from rivet_core.errors import ExecutionError
        from rivet_core.introspection import CatalogNode

        sql = f"SHOW TABLES IN {catalog_name}.{schema_name}"
        try:
            result = api.execute(sql, catalog=catalog_name)
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Legacy introspection failed for schema '{catalog_name}.{schema_name}': {exc}",
                    plugin_name="rivet_databricks",
                    plugin_type="catalog",
                    remediation="Check warehouse_id, credentials, and that the catalog exists.",
                )
            ) from exc

        table_names = result.column("tableName").to_pylist()
        is_temp_col = result.column("isTemporary").to_pylist()
        return [
            CatalogNode(
                name=tname,
                node_type="temporary_table" if _is_truthy(is_temp) else "table",
                path=[schema_name, tname],
                is_container=False,
                children_count=None,
                summary=None,
            )
            for tname, is_temp in zip(table_names, is_temp_col)
        ]

    def _legacy_list_children(
        self,
        catalog: Catalog,
        path: list[str],
    ) -> list[CatalogNode]:
        """Route legacy list_children to the appropriate SQL helper."""
        api = self._create_statement_api(catalog)
        catalog_name = catalog.options["catalog"]
        schema_filter: str | None = catalog.options.get("schema")
        try:
            if len(path) == 0:
                nodes = self._legacy_list_schemas(api, catalog_name)
                if schema_filter:
                    nodes = [n for n in nodes if n.name == schema_filter]
                return nodes
            if len(path) == 1:
                if schema_filter and path[0] != schema_filter:
                    return []
                return self._legacy_list_tables(api, catalog_name, path[0])
            return []
        finally:
            api.close()

    def _legacy_describe_table(
        self,
        api: DatabricksStatementAPI,
        catalog_name: str,
        schema_name: str,
        table_name: str,
    ) -> ObjectSchema:
        """Describe a legacy table via ``DESCRIBE TABLE <fqn>``.

        Parses column definitions and detects partition columns after the
        ``# Partition Information`` separator row.
        """
        from rivet_core.errors import ExecutionError
        from rivet_core.introspection import ColumnDetail, ObjectSchema

        fqn = f"{catalog_name}.{schema_name}.{table_name}"
        sql = f"DESCRIBE TABLE {fqn}"
        try:
            result = api.execute(sql, catalog=catalog_name)
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Legacy introspection failed for table '{fqn}': {exc}",
                    plugin_name="rivet_databricks",
                    plugin_type="catalog",
                    remediation="Check warehouse_id, credentials, and that the table exists.",
                )
            ) from exc

        col_names = result.column("col_name").to_pylist()
        data_types = result.column("data_type").to_pylist()
        comments = result.column("comment").to_pylist()

        columns: list[ColumnDetail] = []
        partition_cols: set[str] = set()
        in_partition_section = False

        for col, dtype, _comment in zip(col_names, data_types, comments):
            col_str = str(col).strip() if col else ""
            dtype_str = str(dtype).strip() if dtype else ""

            # Detect partition separator
            if col_str == "# Partition Information":
                in_partition_section = True
                continue

            # Skip metadata header rows (empty data_type or header labels)
            if not col_str or not dtype_str or col_str.startswith("#"):
                continue

            if in_partition_section:
                partition_cols.add(col_str)

        # Second pass: build ColumnDetail list from pre-partition rows
        in_partition_section = False
        for col, dtype, comment in zip(col_names, data_types, comments):
            col_str = str(col).strip() if col else ""
            dtype_str = str(dtype).strip() if dtype else ""

            if col_str == "# Partition Information":
                break

            if not col_str or not dtype_str or col_str.startswith("#"):
                continue

            comment_str = str(comment).strip() if comment else None
            if comment_str == "":
                comment_str = None

            columns.append(
                ColumnDetail(
                    name=col_str,
                    type=parse_type(dtype_str, _UNITY_TO_ARROW),
                    native_type=dtype_str,
                    nullable=True,
                    default=None,
                    comment=comment_str,
                    is_primary_key=False,
                    is_partition_key=col_str in partition_cols,
                )
            )

        return ObjectSchema(
            path=[catalog_name, schema_name, table_name],
            node_type="table",
            columns=columns,
            primary_key=None,
            comment=None,
        )

    def _legacy_list_tables_all(
        self,
        catalog: Catalog,
    ) -> list[CatalogNode]:
        """Enumerate all tables across all schemas in a legacy catalog.

        Iterates schemas via ``_legacy_list_schemas``, then tables per schema
        via ``_legacy_list_tables``.  Each returned ``CatalogNode`` has
        ``path=[schema_name, table_name]``.

        When ``schema`` is set in catalog options, only that schema is enumerated.
        """
        api = self._create_statement_api(catalog)
        catalog_name = catalog.options["catalog"]
        schema_filter: str | None = catalog.options.get("schema")
        try:
            schemas = self._legacy_list_schemas(api, catalog_name)
            if schema_filter:
                schemas = [s for s in schemas if s.name == schema_filter]
            nodes: list[CatalogNode] = []
            for schema_node in schemas:
                tables = self._legacy_list_tables(api, catalog_name, schema_node.name)
                nodes.extend(tables)
            return nodes
        finally:
            api.close()

    def _legacy_test_connection(self, catalog: Catalog) -> None:
        """Verify connectivity for a legacy catalog via a lightweight SQL query.

        Executes ``SHOW SCHEMAS IN <catalog>`` and raises
        ``ExecutionError`` on failure with ``workspace_url`` and
        ``warehouse_id`` in the message.
        """
        from rivet_core.errors import ExecutionError

        api = self._create_statement_api(catalog)
        catalog_name = catalog.options["catalog"]
        host = catalog.options["workspace_url"]
        warehouse_id = catalog.options["warehouse_id"]
        sql = f"SHOW SCHEMAS IN {catalog_name}"
        try:
            api.execute(sql, catalog=catalog_name)
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Legacy connectivity check failed for {host} "
                    f"(warehouse_id={warehouse_id}): {exc}",
                    plugin_name="rivet_databricks",
                    plugin_type="catalog",
                    remediation="Check workspace_url, warehouse_id, credentials, and network connectivity.",
                    host=host,
                    warehouse_id=warehouse_id,
                    catalog=catalog_name,
                )
            ) from exc
        finally:
            api.close()

    def _legacy_describe_extended(
        self,
        api: DatabricksStatementAPI,
        catalog_name: str,
        schema_name: str,
        table_name: str,
    ) -> ObjectMetadata:
        """Extract metadata via ``DESCRIBE TABLE EXTENDED <fqn>``.

        Parses the detailed table information section (rows after the
        ``# Detailed Table Information`` separator) for ``Location``,
        ``Owner``, and ``Provider``/``Type`` properties and maps them to
        :class:`ObjectMetadata` fields.
        """
        from rivet_core.errors import ExecutionError
        from rivet_core.introspection import ObjectMetadata

        fqn = f"{catalog_name}.{schema_name}.{table_name}"
        sql = f"DESCRIBE TABLE EXTENDED {fqn}"
        try:
            result = api.execute(sql, catalog=catalog_name)
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Legacy introspection failed for table '{fqn}': {exc}",
                    plugin_name="rivet_databricks",
                    plugin_type="catalog",
                    remediation="Check warehouse_id, credentials, and that the table exists.",
                )
            ) from exc

        col_names = result.column("col_name").to_pylist()
        data_types = result.column("data_type").to_pylist()

        location: str | None = None
        owner: str | None = None
        fmt: str | None = None

        in_detailed_section = False
        for col, dtype in zip(col_names, data_types):
            col_str = str(col).strip() if col else ""
            dtype_str = str(dtype).strip() if dtype else ""

            if col_str == "# Detailed Table Information":
                in_detailed_section = True
                continue

            if not in_detailed_section:
                continue

            if col_str == "Location":
                location = dtype_str or None
            elif col_str == "Owner":
                owner = dtype_str or None
            elif col_str in ("Provider", "Type"):
                fmt = dtype_str or None

        return ObjectMetadata(
            path=[catalog_name, schema_name, table_name],
            node_type="table",
            row_count=None,
            size_bytes=None,
            last_modified=None,
            created_at=None,
            format=fmt,
            compression=None,
            owner=owner,
            comment=None,
            location=location,
            column_statistics=[],
            partitioning=None,
            properties={},
        )

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        catalog = options["catalog"]
        schema = options.get("schema", "default")
        return f"{catalog}.{schema}.{logical_name}"

    # ── Introspection ─────────────────────────────────────────────────

    def list_tables(self, catalog: Catalog) -> list[CatalogNode]:
        """List tables via Unity Catalog REST API (4-level hierarchy)."""
        if catalog.options.get("legacy", False):
            return self._legacy_list_tables_all(catalog)

        from rivet_core.introspection import CatalogNode, NodeSummary
        from rivet_databricks.client import UnityCatalogClient

        host = catalog.options["workspace_url"]
        catalog_name = catalog.options["catalog"]
        schema_filter: str | None = catalog.options.get("schema")
        credential = self.resolve_credentials(catalog.options)
        client = UnityCatalogClient(host=host, credential=credential)
        try:
            nodes: list[CatalogNode] = []
            schemas = client.list_schemas(catalog_name)
            if schema_filter:
                schemas = [s for s in schemas if s.get("name") == schema_filter]
            for schema_obj in schemas:
                schema_name = schema_obj.get("name", "")
                tables = client.list_tables(catalog_name, schema_name)
                for tbl in tables:
                    nodes.append(
                        CatalogNode(
                            name=tbl.get("name", ""),
                            node_type=tbl.get("table_type", "table").lower(),
                            path=[catalog_name, schema_name, tbl.get("name", "")],
                            is_container=False,
                            children_count=None,
                            summary=NodeSummary(
                                row_count=tbl.get("properties", {}).get("delta.numRecords"),
                                size_bytes=None,
                                format=tbl.get("data_source_format"),
                                last_modified=_parse_ts(tbl.get("updated_at")),
                                owner=tbl.get("owner"),
                                comment=tbl.get("comment"),
                            ),
                        )
                    )
            return nodes
        finally:
            client.close()

    def test_connection(self, catalog: Catalog) -> None:
        """Lightweight connectivity check via Unity Catalog ``/catalogs`` endpoint.

        Faster than the base-class fallback (which calls ``list_tables``),
        because it avoids iterating schemas and tables.

        Raises ``ExecutionError`` with structured error info on failure.
        """
        if catalog.options.get("legacy", False):
            return self._legacy_test_connection(catalog)

        from rivet_core.errors import ExecutionError
        from rivet_databricks.client import UnityCatalogClient

        host = catalog.options["workspace_url"]
        credential = self.resolve_credentials(catalog.options)
        client = UnityCatalogClient(host=host, credential=credential)
        try:
            client.list_catalogs()
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Databricks connectivity check failed for {host}.",
                    plugin_name="rivet_databricks",
                    plugin_type="catalog",
                    remediation="Check workspace_url, credentials, and network connectivity.",
                    host=host,
                    catalog=catalog.options.get("catalog"),
                )
            ) from exc
        finally:
            client.close()

    def get_schema(self, catalog: Catalog, table: str) -> ObjectSchema:
        """Get schema via GET /tables/{full_name}, mapping type_text to Arrow types."""
        if catalog.options.get("legacy", False):
            parts = table.split(".")
            if len(parts) >= 3:
                catalog_name, schema_name, table_name = parts[0], parts[1], parts[2]
            elif len(parts) == 2:
                catalog_name = catalog.options["catalog"]
                schema_name, table_name = parts[0], parts[1]
            else:
                catalog_name = catalog.options["catalog"]
                schema_name = catalog.options.get("schema", "default")
                table_name = parts[0]
            api = self._create_statement_api(catalog)
            try:
                return self._legacy_describe_table(api, catalog_name, schema_name, table_name)
            finally:
                api.close()

        from rivet_core.introspection import ColumnDetail, ObjectSchema
        from rivet_databricks.client import UnityCatalogClient

        host = catalog.options["workspace_url"]
        credential = self.resolve_credentials(catalog.options)
        client = UnityCatalogClient(host=host, credential=credential)
        try:
            raw = client.get_table(table)
        finally:
            client.close()

        columns_raw = raw.get("columns", [])
        partition_cols = (
            {c.get("name") for c in raw.get("partition_columns", [])}
            if raw.get("partition_columns")
            else set()
        )
        columns = [
            ColumnDetail(
                name=col.get("name", ""),
                type=parse_type(col.get("type_text", "string"), _UNITY_TO_ARROW),
                native_type=col.get("type_text"),
                nullable=col.get("nullable", True),
                default=col.get("default_value"),
                comment=col.get("comment"),
                is_primary_key=False,
                is_partition_key=col.get("name") in partition_cols,
            )
            for col in columns_raw
        ]
        parts = table.split(".")
        return ObjectSchema(
            path=parts,
            node_type=raw.get("table_type", "table").lower(),
            columns=columns,
            primary_key=None,
            comment=raw.get("comment"),
        )

    def list_children(self, catalog: Catalog, path: list[str]) -> list[CatalogNode]:
        """Lazy single-level listing for Databricks Unity Catalog.

        - path=[] → list schemas in the configured catalog
        - path=[schema] → list tables in that schema
        - path=[schema, table] → list columns via get_schema()

        When ``schema`` is set in catalog options, only that schema is visible
        at depth 0 and attempts to list tables in a different schema return [].
        """
        if catalog.options.get("legacy", False):
            return self._legacy_list_children(catalog, path)

        from rivet_core.introspection import CatalogNode, NodeSummary
        from rivet_databricks.client import UnityCatalogClient

        depth = len(path)
        host = catalog.options["workspace_url"]
        catalog_name = catalog.options["catalog"]
        schema_filter: str | None = catalog.options.get("schema")
        credential = self.resolve_credentials(catalog.options)

        if depth == 0:
            # Level 0: list schemas in the catalog
            client = UnityCatalogClient(host=host, credential=credential)
            try:
                schemas = client.list_schemas(catalog_name)
            finally:
                client.close()
            if schema_filter:
                schemas = [s for s in schemas if s.get("name") == schema_filter]
            return [
                CatalogNode(
                    name=s.get("name", ""),
                    node_type="schema",
                    path=[s.get("name", "")],
                    is_container=True,
                    children_count=None,
                    summary=NodeSummary(
                        row_count=None,
                        size_bytes=None,
                        format=None,
                        last_modified=None,
                        owner=s.get("owner"),
                        comment=s.get("comment"),
                    ),
                )
                for s in schemas
            ]

        if depth == 1:
            # Level 1: list tables in a schema
            schema_name = path[0]
            # Reject listing tables in a schema that doesn't match the filter
            if schema_filter and schema_name != schema_filter:
                return []
            client = UnityCatalogClient(host=host, credential=credential)
            try:
                tables = client.list_tables(catalog_name, schema_name)
            finally:
                client.close()
            return [
                CatalogNode(
                    name=tbl.get("name", ""),
                    node_type=tbl.get("table_type", "table").lower(),
                    path=[schema_name, tbl.get("name", "")],
                    is_container=False,
                    children_count=None,
                    summary=NodeSummary(
                        row_count=tbl.get("properties", {}).get("delta.numRecords"),
                        size_bytes=None,
                        format=tbl.get("data_source_format"),
                        last_modified=_parse_ts(tbl.get("updated_at")),
                        owner=tbl.get("owner"),
                        comment=tbl.get("comment"),
                    ),
                )
                for tbl in tables
            ]

        if depth == 2:
            # Level 2: list columns of a table
            schema_name, table_name = path[0], path[1]
            qualified = f"{catalog_name}.{schema_name}.{table_name}"
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

    def get_metadata(self, catalog: Catalog, table: str) -> ObjectMetadata | None:
        """Get metadata via GET /tables/{full_name}."""
        if catalog.options.get("legacy", False):
            parts = table.split(".")
            if len(parts) >= 3:
                catalog_name, schema_name, table_name = parts[0], parts[1], parts[2]
            elif len(parts) == 2:
                catalog_name = catalog.options["catalog"]
                schema_name, table_name = parts[0], parts[1]
            else:
                catalog_name = catalog.options["catalog"]
                schema_name = catalog.options.get("schema", "default")
                table_name = parts[0]
            api = self._create_statement_api(catalog)
            try:
                return self._legacy_describe_extended(
                    api,
                    catalog_name,
                    schema_name,
                    table_name,
                )
            finally:
                api.close()

        from rivet_core.introspection import ObjectMetadata
        from rivet_databricks.client import UnityCatalogClient

        host = catalog.options["workspace_url"]
        credential = self.resolve_credentials(catalog.options)
        client = UnityCatalogClient(host=host, credential=credential)
        try:
            raw = client.get_table(table)
        finally:
            client.close()

        props = raw.get("properties", {}) or {}
        size_bytes_raw = props.get("delta.sizeInBytes") or props.get("size_bytes")
        num_rows_raw = props.get("delta.numRecords") or props.get("num_rows")
        try:
            size_bytes: int | None = int(size_bytes_raw) if size_bytes_raw is not None else None
        except (ValueError, TypeError):
            size_bytes = None
        try:
            num_rows: int | None = int(num_rows_raw) if num_rows_raw is not None else None
        except (ValueError, TypeError):
            num_rows = None

        parts = table.split(".")
        return ObjectMetadata(
            path=parts,
            node_type=raw.get("table_type", "table").lower(),
            row_count=num_rows,
            size_bytes=size_bytes,
            last_modified=_parse_ts(raw.get("updated_at")),
            created_at=_parse_ts(raw.get("created_at")),
            format=raw.get("data_source_format"),
            compression=None,
            owner=raw.get("owner"),
            comment=raw.get("comment"),
            location=raw.get("storage_location"),
            column_statistics=[],
            partitioning=None,
            properties={str(k): str(v) for k, v in props.items()},
        )
