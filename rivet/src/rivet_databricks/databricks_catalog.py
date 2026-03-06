"""Databricks catalog plugin for Rivet."""

from __future__ import annotations

import logging
import warnings
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rivet_core.errors import PluginValidationError, plugin_error
from rivet_core.models import Catalog
from rivet_core.plugins import CatalogPlugin
from rivet_databricks.auth import (
    ResolvedCredential,
    _check_partial_azure,
    _check_partial_oauth_m2m,
    resolve_credentials,
)

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
_OPTIONAL_OPTIONS: dict[str, Any] = {"schema": "default", "http_path": None}
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
    "array": "large_utf8",
    "map": "large_utf8",
    "struct": "large_utf8",
    "void": "null",
}


def _unity_type_to_arrow(type_text: str) -> str:
    """Map Unity Catalog type_text to Arrow type name; unknown → large_utf8 with warning."""
    lower = type_text.lower().strip().split("(")[0].strip()
    if lower in _UNITY_TO_ARROW:
        return _UNITY_TO_ARROW[lower]
    warnings.warn(
        f"Unknown Unity Catalog type '{type_text}'; mapping to large_utf8.",
        stacklevel=4,
    )
    return "large_utf8"


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

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        catalog = options["catalog"]
        schema = options.get("schema", "default")
        return f"{catalog}.{schema}.{logical_name}"

    # ── Introspection ─────────────────────────────────────────────────

    def list_tables(self, catalog: Catalog) -> list[CatalogNode]:
        """List tables via Unity Catalog REST API (4-level hierarchy)."""
        from rivet_core.introspection import CatalogNode, NodeSummary
        from rivet_databricks.client import UnityCatalogClient

        host = catalog.options["workspace_url"]
        catalog_name = catalog.options["catalog"]
        credential = self.resolve_credentials(catalog.options)
        client = UnityCatalogClient(host=host, credential=credential)
        try:
            nodes: list[CatalogNode] = []
            schemas = client.list_schemas(catalog_name)
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

    def get_schema(self, catalog: Catalog, table: str) -> ObjectSchema:
        """Get schema via GET /tables/{full_name}, mapping type_text to Arrow types."""
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
        partition_cols = {c.get("name") for c in raw.get("partition_columns", [])} if raw.get("partition_columns") else set()
        columns = [
            ColumnDetail(
                name=col.get("name", ""),
                type=_unity_type_to_arrow(col.get("type_text", "string")),
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

    def get_metadata(self, catalog: Catalog, table: str) -> ObjectMetadata | None:
        """Get metadata via GET /tables/{full_name}."""
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
