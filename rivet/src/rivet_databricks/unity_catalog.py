"""Unity Catalog plugin for Rivet."""

from __future__ import annotations

import logging
import warnings
from datetime import UTC
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rivet_core.errors import ExecutionError, PluginValidationError, plugin_error
from rivet_core.models import Catalog
from rivet_core.plugins import CatalogPlugin
from rivet_databricks.auth import ResolvedCredential, resolve_credentials

if TYPE_CHECKING:
    from rivet_core.introspection import CatalogNode, ObjectMetadata, ObjectSchema

_logger = logging.getLogger(__name__)

_REQUIRED_OPTIONS = ["host", "catalog_name"]
_CREDENTIAL_OPTIONS = ["token", "client_id", "client_secret", "auth_type"]
_OPTIONAL_OPTIONS: dict[str, Any] = {"schema": "default"}
_KNOWN_OPTIONS = (
    set(_REQUIRED_OPTIONS) | set(_CREDENTIAL_OPTIONS) | set(_OPTIONAL_OPTIONS) | {"table_map", "region"}
)
_VALID_AUTH_TYPES = frozenset({"pat", "oauth_m2m", "azure_cli", "gcp_login"})

# Maps auth_type → which credential options are relevant
_CREDENTIAL_GROUPS: dict[str, list[str]] = {
    "pat": ["token"],
    "oauth_m2m": ["client_id", "client_secret"],
    "azure_cli": [],
    "gcp_login": [],
}

# Well-known env var names for credential hints
_ENV_VAR_HINTS: dict[str, str] = {
    "token": "DATABRICKS_TOKEN",
    "client_id": "DATABRICKS_CLIENT_ID",
    "client_secret": "DATABRICKS_CLIENT_SECRET",
}

# Map Unity Catalog type_text to Arrow type names
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
    "numeric": "float64",
    "boolean": "bool",
    "string": "large_utf8",
    "varchar": "large_utf8",
    "char": "large_utf8",
    "binary": "large_binary",
    "bytes": "large_binary",
    "date": "date32",
    "timestamp": "timestamp[us]",
    "timestamp_ntz": "timestamp[us]",
    "interval": "duration[us]",
    "void": "null",
}


def _unity_type_to_arrow(type_text: str) -> str:
    """Map a Unity Catalog type_text string to an Arrow type name."""
    lower = (type_text or "").lower().strip()
    # Handle parameterized types like decimal(10,2), varchar(255)
    base = lower.split("(")[0].strip()
    return _UNITY_TO_ARROW.get(base, "large_utf8")


class UnityCatalogPlugin(CatalogPlugin):
    type = "unity"
    required_options: list[str] = _REQUIRED_OPTIONS
    optional_options: dict[str, Any] = _OPTIONAL_OPTIONS
    credential_options: list[str] = _CREDENTIAL_OPTIONS
    credential_groups: dict[str, list[str]] = _CREDENTIAL_GROUPS
    env_var_hints: dict[str, str] = _ENV_VAR_HINTS

    def __init__(self) -> None:
        # Per-table cache: (host, full_name) → table metadata dict
        self._table_cache: dict[tuple[str, str], dict[str, Any]] = {}

    def validate(self, options: dict[str, Any]) -> None:
        for key in options:
            if key not in _KNOWN_OPTIONS:
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"Unknown option '{key}' for unity catalog.",
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
                        f"Missing required option '{key}' for unity catalog.",
                        plugin_name="rivet_databricks",
                        plugin_type="catalog",
                        remediation=f"Provide '{key}' in the catalog options.",
                        missing_option=key,
                    )
                )
        auth_type = options.get("auth_type")
        if auth_type is not None and auth_type not in _VALID_AUTH_TYPES:
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"Invalid auth_type '{auth_type}' for unity catalog.",
                    plugin_name="rivet_databricks",
                    plugin_type="catalog",
                    remediation=f"Valid auth_type values: {', '.join(sorted(_VALID_AUTH_TYPES))}",
                    auth_type=auth_type,
                )
            )

    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog:
        self.validate(options)
        return Catalog(name=name, type="unity", options=options)

    def resolve_credentials(
        self,
        options: dict[str, Any],
        config_path: Path | None = None,
    ) -> ResolvedCredential:
        """Resolve credentials using the 4-step chain: explicit → env → ~/.databrickscfg → cloud-native."""
        host = options.get("host")
        return resolve_credentials(options, host=host, config_path=config_path)

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        catalog_name = options["catalog_name"]
        schema = options.get("schema", "default")
        return f"{catalog_name}.{schema}.{logical_name}"

    def resolve_table_reference(self, full_name: str, catalog: Catalog) -> dict[str, Any]:  # type: ignore[override]
        """Fetch table metadata from Unity Catalog REST API with per-table caching.

        Args:
            full_name: Three-part table name (catalog.schema.table).
            catalog: The Catalog instance containing host and credential options.

        Returns:
            Dict with keys: storage_location, file_format, columns,
            partition_columns, table_type, temporary_credentials.
        """
        from rivet_databricks.client import UnityCatalogClient

        host = catalog.options["host"]
        cache_key = (host, full_name)
        if cache_key in self._table_cache:
            return self._table_cache[cache_key]

        credential = self.resolve_credentials(catalog.options)
        client = UnityCatalogClient(host=host, credential=credential)
        try:
            raw = client.get_table(full_name)
            temporary_credentials = self._vend_credentials_for_table(client, raw, full_name)
        finally:
            client.close()

        result: dict[str, Any] = {
            "storage_location": raw.get("storage_location"),
            "file_format": raw.get("data_source_format"),
            "columns": raw.get("columns", []),
            "partition_columns": raw.get("partition_columns", []),
            "table_type": raw.get("table_type"),
            "temporary_credentials": temporary_credentials,
        }
        self._table_cache[cache_key] = result
        return result

    def vend_credentials(
        self, full_name: str, catalog: Catalog, operation: str = "READ"
    ) -> dict[str, Any] | None:
        """Vend temporary scoped storage credentials for a table.

        Args:
            full_name: Three-part table name (catalog.schema.table).
            catalog: The Catalog instance containing host and credential options.
            operation: "READ" or "READ_WRITE".

        Returns:
            Credential dict from Unity Catalog, or None if vending is unavailable.
        """
        from rivet_databricks.client import UnityCatalogClient

        host = catalog.options["host"]
        credential = self.resolve_credentials(catalog.options)
        client = UnityCatalogClient(host=host, credential=credential)
        try:
            raw = client.get_table(full_name)
            return self._vend_credentials_for_table(client, raw, full_name, operation=operation)
        finally:
            client.close()

    @staticmethod
    def _vend_credentials_for_table(
        client: Any,
        raw: dict[str, Any],
        full_name: str,
        operation: str = "READ",
    ) -> dict[str, Any] | None:
        """Call credential vending for a table, returning None on HTTP 403."""
        table_id = raw.get("table_id") or raw.get("full_name") or full_name
        try:
            return client.vend_credentials(table_id, operation=operation)  # type: ignore[no-any-return]
        except ExecutionError as exc:
            if exc.error.code == "RVT-508":
                warnings.warn(
                    f"Credential vending unavailable for '{full_name}': {exc.error.message} "
                    "Falling back to ambient cloud credentials.",
                    stacklevel=4,
                )
                return None
            raise

    # ── Introspection (task 22.6): 4-level hierarchy ──────────────────

    def list_tables(self, catalog: Catalog) -> list[CatalogNode]:
        """List all tables across the 4-level hierarchy: catalog → schema → table → column.

        Uses REST endpoints: GET /catalogs, GET /schemas, GET /tables.
        Returns CatalogNode entries for catalogs, schemas, and tables.
        """
        from rivet_core.introspection import CatalogNode, NodeSummary
        from rivet_databricks.client import UnityCatalogClient

        host = catalog.options["host"]
        credential = self.resolve_credentials(catalog.options)
        client = UnityCatalogClient(host=host, credential=credential)
        nodes: list[CatalogNode] = []
        try:
            catalogs = client.list_catalogs()
            for cat in catalogs:
                cat_name = cat.get("name", "")
                schemas = client.list_schemas(cat_name)
                nodes.append(
                    CatalogNode(
                        name=cat_name,
                        node_type="catalog",
                        path=[cat_name],
                        is_container=True,
                        children_count=len(schemas),
                        summary=NodeSummary(
                            row_count=None,
                            size_bytes=None,
                            format=None,
                            last_modified=None,
                            owner=cat.get("owner"),
                            comment=cat.get("comment"),
                        ),
                    )
                )
                for schema in schemas:
                    schema_name = schema.get("name", "")
                    tables = client.list_tables(cat_name, schema_name)
                    nodes.append(
                        CatalogNode(
                            name=schema_name,
                            node_type="schema",
                            path=[cat_name, schema_name],
                            is_container=True,
                            children_count=len(tables),
                            summary=NodeSummary(
                                row_count=None,
                                size_bytes=None,
                                format=None,
                                last_modified=None,
                                owner=schema.get("owner"),
                                comment=schema.get("comment"),
                            ),
                        )
                    )
                    for table in tables:
                        table_name = table.get("name", "")
                        nodes.append(
                            CatalogNode(
                                name=table_name,
                                node_type="table",
                                path=[cat_name, schema_name, table_name],
                                is_container=False,
                                children_count=None,
                                summary=NodeSummary(
                                    row_count=None,
                                    size_bytes=None,
                                    format=table.get("data_source_format"),
                                    last_modified=None,
                                    owner=table.get("owner"),
                                    comment=table.get("comment"),
                                ),
                            )
                        )
        finally:
            client.close()
        return nodes
    def test_connection(self, catalog: Catalog) -> None:
        """Lightweight connectivity check — single GET /catalogs call."""
        from rivet_databricks.client import UnityCatalogClient

        host = catalog.options["host"]
        credential = self.resolve_credentials(catalog.options)
        client = UnityCatalogClient(host=host, credential=credential)
        try:
            client.list_catalogs()
        finally:
            client.close()

    def list_children(self, catalog: Catalog, path: list[str]) -> list[CatalogNode]:
        """Lazy single-level listing — only fetches immediate children of path.

        - path=[] → list_catalogs()
        - path=[catalog] → list_schemas(catalog)
        - path=[catalog, schema] → list_tables(catalog, schema)
        - path=[catalog, schema, table] → columns via get_schema()
        """
        from rivet_core.introspection import CatalogNode, NodeSummary
        from rivet_databricks.client import UnityCatalogClient

        host = catalog.options["host"]
        credential = self.resolve_credentials(catalog.options)
        client = UnityCatalogClient(host=host, credential=credential)
        nodes: list[CatalogNode] = []
        try:
            if len(path) == 0:
                # Level 0: list Unity catalogs
                for cat in client.list_catalogs():
                    cat_name = cat.get("name", "")
                    nodes.append(CatalogNode(
                        name=cat_name,
                        node_type="catalog",
                        path=[cat_name],
                        is_container=True,
                        children_count=None,
                        summary=NodeSummary(
                            row_count=None, size_bytes=None, format=None,
                            last_modified=None, owner=cat.get("owner"),
                            comment=cat.get("comment"),
                        ),
                    ))
            elif len(path) == 1:
                # Level 1: list schemas in a catalog
                cat_name = path[0]
                for schema in client.list_schemas(cat_name):
                    schema_name = schema.get("name", "")
                    nodes.append(CatalogNode(
                        name=schema_name,
                        node_type="schema",
                        path=[cat_name, schema_name],
                        is_container=True,
                        children_count=None,
                        summary=NodeSummary(
                            row_count=None, size_bytes=None, format=None,
                            last_modified=None, owner=schema.get("owner"),
                            comment=schema.get("comment"),
                        ),
                    ))
            elif len(path) == 2:
                # Level 2: list tables in a schema
                cat_name, schema_name = path[0], path[1]
                for table in client.list_tables(cat_name, schema_name):
                    table_name = table.get("name", "")
                    nodes.append(CatalogNode(
                        name=table_name,
                        node_type="table",
                        path=[cat_name, schema_name, table_name],
                        is_container=False,
                        children_count=None,
                        summary=NodeSummary(
                            row_count=None, size_bytes=None, format=None,
                            last_modified=None,
                            owner=table.get("owner"),
                            comment=table.get("comment"),
                        ),
                    ))
            # Level 3+ (columns): handled by CatalogExplorer via get_schema()
        finally:
            client.close()
        return nodes



    def get_schema(self, catalog: Catalog, table: str) -> ObjectSchema:
        """Get schema for a table via GET /tables/{full_name}.

        Maps Unity column type_text to Arrow types. Unmapped types default to large_utf8.
        """
        from rivet_core.introspection import ColumnDetail, ObjectSchema
        from rivet_databricks.client import UnityCatalogClient

        host = catalog.options["host"]
        credential = self.resolve_credentials(catalog.options)
        client = UnityCatalogClient(host=host, credential=credential)
        try:
            raw = client.get_table(table)
        finally:
            client.close()

        columns_raw = raw.get("columns", [])
        partition_cols = {c.get("name") for c in raw.get("partition_columns", [])}
        columns = []
        for col in columns_raw:
            col_name = col.get("name", "")
            type_text = col.get("type_text", "")
            arrow_type = _unity_type_to_arrow(type_text)
            if arrow_type == "large_utf8" and type_text and type_text.lower().strip().split("(")[0] not in _UNITY_TO_ARROW:
                warnings.warn(
                    f"Unity Catalog type '{type_text}' for column '{col_name}' in '{table}' "
                    "has no Arrow mapping; defaulting to large_utf8.",
                    stacklevel=2,
                )
            columns.append(
                ColumnDetail(
                    name=col_name,
                    type=arrow_type,
                    native_type=type_text or None,
                    nullable=col.get("nullable", True),
                    default=col.get("default_value"),
                    comment=col.get("comment"),
                    is_primary_key=False,
                    is_partition_key=col_name in partition_cols,
                )
            )

        path = table.split(".")
        return ObjectSchema(
            path=path,
            node_type=raw.get("table_type", "table").lower(),
            columns=columns,
            primary_key=None,
            comment=raw.get("comment"),
        )

    def get_metadata(self, catalog: Catalog, table: str) -> ObjectMetadata | None:
        """Get metadata for a table via GET /tables/{full_name}.

        Returns data_source_format, storage_location, updated_at, owner, comment, table_type.
        """
        from datetime import datetime

        from rivet_core.introspection import ObjectMetadata
        from rivet_databricks.client import UnityCatalogClient

        host = catalog.options["host"]
        credential = self.resolve_credentials(catalog.options)
        client = UnityCatalogClient(host=host, credential=credential)
        try:
            raw = client.get_table(table)
        finally:
            client.close()

        # updated_at and created_at may be epoch milliseconds (int) or ISO strings
        def _parse_ts(val: Any) -> datetime | None:
            if val is None:
                return None
            if isinstance(val, (int, float)):
                return datetime.fromtimestamp(val / 1000, tz=UTC)
            try:
                return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
            except (ValueError, TypeError):
                return None

        path = table.split(".")
        return ObjectMetadata(
            path=path,
            node_type=raw.get("table_type", "table").lower(),
            row_count=raw.get("properties", {}).get("numRows") if isinstance(raw.get("properties"), dict) else None,
            size_bytes=raw.get("properties", {}).get("sizeInBytes") if isinstance(raw.get("properties"), dict) else None,
            last_modified=_parse_ts(raw.get("updated_at")),
            created_at=_parse_ts(raw.get("created_at")),
            format=raw.get("data_source_format"),
            compression=None,
            owner=raw.get("owner"),
            comment=raw.get("comment"),
            location=raw.get("storage_location"),
            column_statistics=[],
            partitioning=None,
            properties={k: str(v) for k, v in raw.get("properties", {}).items()} if isinstance(raw.get("properties"), dict) else {},
        )
