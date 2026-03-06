"""Glue catalog plugin for Rivet."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError

from rivet_core.errors import PluginValidationError, plugin_error
from rivet_core.models import Catalog
from rivet_core.plugins import CatalogPlugin

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from rivet_core.introspection import CatalogNode, ObjectMetadata, ObjectSchema

_REQUIRED_OPTIONS: list[str] = []
_OPTIONAL_OPTIONS: dict[str, Any] = {
    "database": None,
    "region": "us-east-1",
    "catalog_id": None,
    "lf_enabled": False,
}
_CREDENTIAL_OPTIONS = [
    "access_key_id",
    "secret_access_key",
    "session_token",
    "profile",
    "role_arn",
    "role_session_name",
    "web_identity_token_file",
    "credential_cache",
    "auth_type",
]
_VALID_AUTH_TYPES = frozenset({
    "iam_keys", "profile", "assume_role", "web_identity", "default",
})

# Maps auth_type → which credential options are relevant
_CREDENTIAL_GROUPS: dict[str, list[str]] = {
    "iam_keys": ["access_key_id", "secret_access_key"],
    "profile": ["profile"],
    "assume_role": ["role_arn"],
    "web_identity": ["role_arn", "web_identity_token_file"],
    "default": [],
}

# Well-known env var names for credential hints
_ENV_VAR_HINTS: dict[str, str] = {
    "access_key_id": "AWS_ACCESS_KEY_ID",
    "secret_access_key": "AWS_SECRET_ACCESS_KEY",
    "session_token": "AWS_SESSION_TOKEN",
    "profile": "AWS_PROFILE",
    "role_arn": "AWS_ROLE_ARN",
    "web_identity_token_file": "AWS_WEB_IDENTITY_TOKEN_FILE",
}

_KNOWN_OPTIONS = (
    set(_REQUIRED_OPTIONS) | set(_OPTIONAL_OPTIONS) | set(_CREDENTIAL_OPTIONS) | {"table_map"}
)


def _credential_resolver_factory(options: dict[str, Any], region: str) -> Any:
    """Create an AWSCredentialResolver from catalog options."""
    from rivet_aws.credentials import AWSCredentialResolver

    return AWSCredentialResolver(options, region)

# Map Glue/Hive column types to Arrow type names
_GLUE_TO_ARROW: dict[str, str] = {
    "bigint": "int64",
    "int": "int32",
    "integer": "int32",
    "smallint": "int16",
    "tinyint": "int8",
    "float": "float32",
    "double": "float64",
    "decimal": "float64",
    "boolean": "bool",
    "string": "large_utf8",
    "varchar": "large_utf8",
    "char": "large_utf8",
    "binary": "large_binary",
    "date": "date32",
    "timestamp": "timestamp[us]",
    "array": "large_utf8",
    "map": "large_utf8",
    "struct": "large_utf8",
}


def _glue_type_to_arrow(native_type: str) -> str:
    lower = native_type.lower().strip()
    # Strip precision/scale: decimal(10,2) → decimal, varchar(255) → varchar
    base = lower.split("(")[0].strip()
    return _GLUE_TO_ARROW.get(base, "large_utf8")


def _resolve_database(catalog: Catalog, table: str | None = None) -> tuple[str, str | None]:
    """Return (database, table) resolving from options or 'db.table' notation.

    When catalog.options["database"] is set, returns (database, table).
    When it's None and table contains a dot, splits as 'database.table'.
    When it's None and table has no dot, raises PluginValidationError.
    If table is None, returns (database_or_none, None).
    """
    db = catalog.options.get("database")
    if db:
        return db, table
    if table is None:
        return db, None  # type: ignore[return-value]
    if "." in table:
        parts = table.split(".", 1)
        return parts[0], parts[1]
    raise PluginValidationError(
        plugin_error(
            "RVT-201",
            f"No database configured and table '{table}' does not include a database prefix. "
            f"Use 'database.table' notation or set 'database' in catalog options.",
            plugin_name="rivet_aws",
            plugin_type="catalog",
            remediation="Either set 'database' in catalog options or use 'database.table_name' format.",
        )
    )


def _make_glue_client(catalog: Catalog) -> Any:
    from rivet_aws.credentials import AWSCredentialResolver

    region = catalog.options.get("region", "us-east-1")
    resolver = AWSCredentialResolver(catalog.options, region)
    return resolver.create_client("glue")


def _make_lf_client(catalog: Catalog) -> Any:
    from rivet_aws.credentials import AWSCredentialResolver

    region = catalog.options.get("region", "us-east-1")
    resolver = AWSCredentialResolver(catalog.options, region)
    return resolver.create_client("lakeformation")


def get_lf_credentials(lf_client: Any, catalog: Catalog, table: str) -> dict[str, Any]:
    """Call GetTemporaryGlueTableCredentials and return the credential dict."""
    database, table = _resolve_database(catalog, table)  # type: ignore[assignment]
    kwargs: dict[str, Any] = {
        "DatabaseName": database,
        "TableName": table,
        "Permissions": ["SELECT"],
    }
    catalog_id = catalog.options.get("catalog_id")
    if catalog_id:
        kwargs["CatalogId"] = catalog_id
    return lf_client.get_temporary_glue_table_credentials(**kwargs)  # type: ignore[no-any-return]


def _make_glue_client_for_table(catalog: Catalog, table: str) -> Any:
    """Return a Glue client, using LF-vended credentials when lf_enabled=True."""
    if not catalog.options.get("lf_enabled", False):
        return _make_glue_client(catalog)

    import boto3

    region = catalog.options.get("region", "us-east-1")
    lf_client = _make_lf_client(catalog)
    creds = get_lf_credentials(lf_client, catalog, table)
    session = boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name=region,
    )
    return session.client("glue", region_name=region)


def _warn_if_parquet_schema_diverges(
    catalog: Catalog, table: str, location: str, glue_schema: Any
) -> None:
    """Log a WARNING if the Glue StorageDescriptor schema diverges from the Parquet footer."""
    try:
        from rivet_aws.s3_catalog import _build_s3fs

        # Parse s3://bucket/key from location
        if not location.startswith("s3://"):
            return
        s3_path = location[5:].rstrip("/")
        fs = _build_s3fs(catalog.options)

        import pyarrow.fs as pafs

        # Find a Parquet file under the location
        selector = pafs.FileSelector(s3_path, allow_not_found=True, recursive=False)
        file_infos = fs.get_file_info(selector)
        parquet_file = None
        for fi in file_infos:
            if fi.type == pafs.FileType.File and fi.path.endswith(".parquet"):
                parquet_file = fi.path
                break
        if not parquet_file:
            return

        import pyarrow.parquet as pq

        with fs.open_input_file(parquet_file) as f:
            footer_schema = pq.read_schema(f)

        # Compare: check column names and types (excluding partition columns)
        # Normalize type aliases (e.g. large_string == large_utf8)
        _TYPE_ALIASES = {"large_string": "large_utf8", "utf8": "string"}

        def _normalize(t: str) -> str:
            return _TYPE_ALIASES.get(t, t)

        glue_cols = [(c.name, _normalize(c.type)) for c in glue_schema.columns if not c.is_partition_key]
        footer_cols = [(field.name, _normalize(str(field.type))) for field in footer_schema]

        if glue_cols != footer_cols:
            logger.warning(
                "Glue table '%s' StorageDescriptor schema diverges from Parquet footer. "
                "Glue columns: %s, Parquet footer columns: %s",
                table,
                glue_cols,
                footer_cols,
            )
    except Exception:
        # Best-effort: never block get_schema() on divergence check failure
        pass


class GlueCatalogPlugin(CatalogPlugin):
    type = "glue"
    required_options: list[str] = _REQUIRED_OPTIONS
    optional_options: dict[str, Any] = _OPTIONAL_OPTIONS
    credential_options: list[str] = _CREDENTIAL_OPTIONS
    credential_groups: dict[str, list[str]] = _CREDENTIAL_GROUPS
    env_var_hints: dict[str, str] = _ENV_VAR_HINTS

    def validate(self, options: dict[str, Any]) -> None:
        for key in options:
            if key not in _KNOWN_OPTIONS:
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"Unknown option '{key}' for glue catalog.",
                        plugin_name="rivet_aws",
                        plugin_type="catalog",
                        remediation=f"Valid options: {', '.join(sorted(_KNOWN_OPTIONS - {'table_map'}))}",
                    )
                )
        for key in _REQUIRED_OPTIONS:
            if key not in options:
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"Missing required option '{key}' for glue catalog.",
                        plugin_name="rivet_aws",
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
                    f"Invalid auth_type '{auth_type}' for glue catalog.",
                    plugin_name="rivet_aws",
                    plugin_type="catalog",
                    remediation=f"Valid auth_type values: {', '.join(sorted(_VALID_AUTH_TYPES))}",
                    auth_type=auth_type,
                )
            )

    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog:
        self.validate(options)
        opts = {**options, "_credential_resolver_factory": _credential_resolver_factory}
        return Catalog(name=name, type="glue", options=opts)

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        return logical_name

    def test_connection(self, catalog: Catalog) -> None:
        """Lightweight Glue connectivity check."""
        client = _make_glue_client(catalog)
        database = catalog.options.get("database")
        catalog_id = catalog.options.get("catalog_id")
        if database:
            kwargs: dict[str, Any] = {"Name": database}
            if catalog_id:
                kwargs["CatalogId"] = catalog_id
            client.get_database(**kwargs)
        else:
            # No database configured — just verify we can reach Glue
            kwargs = {}
            if catalog_id:
                kwargs["CatalogId"] = catalog_id
            client.get_databases(**kwargs)

    def _list_databases(self, catalog: Catalog) -> list[str]:
        """Return all Glue database names visible to this catalog."""
        client = _make_glue_client(catalog)
        catalog_id = catalog.options.get("catalog_id")
        kwargs: dict[str, Any] = {}
        if catalog_id:
            kwargs["CatalogId"] = catalog_id
        databases: list[str] = []
        try:
            paginator = client.get_paginator("get_databases")
            for page in paginator.paginate(**kwargs):
                for db in page.get("DatabaseList", []):
                    databases.append(db["Name"])
        except ClientError as exc:
            from rivet_aws.errors import handle_glue_error
            raise handle_glue_error(exc, database=None, action="glue:GetDatabases") from exc  # type: ignore[arg-type]
        return databases

    def _list_tables_in_database(
        self, catalog: Catalog, database: str
    ) -> list[CatalogNode]:
        """List tables in a single Glue database."""
        from rivet_core.introspection import CatalogNode, NodeSummary

        client = _make_glue_client(catalog)
        catalog_id = catalog.options.get("catalog_id")

        kwargs: dict[str, Any] = {"DatabaseName": database}
        if catalog_id:
            kwargs["CatalogId"] = catalog_id

        nodes: list[CatalogNode] = []
        try:
            paginator = client.get_paginator("get_tables")
            for page in paginator.paginate(**kwargs):
                for tbl in page.get("TableList", []):
                    table_name = tbl["Name"]
                    sd = tbl.get("StorageDescriptor", {})
                    params = tbl.get("Parameters", {})
                    size_bytes_str = params.get("totalSize") or params.get("rawDataSize")
                    size_bytes = int(size_bytes_str) if size_bytes_str else None
                    row_count_str = params.get("numRows")
                    row_count = int(row_count_str) if row_count_str else None
                    fmt = params.get("classification") or _input_format_to_name(
                        sd.get("InputFormat", "")
                    )
                    nodes.append(
                        CatalogNode(
                            name=table_name,
                            node_type="table",
                            path=[catalog.name, database, table_name],
                            is_container=False,
                            children_count=None,
                            summary=NodeSummary(
                                row_count=row_count,
                                size_bytes=size_bytes,
                                format=fmt,
                                last_modified=None,
                                owner=tbl.get("Owner"),
                                comment=tbl.get("Description") or tbl.get("Comment"),
                            ),
                        )
                    )
        except ClientError as exc:
            from rivet_aws.errors import handle_glue_error
            raise handle_glue_error(exc, database=database, action="glue:GetTables") from exc
        return nodes

    def list_tables(self, catalog: Catalog) -> list[CatalogNode]:
        database = catalog.options.get("database")
        if database:
            return self._list_tables_in_database(catalog, database)
        # No database configured — list tables across all databases
        nodes: list[CatalogNode] = []
        for db in self._list_databases(catalog):
            nodes.extend(self._list_tables_in_database(catalog, db))
        return nodes

    def get_schema(self, catalog: Catalog, table: str) -> ObjectSchema:
        from rivet_core.introspection import ColumnDetail, ObjectSchema

        database, table = _resolve_database(catalog, table)  # type: ignore[assignment]
        client = _make_glue_client_for_table(catalog, table)
        catalog_id = catalog.options.get("catalog_id")

        kwargs: dict[str, Any] = {"DatabaseName": database, "Name": table}
        if catalog_id:
            kwargs["CatalogId"] = catalog_id

        try:
            resp = client.get_table(**kwargs)
        except ClientError as exc:
            from rivet_aws.errors import handle_glue_error
            raise handle_glue_error(exc, database=database, table=table, action="glue:GetTable") from exc
        tbl = resp["Table"]
        sd = tbl.get("StorageDescriptor", {})

        # Check for unsupported SerDe
        serde_info = sd.get("SerdeInfo", {})
        serde_lib = serde_info.get("SerializationLibrary")
        if not _is_supported_serde(serde_lib):
            raise PluginValidationError(
                plugin_error(
                    "RVT-872",
                    f"Unsupported SerDe format '{serde_lib}' for table '{table}'.",
                    plugin_name="rivet_aws",
                    plugin_type="catalog",
                    remediation=f"Table '{table}' uses SerDe '{serde_lib}' which is not supported for schema introspection.",
                    table=table,
                    serde_format=serde_lib,
                )
            )
        glue_cols = sd.get("Columns", [])
        partition_keys = {pk["Name"] for pk in tbl.get("PartitionKeys", [])}

        columns = []
        for col in glue_cols:
            col_name = col["Name"]
            col_type = col.get("Type", "string")
            columns.append(
                ColumnDetail(
                    name=col_name,
                    type=_glue_type_to_arrow(col_type),
                    native_type=col_type,
                    nullable=True,
                    default=None,
                    comment=col.get("Comment"),
                    is_primary_key=False,
                    is_partition_key=col_name in partition_keys,
                )
            )
        # Append partition key columns at the end
        for pk in tbl.get("PartitionKeys", []):
            pk_name = pk["Name"]
            pk_type = pk.get("Type", "string")
            columns.append(
                ColumnDetail(
                    name=pk_name,
                    type=_glue_type_to_arrow(pk_type),
                    native_type=pk_type,
                    nullable=True,
                    default=None,
                    comment=pk.get("Comment"),
                    is_primary_key=False,
                    is_partition_key=True,
                )
            )

        schema_obj = ObjectSchema(
            path=[catalog.name, database, table],
            node_type="table",
            columns=columns,
            primary_key=None,
            comment=tbl.get("Description") or tbl.get("Comment"),
        )

        # Best-effort: warn if Glue schema diverges from actual Parquet footer
        location = sd.get("Location")
        if location:
            _warn_if_parquet_schema_diverges(catalog, table, location, schema_obj)

        return schema_obj

    def get_metadata(self, catalog: Catalog, table: str) -> ObjectMetadata | None:
        from rivet_core.introspection import (
            ObjectMetadata,
            PartitionInfo,
            PartitionValue,
        )

        database, table = _resolve_database(catalog, table)  # type: ignore[assignment]
        client = _make_glue_client_for_table(catalog, table)
        catalog_id = catalog.options.get("catalog_id")

        kwargs: dict[str, Any] = {"DatabaseName": database, "Name": table}
        if catalog_id:
            kwargs["CatalogId"] = catalog_id

        try:
            resp = client.get_table(**kwargs)
        except ClientError as exc:
            from rivet_aws.errors import handle_glue_error
            raise handle_glue_error(exc, database=database, table=table, action="glue:GetTable") from exc
        tbl = resp["Table"]
        sd = tbl.get("StorageDescriptor", {})
        params = tbl.get("Parameters", {})
        location = sd.get("Location")

        size_bytes_str = params.get("totalSize") or params.get("rawDataSize")
        size_bytes = int(size_bytes_str) if size_bytes_str else None
        row_count_str = params.get("numRows")
        row_count = int(row_count_str) if row_count_str else None
        fmt = params.get("classification") or _input_format_to_name(sd.get("InputFormat", ""))
        compression = params.get("compressionType") or sd.get("Compressed") and "compressed" or None

        # Fetch partitions
        partition_keys = [pk["Name"] for pk in tbl.get("PartitionKeys", [])]
        partitioning = None
        if partition_keys:
            part_kwargs: dict[str, Any] = {"DatabaseName": database, "TableName": table}
            if catalog_id:
                part_kwargs["CatalogId"] = catalog_id

            partition_values: list[PartitionValue] = []
            try:
                paginator = client.get_paginator("get_partitions")
                for page in paginator.paginate(**part_kwargs):
                    for part in page.get("Partitions", []):
                        psd = part.get("StorageDescriptor", {})
                        pparams = part.get("Parameters", {})
                        p_size_str = pparams.get("totalSize") or pparams.get("rawDataSize")
                        p_size = int(p_size_str) if p_size_str else None
                        p_rows_str = pparams.get("numRows")
                        p_rows = int(p_rows_str) if p_rows_str else None
                        p_location = psd.get("Location")
                        values_list = part.get("Values", [])
                        values_dict = dict(zip(partition_keys, values_list))
                        partition_values.append(
                            PartitionValue(
                                values=values_dict,
                                row_count=p_rows,
                                size_bytes=p_size,
                                last_modified=None,
                                location=p_location,
                            )
                        )
            except ClientError as exc:
                from rivet_aws.errors import handle_glue_error
                raise handle_glue_error(exc, database=database, table=table, action="glue:GetPartitions") from exc
            partitioning = PartitionInfo(columns=partition_keys, partitions=partition_values)

        return ObjectMetadata(
            path=[catalog.name, database, table],
            node_type="table",
            row_count=row_count,
            size_bytes=size_bytes,
            last_modified=None,
            created_at=None,
            format=fmt,
            compression=compression if isinstance(compression, str) else None,
            owner=tbl.get("Owner"),
            comment=tbl.get("Description") or tbl.get("Comment"),
            location=location,
            column_statistics=[],
            partitioning=partitioning,
            properties=dict(params),
        )

    def list_children(self, catalog: Catalog, path: list[str]) -> list[CatalogNode]:
        from rivet_core.introspection import CatalogNode, NodeSummary

        database = catalog.options.get("database")
        depth = len(path)

        # Level 0 / 1: show database(s)
        if depth <= 1:
            if database:
                return [
                    CatalogNode(
                        name=database,
                        node_type="database",
                        path=[catalog.name, database],
                        is_container=True,
                        children_count=None,
                        summary=None,
                    )
                ]
            # No database configured — list all databases
            return [
                CatalogNode(
                    name=db,
                    node_type="database",
                    path=[catalog.name, db],
                    is_container=True,
                    children_count=None,
                    summary=None,
                )
                for db in self._list_databases(catalog)
            ]

        # Level 2: [catalog_name, database] → list tables in that database
        if depth == 2:
            db_name = path[1]
            return self._list_tables_in_database(catalog, db_name)

        # Level 3: [catalog_name, database, table] → list columns
        if depth == 3:
            db_name = path[1]
            table_name = path[2]
            # Use db.table notation so _resolve_database picks it up
            qualified = f"{db_name}.{table_name}" if not database else table_name
            try:
                schema = self.get_schema(catalog, qualified)
            except Exception:
                return []
            return [
                CatalogNode(
                    name=col.name,
                    node_type="column",
                    path=[catalog.name, db_name, table_name, col.name],
                    is_container=False,
                    children_count=None,
                    summary=NodeSummary(
                        row_count=None,
                        size_bytes=None,
                        format=col.type,
                        last_modified=None,
                        owner=None,
                        comment=col.comment,
                    ),
                )
                for col in schema.columns
            ]

        return []


def _input_format_to_name(input_format: str) -> str | None:
    """Map Glue InputFormat class name to a human-readable format name."""
    if not input_format:
        return None
    lower = input_format.lower()
    if "parquet" in lower:
        return "parquet"
    if "orc" in lower:
        return "orc"
    if "avro" in lower:
        return "avro"
    if "json" in lower:
        return "json"
    if "text" in lower or "csv" in lower:
        return "csv"
    return None


_SUPPORTED_SERDE_KEYWORDS = {"parquet", "orc", "json", "lazy", "csv", "opencsv", "regex"}


def _is_supported_serde(serde_lib: str | None) -> bool:
    """Return True if the SerDe library is one we can introspect."""
    if not serde_lib:
        return True  # No SerDe info → treat as supported (best-effort)
    lower = serde_lib.lower()
    return any(kw in lower for kw in _SUPPORTED_SERDE_KEYWORDS)
