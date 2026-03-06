"""GlueDuckDBAdapter: read/write Glue-registered tables via DuckDB + httpfs.

Uses boto3 GetTable/GetPartitions for metadata, applies partition pruning,
configures DuckDB S3 credentials via secret manager, and reads via httpfs.
"""

from __future__ import annotations

from typing import Any

import pyarrow

from rivet_core.credentials import CredentialResolver
from rivet_core.errors import ExecutionError, plugin_error
from rivet_core.models import Column, Material, Schema
from rivet_core.optimizer import AdapterPushdownResult, PushdownPlan
from rivet_core.plugins import ComputeEngineAdapter
from rivet_core.strategies import MaterializedRef
from rivet_duckdb.adapters.pushdown import _apply_duckdb_pushdown

ALL_6_CAPABILITIES = [
    "projection_pushdown",
    "predicate_pushdown",
    "limit_pushdown",
    "cast_pushdown",
    "join",
    "aggregation",
]

_FORMAT_TO_READER: dict[str, str] = {
    "parquet": "read_parquet",
    "csv": "read_csv_auto",
    "json": "read_json_auto",
    "orc": "read_parquet",  # DuckDB doesn't natively read ORC; fallback
}


def _glue_input_format_to_reader(input_format: str) -> str:
    """Map Glue InputFormat class name to a DuckDB reader function."""
    lower = input_format.lower()
    if "parquet" in lower:
        return "read_parquet"
    if "orc" in lower:
        return "read_parquet"
    if "json" in lower:
        return "read_json_auto"
    if "text" in lower or "csv" in lower:
        return "read_csv_auto"
    return "read_parquet"


def _make_resolver(catalog_options: dict[str, Any]) -> CredentialResolver:
    """Create a CredentialResolver from the factory injected by the catalog plugin."""
    factory = catalog_options.get("_credential_resolver_factory")
    if factory is None:
        raise ExecutionError(
            plugin_error(
                "RVT-501",
                "No credential resolver factory in catalog options.",
                plugin_name="rivet_duckdb",
                plugin_type="adapter",
                adapter="GlueDuckDBAdapter",
                remediation="Ensure the Glue catalog plugin is registered.",
            )
        )
    region = catalog_options.get("region", "us-east-1")
    return factory(catalog_options, region)  # type: ignore[no-any-return]


def _configure_s3_secret(conn: Any, catalog_options: dict[str, Any]) -> None:
    """Configure DuckDB S3 credentials from catalog options via secret manager."""
    region = catalog_options.get("region", "us-east-1")
    creds = _make_resolver(catalog_options).resolve()

    conn.execute("DROP SECRET IF EXISTS glue_s3_secret")
    parts = [
        "CREATE SECRET glue_s3_secret (TYPE S3",
        f", KEY_ID '{creds.access_key_id}'",
        f", SECRET '{creds.secret_access_key}'",
        f", REGION '{region}'",
    ]
    if creds.session_token:
        parts.append(f", SESSION_TOKEN '{creds.session_token}'")
    parts.append(")")
    conn.execute("".join(parts))


def _matches_partition_filter(
    partition_values: dict[str, str], partition_filter: dict[str, Any]
) -> bool:
    """Return True if partition_values satisfies all key=value constraints."""
    return all(partition_values.get(key) == str(value) for key, value in partition_filter.items())


def _resolve_glue_table(
    catalog_options: dict[str, Any],
    table_name: str,
    partition_filter: dict[str, Any] | None,
) -> tuple[str, str, list[str], list[str]]:
    """Resolve Glue table metadata with DuckDB-specific error context."""
    client = _make_resolver(catalog_options).create_client("glue")
    database = catalog_options["database"]
    catalog_id = catalog_options.get("catalog_id")

    kwargs: dict[str, Any] = {"DatabaseName": database, "Name": table_name}
    if catalog_id:
        kwargs["CatalogId"] = catalog_id

    try:
        resp = client.get_table(**kwargs)
    except Exception as exc:
        raise ExecutionError(
            plugin_error(
                "RVT-503",
                f"Glue table '{table_name}' not found in database '{database}': {exc}",
                plugin_name="rivet_duckdb",
                plugin_type="adapter",
                adapter="GlueDuckDBAdapter",
                remediation="Check that the table exists in the Glue catalog.",
                table=table_name,
                database=database,
            )
        ) from exc

    tbl = resp["Table"]
    sd = tbl.get("StorageDescriptor", {})
    location = sd.get("Location", "")
    input_format = sd.get("InputFormat", "")
    partition_keys = [pk["Name"] for pk in tbl.get("PartitionKeys", [])]

    partition_locations: list[str] = []
    if partition_keys:
        part_kwargs: dict[str, Any] = {"DatabaseName": database, "TableName": table_name}
        if catalog_id:
            part_kwargs["CatalogId"] = catalog_id

        paginator = client.get_paginator("get_partitions")
        for page in paginator.paginate(**part_kwargs):
            for part in page.get("Partitions", []):
                values_list = part.get("Values", [])
                values_dict = dict(zip(partition_keys, values_list))
                if partition_filter is None or _matches_partition_filter(
                    values_dict, partition_filter
                ):
                    psd = part.get("StorageDescriptor", {})
                    ploc = psd.get("Location", "")
                    if ploc:
                        partition_locations.append(ploc)

    return location, input_format, partition_keys, partition_locations


class GlueDuckDBMaterializedRef(MaterializedRef):
    """Deferred MaterializedRef that reads Glue table data via DuckDB + httpfs."""

    def __init__(
        self,
        catalog_options: dict[str, Any],
        table_name: str,
        partition_filter: dict[str, Any] | None,
        engine_config: dict[str, Any] | None,
        sql_override: str | None = None,
    ) -> None:
        self._catalog_options = catalog_options
        self._table_name = table_name
        self._partition_filter = partition_filter
        self._engine_config = engine_config or {}
        self._sql_override = sql_override

    def to_arrow(self) -> pyarrow.Table:
        import duckdb

        from rivet_duckdb.engine import apply_engine_settings
        from rivet_duckdb.extensions import ensure_extension

        location, input_format, partition_keys, partition_locations = _resolve_glue_table(
            self._catalog_options, self._table_name, self._partition_filter
        )

        reader = _glue_input_format_to_reader(input_format)
        locations = partition_locations if partition_locations else [location]

        # Ensure trailing /* for directory reads
        paths = []
        for loc in locations:
            loc = loc.rstrip("/")
            if not loc.endswith("*"):
                loc = loc + "/*.parquet" if reader == "read_parquet" else loc + "/*"
            paths.append(loc)

        conn = duckdb.connect(":memory:")
        try:
            apply_engine_settings(conn, self._engine_config)
            ensure_extension(conn, "httpfs")
            _configure_s3_secret(conn, self._catalog_options)

            if len(paths) == 1:
                sql = self._sql_override or f"SELECT * FROM {reader}('{paths[0]}')"
            else:
                path_list = ", ".join(f"'{p}'" for p in paths)
                sql = self._sql_override or f"SELECT * FROM {reader}([{path_list}])"

            return conn.execute(sql).arrow().read_all()
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"GlueDuckDBAdapter read failed for table '{self._table_name}': {exc}",
                    plugin_name="rivet_duckdb",
                    plugin_type="adapter",
                    adapter="GlueDuckDBAdapter",
                    remediation="Check S3 access permissions and that the table data exists.",
                    table=self._table_name,
                    location=location,
                )
            ) from exc
        finally:
            conn.close()

    @property
    def schema(self) -> Schema:
        table = self.to_arrow()
        return Schema(
            columns=[
                Column(name=f.name, type=str(f.type), nullable=f.nullable)
                for f in table.schema
            ]
        )

    @property
    def row_count(self) -> int:
        return self.to_arrow().num_rows  # type: ignore[no-any-return]

    @property
    def size_bytes(self) -> int | None:
        return None

    @property
    def storage_type(self) -> str:
        return "glue_duckdb"


class GlueDuckDBAdapter(ComputeEngineAdapter):
    """DuckDB adapter for Glue catalog type.

    Queries Glue API via boto3 for table metadata, applies partition pruning,
    configures DuckDB S3 credentials, and reads/writes via httpfs extension.
    """

    target_engine_type = "duckdb"
    catalog_type = "glue"
    capabilities = ALL_6_CAPABILITIES
    write_capabilities = ["write_append", "write_replace", "write_partition"]
    source = "engine_plugin"
    source_plugin = "rivet_duckdb"

    def read_dispatch(self, engine: Any, catalog: Any, joint: Any, pushdown: PushdownPlan | None = None) -> AdapterPushdownResult:
        catalog_options = catalog.options if hasattr(catalog, "options") else {}
        table_name = getattr(joint, "table", None) or getattr(joint, "name", "unknown")
        partition_filter: dict[str, Any] | None = None
        if hasattr(joint, "source_options") and joint.source_options:
            partition_filter = joint.source_options.get("partition_filter")

        engine_config: dict[str, Any] = {}
        if hasattr(engine, "options"):
            engine_config = engine.options

        # Build base SQL for pushdown modification
        location, input_format, _partition_keys, partition_locations = _resolve_glue_table(
            catalog_options, table_name, partition_filter  # type: ignore[arg-type]
        )
        reader = _glue_input_format_to_reader(input_format)
        locations = partition_locations if partition_locations else [location]
        paths = []
        for loc in locations:
            loc = loc.rstrip("/")
            if not loc.endswith("*"):
                loc = loc + "/*.parquet" if reader == "read_parquet" else loc + "/*"
            paths.append(loc)

        if len(paths) == 1:
            base_sql = f"SELECT * FROM {reader}('{paths[0]}')"
        else:
            path_list = ", ".join(f"'{p}'" for p in paths)
            base_sql = f"SELECT * FROM {reader}([{path_list}])"

        sql, residual = _apply_duckdb_pushdown(base_sql, pushdown)

        ref = GlueDuckDBMaterializedRef(
            catalog_options=catalog_options,
            table_name=table_name,  # type: ignore[arg-type]
            partition_filter=partition_filter,
            engine_config=engine_config,
            sql_override=sql,
        )
        material = Material(
            name=getattr(joint, "name", "glue_read"),
            catalog=getattr(catalog, "name", "glue"),
            materialized_ref=ref,
            state="deferred",
        )
        return AdapterPushdownResult(material=material, residual=residual)

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        import duckdb

        from rivet_duckdb.engine import apply_engine_settings
        from rivet_duckdb.extensions import ensure_extension

        catalog_options = catalog.options if hasattr(catalog, "options") else {}
        table_name = getattr(joint, "table", None) or getattr(joint, "name", "unknown")
        write_strategy = getattr(joint, "write_strategy", "replace") or "replace"

        # Resolve the target S3 location from Glue metadata
        location, input_format, _, _ = _resolve_glue_table(
            catalog_options, table_name, None  # type: ignore[arg-type]
        )
        location = location.rstrip("/")

        conn = duckdb.connect(":memory:")
        try:
            engine_config = engine.options if hasattr(engine, "options") else {}
            apply_engine_settings(conn, engine_config)
            ensure_extension(conn, "httpfs")
            _configure_s3_secret(conn, catalog_options)

            arrow_table = material.to_arrow() if hasattr(material, "to_arrow") else material
            conn.register("__write_data", arrow_table)

            output_path = f"{location}/data.parquet"
            if write_strategy == "append":
                sql = f"COPY __write_data TO '{output_path}' (FORMAT PARQUET, APPEND)"
            elif write_strategy == "partition":
                partition_by = getattr(joint, "partition_by", None)
                if partition_by:
                    cols = ", ".join(partition_by) if isinstance(partition_by, list) else partition_by
                    sql = f"COPY __write_data TO '{location}' (FORMAT PARQUET, PARTITION_BY ({cols}))"
                else:
                    sql = f"COPY __write_data TO '{output_path}' (FORMAT PARQUET)"
            else:  # replace
                sql = f"COPY __write_data TO '{output_path}' (FORMAT PARQUET)"

            conn.execute(sql)
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"GlueDuckDBAdapter write failed for table '{table_name}': {exc}",
                    plugin_name="rivet_duckdb",
                    plugin_type="adapter",
                    adapter="GlueDuckDBAdapter",
                    remediation="Check S3 write permissions and that the location is accessible.",
                    table=table_name,
                    location=location,
                )
            ) from exc
        finally:
            conn.close()
