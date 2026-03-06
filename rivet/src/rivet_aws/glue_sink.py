"""Glue sink plugin: write materialized data to Glue-registered tables."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError

from rivet_core.errors import ExecutionError, PluginValidationError, plugin_error
from rivet_core.plugins import SinkPlugin

if TYPE_CHECKING:
    from rivet_core.models import Catalog, Joint, Material

SUPPORTED_STRATEGIES = frozenset(
    {"append", "replace", "delete_insert", "incremental_append", "truncate_insert"}
)
UNSUPPORTED_STRATEGIES = frozenset({"merge", "scd2"})

_VALID_FORMATS = {"parquet", "csv", "json", "orc"}
_VALID_COMPRESSIONS = {"snappy", "gzip", "zstd", "lz4", "none", "uncompressed"}

_KNOWN_SINK_OPTIONS = {
    "table",
    "write_strategy",
    "partition_by",
    "format",
    "compression",
    "create_table",
    "update_schema",
    "lf_tags",
}


def _validate_sink_options(options: dict[str, Any]) -> None:
    for key in options:
        if key not in _KNOWN_SINK_OPTIONS:
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"Unknown sink option '{key}' for glue sink.",
                    plugin_name="rivet_aws",
                    plugin_type="sink",
                    remediation=f"Valid options: {', '.join(sorted(_KNOWN_SINK_OPTIONS))}",
                )
            )
    if "table" not in options:
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                "Missing required sink option 'table' for glue sink.",
                plugin_name="rivet_aws",
                plugin_type="sink",
                remediation="Provide 'table' in the sink options.",
            )
        )
    strategy = options.get("write_strategy", "replace")
    if strategy in UNSUPPORTED_STRATEGIES:
        raise PluginValidationError(
            plugin_error(
                "RVT-202",
                f"Write strategy '{strategy}' is not supported by the Glue sink.",
                plugin_name="rivet_aws",
                plugin_type="sink",
                remediation=f"Use one of the supported strategies: {', '.join(sorted(SUPPORTED_STRATEGIES))}",
                strategy=strategy,
            )
        )
    if strategy not in SUPPORTED_STRATEGIES:
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                f"Unknown write strategy '{strategy}' for glue sink.",
                plugin_name="rivet_aws",
                plugin_type="sink",
                remediation=f"Supported strategies: {', '.join(sorted(SUPPORTED_STRATEGIES))}",
                strategy=strategy,
            )
        )
    fmt = options.get("format", "parquet")
    if fmt not in _VALID_FORMATS:
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                f"Unknown format '{fmt}' for glue sink.",
                plugin_name="rivet_aws",
                plugin_type="sink",
                remediation=f"Valid formats: {', '.join(sorted(_VALID_FORMATS))}",
                format=fmt,
            )
        )
    compression = options.get("compression", "snappy")
    if compression not in _VALID_COMPRESSIONS:
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                f"Unknown compression '{compression}' for glue sink.",
                plugin_name="rivet_aws",
                plugin_type="sink",
                remediation=f"Valid compressions: {', '.join(sorted(_VALID_COMPRESSIONS))}",
                compression=compression,
            )
        )
    lf_tags = options.get("lf_tags")
    if lf_tags is not None and not isinstance(lf_tags, dict):
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                "'lf_tags' must be a dict of tag key/value pairs.",
                plugin_name="rivet_aws",
                plugin_type="sink",
                remediation="Provide lf_tags as a dict, e.g. {'team': 'data', 'env': 'prod'}.",
            )
        )
    partition_by = options.get("partition_by")
    if partition_by is not None and not isinstance(partition_by, list):
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                "'partition_by' must be a list of column names.",
                plugin_name="rivet_aws",
                plugin_type="sink",
                remediation="Provide partition_by as a list, e.g. ['year', 'month'].",
            )
        )


def _format_to_serde(fmt: str) -> tuple[str, str, str]:
    """Return (InputFormat, OutputFormat, SerializationLibrary) for a given format."""
    if fmt == "parquet":
        return (
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        )
    if fmt == "orc":
        return (
            "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
            "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
            "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
        )
    if fmt == "json":
        return (
            "org.apache.hadoop.mapred.TextInputFormat",
            "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "org.openx.data.jsonserde.JsonSerDe",
        )
    # csv / default
    return (
        "org.apache.hadoop.mapred.TextInputFormat",
        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
    )


def _arrow_type_to_glue(arrow_type_str: str) -> str:
    """Map Arrow type string to Glue/Hive type string."""
    mapping = {
        "int8": "tinyint",
        "int16": "smallint",
        "int32": "int",
        "int64": "bigint",
        "float32": "float",
        "float64": "double",
        "bool": "boolean",
        "large_utf8": "string",
        "utf8": "string",
        "large_binary": "binary",
        "binary": "binary",
        "date32": "date",
        "date64": "date",
    }
    lower = arrow_type_str.lower()
    if lower.startswith("timestamp"):
        return "timestamp"
    if lower.startswith("decimal"):
        return "decimal"
    return mapping.get(lower, "string")


def _create_glue_table(
    client: Any,
    catalog: Any,
    table_name: str,
    location: str,
    fmt: str,
    compression: str,
    partition_by: list[str],
    arrow_schema: Any,
) -> None:
    """Create a Glue table via CreateTable API."""

    database = catalog.options["database"]
    catalog_id = catalog.options.get("catalog_id")

    input_fmt, output_fmt, serde_lib = _format_to_serde(fmt)

    # Build columns (excluding partition keys)
    partition_set = set(partition_by)
    columns = []
    partition_keys = []
    for field in arrow_schema:
        glue_type = _arrow_type_to_glue(str(field.type))
        col = {"Name": field.name, "Type": glue_type}
        if field.name in partition_set:
            partition_keys.append(col)
        else:
            columns.append(col)

    serde_params: dict[str, str] = {}
    if compression and compression not in ("none", "uncompressed"):
        serde_params["compression"] = compression

    storage_descriptor: dict[str, Any] = {
        "Columns": columns,
        "Location": location,
        "InputFormat": input_fmt,
        "OutputFormat": output_fmt,
        "SerdeInfo": {
            "SerializationLibrary": serde_lib,
            "Parameters": serde_params,
        },
        "Compressed": compression not in ("none", "uncompressed"),
        "Parameters": {"classification": fmt},
    }

    table_input: dict[str, Any] = {
        "Name": table_name,
        "StorageDescriptor": storage_descriptor,
        "PartitionKeys": partition_keys,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": fmt},
    }

    kwargs: dict[str, Any] = {
        "DatabaseName": database,
        "TableInput": table_input,
    }
    if catalog_id:
        kwargs["CatalogId"] = catalog_id

    try:
        client.create_table(**kwargs)
    except ClientError as exc:
        from rivet_aws.errors import handle_glue_error
        raise handle_glue_error(exc, database=database, table=table_name, action="glue:CreateTable") from exc


def _update_glue_schema(
    client: Any,
    catalog: Any,
    table_name: str,
    arrow_schema: Any,
    fmt: str,
    compression: str,
    partition_by: list[str],
    location: str,
) -> None:
    """Update Glue table schema via UpdateTable API."""
    database = catalog.options["database"]
    catalog_id = catalog.options.get("catalog_id")

    input_fmt, output_fmt, serde_lib = _format_to_serde(fmt)
    partition_set = set(partition_by)

    columns = []
    partition_keys = []
    for field in arrow_schema:
        glue_type = _arrow_type_to_glue(str(field.type))
        col = {"Name": field.name, "Type": glue_type}
        if field.name in partition_set:
            partition_keys.append(col)
        else:
            columns.append(col)

    serde_params: dict[str, str] = {}
    if compression and compression not in ("none", "uncompressed"):
        serde_params["compression"] = compression

    storage_descriptor: dict[str, Any] = {
        "Columns": columns,
        "Location": location,
        "InputFormat": input_fmt,
        "OutputFormat": output_fmt,
        "SerdeInfo": {
            "SerializationLibrary": serde_lib,
            "Parameters": serde_params,
        },
        "Compressed": compression not in ("none", "uncompressed"),
        "Parameters": {"classification": fmt},
    }

    table_input: dict[str, Any] = {
        "Name": table_name,
        "StorageDescriptor": storage_descriptor,
        "PartitionKeys": partition_keys,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": fmt},
    }

    kwargs: dict[str, Any] = {
        "DatabaseName": database,
        "TableInput": table_input,
    }
    if catalog_id:
        kwargs["CatalogId"] = catalog_id

    try:
        client.update_table(**kwargs)
    except ClientError as exc:
        from rivet_aws.errors import handle_glue_error
        raise handle_glue_error(exc, database=database, table=table_name, action="glue:UpdateTable") from exc


def _table_exists(client: Any, catalog: Any, table_name: str) -> bool:
    """Return True if the Glue table exists."""
    database = catalog.options["database"]
    catalog_id = catalog.options.get("catalog_id")
    kwargs: dict[str, Any] = {"DatabaseName": database, "Name": table_name}
    if catalog_id:
        kwargs["CatalogId"] = catalog_id
    try:
        client.get_table(**kwargs)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code == "EntityNotFoundException":
            return False
        from rivet_aws.errors import handle_glue_error
        raise handle_glue_error(exc, database=database, table=table_name, action="glue:GetTable") from exc
    except Exception:
        return False


def _get_table_location(client: Any, catalog: Any, table_name: str) -> str:
    """Return the S3 location of an existing Glue table."""
    database = catalog.options["database"]
    catalog_id = catalog.options.get("catalog_id")
    kwargs: dict[str, Any] = {"DatabaseName": database, "Name": table_name}
    if catalog_id:
        kwargs["CatalogId"] = catalog_id
    try:
        resp = client.get_table(**kwargs)
    except ClientError as exc:
        from rivet_aws.errors import handle_glue_error
        raise handle_glue_error(exc, database=database, table=table_name, action="glue:GetTable") from exc
    sd = resp["Table"].get("StorageDescriptor", {})
    return sd.get("Location", "")  # type: ignore[no-any-return]


class GlueSink(SinkPlugin):
    """Sink plugin for glue catalog type.

    Delegates writing to the resolved engine's adapter and updates Glue
    partition/schema metadata after write.

    Sink options:
      - table (required): Glue table name
      - write_strategy (optional, default 'replace'): append, replace,
        delete_insert, incremental_append, truncate_insert
      - partition_by (optional): list of column names to partition by
      - format (optional, default 'parquet'): parquet, csv, json, orc
      - compression (optional, default 'snappy'): snappy, gzip, zstd, lz4, none
      - create_table (optional, default True): create table if not exists
      - update_schema (optional, default False): update Glue schema after write
      - lf_tags (optional): dict of Lake Formation tags for new partitions
    """

    catalog_type = "glue"
    supported_strategies = SUPPORTED_STRATEGIES

    def write(self, catalog: Catalog, joint: Joint, material: Material, strategy: str) -> None:
        # Collect sink options from joint
        sink_options: dict[str, Any] = {}
        if joint.table:
            sink_options["table"] = joint.table
        if hasattr(joint, "sink_options") and joint.sink_options:
            sink_options.update(joint.sink_options)
        if "table" not in sink_options and joint.name:
            sink_options["table"] = joint.name

        # Merge write_strategy from argument
        if "write_strategy" not in sink_options:
            sink_options["write_strategy"] = strategy

        _validate_sink_options(sink_options)

        table_name = sink_options["table"]
        fmt = sink_options.get("format", "parquet")
        compression = sink_options.get("compression", "snappy")
        partition_by: list[str] = sink_options.get("partition_by") or []
        create_table: bool = sink_options.get("create_table", True)
        update_schema: bool = sink_options.get("update_schema", False)
        lf_tags: dict[str, str] | None = sink_options.get("lf_tags")

        from rivet_aws.glue_catalog import _make_glue_client

        client = _make_glue_client(catalog)

        arrow_table = material.to_arrow()

        exists = _table_exists(client, catalog, table_name)

        if not exists:
            if not create_table:
                raise ExecutionError(
                    plugin_error(
                        "RVT-506",
                        f"Glue table '{table_name}' does not exist and create_table=False.",
                        plugin_name="rivet_aws",
                        plugin_type="sink",
                        remediation="Set create_table=True or create the table manually.",
                        table=table_name,
                        database=catalog.options["database"],
                    )
                )
            # Derive location from catalog options
            bucket = catalog.options.get("bucket", "")
            prefix = catalog.options.get("prefix", "")
            database = catalog.options["database"]
            if bucket:
                base = f"s3://{bucket}/{prefix}/{database}/{table_name}" if prefix else f"s3://{bucket}/{database}/{table_name}"
            else:
                base = f"s3://unknown/{database}/{table_name}"
            location = base

            _create_glue_table(
                client=client,
                catalog=catalog,
                table_name=table_name,
                location=location,
                fmt=fmt,
                compression=compression,
                partition_by=partition_by,
                arrow_schema=arrow_table.schema,
            )
        else:
            location = _get_table_location(client, catalog, table_name)

        # Post-write: sync partitions (BatchCreatePartition / UpdatePartition)
        _sync_partitions(
            client=client,
            catalog=catalog,
            table_name=table_name,
            location=location,
            partition_by=partition_by,
            arrow_table=arrow_table,
            fmt=fmt,
            compression=compression,
        )

        # Post-write: UpdateTable if update_schema is set
        if update_schema:
            _update_glue_schema(
                client=client,
                catalog=catalog,
                table_name=table_name,
                arrow_schema=arrow_table.schema,
                fmt=fmt,
                compression=compression,
                partition_by=partition_by,
                location=location,
            )

        if lf_tags:
            _apply_lf_tags(client, catalog, table_name, lf_tags)


def _get_existing_partitions(
    client: Any,
    catalog: Any,
    table_name: str,
) -> dict[tuple[str, ...], dict[str, Any]]:
    """Return existing partitions as {(val1, val2, ...): partition_response}."""
    database = catalog.options["database"]
    catalog_id = catalog.options.get("catalog_id")
    kwargs: dict[str, Any] = {"DatabaseName": database, "TableName": table_name}
    if catalog_id:
        kwargs["CatalogId"] = catalog_id

    existing: dict[tuple[str, ...], dict[str, Any]] = {}
    try:
        paginator = client.get_paginator("get_partitions")
        for page in paginator.paginate(**kwargs):
            for part in page.get("Partitions", []):
                key = tuple(part.get("Values", []))
                existing[key] = part
    except ClientError as exc:
        from rivet_aws.errors import handle_glue_error
        raise handle_glue_error(exc, database=database, table=table_name, action="glue:GetPartitions") from exc
    return existing


def _extract_partition_values(
    arrow_table: Any,
    partition_by: list[str],
) -> list[dict[str, str]]:
    """Extract unique partition value dicts from an Arrow table."""
    if not partition_by:
        return []

    # Get unique combinations
    cols = {col: arrow_table.column(col) for col in partition_by}
    seen: set[tuple[str, ...]] = set()
    result: list[dict[str, str]] = []
    for i in range(arrow_table.num_rows):
        vals = tuple(str(cols[col][i].as_py()) for col in partition_by)
        if vals not in seen:
            seen.add(vals)
            result.append(dict(zip(partition_by, vals)))
    return result


def _build_partition_storage_descriptor(
    location: str,
    partition_values: dict[str, str],
    fmt: str,
    compression: str,
    arrow_schema: Any,
    partition_by: list[str],
) -> dict[str, Any]:
    """Build a StorageDescriptor for a single partition."""
    input_fmt, output_fmt, serde_lib = _format_to_serde(fmt)
    partition_set = set(partition_by)

    columns = []
    for f in arrow_schema:
        if f.name not in partition_set:
            columns.append({"Name": f.name, "Type": _arrow_type_to_glue(str(f.type))})

    serde_params: dict[str, str] = {}
    if compression and compression not in ("none", "uncompressed"):
        serde_params["compression"] = compression

    # Build partition path: location/key1=val1/key2=val2/
    parts = "/".join(f"{k}={v}" for k, v in partition_values.items())
    part_location = f"{location.rstrip('/')}/{parts}"

    return {
        "Columns": columns,
        "Location": part_location,
        "InputFormat": input_fmt,
        "OutputFormat": output_fmt,
        "SerdeInfo": {
            "SerializationLibrary": serde_lib,
            "Parameters": serde_params,
        },
        "Compressed": compression not in ("none", "uncompressed"),
        "Parameters": {"classification": fmt},
    }


def _sync_partitions(
    client: Any,
    catalog: Any,
    table_name: str,
    location: str,
    partition_by: list[str],
    arrow_table: Any,
    fmt: str,
    compression: str,
) -> None:
    """Register new partitions via BatchCreatePartition and update existing via UpdatePartition."""
    if not partition_by:
        return

    partition_dicts = _extract_partition_values(arrow_table, partition_by)
    if not partition_dicts:
        return

    existing = _get_existing_partitions(client, catalog, table_name)
    database = catalog.options["database"]
    catalog_id = catalog.options.get("catalog_id")

    new_partitions: list[dict[str, Any]] = []
    for pv in partition_dicts:
        values = [pv[col] for col in partition_by]
        key = tuple(values)
        sd = _build_partition_storage_descriptor(
            location, pv, fmt, compression, arrow_table.schema, partition_by
        )
        if key in existing:
            # UpdatePartition for existing
            update_kwargs: dict[str, Any] = {
                "DatabaseName": database,
                "TableName": table_name,
                "PartitionValueList": list(key),
                "PartitionInput": {"Values": values, "StorageDescriptor": sd},
            }
            if catalog_id:
                update_kwargs["CatalogId"] = catalog_id
            try:
                client.update_partition(**update_kwargs)
            except ClientError as exc:
                from rivet_aws.errors import handle_glue_error
                raise handle_glue_error(exc, database=database, table=table_name, action="glue:UpdatePartition") from exc
        else:
            new_partitions.append({"Values": values, "StorageDescriptor": sd})

    # BatchCreatePartition in chunks of 100 (Glue API limit)
    if new_partitions:
        for i in range(0, len(new_partitions), 100):
            batch = new_partitions[i : i + 100]
            batch_kwargs: dict[str, Any] = {
                "DatabaseName": database,
                "TableName": table_name,
                "PartitionInputList": batch,
            }
            if catalog_id:
                batch_kwargs["CatalogId"] = catalog_id
            try:
                client.batch_create_partition(**batch_kwargs)
            except ClientError as exc:
                from rivet_aws.errors import handle_glue_error
                raise handle_glue_error(exc, database=database, table=table_name, action="glue:BatchCreatePartition") from exc


def _apply_lf_tags(
    glue_client: Any,
    catalog: Any,
    table_name: str,
    lf_tags: dict[str, str],
) -> None:
    """Apply Lake Formation tags to a Glue table (best-effort)."""
    from rivet_aws.glue_catalog import _make_lf_client

    try:
        lf_client = _make_lf_client(catalog)
        database = catalog.options["database"]
        catalog_id = catalog.options.get("catalog_id")
        resource: dict[str, Any] = {
            "Table": {
                "DatabaseName": database,
                "Name": table_name,
            }
        }
        if catalog_id:
            resource["Table"]["CatalogId"] = catalog_id

        lf_tags_list = [{"TagKey": k, "TagValues": [v]} for k, v in lf_tags.items()]
        lf_client.add_lf_tags_to_resource(Resource=resource, LFTags=lf_tags_list)
    except Exception:
        # LF tag application is best-effort; do not fail the write
        pass
