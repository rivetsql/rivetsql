"""S3 sink plugin for Rivet: writes materialized data to S3 locations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rivet_core.errors import ExecutionError, PluginValidationError, plugin_error
from rivet_core.formats import FormatRegistry
from rivet_core.plugins import SinkPlugin

if TYPE_CHECKING:
    from rivet_core.models import Catalog, Joint, Material

_BASE_STRATEGIES = frozenset(
    {"append", "replace", "delete_insert", "incremental_append", "truncate_insert", "partition"}
)
_DELTA_ONLY_STRATEGIES = frozenset({"merge", "scd2"})
_ALL_STRATEGIES = _BASE_STRATEGIES | _DELTA_ONLY_STRATEGIES


def _resolve_sink_path(catalog_options: dict[str, Any], sink_options: dict[str, Any]) -> str:
    """Resolve the S3 write path from sink and catalog options."""
    path = sink_options.get("path")
    if path:
        if path.startswith("s3://"):
            return path  # type: ignore[no-any-return]
        bucket = catalog_options["bucket"]
        prefix = catalog_options.get("prefix", "")
        base = f"{bucket}/{prefix}" if prefix else bucket
        return f"s3://{base}/{path}"
    bucket = catalog_options["bucket"]
    prefix = catalog_options.get("prefix", "")
    base = f"{bucket}/{prefix}" if prefix else bucket
    return f"s3://{base}"


def _parse_sink_options(catalog_options: dict[str, Any], joint: Any) -> dict[str, Any]:
    """Extract and validate sink options from joint and catalog options."""
    sink_options: dict[str, Any] = catalog_options.get("sink_options", {})

    if not sink_options.get("path") and getattr(joint, "table", None):
        sink_options = {**sink_options, "path": joint.table}

    fmt = FormatRegistry.resolve_format(
        sink_options.get("format"),
        catalog_options.get("format"),
    )
    FormatRegistry.validate_plugin_support(fmt, "s3", "sink")

    write_strategy = (
        sink_options.get("write_strategy") or getattr(joint, "write_strategy", None) or "replace"
    )

    if write_strategy in _DELTA_ONLY_STRATEGIES and fmt != "delta":
        raise PluginValidationError(
            plugin_error(
                "RVT-202",
                f"Write strategy '{write_strategy}' requires format 'delta' for S3 sink.",
                plugin_name="rivet_aws",
                plugin_type="sink",
                remediation="Set format: delta to use merge or scd2 write strategies.",
                write_strategy=write_strategy,
                format=fmt,
            )
        )

    if write_strategy not in _ALL_STRATEGIES:
        raise PluginValidationError(
            plugin_error(
                "RVT-202",
                f"Unsupported write strategy '{write_strategy}' for S3 sink.",
                plugin_name="rivet_aws",
                plugin_type="sink",
                remediation=f"Supported strategies: {', '.join(sorted(_ALL_STRATEGIES))}",
                write_strategy=write_strategy,
            )
        )

    partition_by: list[str] | None = sink_options.get("partition_by")
    if isinstance(partition_by, str):
        partition_by = [partition_by]

    return {
        "path": _resolve_sink_path(catalog_options, sink_options),
        "format": fmt,
        "write_strategy": write_strategy,
        "partition_by": partition_by,
        "compression": sink_options.get("compression", "snappy"),
        "overwrite_files": sink_options.get("overwrite_files", True),
    }


class S3Sink(SinkPlugin):
    """Sink plugin for s3 catalog type.

    Writes materialized data to S3 via the resolved engine's S3 adapter.

    Sink options:
      - path (required): S3 path (relative to bucket/prefix or full s3:// URI)
      - format (optional): default catalog format (parquet/csv/json/orc/delta)
      - write_strategy (optional): default "replace"
      - partition_by (optional): list of partition column names
      - compression (optional): default "snappy"
      - overwrite_files (optional): default True

    Supported strategies: append, replace, delete_insert, incremental_append,
        truncate_insert, partition
    Delta format additionally supports: merge, scd2
    """

    catalog_type = "s3"
    supported_strategies = _ALL_STRATEGIES

    def write(self, catalog: Catalog, joint: Joint, material: Material, strategy: str) -> None:
        catalog_options = catalog.options
        opts = _parse_sink_options(catalog_options, joint)

        effective_strategy = strategy or opts["write_strategy"]

        if effective_strategy in _DELTA_ONLY_STRATEGIES and opts["format"] != "delta":
            raise PluginValidationError(
                plugin_error(
                    "RVT-202",
                    f"Write strategy '{effective_strategy}' requires format 'delta' for S3 sink.",
                    plugin_name="rivet_aws",
                    plugin_type="sink",
                    remediation="Set format: delta to use merge or scd2 write strategies.",
                    write_strategy=effective_strategy,
                    format=opts["format"],
                )
            )

        if effective_strategy not in _ALL_STRATEGIES:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Unsupported write strategy '{effective_strategy}' for S3 sink.",
                    plugin_name="rivet_aws",
                    plugin_type="sink",
                    remediation=f"Supported strategies: {', '.join(sorted(_ALL_STRATEGIES))}",
                    strategy=effective_strategy,
                    catalog=catalog.name,
                )
            )

        arrow_table = material.to_arrow()
        _write_to_s3(
            table=arrow_table,
            path=opts["path"],
            fmt=opts["format"],
            strategy=effective_strategy,
            partition_by=opts["partition_by"],
            compression=opts["compression"],
            overwrite_files=opts["overwrite_files"],
            catalog_options=catalog_options,
        )


def _build_s3fs(catalog_options: dict[str, Any]) -> Any:
    """Build a PyArrow S3FileSystem from catalog options."""
    import pyarrow.fs as pafs

    kwargs: dict[str, Any] = {}
    region = catalog_options.get("region", "us-east-1")
    if region:
        kwargs["region"] = region
    access_key = catalog_options.get("access_key_id")
    secret_key = catalog_options.get("secret_access_key")
    if access_key and secret_key:
        kwargs["access_key"] = access_key
        kwargs["secret_key"] = secret_key
        session_token = catalog_options.get("session_token")
        if session_token:
            kwargs["session_token"] = session_token
    endpoint_url = catalog_options.get("endpoint_url")
    if endpoint_url:
        endpoint = endpoint_url
        for scheme in ("https://", "http://"):
            if endpoint.startswith(scheme):
                endpoint = endpoint[len(scheme) :]
                break
        kwargs["endpoint_override"] = endpoint
        if endpoint_url.startswith("http://"):
            kwargs["scheme"] = "http"
    role_arn = catalog_options.get("role_arn")
    if role_arn and not (access_key and secret_key):
        kwargs["role_arn"] = role_arn
        session_name = catalog_options.get("role_session_name", "rivet-session")
        if session_name:
            kwargs["session_name"] = session_name
    return pafs.S3FileSystem(**kwargs)


def _write_to_s3(
    table: Any,
    path: str,
    fmt: str,
    strategy: str,
    partition_by: list[str] | None,
    compression: str,
    overwrite_files: bool,
    catalog_options: dict[str, Any],
) -> None:
    """Write an Arrow table to S3 using PyArrow dataset writer."""
    import pyarrow.dataset as pad

    if fmt == "delta":
        _write_delta(table, path, strategy, partition_by, catalog_options)
        return

    fs = _build_s3fs(catalog_options)

    write_path = path
    if write_path.startswith("s3://"):
        write_path = write_path[len("s3://") :]

    if fmt == "parquet":
        file_format: pad.FileFormat = pad.ParquetFileFormat()
        write_options = file_format.make_write_options(compression=compression)
    elif fmt == "csv":
        file_format = pad.CsvFileFormat()
        write_options = None
    elif fmt == "json":
        raise ExecutionError(
            plugin_error(
                "RVT-501",
                "Direct S3 write for format 'json' is not supported by S3Sink via PyArrow dataset.",
                plugin_name="rivet_aws",
                plugin_type="sink",
                remediation="Use a compute engine adapter (e.g. DuckDB) for JSON writes.",
                format=fmt,
            )
        )
    elif fmt == "orc":
        file_format = pad.OrcFileFormat()
        write_options = None
    else:
        raise ExecutionError(
            plugin_error(
                "RVT-501",
                f"Unsupported format '{fmt}' for S3 sink.",
                plugin_name="rivet_aws",
                plugin_type="sink",
                remediation="Supported formats: csv, delta, json, orc, parquet",
                format=fmt,
            )
        )

    # Map strategy to existing_data_behavior
    if strategy in ("replace", "truncate_insert", "delete_insert", "partition"):
        existing_data_behavior = "delete_matching"
    else:  # append, incremental_append
        existing_data_behavior = "overwrite_or_ignore"

    partitioning = None
    if partition_by:
        partitioning = pad.partitioning(
            schema=table.schema.empty_table().select(partition_by).schema,
            flavor="hive",
        )

    write_kwargs: dict[str, Any] = {
        "data": table,
        "base_dir": write_path,
        "filesystem": fs,
        "format": file_format,
        "existing_data_behavior": existing_data_behavior,
    }
    if write_options is not None:
        write_kwargs["file_options"] = write_options
    if partitioning is not None:
        write_kwargs["partitioning"] = partitioning

    pad.write_dataset(**write_kwargs)


def _write_delta(
    table: Any,
    path: str,
    strategy: str,
    partition_by: list[str] | None,
    catalog_options: dict[str, Any],
) -> None:
    """Write using Delta Lake format via deltalake package."""
    try:
        import deltalake
    except ImportError:
        raise ExecutionError(  # noqa: B904
            plugin_error(
                "RVT-501",
                "Delta format requires the 'deltalake' package.",
                plugin_name="rivet_aws",
                plugin_type="sink",
                remediation="Install it with: pip install deltalake",
                format="delta",
            )
        )

    storage_options = _build_delta_storage_options(catalog_options)

    mode_map = {
        "replace": "overwrite",
        "append": "append",
        "merge": "merge",
        "scd2": "merge",
        "truncate_insert": "overwrite",
        "delete_insert": "overwrite",
        "incremental_append": "append",
        "partition": "overwrite",
    }
    mode = mode_map.get(strategy, "overwrite")

    write_kwargs: dict[str, Any] = {
        "table_or_uri": path,
        "data": table,
        "mode": mode,
        "storage_options": storage_options,
    }
    if partition_by:
        write_kwargs["partition_by"] = partition_by

    deltalake.write_deltalake(**write_kwargs)


def _build_delta_storage_options(catalog_options: dict[str, Any]) -> dict[str, str]:
    """Build storage_options dict for deltalake from catalog options."""
    opts: dict[str, str] = {}
    region = catalog_options.get("region", "us-east-1")
    if region:
        opts["AWS_REGION"] = region
    access_key = catalog_options.get("access_key_id")
    secret_key = catalog_options.get("secret_access_key")
    if access_key:
        opts["AWS_ACCESS_KEY_ID"] = access_key
    if secret_key:
        opts["AWS_SECRET_ACCESS_KEY"] = secret_key
    session_token = catalog_options.get("session_token")
    if session_token:
        opts["AWS_SESSION_TOKEN"] = session_token
    endpoint_url = catalog_options.get("endpoint_url")
    if endpoint_url:
        opts["AWS_ENDPOINT_URL"] = endpoint_url
    return opts
