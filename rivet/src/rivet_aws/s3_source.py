"""S3 source plugin for Rivet: resolves S3 paths and returns deferred Materials."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow

from rivet_core.errors import ExecutionError, plugin_error
from rivet_core.formats import FormatRegistry
from rivet_core.models import Material
from rivet_core.plugins import SourcePlugin
from rivet_core.strategies import MaterializedRef

if TYPE_CHECKING:
    from rivet_core.models import Catalog, Joint


def _resolve_s3_path(catalog_options: dict[str, Any], source_options: dict[str, Any]) -> str:
    """Resolve the S3 path from catalog and source options.

    If source `path` is provided, it is used as-is (may be a glob pattern).
    Otherwise, the catalog bucket/prefix/format are used to build the path.
    """
    explicit_path = source_options.get("path")
    if explicit_path:
        # If already a full s3:// URI, return as-is
        if explicit_path.startswith("s3://"):
            return explicit_path  # type: ignore[no-any-return]
        # Otherwise treat as relative to bucket/prefix
        bucket = catalog_options["bucket"]
        prefix = catalog_options.get("prefix", "")
        base = f"{bucket}/{prefix}" if prefix else bucket
        return f"s3://{base}/{explicit_path}"

    # Build from catalog options
    bucket = catalog_options["bucket"]
    prefix = catalog_options.get("prefix", "")
    fmt = FormatRegistry.resolve_format(
        source_options.get("format"),
        catalog_options.get("format"),
    )
    base = f"{bucket}/{prefix}" if prefix else bucket
    return f"s3://{base}/*.{fmt}"


class S3DeferredMaterializedRef(MaterializedRef):
    """MaterializedRef backed by an S3 path. Reads only on to_arrow()."""

    def __init__(
        self,
        s3_path: str,
        fmt: str,
        catalog_options: dict[str, Any],
        partition_columns: list[str] | None,
        schema: pyarrow.Schema | None,
    ) -> None:
        self._s3_path = s3_path
        self._fmt = fmt
        self._catalog_options = catalog_options
        self._partition_columns = partition_columns
        self._schema = schema

    def _build_s3fs(self) -> Any:
        import pyarrow.fs as pafs

        opts = self._catalog_options
        kwargs: dict[str, Any] = {}
        region = opts.get("region", "us-east-1")
        if region:
            kwargs["region"] = region
        access_key = opts.get("access_key_id")
        secret_key = opts.get("secret_access_key")
        if access_key and secret_key:
            kwargs["access_key"] = access_key
            kwargs["secret_key"] = secret_key
            session_token = opts.get("session_token")
            if session_token:
                kwargs["session_token"] = session_token
        endpoint_url = opts.get("endpoint_url")
        if endpoint_url:
            endpoint = endpoint_url
            for scheme in ("https://", "http://"):
                if endpoint.startswith(scheme):
                    endpoint = endpoint[len(scheme) :]
                    break
            kwargs["endpoint_override"] = endpoint
            if endpoint_url.startswith("http://"):
                kwargs["scheme"] = "http"
        role_arn = opts.get("role_arn")
        if role_arn and not (access_key and secret_key):
            kwargs["role_arn"] = role_arn
            session_name = opts.get("role_session_name", "rivet-session")
            if session_name:
                kwargs["session_name"] = session_name
        return pafs.S3FileSystem(**kwargs)

    def to_arrow(self) -> pyarrow.Table:
        import pyarrow.dataset as pad

        fs = self._build_s3fs()
        # Strip s3:// prefix for pyarrow dataset
        path = self._s3_path
        if path.startswith("s3://"):
            path = path[len("s3://") :]

        fmt = self._fmt
        if fmt == "parquet":
            file_format: pad.FileFormat = pad.ParquetFileFormat()
        elif fmt == "csv":
            file_format = pad.CsvFileFormat()
        elif fmt == "json":
            file_format = pad.JsonFileFormat()
        elif fmt == "orc":
            file_format = pad.OrcFileFormat()
        else:
            # delta — not supported via simple pyarrow dataset; raise informative error
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Direct S3 read for format '{fmt}' is not supported by S3Source",
                    plugin_name="rivet_aws",
                    plugin_type="source",
                    remediation="Use a supported format (parquet, csv, json). Delta format is not yet supported.",
                )
            )

        partitioning = None
        if self._partition_columns:
            partitioning = pad.partitioning(
                pyarrow.schema(
                    [pyarrow.field(c, pyarrow.string()) for c in self._partition_columns]
                ),
                flavor="hive",
            )

        dataset = pad.dataset(
            path,
            filesystem=fs,
            format=file_format,
            schema=self._schema,
            partitioning=partitioning,
        )
        return dataset.to_table()

    @property
    def schema(self) -> Any:
        from rivet_core.models import Column, Schema

        if self._schema is not None:
            return Schema(
                columns=[
                    Column(name=field.name, type=str(field.type), nullable=field.nullable)
                    for field in self._schema
                ]
            )
        table = self.to_arrow()
        return Schema(
            columns=[
                Column(name=field.name, type=str(field.type), nullable=field.nullable)
                for field in table.schema
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
        return "s3"


class S3Source(SourcePlugin):
    """Source plugin for s3 catalog type.

    Resolves S3 file paths from catalog and source-level options, returning a
    deferred Material. Data is not fetched until .to_arrow() is called.

    Source options (passed via joint.table or catalog options):
      - path: optional glob pattern (relative to bucket/prefix or full s3:// URI)
      - partition_columns: optional list of partition column names
      - format: optional format override (parquet/csv/json/orc/delta)
      - schema: optional explicit PyArrow schema
    """

    catalog_type = "s3"

    def read(self, catalog: Catalog, joint: Joint, pushdown: Any | None) -> Material:
        catalog_options = catalog.options
        # Source options are stored in catalog options under "source_options" key,
        # or can be inferred from joint attributes.
        source_options: dict[str, Any] = catalog_options.get("source_options", {})

        # Allow joint.table to act as path override if no explicit source_options.path
        if not source_options.get("path") and joint.table:
            source_options = {**source_options, "path": joint.table}

        fmt = FormatRegistry.resolve_format(
            source_options.get("format"),
            catalog_options.get("format"),
        )
        partition_columns: list[str] | None = source_options.get("partition_columns")
        schema: pyarrow.Schema | None = source_options.get("schema")

        s3_path = _resolve_s3_path(catalog_options, source_options)

        ref = S3DeferredMaterializedRef(
            s3_path=s3_path,
            fmt=fmt,
            catalog_options=catalog_options,
            partition_columns=partition_columns,
            schema=schema,
        )
        return Material(
            name=joint.name,
            catalog=catalog.name,
            materialized_ref=ref,
            state="deferred",
        )
