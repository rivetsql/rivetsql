"""S3DuckDBAdapter: read/write S3 data via DuckDB httpfs extension and secret manager."""

from __future__ import annotations

from typing import Any

import duckdb
import pyarrow

from rivet_core.errors import ExecutionError, plugin_error
from rivet_core.models import Column, Material, Schema
from rivet_core.optimizer import AdapterPushdownResult, PushdownPlan
from rivet_core.plugins import ComputeEngineAdapter
from rivet_core.strategies import MaterializedRef
from rivet_duckdb.adapters.pushdown import _apply_duckdb_pushdown
from rivet_duckdb.engine import ALL_6_CAPABILITIES
from rivet_duckdb.extensions import ensure_extension

_FORMAT_TO_READER: dict[str, str] = {
    "parquet": "read_parquet",
    "csv": "read_csv_auto",
    "json": "read_json_auto",
}


def _configure_s3_secret(conn: Any, catalog_options: dict[str, Any]) -> None:
    """Configure DuckDB's secret manager with S3 credentials from catalog options."""
    parts = ["TYPE S3"]

    key = catalog_options.get("access_key_id")
    secret = catalog_options.get("secret_access_key")
    if key and secret:
        parts.append(f"KEY_ID '{key}'")
        parts.append(f"SECRET '{secret}'")
        token = catalog_options.get("session_token")
        if token:
            parts.append(f"SESSION_TOKEN '{token}'")

    region = catalog_options.get("region", "us-east-1")
    if region:
        parts.append(f"REGION '{region}'")

    endpoint = catalog_options.get("endpoint_url")
    if endpoint:
        parts.append(f"ENDPOINT '{endpoint}'")

    if catalog_options.get("path_style_access"):
        parts.append("URL_STYLE 'path'")

    secret_body = ", ".join(parts)
    conn.execute(f"CREATE OR REPLACE SECRET s3_secret ({secret_body})")


def _build_s3_path(catalog_options: dict[str, Any], table: str | None) -> str:
    """Build the S3 URI for reading."""
    bucket = catalog_options["bucket"]
    prefix = catalog_options.get("prefix", "")
    fmt = catalog_options.get("format", "parquet")
    name = table or "*"
    path = f"{prefix}/{name}" if prefix else name
    if fmt == "delta":
        return f"s3://{bucket}/{path}"
    return f"s3://{bucket}/{path}.{fmt}"


class _S3DuckDBMaterializedRef(MaterializedRef):
    """Deferred MaterializedRef that reads S3 data via DuckDB on to_arrow()."""

    def __init__(self, catalog_options: dict[str, Any], sql: str | None, table: str | None) -> None:
        self._catalog_options = catalog_options
        self._sql = sql
        self._table = table

    def _execute(self) -> pyarrow.Table:
        conn = duckdb.connect(":memory:")
        try:
            ensure_extension(conn, "httpfs")
            _configure_s3_secret(conn, self._catalog_options)

            if self._sql:
                return conn.execute(self._sql).arrow()

            s3_path = _build_s3_path(self._catalog_options, self._table)
            fmt = self._catalog_options.get("format", "parquet")
            reader = _FORMAT_TO_READER.get(fmt)
            if reader is None:
                raise ExecutionError(
                    plugin_error(
                        "RVT-501",
                        f"Unsupported S3 format '{fmt}' for DuckDB read.",
                        plugin_name="rivet_duckdb",
                        plugin_type="adapter",
                        adapter="S3DuckDBAdapter",
                        remediation=f"Supported formats: {', '.join(sorted(_FORMAT_TO_READER))}",
                        format=fmt,
                    )
                )
            return conn.execute(f"SELECT * FROM {reader}('{s3_path}')").arrow()
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"S3 DuckDB read failed: {exc}",
                    plugin_name="rivet_duckdb",
                    plugin_type="adapter",
                    adapter="S3DuckDBAdapter",
                    remediation="Check S3 credentials, bucket name, and network connectivity.",
                    bucket=self._catalog_options.get("bucket"),
                )
            ) from exc
        finally:
            conn.close()

    def to_arrow(self) -> pyarrow.Table:
        return self._execute()

    @property
    def schema(self) -> Schema:
        table = self._execute()
        return Schema(
            columns=[
                Column(name=f.name, type=str(f.type), nullable=f.nullable)
                for f in table.schema
            ]
        )

    @property
    def row_count(self) -> int:
        return self._execute().num_rows  # type: ignore[no-any-return]

    @property
    def size_bytes(self) -> int | None:
        return None

    @property
    def storage_type(self) -> str:
        return "s3"


class S3DuckDBAdapter(ComputeEngineAdapter):
    """DuckDB adapter for S3 catalog type.

    Loads httpfs extension, configures S3 credentials via DuckDB secret manager,
    and reads data using read_parquet/read_csv_auto/read_json_auto.
    """

    target_engine_type = "duckdb"
    catalog_type = "s3"
    capabilities = [
        *ALL_6_CAPABILITIES,
        "write_append",
        "write_replace",
        "write_partition",
    ]
    source = "engine_plugin"
    source_plugin = "rivet_duckdb"

    def read_dispatch(self, engine: Any, catalog: Any, joint: Any, pushdown: PushdownPlan | None = None) -> AdapterPushdownResult:
        # Build base SQL upfront so pushdown can modify it
        if joint.sql:
            base_sql = joint.sql
        else:
            s3_path = _build_s3_path(catalog.options, joint.table)
            fmt = catalog.options.get("format", "parquet")
            reader = _FORMAT_TO_READER.get(fmt)
            if reader is None:
                raise ExecutionError(
                    plugin_error(
                        "RVT-501",
                        f"Unsupported S3 format '{fmt}' for DuckDB read.",
                        plugin_name="rivet_duckdb",
                        plugin_type="adapter",
                        adapter="S3DuckDBAdapter",
                        remediation=f"Supported formats: {', '.join(sorted(_FORMAT_TO_READER))}",
                        format=fmt,
                    )
                )
            base_sql = f"SELECT * FROM {reader}('{s3_path}')"

        sql, residual = _apply_duckdb_pushdown(base_sql, pushdown)

        material = Material(
            name=joint.name,
            catalog=catalog.name,
            materialized_ref=_S3DuckDBMaterializedRef(
                catalog_options=catalog.options,
                sql=sql,
                table=joint.table,
            ),
            state="deferred",
        )
        return AdapterPushdownResult(material=material, residual=residual)

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        conn = duckdb.connect(":memory:")
        try:
            ensure_extension(conn, "httpfs")
            _configure_s3_secret(conn, catalog.options)

            arrow_table = material.to_arrow()
            conn.register("__write_data", arrow_table)

            s3_path = _build_s3_path(catalog.options, joint.table)
            fmt = catalog.options.get("format", "parquet")
            strategy = joint.write_strategy or "replace"

            if fmt == "parquet":
                self._write_parquet(conn, s3_path, strategy, joint)
            elif fmt == "csv":
                self._write_csv(conn, s3_path, strategy)
            elif fmt == "json":
                self._write_json(conn, s3_path, strategy)
            else:
                raise ExecutionError(
                    plugin_error(
                        "RVT-501",
                        f"Unsupported S3 write format '{fmt}' for DuckDB.",
                        plugin_name="rivet_duckdb",
                        plugin_type="adapter",
                        adapter="S3DuckDBAdapter",
                        remediation="Supported write formats: parquet, csv, json",
                        format=fmt,
                    )
                )
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"S3 DuckDB write failed: {exc}",
                    plugin_name="rivet_duckdb",
                    plugin_type="adapter",
                    adapter="S3DuckDBAdapter",
                    remediation="Check S3 credentials, bucket name, and write permissions.",
                    bucket=catalog.options.get("bucket"),
                )
            ) from exc
        finally:
            conn.close()

    @staticmethod
    def _write_parquet(conn: Any, s3_path: str, strategy: str, joint: Any) -> None:
        partition_by = getattr(joint, "partition_by", None)
        if strategy == "partition" and partition_by:
            cols = ", ".join(partition_by)
            conn.execute(
                f"COPY __write_data TO '{s3_path}' (FORMAT PARQUET, PARTITION_BY ({cols}))"
            )
        else:
            conn.execute(f"COPY __write_data TO '{s3_path}' (FORMAT PARQUET)")

    @staticmethod
    def _write_csv(conn: Any, s3_path: str, strategy: str) -> None:
        conn.execute(f"COPY __write_data TO '{s3_path}' (FORMAT CSV, HEADER)")

    @staticmethod
    def _write_json(conn: Any, s3_path: str, strategy: str) -> None:
        conn.execute(f"COPY __write_data TO '{s3_path}' (FORMAT JSON)")
