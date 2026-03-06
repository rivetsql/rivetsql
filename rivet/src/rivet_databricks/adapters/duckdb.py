"""DatabricksDuckDBAdapter: credential vending, httpfs + storage read/write via DuckDB."""

from __future__ import annotations

import logging
import warnings
from typing import Any

import pyarrow

from rivet_core.errors import ExecutionError, plugin_error
from rivet_core.models import Material
from rivet_core.optimizer import AdapterPushdownResult, Cast, PushdownPlan, ResidualPlan
from rivet_core.plugins import ComputeEngineAdapter
from rivet_core.strategies import MaterializedRef

_logger = logging.getLogger(__name__)

_READ_CAPABILITIES = [
    "projection_pushdown",
    "predicate_pushdown",
    "limit_pushdown",
]

_WRITE_CAPABILITIES = [
    "write_append",
    "write_replace",
    "write_partition",
]

_FORMAT_TO_READER: dict[str, str] = {
    "PARQUET": "read_parquet",
    "DELTA": "delta_scan",
    "CSV": "read_csv_auto",
    "JSON": "read_json_auto",
    "AVRO": "read_parquet",
}

_DIRECTORY_READERS = frozenset({"delta_scan"})

_FILE_EXTENSIONS = frozenset({".parquet", ".csv", ".json", ".avro", ".orc", ".gz", ".snappy", ".zst"})

_EMPTY_RESIDUAL = ResidualPlan(predicates=[], limit=None, casts=[])


def _ensure_duckdb_extension(conn: Any, ext: str) -> None:
    """Check-then-load a DuckDB extension."""
    try:
        row = conn.execute(
            "SELECT installed, loaded FROM duckdb_extensions() WHERE extension_name = ?",
            [ext],
        ).fetchone()
        installed = row[0] if row is not None else False
        loaded = row[1] if row is not None else False
        if loaded:
            return
        if not installed:
            conn.execute(f"INSTALL {ext}")
        conn.execute(f"LOAD {ext}")
    except ExecutionError:
        raise
    except Exception as exc:
        raise ExecutionError(
            plugin_error(
                "RVT-502",
                f"Failed to load DuckDB extension '{ext}': {exc}",
                plugin_name="rivet_databricks",
                plugin_type="adapter",
                remediation=(
                    f"Run: INSTALL {ext}; LOAD {ext}; "
                    "or set the DuckDB extension directory for offline environments."
                ),
                extension=ext,
            )
        ) from exc


def _apply_duckdb_pushdown(
    base_sql: str,
    pushdown: PushdownPlan | None,
) -> tuple[str, ResidualPlan]:
    """Apply pushdown operations to a DuckDB SQL query."""
    if pushdown is None:
        return base_sql, _EMPTY_RESIDUAL

    sql = base_sql
    residual_predicates: list[Any] = list(pushdown.predicates.residual)
    residual_casts: list[Cast] = list(pushdown.casts.residual)
    residual_limit: int | None = pushdown.limit.residual_limit

    if pushdown.projections.pushed_columns is not None:
        try:
            cols = ", ".join(pushdown.projections.pushed_columns)
            sql = sql.replace("SELECT *", f"SELECT {cols}", 1)
        except Exception:
            pass

    if pushdown.predicates.pushed:
        where_parts: list[str] = []
        for pred in pushdown.predicates.pushed:
            try:
                where_parts.append(pred.expression)
            except Exception:
                residual_predicates.append(pred)
        if where_parts:
            where_clause = " AND ".join(where_parts)
            sql = f"SELECT * FROM ({sql}) AS __pd WHERE {where_clause}"

    if pushdown.limit.pushed_limit is not None:
        try:
            sql = f"{sql} LIMIT {pushdown.limit.pushed_limit}"
        except Exception:
            residual_limit = pushdown.limit.pushed_limit

    for cast in pushdown.casts.pushed:
        try:
            sql = sql.replace(cast.column, f"CAST({cast.column} AS {cast.to_type})")
        except Exception:
            residual_casts.append(cast)

    return sql, ResidualPlan(predicates=residual_predicates, limit=residual_limit, casts=residual_casts)


def _resolve_full_name(joint: Any, catalog: Any) -> str:
    """Build the three-part Databricks table name from joint and catalog options."""
    table = getattr(joint, "table", None) or joint.name
    catalog_name = catalog.options.get("catalog", "")
    schema = catalog.options.get("schema", "default")
    dot_count = table.count(".")
    if dot_count >= 2:
        return table  # type: ignore[no-any-return]
    if dot_count == 1:
        return f"{catalog_name}.{table}"
    return f"{catalog_name}.{schema}.{table}"


def _resolve_storage_path(storage_location: str, reader_func: str) -> str:
    """Resolve the storage path for DuckDB reader functions."""
    if reader_func in _DIRECTORY_READERS:
        return storage_location.rstrip("/")

    from pathlib import PurePosixPath

    suffix = PurePosixPath(storage_location.rstrip("/")).suffix.lower()
    if suffix in _FILE_EXTENSIONS:
        return storage_location

    ext = {
        "read_parquet": "parquet",
        "read_csv_auto": "csv",
        "read_json_auto": "json",
    }.get(reader_func, "parquet")

    return f"{storage_location.rstrip('/')}/**/*.{ext}"


def _configure_duckdb_credentials(
    conn: Any,
    storage_location: str,
    credentials: dict[str, Any] | None,
    catalog_options: dict[str, Any] | None = None,
) -> None:
    """Configure DuckDB secret manager with vended or ambient credentials."""
    if credentials is None:
        warnings.warn(
            "No vended credentials available; using ambient cloud credentials.",
            stacklevel=3,
        )
        return

    catalog_options = catalog_options or {}

    aws_creds = credentials.get("aws_temp_credentials")
    if aws_creds:
        access_key = aws_creds.get("access_key_id", "")
        secret_key = aws_creds.get("secret_access_key", "")
        session_token = aws_creds.get("session_token", "")
        region = (
            aws_creds.get("region")
            or catalog_options.get("region")
            or "us-east-1"
        )
        conn.execute(f"""
            CREATE OR REPLACE SECRET databricks_s3 (
                TYPE S3,
                KEY_ID '{access_key}',
                SECRET '{secret_key}',
                SESSION_TOKEN '{session_token}',
                REGION '{region}'
            )
        """)
        return

    azure_creds = credentials.get("azure_user_delegation_sas")
    if azure_creds:
        sas_token = azure_creds.get("sas_token", "")
        conn.execute(f"""
            CREATE OR REPLACE SECRET databricks_azure (
                TYPE AZURE,
                CONNECTION_STRING 'BlobEndpoint=https://placeholder.blob.core.windows.net;SharedAccessSignature={sas_token}'
            )
        """)
        return

    gcs_creds = credentials.get("gcp_oauth_token")
    if gcs_creds:
        oauth_token = gcs_creds.get("oauth_token", "")
        conn.execute(f"""
            CREATE OR REPLACE SECRET databricks_gcs (
                TYPE GCS,
                TOKEN '{oauth_token}'
            )
        """)
        return

    warnings.warn(
        f"Unrecognized credential format from Databricks vending: {list(credentials.keys())}. "
        "Falling back to ambient cloud credentials.",
        stacklevel=3,
    )


class _DatabricksDuckDBMaterializedRef(MaterializedRef):
    """Deferred ref that reads from Databricks-managed storage via DuckDB + httpfs."""

    def __init__(
        self,
        storage_location: str,
        reader_func: str,
        credentials: dict[str, Any] | None,
        catalog_options: dict[str, Any] | None = None,
        sql_override: str | None = None,
    ) -> None:
        self._storage_location = storage_location
        self._reader_func = reader_func
        self._credentials = credentials
        self._catalog_options = catalog_options or {}
        self._sql_override = sql_override

    def to_arrow(self) -> pyarrow.Table:
        import duckdb

        conn = duckdb.connect(":memory:")
        try:
            _ensure_duckdb_extension(conn, "httpfs")
            if self._reader_func == "delta_scan":
                _ensure_duckdb_extension(conn, "delta")
            _configure_duckdb_credentials(conn, self._storage_location, self._credentials, self._catalog_options)
            location = _resolve_storage_path(self._storage_location, self._reader_func)
            sql = self._sql_override or f"SELECT * FROM {self._reader_func}('{location}')"
            return conn.execute(sql).arrow().read_all()
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"DuckDB Databricks read failed: {exc}",
                    plugin_name="rivet_databricks",
                    plugin_type="adapter",
                    adapter="DatabricksDuckDBAdapter",
                    remediation="Check storage location accessibility and credential validity.",
                    storage_location=self._storage_location,
                )
            ) from exc
        finally:
            conn.close()

    @property
    def schema(self) -> Any:
        from rivet_core.models import Column, Schema

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
        return "databricks_storage"


class DatabricksDuckDBAdapter(ComputeEngineAdapter):
    """DuckDB adapter for Databricks catalog: REST API metadata + credential vending + httpfs."""

    target_engine_type = "duckdb"
    catalog_type = "databricks"
    capabilities = _READ_CAPABILITIES + _WRITE_CAPABILITIES
    source = "catalog_plugin"
    source_plugin = "rivet_databricks"

    def read_dispatch(self, engine: Any, catalog: Any, joint: Any, pushdown: PushdownPlan | None = None) -> AdapterPushdownResult:
        """Read from Databricks-managed storage via DuckDB httpfs with vended credentials."""
        from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin

        plugin = DatabricksCatalogPlugin()
        full_name = _resolve_full_name(joint, catalog)

        table_meta = _get_table_metadata(plugin, full_name, catalog)
        storage_location = table_meta.get("storage_location")
        if not storage_location:
            raise ExecutionError(
                plugin_error(
                    "RVT-503",
                    f"No storage_location for Databricks table '{full_name}'.",
                    plugin_name="rivet_databricks",
                    plugin_type="adapter",
                    adapter="DatabricksDuckDBAdapter",
                    remediation="Verify the table exists and has a storage location.",
                    table=full_name,
                )
            )

        file_format = (table_meta.get("file_format") or "PARQUET").upper()
        reader_func = _FORMAT_TO_READER.get(file_format, "read_parquet")
        credentials = table_meta.get("temporary_credentials")

        location = _resolve_storage_path(storage_location, reader_func)
        base_sql = f"SELECT * FROM {reader_func}('{location}')"
        sql, residual = _apply_duckdb_pushdown(base_sql, pushdown)

        ref = _DatabricksDuckDBMaterializedRef(
            storage_location=storage_location,
            reader_func=reader_func,
            credentials=credentials,
            catalog_options=catalog.options,
            sql_override=sql,
        )
        material = Material(
            name=joint.name,
            catalog=catalog.name,
            materialized_ref=ref,
            state="deferred",
        )
        return AdapterPushdownResult(material=material, residual=residual)

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        """Write to Databricks-managed storage via DuckDB httpfs with vended credentials."""
        import duckdb

        from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin

        plugin = DatabricksCatalogPlugin()
        full_name = _resolve_full_name(joint, catalog)

        table_meta = _get_table_metadata(plugin, full_name, catalog)
        storage_location = table_meta.get("storage_location")
        if not storage_location:
            raise ExecutionError(
                plugin_error(
                    "RVT-503",
                    f"No storage_location for Databricks table '{full_name}'.",
                    plugin_name="rivet_databricks",
                    plugin_type="adapter",
                    adapter="DatabricksDuckDBAdapter",
                    remediation="Verify the table exists and has a storage location.",
                    table=full_name,
                )
            )

        credentials = table_meta.get("temporary_credentials")
        arrow_table = material.to_arrow()
        strategy = getattr(joint, "write_strategy", None) or "replace"

        conn = duckdb.connect(":memory:")
        try:
            _ensure_duckdb_extension(conn, "httpfs")
            _configure_duckdb_credentials(conn, storage_location, credentials, catalog.options)
            conn.register("__write_data", arrow_table)

            if strategy == "append":
                sql = f"COPY __write_data TO '{storage_location}' (FORMAT PARQUET, APPEND)"
            elif strategy == "partition":
                partition_by = getattr(joint, "partition_by", None)
                if partition_by:
                    cols = ", ".join(partition_by) if isinstance(partition_by, list) else partition_by
                    sql = f"COPY __write_data TO '{storage_location}' (FORMAT PARQUET, PARTITION_BY ({cols}))"
                else:
                    sql = f"COPY __write_data TO '{storage_location}' (FORMAT PARQUET)"
            else:
                sql = f"COPY __write_data TO '{storage_location}' (FORMAT PARQUET)"

            conn.execute(sql)
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"DuckDB Databricks write failed: {exc}",
                    plugin_name="rivet_databricks",
                    plugin_type="adapter",
                    adapter="DatabricksDuckDBAdapter",
                    remediation="Check storage location write permissions and credential validity.",
                    storage_location=storage_location,
                    strategy=strategy,
                )
            ) from exc
        finally:
            conn.close()

        return material


def _get_table_metadata(plugin: Any, full_name: str, catalog: Any) -> dict[str, Any]:
    """Get table metadata via the DatabricksCatalogPlugin, with graceful fallback on HTTP 403."""
    from rivet_databricks.auth import resolve_credentials
    from rivet_databricks.client import UnityCatalogClient

    host = catalog.options["workspace_url"]
    credential = resolve_credentials(catalog.options, host=host)
    client = UnityCatalogClient(host=host, credential=credential)
    try:
        raw = client.get_table(full_name)
        table_id = raw.get("table_id") or raw.get("full_name") or full_name
        try:
            temporary_credentials = client.vend_credentials(table_id, operation="READ")
        except ExecutionError as exc:
            if exc.error.code == "RVT-508":
                warnings.warn(
                    f"Credential vending unavailable for '{full_name}': {exc.error.message} "
                    "Falling back to ambient cloud credentials.",
                    stacklevel=4,
                )
                temporary_credentials = None
            else:
                raise
    finally:
        client.close()

    return {
        "storage_location": raw.get("storage_location"),
        "file_format": raw.get("data_source_format"),
        "temporary_credentials": temporary_credentials,
    }
