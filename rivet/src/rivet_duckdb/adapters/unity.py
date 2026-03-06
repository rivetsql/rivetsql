"""UnityDuckDBAdapter: REST API metadata, credential vending, httpfs + storage read/write."""

from __future__ import annotations

import logging
import warnings
from typing import Any

import pyarrow

from rivet_core.errors import ExecutionError, plugin_error
from rivet_core.models import Material
from rivet_core.optimizer import AdapterPushdownResult, PushdownPlan
from rivet_core.plugins import ComputeEngineAdapter
from rivet_core.strategies import MaterializedRef
from rivet_duckdb.adapters.pushdown import _apply_duckdb_pushdown

_logger = logging.getLogger(__name__)

_READ_CAPABILITIES = [
    "projection_pushdown",
    "predicate_pushdown",
    "limit_pushdown",
]

_WRITE_CAPABILITIES = [
    "write_append",
    "write_replace",
]

# Map Unity data_source_format to DuckDB reader function
_FORMAT_TO_READER: dict[str, str] = {
    "PARQUET": "read_parquet",
    "DELTA": "delta_scan",
    "CSV": "read_csv_auto",
    "JSON": "read_json_auto",
    "AVRO": "read_parquet",  # fallback
}

# Reader functions that operate on a directory directly (no glob needed)
_DIRECTORY_READERS = frozenset({"delta_scan"})


class _DuckDBUnityMaterializedRef(MaterializedRef):
    """Deferred ref that reads from cloud storage via DuckDB + httpfs using vended credentials."""

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

        from rivet_duckdb.extensions import ensure_extension

        conn = duckdb.connect(":memory:")
        try:
            ensure_extension(conn, "httpfs")
            if self._reader_func == "delta_scan":
                ensure_extension(conn, "delta")
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
                    f"DuckDB Unity read failed: {exc}",
                    plugin_name="rivet_duckdb",
                    plugin_type="adapter",
                    adapter="UnityDuckDBAdapter",
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
        return "unity_storage"


class UnityDuckDBAdapter(ComputeEngineAdapter):
    """DuckDB adapter for Unity catalog: REST API metadata + credential vending + httpfs."""

    target_engine_type = "duckdb"
    catalog_type = "unity"
    capabilities = _READ_CAPABILITIES + _WRITE_CAPABILITIES
    source = "engine_plugin"
    source_plugin = "rivet_duckdb"

    def _get_unity_plugin(self) -> Any:
        """Retrieve the Unity catalog plugin from the registry (no cross-plugin import)."""
        if self._registry is None:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    "UnityDuckDBAdapter has no plugin registry; cannot resolve Unity catalog plugin.",
                    plugin_name="rivet_duckdb",
                    plugin_type="adapter",
                    adapter="UnityDuckDBAdapter",
                    remediation="Ensure the adapter is registered via PluginRegistry.register_adapter().",
                )
            )
        plugin = self._registry.get_catalog_plugin("unity")
        if plugin is None:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    "Unity catalog plugin not registered.",
                    plugin_name="rivet_duckdb",
                    plugin_type="adapter",
                    adapter="UnityDuckDBAdapter",
                    remediation="Install and register the rivet_databricks plugin.",
                )
            )
        return plugin

    def read_dispatch(self, engine: Any, catalog: Any, joint: Any, pushdown: PushdownPlan | None = None) -> AdapterPushdownResult:
        """Read from Unity-managed storage via DuckDB httpfs with vended credentials."""
        plugin = self._get_unity_plugin()
        full_name = _resolve_full_name(joint, catalog)

        table_meta = plugin.resolve_table_reference(full_name, catalog)
        storage_location = table_meta.get("storage_location")
        if not storage_location:
            raise ExecutionError(
                plugin_error(
                    "RVT-503",
                    f"No storage_location for Unity table '{full_name}'.",
                    plugin_name="rivet_duckdb",
                    plugin_type="adapter",
                    adapter="UnityDuckDBAdapter",
                    remediation="Verify the table exists and has a storage location.",
                    table=full_name,
                )
            )

        file_format = (table_meta.get("file_format") or "PARQUET").upper()
        reader_func = _FORMAT_TO_READER.get(file_format, "read_parquet")
        credentials = table_meta.get("temporary_credentials")

        # Build base SQL and apply pushdown
        location = _resolve_storage_path(storage_location, reader_func)
        base_sql = f"SELECT * FROM {reader_func}('{location}')"
        sql, residual = _apply_duckdb_pushdown(base_sql, pushdown)

        ref = _DuckDBUnityMaterializedRef(
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
        """Write to Unity-managed storage via DuckDB httpfs with vended credentials."""
        import duckdb

        from rivet_duckdb.extensions import ensure_extension

        plugin = self._get_unity_plugin()
        full_name = _resolve_full_name(joint, catalog)

        credentials = plugin.vend_credentials(full_name, catalog, operation="READ_WRITE")

        table_meta = plugin.resolve_table_reference(full_name, catalog)
        storage_location = table_meta.get("storage_location")
        if not storage_location:
            raise ExecutionError(
                plugin_error(
                    "RVT-503",
                    f"No storage_location for Unity table '{full_name}'.",
                    plugin_name="rivet_duckdb",
                    plugin_type="adapter",
                    adapter="UnityDuckDBAdapter",
                    remediation="Verify the table exists and has a storage location.",
                    table=full_name,
                )
            )

        arrow_table = material.to_arrow()
        strategy = getattr(joint, "write_strategy", None) or "replace"

        conn = duckdb.connect(":memory:")
        try:
            ensure_extension(conn, "httpfs")
            _configure_duckdb_credentials(conn, storage_location, credentials, catalog.options)
            conn.register("__write_data", arrow_table)

            if strategy == "append":
                sql = f"COPY __write_data TO '{storage_location}' (FORMAT PARQUET, APPEND)"
            else:
                # replace (default)
                sql = f"COPY __write_data TO '{storage_location}' (FORMAT PARQUET)"

            conn.execute(sql)
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"DuckDB Unity write failed: {exc}",
                    plugin_name="rivet_duckdb",
                    plugin_type="adapter",
                    adapter="UnityDuckDBAdapter",
                    remediation="Check storage location write permissions and credential validity.",
                    storage_location=storage_location,
                    strategy=strategy,
                )
            ) from exc
        finally:
            conn.close()

        return material


def _resolve_full_name(joint: Any, catalog: Any) -> str:
    """Build the three-part Unity table name from joint and catalog options."""
    table = getattr(joint, "table", None) or joint.name
    catalog_name = catalog.options.get("catalog_name", "")
    schema = catalog.options.get("schema", "default")
    # Count dots to determine qualification level
    dot_count = table.count(".")
    if dot_count >= 2:
        # Already fully qualified (catalog.schema.table) — use as-is
        return table  # type: ignore[no-any-return]
    if dot_count == 1:
        # Two-part (schema.table) — prepend Unity catalog_name
        return f"{catalog_name}.{table}"
    # Unqualified — use configured catalog_name + schema
    return f"{catalog_name}.{schema}.{table}"

# File extensions that indicate the path points to a specific file, not a directory
_FILE_EXTENSIONS = frozenset({".parquet", ".csv", ".json", ".avro", ".orc", ".gz", ".snappy", ".zst"})


def _resolve_storage_path(storage_location: str, reader_func: str) -> str:
    """Resolve the storage path for DuckDB reader functions.

    Directory-native readers (e.g. delta_scan) receive the path as-is.
    File-based readers (e.g. read_parquet) get a glob pattern appended
    when the path looks like a directory (no file extension).
    """
    # Directory-native readers handle the path directly
    if reader_func in _DIRECTORY_READERS:
        return storage_location.rstrip("/")

    from pathlib import PurePosixPath

    suffix = PurePosixPath(storage_location.rstrip("/")).suffix.lower()
    if suffix in _FILE_EXTENSIONS:
        return storage_location

    # Map reader function to expected file extension for the glob
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
    """Configure DuckDB secret manager with vended or ambient credentials for the storage URI.

    Region resolution order for S3:
      1. Vended credentials (aws_temp_credentials.region)
      2. Catalog option (catalog_options["region"])
      3. Fallback: us-east-1
    """
    if credentials is None:
        warnings.warn(
            "No vended credentials available; using ambient cloud credentials.",
            stacklevel=3,
        )
        return

    catalog_options = catalog_options or {}

    # S3 credentials (AWS STS temporary credentials)
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
            CREATE OR REPLACE SECRET unity_s3 (
                TYPE S3,
                KEY_ID '{access_key}',
                SECRET '{secret_key}',
                SESSION_TOKEN '{session_token}',
                REGION '{region}'
            )
        """)
        return

    # Azure credentials (SAS token)
    azure_creds = credentials.get("azure_user_delegation_sas")
    if azure_creds:
        sas_token = azure_creds.get("sas_token", "")
        conn.execute(f"""
            CREATE OR REPLACE SECRET unity_azure (
                TYPE AZURE,
                CONNECTION_STRING 'BlobEndpoint=https://placeholder.blob.core.windows.net;SharedAccessSignature={sas_token}'
            )
        """)
        return

    # GCS credentials (signed token)
    gcs_creds = credentials.get("gcp_oauth_token")
    if gcs_creds:
        oauth_token = gcs_creds.get("oauth_token", "")
        conn.execute(f"""
            CREATE OR REPLACE SECRET unity_gcs (
                TYPE GCS,
                TOKEN '{oauth_token}'
            )
        """)
        return

    # Unknown credential format — warn and proceed with ambient
    warnings.warn(
        f"Unrecognized credential format from Unity vending: {list(credentials.keys())}. "
        "Falling back to ambient cloud credentials.",
        stacklevel=3,
    )


