"""UnityPolarsAdapter: REST API + credential vending, deltalake for Delta tables."""

from __future__ import annotations

import warnings
from typing import Any

import pyarrow

from rivet_core.errors import ExecutionError, PluginValidationError, plugin_error
from rivet_core.models import Material
from rivet_core.optimizer import EMPTY_RESIDUAL, AdapterPushdownResult, PushdownPlan
from rivet_core.plugins import ComputeEngineAdapter
from rivet_core.strategies import MaterializedRef

_READ_CAPABILITIES = [
    "projection_pushdown",
    "predicate_pushdown",
    "limit_pushdown",
]

_WRITE_CAPABILITIES = [
    "write_append",
    "write_replace",
    "write_merge",
    "write_scd2",
    "write_partition",
]

# Map Unity data_source_format to Polars reader
_FORMAT_TO_READER: dict[str, str] = {
    "PARQUET": "parquet",
    "CSV": "csv",
    "JSON": "json",
    "DELTA": "delta",
}


def _check_deltalake() -> None:
    """Raise PluginValidationError if deltalake is not installed."""
    try:
        import deltalake  # noqa: F401
    except ImportError as exc:
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                "The 'deltalake' package is required for Delta table support in UnityPolarsAdapter.",
                plugin_name="rivet_polars",
                plugin_type="adapter",
                remediation="Install it with: pip install rivet-polars[delta] or pip install deltalake",
                format="DELTA",
            )
        ) from exc


def _resolve_full_name(joint: Any, catalog: Any) -> str:
    """Build the three-part Unity table name from joint and catalog options."""
    table = getattr(joint, "table", None) or joint.name
    catalog_name = catalog.options.get("catalog_name", "")
    schema = catalog.options.get("schema", "default")
    if "." in str(table):
        return table  # type: ignore[no-any-return]
    return f"{catalog_name}.{schema}.{table}"


def _configure_storage_options(
    storage_location: str,
    credentials: dict[str, Any] | None,
    catalog_options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build storage_options dict for Polars/deltalake from vended credentials.

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
        return {}

    catalog_options = catalog_options or {}

    aws_creds = credentials.get("aws_temp_credentials")
    if aws_creds:
        region = (
            aws_creds.get("region")
            or catalog_options.get("region")
            or "us-east-1"
        )
        return {
            "AWS_ACCESS_KEY_ID": aws_creds.get("access_key_id", ""),
            "AWS_SECRET_ACCESS_KEY": aws_creds.get("secret_access_key", ""),
            "AWS_SESSION_TOKEN": aws_creds.get("session_token", ""),
            "AWS_REGION": region,
        }

    azure_creds = credentials.get("azure_user_delegation_sas")
    if azure_creds:
        return {
            "SAS_TOKEN": azure_creds.get("sas_token", ""),
        }

    gcs_creds = credentials.get("gcp_oauth_token")
    if gcs_creds:
        return {
            "GOOGLE_SERVICE_ACCOUNT_KEY": gcs_creds.get("oauth_token", ""),
        }

    warnings.warn(
        f"Unrecognized credential format from Unity vending: {list(credentials.keys())}. "
        "Falling back to ambient cloud credentials.",
        stacklevel=3,
    )
    return {}


class _PolarsUnityMaterializedRef(MaterializedRef):
    """Deferred ref that reads from Unity-managed storage via Polars (or deltalake for Delta)."""

    def __init__(
        self,
        storage_location: str,
        file_format: str,
        storage_options: dict[str, Any],
    ) -> None:
        self._storage_location = storage_location
        self._file_format = file_format
        self._storage_options = storage_options

    def to_arrow(self) -> pyarrow.Table:
        try:
            return self._read().to_arrow()
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Polars Unity read failed: {exc}",
                    plugin_name="rivet_polars",
                    plugin_type="adapter",
                    remediation="Check storage location accessibility and credential validity.",
                    storage_location=self._storage_location,
                    format=self._file_format,
                )
            ) from exc

    def _read(self) -> Any:
        import polars as pl

        fmt = self._file_format.upper()
        loc = self._storage_location
        opts = self._storage_options

        if fmt == "DELTA":
            import deltalake

            dt = deltalake.DeltaTable(loc, storage_options=opts or None)
            return pl.from_arrow(dt.to_pyarrow_table())

        if fmt == "CSV":
            return pl.scan_csv(loc, storage_options=opts or None)

        if fmt == "JSON":
            return pl.scan_ndjson(loc, storage_options=opts or None)

        # Default: parquet
        return pl.scan_parquet(loc, storage_options=opts or None)

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


class UnityPolarsAdapter(ComputeEngineAdapter):
    """Polars adapter for Unity catalog: REST API metadata + credential vending + deltalake."""

    target_engine_type = "polars"
    catalog_type = "unity"
    capabilities = _READ_CAPABILITIES + _WRITE_CAPABILITIES
    source = "engine_plugin"
    source_plugin = "rivet_polars"

    def _get_unity_plugin(self) -> Any:
        """Retrieve the Unity catalog plugin from the registry (no cross-plugin import)."""
        if self._registry is None:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    "UnityPolarsAdapter has no plugin registry; cannot resolve Unity catalog plugin.",
                    plugin_name="rivet_polars",
                    plugin_type="adapter",
                    remediation="Ensure the adapter is registered via PluginRegistry.register_adapter().",
                )
            )
        plugin = self._registry.get_catalog_plugin("unity")
        if plugin is None:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    "Unity catalog plugin not registered.",
                    plugin_name="rivet_polars",
                    plugin_type="adapter",
                    remediation="Install and register the rivet_databricks plugin.",
                )
            )
        return plugin

    def validate(self, catalog: Any) -> None:
        """Fail at validation time if Delta format requested but deltalake not installed."""
        fmt = (catalog.options.get("format") or "delta").upper()
        if fmt == "DELTA":
            _check_deltalake()

    def read_dispatch(self, engine: Any, catalog: Any, joint: Any, pushdown: PushdownPlan | None = None) -> AdapterPushdownResult:
        """Read from Unity-managed storage via Polars with vended credentials."""
        plugin = self._get_unity_plugin()
        full_name = _resolve_full_name(joint, catalog)

        table_meta = plugin.resolve_table_reference(full_name, catalog)
        storage_location = table_meta.get("storage_location")
        if not storage_location:
            raise ExecutionError(
                plugin_error(
                    "RVT-503",
                    f"No storage_location for Unity table '{full_name}'.",
                    plugin_name="rivet_polars",
                    plugin_type="adapter",
                    remediation="Verify the table exists and has a storage location.",
                    table=full_name,
                )
            )

        file_format = (table_meta.get("file_format") or "PARQUET").upper()

        if file_format == "DELTA":
            _check_deltalake()

        credentials = table_meta.get("temporary_credentials")
        storage_options = _configure_storage_options(
            storage_location, credentials, catalog_options=catalog.options,
        )

        ref = _PolarsUnityMaterializedRef(
            storage_location=storage_location,
            file_format=file_format,
            storage_options=storage_options,
        )

        if pushdown is not None:
            import polars as pl

            from rivet_polars.adapters.pushdown import _apply_polars_pushdown
            from rivet_polars.engine import PolarsLazyMaterializedRef

            try:
                df = ref._read()
            except Exception:
                # If read fails, fall through to return deferred ref with empty residual
                material = Material(
                    name=joint.name,
                    catalog=catalog.name,
                    materialized_ref=ref,
                    state="deferred",
                )
                return AdapterPushdownResult(material=material, residual=EMPTY_RESIDUAL)

            df, residual = _apply_polars_pushdown(df, pushdown)

            if isinstance(df, pl.LazyFrame):
                new_ref = PolarsLazyMaterializedRef(df)
            else:
                new_ref = PolarsLazyMaterializedRef(df.lazy())

            material = Material(
                name=joint.name,
                catalog=catalog.name,
                materialized_ref=new_ref,
                state="deferred",
            )
            return AdapterPushdownResult(material=material, residual=residual)

        material = Material(
            name=joint.name,
            catalog=catalog.name,
            materialized_ref=ref,
            state="deferred",
        )
        return AdapterPushdownResult(material=material, residual=EMPTY_RESIDUAL)

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        """Write to Unity-managed storage via Polars/deltalake with vended credentials."""
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
                    plugin_name="rivet_polars",
                    plugin_type="adapter",
                    remediation="Verify the table exists and has a storage location.",
                    table=full_name,
                )
            )

        file_format = (table_meta.get("file_format") or "PARQUET").upper()
        strategy = getattr(joint, "write_strategy", None) or "replace"

        if file_format == "DELTA":
            _check_deltalake()
            _write_delta(material, storage_location, strategy, credentials, catalog_options=catalog.options)
        else:
            _write_parquet(material, storage_location, strategy, credentials, catalog_options=catalog.options)

        return material


def _write_delta(
    material: Any,
    storage_location: str,
    strategy: str,
    credentials: dict[str, Any] | None,
    catalog_options: dict[str, Any] | None = None,
) -> None:
    """Write Arrow data to a Delta table using deltalake."""
    import deltalake

    storage_options = _configure_storage_options(
        storage_location, credentials, catalog_options=catalog_options,
    )
    arrow_table = material.to_arrow()

    mode_map = {
        "append": "append",
        "replace": "overwrite",
        "merge": "merge",
        "scd2": "merge",
        "partition": "overwrite",
    }
    mode = mode_map.get(strategy, "overwrite")

    try:
        deltalake.write_deltalake(  # type: ignore[call-overload]
            storage_location,
            arrow_table,
            mode=mode,
            storage_options=storage_options or None,
        )
    except Exception as exc:
        raise ExecutionError(
            plugin_error(
                "RVT-501",
                f"Delta write failed for '{storage_location}': {exc}",
                plugin_name="rivet_polars",
                plugin_type="adapter",
                remediation="Check storage location write permissions and credential validity.",
                storage_location=storage_location,
                strategy=strategy,
            )
        ) from exc


def _write_parquet(
    material: Any,
    storage_location: str,
    strategy: str,
    credentials: dict[str, Any] | None,
    catalog_options: dict[str, Any] | None = None,
) -> None:
    """Write Arrow data as Parquet using Polars."""
    import polars as pl

    storage_options = _configure_storage_options(
        storage_location, credentials, catalog_options=catalog_options,
    )
    arrow_table = material.to_arrow()
    df = pl.from_arrow(arrow_table)

    try:
        df.write_parquet(storage_location, storage_options=storage_options or None)  # type: ignore[union-attr]
    except Exception as exc:
        raise ExecutionError(
            plugin_error(
                "RVT-501",
                f"Polars Parquet write failed for '{storage_location}': {exc}",
                plugin_name="rivet_polars",
                plugin_type="adapter",
                remediation="Check storage location write permissions and credential validity.",
                storage_location=storage_location,
                strategy=strategy,
            )
        ) from exc
