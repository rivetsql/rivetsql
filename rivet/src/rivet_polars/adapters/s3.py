"""S3PolarsAdapter: read/write S3 data via Polars native storage_options and s3fs for listing."""

from __future__ import annotations

from typing import Any

from rivet_core.errors import ExecutionError, PluginValidationError, plugin_error
from rivet_core.models import Material
from rivet_core.optimizer import EMPTY_RESIDUAL, AdapterPushdownResult, PushdownPlan
from rivet_core.plugins import ComputeEngineAdapter
from rivet_polars.engine import ALL_6_CAPABILITIES


def _build_storage_options(catalog_options: dict[str, Any]) -> dict[str, Any]:
    """Build Polars-compatible storage_options dict from catalog options."""
    opts: dict[str, Any] = {}

    key = catalog_options.get("access_key_id")
    secret = catalog_options.get("secret_access_key")
    if key and secret:
        opts["aws_access_key_id"] = key
        opts["aws_secret_access_key"] = secret
        token = catalog_options.get("session_token")
        if token:
            opts["aws_session_token"] = token

    region = catalog_options.get("region")
    if region:
        opts["region_name"] = region

    endpoint = catalog_options.get("endpoint_url")
    if endpoint:
        opts["endpoint_url"] = endpoint

    return opts


def _build_s3_path(catalog_options: dict[str, Any], table: str) -> str:
    """Build the S3 URI for reading/writing."""
    bucket = catalog_options["bucket"]
    prefix = catalog_options.get("prefix", "")
    fmt = catalog_options.get("format", "parquet")
    path = f"{prefix}/{table}" if prefix else table
    if fmt == "delta":
        return f"s3://{bucket}/{path}"
    return f"s3://{bucket}/{path}.{fmt}"


def _check_deltalake() -> None:
    """Raise PluginValidationError if deltalake is not installed."""
    import importlib.util

    if importlib.util.find_spec("deltalake") is None:
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                "Delta format requires the 'deltalake' package, which is not installed.",
                plugin_name="rivet_polars",
                plugin_type="adapter",
                remediation="Install it with: pip install rivet-polars[delta]",
                format="delta",
            )
        )


class S3PolarsAdapter(ComputeEngineAdapter):
    """Polars adapter for S3 catalog type.

    Uses Polars native storage_options for S3 reads/writes,
    s3fs for path listing, and deltalake for Delta table support.
    """

    target_engine_type = "polars"
    catalog_type = "s3"
    capabilities = [
        *ALL_6_CAPABILITIES,
        "write_append",
        "write_replace",
        "write_partition",
        "write_merge",
        "write_incremental_append",
        "write_scd2",
    ]
    source = "engine_plugin"
    source_plugin = "rivet_polars"

    def validate_catalog_options(self, catalog_options: dict[str, Any]) -> None:
        """Validate catalog options at validation time.

        Fails with RVT-201 if format=delta and deltalake is not installed.
        """
        fmt = catalog_options.get("format", "parquet")
        if fmt == "delta":
            _check_deltalake()

    def list_paths(self, catalog_options: dict[str, Any]) -> list[str]:
        """List S3 paths using s3fs."""
        import s3fs  # type: ignore[import-not-found]

        bucket = catalog_options["bucket"]
        prefix = catalog_options.get("prefix", "")
        fmt = catalog_options.get("format", "parquet")

        storage_options = _build_storage_options(catalog_options)
        s3fs_kwargs: dict[str, Any] = {}
        if "aws_access_key_id" in storage_options:
            s3fs_kwargs["key"] = storage_options["aws_access_key_id"]
            s3fs_kwargs["secret"] = storage_options["aws_secret_access_key"]
        if "aws_session_token" in storage_options:
            s3fs_kwargs["token"] = storage_options["aws_session_token"]
        if "endpoint_url" in storage_options:
            s3fs_kwargs["endpoint_url"] = storage_options["endpoint_url"]

        fs = s3fs.S3FileSystem(**s3fs_kwargs)
        glob_pattern = f"{bucket}/{prefix}/**/*.{fmt}" if prefix else f"{bucket}/**/*.{fmt}"
        return fs.glob(glob_pattern)  # type: ignore[no-any-return]

    def read_dispatch(self, engine: Any, catalog: Any, joint: Any, pushdown: PushdownPlan | None = None) -> AdapterPushdownResult:
        import polars as pl

        catalog_options = catalog.options
        fmt = catalog_options.get("format", "parquet")
        table = joint.table or joint.name
        path = _build_s3_path(catalog_options, table)
        storage_options = _build_storage_options(catalog_options)

        try:
            if fmt == "parquet":
                lf = pl.scan_parquet(path, storage_options=storage_options)
            elif fmt == "csv":
                lf = pl.scan_csv(path, storage_options=storage_options)
            elif fmt == "json":
                lf = pl.scan_ndjson(path, storage_options=storage_options)
            elif fmt == "delta":
                lf = pl.scan_delta(path, storage_options=storage_options)
            else:
                raise ExecutionError(
                    plugin_error(
                        "RVT-501",
                        f"Unsupported S3 format '{fmt}' for Polars read.",
                        plugin_name="rivet_polars",
                        plugin_type="adapter",
                        remediation="Supported formats: parquet, csv, json, delta",
                        format=fmt,
                    )
                )
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"S3 Polars read failed: {exc}",
                    plugin_name="rivet_polars",
                    plugin_type="adapter",
                    remediation="Check S3 credentials, bucket name, and network connectivity.",
                    bucket=catalog_options.get("bucket"),
                )
            ) from exc

        from rivet_polars.engine import PolarsLazyMaterializedRef

        if pushdown is not None:
            from rivet_polars.adapters.pushdown import _apply_polars_pushdown

            lf, residual = _apply_polars_pushdown(lf, pushdown)  # type: ignore[assignment]
            if isinstance(lf, pl.DataFrame):
                lf = lf.lazy()
            ref = PolarsLazyMaterializedRef(lf)
            material = Material(
                name=joint.name,
                catalog=catalog.name,
                materialized_ref=ref,
                state="deferred",
            )
            return AdapterPushdownResult(material=material, residual=residual)

        ref = PolarsLazyMaterializedRef(lf)
        material = Material(
            name=joint.name,
            catalog=catalog.name,
            materialized_ref=ref,
            state="deferred",
        )
        return AdapterPushdownResult(material=material, residual=EMPTY_RESIDUAL)

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> None:
        import polars as pl

        catalog_options = catalog.options
        fmt = catalog_options.get("format", "parquet")
        table = joint.table or joint.name
        path = _build_s3_path(catalog_options, table)
        storage_options = _build_storage_options(catalog_options)
        strategy = getattr(joint, "write_strategy", None) or "replace"

        arrow_table = material.materialized_ref.to_arrow()
        df = pl.from_arrow(arrow_table)

        try:
            if fmt == "parquet":
                df.write_parquet(path, storage_options=storage_options)  # type: ignore[union-attr]
            elif fmt == "csv":
                df.write_csv(path)  # type: ignore[union-attr]
            elif fmt == "json":
                df.write_ndjson(path)  # type: ignore[union-attr]
            elif fmt == "delta":
                mode = _delta_write_mode(strategy)
                df.write_delta(path, mode=mode, storage_options=storage_options)  # type: ignore[call-overload, union-attr]
            else:
                raise ExecutionError(
                    plugin_error(
                        "RVT-501",
                        f"Unsupported S3 write format '{fmt}' for Polars.",
                        plugin_name="rivet_polars",
                        plugin_type="adapter",
                        remediation="Supported write formats: parquet, csv, json, delta",
                        format=fmt,
                    )
                )
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"S3 Polars write failed: {exc}",
                    plugin_name="rivet_polars",
                    plugin_type="adapter",
                    remediation="Check S3 credentials, bucket name, and write permissions.",
                    bucket=catalog_options.get("bucket"),
                )
            ) from exc


def _delta_write_mode(strategy: str) -> str:
    """Map Rivet write strategy to Delta write mode."""
    _MAP = {
        "replace": "overwrite",
        "append": "append",
        "merge": "merge",
        "incremental_append": "append",
        "scd2": "merge",
        "partition": "overwrite",
    }
    return _MAP.get(strategy, "overwrite")
