"""GluePolarsAdapter: boto3 for metadata, Polars S3 for data, reject ORC format.

Uses boto3 GetTable/GetPartitions for Glue metadata, applies partition pruning,
builds Polars storage_options from AWS credentials, and reads/writes via Polars
native S3 support. ORC format is rejected with RVT-202.
"""

from __future__ import annotations

import sys
from typing import Any

import pyarrow

from rivet_core.credentials import CredentialResolver
from rivet_core.errors import ExecutionError, plugin_error
from rivet_core.models import Column, Material, Schema
from rivet_core.optimizer import EMPTY_RESIDUAL, AdapterPushdownResult, PushdownPlan
from rivet_core.plugins import ComputeEngineAdapter
from rivet_core.strategies import MaterializedRef

ALL_6_CAPABILITIES = [
    "projection_pushdown",
    "predicate_pushdown",
    "limit_pushdown",
    "cast_pushdown",
    "join",
    "aggregation",
]


def _check_deltalake(catalog: Any) -> None:
    """Raise PluginValidationError if Delta format requested but deltalake not installed."""
    fmt = catalog.options.get("format", "parquet")
    if fmt == "delta" and sys.modules.get("deltalake") is None:
        try:
            import deltalake  # noqa: F401
        except ImportError:
            from rivet_core.errors import PluginValidationError
            raise PluginValidationError(  # noqa: B904
                plugin_error(
                    "RVT-201",
                    "Delta format requested but 'deltalake' package is not installed.",
                    plugin_name="rivet_polars",
                    plugin_type="adapter",
                    remediation="Install the deltalake package: pip install rivet-polars[delta] or pip install deltalake",
                    format="delta",
                )
            )


def _detect_format(input_format: str) -> str:
    """Map Glue InputFormat class name to a format string. Raises RVT-202 for ORC."""
    lower = input_format.lower()
    if "orc" in lower:
        raise ExecutionError(
            plugin_error(
                "RVT-202",
                (
                    f"GluePolarsAdapter does not support ORC format (InputFormat: {input_format}). "
                    "Polars does not natively read ORC files."
                ),
                plugin_name="rivet_polars",
                plugin_type="adapter",
                remediation="Convert the Glue table to Parquet or CSV format, "
                    "or use a DuckDB or PySpark engine which supports ORC.",
                input_format=input_format,
            )
        )
    if "parquet" in lower:
        return "parquet"
    if "json" in lower:
        return "json"
    # TextInputFormat and others → csv
    return "csv"


def _make_resolver(catalog_options: dict[str, Any]) -> CredentialResolver:
    """Create a CredentialResolver from the factory injected by the catalog plugin."""
    factory = catalog_options.get("_credential_resolver_factory")
    if factory is None:
        raise ExecutionError(
            plugin_error(
                "RVT-501",
                "No credential resolver factory in catalog options.",
                plugin_name="rivet_polars",
                plugin_type="adapter",
                adapter="GluePolarsAdapter",
                remediation="Ensure the Glue catalog plugin is registered.",
            )
        )
    region = catalog_options.get("region", "us-east-1")
    return factory(catalog_options, region)  # type: ignore[no-any-return]


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
    """Resolve Glue table via boto3, applying Polars format detection.

    Returns (location, format, partition_keys, filtered_locations).
    Raises RVT-202 for ORC format, RVT-503 if table not found.
    """
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
                plugin_name="rivet_polars",
                plugin_type="adapter",
                adapter="GluePolarsAdapter",
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

    fmt = _detect_format(input_format)
    return location, fmt, partition_keys, partition_locations


def _build_storage_options(catalog_options: dict[str, Any]) -> dict[str, Any]:
    """Build Polars storage_options dict from AWS credentials."""
    region = catalog_options.get("region", "us-east-1")
    creds = _make_resolver(catalog_options).resolve()

    opts: dict[str, Any] = {
        "aws_access_key_id": creds.access_key_id,
        "aws_secret_access_key": creds.secret_access_key,
        "aws_region": region,
    }
    if creds.session_token:
        opts["aws_session_token"] = creds.session_token
    endpoint_url = catalog_options.get("endpoint_url")
    if endpoint_url:
        opts["aws_endpoint_url"] = endpoint_url
    return opts


class GluePolarsReadRef(MaterializedRef):
    """Deferred MaterializedRef that reads Glue table data via Polars S3 support."""

    def __init__(
        self,
        catalog_options: dict[str, Any],
        table_name: str,
        partition_filter: dict[str, Any] | None,
    ) -> None:
        self._catalog_options = catalog_options
        self._table_name = table_name
        self._partition_filter = partition_filter
        self._cached_table: pyarrow.Table | None = None

    def _read(self) -> pyarrow.Table:
        import polars as pl

        location, fmt, partition_keys, partition_locations = _resolve_glue_table(
            self._catalog_options, self._table_name, self._partition_filter
        )
        storage_options = _build_storage_options(self._catalog_options)

        # Determine paths to read
        if partition_locations:
            paths = [loc.rstrip("/") + "/**" for loc in partition_locations]
        else:
            path = location.rstrip("/") + "/**"
            paths = [path]

        try:
            if fmt == "parquet":
                if len(paths) == 1:
                    df = pl.read_parquet(paths[0], storage_options=storage_options)
                else:
                    frames = [pl.read_parquet(p, storage_options=storage_options) for p in paths]
                    df = pl.concat(frames)
            elif fmt == "csv":
                if len(paths) == 1:
                    df = pl.read_csv(paths[0], storage_options=storage_options)
                else:
                    frames = [pl.read_csv(p, storage_options=storage_options) for p in paths]
                    df = pl.concat(frames)
            else:  # json
                if len(paths) == 1:
                    df = pl.read_ndjson(paths[0], storage_options=storage_options)
                else:
                    frames = [pl.read_ndjson(p, storage_options=storage_options) for p in paths]
                    df = pl.concat(frames)
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"GluePolarsAdapter read failed for table '{self._table_name}': {exc}",
                    plugin_name="rivet_polars",
                    plugin_type="adapter",
                    remediation="Check S3 access permissions and that the table data exists.",
                    table=self._table_name,
                    location=location,
                )
            ) from exc

        return df.to_arrow()

    def to_arrow(self) -> pyarrow.Table:
        if self._cached_table is None:
            self._cached_table = self._read()
        return self._cached_table

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
        return "glue_polars"


class GluePolarsAdapter(ComputeEngineAdapter):
    """Polars adapter for Glue catalog type.

    Queries Glue API via boto3 for table metadata, applies partition pruning,
    builds Polars storage_options from AWS credentials, and reads/writes via
    Polars native S3 support. ORC format is rejected with RVT-202.
    """

    target_engine_type = "polars"
    catalog_type = "glue"
    capabilities = ALL_6_CAPABILITIES
    write_capabilities = [
        "write_append",
        "write_replace",
        "write_delete_insert",
        "write_partition",
    ]
    source = "engine_plugin"
    source_plugin = "rivet_polars"

    def validate(self, catalog: Any) -> None:
        """Validate at compile time. Fails if Delta requested without deltalake installed."""
        _check_deltalake(catalog)

    def read_dispatch(self, engine: Any, catalog: Any, joint: Any, pushdown: PushdownPlan | None = None) -> AdapterPushdownResult:
        catalog_options = catalog.options if hasattr(catalog, "options") else {}
        table_name = getattr(joint, "table", None) or getattr(joint, "name", "unknown")
        partition_filter: dict[str, Any] | None = None
        if hasattr(joint, "source_options") and joint.source_options:
            partition_filter = joint.source_options.get("partition_filter")

        ref = GluePolarsReadRef(
            catalog_options=catalog_options,
            table_name=table_name,  # type: ignore[arg-type]
            partition_filter=partition_filter,
        )

        if pushdown is not None:
            import polars as pl

            from rivet_polars.adapters.pushdown import _apply_polars_pushdown
            from rivet_polars.engine import PolarsLazyMaterializedRef

            try:
                arrow_table = ref.to_arrow()
                df = pl.from_arrow(arrow_table)
            except Exception:
                material = Material(
                    name=getattr(joint, "name", "glue_read"),
                    catalog=getattr(catalog, "name", "glue"),
                    materialized_ref=ref,
                    state="deferred",
                )
                return AdapterPushdownResult(material=material, residual=EMPTY_RESIDUAL)

            df, residual = _apply_polars_pushdown(df, pushdown)  # type: ignore[arg-type, assignment]
            if isinstance(df, pl.DataFrame):
                df = df.lazy()  # type: ignore[assignment]
            new_ref = PolarsLazyMaterializedRef(df)
            material = Material(
                name=getattr(joint, "name", "glue_read"),
                catalog=getattr(catalog, "name", "glue"),
                materialized_ref=new_ref,
                state="deferred",
            )
            return AdapterPushdownResult(material=material, residual=residual)

        material = Material(
            name=getattr(joint, "name", "glue_read"),
            catalog=getattr(catalog, "name", "glue"),
            materialized_ref=ref,
            state="deferred",
        )
        return AdapterPushdownResult(material=material, residual=EMPTY_RESIDUAL)

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        import polars as pl

        catalog_options = catalog.options if hasattr(catalog, "options") else {}
        table_name = getattr(joint, "table", None) or getattr(joint, "name", "unknown")
        write_strategy = getattr(joint, "write_strategy", "replace") or "replace"

        # Resolve Glue metadata (also validates format — raises RVT-202 for ORC)
        location, fmt, partition_keys, _ = _resolve_glue_table(
            catalog_options, table_name, None  # type: ignore[arg-type]
        )
        location = location.rstrip("/")
        storage_options = _build_storage_options(catalog_options)

        arrow_table = material.to_arrow() if hasattr(material, "to_arrow") else material
        df = pl.from_arrow(arrow_table)

        try:
            if write_strategy == "partition":
                partition_by = getattr(joint, "partition_by", None) or partition_keys
                if partition_by:
                    cols = partition_by if isinstance(partition_by, list) else [partition_by]
                    for part_val in df.select(cols).unique().to_dicts():  # type: ignore[union-attr]
                        mask = pl.lit(True)
                        for col, val in part_val.items():
                            mask = mask & (pl.col(col) == val)
                        part_df = df.filter(mask)  # type: ignore[arg-type]
                        part_path = location
                        for col, val in part_val.items():
                            part_path = f"{part_path}/{col}={val}"
                        part_df.write_parquet(  # type: ignore[union-attr]
                            f"{part_path}/data.parquet",
                            storage_options=storage_options,
                        )
                else:
                    df.write_parquet(  # type: ignore[union-attr]
                        f"{location}/data.parquet",
                        storage_options=storage_options,
                    )
            else:
                # append, replace, delete_insert all write parquet to the location
                df.write_parquet(  # type: ignore[union-attr]
                    f"{location}/data.parquet",
                    storage_options=storage_options,
                )
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"GluePolarsAdapter write failed for table '{table_name}': {exc}",
                    plugin_name="rivet_polars",
                    plugin_type="adapter",
                    remediation="Check S3 write permissions and that the location is accessible.",
                    table=table_name,
                    location=location,
                )
            ) from exc
