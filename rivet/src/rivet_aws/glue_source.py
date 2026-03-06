"""Glue source plugin: resolve table metadata via Glue API, return deferred Material."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import pyarrow
from botocore.exceptions import ClientError

from rivet_core.errors import ExecutionError, PluginValidationError, plugin_error
from rivet_core.models import Material
from rivet_core.plugins import SourcePlugin
from rivet_core.strategies import MaterializedRef

if TYPE_CHECKING:
    from rivet_core.models import Catalog, Joint, Schema

_ISO8601_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?)?$"
)

_KNOWN_SOURCE_OPTIONS = {"table", "partition_filter", "snapshot_time"}


def _validate_source_options(options: dict[str, Any]) -> None:
    for key in options:
        if key not in _KNOWN_SOURCE_OPTIONS:
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"Unknown source option '{key}' for glue source.",
                    plugin_name="rivet_aws",
                    plugin_type="source",
                    remediation=f"Valid options: {', '.join(sorted(_KNOWN_SOURCE_OPTIONS))}",
                )
            )
    if "table" not in options:
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                "Missing required source option 'table' for glue source.",
                plugin_name="rivet_aws",
                plugin_type="source",
                remediation="Provide 'table' in the source options.",
            )
        )
    snapshot_time = options.get("snapshot_time")
    if snapshot_time is not None and not _ISO8601_RE.match(str(snapshot_time)):
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                f"Invalid 'snapshot_time' value '{snapshot_time}': must be an ISO-8601 timestamp.",
                plugin_name="rivet_aws",
                plugin_type="source",
                remediation="Provide snapshot_time as an ISO-8601 string, e.g. '2024-01-15T00:00:00Z'.",
                snapshot_time=snapshot_time,
            )
        )


class GlueDeferredMaterializedRef(MaterializedRef):
    """MaterializedRef backed by a Glue table S3 location. Executes only on to_arrow()."""

    def __init__(
        self,
        location: str,
        input_format: str,
        partition_filter: dict[str, Any] | None,
        snapshot_time: str | None,
        partition_keys: list[str],
        partition_locations: list[str],
    ) -> None:
        self._location = location
        self._input_format = input_format
        self._partition_filter = partition_filter
        self._snapshot_time = snapshot_time
        self._partition_keys = partition_keys
        self._partition_locations = partition_locations

    def to_arrow(self) -> pyarrow.Table:
        import pyarrow.dataset as ds

        try:
            locations = self._partition_locations if self._partition_locations else [self._location]
            format_name = _input_format_to_ds_format(self._input_format)
            dataset = ds.dataset(locations, format=format_name)
            return dataset.to_table()
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Glue source read failed: {exc}",
                    plugin_name="rivet_aws",
                    plugin_type="source",
                    remediation="Check that the S3 location is accessible and the format is correct.",
                    location=self._location,
                )
            ) from exc

    @property
    def schema(self) -> Schema:
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
        return "glue"


def _input_format_to_ds_format(input_format: str) -> str:
    lower = input_format.lower()
    if "parquet" in lower:
        return "parquet"
    if "orc" in lower:
        return "orc"
    if "json" in lower:
        return "json"
    return "parquet"


def _matches_partition_filter(
    partition_values: dict[str, str], partition_filter: dict[str, Any]
) -> bool:
    """Return True if partition_values satisfies all key=value constraints."""
    return all(partition_values.get(key) == str(value) for key, value in partition_filter.items())


def _resolve_glue_table(
    catalog: Catalog,
    table_name: str,
    partition_filter: dict[str, Any] | None,
) -> tuple[str, str, list[str], list[str]]:
    """Resolve Glue table to (location, input_format, partition_keys, filtered_partition_locations).

    Only fetches partitions when partition_filter is provided.
    """
    from rivet_aws.glue_catalog import _make_glue_client as _make_client

    client = _make_client(catalog)
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
    except Exception as exc:
        raise ExecutionError(
            plugin_error(
                "RVT-503",
                f"Glue table '{table_name}' not found in database '{database}': {exc}",
                plugin_name="rivet_aws",
                plugin_type="source",
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
    if partition_keys and partition_filter is not None:
        part_kwargs: dict[str, Any] = {"DatabaseName": database, "TableName": table_name}
        if catalog_id:
            part_kwargs["CatalogId"] = catalog_id

        paginator = client.get_paginator("get_partitions")
        try:
            for page in paginator.paginate(**part_kwargs):
                for part in page.get("Partitions", []):
                    values_list = part.get("Values", [])
                    values_dict = dict(zip(partition_keys, values_list))
                    if _matches_partition_filter(values_dict, partition_filter):
                        psd = part.get("StorageDescriptor", {})
                        ploc = psd.get("Location", "")
                        if ploc:
                            partition_locations.append(ploc)
        except ClientError as exc:
            from rivet_aws.errors import handle_glue_error
            raise handle_glue_error(exc, database=database, table=table_name, action="glue:GetPartitions") from exc

    return location, input_format, partition_keys, partition_locations


class GlueSource(SourcePlugin):
    """Source plugin for glue catalog type.

    Resolves table metadata via Glue API and returns a deferred Material.
    Supports partition_filter (evaluated locally against partition key/value pairs)
    and snapshot_time (ISO-8601 timestamp, stored for downstream use).
    """

    catalog_type = "glue"

    def read(self, catalog: Catalog, joint: Joint, pushdown: Any | None) -> Material:
        source_options: dict[str, Any] = {}
        if joint.table:
            source_options["table"] = joint.table
        if hasattr(joint, "source_options") and joint.source_options:
            source_options.update(joint.source_options)

        # Fall back to joint.name if no table specified
        if "table" not in source_options and joint.name:
            source_options["table"] = joint.name

        _validate_source_options(source_options)

        table_name = source_options["table"]
        partition_filter: dict[str, Any] | None = source_options.get("partition_filter")
        snapshot_time: str | None = source_options.get("snapshot_time")

        location, input_format, partition_keys, partition_locations = _resolve_glue_table(
            catalog, table_name, partition_filter
        )

        ref = GlueDeferredMaterializedRef(
            location=location,
            input_format=input_format,
            partition_filter=partition_filter,
            snapshot_time=snapshot_time,
            partition_keys=partition_keys,
            partition_locations=partition_locations,
        )
        return Material(
            name=joint.name,
            catalog=catalog.name,
            materialized_ref=ref,
            state="deferred",
        )
