"""Unity source plugin: validate source options and return a deferred Material."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import pyarrow

from rivet_core.errors import ExecutionError, PluginValidationError, plugin_error
from rivet_core.models import Material
from rivet_core.plugins import SourcePlugin
from rivet_core.strategies import MaterializedRef

if TYPE_CHECKING:
    from rivet_core.models import Catalog, Joint, Schema

_ISO8601_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?)?$"
)

_KNOWN_SOURCE_OPTIONS = {"table", "version", "timestamp", "partition_filter"}


def _validate_source_options(options: dict[str, Any]) -> None:
    for key in options:
        if key not in _KNOWN_SOURCE_OPTIONS:
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"Unknown source option '{key}' for unity source.",
                    plugin_name="rivet_databricks",
                    plugin_type="source",
                    remediation=f"Valid options: {', '.join(sorted(_KNOWN_SOURCE_OPTIONS))}",
                )
            )
    if "table" not in options:
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                "Missing required source option 'table' for unity source.",
                plugin_name="rivet_databricks",
                plugin_type="source",
                remediation="Provide 'table' in the source options.",
            )
        )
    version = options.get("version")
    if version is not None and not isinstance(version, int):
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                f"Invalid 'version' value '{version}': must be an integer.",
                plugin_name="rivet_databricks",
                plugin_type="source",
                remediation="Provide 'version' as an integer Delta version number.",
                version=version,
            )
        )
    timestamp = options.get("timestamp")
    if timestamp is not None and not _ISO8601_RE.match(str(timestamp)):
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                f"Invalid 'timestamp' value '{timestamp}': must be an ISO-8601 timestamp.",
                plugin_name="rivet_databricks",
                plugin_type="source",
                remediation="Provide 'timestamp' as an ISO-8601 string, e.g. '2024-01-15T00:00:00Z'.",
                timestamp=timestamp,
            )
        )


class UnityDeferredMaterializedRef(MaterializedRef):
    """Deferred MaterializedRef for a Unity Catalog table. Executes only on to_arrow()."""

    def __init__(
        self,
        table: str,
        catalog: Any,
        version: int | None,
        timestamp: str | None,
        partition_filter: dict[str, Any] | None,
    ) -> None:
        self._table = table
        self._catalog = catalog
        self._version = version
        self._timestamp = timestamp
        self._partition_filter = partition_filter

    @property
    def effective_version(self) -> int | None:
        """Return the version for time travel. Version takes precedence over timestamp."""
        return self._version

    @property
    def effective_timestamp(self) -> str | None:
        """Return the timestamp for time travel, only when version is not set."""
        if self._version is not None:
            return None
        return self._timestamp

    def check_time_travel_format(self, table_format: str) -> None:
        """Raise RVT-502 if time travel is requested on a non-Delta table.

        Args:
            table_format: The table's data_source_format (e.g. 'DELTA', 'PARQUET').
        """
        if self._version is None and self._timestamp is None:
            return
        if table_format.upper() != "DELTA":
            raise ExecutionError(
                plugin_error(
                    "RVT-502",
                    (
                        f"Time travel is not supported on table '{self._table}' "
                        f"with format '{table_format}'. Only Delta tables support time travel."
                    ),
                    plugin_name="rivet_databricks",
                    plugin_type="source",
                    remediation="Remove 'version' and 'timestamp' options, or use a Delta-format table.",
                    table=self._table,
                    format=table_format,
                )
            )

    def to_arrow(self) -> pyarrow.Table:
        raise ExecutionError(
            plugin_error(
                "RVT-501",
                (
                    f"Unity source '{self._table}' requires an engine adapter to read data. "
                    "Direct to_arrow() is not supported without an adapter dispatch."
                ),
                plugin_name="rivet_databricks",
                plugin_type="source",
                remediation="Configure a compute engine with a unity adapter (e.g. DuckDB with UnityDuckDBAdapter).",
                table=self._table,
            )
        )

    @property
    def schema(self) -> Schema:
        raise NotImplementedError("Schema resolution requires adapter dispatch.")

    @property
    def row_count(self) -> int:
        raise NotImplementedError("Row count requires adapter dispatch.")

    @property
    def size_bytes(self) -> int | None:
        return None

    @property
    def storage_type(self) -> str:
        return "unity"


class UnitySource(SourcePlugin):
    """Source plugin for unity catalog type.

    Validates source options and returns a deferred Material.
    Supports table (required), version (optional Delta version integer),
    timestamp (optional ISO-8601 for time travel), and partition_filter
    (optional partition pruning predicate).
    """

    catalog_type = "unity"

    def read(self, catalog: Catalog, joint: Joint, pushdown: Any | None) -> Material:
        source_options: dict[str, Any] = {}
        if joint.table:
            source_options["table"] = joint.table
        if hasattr(joint, "source_options") and joint.source_options:
            source_options.update(joint.source_options)

        if "table" not in source_options and joint.name:
            source_options["table"] = joint.name

        _validate_source_options(source_options)

        table_name: str = source_options["table"]
        version: int | None = source_options.get("version")
        timestamp: str | None = source_options.get("timestamp")
        partition_filter: dict[str, Any] | None = source_options.get("partition_filter")

        ref = UnityDeferredMaterializedRef(
            table=table_name,
            catalog=catalog,
            version=version,
            timestamp=timestamp,
            partition_filter=partition_filter,
        )
        return Material(
            name=joint.name,
            catalog=catalog.name,
            materialized_ref=ref,
            state="deferred",
        )
