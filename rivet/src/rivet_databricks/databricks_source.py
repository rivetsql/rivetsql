"""Databricks source plugin: time travel (VERSION AS OF, TIMESTAMP AS OF) and Change Data Feed."""

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

_KNOWN_SOURCE_OPTIONS = {"table", "version", "change_data_feed"}


def _validate_source_options(options: dict[str, Any]) -> None:
    for key in options:
        if key not in _KNOWN_SOURCE_OPTIONS:
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"Unknown source option '{key}' for databricks source.",
                    plugin_name="rivet_databricks",
                    plugin_type="source",
                    remediation=f"Valid options: {', '.join(sorted(_KNOWN_SOURCE_OPTIONS))}",
                )
            )
    if "table" not in options:
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                "Missing required source option 'table' for databricks source.",
                plugin_name="rivet_databricks",
                plugin_type="source",
                remediation="Provide 'table' in the source options.",
            )
        )
    version = options.get("version")
    if (
        version is not None
        and not isinstance(version, int)
        and not (isinstance(version, str) and _ISO8601_RE.match(version))
    ):
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                f"Invalid 'version' value '{version}': must be an integer or ISO-8601 timestamp string.",
                plugin_name="rivet_databricks",
                plugin_type="source",
                remediation="Provide 'version' as an integer Delta version or ISO-8601 timestamp string.",
                version=version,
            )
        )
    cdf = options.get("change_data_feed")
    if cdf is not None and not isinstance(cdf, bool):
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                f"Invalid 'change_data_feed' value '{cdf}': must be a boolean.",
                plugin_name="rivet_databricks",
                plugin_type="source",
                remediation="Set 'change_data_feed' to true or false.",
            )
        )


def build_source_sql(
    table: str,
    version: int | str | None = None,
    change_data_feed: bool = False,
) -> str:
    """Build the SQL for a Databricks source read.

    Returns SQL with time travel or table_changes() as appropriate.
    """
    if change_data_feed:
        if isinstance(version, int):
            return f"SELECT * FROM table_changes('{table}', {version})"
        if isinstance(version, str):
            return f"SELECT * FROM table_changes('{table}', '{version}')"
        return f"SELECT * FROM table_changes('{table}')"

    if isinstance(version, int):
        return f"SELECT * FROM {table} VERSION AS OF {version}"
    if isinstance(version, str):
        return f"SELECT * FROM {table} TIMESTAMP AS OF '{version}'"
    return f"SELECT * FROM {table}"


class DatabricksDeferredMaterializedRef(MaterializedRef):
    """Deferred MaterializedRef for a Databricks table. Executes only on to_arrow()."""

    def __init__(
        self,
        table: str,
        sql: str,
        version: int | str | None,
        change_data_feed: bool,
    ) -> None:
        self._table = table
        self._sql = sql
        self._version = version
        self._change_data_feed = change_data_feed

    @property
    def sql(self) -> str:
        return self._sql

    def check_time_travel_format(self, table_format: str) -> None:
        """Raise RVT-504 if time travel is requested on a non-Delta table."""
        if self._version is None:
            return
        if table_format.upper() != "DELTA":
            raise ExecutionError(
                plugin_error(
                    "RVT-504",
                    (
                        f"Time travel is not supported on table '{self._table}' "
                        f"with format '{table_format}'. Only Delta tables support time travel."
                    ),
                    plugin_name="rivet_databricks",
                    plugin_type="source",
                    remediation="Remove 'version' option, or use a Delta-format table.",
                    table=self._table,
                    format=table_format,
                )
            )

    def check_cdf_enabled(self, cdf_enabled: bool) -> None:
        """Raise RVT-505 if change_data_feed is requested but not enabled on the table."""
        if not self._change_data_feed:
            return
        if not cdf_enabled:
            raise ExecutionError(
                plugin_error(
                    "RVT-505",
                    (
                        f"Change Data Feed is not enabled on table '{self._table}'. "
                        "Enable it with ALTER TABLE SET TBLPROPERTIES ('delta.enableChangeDataFeed' = true)."
                    ),
                    plugin_name="rivet_databricks",
                    plugin_type="source",
                    remediation="Enable Change Data Feed on the table or remove 'change_data_feed' option.",
                    table=self._table,
                )
            )

    def to_arrow(self) -> pyarrow.Table:
        raise ExecutionError(
            plugin_error(
                "RVT-501",
                (
                    f"Databricks source '{self._table}' requires the Databricks engine to read data. "
                    "Direct to_arrow() is not supported without engine execution."
                ),
                plugin_name="rivet_databricks",
                plugin_type="source",
                remediation="Configure a Databricks compute engine with a SQL Warehouse.",
                table=self._table,
            )
        )

    @property
    def schema(self) -> Schema:
        raise ExecutionError(
            plugin_error(
                "RVT-501",
                f"Schema for Databricks source '{self._table}' is not available without engine execution.",
                plugin_name="rivet_databricks",
                plugin_type="source",
                remediation="Execute the pipeline through an engine to resolve schema.",
                table=self._table,
            )
        )

    @property
    def row_count(self) -> int:
        raise ExecutionError(
            plugin_error(
                "RVT-501",
                f"Row count for Databricks source '{self._table}' is not available without engine execution.",
                plugin_name="rivet_databricks",
                plugin_type="source",
                remediation="Execute the pipeline through an engine to resolve row count.",
                table=self._table,
            )
        )

    @property
    def size_bytes(self) -> int | None:
        return None

    @property
    def storage_type(self) -> str:
        return "databricks"


class DatabricksSource(SourcePlugin):
    """Source plugin for databricks catalog type.

    Supports time travel (VERSION AS OF, TIMESTAMP AS OF) and Change Data Feed (table_changes).
    """

    catalog_type = "databricks"

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
        version: int | str | None = source_options.get("version")
        change_data_feed: bool = source_options.get("change_data_feed", False)

        sql = build_source_sql(table_name, version=version, change_data_feed=change_data_feed)

        ref = DatabricksDeferredMaterializedRef(
            table=table_name,
            sql=sql,
            version=version,
            change_data_feed=change_data_feed,
        )
        return Material(
            name=joint.name,
            catalog=catalog.name,
            materialized_ref=ref,
            state="deferred",
        )
