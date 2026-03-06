"""Unity sink plugin: validate sink options and delegate writes to engine adapters."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pyarrow

from rivet_core.errors import ExecutionError, PluginValidationError, plugin_error
from rivet_core.plugins import SinkPlugin

if TYPE_CHECKING:
    from rivet_core.models import Catalog, Joint, Material

_logger = logging.getLogger(__name__)

SUPPORTED_STRATEGIES = frozenset(
    {"append", "replace", "merge", "truncate_insert", "delete_insert",
     "incremental_append", "scd2", "partition"}
)

_MERGE_KEY_REQUIRED_STRATEGIES = frozenset({"merge", "delete_insert", "scd2"})

_VALID_FORMATS = frozenset({"delta", "parquet", "csv", "json"})

_KNOWN_SINK_OPTIONS = {
    "table", "write_strategy", "merge_key", "partition_by", "format", "create_table",
}


def _validate_sink_options(options: dict[str, Any]) -> None:
    """Validate Unity sink options. Raises PluginValidationError on invalid input."""
    for key in options:
        if key not in _KNOWN_SINK_OPTIONS:
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"Unknown sink option '{key}' for unity sink.",
                    plugin_name="rivet_databricks",
                    plugin_type="sink",
                    remediation=f"Valid options: {', '.join(sorted(_KNOWN_SINK_OPTIONS))}",
                )
            )

    if "table" not in options:
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                "Missing required sink option 'table' for unity sink.",
                plugin_name="rivet_databricks",
                plugin_type="sink",
                remediation="Provide 'table' in the sink options.",
            )
        )

    strategy = options.get("write_strategy", "replace")
    if strategy not in SUPPORTED_STRATEGIES:
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                f"Unknown write strategy '{strategy}' for unity sink.",
                plugin_name="rivet_databricks",
                plugin_type="sink",
                remediation=f"Supported strategies: {', '.join(sorted(SUPPORTED_STRATEGIES))}",
                strategy=strategy,
            )
        )

    if strategy in _MERGE_KEY_REQUIRED_STRATEGIES and not options.get("merge_key"):
        raise PluginValidationError(
            plugin_error(
                "RVT-207",
                f"Missing 'merge_key' for write strategy '{strategy}' in unity sink.",
                plugin_name="rivet_databricks",
                plugin_type="sink",
                remediation="Provide 'merge_key' as a list of column names for merge/delete_insert/scd2.",
                strategy=strategy,
            )
        )

    merge_key = options.get("merge_key")
    if merge_key is not None and not isinstance(merge_key, list):
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                "'merge_key' must be a list of column names.",
                plugin_name="rivet_databricks",
                plugin_type="sink",
                remediation="Provide merge_key as a list, e.g. ['id'].",
            )
        )

    partition_by = options.get("partition_by")
    if partition_by is not None and not isinstance(partition_by, list):
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                "'partition_by' must be a list of column names.",
                plugin_name="rivet_databricks",
                plugin_type="sink",
                remediation="Provide partition_by as a list, e.g. ['year', 'month'].",
            )
        )

    fmt = options.get("format", "delta")
    if fmt not in _VALID_FORMATS:
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                f"Unknown format '{fmt}' for unity sink.",
                plugin_name="rivet_databricks",
                plugin_type="sink",
                remediation=f"Valid formats: {', '.join(sorted(_VALID_FORMATS))}",
                format=fmt,
            )
        )

    create_table = options.get("create_table", True)
    if not isinstance(create_table, bool):
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                "'create_table' must be a boolean.",
                plugin_name="rivet_databricks",
                plugin_type="sink",
                remediation="Provide create_table as true or false.",
            )
        )


# ── Arrow → Unity type mapping ───────────────────────────────────────

_ARROW_TO_UNITY: dict[str, str] = {
    "int8": "TINYINT",
    "int16": "SMALLINT",
    "int32": "INT",
    "int64": "BIGINT",
    "uint8": "SMALLINT",
    "uint16": "INT",
    "uint32": "BIGINT",
    "uint64": "BIGINT",
    "float16": "FLOAT",
    "float32": "FLOAT",
    "float": "FLOAT",
    "float64": "DOUBLE",
    "double": "DOUBLE",
    "bool": "BOOLEAN",
    "string": "STRING",
    "utf8": "STRING",
    "large_string": "STRING",
    "large_utf8": "STRING",
    "binary": "BINARY",
    "large_binary": "BINARY",
    "date32": "DATE",
    "date32[day]": "DATE",
    "date64": "DATE",
    "null": "VOID",
}


def _arrow_type_to_unity(arrow_type: pyarrow.DataType) -> str:
    """Map a PyArrow DataType to a Unity Catalog type_text string."""
    type_str = str(arrow_type)
    if type_str in _ARROW_TO_UNITY:
        return _ARROW_TO_UNITY[type_str]
    if type_str.startswith("timestamp"):
        return "TIMESTAMP"
    if type_str.startswith("decimal"):
        return f"DECIMAL{type_str[len('decimal128'):]}" if "decimal128" in type_str else f"DECIMAL{type_str[len('decimal'):]}"
    if type_str.startswith("duration"):
        return "INTERVAL"
    return "STRING"


def _build_table_def(
    table_name: str,
    catalog_name: str,
    schema_name: str,
    arrow_schema: pyarrow.Schema,
    data_source_format: str,
    partition_by: list[str] | None = None,
) -> dict[str, Any]:
    """Build a Unity Catalog table definition dict from an Arrow schema."""
    columns = []
    for i, field in enumerate(arrow_schema):
        col: dict[str, Any] = {
            "name": field.name,
            "type_text": _arrow_type_to_unity(field.type),
            "type_name": _arrow_type_to_unity(field.type),
            "position": i,
            "nullable": field.nullable,
        }
        columns.append(col)

    table_def: dict[str, Any] = {
        "name": table_name,
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "table_type": "MANAGED",
        "data_source_format": data_source_format.upper(),
        "columns": columns,
    }
    if partition_by:
        table_def["partition_columns"] = partition_by
    return table_def


def _ensure_table_exists(
    catalog: Catalog,
    full_table_name: str,
    material: Material,
    sink_options: dict[str, Any],
) -> None:
    """Create the table via POST /tables if it does not exist.

    Args:
        catalog: The Unity catalog instance with host and credential options.
        full_table_name: Three-part table name (catalog.schema.table).
        material: The Material whose Arrow schema defines the table columns.
        sink_options: Sink options including format and partition_by.
    """
    from rivet_databricks.auth import resolve_credentials
    from rivet_databricks.client import UnityCatalogClient

    host = catalog.options["host"]
    credential = resolve_credentials(catalog.options, host=host)
    client = UnityCatalogClient(host=host, credential=credential)
    try:
        try:
            client.get_table(full_table_name)
            return  # Table exists, nothing to do
        except ExecutionError as exc:
            if exc.error.code != "RVT-503":
                raise

        parts = full_table_name.split(".")
        if len(parts) != 3:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Invalid table name '{full_table_name}': expected catalog.schema.table format.",
                    plugin_name="rivet_databricks",
                    plugin_type="sink",
                    remediation="Use a three-part table name: catalog.schema.table.",
                    table=full_table_name,
                )
            )
        cat_name, schema_name, table_name = parts

        arrow_table = material.to_arrow()
        fmt = sink_options.get("format", "delta")
        partition_by = sink_options.get("partition_by")

        table_def = _build_table_def(
            table_name=table_name,
            catalog_name=cat_name,
            schema_name=schema_name,
            arrow_schema=arrow_table.schema,
            data_source_format=fmt,
            partition_by=partition_by,
        )
        client.create_table(table_def)
        _logger.info("Created Unity Catalog table '%s'.", full_table_name)
    finally:
        client.close()


class UnitySink(SinkPlugin):
    """Sink plugin for unity catalog type.

    Validates sink options and delegates writes to the resolved engine adapter.
    Supports: table (required), write_strategy (default 'replace'), merge_key,
    partition_by, format (default 'delta'), create_table (default true).
    """

    catalog_type = "unity"
    supported_strategies = SUPPORTED_STRATEGIES

    def write(self, catalog: Catalog, joint: Joint, material: Material, strategy: str) -> None:
        sink_options: dict[str, Any] = {}
        if joint.table:
            sink_options["table"] = joint.table
        if hasattr(joint, "sink_options") and joint.sink_options:
            sink_options.update(joint.sink_options)
        if "table" not in sink_options and joint.name:
            sink_options["table"] = joint.name
        if strategy:
            sink_options.setdefault("write_strategy", strategy)

        _validate_sink_options(sink_options)

        if sink_options.get("create_table", True):
            table_name = sink_options["table"]
            if table_name.count(".") < 2:
                cat_name = catalog.options.get("catalog_name", "")
                schema_name = catalog.options.get("schema", "default")
                full_name = f"{cat_name}.{schema_name}.{table_name}"
            else:
                full_name = table_name
            _ensure_table_exists(catalog, full_name, material, sink_options)

        raise ExecutionError(
            plugin_error(
                "RVT-501",
                (
                    f"Unity sink '{sink_options['table']}' requires an engine adapter to write data. "
                    "Direct write is not supported without an adapter dispatch."
                ),
                plugin_name="rivet_databricks",
                plugin_type="sink",
                remediation="Configure a compute engine with a unity adapter (e.g. DuckDB with UnityDuckDBAdapter).",
                table=sink_options["table"],
            )
        )
