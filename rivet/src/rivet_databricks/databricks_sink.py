"""Databricks sink plugin: write to Databricks Unity Catalog tables via SQL Warehouse.

Supports all 8 write strategies, merge_key validation, format validation,
optimize_after_write, and liquid_clustering.
"""

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
    {"append", "replace", "truncate_insert", "merge", "delete_insert",
     "incremental_append", "scd2", "partition"}
)

_MERGE_KEY_REQUIRED_STRATEGIES = frozenset({"merge", "delete_insert", "scd2"})

_VALID_FORMATS = frozenset({"delta", "parquet"})

_KNOWN_SINK_OPTIONS = {
    "table", "write_strategy", "merge_key", "partition_by", "format",
    "create_table", "optimize_after_write", "liquid_clustering",
}


def _validate_sink_options(options: dict[str, Any]) -> None:
    """Validate Databricks sink options. Raises PluginValidationError on invalid input."""
    for key in options:
        if key not in _KNOWN_SINK_OPTIONS:
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"Unknown sink option '{key}' for databricks sink.",
                    plugin_name="rivet_databricks",
                    plugin_type="sink",
                    remediation=f"Valid options: {', '.join(sorted(_KNOWN_SINK_OPTIONS))}",
                )
            )

    if "table" not in options:
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                "Missing required sink option 'table' for databricks sink.",
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
                f"Unknown write strategy '{strategy}' for databricks sink.",
                plugin_name="rivet_databricks",
                plugin_type="sink",
                remediation=f"Supported strategies: {', '.join(sorted(SUPPORTED_STRATEGIES))}",
                strategy=strategy,
            )
        )

    # RVT-207: merge_key required for merge/delete_insert/scd2
    if strategy in _MERGE_KEY_REQUIRED_STRATEGIES and not options.get("merge_key"):
        raise PluginValidationError(
            plugin_error(
                "RVT-207",
                f"Missing 'merge_key' for write strategy '{strategy}' in databricks sink.",
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

    fmt = options.get("format", "delta")
    if fmt not in _VALID_FORMATS:
        raise PluginValidationError(
            plugin_error(
                "RVT-206",
                f"Invalid table format '{fmt}' for databricks sink. Only delta and parquet are supported.",
                plugin_name="rivet_databricks",
                plugin_type="sink",
                remediation=f"Valid formats: {', '.join(sorted(_VALID_FORMATS))}",
                format=fmt,
            )
        )

    # RVT-208: liquid_clustering on parquet
    liquid_clustering = options.get("liquid_clustering")
    if liquid_clustering is not None and fmt == "parquet":
        raise PluginValidationError(
            plugin_error(
                "RVT-208",
                "liquid_clustering is not supported on Parquet tables.",
                plugin_name="rivet_databricks",
                plugin_type="sink",
                remediation="Use format 'delta' for liquid clustering, or remove the liquid_clustering option.",
                format=fmt,
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

    optimize_after_write = options.get("optimize_after_write", False)
    if not isinstance(optimize_after_write, bool):
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                "'optimize_after_write' must be a boolean.",
                plugin_name="rivet_databricks",
                plugin_type="sink",
                remediation="Provide optimize_after_write as true or false.",
            )
        )


# ── SQL generation helpers ────────────────────────────────────────────

def _quote(name: str) -> str:
    """Quote an identifier with backticks for Databricks SQL."""
    return f"`{name}`"


def _col_list(columns: list[str]) -> str:
    return ", ".join(_quote(c) for c in columns)


def _staging_table_name(table: str) -> str:
    """Generate a temporary staging table name."""
    safe = table.replace(".", "_").replace("`", "")
    return f"__rivet_staging_{safe}"


def _arrow_type_to_databricks(arrow_type: pyarrow.DataType) -> str:
    """Map a PyArrow DataType to a Databricks SQL type string."""
    t = str(arrow_type)
    mapping: dict[str, str] = {
        "int8": "TINYINT", "int16": "SMALLINT", "int32": "INT", "int64": "BIGINT",
        "uint8": "SMALLINT", "uint16": "INT", "uint32": "BIGINT", "uint64": "BIGINT",
        "float16": "FLOAT", "float32": "FLOAT", "float": "FLOAT",
        "float64": "DOUBLE", "double": "DOUBLE",
        "bool": "BOOLEAN", "string": "STRING", "utf8": "STRING",
        "large_string": "STRING", "large_utf8": "STRING",
        "binary": "BINARY", "large_binary": "BINARY",
        "date32": "DATE", "date32[day]": "DATE", "date64": "DATE",
        "null": "VOID",
    }
    if t in mapping:
        return mapping[t]
    if t.startswith("timestamp"):
        return "TIMESTAMP"
    if t.startswith("decimal"):
        return t.upper().replace("DECIMAL128", "DECIMAL")
    return "STRING"


def _create_table_sql(
    table: str,
    schema: pyarrow.Schema,
    fmt: str,
    partition_by: list[str] | None,
    liquid_clustering: list[str] | None,
) -> str:
    """Generate CREATE TABLE IF NOT EXISTS SQL."""
    cols = ", ".join(
        f"{_quote(f.name)} {_arrow_type_to_databricks(f.type)}"
        for f in schema
    )
    sql = f"CREATE TABLE IF NOT EXISTS {table} ({cols}) USING {fmt.upper()}"
    if partition_by:
        sql += f" PARTITIONED BY ({_col_list(partition_by)})"
    if liquid_clustering:
        sql += f" CLUSTER BY ({_col_list(liquid_clustering)})"
    return sql


def _build_values_sql(table: pyarrow.Table) -> str:
    """Build a VALUES clause from an Arrow table for staging data."""
    rows = []
    for i in range(table.num_rows):
        vals = []
        for col_idx in range(table.num_columns):
            v = table.column(col_idx)[i].as_py()
            if v is None:
                vals.append("NULL")
            elif isinstance(v, bool):
                vals.append("TRUE" if v else "FALSE")
            elif isinstance(v, (int, float)):
                vals.append(str(v))
            elif isinstance(v, bytes):
                vals.append(f"X'{v.hex()}'")
            else:
                escaped = str(v).replace("'", "''")
                vals.append(f"'{escaped}'")
        rows.append(f"({', '.join(vals)})")
    return ", ".join(rows)


def _generate_write_sql(
    table: str,
    staging: str,
    strategy: str,
    columns: list[str],
    merge_key: list[str] | None,
) -> list[str]:
    """Generate SQL statements for the given write strategy.

    Returns a list of SQL strings to execute in order.
    """
    if strategy == "append":
        return [f"INSERT INTO {table} SELECT * FROM {staging}"]

    if strategy == "replace":
        return [
            f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM {staging}",
        ]

    if strategy == "truncate_insert":
        return [
            f"TRUNCATE TABLE {table}",
            f"INSERT INTO {table} SELECT * FROM {staging}",
        ]

    if strategy == "merge":
        keys = merge_key or []
        on_clause = " AND ".join(f"t.{_quote(k)} = s.{_quote(k)}" for k in keys)
        non_keys = [c for c in columns if c not in keys]
        update_clause = ", ".join(f"t.{_quote(c)} = s.{_quote(c)}" for c in non_keys)
        insert_cols = _col_list(columns)
        insert_vals = ", ".join(f"s.{_quote(c)}" for c in columns)
        sql = f"MERGE INTO {table} AS t USING {staging} AS s ON {on_clause}"
        if update_clause:
            sql += f" WHEN MATCHED THEN UPDATE SET {update_clause}"
        sql += f" WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        return [sql]

    if strategy == "delete_insert":
        keys = merge_key or []
        where_clause = " AND ".join(
            f"{table}.{_quote(k)} IN (SELECT {_quote(k)} FROM {staging})" for k in keys
        )
        return [
            f"DELETE FROM {table} WHERE {where_clause}",
            f"INSERT INTO {table} SELECT * FROM {staging}",
        ]

    if strategy == "incremental_append":
        keys = merge_key or columns
        on_clause = " AND ".join(f"t.{_quote(k)} = s.{_quote(k)}" for k in keys)
        insert_cols = _col_list(columns)
        insert_vals = ", ".join(f"s.{_quote(c)}" for c in columns)
        return [
            f"MERGE INTO {table} AS t USING {staging} AS s ON {on_clause}"
            f" WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})",
        ]

    if strategy == "scd2":
        keys = merge_key or []
        non_keys = [c for c in columns if c not in keys
                    and c not in ("valid_from", "valid_to", "is_current")]
        on_clause = " AND ".join(f"t.{_quote(k)} = s.{_quote(k)}" for k in keys)
        change_cond = " OR ".join(
            f"t.{_quote(c)} IS DISTINCT FROM s.{_quote(c)}" for c in non_keys
        ) if non_keys else "FALSE"
        data_cols = [c for c in columns if c not in ("valid_from", "valid_to", "is_current")]
        insert_cols = _col_list(data_cols + ["valid_from", "valid_to", "is_current"])
        insert_vals = ", ".join(f"s.{_quote(c)}" for c in data_cols) + ", current_timestamp(), NULL, TRUE"
        return [
            (
                f"MERGE INTO {table} AS t USING {staging} AS s ON {on_clause} AND t.`is_current` = TRUE"
                f" WHEN MATCHED AND ({change_cond}) THEN UPDATE SET"
                f" t.`valid_to` = current_timestamp(), t.`is_current` = FALSE"
                f" WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
            ),
        ]

    if strategy == "partition":
        return [
            f"INSERT OVERWRITE {table} SELECT * FROM {staging}",
        ]

    return []


class DatabricksSink(SinkPlugin):
    """Sink plugin for databricks catalog type.

    Writes materialized data via the DatabricksEngine SQL Warehouse.
    Supports all 8 write strategies, merge_key validation, format validation,
    optimize_after_write, and liquid_clustering.
    """

    catalog_type = "databricks"
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

        table_name = sink_options["table"]
        effective_strategy = sink_options.get("write_strategy", "replace")
        merge_key: list[str] | None = sink_options.get("merge_key")
        partition_by: list[str] | None = sink_options.get("partition_by")
        fmt = sink_options.get("format", "delta")
        create_table = sink_options.get("create_table", True)
        optimize_after_write = sink_options.get("optimize_after_write", False)
        liquid_clustering: list[str] | None = sink_options.get("liquid_clustering")

        # Resolve full table name
        if table_name.count(".") < 2:
            cat_name = catalog.options.get("catalog", "")
            schema_name = catalog.options.get("schema", "default")
            full_table = f"{cat_name}.{schema_name}.{table_name}"
        else:
            full_table = table_name

        # Get Arrow data and build statement API
        arrow_table = material.to_arrow()
        columns = [f.name for f in arrow_table.schema]

        from rivet_databricks.auth import resolve_credentials
        from rivet_databricks.engine import DatabricksStatementAPI

        workspace_url = catalog.options["workspace_url"]
        credential = resolve_credentials(catalog.options, host=workspace_url)
        token = credential.token or ""

        engine_options = catalog.options.get("_engine_options", {})
        warehouse_id = engine_options.get("warehouse_id") or catalog.options.get("warehouse_id", "")
        if not warehouse_id:
            raise ExecutionError(
                plugin_error(
                    "RVT-204",
                    "warehouse_id is required for Databricks sink writes.",
                    plugin_name="rivet_databricks",
                    plugin_type="sink",
                    remediation="Provide warehouse_id in the engine options.",
                    workspace_url=workspace_url,
                )
            )

        api = DatabricksStatementAPI(
            workspace_url=workspace_url,
            token=token,
            warehouse_id=warehouse_id,
            wait_timeout=engine_options.get("wait_timeout", "30s"),
        )

        catalog_name = catalog.options.get("catalog")
        schema_name = catalog.options.get("schema", "default")

        try:
            # Create table if needed
            if create_table:
                create_sql = _create_table_sql(
                    full_table, arrow_table.schema, fmt, partition_by, liquid_clustering,
                )
                api.execute(create_sql, catalog=catalog_name, schema=schema_name)

            # Stage data via temporary view
            staging = _staging_table_name(table_name)
            if arrow_table.num_rows > 0:
                values_sql = _build_values_sql(arrow_table)
                col_defs = ", ".join(
                    f"{_quote(f.name)} {_arrow_type_to_databricks(f.type)}"
                    for f in arrow_table.schema
                )
                stage_sql = (
                    f"CREATE OR REPLACE TEMPORARY VIEW {staging} ({col_defs})"
                    f" AS SELECT * FROM VALUES {values_sql}"
                )
                api.execute(stage_sql, catalog=catalog_name, schema=schema_name)
            else:
                # Empty data: create empty temp view
                col_defs = ", ".join(
                    f"CAST(NULL AS {_arrow_type_to_databricks(f.type)}) AS {_quote(f.name)}"
                    for f in arrow_table.schema
                )
                stage_sql = (
                    f"CREATE OR REPLACE TEMPORARY VIEW {staging}"
                    f" AS SELECT {col_defs} WHERE FALSE"
                )
                api.execute(stage_sql, catalog=catalog_name, schema=schema_name)

            # Execute write strategy
            stmts = _generate_write_sql(
                full_table, staging, effective_strategy, columns, merge_key,
            )
            for stmt in stmts:
                api.execute(stmt, catalog=catalog_name, schema=schema_name)

            # Optimize after write
            if optimize_after_write and fmt == "delta":
                api.execute(f"OPTIMIZE {full_table}", catalog=catalog_name, schema=schema_name)

            _logger.info(
                "Databricks sink wrote to '%s' with strategy '%s'.",
                full_table, effective_strategy,
            )
        finally:
            api.close()
