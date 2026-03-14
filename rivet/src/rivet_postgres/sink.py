"""PostgreSQL sink plugin: binary COPY for append/truncate_insert, ON CONFLICT for merge, transaction-wrapped."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow as pa

from rivet_core.async_utils import safe_run_async
from rivet_core.errors import ExecutionError, PluginValidationError, plugin_error
from rivet_core.plugins import SinkPlugin

if TYPE_CHECKING:
    from rivet_core.models import Catalog, Joint, Material

SUPPORTED_STRATEGIES = frozenset(
    {
        "append",
        "replace",
        "truncate_insert",
        "merge",
        "delete_insert",
        "incremental_append",
        "scd2",
        "partition",
    }
)

_VALID_ON_CONFLICT_ACTIONS = frozenset({"error", "update", "nothing"})
_STRATEGIES_REQUIRING_CONFLICT_KEY = frozenset({"merge", "delete_insert", "scd2"})
_KNOWN_SINK_OPTIONS = frozenset(
    {
        "table",
        "write_strategy",
        "create_table",
        "batch_size",
        "on_conflict_action",
        "on_conflict_key",
    }
)


def _build_conninfo(options: dict[str, Any]) -> str:
    host = options.get("host", "localhost")
    port = options.get("port", 5432)
    database = options.get("database", "")
    user = options.get("user", "")
    password = options.get("password", "")
    return f"host={host} port={port} dbname={database} user={user} password={password}"


def _get_merge_keys(joint: Any) -> list[str]:
    ws = getattr(joint, "write_strategy_config", None) or {}
    keys = (
        ws.get("merge_key") or ws.get("key_columns") or ws.get("keys") or ws.get("on_conflict_key")
    )
    if keys:
        return keys if isinstance(keys, list) else [keys]
    return []


def _col_names(arrow_table: pa.Table) -> list[str]:
    return [f.name for f in arrow_table.schema]


def _quote(identifier: str) -> str:
    """Quote a PostgreSQL identifier."""
    return f'"{identifier}"'


def _pg_type(arrow_type: pa.DataType) -> str:
    """Map Arrow type to PostgreSQL type for CREATE TABLE."""
    mapping: dict[str, str] = {
        "int8": "SMALLINT",
        "int16": "SMALLINT",
        "int32": "INTEGER",
        "int64": "BIGINT",
        "uint8": "SMALLINT",
        "uint16": "INTEGER",
        "uint32": "BIGINT",
        "uint64": "BIGINT",
        "halffloat": "REAL",
        "float": "REAL",
        "double": "DOUBLE PRECISION",
        "bool": "BOOLEAN",
        "string": "TEXT",
        "large_string": "TEXT",
        "binary": "BYTEA",
        "large_binary": "BYTEA",
    }
    type_str = str(arrow_type)
    if type_str in mapping:
        return mapping[type_str]
    if type_str.startswith("date"):
        return "DATE"
    if type_str.startswith("timestamp"):
        return "TIMESTAMP"
    if type_str.startswith("time"):
        return "TIME"
    if type_str.startswith("decimal"):
        return "NUMERIC"
    if type_str.startswith("duration"):
        return "INTERVAL"
    return "TEXT"


def _create_table_sql(table: str, arrow_schema: pa.Schema) -> str:
    cols = ", ".join(f"{_quote(f.name)} {_pg_type(f.type)}" for f in arrow_schema)
    return f"CREATE TABLE IF NOT EXISTS {table} ({cols})"


class PostgresSink(SinkPlugin):
    """Sink plugin for postgres catalog type.

    Uses binary COPY protocol for append/truncate_insert, ON CONFLICT for merge,
    and wraps all strategies in transactions.
    """

    catalog_type = "postgres"
    supported_strategies = SUPPORTED_STRATEGIES

    def validate_options(self, options: dict[str, Any]) -> None:
        """Validate sink-specific options for PostgreSQL sink.

        Args:
            options: Sink options dict (typically from write_strategy_config).

        Raises:
            PluginValidationError: If options are invalid.
        """
        unknown = set(options) - _KNOWN_SINK_OPTIONS
        if unknown:
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"Unknown PostgreSQL sink options: {', '.join(sorted(unknown))}.",
                    plugin_name="rivet_postgres",
                    plugin_type="sink",
                    remediation=f"Valid options: {', '.join(sorted(_KNOWN_SINK_OPTIONS))}",
                    unknown_options=sorted(unknown),
                )
            )

        if "table" in options and not isinstance(options["table"], str):
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    "PostgreSQL sink option 'table' must be a string.",
                    plugin_name="rivet_postgres",
                    plugin_type="sink",
                    remediation="Provide a string table name.",
                    table=options["table"],
                )
            )

        write_strategy = options.get("write_strategy", "replace")
        if write_strategy not in SUPPORTED_STRATEGIES:
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"Invalid write_strategy '{write_strategy}' for PostgreSQL sink.",
                    plugin_name="rivet_postgres",
                    plugin_type="sink",
                    remediation=f"Supported strategies: {', '.join(sorted(SUPPORTED_STRATEGIES))}",
                    write_strategy=write_strategy,
                )
            )

        if "create_table" in options and not isinstance(options["create_table"], bool):
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    "PostgreSQL sink option 'create_table' must be a boolean.",
                    plugin_name="rivet_postgres",
                    plugin_type="sink",
                    remediation="Set create_table to true or false.",
                    create_table=options["create_table"],
                )
            )

        if "batch_size" in options:
            batch_size = options["batch_size"]
            if not isinstance(batch_size, int) or batch_size <= 0:
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        "PostgreSQL sink option 'batch_size' must be a positive integer.",
                        plugin_name="rivet_postgres",
                        plugin_type="sink",
                        remediation="Set batch_size to a positive integer (default: 10000).",
                        batch_size=batch_size,
                    )
                )

        if "on_conflict_action" in options:
            action = options["on_conflict_action"]
            if action not in _VALID_ON_CONFLICT_ACTIONS:
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"Invalid on_conflict_action '{action}' for PostgreSQL sink.",
                        plugin_name="rivet_postgres",
                        plugin_type="sink",
                        remediation=f"Valid values: {', '.join(sorted(_VALID_ON_CONFLICT_ACTIONS))}",
                        on_conflict_action=action,
                    )
                )

        if write_strategy in _STRATEGIES_REQUIRING_CONFLICT_KEY:
            conflict_key = options.get("on_conflict_key")
            if not conflict_key:
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"PostgreSQL sink option 'on_conflict_key' is required for write_strategy='{write_strategy}'.",
                        plugin_name="rivet_postgres",
                        plugin_type="sink",
                        remediation="Provide on_conflict_key with one or more column names.",
                        write_strategy=write_strategy,
                    )
                )

    def write(self, catalog: Catalog, joint: Joint, material: Material, strategy: str) -> None:
        if strategy not in SUPPORTED_STRATEGIES:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Unsupported write strategy '{strategy}' for PostgreSQL sink.",
                    plugin_name="rivet_postgres",
                    plugin_type="sink",
                    remediation=f"Supported strategies: {', '.join(sorted(SUPPORTED_STRATEGIES))}",
                    strategy=strategy,
                    catalog=catalog.name,
                )
            )

        options = catalog.options
        if options.get("read_only", False):
            raise ExecutionError(
                plugin_error(
                    "RVT-201",
                    f"Cannot write to read-only PostgreSQL catalog '{catalog.name}'.",
                    plugin_name="rivet_postgres",
                    plugin_type="sink",
                    remediation="Set read_only to false or use a different catalog.",
                    catalog=catalog.name,
                )
            )

        conninfo = _build_conninfo(options)
        table_name = joint.table or joint.name
        arrow_table = material.to_arrow()

        try:
            safe_run_async(_execute_strategy(conninfo, table_name, arrow_table, strategy, joint))
        except ExecutionError:
            raise
        except Exception as exc:
            from rivet_postgres.errors import classify_pg_error

            code, message, remediation = classify_pg_error(exc, plugin_type="sink")
            raise ExecutionError(
                plugin_error(
                    code,
                    message,
                    plugin_name="rivet_postgres",
                    plugin_type="sink",
                    remediation=remediation,
                    table=table_name,
                    strategy=strategy,
                )
            ) from exc


async def _execute_strategy(
    conninfo: str, table: str, data: pa.Table, strategy: str, joint: Any
) -> None:
    import psycopg

    async with await psycopg.AsyncConnection.connect(conninfo, autocommit=False) as conn:
        if strategy == "append":
            await _ensure_table(conn, table, data)
            await _binary_copy(conn, table, data)
            await conn.commit()

        elif strategy == "replace":
            async with conn.cursor() as cur:
                await cur.execute(f"DROP TABLE IF EXISTS {table}")
                await cur.execute(_create_table_sql(table, data.schema))
            await _binary_copy(conn, table, data)
            await conn.commit()

        elif strategy == "truncate_insert":
            await _ensure_table(conn, table, data)
            async with conn.cursor() as cur:
                await cur.execute(f"TRUNCATE {table}")
            await _binary_copy(conn, table, data)
            await conn.commit()

        elif strategy == "merge":
            await _do_merge(conn, table, data, joint)

        elif strategy == "delete_insert":
            await _do_delete_insert(conn, table, data, joint)

        elif strategy == "incremental_append":
            await _do_incremental_append(conn, table, data, joint)

        elif strategy == "scd2":
            await _do_scd2(conn, table, data, joint)

        elif strategy == "partition":
            await _do_partition(conn, table, data, joint)


async def _ensure_table(conn: Any, table: str, data: pa.Table) -> None:
    async with conn.cursor() as cur:
        await cur.execute(_create_table_sql(table, data.schema))


async def _binary_copy(conn: Any, table: str, data: pa.Table) -> None:
    """Write data using psycopg3 binary COPY protocol."""
    cols = _col_names(data)
    col_list = ", ".join(_quote(c) for c in cols)
    copy_sql = f"COPY {table} ({col_list}) FROM STDIN (FORMAT BINARY)"

    async with conn.cursor() as cur, cur.copy(copy_sql) as copy:
        copy.set_types([_oid_for_arrow(data.schema.field(c).type) for c in cols])
        for row_idx in range(data.num_rows):
            row = tuple(_py_value(data.column(c)[row_idx].as_py()) for c in cols)
            await copy.write_row(row)


def _py_value(val: Any) -> Any:
    """Ensure value is a plain Python type for psycopg binary copy."""
    return val


def _oid_for_arrow(arrow_type: pa.DataType) -> int:
    """Map Arrow type to PostgreSQL OID for binary COPY type hints."""
    type_str = str(arrow_type)
    oid_map: dict[str, int] = {
        "int8": 21,  # int2
        "int16": 21,  # int2
        "int32": 23,  # int4
        "int64": 20,  # int8
        "uint8": 21,
        "uint16": 23,
        "uint32": 20,
        "uint64": 20,
        "halffloat": 700,  # float4
        "float": 700,  # float4
        "double": 701,  # float8
        "bool": 16,  # bool
        "string": 25,  # text
        "large_string": 25,
        "binary": 17,  # bytea
        "large_binary": 17,
    }
    if type_str in oid_map:
        return oid_map[type_str]
    if type_str.startswith("date"):
        return 1082  # date
    if type_str.startswith("timestamp"):
        return 1114  # timestamp
    return 25  # default to text


async def _do_merge(conn: Any, table: str, data: pa.Table, joint: Any) -> None:
    """Merge using INSERT ... ON CONFLICT ... DO UPDATE."""
    await _ensure_table(conn, table, data)
    keys = _get_merge_keys(joint)
    cols = _col_names(data)

    if not keys:
        # Without keys, fall back to simple append
        await _binary_copy(conn, table, data)
        await conn.commit()
        return

    non_keys = [c for c in cols if c not in keys]
    col_list = ", ".join(_quote(c) for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    key_list = ", ".join(_quote(k) for k in keys)

    if non_keys:
        set_clause = ", ".join(f"{_quote(c)} = EXCLUDED.{_quote(c)}" for c in non_keys)
        conflict = f"ON CONFLICT ({key_list}) DO UPDATE SET {set_clause}"
    else:
        conflict = f"ON CONFLICT ({key_list}) DO NOTHING"

    sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) {conflict}"

    async with conn.cursor() as cur:
        for row_idx in range(data.num_rows):
            row = tuple(data.column(c)[row_idx].as_py() for c in cols)
            await cur.execute(sql, row)
    await conn.commit()


async def _do_delete_insert(conn: Any, table: str, data: pa.Table, joint: Any) -> None:
    await _ensure_table(conn, table, data)
    keys = _get_merge_keys(joint)
    _col_names(data)

    async with conn.cursor() as cur:
        if keys:
            # Collect distinct key values from incoming data
            key_vals = set()
            for row_idx in range(data.num_rows):
                key_vals.add(tuple(data.column(k)[row_idx].as_py() for k in keys))
            if key_vals:
                cond = " AND ".join(f"{_quote(k)} = %s" for k in keys)
                for kv in key_vals:
                    await cur.execute(f"DELETE FROM {table} WHERE {cond}", kv)
        else:
            await cur.execute(f"TRUNCATE {table}")

    await _binary_copy(conn, table, data)
    await conn.commit()


async def _do_incremental_append(conn: Any, table: str, data: pa.Table, joint: Any) -> None:
    await _ensure_table(conn, table, data)
    keys = _get_merge_keys(joint)
    cols = _col_names(data)

    if not keys:
        keys = cols

    col_list = ", ".join(_quote(c) for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    key_list = ", ".join(_quote(k) for k in keys)
    sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) ON CONFLICT ({key_list}) DO NOTHING"

    async with conn.cursor() as cur:
        for row_idx in range(data.num_rows):
            row = tuple(data.column(c)[row_idx].as_py() for c in cols)
            await cur.execute(sql, row)
    await conn.commit()


async def _do_scd2(conn: Any, table: str, data: pa.Table, joint: Any) -> None:
    await _ensure_table_scd2(conn, table, data)
    keys = _get_merge_keys(joint)
    cols = _col_names(data)

    if not keys:
        base_cols = [c for c in cols if c not in ("valid_from", "valid_to", "is_current")]
        keys = base_cols[:1]

    non_keys = [
        c for c in cols if c not in keys and c not in ("valid_from", "valid_to", "is_current")
    ]

    async with conn.cursor() as cur:
        # Close existing current records that have changed
        if non_keys:
            change_cond = " OR ".join(f"{table}.{_quote(c)} IS DISTINCT FROM %s" for c in non_keys)
            key_cond = " AND ".join(f"{table}.{_quote(k)} = %s" for k in keys)
            close_sql = (
                f"UPDATE {table} SET valid_to = NOW(), is_current = false "
                f"WHERE {key_cond} AND is_current = true AND ({change_cond})"
            )
            for row_idx in range(data.num_rows):
                key_vals = [data.column(k)[row_idx].as_py() for k in keys]
                non_key_vals = [data.column(c)[row_idx].as_py() for c in non_keys]
                await cur.execute(close_sql, key_vals + non_key_vals)

        # Insert new versions for changed/new records
        insert_cols = keys + non_keys + ["valid_from", "valid_to", "is_current"]
        col_list = ", ".join(_quote(c) for c in insert_cols)
        key_cond = " AND ".join(f"{table}.{_quote(k)} = %s" for k in keys)
        check_sql = f"SELECT 1 FROM {table} WHERE {key_cond} AND is_current = true"

        for row_idx in range(data.num_rows):
            key_vals = [data.column(k)[row_idx].as_py() for k in keys]
            await cur.execute(check_sql, key_vals)
            exists = await cur.fetchone()
            if not exists:
                non_key_vals = [data.column(c)[row_idx].as_py() for c in non_keys]
                placeholders = ", ".join(["%s"] * len(insert_cols))
                insert_sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
                import datetime

                await cur.execute(
                    insert_sql, key_vals + non_key_vals + [datetime.datetime.now(), None, True]
                )

    await conn.commit()


async def _ensure_table_scd2(conn: Any, table: str, data: pa.Table) -> None:
    """Create table with SCD2 columns if it doesn't exist."""
    async with conn.cursor() as cur:
        # Build columns from data schema
        col_defs = [f"{_quote(f.name)} {_pg_type(f.type)}" for f in data.schema]
        # Add SCD2 columns if not in schema
        existing_names = {f.name for f in data.schema}
        if "valid_from" not in existing_names:
            col_defs.append('"valid_from" TIMESTAMP DEFAULT NOW()')
        if "valid_to" not in existing_names:
            col_defs.append('"valid_to" TIMESTAMP DEFAULT NULL')
        if "is_current" not in existing_names:
            col_defs.append('"is_current" BOOLEAN DEFAULT true')
        create_sql = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(col_defs)})"
        await cur.execute(create_sql)


async def _do_partition(conn: Any, table: str, data: pa.Table, joint: Any) -> None:
    ws = getattr(joint, "write_strategy_config", None) or {}
    partition_cols = ws.get("partition_by") or ws.get("partition_columns")

    if not partition_cols:
        # No partition columns: fall back to replace
        async with conn.cursor() as cur:
            await cur.execute(f"DROP TABLE IF EXISTS {table}")
            await cur.execute(_create_table_sql(table, data.schema))
        await _binary_copy(conn, table, data)
        await conn.commit()
        return

    if isinstance(partition_cols, str):
        partition_cols = [partition_cols]

    await _ensure_table(conn, table, data)

    # Delete existing data for partitions present in staging, then insert
    async with conn.cursor() as cur:
        part_vals = set()
        for row_idx in range(data.num_rows):
            part_vals.add(tuple(data.column(c)[row_idx].as_py() for c in partition_cols))
        if part_vals:
            cond = " AND ".join(f"{_quote(c)} = %s" for c in partition_cols)
            for pv in part_vals:
                await cur.execute(f"DELETE FROM {table} WHERE {cond}", pv)

    await _binary_copy(conn, table, data)
    await conn.commit()
