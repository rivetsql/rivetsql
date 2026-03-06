"""DuckDB sink plugin: write materialized data to DuckDB tables."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rivet_core.errors import ExecutionError, plugin_error
from rivet_core.plugins import SinkPlugin

if TYPE_CHECKING:
    from rivet_core.models import Catalog, Joint, Material

SUPPORTED_STRATEGIES = frozenset(
    {"append", "replace", "truncate_insert", "merge", "delete_insert", "incremental_append", "scd2", "partition"}
)


class DuckDBSink(SinkPlugin):
    """Sink plugin for duckdb catalog type.

    Writes materialized data to the specified DuckDB table using one of 8 strategies.
    """

    catalog_type = "duckdb"
    supported_strategies = SUPPORTED_STRATEGIES

    def write(self, catalog: Catalog, joint: Joint, material: Material, strategy: str) -> None:
        if strategy not in SUPPORTED_STRATEGIES:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Unsupported write strategy '{strategy}' for DuckDB sink.",
                    plugin_name="rivet_duckdb",
                    plugin_type="sink",
                    remediation=f"Supported strategies: {', '.join(sorted(SUPPORTED_STRATEGIES))}",
                    strategy=strategy,
                    catalog=catalog.name,
                )
            )

        import duckdb

        path = catalog.options.get("path", ":memory:")
        read_only = catalog.options.get("read_only", False)
        if read_only:
            raise ExecutionError(
                plugin_error(
                    "RVT-201",
                    f"Cannot write to read-only DuckDB catalog '{catalog.name}'.",
                    plugin_name="rivet_duckdb",
                    plugin_type="sink",
                    remediation="Set read_only to false or use a different catalog.",
                    catalog=catalog.name,
                    path=path,
                )
            )

        table_name = joint.table or joint.name
        arrow_table = material.to_arrow()

        try:
            conn = duckdb.connect(path, read_only=False)
            try:
                _execute_strategy(conn, table_name, arrow_table, strategy, joint)
            finally:
                conn.close()
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"DuckDB sink write failed: {exc}",
                    plugin_name="rivet_duckdb",
                    plugin_type="sink",
                    remediation="Check that the table schema is compatible with the data.",
                    table=table_name,
                    strategy=strategy,
                    path=path,
                )
            ) from exc


def _execute_strategy(
    conn: Any, table: str, data: Any, strategy: str, joint: Any
) -> None:
    """Execute the write strategy against the DuckDB connection."""
    conn.register("__rivet_staging", data)

    if strategy == "append":
        _ensure_table(conn, table, data)
        conn.execute(f"INSERT INTO {table} SELECT * FROM __rivet_staging")

    elif strategy == "replace":
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute(f"CREATE TABLE {table} AS SELECT * FROM __rivet_staging")

    elif strategy == "truncate_insert":
        _ensure_table(conn, table, data)
        conn.execute(f"DELETE FROM {table}")
        conn.execute(f"INSERT INTO {table} SELECT * FROM __rivet_staging")

    elif strategy == "merge":
        _do_merge(conn, table, data, joint)

    elif strategy == "delete_insert":
        _do_delete_insert(conn, table, data, joint)

    elif strategy == "incremental_append":
        _do_incremental_append(conn, table, data, joint)

    elif strategy == "scd2":
        _do_scd2(conn, table, data, joint)

    elif strategy == "partition":
        _do_partition(conn, table, data, joint)

    conn.unregister("__rivet_staging")


def _ensure_table(conn: Any, table: str, data: Any) -> None:
    """Create the table from data schema if it doesn't exist."""
    exists = conn.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE table_name = ?", [table]
    ).fetchone()[0]
    if not exists:
        conn.execute(f"CREATE TABLE {table} AS SELECT * FROM __rivet_staging WHERE false")


def _get_merge_keys(joint: Any) -> list[str]:
    """Extract merge key columns from joint write_strategy config or options."""
    ws = getattr(joint, "write_strategy_config", None) or {}
    keys = ws.get("merge_key") or ws.get("key_columns") or ws.get("keys")
    if keys:
        return keys if isinstance(keys, list) else [keys]
    return []


def _do_merge(conn: Any, table: str, data: Any, joint: Any) -> None:
    """Merge: INSERT OR REPLACE for simple merge, or UPDATE+INSERT with key columns."""
    _ensure_table(conn, table, data)
    keys = _get_merge_keys(joint)
    if not keys:
        # Simple merge: INSERT OR REPLACE
        conn.execute(f"INSERT OR REPLACE INTO {table} SELECT * FROM __rivet_staging")
    else:
        # Transaction-wrapped UPDATE + INSERT
        cols = [f.name for f in data.schema]
        key_cond = " AND ".join(f"{table}.{k} = __rivet_staging.{k}" for k in keys)
        non_keys = [c for c in cols if c not in keys]
        conn.execute("BEGIN TRANSACTION")
        try:
            if non_keys:
                set_clause = ", ".join(f"{c} = __rivet_staging.{c}" for c in non_keys)
                conn.execute(
                    f"UPDATE {table} SET {set_clause} FROM __rivet_staging WHERE {key_cond}"
                )
            conn.execute(
                f"INSERT INTO {table} SELECT s.* FROM __rivet_staging s "
                f"WHERE NOT EXISTS (SELECT 1 FROM {table} t WHERE "
                + " AND ".join(f"t.{k} = s.{k}" for k in keys)
                + ")"
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def _do_delete_insert(conn: Any, table: str, data: Any, joint: Any) -> None:
    """Delete matching rows by key, then insert all staging rows."""
    _ensure_table(conn, table, data)
    keys = _get_merge_keys(joint)
    conn.execute("BEGIN TRANSACTION")
    try:
        if keys:
            key_cond = " AND ".join(
                f"{table}.{k} = __rivet_staging.{k}" for k in keys
            )
            conn.execute(
                f"DELETE FROM {table} USING __rivet_staging WHERE {key_cond}"
            )
        else:
            conn.execute(f"DELETE FROM {table}")
        conn.execute(f"INSERT INTO {table} SELECT * FROM __rivet_staging")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _do_incremental_append(conn: Any, table: str, data: Any, joint: Any) -> None:
    """Append only rows not already present (by key columns or all columns)."""
    _ensure_table(conn, table, data)
    keys = _get_merge_keys(joint)
    if keys:
        cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    else:
        cols = [f.name for f in data.schema]
        cond = " AND ".join(f"t.{c} = s.{c}" for c in cols)
    conn.execute(
        f"INSERT INTO {table} SELECT s.* FROM __rivet_staging s "
        f"WHERE NOT EXISTS (SELECT 1 FROM {table} t WHERE {cond})"
    )


def _do_scd2(conn: Any, table: str, data: Any, joint: Any) -> None:
    """Slowly Changing Dimension Type 2: track history with valid_from/valid_to."""
    _ensure_table_scd2(conn, table, data)
    keys = _get_merge_keys(joint)
    if not keys:
        cols = [f.name for f in data.schema if f.name not in ("valid_from", "valid_to", "is_current")]
        keys = cols[:1]  # fallback to first column

    non_keys = [
        f.name for f in data.schema
        if f.name not in keys and f.name not in ("valid_from", "valid_to", "is_current")
    ]
    key_cond = " AND ".join(f"{table}.{k} = s.{k}" for k in keys)
    change_cond = " OR ".join(f"{table}.{c} IS DISTINCT FROM s.{c}" for c in non_keys) if non_keys else "false"

    conn.execute("BEGIN TRANSACTION")
    try:
        # Close existing current records that have changed
        conn.execute(
            f"UPDATE {table} SET valid_to = CURRENT_TIMESTAMP, is_current = false "
            f"FROM __rivet_staging s WHERE {key_cond} "
            f"AND {table}.is_current = true AND ({change_cond})"
        )
        # Insert new versions for changed records + brand new records
        staging_cols = ", ".join(f"s.{c}" for c in [*keys, *non_keys])
        all_cols = ", ".join([*keys, *non_keys, "valid_from", "valid_to", "is_current"])
        # A row qualifies if it was just closed (changed) or never existed
        conn.execute(
            f"INSERT INTO {table} ({all_cols}) "
            f"SELECT {staging_cols}, CURRENT_TIMESTAMP, NULL, true "
            f"FROM __rivet_staging s "
            f"WHERE NOT EXISTS ("
            f"SELECT 1 FROM {table} WHERE "
            + " AND ".join(f"{table}.{k} = s.{k}" for k in keys)
            + f" AND {table}.is_current = true)"
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _ensure_table_scd2(conn: Any, table: str, data: Any) -> None:
    """Create SCD2 table with valid_from, valid_to, is_current columns if needed."""
    exists = conn.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE table_name = ?", [table]
    ).fetchone()[0]
    if not exists:
        conn.execute(f"CREATE TABLE {table} AS SELECT * FROM __rivet_staging WHERE false")
        # Add SCD2 columns if not present
        existing_cols = {
            row[0] for row in conn.execute(f"SELECT column_name FROM duckdb_columns() WHERE table_name = '{table}'").fetchall()
        }
        if "valid_from" not in existing_cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        if "valid_to" not in existing_cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN valid_to TIMESTAMP DEFAULT NULL")
        if "is_current" not in existing_cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN is_current BOOLEAN DEFAULT true")


def _do_partition(conn: Any, table: str, data: Any, joint: Any) -> None:
    """Partition write: replace data within partition boundaries, append new partitions."""
    ws = getattr(joint, "write_strategy_config", None) or {}
    partition_cols = ws.get("partition_by") or ws.get("partition_columns")
    if not partition_cols:
        # No partition columns: fall back to replace
        conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute(f"CREATE TABLE {table} AS SELECT * FROM __rivet_staging")
        return

    if isinstance(partition_cols, str):
        partition_cols = [partition_cols]

    _ensure_table(conn, table, data)
    # Delete existing data for partitions present in staging
    key_cond = " AND ".join(
        f"{table}.{c} = __rivet_staging.{c}" for c in partition_cols
    )
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(
            f"DELETE FROM {table} USING __rivet_staging WHERE {key_cond}"
        )
        conn.execute(f"INSERT INTO {table} SELECT * FROM __rivet_staging")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
