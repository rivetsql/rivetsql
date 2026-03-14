"""PostgresDuckDBAdapter: catalog-plugin-contributed adapter for DuckDB ← postgres.

Registers with target_engine="duckdb", catalog_type="postgres", source_plugin="rivet_postgres".
This adapter takes priority over any engine-plugin adapter for the same pair per Core adapter
precedence (catalog_plugin > engine_plugin).

Uses DuckDB's postgres community extension with ATTACH to establish direct
PostgreSQL access from DuckDB queries.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow

from rivet_core.errors import ExecutionError, RivetError, plugin_error
from rivet_core.models import Column, Material, Schema
from rivet_core.optimizer import AdapterPushdownResult, Cast, PushdownPlan, ResidualPlan
from rivet_core.plugins import ComputeEngineAdapter
from rivet_core.strategies import MaterializedRef

if TYPE_CHECKING:
    import duckdb

    from rivet_core.sql_parser import Predicate

_EMPTY_RESIDUAL = ResidualPlan(predicates=[], limit=None, casts=[])


def _apply_pushdown(
    base_sql: str,
    pushdown: PushdownPlan | None,
) -> tuple[str, ResidualPlan]:
    """Apply pushdown operations to a SQL query string (DuckDB dialect)."""
    if pushdown is None:
        return base_sql, _EMPTY_RESIDUAL

    sql = base_sql
    residual_predicates: list[Predicate] = list(pushdown.predicates.residual)
    residual_casts: list[Cast] = list(pushdown.casts.residual)
    residual_limit: int | None = pushdown.limit.residual_limit

    if pushdown.projections.pushed_columns is not None:
        try:
            cols = ", ".join(pushdown.projections.pushed_columns)
            sql = sql.replace("SELECT *", f"SELECT {cols}", 1)
        except Exception:
            pass

    if pushdown.predicates.pushed:
        where_parts: list[str] = []
        for pred in pushdown.predicates.pushed:
            try:
                where_parts.append(pred.expression)
            except Exception:
                residual_predicates.append(pred)
        if where_parts:
            where_clause = " AND ".join(where_parts)
            sql = f"SELECT * FROM ({sql}) AS __pd WHERE {where_clause}"

    if pushdown.limit.pushed_limit is not None:
        try:
            sql = f"{sql} LIMIT {pushdown.limit.pushed_limit}"
        except Exception:
            residual_limit = pushdown.limit.pushed_limit

    for cast in pushdown.casts.pushed:
        try:
            sql = sql.replace(cast.column, f"CAST({cast.column} AS {cast.to_type})")
        except Exception:
            residual_casts.append(cast)

    return sql, ResidualPlan(
        predicates=residual_predicates, limit=residual_limit, casts=residual_casts
    )


def _ensure_duckdb_extension(
    conn: duckdb.DuckDBPyConnection, ext: str, install_from: str | None = None
) -> None:
    """Check-then-load a DuckDB extension."""
    try:
        row = conn.execute(
            "SELECT installed, loaded FROM duckdb_extensions() WHERE extension_name = ?",
            [ext],
        ).fetchone()
        installed = row[0] if row is not None else False
        loaded = row[1] if row is not None else False
        if loaded:
            return
        if not installed:
            install_sql = f"INSTALL {ext} FROM {install_from}" if install_from else f"INSTALL {ext}"
            conn.execute(install_sql)
        conn.execute(f"LOAD {ext}")
    except ExecutionError:
        raise
    except Exception as exc:
        raise ExecutionError(
            plugin_error(
                "RVT-502",
                f"Failed to load DuckDB extension '{ext}': {exc}",
                plugin_name="rivet_postgres",
                plugin_type="adapter",
                remediation=(
                    f"Run: INSTALL {ext}; LOAD {ext}; "
                    "or set the DuckDB extension directory for offline environments."
                ),
                extension=ext,
            )
        ) from exc


def _build_dsn(catalog_options: dict[str, Any]) -> str:
    """Build a PostgreSQL connection string from catalog options."""
    host = catalog_options["host"]
    port = catalog_options.get("port", 5432)
    database = catalog_options["database"]
    user = catalog_options.get("user", "")
    password = catalog_options.get("password", "")
    parts = [f"host={host}", f"port={port}", f"dbname={database}"]
    if user:
        parts.append(f"user={user}")
    if password:
        parts.append(f"password={password}")
    return " ".join(parts)


def _attach_alias(catalog_name: str) -> str:
    """Derive a DuckDB ATTACH alias from the catalog name."""
    return f"pg_{catalog_name}"


class _PostgresDuckDBMaterializedRef(MaterializedRef):
    """Deferred ref that reads PostgreSQL data via DuckDB postgres extension on to_arrow()."""

    def __init__(
        self, catalog_options: dict[str, Any], catalog_name: str, sql: str | None, table: str | None
    ) -> None:
        self._catalog_options = catalog_options
        self._catalog_name = catalog_name
        self._sql = sql
        self._table = table
        self._cached: pyarrow.Table | None = None

    def _execute(self) -> pyarrow.Table:
        if self._cached is not None:
            return self._cached
        import duckdb

        conn = duckdb.connect(":memory:")
        try:
            _ensure_duckdb_extension(conn, "postgres")
            dsn = _build_dsn(self._catalog_options)
            alias = _attach_alias(self._catalog_name)
            conn.execute(f"ATTACH '{dsn}' AS {alias} (TYPE postgres, READ_ONLY)")

            if self._sql:
                arrow_result = conn.execute(self._sql).arrow()
            else:
                pg_schema = self._catalog_options.get("schema", "public")
                table_ref = f"{alias}.{pg_schema}.{self._table}"
                arrow_result = conn.execute(f"SELECT * FROM {table_ref}").arrow()

            # DuckDB .arrow() can return either Table or RecordBatchReader
            # Convert to Table if needed
            if isinstance(arrow_result, pyarrow.RecordBatchReader):
                result = arrow_result.read_all()
            else:
                result = arrow_result

            self._cached = result
            return result
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                RivetError(
                    code="RVT-504",
                    message=f"DuckDB postgres extension failed: {exc}",
                    context={
                        "host": self._catalog_options.get("host"),
                        "database": self._catalog_options.get("database"),
                    },
                    remediation="Check PostgreSQL connectivity, credentials, and that the DuckDB postgres extension is available.",
                )
            ) from exc
        finally:
            conn.close()

    def to_arrow(self) -> pyarrow.Table:
        return self._execute()

    @property
    def schema(self) -> Schema:
        table = self._execute()
        return Schema(
            columns=[
                Column(name=f.name, type=str(f.type), nullable=f.nullable) for f in table.schema
            ]
        )

    @property
    def row_count(self) -> int:
        return self._execute().num_rows  # type: ignore[no-any-return]

    @property
    def size_bytes(self) -> int | None:
        return None

    @property
    def storage_type(self) -> str:
        return "postgres"


class PostgresDuckDBAdapter(ComputeEngineAdapter):
    """DuckDB adapter for PostgreSQL catalog type, shipped by rivet_postgres.

    Uses DuckDB's postgres community extension with ATTACH for direct
    PostgreSQL access from DuckDB queries.
    """

    target_engine_type = "duckdb"
    catalog_type = "postgres"
    capabilities: list[str] = [
        "projection_pushdown",
        "predicate_pushdown",
        "limit_pushdown",
        "cast_pushdown",
        "join",
        "aggregation",
    ]
    source = "catalog_plugin"
    source_plugin = "rivet_postgres"

    def read_dispatch(
        self, engine: Any, catalog: Any, joint: Any, pushdown: PushdownPlan | None = None
    ) -> AdapterPushdownResult:
        if joint.sql:
            base_sql = joint.sql
        else:
            pg_schema = catalog.options.get("schema", "public")
            alias = _attach_alias(catalog.name)
            base_sql = f"SELECT * FROM {alias}.{pg_schema}.{joint.table}"

        sql, residual = _apply_pushdown(base_sql, pushdown)

        material = Material(
            name=joint.name,
            catalog=catalog.name,
            materialized_ref=_PostgresDuckDBMaterializedRef(
                catalog_options=catalog.options,
                catalog_name=catalog.name,
                sql=sql,
                table=joint.table,
            ),
            state="deferred",
        )
        return AdapterPushdownResult(material=material, residual=residual)

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        import duckdb

        conn = duckdb.connect(":memory:")
        try:
            _ensure_duckdb_extension(conn, "postgres")
            dsn = _build_dsn(catalog.options)
            alias = _attach_alias(catalog.name)
            conn.execute(f"ATTACH '{dsn}' AS {alias} (TYPE postgres)")

            arrow_table = material.to_arrow()
            conn.register("__write_data", arrow_table)

            pg_schema = catalog.options.get("schema", "public")
            table_name = joint.table
            table_ref = f"{alias}.{pg_schema}.{table_name}"
            strategy = joint.write_strategy or "replace"

            if strategy == "replace":
                conn.execute(f"DROP TABLE IF EXISTS {table_ref}")
                conn.execute(f"CREATE TABLE {table_ref} AS SELECT * FROM __write_data")
            elif strategy == "append":
                conn.execute(f"INSERT INTO {table_ref} SELECT * FROM __write_data")
            elif strategy == "truncate_insert":
                conn.execute(f"DELETE FROM {table_ref}")
                conn.execute(f"INSERT INTO {table_ref} SELECT * FROM __write_data")
            else:
                conn.execute(f"INSERT INTO {table_ref} SELECT * FROM __write_data")
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                RivetError(
                    code="RVT-504",
                    message=f"DuckDB postgres extension write failed: {exc}",
                    context={
                        "host": catalog.options.get("host"),
                        "database": catalog.options.get("database"),
                    },
                    remediation="Check PostgreSQL connectivity, credentials, and write permissions.",
                )
            ) from exc
        finally:
            conn.close()
