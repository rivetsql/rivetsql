"""PostgreSQL compute engine plugin."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from typing import Any

import pyarrow as pa

from rivet_core.models import ComputeEngine
from rivet_core.plugins import ComputeEnginePlugin, ReferenceResolver


class PostgresComputeEngine(ComputeEngine):
    """PostgreSQL engine with lazy AsyncConnectionPool lifecycle.

    The pool is created on first call to get_pool() and torn down via teardown().
    """

    def __init__(self, name: str, config: dict[str, Any]) -> None:
        super().__init__(name=name, engine_type="postgres")
        self.config = config
        self._pool: Any = None  # AsyncConnectionPool | None

    async def get_pool(self) -> Any:
        """Lazily create and return the AsyncConnectionPool."""
        if self._pool is None:
            from psycopg_pool import AsyncConnectionPool

            conninfo = self._build_conninfo()
            self._pool = AsyncConnectionPool(
                conninfo=conninfo,
                min_size=self.config.get("pool_min_size", 1),
                max_size=self.config.get("pool_max_size", 10),
                open=False,
            )
            await self._pool.open()
        return self._pool

    def _build_conninfo(self) -> str:
        """Build connection string from individual parameters or use explicit conninfo.

        Accepts either:
        - Individual parameters: host, port, database, user, password
        - Explicit conninfo string

        Returns:
            PostgreSQL connection string.
        """
        # If explicit conninfo is provided, use it directly
        if "conninfo" in self.config:
            conninfo: str = self.config["conninfo"]
            return conninfo

        # Otherwise, build from individual parameters
        host: str = self.config.get("host", "localhost")
        port: int = self.config.get("port", 5432)
        database: str = self.config.get("database", "")
        user: str = self.config.get("user", "")
        password: str = self.config.get("password", "")
        return f"host={host} port={port} dbname={database} user={user} password={password}"

    async def teardown(self) -> None:
        """Close the connection pool if it was created."""
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def stream_arrow(self, sql: str) -> AsyncIterator[pa.RecordBatch]:
        """Stream query results as Arrow RecordBatches via a server-side cursor.

        Uses a named (server-side) cursor so PostgreSQL sends rows in pages,
        avoiding loading the full result set into memory.  Each page is
        converted to a ``pyarrow.RecordBatch`` and yielded.

        Args:
            sql: The SQL query to execute.

        Yields:
            pyarrow.RecordBatch for each non-empty page of results.
        """
        fetch_batch_size: int = self.config.get("fetch_batch_size", 10000)
        pool = await self.get_pool()
        cursor_name = f"rivet_{uuid.uuid4().hex}"

        async with pool.connection() as conn, conn.cursor(name=cursor_name) as cur:
            await cur.execute(sql)
            while True:
                rows = await cur.fetchmany(fetch_batch_size)
                if not rows:
                    break
                col_names = [desc.name for desc in cur.description]
                # Transpose rows (list of dicts or list of tuples) into columns
                if rows and isinstance(rows[0], dict):
                    columns = {name: [row[name] for row in rows] for name in col_names}
                else:
                    columns = {name: [row[i] for row in rows] for i, name in enumerate(col_names)}
                arrays = [pa.array(columns[name]) for name in col_names]
                yield pa.record_batch(arrays, names=col_names)


class PostgresReferenceResolver(ReferenceResolver):
    """Rewrite source joint name references to fully-qualified PostgreSQL table names.

    In a fused group, the SQL joint references upstream source joints by name.
    PostgreSQL needs those replaced with schema.table so the SQL can execute
    server-side against PostgreSQL tables.
    """

    def resolve_references(
        self,
        sql: str,
        joint: Any,
        catalog: Any,
        compiled_joints: dict[str, Any] | None = None,
        catalog_map: dict[str, Any] | None = None,
        fused_group_joints: list[str] | None = None,
    ) -> str | None:
        if not compiled_joints or not catalog_map:
            return None

        import re

        upstream = getattr(joint, "upstream", [])
        if not upstream:
            return None

        # Only joints that actually contribute SQL to the CTE are true CTE aliases.
        # Source joints without SQL don't produce CTE entries, so they must still
        # be resolved to fully-qualified table names for server-side execution.
        cte_siblings: set[str] = set()
        if fused_group_joints and compiled_joints:
            for jn in fused_group_joints:
                cj = compiled_joints.get(jn)
                if cj and (getattr(cj, "sql", None) or getattr(cj, "sql_translated", None)):
                    cte_siblings.add(jn)

        result = sql
        changed = False
        for up_name in upstream:
            # Skip sources that are CTE siblings — they're referenced by alias, not table name.
            if up_name in cte_siblings:
                continue

            up_cj = compiled_joints.get(up_name)
            if not up_cj or getattr(up_cj, "type", None) != "source":
                continue
            up_catalog_name = getattr(up_cj, "catalog", None)
            if not up_catalog_name:
                continue
            cat = catalog_map.get(up_catalog_name)
            if not cat:
                continue

            opts = getattr(cat, "options", {})
            pg_schema = opts.get("schema", "public")

            table = getattr(up_cj, "table", None)
            if table:
                # If table already contains schema (e.g., "myschema.mytable"), use as-is
                if "." in table:
                    qualified_table = table
                else:
                    qualified_table = f"{pg_schema}.{table}"

                # Replace joint name references with qualified table name
                # Use word boundaries to avoid partial matches
                pattern = r"\b" + re.escape(up_name) + r"\b"
                result = re.sub(pattern, qualified_table, result)
                changed = True

        return result if changed else None


class PostgresComputeEnginePlugin(ComputeEnginePlugin):
    engine_type = "postgres"
    dialect = "postgres"
    supported_catalog_types: dict[str, list[str]] = {
        "postgres": [
            "projection_pushdown",
            "predicate_pushdown",
            "limit_pushdown",
            "cast_pushdown",
            "join",
            "aggregation",
        ],
    }
    supported_write_strategies: dict[str, list[str]] = {
        "postgres": [
            "append",
            "replace",
            "truncate_insert",
            "merge",
            "delete_insert",
            "incremental_append",
            "scd2",
            "partition",
        ],
    }
    required_options: list[str] = []
    optional_options: dict[str, Any] = {
        "conninfo": None,
        "host": "localhost",
        "port": 5432,
        "database": "",
        "user": "",
        "password": "",
        "statement_timeout": None,
        "pool_min_size": 1,
        "pool_max_size": 10,
        "application_name": "rivet",
        "connect_timeout": 30,
        "fetch_batch_size": 10000,
    }
    credential_options: list[str] = ["password"]

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return PostgresComputeEngine(name=name, config=config)

    def get_reference_resolver(self) -> ReferenceResolver | None:
        return PostgresReferenceResolver()

    def collect_metrics(self, execution_context: Any) -> Any:
        """Collect PostgreSQL metrics: query_planning, io, memory, cache + pool/copy extensions.

        Args:
            execution_context: Dict optionally containing:
                - timing: PhasedTiming with engine_ms
                - rows_out: int
                - bytes_read: int
                - bytes_written: int
                - peak_bytes: int
                - cache_hits: int
                - cache_misses: int
                - pool_size: int
                - pool_available: int
                - copy_rows: int

        Returns:
            PluginMetrics or None (never raises).
        """
        try:
            return _collect_postgres_metrics(execution_context)
        except Exception:
            return None

    def validate(self, options: dict[str, Any]) -> None:
        from rivet_core.errors import PluginValidationError, plugin_error

        recognized = set(self.optional_options) | set(self.required_options)
        for key in options:
            if key not in recognized:
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"Unknown option '{key}' for postgres engine.",
                        plugin_name="rivet_postgres",
                        plugin_type="engine",
                        remediation=f"Valid options: {', '.join(sorted(recognized))}",
                        option=key,
                    )
                )

        def _fail(option: str, msg: str) -> None:
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"Invalid value for option '{option}' in postgres engine: {msg}",
                    plugin_name="rivet_postgres",
                    plugin_type="engine",
                    remediation=f"Check the expected type for '{option}'.",
                    option=option,
                )
            )

        # Validate connection configuration
        has_conninfo = "conninfo" in options
        has_individual = any(k in options for k in ["host", "database", "user"])

        if has_conninfo and has_individual:
            _fail(
                "conninfo",
                "cannot specify both 'conninfo' and individual connection parameters (host, database, user, password)",
            )

        if "conninfo" in options and not isinstance(options["conninfo"], str):
            _fail("conninfo", "must be a string")

        if "host" in options and not isinstance(options["host"], str):
            _fail("host", "must be a string")

        if "port" in options:
            v = options["port"]
            if not isinstance(v, int) or isinstance(v, bool):
                _fail("port", "must be an integer")
            if v < 1 or v > 65535:
                _fail("port", "must be between 1 and 65535")

        if "database" in options and not isinstance(options["database"], str):
            _fail("database", "must be a string")

        if "user" in options and not isinstance(options["user"], str):
            _fail("user", "must be a string")

        if "password" in options and not isinstance(options["password"], str):
            _fail("password", "must be a string")

        if "statement_timeout" in options and options["statement_timeout"] is not None:
            if not isinstance(options["statement_timeout"], int):
                _fail("statement_timeout", "must be an integer (milliseconds) or None")

        if "pool_min_size" in options:
            v = options["pool_min_size"]
            if not isinstance(v, int) or isinstance(v, bool):
                _fail("pool_min_size", "must be a positive integer")
            if v < 0:
                _fail("pool_min_size", "must be >= 0")

        if "pool_max_size" in options:
            v = options["pool_max_size"]
            if not isinstance(v, int) or isinstance(v, bool):
                _fail("pool_max_size", "must be a positive integer")
            if v < 1:
                _fail("pool_max_size", "must be >= 1")

        if "application_name" in options:
            if not isinstance(options["application_name"], str):
                _fail("application_name", "must be a string")

        if "connect_timeout" in options:
            v = options["connect_timeout"]
            if not isinstance(v, int) or isinstance(v, bool):
                _fail("connect_timeout", "must be a positive integer (seconds)")
            if v < 0:
                _fail("connect_timeout", "must be >= 0")

        if "fetch_batch_size" in options:
            v = options["fetch_batch_size"]
            if not isinstance(v, int) or isinstance(v, bool):
                _fail("fetch_batch_size", "must be a positive integer")
            if v < 1:
                _fail("fetch_batch_size", "must be >= 1")

    def execute_sql(
        self,
        engine: ComputeEngine,
        sql: str,
        input_tables: dict[str, pa.Table],
    ) -> pa.Table:
        """Execute SQL on Postgres via connection pool.

        Rejects non-empty input_tables (Postgres cannot consume local Arrow).
        Wraps connection/query errors in RVT-503.
        """
        from rivet_core.async_utils import safe_run_async
        from rivet_core.errors import ExecutionError, plugin_error

        if input_tables:
            raise ExecutionError(
                plugin_error(
                    "RVT-502",
                    f"Postgres engine cannot consume local Arrow tables. "
                    f"Input tables: {list(input_tables.keys())}",
                    plugin_name="rivet_postgres",
                    plugin_type="engine",
                    remediation="Ensure all upstream data is accessible as Postgres tables, "
                    "or use a different engine for this computation.",
                )
            )

        async def _run() -> pa.Table:
            pg_engine: PostgresComputeEngine = engine  # type: ignore[assignment]
            batches: list[pa.RecordBatch] = []
            async for batch in pg_engine.stream_arrow(sql):
                batches.append(batch)
            if batches:
                return pa.Table.from_batches(batches)
            return pa.table({})

        try:
            return safe_run_async(_run())
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-503",
                    f"Postgres SQL execution failed: {exc}",
                    plugin_name="rivet_postgres",
                    plugin_type="engine",
                    remediation="Check Postgres connectivity and SQL syntax.",
                    sql=sql[:200],
                )
            ) from exc


def _collect_postgres_metrics(execution_context: Any) -> Any:
    """Build PluginMetrics from a PostgreSQL execution context dict."""
    from rivet_core.metrics import (
        CacheMetrics,
        IOMetrics,
        MemoryMetrics,
        PluginMetrics,
        QueryPlanningMetrics,
    )

    ctx: dict[str, Any] = execution_context if isinstance(execution_context, dict) else {}
    timing = ctx.get("timing")

    # --- query_planning ---
    planning_time_ms: float | None = None
    if timing is not None:
        planning_time_ms = getattr(timing, "engine_ms", None)
    actual_rows: int | None = ctx.get("rows_out")

    # --- io ---
    bytes_read: int | None = ctx.get("bytes_read")
    bytes_written: int | None = ctx.get("bytes_written")

    # --- memory ---
    peak_bytes: int | None = ctx.get("peak_bytes")

    # --- cache ---
    cache_hits: int | None = ctx.get("cache_hits")
    cache_misses: int | None = ctx.get("cache_misses")
    hit_ratio: float | None = None
    if cache_hits is not None and cache_misses is not None:
        total = cache_hits + cache_misses
        if total > 0:
            hit_ratio = cache_hits / total

    # --- extensions: pool + copy metrics ---
    extensions: dict[str, Any] = {
        "postgres.pool_size": ctx.get("pool_size"),
        "postgres.pool_available": ctx.get("pool_available"),
        "postgres.copy_rows": ctx.get("copy_rows"),
    }

    return PluginMetrics(
        well_known={
            "query_planning": QueryPlanningMetrics(
                planning_time_ms=planning_time_ms,
                actual_rows=actual_rows,
            ),
            "io": IOMetrics(
                bytes_read=bytes_read,
                bytes_written=bytes_written,
            ),
            "memory": MemoryMetrics(
                peak_bytes=peak_bytes,
            ),
            "cache": CacheMetrics(
                hits=cache_hits,
                misses=cache_misses,
                hit_ratio=hit_ratio,
            ),
        },
        extensions=extensions,
        engine="postgres",
    )
