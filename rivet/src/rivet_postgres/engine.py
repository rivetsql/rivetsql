"""PostgreSQL compute engine plugin."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from typing import Any

import pyarrow as pa

from rivet_core.models import ComputeEngine
from rivet_core.plugins import ComputeEnginePlugin


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

            conninfo = self.config.get("conninfo", "")
            self._pool = AsyncConnectionPool(
                conninfo=conninfo,
                min_size=self.config.get("pool_min_size", 1),
                max_size=self.config.get("pool_max_size", 10),
                open=False,
            )
            await self._pool.open()
        return self._pool

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
        "statement_timeout": None,
        "pool_min_size": 1,
        "pool_max_size": 10,
        "application_name": "rivet",
        "connect_timeout": 30,
        "fetch_batch_size": 10000,
    }
    credential_options: list[str] = []

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return PostgresComputeEngine(name=name, config=config)

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

        import asyncio

        async def _run() -> pa.Table:
            pg_engine: PostgresComputeEngine = engine  # type: ignore[assignment]
            batches: list[pa.RecordBatch] = []
            async for batch in pg_engine.stream_arrow(sql):
                batches.append(batch)
            if batches:
                return pa.Table.from_batches(batches)
            return pa.table({})

        try:
            return asyncio.run(_run())
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
