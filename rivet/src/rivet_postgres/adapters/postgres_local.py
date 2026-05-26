"""Postgres local adapter: native SQL write for Postgres engine → Postgres catalog.

When the compute engine and sink/checkpoint catalog are both PostgreSQL (same
server), the fused SQL can be embedded directly into the write DDL (e.g.
``CREATE TABLE ... AS <fused_sql>``), eliminating the Arrow round-trip through
``SinkPlugin.write()``.

For reads, Postgres→Postgres uses engine-native SQL — no adapter dispatch needed.
This adapter exists primarily for ``write_dispatch`` with native SQL write.
"""

from __future__ import annotations

import logging
from typing import Any

from rivet_core.async_utils import safe_run_async
from rivet_core.errors import ExecutionError, plugin_error
from rivet_core.optimizer import AdapterPushdownResult, PushdownPlan
from rivet_core.plugins import ComputeEngineAdapter, NativeSqlWriteContext

_logger = logging.getLogger(__name__)

_NATIVE_WRITE_STRATEGIES = frozenset({"replace", "append", "truncate_insert"})


class PostgresLocalAdapter(ComputeEngineAdapter):
    """Adapter for Postgres engine writing to Postgres catalog via native SQL."""

    target_engine_type = "postgres"
    catalog_type = "postgres"
    capabilities: list[str] = ["native_sql_write"]
    source = "engine_plugin"
    source_plugin = "rivet_postgres"

    def supports_native_sql_write(self, write_strategy: str) -> bool:
        return write_strategy in _NATIVE_WRITE_STRATEGIES

    def read_dispatch(
        self, engine: Any, catalog: Any, joint: Any, pushdown: PushdownPlan | None = None
    ) -> AdapterPushdownResult:
        raise ExecutionError(
            plugin_error(
                "RVT-501",
                "Postgres local reads use engine-native SQL, not adapter dispatch.",
                plugin_name="rivet_postgres",
                plugin_type="adapter",
                remediation="This adapter only supports write_dispatch. Reads should go through the engine directly.",
            )
        )

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        if isinstance(material, NativeSqlWriteContext):
            return self._native_sql_write(material)
        return self._arrow_write(engine, catalog, joint, material)

    def _native_sql_write(self, ctx: NativeSqlWriteContext) -> None:
        safe_run_async(self._native_sql_write_async(ctx))

    async def _native_sql_write_async(self, ctx: NativeSqlWriteContext) -> None:
        import psycopg

        conninfo = _build_conninfo(ctx.catalog.options)
        target = ctx.target_table
        sql = ctx.fused_sql
        schema = ctx.catalog.options.get("schema", "public")
        qualified = f"{schema}.{target}" if "." not in target else target

        try:
            async with await psycopg.AsyncConnection.connect(conninfo, autocommit=False) as conn:
                async with conn.cursor() as cur:
                    if ctx.write_strategy == "replace":
                        await cur.execute(f"DROP TABLE IF EXISTS {qualified}")
                        await cur.execute(f"CREATE TABLE {qualified} AS {sql}")
                    elif ctx.write_strategy == "append":
                        await _ensure_table_from_sql(cur, qualified, sql)
                        await cur.execute(f"INSERT INTO {qualified} {sql}")
                    elif ctx.write_strategy == "truncate_insert":
                        await _ensure_table_from_sql(cur, qualified, sql)
                        await cur.execute(f"TRUNCATE {qualified}")
                        await cur.execute(f"INSERT INTO {qualified} {sql}")
                await conn.commit()

            _logger.debug(
                "native_sql_write: %s strategy=%s target=%s",
                ctx.catalog.name,
                ctx.write_strategy,
                qualified,
            )
        except ExecutionError:
            raise
        except Exception as exc:
            from rivet_postgres.errors import classify_pg_error

            code, message, remediation = classify_pg_error(exc, plugin_type="adapter")
            raise ExecutionError(
                plugin_error(
                    code,
                    f"Postgres native SQL write failed: {message}",
                    plugin_name="rivet_postgres",
                    plugin_type="adapter",
                    adapter="PostgresLocalAdapter",
                    remediation=remediation,
                    target_table=ctx.target_table,
                    strategy=ctx.write_strategy,
                )
            ) from exc

    def _arrow_write(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        """Fallback for non-native writes — delegate to PostgresSink."""
        from rivet_core.models import Catalog, Joint, Material
        from rivet_postgres.sink import PostgresSink

        cat = Catalog(name=catalog.name, type=catalog.type, options=catalog.options)
        j = Joint(
            name=joint.name,
            joint_type=getattr(joint, "joint_type", "sink"),
            catalog=getattr(joint, "catalog", None),
            table=getattr(joint, "table", None),
        )
        strategy = getattr(joint, "write_strategy", "replace") or "replace"
        mat = Material(
            name=j.name,
            catalog=j.catalog or catalog.name,
            materialized_ref=material,
            state="materialized",
        )
        PostgresSink().write(cat, j, mat, strategy)


def _build_conninfo(options: dict[str, Any]) -> str:
    """Build connection string from catalog options."""
    if "conninfo" in options:
        return str(options["conninfo"])
    host = options.get("host", "localhost")
    port = options.get("port", 5432)
    database = options.get("database", "")
    user = options.get("user", "")
    password = options.get("password", "")
    return f"host={host} port={port} dbname={database} user={user} password={password}"


async def _ensure_table_from_sql(cur: Any, table: str, sql: str) -> None:
    """Create table from SQL schema if it doesn't exist (Postgres)."""
    result = await cur.execute(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)",
        [table.split(".")[-1]],
    )
    row = await result.fetchone()
    if not row or not row[0]:
        await cur.execute(f"CREATE TABLE {table} AS {sql} LIMIT 0")
