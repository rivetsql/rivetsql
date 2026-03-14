"""PostgreSQL source plugin: execute SQL via psycopg3, return deferred Material."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow as pa

from rivet_core.models import Material
from rivet_core.plugins import SourcePlugin
from rivet_core.strategies import MaterializedRef

if TYPE_CHECKING:
    from rivet_core.models import Catalog, Joint, Schema


class PostgresDeferredMaterializedRef(MaterializedRef):
    """MaterializedRef backed by a PostgreSQL query. Executes only on to_arrow()."""

    def __init__(self, conninfo: str, sql: str) -> None:
        self._conninfo = conninfo
        self._sql = sql

    def to_arrow(self) -> pa.Table:

        from rivet_core.async_utils import safe_run_async
        from rivet_core.errors import ExecutionError, plugin_error

        async def _fetch() -> pa.Table:
            try:
                import psycopg
            except ImportError as exc:
                raise ExecutionError(
                    plugin_error(
                        "RVT-501",
                        f"psycopg3 is required for PostgreSQL source: {exc}",
                        plugin_name="rivet_postgres",
                        plugin_type="source",
                        remediation="Install psycopg3: pip install psycopg[binary]",
                        conninfo=self._conninfo,
                    )
                ) from exc

            try:
                async with await psycopg.AsyncConnection.connect(self._conninfo) as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(self._sql)
                        rows = await cur.fetchall()
                        col_names = (
                            [desc.name for desc in cur.description] if cur.description else []
                        )
            except ExecutionError:
                raise
            except Exception as exc:
                from rivet_postgres.errors import classify_pg_error

                code, message, remediation = classify_pg_error(exc, plugin_type="source")
                raise ExecutionError(
                    plugin_error(
                        code,
                        message,
                        plugin_name="rivet_postgres",
                        plugin_type="source",
                        remediation=remediation,
                        sql=self._sql,
                    )
                ) from exc

            if not col_names:
                return pa.table({})

            if rows and isinstance(rows[0], dict):
                columns = {name: [row[name] for row in rows] for name in col_names}
            else:
                columns = {name: [row[i] for row in rows] for i, name in enumerate(col_names)}

            arrays = [pa.array(columns[name]) for name in col_names]
            return pa.table(dict(zip(col_names, arrays)))

        return safe_run_async(_fetch())

    @property
    def schema(self) -> Schema:
        from rivet_core.models import Column, Schema

        table = self.to_arrow()
        return Schema(
            columns=[
                Column(name=field.name, type=str(field.type), nullable=field.nullable)
                for field in table.schema
            ]
        )

    @property
    def row_count(self) -> int:
        return self.to_arrow().num_rows  # type: ignore[no-any-return]

    @property
    def size_bytes(self) -> int | None:
        return None

    @property
    def storage_type(self) -> str:
        return "postgres"


class PostgresSource(SourcePlugin):
    """Source plugin for postgres catalog type.

    Executes source SQL against the PostgreSQL database via psycopg3 and returns
    a deferred Material. Data is not fetched until .to_arrow() is called.
    """

    catalog_type = "postgres"

    def read(self, catalog: Catalog, joint: Joint, pushdown: Any | None) -> Material:
        options = catalog.options
        host = options.get("host", "localhost")
        port = options.get("port", 5432)
        database = options.get("database", "")
        user = options.get("user", "")
        password = options.get("password", "")

        conninfo = f"host={host} port={port} dbname={database} user={user} password={password}"

        if joint.sql:
            sql = joint.sql
        elif joint.table:
            sql = f"SELECT * FROM {joint.table}"
        else:
            sql = "SELECT 1"

        ref = PostgresDeferredMaterializedRef(conninfo=conninfo, sql=sql)
        return Material(
            name=joint.name,
            catalog=catalog.name,
            materialized_ref=ref,
            state="deferred",
        )
