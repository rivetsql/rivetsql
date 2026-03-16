"""DuckDB local adapter: native SQL write for DuckDB engine → DuckDB catalog.

When the compute engine and sink/checkpoint catalog are both DuckDB, the fused
SQL can be embedded directly into the write DDL (e.g. ``CREATE TABLE ... AS
<fused_sql>``), eliminating the Arrow round-trip through ``SinkPlugin.write()``.

For cross-catalog scenarios (e.g. filesystem source → DuckDB sink), the adapter
ATTACHes the sink database to the engine's existing connection so that fused SQL
can reference CTE aliases registered as Arrow views in the engine connection.

For reads, DuckDB→DuckDB uses engine-native SQL — no adapter dispatch needed.
This adapter exists primarily for ``write_dispatch`` with native SQL write.
"""

from __future__ import annotations

import logging
from typing import Any

from rivet_core.errors import ExecutionError, plugin_error
from rivet_core.optimizer import AdapterPushdownResult, PushdownPlan
from rivet_core.plugins import ComputeEngineAdapter, NativeSqlWriteContext

_logger = logging.getLogger(__name__)

_NATIVE_WRITE_STRATEGIES = frozenset({"replace", "append", "truncate_insert"})


class DuckDBLocalAdapter(ComputeEngineAdapter):
    """Adapter for DuckDB engine writing to DuckDB catalog via native SQL."""

    target_engine_type = "duckdb"
    catalog_type = "duckdb"
    capabilities: list[str] = ["native_sql_write"]
    source = "engine_plugin"
    source_plugin = "rivet_duckdb"

    def supports_native_sql_write(self, write_strategy: str) -> bool:
        return write_strategy in _NATIVE_WRITE_STRATEGIES

    def read_dispatch(
        self, engine: Any, catalog: Any, joint: Any, pushdown: PushdownPlan | None = None
    ) -> AdapterPushdownResult:
        # DuckDB→DuckDB reads use engine-native SQL, not adapter dispatch.
        raise NotImplementedError("DuckDB local reads use engine-native SQL, not adapter dispatch")

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        if isinstance(material, NativeSqlWriteContext):
            return self._native_sql_write(material)
        # Arrow fallback: delegate to DuckDBSink
        return self._arrow_write(engine, catalog, joint, material)

    # ------------------------------------------------------------------
    # Native SQL write
    # ------------------------------------------------------------------

    def _native_sql_write(self, ctx: NativeSqlWriteContext) -> None:
        path = ctx.catalog.options.get("path", ":memory:")
        read_only = ctx.catalog.options.get("read_only", False)
        if read_only:
            raise ExecutionError(
                plugin_error(
                    "RVT-201",
                    f"Cannot write to read-only DuckDB catalog '{ctx.catalog.name}'.",
                    plugin_name="rivet_duckdb",
                    plugin_type="adapter",
                    adapter="DuckDBLocalAdapter",
                    remediation="Set read_only to false or use a different catalog.",
                )
            )

        # Prefer the engine's existing connection (which already has Arrow
        # views registered for upstream CTE references) with the sink DB
        # ATTACHed.  This is required for cross-catalog scenarios where the
        # fused SQL references tables from a different catalog (e.g.
        # filesystem source → DuckDB sink).
        engine_conn = self._get_engine_connection(ctx)
        if engine_conn is not None:
            self._write_via_engine(engine_conn, ctx, path)
        else:
            # Fallback: standalone connection (works when engine and sink
            # share the same DuckDB file, i.e. no cross-catalog refs).
            self._write_standalone(ctx, path)

    def _get_engine_connection(self, ctx: NativeSqlWriteContext) -> Any:
        """Retrieve the DuckDB engine connection from the plugin registry.

        Returns the per-engine connection that already has Arrow views
        registered, or None if unavailable.
        """
        if self._registry is None or ctx.engine is None:
            return None
        engine_plugin = self._registry.get_engine_plugin("duckdb")
        if engine_plugin is None:
            return None
        engine_name = ctx.engine.name if ctx.engine is not None else "__default__"
        return engine_plugin._engine_conns.get(engine_name)  # type: ignore[attr-defined]

    def _write_via_engine(self, conn: Any, ctx: NativeSqlWriteContext, sink_path: str) -> None:
        """Write using the engine connection with the sink DB ATTACHed.

        ATTACHes the sink database file under alias ``__rivet_sink``, registers
        upstream Arrow tables (the engine clears views between calls), executes
        the write DDL referencing ``__rivet_sink.<table>``, then DETACHes.
        """
        from rivet_duckdb.engine import register_arrow_tables

        alias = "__rivet_sink"
        target = ctx.target_table
        qualified = f"{alias}.{target}"
        sql = ctx.fused_sql

        try:
            conn.execute(f"ATTACH '{sink_path}' AS {alias}")
            # Re-register upstream Arrow tables — the engine connection clears
            # views after each execute_sql() call.
            register_arrow_tables(conn, ctx.input_tables)
            try:
                if ctx.write_strategy == "replace":
                    conn.execute(f"DROP TABLE IF EXISTS {qualified}")
                    conn.execute(f"CREATE TABLE {qualified} AS {sql}")
                elif ctx.write_strategy == "append":
                    _ensure_table_from_sql(conn, qualified, sql)
                    conn.execute(f"INSERT INTO {qualified} {sql}")
                elif ctx.write_strategy == "truncate_insert":
                    _ensure_table_from_sql(conn, qualified, sql)
                    conn.execute(f"DELETE FROM {qualified}")
                    conn.execute(f"INSERT INTO {qualified} {sql}")
            finally:
                try:
                    conn.execute(f"DETACH {alias}")
                except Exception:
                    pass  # Best-effort detach

            _logger.debug(
                "native_sql_write (engine conn): %s strategy=%s target=%s",
                ctx.catalog.name,
                ctx.write_strategy,
                target,
            )
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"DuckDB native SQL write failed: {exc}",
                    plugin_name="rivet_duckdb",
                    plugin_type="adapter",
                    adapter="DuckDBLocalAdapter",
                    remediation="Check that the fused SQL is valid and the target table is writable.",
                    target_table=ctx.target_table,
                    strategy=ctx.write_strategy,
                )
            ) from exc

    def _write_standalone(self, ctx: NativeSqlWriteContext, path: str) -> None:
        """Write using a dedicated connection to the sink DB (no cross-catalog refs)."""
        import duckdb

        from rivet_duckdb.engine import register_arrow_tables

        conn = duckdb.connect(path, read_only=False)
        try:
            register_arrow_tables(conn, ctx.input_tables)

            target = ctx.target_table
            sql = ctx.fused_sql

            if ctx.write_strategy == "replace":
                conn.execute(f"DROP TABLE IF EXISTS {target}")
                conn.execute(f"CREATE TABLE {target} AS {sql}")
            elif ctx.write_strategy == "append":
                _ensure_table_from_sql(conn, target, sql)
                conn.execute(f"INSERT INTO {target} {sql}")
            elif ctx.write_strategy == "truncate_insert":
                _ensure_table_from_sql(conn, target, sql)
                conn.execute(f"DELETE FROM {target}")
                conn.execute(f"INSERT INTO {target} {sql}")

            _logger.debug(
                "native_sql_write (standalone): %s strategy=%s target=%s",
                ctx.catalog.name,
                ctx.write_strategy,
                target,
            )
        except ExecutionError:
            raise
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"DuckDB native SQL write failed: {exc}",
                    plugin_name="rivet_duckdb",
                    plugin_type="adapter",
                    adapter="DuckDBLocalAdapter",
                    remediation="Check that the fused SQL is valid and the target table is writable.",
                    target_table=ctx.target_table,
                    strategy=ctx.write_strategy,
                )
            ) from exc
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Arrow fallback
    # ------------------------------------------------------------------

    def _arrow_write(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        """Fallback for non-native writes — delegate to DuckDBSink."""
        from rivet_core.models import Catalog, Joint, Material
        from rivet_duckdb.sink import DuckDBSink

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
        DuckDBSink().write(cat, j, mat, strategy)


def _ensure_table_from_sql(conn: Any, table: str, sql: str) -> None:
    """Create table from SQL schema if it doesn't exist."""
    exists = conn.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE table_name = ?", [table]
    ).fetchone()[0]
    if not exists:
        conn.execute(f"CREATE TABLE {table} AS {sql} LIMIT 0")
