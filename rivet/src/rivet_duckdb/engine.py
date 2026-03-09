"""DuckDB compute engine plugin."""

from __future__ import annotations

from typing import Any

import pyarrow

from rivet_core.models import ComputeEngine
from rivet_core.plugins import ComputeEnginePlugin

ALL_6_CAPABILITIES = [
    "projection_pushdown",
    "predicate_pushdown",
    "limit_pushdown",
    "cast_pushdown",
    "join",
    "aggregation",
]


class DuckDBComputeEnginePlugin(ComputeEnginePlugin):
    engine_type = "duckdb"
    dialect = "duckdb"
    supported_catalog_types: dict[str, list[str]] = {
        "duckdb": ALL_6_CAPABILITIES,
        "arrow": ALL_6_CAPABILITIES,
        "filesystem": ALL_6_CAPABILITIES,
    }
    required_options: list[str] = []
    optional_options: dict[str, Any] = {
        "threads": None,
        "memory_limit": "4GB",
        "temp_directory": None,
        "extensions": [],
    }
    credential_options: list[str] = []

    def __init__(self) -> None:
        super().__init__()
        import threading
        self._conn: Any = None  # duckdb.DuckDBPyConnection | None (backward compat)
        self._registered_views: set[str] = set()  # backward compat
        # Per-engine-name connections and view sets for thread-safe parallel execution.
        self._engine_conns: dict[str, Any] = {}
        self._engine_views: dict[str, set[str]] = {}
        self._engine_locks: dict[str, threading.Lock] = {}
        self._meta_lock = threading.Lock()

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type="duckdb")

    def collect_metrics(self, execution_context: Any) -> Any:
        """Collect DuckDB metrics: query_planning, io, memory, parallelism, scan + extensions.

        Args:
            execution_context: Dict optionally containing:
                - connection: DuckDB connection for live queries
                - timing: PhasedTiming with engine_ms / total_ms
                - rows_scanned: int
                - rows_filtered: int
                - bytes_read: int
                - bytes_written: int

        Returns:
            PluginMetrics or None (never raises).
        """
        try:
            return _collect_duckdb_metrics(execution_context)
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
                        f"Unknown option '{key}' for duckdb engine.",
                        plugin_name="rivet_duckdb",
                        plugin_type="engine",
                        remediation=f"Valid options: {', '.join(sorted(recognized))}",
                        option=key,
                    )
                )

        def _fail(option: str, msg: str) -> None:
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"Invalid value for option '{option}' in duckdb engine: {msg}",
                    plugin_name="rivet_duckdb",
                    plugin_type="engine",
                    remediation=f"Check the expected type for '{option}'.",
                    option=option,
                )
            )

        if "threads" in options and options["threads"] is not None:
            if not isinstance(options["threads"], int):
                _fail("threads", "must be an integer or None")

        if "memory_limit" in options and options["memory_limit"] is not None:
            if not isinstance(options["memory_limit"], str):
                _fail("memory_limit", "must be a string (e.g. '4GB')")

        if "temp_directory" in options and options["temp_directory"] is not None:
            if not isinstance(options["temp_directory"], str):
                _fail("temp_directory", "must be a string path or None")

        if "extensions" in options:
            exts = options["extensions"]
            if not isinstance(exts, list):
                _fail("extensions", "must be a list of extension name strings")
            if not all(isinstance(e, str) for e in exts):
                _fail("extensions", "all extension names must be strings")

    def _get_connection(self) -> Any:
        """Lazily create and return the reusable DuckDB connection."""
        if self._conn is None:
            import duckdb
            self._conn = duckdb.connect()
        return self._conn

    def _cleanup_views(self, conn: Any) -> None:
        """Unregister previously registered views from the connection."""
        for view in self._registered_views:
            try:
                conn.unregister(view)
            except Exception:
                pass
        self._registered_views.clear()

    def _get_engine_lock(self, engine_name: str) -> Any:
        """Return a per-engine lock, creating one if needed."""
        import threading
        with self._meta_lock:
            if engine_name not in self._engine_locks:
                self._engine_locks[engine_name] = threading.Lock()
            return self._engine_locks[engine_name]

    def execute_sql(
        self,
        engine: ComputeEngine,
        sql: str,
        input_tables: dict[str, pyarrow.Table],
    ) -> pyarrow.Table:
        """Execute SQL by registering Arrow tables in a per-engine DuckDB connection.

        Each engine instance (e.g. ``duckdb_primary``, ``duckdb_secondary``)
        gets its own connection and lock so that the parallel executor can
        dispatch groups on different engines concurrently without races on
        shared state.  Falls back to a shared connection when *engine* is
        ``None`` (e.g. interactive / REPL queries).
        """
        engine_name = engine.name if engine is not None else "__default__"
        lock = self._get_engine_lock(engine_name)

        with lock:
            conn = self._engine_conns.get(engine_name)
            if conn is None:
                import duckdb
                conn = duckdb.connect()
                self._engine_conns[engine_name] = conn
                self._engine_views[engine_name] = set()

            # Also maintain _conn / _registered_views for backward compat
            self._conn = conn
            views = self._engine_views[engine_name]

            try:
                # Unregister previously registered views on this connection
                for view in list(views):
                    try:
                        conn.unregister(view)
                    except Exception:
                        pass
                views.clear()

                for name, table in input_tables.items():
                    conn.register(name, table)
                    views.add(name)
                self._registered_views = views
                return conn.execute(sql).fetch_arrow_table()
            except Exception:
                # Discard connection on unrecoverable error
                self._engine_conns.pop(engine_name, None)
                self._engine_views.pop(engine_name, None)
                self._conn = None
                self._registered_views = set()
                raise


def apply_engine_settings(conn: Any, config: dict[str, Any]) -> None:
    """Apply memory_limit and threads to a DuckDB connection before query execution."""
    memory_limit = config.get("memory_limit")
    if memory_limit is not None:
        conn.execute(f"SET memory_limit='{memory_limit}'")

    threads = config.get("threads")
    if threads is not None:
        conn.execute(f"SET threads={threads}")


_EXTENSION_TO_READER: dict[str, str] = {
    ".parquet": "read_parquet",
    ".csv": "read_csv_auto",
    ".tsv": "read_csv_auto",
    ".json": "read_json_auto",
    ".ndjson": "read_json_auto",
    ".jsonl": "read_json_auto",
}

_SUPPORTED_EXTENSIONS = ", ".join(sorted(_EXTENSION_TO_READER))


def infer_filesystem_reader(path: str) -> str:
    """Infer the DuckDB reader function from a file path's extension.

    Args:
        path: File path (local or remote).

    Returns:
        DuckDB reader function name (e.g. 'read_parquet').

    Raises:
        ExecutionError (RVT-501) if the extension is unrecognized.
    """
    import os

    from rivet_core.errors import ExecutionError, plugin_error

    _, ext = os.path.splitext(path.lower())
    reader = _EXTENSION_TO_READER.get(ext)
    if reader is None:
        raise ExecutionError(
            plugin_error(
                "RVT-501",
                f"Unrecognized file extension '{ext}' for filesystem read: '{path}'.",
                plugin_name="rivet_duckdb",
                plugin_type="engine",
                remediation=f"Supported extensions: {_SUPPORTED_EXTENSIONS}",
                path=path,
                extension=ext,
            )
        )
    return reader


def register_arrow_tables(conn: Any, tables: dict[str, Any]) -> None:
    """Register PyArrow tables into a DuckDB connection using zero-copy Arrow registration.

    DuckDB's conn.register() uses the Arrow C Data Interface for zero-copy access
    when the memory layout is compatible, avoiding data duplication.

    Args:
        conn: A DuckDB connection.
        tables: Mapping of view name to pyarrow.Table (or RecordBatch/RecordBatchReader).
    """
    for name, arrow_table in tables.items():
        conn.register(name, arrow_table)


def _collect_duckdb_metrics(execution_context: Any) -> Any:
    """Build PluginMetrics from a DuckDB execution context dict.

    Queries live DuckDB system tables when a connection is available.
    All fields are optional — missing data is represented as None.
    """
    from rivet_core.metrics import (
        IOMetrics,
        MemoryMetrics,
        ParallelismMetrics,
        PluginMetrics,
        QueryPlanningMetrics,
        ScanMetrics,
    )

    ctx: dict[str, Any] = execution_context if isinstance(execution_context, dict) else {}
    conn = ctx.get("connection")
    timing = ctx.get("timing")

    # --- query_planning ---
    planning_time_ms: float | None = None
    actual_rows: int | None = None
    if timing is not None:
        planning_time_ms = getattr(timing, "engine_ms", None)
    actual_rows = ctx.get("rows_out")

    # --- io ---
    bytes_read: int | None = ctx.get("bytes_read")
    bytes_written: int | None = ctx.get("bytes_written")

    # --- memory ---
    peak_bytes: int | None = None
    spilled_bytes: int | None = None
    if conn is not None:
        try:
            rows = conn.execute(
                "SELECT sum(memory_usage_bytes), sum(temporary_storage_bytes) FROM duckdb_memory()"
            ).fetchone()
            if rows:
                peak_bytes = int(rows[0]) if rows[0] is not None else None
                spilled_bytes = int(rows[1]) if rows[1] is not None else None
        except Exception:
            pass

    # --- parallelism ---
    threads_used: int | None = None
    if conn is not None:
        try:
            row = conn.execute("SELECT current_setting('threads')").fetchone()
            if row and row[0] is not None:
                threads_used = int(row[0])
        except Exception:
            pass
    if threads_used is None:
        threads_used = ctx.get("threads_used")

    # --- scan ---
    rows_scanned: int | None = ctx.get("rows_scanned")
    rows_filtered: int | None = ctx.get("rows_filtered")
    filter_selectivity: float | None = None
    if rows_scanned and rows_filtered is not None and rows_scanned > 0:
        filter_selectivity = 1.0 - (rows_filtered / rows_scanned)

    well_known = {
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
            spilled_bytes=spilled_bytes,
            spilled=(spilled_bytes is not None and spilled_bytes > 0),
        ),
        "parallelism": ParallelismMetrics(
            threads_used=threads_used,
        ),
        "scan": ScanMetrics(
            rows_scanned=rows_scanned,
            rows_filtered=rows_filtered,
            filter_selectivity=filter_selectivity,
        ),
    }

    extensions: dict[str, Any] = {}
    if conn is not None:
        try:
            loaded = conn.execute(
                "SELECT extension_name FROM duckdb_extensions() WHERE loaded = true"
            ).fetchall()
            extensions["duckdb.loaded_extensions"] = [r[0] for r in loaded]
        except Exception:
            pass

    return PluginMetrics(
        well_known=well_known,  # type: ignore[arg-type]
        extensions=extensions,
        engine="duckdb",
    )
