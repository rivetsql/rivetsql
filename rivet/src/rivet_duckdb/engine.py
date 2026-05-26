"""DuckDB compute engine plugin."""

from __future__ import annotations

import logging
from typing import Any

import pyarrow

from rivet_core.models import ComputeEngine
from rivet_core.plugins import ComputeEnginePlugin, ReferenceResolver

_logger = logging.getLogger(__name__)
ALL_6_CAPABILITIES = [
    "projection_pushdown",
    "predicate_pushdown",
    "limit_pushdown",
    "cast_pushdown",
    "join",
    "aggregation",
]


class DuckDBReferenceResolver(ReferenceResolver):
    """Compile-time resolver that rewrites logical source names to DuckDB-native expressions.

    Called by the compiler's ``_resolve_references`` pass for fused groups targeting
    the DuckDB engine.  For each upstream source joint, the resolver substitutes the
    logical name with:

    * **Filesystem sources** — a reader function call such as
      ``read_csv_auto('/abs/path/file.csv')``, ``read_parquet(...)`` or
      ``read_json_auto(...)`` depending on the catalog format or file extension.
    * **DuckDB sources** — the qualified (or simple) table name from the source
      catalog entry.

    CTE siblings (non-source joints with SQL in the same fused group) are left
    untouched so their CTE aliases remain valid.

    Replacement uses word-boundary regex (``\\b``) to avoid partial substitution
    inside longer identifiers (e.g. ``src_products`` won't match
    ``src_products_v2``).

    Returns ``None`` when no substitution was made, signalling the compiler to
    keep the original SQL and fall back to the Arrow staging path at runtime.
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
        import re

        joint_type = getattr(joint, "type", None)

        # Source joints: resolve their own table reference to avoid
        # self-referencing CTEs (e.g. `x AS (SELECT * FROM x)`).
        if joint_type == "source" and catalog_map:
            joint_name = getattr(joint, "name", None)
            table_ref = getattr(joint, "table", None) or joint_name
            if joint_name and table_ref:
                # Try the joint's own catalog first, then fall back to any
                # catalog in the map that can resolve this source.
                cat_name = getattr(joint, "catalog", None)
                candidates: list[Any] = []
                if cat_name:
                    exact = catalog_map.get(cat_name)
                    if exact:
                        candidates.append(exact)
                # Fallback: try all catalogs (handles catalog name mismatches
                # between production and local profiles).
                if not candidates:
                    candidates.extend(catalog_map.values())
                for cat in candidates:
                    cat_type = getattr(cat, "type", None) or (
                        getattr(cat, "options", {}).get("type")
                    )
                    replacement: str | None = None
                    if cat_type == "filesystem":
                        replacement = _resolve_filesystem_source(joint, cat)
                    elif cat_type == "duckdb":
                        replacement = _resolve_duckdb_source(joint, cat)
                    if replacement and replacement != table_ref:
                        pattern = re.compile(r"\b" + re.escape(table_ref) + r"\b")
                        new_sql = pattern.sub(replacement, sql)
                        if new_sql != sql:
                            return new_sql
            return None

        upstream = getattr(joint, "upstream", [])
        if not upstream or not compiled_joints or not catalog_map:
            return None

        # Identify CTE siblings: non-source joints with SQL in the same fused group.
        cte_siblings: set[str] = set()
        if fused_group_joints and compiled_joints:
            for jn in fused_group_joints:
                cj = compiled_joints.get(jn)
                if not cj:
                    continue
                if getattr(cj, "type", None) == "source":
                    continue
                if getattr(cj, "sql", None) or getattr(cj, "sql_translated", None):
                    cte_siblings.add(jn)

        result = sql
        changed = False

        for up_name in upstream:
            if up_name in cte_siblings:
                continue

            up_cj = compiled_joints.get(up_name)
            if not up_cj:
                continue
            up_type = getattr(up_cj, "type", None)
            if up_type not in ("source", "checkpoint"):
                continue

            up_catalog_name = getattr(up_cj, "catalog", None)
            if not up_catalog_name:
                continue
            cat = catalog_map.get(up_catalog_name)
            # Fallback: when the catalog name doesn't match any profile
            # catalog (e.g. production "unity" vs local "datalake"), try
            # all catalogs to find one that can resolve this source.
            up_candidates: list[Any] = [cat] if cat else list(catalog_map.values())

            resolved_replacement: str | None = None
            for cand in up_candidates:
                if cand is None:
                    continue
                catalog_type = getattr(cand, "type", None) or (
                    getattr(cand, "options", {}).get("type")
                )

                if catalog_type == "filesystem":
                    resolved_replacement = _resolve_filesystem_source(up_cj, cand)
                elif catalog_type == "duckdb":
                    resolved_replacement = _resolve_duckdb_source(up_cj, cand)

                if resolved_replacement is not None:
                    break

            if resolved_replacement is None:
                continue

            pattern = re.compile(r"\b" + re.escape(up_name) + r"\b")
            new_result = pattern.sub(resolved_replacement, result)
            if new_result != result:
                result = new_result
                changed = True

        return result if changed else None


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
                _logger.debug('DuckDB view unregister (idempotent)', exc_info=True)  # best-effort: see RVT logs at debug level
        self._registered_views.clear()

    def _get_engine_lock(self, engine_name: str) -> Any:
        """Return a per-engine lock, creating one if needed."""
        import threading

        with self._meta_lock:
            if engine_name not in self._engine_locks:
                self._engine_locks[engine_name] = threading.Lock()
            return self._engine_locks[engine_name]

    @property
    def supports_native_assertions(self) -> bool:
        """DuckDB supports running assertion checks via SQL."""
        return True

    def execute_assertion_sql(
        self,
        engine: ComputeEngine,
        sql: str,
        input_tables: dict[str, pyarrow.Table],
    ) -> pyarrow.Table:
        """Execute assertion SQL by delegating to execute_sql."""
        return self.execute_sql(engine, sql, input_tables)

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
                        _logger.debug('DuckDB view cleanup (idempotent)', exc_info=True)  # best-effort: see RVT logs at debug level
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

    def get_reference_resolver(self) -> ReferenceResolver | None:
        """Return a resolver that rewrites source references to DuckDB-native expressions."""
        return DuckDBReferenceResolver()


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


_FORMAT_TO_READER: dict[str, str] = {
    "csv": "read_csv_auto",
    "parquet": "read_parquet",
    "json": "read_json_auto",
    "ndjson": "read_json_auto",
    "jsonl": "read_json_auto",
}
"""Mapping from catalog ``format`` option value to DuckDB reader function name.

Used by :func:`_resolve_filesystem_source` when the catalog declares an explicit
format.  Falls back to :data:`_EXTENSION_TO_READER` when no format is set.
"""

_FORMAT_TO_EXTENSION: dict[str, str] = {
    "csv": ".csv",
    "parquet": ".parquet",
    "json": ".json",
    "ndjson": ".ndjson",
    "jsonl": ".jsonl",
}
"""Mapping from catalog ``format`` option value to file extension.

Used by :func:`_resolve_filesystem_source` to construct the expected file path
when the file does not exist yet (e.g. checkpoint tables written at runtime).
"""


def _resolve_filesystem_source(source_cj: Any, catalog: Any) -> str | None:
    """Resolve a filesystem source to a DuckDB reader function call.

    Builds an expression like ``read_csv_auto('/abs/path/file.csv')`` by:

    1. Reading the catalog's ``path`` option as the base directory.
    2. Appending the source's ``table`` (or ``name``) to form the file path.
    3. If the exact path doesn't exist, falling back to stem matching — scanning
       the base directory for a file whose stem equals the table name.
    4. If the file still doesn't exist but the catalog declares an explicit
       ``format``, constructing the expected path with the format extension.
       This handles checkpoint tables that will be written at runtime.
    5. Choosing the reader function from the catalog's explicit ``format`` option
       (via :data:`_FORMAT_TO_READER`) or, when absent, from the resolved file's
       extension (via :data:`_EXTENSION_TO_READER`).

    Args:
        source_cj: The compiled joint for the upstream source.
        catalog: The catalog object for the source (must be ``type="filesystem"``).

    Returns:
        A DuckDB reader call string, or ``None`` if the file cannot be found or
        the format is not recognised.
    """
    from pathlib import Path

    opts = getattr(catalog, "options", {})
    base_path = opts.get("path")
    if not base_path:
        return None

    table_name = getattr(source_cj, "table", None) or getattr(source_cj, "name", None)
    if not table_name:
        return None

    file_path = Path(base_path) / table_name
    if not file_path.exists():
        # Fallback: find a file whose stem matches the table name.
        base = Path(base_path)
        if base.is_dir():
            for entry in base.iterdir():
                if entry.is_file() and entry.stem == table_name:
                    file_path = entry
                    break
        if not file_path.exists():
            # Last resort: if the catalog declares an explicit format, we can
            # construct the expected path even when the file doesn't exist yet
            # (e.g. checkpoint tables that will be written at runtime).
            fmt = opts.get("format")
            if fmt:
                reader = _FORMAT_TO_READER.get(fmt)
                ext = _FORMAT_TO_EXTENSION.get(fmt)
                if reader and ext:
                    expected = Path(base_path) / f"{table_name}{ext}"
                    return f"{reader}('{expected}')"
            return None

    # Determine reader function: explicit catalog format takes priority.
    fmt = opts.get("format")
    if fmt:
        reader = _FORMAT_TO_READER.get(fmt)
    else:
        reader = _EXTENSION_TO_READER.get(file_path.suffix.lower())

    if reader is None:
        return None

    return f"{reader}('{file_path}')"


def _resolve_duckdb_source(source_cj: Any, catalog: Any) -> str | None:
    """Resolve a DuckDB source to a table reference.

    Returns the source's ``table`` field as-is (which may be a qualified name
    like ``schema.table``), or falls back to the joint ``name`` when ``table``
    is not set.

    Args:
        source_cj: The compiled joint for the upstream source.
        catalog: The catalog object for the source (must be ``type="duckdb"``).

    Returns:
        A table name string, or ``None`` if neither ``table`` nor ``name`` is
        available.
    """
    table: str | None = getattr(source_cj, "table", None)
    if not table:
        name: str | None = getattr(source_cj, "name", None)
        return name
    return table


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
            _logger.debug('DuckDB profiling stats read', exc_info=True)  # best-effort: see RVT logs at debug level

    # --- parallelism ---
    threads_used: int | None = None
    if conn is not None:
        try:
            row = conn.execute("SELECT current_setting('threads')").fetchone()
            if row and row[0] is not None:
                threads_used = int(row[0])
        except Exception:
            _logger.debug('DuckDB threads-setting read', exc_info=True)  # best-effort: see RVT logs at debug level
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
            _logger.debug('DuckDB loaded-extensions read', exc_info=True)  # best-effort: see RVT logs at debug level

    return PluginMetrics(
        well_known=well_known,  # type: ignore[arg-type]
        extensions=extensions,
        engine="duckdb",
    )
