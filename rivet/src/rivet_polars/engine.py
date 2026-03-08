"""Polars compute engine plugin."""

from __future__ import annotations

from typing import Any

import pyarrow

from rivet_core.models import ComputeEngine, Material
from rivet_core.plugins import ComputeEnginePlugin
from rivet_core.strategies import MaterializedRef

ALL_6_CAPABILITIES = [
    "projection_pushdown",
    "predicate_pushdown",
    "limit_pushdown",
    "cast_pushdown",
    "join",
    "aggregation",
]


class PolarsLazyMaterializedRef(MaterializedRef):
    """MaterializedRef backed by a polars LazyFrame. Collects only on to_arrow()."""

    def __init__(self, lazy_frame: Any, streaming: bool = False) -> None:
        self._lazy_frame = lazy_frame
        self._streaming = streaming
        self._collected: Any = None  # polars.DataFrame | None

    def _collect(self) -> Any:
        if self._collected is None:
            if self._streaming:
                self._collected = self._lazy_frame.collect(engine="streaming")
            else:
                self._collected = self._lazy_frame.collect()
        return self._collected

    def to_arrow(self) -> pyarrow.Table:
        return self._collect().to_arrow()

    @property
    def schema(self) -> Any:
        from rivet_core.models import Column, Schema

        return Schema(
            columns=[
                Column(name=name, type=str(dtype), nullable=True)
                for name, dtype in self._lazy_frame.collect_schema().items()
            ]
        )

    @property
    def row_count(self) -> int:
        return self._collect().height  # type: ignore[no-any-return]

    @property
    def size_bytes(self) -> int | None:
        return None

    @property
    def storage_type(self) -> str:
        return "polars_lazy"


class PolarsComputeEnginePlugin(ComputeEnginePlugin):
    engine_type = "polars"
    dialect = "duckdb"
    supported_catalog_types: dict[str, list[str]] = {
        "arrow": ALL_6_CAPABILITIES,
        "filesystem": ALL_6_CAPABILITIES,
    }
    required_options: list[str] = []
    optional_options: dict[str, Any] = {
        "streaming": False,
        "n_threads": None,
        "check_dtypes": True,
    }
    credential_options: list[str] = []

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type="polars")

    def execute_sql(
        self,
        engine: ComputeEngine,
        sql: str,
        input_tables: dict[str, pyarrow.Table],
    ) -> pyarrow.Table:
        """Execute SQL via polars.SQLContext, return Arrow table."""
        import polars as pl

        ctx = pl.SQLContext()
        for name, table in input_tables.items():
            ctx.register(name, pl.from_arrow(table).lazy())  # type: ignore[union-attr]
        result_lf = ctx.execute(sql)
        return result_lf.collect().to_arrow()

    def execute_sql_lazy(self, sql: str, upstream_frames: dict[str, Any], streaming: bool = False) -> Material:
        """Execute SQL via polars.SQLContext, return deferred Material backed by LazyFrame."""
        import polars as pl

        from rivet_core.errors import ExecutionError, plugin_error

        ctx = pl.SQLContext()
        for name, lf in upstream_frames.items():
            ctx.register(name, lf)
        try:
            result_lf = ctx.execute(sql)
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Polars SQL execution failed: {exc}",
                    plugin_name="rivet_polars",
                    plugin_type="engine",
                    remediation="Check that all referenced tables are registered and SQL is valid.",
                    sql=sql,
                )
            ) from exc
        ref = PolarsLazyMaterializedRef(result_lf, streaming=streaming)
        return Material(name="polars_result", catalog="", materialized_ref=ref, state="deferred")

    def execute_fused_group(
        self,
        joints: list[Any],
        upstream_frames: dict[str, Any],
        streaming: bool = False,
    ) -> Material:
        """Execute a fused group of joints using one fresh SQLContext, then discard it.

        Creates a single polars.SQLContext, registers all upstream LazyFrames,
        executes the terminal joint's SQL, and returns a deferred Material.
        The SQLContext is discarded after the result LazyFrame is obtained.
        """
        import polars as pl

        from rivet_core.errors import ExecutionError, plugin_error

        terminal = joints[-1]
        ctx = pl.SQLContext()
        for name, lf in upstream_frames.items():
            ctx.register(name, lf)
        try:
            result_lf = ctx.execute(terminal.sql)
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Polars SQL execution failed in fused group: {exc}",
                    plugin_name="rivet_polars",
                    plugin_type="engine",
                    remediation="Check that all referenced tables are registered and SQL is valid.",
                    sql=terminal.sql,
                    joint=terminal.name,
                )
            ) from exc
        # ctx goes out of scope here — discarded after result_lf is obtained
        ref = PolarsLazyMaterializedRef(result_lf, streaming=streaming)
        return Material(
            name=terminal.name,
            catalog="",
            materialized_ref=ref,
            state="deferred",
        )

    def collect_metrics(self, execution_context: Any) -> Any:
        """Return PluginMetrics with query_planning, scan, memory, io + polars extensions."""
        from rivet_core.metrics import (
            IOMetrics,
            MemoryMetrics,
            PluginMetrics,
            QueryPlanningMetrics,
            ScanMetrics,
        )

        ctx = execution_context if isinstance(execution_context, dict) else {}
        return PluginMetrics(
            well_known={
                "query_planning": QueryPlanningMetrics(
                    planning_time_ms=ctx.get("planning_time_ms"),
                ),
                "scan": ScanMetrics(
                    rows_scanned=ctx.get("rows_scanned"),
                    rows_filtered=ctx.get("rows_filtered"),
                ),
                "memory": MemoryMetrics(
                    peak_bytes=ctx.get("peak_bytes"),
                    spilled_bytes=ctx.get("spilled_bytes"),
                ),
                "io": IOMetrics(
                    bytes_read=ctx.get("bytes_read"),
                    bytes_written=ctx.get("bytes_written"),
                    files_read=ctx.get("files_read"),
                ),
            },
            extensions={
                "polars.streaming_mode": ctx.get("streaming", False),
                "polars.collect_time_ms": ctx.get("collect_time_ms"),
                "polars.n_threads": ctx.get("n_threads"),
            },
            engine="polars",
        )

    def validate(self, options: dict[str, Any]) -> None:
        from rivet_core.errors import PluginValidationError, plugin_error

        recognized = set(self.optional_options) | set(self.required_options)
        for key in options:
            if key not in recognized:
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"Unknown option '{key}' for polars engine.",
                        plugin_name="rivet_polars",
                        plugin_type="engine",
                        remediation=f"Valid options: {', '.join(sorted(recognized))}",
                        option=key,
                    )
                )

        def _fail(option: str, msg: str) -> None:
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"Invalid value for option '{option}' in polars engine: {msg}",
                    plugin_name="rivet_polars",
                    plugin_type="engine",
                    remediation=f"Check the expected type for '{option}'.",
                    option=option,
                )
            )

        if "streaming" in options and not isinstance(options["streaming"], bool):
            _fail("streaming", "must be a boolean")

        if "n_threads" in options and options["n_threads"] is not None:
            if not isinstance(options["n_threads"], int):
                _fail("n_threads", "must be an integer or None")

        if "check_dtypes" in options and not isinstance(options["check_dtypes"], bool):
            _fail("check_dtypes", "must be a boolean")
