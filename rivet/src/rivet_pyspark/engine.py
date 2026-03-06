"""PySpark compute engine plugin."""

from __future__ import annotations

import logging
from typing import Any

import pyarrow

_logger = logging.getLogger(__name__)

from rivet_core.models import ComputeEngine, Material
from rivet_core.plugins import ComputeEnginePlugin
from rivet_core.strategies import MaterializedRef


def fuse_joints(joints: list[Any]) -> str:
    """Build a CTE SQL string from a fused group of joints.

    For a single joint, returns its SQL unchanged.
    For multiple joints, wraps all but the last as CTEs:
      WITH j1 AS (j1.sql), j2 AS (j2.sql) ... terminal_sql
    """
    if len(joints) == 1:
        return joints[-1].sql  # type: ignore[no-any-return]
    ctes = [f"{j.name} AS ({j.sql})" for j in joints[:-1]]
    return f"WITH {', '.join(ctes)} {joints[-1].sql}"


def _pandas_df_to_arrow(pandas_df: Any) -> pyarrow.Table:
    """Convert a pandas DataFrame to a PyArrow Table. Extracted for testability."""
    return pyarrow.Table.from_pandas(pandas_df)


class SparkDataFrameMaterializedRef(MaterializedRef):
    """MaterializedRef backed by a Spark DataFrame. Materializes to Arrow on to_arrow()."""

    def __init__(self, df: Any) -> None:
        self._df = df

    def to_arrow(self) -> pyarrow.Table:
        # Prefer toArrow() (Spark >= 3.3), fall back to toPandas()
        if hasattr(self._df, "toArrow"):
            result = self._df.toArrow()
            # Spark 4.0 / Spark Connect returns RecordBatchReader, not Table
            if isinstance(result, pyarrow.RecordBatchReader):
                return result.read_all()
            return result
        return _pandas_df_to_arrow(self._df.toPandas())

    @property
    def schema(self) -> Any:
        from rivet_core.models import Column, Schema

        return Schema(
            columns=[
                Column(name=field.name, type=str(field.dataType), nullable=field.nullable)
                for field in self._df.schema.fields
            ]
        )

    @property
    def row_count(self) -> int:
        return self._df.count()  # type: ignore[no-any-return]

    @property
    def size_bytes(self) -> int | None:
        return None

    @property
    def storage_type(self) -> str:
        return "spark_dataframe"


ALL_6_CAPABILITIES = [
    "projection_pushdown",
    "predicate_pushdown",
    "limit_pushdown",
    "cast_pushdown",
    "join",
    "aggregation",
]


class PySparkComputeEngine(ComputeEngine):
    """PySpark engine with SparkSession lifecycle management.

    - Lazy creation: session created on first get_session() call
    - Singleton per process: reuses existing active session if present
    - Teardown: calls spark.stop() unless session was externally managed
    """

    def __init__(self, name: str, config: dict[str, Any]) -> None:
        super().__init__(name=name, engine_type="pyspark")
        self._config = config
        self._session: Any = None
        self._externally_managed: bool = False

    def get_session(self) -> Any:
        """Return the SparkSession, creating it lazily if needed.

        When explicit ``config`` options are provided (e.g. ``spark.jars.packages``),
        a fresh session is always built so that the config is applied.  Reusing an
        existing active session would silently ignore those options.
        """
        if self._session is not None:
            return self._session

        connect_url = self._config.get("connect_url")
        if connect_url:
            from pyspark.sql.connect import (  # type: ignore[attr-defined]
                SparkSession as ConnectSparkSession,
            )

            self._session = ConnectSparkSession.builder.remote(connect_url).getOrCreate()
            return self._session

        from pyspark.sql import SparkSession

        explicit_config: dict[str, str] = self._config.get("config", {})
        _logger.debug("self._config = %s", self._config)
        _logger.debug("explicit_config = %s", explicit_config)

        # Only reuse an existing session when no explicit config is requested.
        # spark.jars.packages and other build-time settings cannot be changed
        # after session creation, so reusing would silently drop them.
        if not explicit_config:
            existing = SparkSession.getActiveSession()
            if existing is not None:
                _logger.debug("Reusing existing SparkSession (no explicit config)")
                self._session = existing
                self._externally_managed = True
                return self._session

        builder = SparkSession.builder
        builder = builder.master(self._config.get("master", "local[*]"))
        builder = builder.appName(self._config.get("app_name", "rivet"))
        for k, v in explicit_config.items():
            _logger.debug("Setting Spark config: %s = %s", k, v)
            builder = builder.config(k, v)
        self._session = builder.getOrCreate()
        _logger.debug("SparkSession created (version %s)", self._session.version)
        return self._session

    def execute_fused_group(self, joints: list[Any]) -> Material:
        """Execute a fused group of SQL joints as a single Spark action using CTEs.

        Builds a CTE SQL string from the group and submits it as one spark.sql() call.
        Returns a deferred Material backed by the resulting Spark DataFrame.
        """
        sql = fuse_joints(joints)
        session = self.get_session()
        df = session.sql(sql)
        ref = SparkDataFrameMaterializedRef(df)
        return Material(
            name=joints[-1].name,
            catalog="",
            materialized_ref=ref,
            state="deferred",
        )

    def teardown(self) -> None:
        """Stop the SparkSession unless it was externally managed."""
        if self._session is not None and not self._externally_managed:
            self._session.stop()
            self._session = None


class PySparkComputeEnginePlugin(ComputeEnginePlugin):
    engine_type = "pyspark"
    dialect = "spark"
    supported_catalog_types: dict[str, list[str]] = {
        "arrow": ALL_6_CAPABILITIES,
        "filesystem": ALL_6_CAPABILITIES,
    }
    required_options: list[str] = []
    optional_options: dict[str, Any] = {
        "master": "local[*]",
        "app_name": "rivet",
        "config": {},
        "spark_home": None,
        "packages": [],
        "connect_url": None,
    }
    credential_options: list[str] = []

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return PySparkComputeEngine(name=name, config=config)

    def validate(self, options: dict[str, Any]) -> None:
        from rivet_core.errors import PluginValidationError, plugin_error

        recognized = set(self.optional_options) | set(self.required_options)
        for key in options:
            if key not in recognized:
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"Unknown option '{key}' for pyspark engine.",
                        plugin_name="rivet_pyspark",
                        plugin_type="engine",
                        remediation=f"Valid options: {', '.join(sorted(recognized))}",
                        option=key,
                    )
                )

        def _fail(option: str, msg: str) -> None:
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"Invalid value for option '{option}' in pyspark engine: {msg}",
                    plugin_name="rivet_pyspark",
                    plugin_type="engine",
                    remediation=f"Check the expected type for '{option}'.",
                    option=option,
                )
            )

        if "master" in options and not isinstance(options["master"], str):
            _fail("master", "must be a string")
        if "app_name" in options and not isinstance(options["app_name"], str):
            _fail("app_name", "must be a string")
        if "config" in options and not isinstance(options["config"], dict):
            _fail("config", "must be a dict")
        if "spark_home" in options and options["spark_home"] is not None:
            if not isinstance(options["spark_home"], str):
                _fail("spark_home", "must be a string or None")
        if "packages" in options and not isinstance(options["packages"], list):
            _fail("packages", "must be a list")
        if "connect_url" in options and options["connect_url"] is not None:
            if not isinstance(options["connect_url"], str):
                _fail("connect_url", "must be a string or None")

    def execute_sql(
        self,
        engine: ComputeEngine,
        sql: str,
        input_tables: dict[str, pyarrow.Table],
    ) -> pyarrow.Table:
        """Execute SQL via PySpark session.

        Converts input Arrow tables to Spark DataFrames, registers them as
        temp views, executes the SQL, and returns the result as Arrow.
        """
        pyspark_engine: PySparkComputeEngine = engine  # type: ignore[assignment]
        session = pyspark_engine.get_session()
        from pyspark.sql.pandas.types import from_arrow_schema

        for name, table in input_tables.items():
            # Normalise RecordBatchReader → Table (Spark 4.0 compat)
            if isinstance(table, pyarrow.RecordBatchReader):
                table = table.read_all()
            # Convert Arrow schema to Spark schema so Spark doesn't have to
            # infer types from the pandas DataFrame (avoids CANNOT_DETERMINE_TYPE
            # errors for null / ambiguous columns).
            spark_schema = from_arrow_schema(table.schema)
            df = session.createDataFrame(table.to_pandas(), schema=spark_schema)
            df.createOrReplaceTempView(name)
        result_df = session.sql(sql)
        return SparkDataFrameMaterializedRef(result_df).to_arrow()

    def collect_metrics(self, execution_context: Any) -> Any:
        """Return PluginMetrics with query_planning, io, memory, parallelism, scan + pyspark extensions."""
        from rivet_core.metrics import (
            IOMetrics,
            MemoryMetrics,
            ParallelismMetrics,
            PluginMetrics,
            QueryPlanningMetrics,
            ScanMetrics,
        )

        ctx = execution_context if isinstance(execution_context, dict) else {}
        return PluginMetrics(
            well_known={
                "query_planning": QueryPlanningMetrics(
                    planning_time_ms=ctx.get("planning_time_ms"),
                    actual_rows=ctx.get("rows_out"),
                ),
                "io": IOMetrics(
                    bytes_read=ctx.get("bytes_read"),
                    bytes_written=ctx.get("bytes_written"),
                ),
                "memory": MemoryMetrics(
                    peak_bytes=ctx.get("peak_bytes"),
                    spilled_bytes=ctx.get("spilled_bytes"),
                ),
                "parallelism": ParallelismMetrics(
                    threads_used=ctx.get("threads_used"),
                ),
                "scan": ScanMetrics(
                    rows_scanned=ctx.get("rows_scanned"),
                    rows_filtered=ctx.get("rows_filtered"),
                ),
            },
            extensions={
                "pyspark.job_id": ctx.get("job_id"),
                "pyspark.stage_count": ctx.get("stage_count"),
                "pyspark.shuffle_bytes_written": ctx.get("shuffle_bytes_written"),
                "pyspark.connect_mode": ctx.get("connect_mode", False),
            },
            engine="pyspark",
        )
