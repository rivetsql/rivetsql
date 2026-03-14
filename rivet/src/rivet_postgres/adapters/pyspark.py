"""PostgresPySparkAdapter: JDBC read/write for PostgreSQL catalogs via PySpark.

Simple writes (append, replace) use Spark JDBC. Complex writes (truncate_insert,
merge, delete_insert, incremental_append, scd2) use a psycopg3 side-channel that
materializes to Arrow and writes directly to PostgreSQL.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow

from rivet_core.async_utils import safe_run_async
from rivet_core.errors import ExecutionError, RivetError
from rivet_core.models import Column, Material, Schema
from rivet_core.optimizer import AdapterPushdownResult, Cast, PushdownPlan, ResidualPlan
from rivet_core.plugins import ComputeEngineAdapter
from rivet_core.strategies import MaterializedRef

if TYPE_CHECKING:
    from rivet_core.sql_parser import Predicate

ALL_6_CAPABILITIES = [
    "projection_pushdown",
    "predicate_pushdown",
    "limit_pushdown",
    "cast_pushdown",
    "join",
    "aggregation",
]

_JDBC_DRIVER = "org.postgresql.Driver"
_JDBC_MAVEN = "org.postgresql:postgresql:42.7.3"
_SIDE_CHANNEL_STRATEGIES = frozenset(
    {"truncate_insert", "merge", "delete_insert", "incremental_append", "scd2"}
)

_EMPTY_RESIDUAL = ResidualPlan(predicates=[], limit=None, casts=[])


def _apply_pyspark_pushdown(
    df: Any,
    pushdown: PushdownPlan | None,
) -> tuple[Any, ResidualPlan]:
    """Apply pushdown operations to a PySpark DataFrame.

    Returns (modified_df, residual) where residual contains any operations
    that could not be applied.
    """
    if pushdown is None:
        return df, _EMPTY_RESIDUAL

    residual_predicates: list[Predicate] = list(pushdown.predicates.residual)
    residual_casts: list[Cast] = list(pushdown.casts.residual)
    residual_limit: int | None = pushdown.limit.residual_limit

    if pushdown.projections.pushed_columns is not None:
        try:
            df = df.select(*pushdown.projections.pushed_columns)
        except Exception:
            pass

    for pred in pushdown.predicates.pushed:
        try:
            df = df.filter(pred.expression)
        except Exception:
            residual_predicates.append(pred)

    if pushdown.limit.pushed_limit is not None:
        try:
            df = df.limit(pushdown.limit.pushed_limit)
        except Exception:
            residual_limit = pushdown.limit.pushed_limit

    if pushdown.casts.pushed:
        from pyspark.sql import functions as F

        for cast in pushdown.casts.pushed:
            try:
                df = df.withColumn(cast.column, F.col(cast.column).cast(cast.to_type))
            except Exception:
                residual_casts.append(cast)

    return df, ResidualPlan(
        predicates=residual_predicates,
        limit=residual_limit,
        casts=residual_casts,
    )


class _SparkDataFrameMaterializedRef(MaterializedRef):
    """MaterializedRef backed by a Spark DataFrame. Materializes to Arrow on to_arrow()."""

    def __init__(self, df: Any) -> None:
        self._df = df

    def to_arrow(self) -> pyarrow.Table:
        if hasattr(self._df, "toArrow"):
            result = self._df.toArrow()
            if isinstance(result, pyarrow.RecordBatchReader):
                return result.read_all()
            return result
        return pyarrow.Table.from_pandas(self._df.toPandas())

    @property
    def schema(self) -> Schema:
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


def _check_jdbc_driver(session: Any) -> None:
    """Fail with RVT-505 if the PostgreSQL JDBC driver JAR is missing."""
    try:
        session._jvm.java.lang.Class.forName(_JDBC_DRIVER)
    except Exception:
        raise ExecutionError(  # noqa: B904
            RivetError(
                code="RVT-505",
                message="PostgreSQL JDBC driver JAR not found on Spark classpath.",
                context={"driver": _JDBC_DRIVER, "adapter": "PostgresPySparkAdapter"},
                remediation=(
                    f"Add the PostgreSQL JDBC driver to Spark via "
                    f"'spark.jars.packages' option: '{_JDBC_MAVEN}'."
                ),
            )
        )


def _build_jdbc_url(options: dict[str, Any]) -> str:
    """Build a JDBC URL from postgres catalog options."""
    host = options["host"]
    port = options.get("port", 5432)
    database = options["database"]
    ssl_mode = options.get("ssl_mode", "prefer")
    return f"jdbc:postgresql://{host}:{port}/{database}?sslmode={ssl_mode}"


def _build_jdbc_properties(options: dict[str, Any]) -> dict[str, str]:
    """Build JDBC connection properties from catalog options."""
    props: dict[str, str] = {"driver": _JDBC_DRIVER}
    if options.get("user"):
        props["user"] = options["user"]
    if options.get("password"):
        props["password"] = options["password"]
    return props


class PostgresPySparkAdapter(ComputeEngineAdapter):
    """Adapter enabling PySpark engine to read/write PostgreSQL catalogs via JDBC.

    Registered as catalog_plugin-contributed so it takes precedence over any
    engine_plugin adapter for the same (pyspark, postgres) pair.
    """

    target_engine_type = "pyspark"
    catalog_type = "postgres"
    capabilities = ALL_6_CAPABILITIES
    source = "catalog_plugin"
    source_plugin = "rivet_postgres"

    def read_dispatch(
        self, engine: Any, catalog: Any, joint: Any, pushdown: PushdownPlan | None = None
    ) -> AdapterPushdownResult:
        """Read from PostgreSQL via Spark JDBC with optional parallel partitioned reads."""
        session = engine.get_session()
        _check_jdbc_driver(session)

        url = _build_jdbc_url(catalog.options)
        props = _build_jdbc_properties(catalog.options)

        # Determine what to read: wrap SQL as subquery or use table reference
        sql = getattr(joint, "sql", None)
        table = getattr(joint, "table", None)
        if sql:
            dbtable = f"({sql}) AS _rivet_subquery"
        elif table:
            schema = catalog.options.get("schema", "public")
            dbtable = f"{schema}.{table}"
        else:
            schema = catalog.options.get("schema", "public")
            dbtable = f"{schema}.{joint.name}"

        # Check for parallel partitioned read options on the joint
        partition_column = getattr(joint, "jdbc_partition_column", None)
        lower_bound = getattr(joint, "jdbc_lower_bound", None)
        upper_bound = getattr(joint, "jdbc_upper_bound", None)
        num_partitions = getattr(joint, "jdbc_num_partitions", None)

        try:
            if partition_column and num_partitions:
                df = session.read.jdbc(
                    url=url,
                    table=dbtable,
                    column=partition_column,
                    lowerBound=int(lower_bound) if lower_bound is not None else 0,
                    upperBound=int(upper_bound) if upper_bound is not None else 1000000,
                    numPartitions=int(num_partitions),
                    properties=props,
                )
            else:
                df = session.read.jdbc(url=url, table=dbtable, properties=props)
        except Exception as exc:
            raise ExecutionError(
                RivetError(
                    code="RVT-501",
                    message=f"PostgreSQL JDBC read failed: {exc}",
                    context={
                        "host": catalog.options.get("host"),
                        "database": catalog.options.get("database"),
                    },
                    remediation="Check PostgreSQL credentials, host, and network connectivity.",
                )
            ) from exc

        df, residual = _apply_pyspark_pushdown(df, pushdown)

        ref = _SparkDataFrameMaterializedRef(df)
        material = Material(
            name=joint.name,
            catalog=catalog.name,
            materialized_ref=ref,
            state="deferred",
        )
        return AdapterPushdownResult(material=material, residual=residual)

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        """Write to PostgreSQL via Spark JDBC (append, replace) or psycopg3 side-channel (complex)."""
        strategy = getattr(joint, "write_strategy", None) or "replace"

        if strategy in _SIDE_CHANNEL_STRATEGIES:
            return _psycopg3_side_channel(catalog, joint, material, strategy)

        session = engine.get_session()
        _check_jdbc_driver(session)

        url = _build_jdbc_url(catalog.options)
        props = _build_jdbc_properties(catalog.options)

        # Resolve target table name
        table = getattr(joint, "table", None) or joint.name
        schema = catalog.options.get("schema", "public")
        dbtable = f"{schema}.{table}"

        # Map strategy to Spark JDBC write mode
        mode = "append" if strategy == "append" else "overwrite"

        # Materialize to Spark DataFrame
        arrow_table = material.materialized_ref.to_arrow()
        df = session.createDataFrame(arrow_table.to_pandas())

        try:
            df.write.jdbc(url=url, table=dbtable, mode=mode, properties=props)
        except Exception as exc:
            raise ExecutionError(
                RivetError(
                    code="RVT-501",
                    message=f"PostgreSQL JDBC write failed: {exc}",
                    context={
                        "host": catalog.options.get("host"),
                        "database": catalog.options.get("database"),
                    },
                    remediation="Check PostgreSQL credentials, host, and write permissions.",
                )
            ) from exc


# --- psycopg3 side-channel for complex write strategies ---


def _psycopg3_side_channel(catalog: Any, joint: Any, material: Any, strategy: str) -> None:
    """Execute complex write strategies via psycopg3 directly, bypassing Spark JDBC."""
    from rivet_postgres.sink import _build_conninfo, _execute_strategy

    arrow_table = material.materialized_ref.to_arrow()
    conninfo = _build_conninfo(catalog.options)
    table = getattr(joint, "table", None) or joint.name
    schema = catalog.options.get("schema", "public")
    qualified_table = f"{schema}.{table}"

    try:
        safe_run_async(_execute_strategy(conninfo, qualified_table, arrow_table, strategy, joint))
    except ExecutionError:
        raise
    except Exception as exc:
        raise ExecutionError(
            RivetError(
                code="RVT-501",
                message=f"PostgreSQL psycopg3 side-channel write failed: {exc}",
                context={
                    "host": catalog.options.get("host"),
                    "database": catalog.options.get("database"),
                    "strategy": strategy,
                },
                remediation="Check PostgreSQL credentials, host, and write permissions.",
            )
        ) from exc
