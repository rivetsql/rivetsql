"""Core data models: Catalog, ComputeEngine, Column, Schema, Joint, Material."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pyarrow

from rivet_core.errors import ExecutionError, RivetError

if TYPE_CHECKING:
    from rivet_core.checks import Assertion
    from rivet_core.strategies import MaterializedRef


@dataclass(frozen=True)
class Catalog:
    """Immutable named data domain where data resides.

    Pure metadata — no execution logic. Options are opaque and validated by the
    corresponding CatalogPlugin.
    """

    name: str
    type: str
    options: dict[str, Any] = field(default_factory=dict)


@dataclass
class ComputeEngine:
    """Named execution backend instance.

    engine_type is set by the plugin and is immutable after creation.
    Instance names must be globally unique within the compute engine namespace.
    """

    name: str
    engine_type: str
    config: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Column:
    """A single column in a Schema."""

    name: str
    type: str  # Arrow type name: "int64", "utf8", "float64", "bool", "timestamp[us]", etc.
    nullable: bool


@dataclass(frozen=True)
class Schema:
    """Describes the structure of a Material with column names, types, and nullability."""

    columns: list[Column]


JOINT_TYPES = frozenset({"source", "sql", "sink", "python"})


@dataclass
class Joint:
    """Declarative node in the DAG representing a computation.

    Immutable metadata only — no execution logic.

    joint_type must be one of: "source", "sql", "sink", "python"
    Upstream constraints are enforced by Assembly:
      - source: no upstream
      - sql: zero or more upstream
      - python: explicit upstream required
      - sink: at least one upstream
    """

    name: str
    joint_type: str
    catalog: str | None = None
    upstream: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    description: str | None = None
    assertions: list[Assertion] = field(default_factory=list)
    path: str | None = None
    sql: str | None = None
    engine: str | None = None
    eager: bool = False
    table: str | None = None
    write_strategy: str | None = None
    function: str | None = None
    source_file: str | None = None
    dialect: str | None = None
    fusion_strategy_override: str | None = None
    materialization_strategy_override: str | None = None

    def __post_init__(self) -> None:
        if self.joint_type not in JOINT_TYPES:
            raise ValueError(
                f"Invalid joint_type: {self.joint_type!r}. Must be one of {sorted(JOINT_TYPES)}"
            )


@dataclass
class Material:
    """Data flowing between joints, either deferred or materialized.

    States:
      - deferred: logical plan, no physical data
      - materialized: backed by a MaterializedRef
      - retained: execution complete, kept per policy
      - evicted: data dropped, metadata preserved, .to_arrow() fails
    """

    name: str
    catalog: str
    table: str | None = None
    schema: dict[str, str] | None = None  # column_name → arrow_type_str
    state: str = "deferred"
    materialized_ref: MaterializedRef | None = None

    def _require_ref(self) -> MaterializedRef:
        if self.state == "evicted":
            raise ExecutionError(
                RivetError(
                    code="RVT-401",
                    message=f"Material '{self.name}' has been evicted and data is no longer accessible.",
                    remediation="Retain the material or re-execute the pipeline to access data.",
                )
            )
        if self.materialized_ref is None:
            raise ExecutionError(
                RivetError(
                    code="RVT-401",
                    message=f"Material '{self.name}' is in '{self.state}' state with no materialized data.",
                    remediation="Materialize the material before accessing data.",
                )
            )
        return self.materialized_ref

    def to_arrow(self) -> pyarrow.Table:
        """Return data as a PyArrow Table."""
        return self._require_ref().to_arrow()

    def to_pandas(self) -> Any:
        """Return data as a pandas DataFrame. Raises ImportError if pandas is not installed."""
        try:
            import pandas  # noqa: F401, F811
        except ImportError:
            raise ImportError(  # noqa: B904
                "pandas is required for .to_pandas(). Install it with: pip install pandas"
            )
        ref = self._require_ref()
        # Zero-copy: if the ref already holds a pandas DataFrame, return it directly
        if hasattr(ref, "_pandas_df"):
            return ref._pandas_df
        return self.to_arrow().to_pandas()

    def to_polars(self) -> Any:
        """Return data as a polars DataFrame. Raises ImportError if polars is not installed."""
        try:
            import polars  # noqa: F811
        except ImportError:
            raise ImportError(  # noqa: B904
                "polars is required for .to_polars(). Install it with: pip install polars"
            )
        ref = self._require_ref()
        if hasattr(ref, "_polars_df"):
            return ref._polars_df
        return polars.from_arrow(self.to_arrow())

    def to_duckdb(self) -> Any:
        """Return data as a DuckDB relation. Raises ImportError if duckdb is not installed."""
        try:
            import duckdb  # noqa: F811
        except ImportError:
            raise ImportError(  # noqa: B904
                "duckdb is required for .to_duckdb(). Install it with: pip install duckdb"
            )
        ref = self._require_ref()
        if hasattr(ref, "_duckdb_rel"):
            return ref._duckdb_rel
        return duckdb.from_arrow(self.to_arrow())

    def to_spark(self) -> Any:
        """Return data as a PySpark DataFrame. Raises ImportError if pyspark is not installed."""
        try:
            import pyspark.sql  # noqa: F401, F811
        except ImportError:
            raise ImportError(  # noqa: B904
                "pyspark is required for .to_spark(). Install it with: pip install pyspark"
            )
        ref = self._require_ref()
        if hasattr(ref, "_spark_df"):
            return ref._spark_df
        # Spark conversion requires an active SparkSession
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No active SparkSession found for Arrow-to-Spark conversion.")
        return spark.createDataFrame(self.to_arrow().to_pandas())

    @property
    def columns(self) -> list[str]:
        """Column names from schema or materialized data."""
        if self.schema is not None:
            return list(self.schema.keys())
        if self.materialized_ref is not None and self.state != "evicted":
            return [c.name for c in self.materialized_ref.schema.columns]
        return []

    @property
    def num_rows(self) -> int:
        """Number of rows in the materialized data."""
        return self._require_ref().row_count
