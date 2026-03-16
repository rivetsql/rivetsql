"""Integration tests for arrow_converter with a real PySpark session.

Exercises real Spark ↔ Arrow conversions through the pandas fallback path
(Spark 3.x does not support native Arrow APIs). Skipped when PySpark is
not available.

Validates: Requirements 5.1, 5.2
"""

from __future__ import annotations

import pyarrow as pa
import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from rivet_pyspark.arrow_converter import (
    arrow_to_spark,
    clear_cache,
    spark_to_arrow,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Create a local Spark session for the test module."""
    try:
        return (
            SparkSession.builder.master("local[1]")
            .appName("arrow_converter_integration")
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config(
                "spark.driver.extraJavaOptions",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
                "--add-opens=java.base/java.nio=ALL-UNNAMED",
            )
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"Spark unavailable: {exc}")


@pytest.fixture(autouse=True)
def _clear_cache() -> None:
    clear_cache()


def _round_trip_arrow(spark: SparkSession, table: pa.Table) -> pa.Table:
    """Arrow → Spark → Arrow round-trip."""
    sdf = arrow_to_spark(table, spark)
    return spark_to_arrow(sdf, spark)


# ---------------------------------------------------------------------------
# Round-trip: Arrow → Spark → Arrow
# ---------------------------------------------------------------------------


class TestRoundTripColumnTypes:
    """Round-trip Arrow → Spark → Arrow preserves data for various types."""

    def test_int_columns(self, spark: SparkSession) -> None:
        table = pa.table({"i64": pa.array([10, 20, 30], type=pa.int64())})
        result = _round_trip_arrow(spark, table)
        assert result.column("i64").to_pylist() == [10, 20, 30]

    def test_string_column(self, spark: SparkSession) -> None:
        table = pa.table({"s": pa.array(["a", "bb", "ccc"], type=pa.string())})
        result = _round_trip_arrow(spark, table)
        assert result.column("s").to_pylist() == ["a", "bb", "ccc"]

    def test_float_column(self, spark: SparkSession) -> None:
        table = pa.table({"f": pa.array([1.5, 2.5, 3.5], type=pa.float64())})
        result = _round_trip_arrow(spark, table)
        assert result.column("f").to_pylist() == pytest.approx([1.5, 2.5, 3.5])

    def test_boolean_column(self, spark: SparkSession) -> None:
        table = pa.table({"b": pa.array([True, False, True], type=pa.bool_())})
        result = _round_trip_arrow(spark, table)
        assert result.column("b").to_pylist() == [True, False, True]

    def test_mixed_types(self, spark: SparkSession) -> None:
        table = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "name": pa.array(["alice", "bob"], type=pa.string()),
                "score": pa.array([9.5, 8.0], type=pa.float64()),
                "active": pa.array([True, False], type=pa.bool_()),
            }
        )
        result = _round_trip_arrow(spark, table)
        assert result.column_names == ["id", "name", "score", "active"]
        assert result.column("name").to_pylist() == ["alice", "bob"]


# ---------------------------------------------------------------------------
# Schema equivalence
# ---------------------------------------------------------------------------


class TestSchemaEquivalence:
    """Conversion preserves schema structure."""

    def test_schema_column_names_match(self, spark: SparkSession) -> None:
        table = pa.table(
            {
                "a": pa.array([1], type=pa.int64()),
                "b": pa.array(["x"], type=pa.string()),
                "c": pa.array([1.0], type=pa.float64()),
            }
        )
        result = _round_trip_arrow(spark, table)
        assert result.schema.names == table.schema.names

    def test_schema_types_preserved(self, spark: SparkSession) -> None:
        table = pa.table(
            {
                "i": pa.array([1], type=pa.int64()),
                "f": pa.array([1.0], type=pa.float64()),
                "b": pa.array([True], type=pa.bool_()),
                "s": pa.array(["x"], type=pa.string()),
            }
        )
        result = _round_trip_arrow(spark, table)
        for field in table.schema:
            result_field = result.schema.field(field.name)
            assert result_field is not None, f"Missing field: {field.name}"


# ---------------------------------------------------------------------------
# Nullability preservation
# ---------------------------------------------------------------------------


class TestNullabilityPreservation:
    """Nullability metadata survives the round-trip."""

    def test_nullable_column_stays_nullable(self, spark: SparkSession) -> None:
        field = pa.field("x", pa.int64(), nullable=True)
        table = pa.table(
            {"x": pa.array([1, 2, 3], type=pa.int64())},
            schema=pa.schema([field]),
        )
        result = _round_trip_arrow(spark, table)
        assert result.schema.field("x").nullable is True

    def test_null_values_round_trip(self, spark: SparkSession) -> None:
        """String nulls survive the round-trip (avoids pandas int-null issues)."""
        table = pa.table({"s": pa.array(["a", None, "c"], type=pa.string())})
        result = _round_trip_arrow(spark, table)
        assert result.column("s").to_pylist() == ["a", None, "c"]
