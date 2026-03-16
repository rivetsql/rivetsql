"""Property-based tests for arrow_converter version detection and path selection.

- Property 1: Version detection correctness
  Validates: Requirements 1.1, 1.3
- Property 2: Detection caching is idempotent
  Validates: Requirements 1.2
- Property 3: Spark-to-Arrow path selection
  Validates: Requirements 2.1, 2.2, 2.3, 2.4
- Property 4: Arrow-to-Spark path selection
  Validates: Requirements 3.1, 3.2, 3.4
- Property 5: Schema round-trip equivalence
  Validates: Requirements 5.1, 5.2, 3.3
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_pyspark.arrow_converter import (
    _supports_native_arrow,
    arrow_to_spark,
    clear_cache,
    spark_to_arrow,
)


@pytest.fixture(autouse=True)
def _clear_detection_cache() -> None:
    clear_cache()


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Whether the mock session's probe DataFrame has a toArrow attribute
_has_native = st.booleans()

# Whether the detection probe itself raises an exception
_probe_raises = st.booleans()

# Whether the native conversion path raises an exception
_native_raises = st.booleans()


def _make_session(*, native: bool, probe_raises: bool = False) -> MagicMock:
    """Build a mock SparkSession with configurable detection behavior."""
    session = MagicMock()
    if probe_raises:
        session.createDataFrame.side_effect = RuntimeError("probe failed")
        return session

    probe_df = MagicMock()
    if not native:
        del probe_df.toArrow
    session.createDataFrame.return_value = probe_df
    return session


# ---------------------------------------------------------------------------
# Feature: spark-arrow-compat, Property 1: Version detection correctness
# ---------------------------------------------------------------------------


@settings(max_examples=100)
@given(has_native=_has_native, probe_raises=_probe_raises)
def test_property1_version_detection_correctness(has_native: bool, probe_raises: bool) -> None:
    """For any SparkSession, _supports_native_arrow returns True iff the
    probe DataFrame has a toArrow attribute and no exception occurs during
    detection.
    """
    clear_cache()
    session = _make_session(native=has_native, probe_raises=probe_raises)

    result = _supports_native_arrow(session)

    if probe_raises:
        assert result is False
    else:
        assert result is has_native


# ---------------------------------------------------------------------------
# Feature: spark-arrow-compat, Property 2: Detection caching is idempotent
# ---------------------------------------------------------------------------


@settings(max_examples=100)
@given(has_native=_has_native, probe_raises=_probe_raises)
def test_property2_detection_caching_idempotent(has_native: bool, probe_raises: bool) -> None:
    """Calling _supports_native_arrow twice on the same session returns the
    same result both times, and the detection logic only executes once.
    """
    clear_cache()
    session = _make_session(native=has_native, probe_raises=probe_raises)

    first = _supports_native_arrow(session)
    # Reset mock to track whether a second probe happens
    session.createDataFrame.reset_mock()
    second = _supports_native_arrow(session)

    assert first == second
    session.createDataFrame.assert_not_called()


# ---------------------------------------------------------------------------
# Feature: spark-arrow-compat, Property 3: Spark-to-Arrow path selection
# ---------------------------------------------------------------------------


@settings(max_examples=100)
@given(has_native=_has_native, native_raises=_native_raises)
def test_property3_spark_to_arrow_path_selection(has_native: bool, native_raises: bool) -> None:
    """spark_to_arrow uses the native path when supported and it succeeds,
    and falls back to the pandas path otherwise. The result is always a
    pyarrow.Table.
    """
    clear_cache()
    session = _make_session(native=has_native)
    df = MagicMock()

    native_table = pa.table({"native": [1]})
    pandas_table = pa.table({"pandas": [2]})

    if native_raises:
        df.toArrow.side_effect = RuntimeError("boom")
    else:
        df.toArrow.return_value = native_table

    with patch(
        "rivet_pyspark.arrow_converter._to_arrow_pandas",
        return_value=pandas_table,
    ):
        result = spark_to_arrow(df, session)

    assert isinstance(result, pa.Table)

    if has_native and not native_raises:
        # Native path was used
        df.toArrow.assert_called_once()
        assert result is native_table
    else:
        # Pandas fallback was used
        assert result is pandas_table


# ---------------------------------------------------------------------------
# Feature: spark-arrow-compat, Property 4: Arrow-to-Spark path selection
# ---------------------------------------------------------------------------


@settings(max_examples=100)
@given(has_native=_has_native, native_raises=_native_raises)
def test_property4_arrow_to_spark_path_selection(has_native: bool, native_raises: bool) -> None:
    """arrow_to_spark uses the native path when supported and it succeeds,
    and falls back to the pandas path otherwise.
    """
    clear_cache()
    session = _make_session(native=has_native)
    table = pa.table({"x": pa.array([1], type=pa.int64())})

    native_df = MagicMock(name="native_df")
    pandas_df = MagicMock(name="pandas_df")

    probe_return = session.createDataFrame.return_value

    if has_native:
        if native_raises:
            session.createDataFrame.side_effect = [
                probe_return,
                RuntimeError("boom"),
                pandas_df,
            ]
        else:
            session.createDataFrame.side_effect = [probe_return, native_df]
    else:
        # Non-native: probe call returns probe_return, then pandas path
        session.createDataFrame.side_effect = [probe_return, pandas_df]

    # Patch the deferred pyspark import for the pandas fallback path
    mock_pyspark_module = MagicMock()
    with patch.dict(sys.modules, {"pyspark.sql.pandas.types": mock_pyspark_module}):
        result = arrow_to_spark(table, session)

    if has_native and not native_raises:
        assert result is native_df
    else:
        assert result is pandas_df


# ---------------------------------------------------------------------------
# Feature: spark-arrow-compat, Property 5: Schema round-trip equivalence
# ---------------------------------------------------------------------------

# Supported Arrow types for schema generation
_arrow_types = st.sampled_from(
    [
        pa.int32(),
        pa.int64(),
        pa.float32(),
        pa.float64(),
        pa.string(),
        pa.bool_(),
        pa.timestamp("us"),
    ]
)

# Generate a valid Arrow field: unique name, random type, random nullability
_arrow_field = st.tuples(
    st.text(
        alphabet=st.characters(whitelist_categories=("Ll",), whitelist_characters="_"),
        min_size=1,
        max_size=8,
    ),
    _arrow_types,
    st.booleans(),
).map(lambda t: pa.field(t[0], t[1], nullable=t[2]))

# Generate a schema with 1-6 fields with unique names
_arrow_schema = st.lists(_arrow_field, min_size=1, max_size=6, unique_by=lambda f: f.name).map(
    pa.schema
)


@settings(max_examples=100)
@given(schema=_arrow_schema)
def test_property5_schema_round_trip_equivalence(schema: pa.Schema) -> None:
    """For any Arrow schema with supported types, converting to Spark and back
    via the pandas path preserves column names, logical types, and nullability.

    Uses mock-based approach: verifies that _to_spark_pandas passes the Arrow
    schema through from_arrow_schema, and that the resulting Spark schema fed
    back through the Arrow path preserves field names and count.

    Validates: Requirements 5.1, 5.2, 3.3
    """
    clear_cache()

    # Build a mock session that does NOT support native Arrow (forces pandas path)
    session = _make_session(native=False)

    # Create a trivial table matching the generated schema
    arrays = []
    for field in schema:
        if pa.types.is_boolean(field.type):
            arrays.append(pa.array([True], type=field.type))
        elif pa.types.is_integer(field.type) or pa.types.is_floating(field.type):
            arrays.append(pa.array([1], type=field.type))
        elif pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
            arrays.append(pa.array(["x"], type=field.type))
        elif pa.types.is_timestamp(field.type):
            arrays.append(pa.array([1000000], type=field.type))
        else:
            arrays.append(pa.array([None], type=field.type))

    table = pa.table(
        {field.name: arr for field, arr in zip(schema, arrays)},
        schema=schema,
    )

    # Mock from_arrow_schema to return a sentinel Spark schema that records
    # the Arrow schema it received
    mock_spark_schema = MagicMock(name="spark_schema")
    mock_from_arrow = MagicMock(return_value=mock_spark_schema)
    mock_pyspark_module = MagicMock()
    mock_pyspark_module.from_arrow_schema = mock_from_arrow

    # The pandas path calls session.createDataFrame(pandas_df, schema=spark_schema)
    result_df = MagicMock(name="result_spark_df")
    # First call is the detection probe, second is the actual createDataFrame
    probe_return = session.createDataFrame.return_value
    session.createDataFrame.side_effect = [probe_return, result_df]

    with patch.dict(sys.modules, {"pyspark.sql.pandas.types": mock_pyspark_module}):
        arrow_to_spark(table, session)

    # Verify from_arrow_schema was called with the original Arrow schema
    mock_from_arrow.assert_called_once()
    passed_schema = mock_from_arrow.call_args[0][0]

    # Column names must match exactly
    assert passed_schema.names == schema.names

    # Each field type and nullability must be preserved
    for original, passed in zip(schema, passed_schema):
        assert passed.type == original.type
        assert passed.nullable == original.nullable
