"""Property-based tests: Spark Arrow Cache Conditional Reuse (Property 20).

Property 20: Spark Arrow Cache Conditional Reuse
  (a) After calling to_arrow(), row_count shall use the cached Arrow table's num_rows
      and NOT call self._df.count().
  (b) When row_count is called without a prior to_arrow(), it shall call self._df.count()
      and NOT trigger toArrow().

Validates: Requirements 17.1, 17.2, 17.3, 17.4
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_pyspark.engine import SparkDataFrameMaterializedRef

# ── Strategies ──────────────────────────────────────────────────────────────────

_column_name = st.text(
    alphabet=st.characters(whitelist_categories=("Ll",), whitelist_characters="_"),
    min_size=1,
    max_size=10,
)

_small_int_array = st.lists(st.integers(min_value=-1000, max_value=1000), min_size=1, max_size=50)


@st.composite
def arrow_tables(draw: st.DrawFn) -> pa.Table:
    """Generate a random Arrow table with 1-4 int64 columns and 1-50 rows."""
    n_cols = draw(st.integers(min_value=1, max_value=4))
    names = draw(st.lists(_column_name, min_size=n_cols, max_size=n_cols, unique=True))
    first_col = draw(_small_int_array)
    n_rows = len(first_col)
    columns = [first_col]
    for _ in range(n_cols - 1):
        columns.append(draw(st.lists(st.integers(min_value=-1000, max_value=1000), min_size=n_rows, max_size=n_rows)))
    return pa.table({name: col for name, col in zip(names, columns)})


def _make_mock_spark_df(arrow_table: pa.Table) -> MagicMock:
    """Create a mock PySpark DataFrame with toArrow() and count()."""
    mock_df = MagicMock()
    mock_df.toArrow.return_value = arrow_table
    mock_df.count.return_value = arrow_table.num_rows
    # schema.fields for the schema property
    mock_fields = []
    for f in arrow_table.schema:
        mock_field = MagicMock()
        mock_field.name = f.name
        mock_field.dataType = str(f.type)
        mock_field.nullable = f.nullable
        mock_fields.append(mock_field)
    mock_df.schema.fields = mock_fields
    return mock_df


# ── Property 20: Spark Arrow Cache Conditional Reuse ─────────────────────────────


@given(table=arrow_tables())
@settings(max_examples=100)
def test_property20_scenario_a_row_count_after_to_arrow_uses_cache(table: pa.Table) -> None:
    """Property 20a: After to_arrow(), row_count uses cached table — count() not called."""
    mock_df = _make_mock_spark_df(table)
    ref = SparkDataFrameMaterializedRef(mock_df)

    # Call to_arrow first to populate cache
    result = ref.to_arrow()
    assert result is table
    mock_df.toArrow.assert_called_once()

    # Now row_count should use cached table, not call count()
    rc = ref.row_count
    assert rc == table.num_rows
    mock_df.count.assert_not_called()


@given(table=arrow_tables())
@settings(max_examples=100)
def test_property20_scenario_b_row_count_without_to_arrow_calls_count(table: pa.Table) -> None:
    """Property 20b: Without prior to_arrow(), row_count calls count() — toArrow() not called."""
    mock_df = _make_mock_spark_df(table)
    ref = SparkDataFrameMaterializedRef(mock_df)

    # Call row_count without prior to_arrow
    rc = ref.row_count
    assert rc == table.num_rows
    mock_df.count.assert_called_once()
    mock_df.toArrow.assert_not_called()


@given(table=arrow_tables())
@settings(max_examples=100)
def test_property20_to_arrow_called_once_on_repeated_calls(table: pa.Table) -> None:
    """Property 20: repeated to_arrow() calls invoke toArrow() exactly once."""
    mock_df = _make_mock_spark_df(table)
    ref = SparkDataFrameMaterializedRef(mock_df)

    first = ref.to_arrow()
    second = ref.to_arrow()
    assert first is second
    mock_df.toArrow.assert_called_once()


@given(table=arrow_tables())
@settings(max_examples=100)
def test_property20_schema_does_not_trigger_arrow_conversion(table: pa.Table) -> None:
    """Property 20: schema uses _df.schema.fields — no Arrow conversion triggered."""
    mock_df = _make_mock_spark_df(table)
    ref = SparkDataFrameMaterializedRef(mock_df)

    _ = ref.schema
    mock_df.toArrow.assert_not_called()
    mock_df.count.assert_not_called()
