"""Property-based tests: Unity Polars Arrow Cache Idempotence (Property 17).

Property 17: Unity Polars Arrow Cache Idempotence
  For any _PolarsUnityMaterializedRef instance backed by a valid Unity storage
  location, calling any combination of to_arrow(), schema, and row_count N times
  (N >= 1) shall invoke the underlying _read() method exactly once, and all
  accessors shall return values consistent with the single fetched Arrow table.

Validates: Requirements 14.1, 14.2, 14.3, 14.4
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_polars.adapters.unity import _PolarsUnityMaterializedRef

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


_accessor_choice = st.sampled_from(["to_arrow", "schema", "row_count"])
_accessor_sequence = st.lists(_accessor_choice, min_size=1, max_size=10)


def _make_ref() -> _PolarsUnityMaterializedRef:
    """Create a ref instance with dummy options (we mock _read)."""
    return _PolarsUnityMaterializedRef(
        storage_location="s3://bucket/path",
        file_format="parquet",
        storage_options={},
    )


def _mock_polars_df(arrow_table: pa.Table) -> MagicMock:
    """Create a mock Polars DataFrame whose .to_arrow() returns the given table."""
    mock_df = MagicMock()
    mock_df.to_arrow.return_value = arrow_table
    return mock_df


# ── Property 17: Unity Polars Arrow Cache Idempotence ────────────────────────────


@given(table=arrow_tables(), accessors=_accessor_sequence)
@settings(max_examples=100)
def test_property17_read_called_exactly_once(table: pa.Table, accessors: list[str]) -> None:
    """Property 17: _read() is called exactly once regardless of accessor call count."""
    ref = _make_ref()
    mock_df = _mock_polars_df(table)
    with patch.object(ref, "_read", return_value=mock_df) as mock_read:
        for accessor in accessors:
            if accessor == "to_arrow":
                ref.to_arrow()
            elif accessor == "schema":
                _ = ref.schema
            elif accessor == "row_count":
                _ = ref.row_count

        mock_read.assert_called_once()


@given(table=arrow_tables())
@settings(max_examples=100)
def test_property17_to_arrow_returns_same_object(table: pa.Table) -> None:
    """Property 17: repeated to_arrow() calls return the exact same table object."""
    ref = _make_ref()
    mock_df = _mock_polars_df(table)
    with patch.object(ref, "_read", return_value=mock_df):
        first = ref.to_arrow()
        second = ref.to_arrow()
        assert first is second


@given(table=arrow_tables())
@settings(max_examples=100)
def test_property17_schema_consistent_with_table(table: pa.Table) -> None:
    """Property 17: schema accessor returns columns matching the cached Arrow table."""
    ref = _make_ref()
    mock_df = _mock_polars_df(table)
    with patch.object(ref, "_read", return_value=mock_df):
        schema = ref.schema
        assert len(schema.columns) == len(table.schema)
        for col, field in zip(schema.columns, table.schema):
            assert col.name == field.name


@given(table=arrow_tables())
@settings(max_examples=100)
def test_property17_row_count_consistent_with_table(table: pa.Table) -> None:
    """Property 17: row_count accessor returns num_rows from the cached Arrow table."""
    ref = _make_ref()
    mock_df = _mock_polars_df(table)
    with patch.object(ref, "_read", return_value=mock_df):
        assert ref.row_count == table.num_rows
