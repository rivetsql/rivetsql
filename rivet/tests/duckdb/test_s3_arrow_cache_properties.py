"""Property-based tests: S3 Arrow Cache Idempotence and Error Non-Caching (Properties 1 & 2).

Property 1: S3 Arrow Cache Idempotence
  For any _S3DuckDBMaterializedRef instance backed by a valid S3 query, calling any
  combination of to_arrow(), schema, and row_count N times (N >= 1) shall invoke the
  underlying _execute() method exactly once, and all accessors shall return values
  consistent with the single fetched Arrow table.

Property 2: S3 Cache Error Non-Caching
  For any _S3DuckDBMaterializedRef whose _execute() raises an exception, calling
  to_arrow() shall propagate the exception and leave the internal cache empty, so that
  a subsequent call to to_arrow() retries _execute() rather than returning a stale or
  partial result.
"""

from __future__ import annotations

import re
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_duckdb.adapters.s3 import _S3DuckDBMaterializedRef

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
    names = draw(
        st.lists(_column_name, min_size=n_cols, max_size=n_cols, unique=True)
    )
    first_col = draw(_small_int_array)
    n_rows = len(first_col)
    columns = [first_col]
    for _ in range(n_cols - 1):
        columns.append(draw(st.lists(st.integers(min_value=-1000, max_value=1000), min_size=n_rows, max_size=n_rows)))
    return pa.table({name: col for name, col in zip(names, columns)})


_accessor_choice = st.sampled_from(["to_arrow", "schema", "row_count"])

_accessor_sequence = st.lists(_accessor_choice, min_size=1, max_size=10)


def _make_ref() -> _S3DuckDBMaterializedRef:
    """Create a ref instance with dummy options (we mock _execute)."""
    return _S3DuckDBMaterializedRef(
        catalog_options={"bucket": "test-bucket", "region": "us-east-1"},
        sql="SELECT 1",
        table=None,
    )


# ── Property 1: S3 Arrow Cache Idempotence ──────────────────────────────────────


@given(table=arrow_tables(), accessors=_accessor_sequence)
@settings(max_examples=100)
def test_property1_execute_called_exactly_once(table: pa.Table, accessors: list[str]) -> None:
    """Property 1: _execute() is called exactly once regardless of accessor call count."""
    ref = _make_ref()
    with patch.object(ref, "_execute", return_value=table) as mock_exec:
        for accessor in accessors:
            if accessor == "to_arrow":
                ref.to_arrow()
            elif accessor == "schema":
                _ = ref.schema
            elif accessor == "row_count":
                _ = ref.row_count

        mock_exec.assert_called_once()


@given(table=arrow_tables())
@settings(max_examples=100)
def test_property1_to_arrow_returns_same_object(table: pa.Table) -> None:
    """Property 1: repeated to_arrow() calls return the exact same table object."""
    ref = _make_ref()
    with patch.object(ref, "_execute", return_value=table):
        first = ref.to_arrow()
        second = ref.to_arrow()
        assert first is second


@given(table=arrow_tables())
@settings(max_examples=100)
def test_property1_schema_consistent_with_table(table: pa.Table) -> None:
    """Property 1: schema accessor returns columns matching the cached Arrow table."""
    ref = _make_ref()
    with patch.object(ref, "_execute", return_value=table):
        schema = ref.schema
        assert len(schema.columns) == len(table.schema)
        for col, field in zip(schema.columns, table.schema):
            assert col.name == field.name


@given(table=arrow_tables())
@settings(max_examples=100)
def test_property1_row_count_consistent_with_table(table: pa.Table) -> None:
    """Property 1: row_count accessor returns num_rows from the cached Arrow table."""
    ref = _make_ref()
    with patch.object(ref, "_execute", return_value=table):
        assert ref.row_count == table.num_rows


# ── Property 2: S3 Cache Error Non-Caching ──────────────────────────────────────


@given(data=st.data())
@settings(max_examples=100)
def test_property2_exception_propagates_and_cache_stays_none(data: st.DataObject) -> None:
    """Property 2: _execute() exception propagates and _cached_table remains None."""
    error_msg = data.draw(st.text(min_size=1, max_size=50))
    ref = _make_ref()
    with patch.object(ref, "_execute", side_effect=RuntimeError(error_msg)):
        with pytest.raises(RuntimeError, match=re.escape(error_msg)):
            ref.to_arrow()
        assert ref._cached_table is None


@given(table=arrow_tables())
@settings(max_examples=100)
def test_property2_retry_after_error_succeeds(table: pa.Table) -> None:
    """Property 2: after an error, a subsequent call retries _execute() and succeeds."""
    ref = _make_ref()
    mock_exec = MagicMock(side_effect=[RuntimeError("transient"), table])
    with patch.object(ref, "_execute", mock_exec):
        with pytest.raises(RuntimeError):
            ref.to_arrow()
        assert ref._cached_table is None

        result = ref.to_arrow()
        assert result is table
        assert ref._cached_table is table
        assert mock_exec.call_count == 2
