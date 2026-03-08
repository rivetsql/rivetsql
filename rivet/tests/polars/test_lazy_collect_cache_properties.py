"""Property-based tests: Polars LazyFrame Single Collect (Property 18).

Property 18: Polars LazyFrame Single Collect
  For any PolarsLazyMaterializedRef instance backed by a valid LazyFrame, calling
  any combination of to_arrow() and row_count N times (N >= 1) shall invoke
  .collect() exactly once. The schema property shall not trigger .collect().

Validates: Requirements 15.1, 15.2, 15.3, 15.4
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_polars.engine import PolarsLazyMaterializedRef

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


_materializing_accessor = st.sampled_from(["to_arrow", "row_count"])
_materializing_sequence = st.lists(_materializing_accessor, min_size=1, max_size=10)


def _make_mock_lazy_frame(arrow_table: pa.Table) -> MagicMock:
    """Create a mock LazyFrame whose .collect() returns a mock DataFrame."""
    mock_df = MagicMock()
    mock_df.to_arrow.return_value = arrow_table
    mock_df.height = arrow_table.num_rows

    mock_lf = MagicMock()
    mock_lf.collect.return_value = mock_df
    # collect_schema returns a dict-like of {name: dtype}
    schema_dict = {f.name: str(f.type) for f in arrow_table.schema}
    mock_lf.collect_schema.return_value = schema_dict
    return mock_lf


# ── Property 18: Polars LazyFrame Single Collect ─────────────────────────────────


@given(table=arrow_tables(), accessors=_materializing_sequence)
@settings(max_examples=100)
def test_property18_collect_called_exactly_once(table: pa.Table, accessors: list[str]) -> None:
    """Property 18: .collect() is called exactly once regardless of to_arrow/row_count call count."""
    mock_lf = _make_mock_lazy_frame(table)
    ref = PolarsLazyMaterializedRef(mock_lf, streaming=False)

    for accessor in accessors:
        if accessor == "to_arrow":
            ref.to_arrow()
        elif accessor == "row_count":
            _ = ref.row_count

    mock_lf.collect.assert_called_once()


@given(table=arrow_tables())
@settings(max_examples=100)
def test_property18_to_arrow_consistent_with_table(table: pa.Table) -> None:
    """Property 18: to_arrow() returns the Arrow table from the collected DataFrame."""
    mock_lf = _make_mock_lazy_frame(table)
    ref = PolarsLazyMaterializedRef(mock_lf, streaming=False)

    result = ref.to_arrow()
    assert result is table


@given(table=arrow_tables())
@settings(max_examples=100)
def test_property18_row_count_consistent_with_table(table: pa.Table) -> None:
    """Property 18: row_count returns height from the collected DataFrame."""
    mock_lf = _make_mock_lazy_frame(table)
    ref = PolarsLazyMaterializedRef(mock_lf, streaming=False)

    assert ref.row_count == table.num_rows


@given(table=arrow_tables())
@settings(max_examples=100)
def test_property18_schema_does_not_trigger_collect(table: pa.Table) -> None:
    """Property 18: schema property uses collect_schema() and does not trigger .collect()."""
    mock_lf = _make_mock_lazy_frame(table)
    ref = PolarsLazyMaterializedRef(mock_lf, streaming=False)

    _ = ref.schema
    mock_lf.collect.assert_not_called()
    mock_lf.collect_schema.assert_called()
