"""Property-based tests: Glue DuckDB Arrow Cache Idempotence (Property 19).

Property 19: Glue DuckDB Arrow Cache Idempotence
  For any GlueDuckDBMaterializedRef instance backed by a valid Glue table, calling
  any combination of to_arrow(), schema, and row_count N times (N >= 1) shall invoke
  the underlying _execute() method exactly once, and all accessors shall return values
  consistent with the single fetched Arrow table.

Validates: Requirements 16.1, 16.2, 16.3, 16.4
"""

from __future__ import annotations

from unittest.mock import patch

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_duckdb.adapters.glue import GlueDuckDBMaterializedRef

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


def _make_ref() -> GlueDuckDBMaterializedRef:
    """Create a ref instance with dummy options (we mock _execute)."""
    return GlueDuckDBMaterializedRef(
        catalog_options={"bucket": "test-bucket", "region": "us-east-1"},
        table_name="test_table",
        partition_filter=None,
        engine_config=None,
    )


# ── Property 19: Glue DuckDB Arrow Cache Idempotence ────────────────────────────


@given(table=arrow_tables(), accessors=_accessor_sequence)
@settings(max_examples=100)
def test_property19_execute_called_exactly_once(table: pa.Table, accessors: list[str]) -> None:
    """Property 19: _execute() is called exactly once regardless of accessor call count."""
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
def test_property19_to_arrow_returns_same_object(table: pa.Table) -> None:
    """Property 19: repeated to_arrow() calls return the exact same table object."""
    ref = _make_ref()
    with patch.object(ref, "_execute", return_value=table):
        first = ref.to_arrow()
        second = ref.to_arrow()
        assert first is second


@given(table=arrow_tables())
@settings(max_examples=100)
def test_property19_schema_consistent_with_table(table: pa.Table) -> None:
    """Property 19: schema accessor returns columns matching the cached Arrow table."""
    ref = _make_ref()
    with patch.object(ref, "_execute", return_value=table):
        schema = ref.schema
        assert len(schema.columns) == len(table.schema)
        for col, field in zip(schema.columns, table.schema):
            assert col.name == field.name


@given(table=arrow_tables())
@settings(max_examples=100)
def test_property19_row_count_consistent_with_table(table: pa.Table) -> None:
    """Property 19: row_count accessor returns num_rows from the cached Arrow table."""
    ref = _make_ref()
    with patch.object(ref, "_execute", return_value=table):
        assert ref.row_count == table.num_rows
