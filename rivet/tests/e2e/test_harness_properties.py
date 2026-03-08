"""Property-based tests for the E2E test harness helpers.

Covers Property 1 (write/read sink round-trip) from the design document.

**Validates: Requirements 1.3, 2.4**
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pcsv
from hypothesis import given, settings
from hypothesis import strategies as st

from .conftest import read_sink_csv

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Column name: simple ASCII identifiers (CSV-safe, no commas/quotes/newlines)
_col_name_st = st.text(
    alphabet=st.characters(categories=("Ll",)),
    min_size=1,
    max_size=10,
).map(lambda s: "col_" + s)

# Value strategies per type — constrained to survive CSV round-trip faithfully
_int_value_st = st.integers(min_value=-(2**31), max_value=2**31 - 1)
_float_value_st = st.floats(
    min_value=-1e15,
    max_value=1e15,
    allow_nan=False,
    allow_infinity=False,
    allow_subnormal=False,
)
_str_value_st = st.text(
    alphabet=st.characters(
        categories=("L", "N", "Zs"),
        exclude_characters="\x00\n\r,\"",
    ),
    min_size=1,
    max_size=30,
).filter(
    # Exclude strings that look numeric (e.g. "00", "123", "3.14") because
    # CSV readers auto-detect types and would parse them as int/float,
    # breaking the string round-trip.
    lambda s: not s.strip().replace(".", "", 1).replace("-", "", 1).replace("+", "", 1).isdigit()
    and len(s.strip()) > 0
)

# A column strategy: pick a type and generate a list of values
_column_st = st.one_of(
    st.tuples(st.just("int"), st.lists(_int_value_st, min_size=1, max_size=10)),
    st.tuples(st.just("float"), st.lists(_float_value_st, min_size=1, max_size=10)),
    st.tuples(st.just("str"), st.lists(_str_value_st, min_size=1, max_size=10)),
)


@st.composite
def pyarrow_table_st(draw: st.DrawFn) -> pa.Table:
    """Generate a random PyArrow table with 1-5 columns and uniform row count."""
    num_cols = draw(st.integers(min_value=1, max_value=5))

    # Generate unique column names
    col_names = draw(
        st.lists(_col_name_st, min_size=num_cols, max_size=num_cols, unique=True)
    )

    # Pick a consistent row count for all columns
    row_count = draw(st.integers(min_value=1, max_value=10))

    columns: dict[str, list] = {}
    for name in col_names:
        col_type, _ = draw(_column_st)
        if col_type == "int":
            values = draw(st.lists(_int_value_st, min_size=row_count, max_size=row_count))
        elif col_type == "float":
            values = draw(st.lists(_float_value_st, min_size=row_count, max_size=row_count))
        else:
            values = draw(st.lists(_str_value_st, min_size=row_count, max_size=row_count))
        columns[name] = values

    return pa.table(columns)


# ---------------------------------------------------------------------------
# Property 1: Write/read sink round-trip
# ---------------------------------------------------------------------------


@given(table=pyarrow_table_st())
@settings(max_examples=100)
def test_property1_write_read_sink_round_trip(table: pa.Table) -> None:
    """Feature: e2e-tests, Property 1: Write/read sink round-trip.

    For any PyArrow table written as CSV to a temp data directory, reading it
    back via ``read_sink_csv`` produces a table with the same column names and
    row values.

    **Validates: Requirements 1.3, 2.4**
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        project = Path(tmpdir)
        data_dir = project / "data"
        data_dir.mkdir()

        sink_name = "roundtrip"
        csv_path = data_dir / f"{sink_name}.csv"

        # Write the table as CSV
        pcsv.write_csv(table, str(csv_path))

        # Read it back via the harness helper
        result = read_sink_csv(project, sink_name)

        # Column names must match exactly
        assert result.column_names == table.column_names, (
            f"Column names differ: {result.column_names} != {table.column_names}"
        )

        # Row count must match
        assert result.num_rows == table.num_rows, (
            f"Row count differs: {result.num_rows} != {table.num_rows}"
        )

        # Compare values column by column
        for col_name in table.column_names:
            original = table.column(col_name)
            restored = result.column(col_name)

            for i in range(table.num_rows):
                orig_val = original[i].as_py()
                rest_val = restored[i].as_py()

                if isinstance(orig_val, float):
                    # Float values may lose precision through CSV; compare with tolerance
                    assert isinstance(rest_val, (int, float)), (
                        f"Column {col_name!r} row {i}: expected numeric, got {type(rest_val)}"
                    )
                    assert abs(orig_val - float(rest_val)) < 1e-6 * max(1.0, abs(orig_val)), (
                        f"Column {col_name!r} row {i}: {orig_val} != {rest_val}"
                    )
                elif isinstance(orig_val, int):
                    # CSV may read ints back as ints or as strings; coerce
                    assert int(rest_val) == orig_val, (
                        f"Column {col_name!r} row {i}: {orig_val} != {rest_val}"
                    )
                else:
                    # String comparison
                    assert str(rest_val) == str(orig_val), (
                        f"Column {col_name!r} row {i}: {orig_val!r} != {rest_val!r}"
                    )
