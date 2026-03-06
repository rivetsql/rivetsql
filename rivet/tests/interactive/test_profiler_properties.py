"""Property-based tests for rivet_core.interactive.profiler.

Property 8: Profile result covers all columns with type-appropriate stats.

Properties verified:
- ProfileResult always has one ColumnProfile per column.
- Numeric columns have min, max, mean, median, stddev, and an 8-bin histogram.
- String columns have min/max/mean (lengths), top_values, and no histogram.
- Boolean columns have top_values with True/False counts, no histogram.
- Temporal columns have min, max, and an 8-bin histogram.
- null_pct is always in [0, 100] and consistent with null_count.
- histogram bins always sum to the non-null count.

Validates: Requirements 14.2, 14.3, 14.4, 14.5, 14.6
"""

from __future__ import annotations

from datetime import date

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.interactive.profiler import Profiler

# ── Strategies ────────────────────────────────────────────────────────────────

_maybe_none = st.none()

_int_values = st.lists(
    st.one_of(st.integers(min_value=-1_000_000, max_value=1_000_000), _maybe_none),
    min_size=1,
    max_size=50,
)

_float_values = st.lists(
    st.one_of(st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False), _maybe_none),
    min_size=1,
    max_size=50,
)

_string_values = st.lists(
    st.one_of(st.text(min_size=0, max_size=20), _maybe_none),
    min_size=1,
    max_size=50,
)

_bool_values = st.lists(
    st.one_of(st.booleans(), _maybe_none),
    min_size=1,
    max_size=50,
)

_date_values = st.lists(
    st.one_of(
        st.dates(min_value=date(2000, 1, 1), max_value=date(2099, 12, 31)),
        _maybe_none,
    ),
    min_size=1,
    max_size=50,
)


def _int_table(values: list) -> pa.Table:
    return pa.table({"col": pa.array(values, type=pa.int64())})


def _float_table(values: list) -> pa.Table:
    return pa.table({"col": pa.array(values, type=pa.float64())})


def _string_table(values: list) -> pa.Table:
    return pa.table({"col": pa.array(values, type=pa.string())})


def _bool_table(values: list) -> pa.Table:
    return pa.table({"col": pa.array(values, type=pa.bool_())})


def _date_table(values: list) -> pa.Table:
    return pa.table({"col": pa.array(values, type=pa.date32())})


# ── Helpers ───────────────────────────────────────────────────────────────────

_profiler = Profiler()


def _assert_common(col, num_rows: int) -> None:
    """Invariants that hold for every column type."""
    assert 0.0 <= col.null_pct <= 100.0
    if num_rows > 0:
        expected_pct = col.null_count / num_rows * 100.0
        assert abs(col.null_pct - expected_pct) < 1e-9
    else:
        assert col.null_pct == 0.0


def _assert_histogram(col, num_rows: int) -> None:
    """Histogram must have 8 bins summing to the non-null count (or be None when all null)."""
    non_null = num_rows - col.null_count
    if non_null == 0:
        # histogram may be None when there are no non-null values
        return
    assert col.histogram is not None, "histogram must be present for non-empty numeric/temporal"
    assert len(col.histogram) == 8
    assert sum(col.histogram) == non_null


# ── Property 8a: numeric columns ─────────────────────────────────────────────


@given(values=_int_values)
@settings(max_examples=100)
def test_numeric_int_type_appropriate_stats(values: list) -> None:
    """Integer columns have min, max, mean, median, stddev, and 8-bin histogram (Req 14.2)."""
    table = _int_table(values)
    result = _profiler.profile(table)

    assert result.row_count == len(values)
    assert result.column_count == 1
    assert len(result.columns) == 1

    col = result.columns[0]
    _assert_common(col, len(values))

    non_null_count = sum(1 for v in values if v is not None)
    if non_null_count > 0:
        assert col.min is not None
        assert col.max is not None
        assert col.mean is not None
        assert col.median is not None
        assert col.stddev is not None
    else:
        assert col.min is None
        assert col.max is None

    _assert_histogram(col, len(values))
    assert col.top_values is None


@given(values=_float_values)
@settings(max_examples=100)
def test_numeric_float_type_appropriate_stats(values: list) -> None:
    """Float columns have min, max, mean, median, stddev, and 8-bin histogram (Req 14.2)."""
    table = _float_table(values)
    result = _profiler.profile(table)

    col = result.columns[0]
    _assert_common(col, len(values))

    non_null_count = sum(1 for v in values if v is not None)
    if non_null_count > 0:
        assert col.min is not None
        assert col.max is not None
        assert col.mean is not None

    _assert_histogram(col, len(values))
    assert col.top_values is None


# ── Property 8b: string columns ───────────────────────────────────────────────


@given(values=_string_values)
@settings(max_examples=100)
def test_string_type_appropriate_stats(values: list) -> None:
    """String columns have length-based min/max/mean, top_values, and no histogram (Req 14.3)."""
    table = _string_table(values)
    result = _profiler.profile(table)

    col = result.columns[0]
    _assert_common(col, len(values))

    non_null = [v for v in values if v is not None]
    if non_null:
        assert col.min is not None  # min length
        assert col.max is not None  # max length
        assert col.mean is not None  # avg length
        assert col.top_values is not None
        assert len(col.top_values) <= 5
        # top_values counts must be positive
        for _val, cnt in col.top_values:
            assert cnt > 0

    assert col.histogram is None
    assert col.median is None
    assert col.stddev is None


# ── Property 8c: boolean columns ─────────────────────────────────────────────


@given(values=_bool_values)
@settings(max_examples=100)
def test_boolean_type_appropriate_stats(values: list) -> None:
    """Boolean columns have top_values with True/False counts, no histogram (Req 14.4)."""
    table = _bool_table(values)
    result = _profiler.profile(table)

    col = result.columns[0]
    _assert_common(col, len(values))

    assert col.histogram is None
    assert col.top_values is not None

    top_dict = dict(col.top_values)
    assert True in top_dict
    assert False in top_dict

    true_count = top_dict[True]
    false_count = top_dict[False]
    non_null = len(values) - col.null_count
    assert true_count + false_count == non_null

    # null% + true% + false% == 100%
    if len(values) > 0:
        total_pct = col.null_pct + (true_count / len(values) * 100) + (false_count / len(values) * 100)
        assert abs(total_pct - 100.0) < 1e-9


# ── Property 8d: temporal columns ────────────────────────────────────────────


@given(values=_date_values)
@settings(max_examples=100)
def test_temporal_type_appropriate_stats(values: list) -> None:
    """Temporal columns have min, max, and 8-bin histogram (Req 14.5)."""
    table = _date_table(values)
    result = _profiler.profile(table)

    col = result.columns[0]
    _assert_common(col, len(values))

    non_null = [v for v in values if v is not None]
    if non_null:
        assert col.min is not None
        assert col.max is not None

    _assert_histogram(col, len(values))
    assert col.top_values is None
    assert col.mean is None
    assert col.median is None
    assert col.stddev is None


# ── Property 8e: one ColumnProfile per column ────────────────────────────────


@given(
    int_vals=_int_values,
    str_vals=_string_values,
    bool_vals=_bool_values,
    date_vals=_date_values,
)
@settings(max_examples=50)
def test_one_profile_per_column(
    int_vals: list,
    str_vals: list,
    bool_vals: list,
    date_vals: list,
) -> None:
    """ProfileResult always has exactly one ColumnProfile per column (Req 14.2–14.6)."""
    # Align lengths
    n = min(len(int_vals), len(str_vals), len(bool_vals), len(date_vals))
    table = pa.table({
        "num": pa.array(int_vals[:n], type=pa.int64()),
        "txt": pa.array(str_vals[:n], type=pa.string()),
        "flag": pa.array(bool_vals[:n], type=pa.bool_()),
        "dt": pa.array(date_vals[:n], type=pa.date32()),
    })
    result = _profiler.profile(table)

    assert result.row_count == n
    assert result.column_count == 4
    assert len(result.columns) == 4
    assert [c.name for c in result.columns] == ["num", "txt", "flag", "dt"]
