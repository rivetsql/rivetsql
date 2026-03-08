"""Property-based tests: Sampled Materialization Stats (Properties 7 & 8).

Property 7: Sampling Threshold for Distinct Counts
  For any PyArrow table, _compute_materialization_stats shall set sampled=True
  if and only if table.num_rows > 1,000,000.

Property 8: Full-Table Stats Regardless of Sampling
  For any PyArrow table (regardless of row count), null_count, min_value, and
  max_value in each ColumnExecutionStats shall equal the values computed on the
  full column, not a sample.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.executor import (
    SAMPLE_THRESHOLD,
    _compute_materialization_stats,
)

# ── Strategies ──────────────────────────────────────────────────────────────────


@st.composite
def arrow_table_with_rows(draw: st.DrawFn, min_rows: int = 0, max_rows: int = 50):
    """Generate a small Arrow table with random int64 data and optional nulls."""
    n_rows = draw(st.integers(min_value=min_rows, max_value=max_rows))
    n_cols = draw(st.integers(min_value=1, max_value=3))
    columns: dict[str, list[int | None]] = {}
    for c in range(n_cols):
        col_name = f"col_{c}"
        values: list[int | None] = []
        for _ in range(n_rows):
            if draw(st.booleans()):
                values.append(draw(st.integers(min_value=-1000, max_value=1000)))
            else:
                values.append(None)
        columns[col_name] = values
    return pa.table({k: pa.array(v, type=pa.int64()) for k, v in columns.items()})


def _make_large_table(n_rows: int, n_cols: int = 2) -> pa.Table:
    """Build a table with n_rows rows of sequential int64 data (no nulls)."""
    return pa.table(
        {f"col_{c}": pa.array(range(n_rows), type=pa.int64()) for c in range(n_cols)}
    )


# ── Property 7: Sampling Threshold for Distinct Counts ──────────────────────────


@given(table=arrow_table_with_rows(min_rows=0, max_rows=50))
@settings(max_examples=100)
def test_property7_small_table_not_sampled(table: pa.Table) -> None:
    """Tables with num_rows <= SAMPLE_THRESHOLD must have sampled=False."""
    # All generated tables here have at most 50 rows, well below threshold
    stats = _compute_materialization_stats(table)
    assert stats.sampled is False


def test_property7_at_threshold_not_sampled() -> None:
    """A table with exactly SAMPLE_THRESHOLD rows must have sampled=False."""
    table = _make_large_table(SAMPLE_THRESHOLD, n_cols=1)
    stats = _compute_materialization_stats(table)
    assert stats.sampled is False


def test_property7_above_threshold_sampled() -> None:
    """A table with SAMPLE_THRESHOLD + 1 rows must have sampled=True."""
    table = _make_large_table(SAMPLE_THRESHOLD + 1, n_cols=1)
    stats = _compute_materialization_stats(table)
    assert stats.sampled is True


# ── Property 8: Full-Table Stats Regardless of Sampling ─────────────────────────


@given(table=arrow_table_with_rows(min_rows=0, max_rows=50))
@settings(max_examples=100)
def test_property8_full_table_stats_match_small(table: pa.Table) -> None:
    """For small tables, null_count/min/max must match full-column values."""
    stats = _compute_materialization_stats(table)
    for cs in stats.column_stats:
        arr = table.column(cs.column)
        assert cs.null_count == arr.null_count

        expected_min = pc.min(arr).as_py()
        expected_max = pc.max(arr).as_py()
        expected_min_str = str(expected_min) if expected_min is not None else None
        expected_max_str = str(expected_max) if expected_max is not None else None
        assert cs.min_value == expected_min_str
        assert cs.max_value == expected_max_str


def test_property8_full_table_stats_match_large() -> None:
    """For large (sampled) tables, null_count/min/max must still match full column."""
    table = _make_large_table(SAMPLE_THRESHOLD + 1, n_cols=2)
    stats = _compute_materialization_stats(table)
    assert stats.sampled is True

    for cs in stats.column_stats:
        arr = table.column(cs.column)
        assert cs.null_count == arr.null_count

        expected_min = pc.min(arr).as_py()
        expected_max = pc.max(arr).as_py()
        assert cs.min_value == str(expected_min)
        assert cs.max_value == str(expected_max)
