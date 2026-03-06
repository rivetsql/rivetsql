"""Property test for result truncation at configured limit.

# Feature: cli-repl, Property 25: Result truncation at configured limit
# Validates: Requirements 39.5

For any query that would return more rows than max_results, the QueryResult
should have truncated=True and row_count <= max_results.
"""

from __future__ import annotations

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.interactive.session import InteractiveSession

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_session(max_results: int) -> InteractiveSession:
    """Return a minimal session with no project loaded, just for truncation tests."""
    from pathlib import Path
    return InteractiveSession(project_path=Path("."), max_results=max_results)


# ---------------------------------------------------------------------------
# Property 25: Result truncation at configured limit
# ---------------------------------------------------------------------------

@given(
    max_results=st.integers(min_value=1, max_value=1000),
    extra_rows=st.integers(min_value=1, max_value=500),
)
@settings(max_examples=200)
def test_truncation_applied_when_rows_exceed_limit(
    max_results: int,
    extra_rows: int,
) -> None:
    """Property 25: When a table has more rows than max_results, truncation is applied.

    truncated=True and row_count <= max_results.
    """
    session = _make_session(max_results)
    total_rows = max_results + extra_rows
    table = pa.table({"x": list(range(total_rows))})

    result_table, was_truncated = session._apply_truncation(table)

    assert was_truncated is True
    assert result_table.num_rows <= max_results
    assert result_table.num_rows == max_results


@given(
    max_results=st.integers(min_value=1, max_value=1000),
    row_count=st.integers(min_value=0, max_value=1000),
)
@settings(max_examples=200)
def test_no_truncation_when_rows_within_limit(
    max_results: int,
    row_count: int,
) -> None:
    """Property 25 (inverse): When rows <= max_results, truncated=False and all rows kept."""
    # Only test cases where row_count <= max_results
    row_count = min(row_count, max_results)
    session = _make_session(max_results)
    table = pa.table({"x": list(range(row_count))})

    result_table, was_truncated = session._apply_truncation(table)

    assert was_truncated is False
    assert result_table.num_rows == row_count


@given(
    max_results=st.integers(min_value=1, max_value=500),
    extra_rows=st.integers(min_value=1, max_value=500),
)
@settings(max_examples=100)
def test_truncation_preserves_schema(
    max_results: int,
    extra_rows: int,
) -> None:
    """Property 25: Truncation preserves the table schema (column names and types)."""
    session = _make_session(max_results)
    total_rows = max_results + extra_rows
    table = pa.table({
        "id": pa.array(list(range(total_rows)), type=pa.int64()),
        "name": pa.array([f"row_{i}" for i in range(total_rows)], type=pa.string()),
    })

    result_table, was_truncated = session._apply_truncation(table)

    assert was_truncated is True
    assert result_table.schema == table.schema
    assert result_table.num_rows == max_results
