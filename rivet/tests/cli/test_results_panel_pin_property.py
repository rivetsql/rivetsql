"""Property test for result pinning replaces previous pin.

# Feature: cli-repl, Property 28: Pinning replaces previous pin
# Validates: Requirements 13.2

For any sequence of pin operations, only the most recently pinned result
is retained. Pinning again always replaces the previous pin.
"""

from __future__ import annotations

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_cli.repl.widgets.results import ViewMode, _ResultsPanelState
from rivet_core.interactive.types import QueryResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_panel() -> _ResultsPanelState:
    panel = _ResultsPanelState()
    panel._init_state()
    return panel


def _make_query_result(n: int, elapsed_ms: float = 1.0) -> QueryResult:
    table = pa.table({"id": list(range(n))})
    return QueryResult(
        table=table,
        row_count=n,
        column_names=["id"],
        column_types=["int64"],
        elapsed_ms=elapsed_ms,
        query_plan=None,
        quality_results=None,
        truncated=False,
    )


# ---------------------------------------------------------------------------
# Property 28: Pinning replaces previous pin
# ---------------------------------------------------------------------------


@given(
    first_rows=st.integers(min_value=1, max_value=100),
    second_rows=st.integers(min_value=1, max_value=100),
)
@settings(max_examples=200)
def test_second_pin_replaces_first(first_rows: int, second_rows: int) -> None:
    """Property 28: Pinning a second result replaces the first pin."""
    panel = _make_panel()

    panel.show_query_result(_make_query_result(first_rows))
    panel.pin()
    assert panel.pinned_table is not None
    assert panel.pinned_table.num_rows == first_rows

    panel.show_query_result(_make_query_result(second_rows))
    panel.pin()

    assert panel.pinned_table is not None
    assert panel.pinned_table.num_rows == second_rows
    assert str(second_rows) in panel.pinned_label


@given(
    row_counts=st.lists(st.integers(min_value=1, max_value=50), min_size=2, max_size=10),
)
@settings(max_examples=100)
def test_sequence_of_pins_retains_only_last(row_counts: list[int]) -> None:
    """Property 28: After N pins, only the last pinned table is retained."""
    panel = _make_panel()

    for n in row_counts:
        panel.show_query_result(_make_query_result(n))
        panel.pin()

    assert panel.pinned_table is not None
    assert panel.pinned_table.num_rows == row_counts[-1]
    assert str(row_counts[-1]) in panel.pinned_label


@given(
    rows=st.integers(min_value=1, max_value=100),
)
@settings(max_examples=100)
def test_unpin_after_pin_clears_state(rows: int) -> None:
    """Property 28: Unpinning after a pin clears the pinned table and label."""
    panel = _make_panel()

    panel.show_query_result(_make_query_result(rows))
    panel.pin()
    assert panel.pinned_table is not None

    panel.unpin()

    assert panel.pinned_table is None
    assert panel.pinned_label == ""


@given(
    rows=st.integers(min_value=1, max_value=100),
)
@settings(max_examples=100)
def test_unpin_resets_diff_view_to_data(rows: int) -> None:
    """Property 28: Unpinning while in DIFF view resets to DATA view."""
    panel = _make_panel()

    panel.show_query_result(_make_query_result(rows))
    panel.pin()
    panel._view_mode = ViewMode.DIFF

    panel.unpin()

    assert panel.view_mode == ViewMode.DATA
    assert panel._diff_result is None
