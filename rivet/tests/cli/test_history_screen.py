"""Tests for the execution history overlay screen.

Validates: Requirements 21.2
"""

from __future__ import annotations

from datetime import UTC, datetime

from rivet_cli.repl.screens.history import _format_duration, _format_row_count
from rivet_core.interactive.types import QueryHistoryEntry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_entry(
    action_type: str = "query",
    name: str = "SELECT 1",
    row_count: int | None = 10,
    duration_ms: float = 42.0,
    status: str = "success",
) -> QueryHistoryEntry:
    return QueryHistoryEntry(
        timestamp=datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC),
        action_type=action_type,
        name=name,
        row_count=row_count,
        duration_ms=duration_ms,
        status=status,
    )


# ---------------------------------------------------------------------------
# _format_duration
# ---------------------------------------------------------------------------


class TestFormatDuration:
    def test_sub_second(self):
        assert _format_duration(42.0) == "42ms"

    def test_exactly_1000ms(self):
        assert _format_duration(1000.0) == "1.00s"

    def test_over_1000ms(self):
        assert _format_duration(2500.0) == "2.50s"

    def test_zero(self):
        assert _format_duration(0.0) == "0ms"

    def test_fractional_ms(self):
        assert _format_duration(999.9) == "1000ms"


# ---------------------------------------------------------------------------
# _format_row_count
# ---------------------------------------------------------------------------


class TestFormatRowCount:
    def test_none_returns_dash(self):
        assert _format_row_count(None) == "—"

    def test_zero(self):
        assert _format_row_count(0) == "0"

    def test_positive(self):
        assert _format_row_count(42) == "42"


# ---------------------------------------------------------------------------
# HistoryScreen — construction and entry handling
# ---------------------------------------------------------------------------


class TestHistoryScreenConstruction:
    def test_empty_entries(self):
        from rivet_cli.repl.screens.history import HistoryScreen

        screen = HistoryScreen(entries=[])
        assert screen._entries == []

    def test_stores_entries(self):
        from rivet_cli.repl.screens.history import HistoryScreen

        entries = [_make_entry(), _make_entry(action_type="joint", name="my_joint")]
        screen = HistoryScreen(entries=entries)
        assert len(screen._entries) == 2

    def test_all_action_types_accepted(self):
        from rivet_cli.repl.screens.history import HistoryScreen

        for action_type in ("query", "joint", "pipeline", "preview"):
            entry = _make_entry(action_type=action_type)
            screen = HistoryScreen(entries=[entry])
            assert screen._entries[0].action_type == action_type

    def test_all_statuses_accepted(self):
        from rivet_cli.repl.screens.history import HistoryScreen

        for status in ("success", "failed", "canceled", "warning"):
            entry = _make_entry(status=status)
            screen = HistoryScreen(entries=[entry])
            assert screen._entries[0].status == status

    def test_null_row_count_accepted(self):
        from rivet_cli.repl.screens.history import HistoryScreen

        entry = _make_entry(row_count=None)
        screen = HistoryScreen(entries=[entry])
        assert screen._entries[0].row_count is None
