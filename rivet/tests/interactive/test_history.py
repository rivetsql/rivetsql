"""Property 18: History round-trip with cap.

Feature: cli-repl, Property 18: History round-trip with cap
Validates: Requirements 21.1, 21.3
"""

from __future__ import annotations

import tempfile
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

import rivet_core.interactive.history as hist_mod
from rivet_core.interactive.history import HISTORY_CAP, load_history, save_history
from rivet_core.interactive.types import QueryHistoryEntry


def _make_entry(
    action_type: str = "query",
    name: str = "SELECT 1",
    row_count: int | None = 1,
    duration_ms: float = 10.0,
    status: str = "success",
) -> QueryHistoryEntry:
    return QueryHistoryEntry(
        timestamp=datetime.now(UTC),
        action_type=action_type,
        name=name,
        row_count=row_count,
        duration_ms=duration_ms,
        status=status,
    )


_entry_strategy = st.builds(
    QueryHistoryEntry,
    timestamp=st.just(datetime(2024, 1, 1, tzinfo=UTC)),
    action_type=st.sampled_from(["query", "joint", "pipeline", "preview"]),
    name=st.text(min_size=1, max_size=50),
    row_count=st.one_of(st.none(), st.integers(min_value=0, max_value=10_000)),
    duration_ms=st.floats(min_value=0.0, max_value=60_000.0, allow_nan=False),
    status=st.sampled_from(["success", "failed", "canceled", "warning"]),
)


import pytest


@pytest.fixture(autouse=True)
def _isolated_history(tmp_path: Path) -> Iterator[None]:
    """Redirect history to a temp file and disable ephemeral-path guard."""
    orig_file = hist_mod._HISTORY_FILE
    orig_check = hist_mod._is_ephemeral
    hist_mod._HISTORY_FILE = tmp_path / "history.json"
    hist_mod._is_ephemeral = lambda _: False  # type: ignore[assignment]
    try:
        yield
    finally:
        hist_mod._HISTORY_FILE = orig_file
        hist_mod._is_ephemeral = orig_check  # type: ignore[assignment]


class TestHistoryRoundTrip:
    """Unit tests for history serialization/deserialization."""

    def test_empty_history_round_trip(self, tmp_path: Path) -> None:
        project = tmp_path / "project"
        project.mkdir()
        save_history(project, [])
        loaded = load_history(project)
        assert loaded == []

    def test_single_entry_round_trip(self, tmp_path: Path) -> None:
        project = tmp_path / "project"
        project.mkdir()
        entry = _make_entry()
        save_history(project, [entry])
        loaded = load_history(project)
        assert len(loaded) == 1
        assert loaded[0].action_type == entry.action_type
        assert loaded[0].name == entry.name
        assert loaded[0].row_count == entry.row_count
        assert loaded[0].duration_ms == entry.duration_ms
        assert loaded[0].status == entry.status

    def test_multiple_entries_round_trip(self, tmp_path: Path) -> None:
        project = tmp_path / "project"
        project.mkdir()
        entries = [_make_entry(name=f"query_{i}") for i in range(10)]
        save_history(project, entries)
        loaded = load_history(project)
        assert len(loaded) == 10
        for orig, restored in zip(entries, loaded):
            assert orig.name == restored.name
            assert orig.action_type == restored.action_type
            assert orig.status == restored.status

    def test_cap_at_1000_retains_most_recent(self, tmp_path: Path) -> None:
        project = tmp_path / "project"
        project.mkdir()
        entries = [_make_entry(name=f"query_{i}") for i in range(1500)]
        save_history(project, entries)
        loaded = load_history(project)
        assert len(loaded) == HISTORY_CAP
        # Most recent 1000 are retained (last 1000 of the 1500)
        assert loaded[0].name == "query_500"
        assert loaded[-1].name == "query_1499"

    def test_cap_exactly_1000_not_truncated(self, tmp_path: Path) -> None:
        project = tmp_path / "project"
        project.mkdir()
        entries = [_make_entry(name=f"q_{i}") for i in range(1000)]
        save_history(project, entries)
        loaded = load_history(project)
        assert len(loaded) == 1000

    def test_cap_999_not_truncated(self, tmp_path: Path) -> None:
        project = tmp_path / "project"
        project.mkdir()
        entries = [_make_entry(name=f"q_{i}") for i in range(999)]
        save_history(project, entries)
        loaded = load_history(project)
        assert len(loaded) == 999

    def test_missing_file_returns_empty(self, tmp_path: Path) -> None:
        project = tmp_path / "project"
        project.mkdir()
        loaded = load_history(project)
        assert loaded == []

    def test_corrupt_file_returns_empty(self, tmp_path: Path) -> None:
        project = tmp_path / "project"
        project.mkdir()
        corrupt_file = tmp_path / "corrupt.json"
        corrupt_file.write_text("not valid json", encoding="utf-8")
        hist_mod._HISTORY_FILE = corrupt_file
        loaded = load_history(project)
        assert loaded == []

    def test_projects_are_isolated(self, tmp_path: Path) -> None:
        """History for different projects is stored separately."""
        project_a = tmp_path / "project_a"
        project_b = tmp_path / "project_b"
        project_a.mkdir()
        project_b.mkdir()

        entries_a = [_make_entry(name="query_a")]
        entries_b = [_make_entry(name="query_b")]
        save_history(project_a, entries_a)
        save_history(project_b, entries_b)

        loaded_a = load_history(project_a)
        loaded_b = load_history(project_b)

        assert len(loaded_a) == 1
        assert loaded_a[0].name == "query_a"
        assert len(loaded_b) == 1
        assert loaded_b[0].name == "query_b"

    def test_timestamp_preserved(self, tmp_path: Path) -> None:
        project = tmp_path / "project"
        project.mkdir()
        ts = datetime(2025, 6, 15, 12, 30, 45, tzinfo=UTC)
        entry = QueryHistoryEntry(
            timestamp=ts,
            action_type="query",
            name="SELECT 1",
            row_count=1,
            duration_ms=5.0,
            status="success",
        )
        save_history(project, [entry])
        loaded = load_history(project)
        assert len(loaded) == 1
        assert loaded[0].timestamp == ts

    def test_null_row_count_preserved(self, tmp_path: Path) -> None:
        project = tmp_path / "project"
        project.mkdir()
        entry = _make_entry(row_count=None)
        save_history(project, [entry])
        loaded = load_history(project)
        assert loaded[0].row_count is None


@settings(max_examples=100)
@given(entries=st.lists(_entry_strategy, min_size=0, max_size=1200))
def test_history_round_trip_property(entries: list[QueryHistoryEntry]) -> None:
    """Property 18: History round-trip with cap.

    For any sequence of QueryHistoryEntry objects, serializing to JSON and
    deserializing should produce equivalent entries. When the history exceeds
    1,000 entries, only the most recent 1,000 should be retained.
    """
    orig_file = hist_mod._HISTORY_FILE
    orig_check = hist_mod._is_ephemeral

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        project = tmp_path / "project"
        project.mkdir()
        hist_mod._HISTORY_FILE = tmp_path / "history.json"
        hist_mod._is_ephemeral = lambda _: False  # type: ignore[assignment]
        try:
            save_history(project, entries)
            loaded = load_history(project)

            expected_count = min(len(entries), HISTORY_CAP)
            assert len(loaded) == expected_count

            # Verify most recent entries are retained
            expected_entries = entries[-HISTORY_CAP:] if len(entries) > HISTORY_CAP else entries
            for orig, restored in zip(expected_entries, loaded):
                assert orig.action_type == restored.action_type
                assert orig.name == restored.name
                assert orig.row_count == restored.row_count
                assert orig.status == restored.status
                assert abs(orig.duration_ms - restored.duration_ms) < 1e-6
        finally:
            hist_mod._HISTORY_FILE = orig_file
            hist_mod._is_ephemeral = orig_check  # type: ignore[assignment]
