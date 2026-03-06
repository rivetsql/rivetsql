"""Tests for the export dialog screen.

Validates: Requirements 26.1, 26.3
"""

from __future__ import annotations

from rivet_cli.repl.screens.export import (
    EXPORT_FORMATS,
    ExportDialogState,
    ExportResult,
    ExportScreen,
)

# ---------------------------------------------------------------------------
# ExportDialogState
# ---------------------------------------------------------------------------


class TestExportDialogState:
    def test_default_format_is_csv(self):
        state = ExportDialogState(row_count=100)
        assert state.selected_format == "csv"

    def test_default_file_path_empty(self):
        state = ExportDialogState(row_count=100)
        assert state.file_path == ""

    def test_is_valid_false_when_no_path_and_not_clipboard(self):
        state = ExportDialogState(row_count=100, selected_format="csv", file_path="")
        assert not state.is_valid()

    def test_is_valid_true_when_path_provided(self):
        state = ExportDialogState(row_count=100, selected_format="csv", file_path="/tmp/out.csv")
        assert state.is_valid()

    def test_is_valid_true_for_clipboard_without_path(self):
        state = ExportDialogState(row_count=100, selected_format="clipboard", file_path="")
        assert state.is_valid()

    def test_is_valid_false_for_whitespace_only_path(self):
        state = ExportDialogState(row_count=100, selected_format="csv", file_path="   ")
        assert not state.is_valid()

    def test_format_label_returns_human_readable(self):
        state = ExportDialogState(row_count=0, selected_format="parquet")
        assert "Parquet" in state.format_label()

    def test_format_label_unknown_returns_format_id(self):
        state = ExportDialogState(row_count=0, selected_format="unknown_fmt")
        assert state.format_label() == "unknown_fmt"

    def test_row_count_stored(self):
        state = ExportDialogState(row_count=42)
        assert state.row_count == 42


# ---------------------------------------------------------------------------
# EXPORT_FORMATS
# ---------------------------------------------------------------------------


class TestExportFormats:
    def test_all_required_formats_present(self):
        fmt_ids = {fmt for _, fmt in EXPORT_FORMATS}
        for required in ("csv", "tsv", "parquet", "json", "jsonl", "clipboard"):
            assert required in fmt_ids, f"Missing format: {required}"

    def test_each_format_has_label_and_id(self):
        for label, fmt_id in EXPORT_FORMATS:
            assert label, "Label must not be empty"
            assert fmt_id, "Format ID must not be empty"


# ---------------------------------------------------------------------------
# ExportResult
# ---------------------------------------------------------------------------


class TestExportResult:
    def test_confirmed_result(self):
        result = ExportResult(confirmed=True, format="csv", file_path="/tmp/out.csv")
        assert result.confirmed is True
        assert result.format == "csv"
        assert result.file_path == "/tmp/out.csv"

    def test_clipboard_result_has_empty_path(self):
        result = ExportResult(confirmed=True, format="clipboard", file_path="")
        assert result.file_path == ""

    def test_cancelled_result(self):
        result = ExportResult(confirmed=False, format="csv", file_path="")
        assert result.confirmed is False


# ---------------------------------------------------------------------------
# ExportScreen construction
# ---------------------------------------------------------------------------


class TestExportScreenConstruction:
    def test_stores_row_count(self):
        screen = ExportScreen(row_count=500)
        assert screen._state.row_count == 500

    def test_stores_default_path(self):
        screen = ExportScreen(row_count=10, default_path="/tmp/data.csv")
        assert screen._state.file_path == "/tmp/data.csv"

    def test_default_path_empty_when_not_provided(self):
        screen = ExportScreen(row_count=10)
        assert screen._state.file_path == ""

    def test_default_format_is_csv(self):
        screen = ExportScreen(row_count=10)
        assert screen._state.selected_format == "csv"

    def test_zero_row_count_accepted(self):
        screen = ExportScreen(row_count=0)
        assert screen._state.row_count == 0

    def test_large_row_count_accepted(self):
        screen = ExportScreen(row_count=1_000_000)
        assert screen._state.row_count == 1_000_000
