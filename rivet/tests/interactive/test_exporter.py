"""Tests for rivet_core.interactive.exporter — export_table."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from rivet_core.interactive.exporter import export_table


@pytest.fixture
def sample_table() -> pa.Table:
    return pa.table({"id": [1, 2, 3], "name": ["alice", "bob", "carol"], "score": [9.5, 7.0, 8.25]})


class TestExportCSV:
    def test_csv_row_count(self, sample_table: pa.Table, tmp_path: Path) -> None:
        dest = tmp_path / "out.csv"
        export_table(sample_table, dest, "csv")
        rows = list(csv.DictReader(dest.read_text().splitlines()))
        assert len(rows) == 3

    def test_csv_column_names(self, sample_table: pa.Table, tmp_path: Path) -> None:
        dest = tmp_path / "out.csv"
        export_table(sample_table, dest, "csv")
        rows = list(csv.DictReader(dest.read_text().splitlines()))
        assert set(rows[0].keys()) == {"id", "name", "score"}

    def test_csv_values(self, sample_table: pa.Table, tmp_path: Path) -> None:
        dest = tmp_path / "out.csv"
        export_table(sample_table, dest, "csv")
        rows = list(csv.DictReader(dest.read_text().splitlines()))
        assert rows[0]["name"] == "alice"


class TestExportTSV:
    def test_tsv_delimiter(self, sample_table: pa.Table, tmp_path: Path) -> None:
        dest = tmp_path / "out.tsv"
        export_table(sample_table, dest, "tsv")
        first_line = dest.read_text().splitlines()[0]
        assert "\t" in first_line
        assert "," not in first_line

    def test_tsv_row_count(self, sample_table: pa.Table, tmp_path: Path) -> None:
        dest = tmp_path / "out.tsv"
        export_table(sample_table, dest, "tsv")
        rows = list(csv.DictReader(dest.read_text().splitlines(), delimiter="\t"))
        assert len(rows) == 3


class TestExportParquet:
    def test_parquet_round_trip_row_count(self, sample_table: pa.Table, tmp_path: Path) -> None:
        dest = tmp_path / "out.parquet"
        export_table(sample_table, dest, "parquet")
        back = pq.read_table(dest)
        assert back.num_rows == sample_table.num_rows

    def test_parquet_round_trip_columns(self, sample_table: pa.Table, tmp_path: Path) -> None:
        dest = tmp_path / "out.parquet"
        export_table(sample_table, dest, "parquet")
        back = pq.read_table(dest)
        assert back.column_names == sample_table.column_names

    def test_parquet_round_trip_values(self, sample_table: pa.Table, tmp_path: Path) -> None:
        dest = tmp_path / "out.parquet"
        export_table(sample_table, dest, "parquet")
        back = pq.read_table(dest)
        assert back.column("name").to_pylist() == ["alice", "bob", "carol"]


class TestExportJSON:
    def test_json_is_array(self, sample_table: pa.Table, tmp_path: Path) -> None:
        dest = tmp_path / "out.json"
        export_table(sample_table, dest, "json")
        data = json.loads(dest.read_text())
        assert isinstance(data, list)
        assert len(data) == 3

    def test_json_column_names(self, sample_table: pa.Table, tmp_path: Path) -> None:
        dest = tmp_path / "out.json"
        export_table(sample_table, dest, "json")
        data = json.loads(dest.read_text())
        assert set(data[0].keys()) == {"id", "name", "score"}


class TestExportJSONL:
    def test_jsonl_line_count(self, sample_table: pa.Table, tmp_path: Path) -> None:
        dest = tmp_path / "out.jsonl"
        export_table(sample_table, dest, "jsonl")
        lines = [l for l in dest.read_text().splitlines() if l.strip()]
        assert len(lines) == 3

    def test_jsonl_each_line_valid_json(self, sample_table: pa.Table, tmp_path: Path) -> None:
        dest = tmp_path / "out.jsonl"
        export_table(sample_table, dest, "jsonl")
        for line in dest.read_text().splitlines():
            if line.strip():
                obj = json.loads(line)
                assert isinstance(obj, dict)


class TestExportEdgeCases:
    def test_empty_table_csv(self, tmp_path: Path) -> None:
        table = pa.table({"x": pa.array([], type=pa.int64())})
        dest = tmp_path / "empty.csv"
        export_table(table, dest, "csv")
        rows = list(csv.DictReader(dest.read_text().splitlines()))
        assert rows == []

    def test_empty_table_parquet(self, tmp_path: Path) -> None:
        table = pa.table({"x": pa.array([], type=pa.int64())})
        dest = tmp_path / "empty.parquet"
        export_table(table, dest, "parquet")
        back = pq.read_table(dest)
        assert back.num_rows == 0
        assert back.column_names == ["x"]

    def test_unsupported_format_raises(self, sample_table: pa.Table, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Unsupported export format"):
            export_table(sample_table, tmp_path / "out.xyz", "xlsx")  # type: ignore[arg-type]

    def test_null_values_csv(self, tmp_path: Path) -> None:
        table = pa.table({"a": [1, None, 3]})
        dest = tmp_path / "nulls.csv"
        export_table(table, dest, "csv")
        rows = list(csv.DictReader(dest.read_text().splitlines()))
        assert len(rows) == 3
