"""Unit tests for rivet_core.testing.fixtures.load_fixture_file."""

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq
import pytest

from rivet_core.testing.fixtures import FixtureError, load_fixture_file

_TABLE = pa.table({"id": [1, 2], "name": ["a", "b"]})


class TestLoadParquet:
    def test_load(self, tmp_path):
        pq.write_table(_TABLE, tmp_path / "data.parquet")
        result = load_fixture_file(Path("data.parquet"), tmp_path)
        assert result.equals(_TABLE)


class TestLoadCSV:
    def test_load_csv(self, tmp_path):
        pa_csv.write_csv(_TABLE, tmp_path / "data.csv")
        result = load_fixture_file(Path("data.csv"), tmp_path)
        assert result.column_names == _TABLE.column_names
        assert result.num_rows == _TABLE.num_rows

    def test_load_tsv(self, tmp_path):
        (tmp_path / "data.tsv").write_text("id\tname\n1\ta\n2\tb\n")
        result = load_fixture_file(Path("data.tsv"), tmp_path)
        assert result.num_rows == 2


class TestLoadJSON:
    def test_load_json(self, tmp_path):
        # pyarrow.json.read_json reads newline-delimited JSON
        lines = [json.dumps({"id": 1, "name": "a"}), json.dumps({"id": 2, "name": "b"})]
        (tmp_path / "data.json").write_text("\n".join(lines) + "\n")
        result = load_fixture_file(Path("data.json"), tmp_path)
        assert result.num_rows == 2

    def test_load_ndjson(self, tmp_path):
        (tmp_path / "data.ndjson").write_text('{"id":1,"name":"a"}\n{"id":2,"name":"b"}\n')
        result = load_fixture_file(Path("data.ndjson"), tmp_path)
        assert result.num_rows == 2

    def test_load_jsonl(self, tmp_path):
        (tmp_path / "data.jsonl").write_text('{"id":1,"name":"a"}\n{"id":2,"name":"b"}\n')
        result = load_fixture_file(Path("data.jsonl"), tmp_path)
        assert result.num_rows == 2


class TestLoadArrowIPC:
    def test_load_arrow(self, tmp_path):
        with pa_ipc.new_file(str(tmp_path / "data.arrow"), _TABLE.schema) as w:
            w.write_table(_TABLE)
        result = load_fixture_file(Path("data.arrow"), tmp_path)
        assert result.equals(_TABLE)

    def test_load_ipc(self, tmp_path):
        with pa_ipc.new_file(str(tmp_path / "data.ipc"), _TABLE.schema) as w:
            w.write_table(_TABLE)
        result = load_fixture_file(Path("data.ipc"), tmp_path)
        assert result.equals(_TABLE)


class TestPathResolution:
    def test_relative_to_project_root(self, tmp_path):
        sub = tmp_path / "fixtures"
        sub.mkdir()
        pq.write_table(_TABLE, sub / "data.parquet")
        result = load_fixture_file(Path("fixtures/data.parquet"), tmp_path)
        assert result.equals(_TABLE)

    def test_absolute_path(self, tmp_path):
        pq.write_table(_TABLE, tmp_path / "abs.parquet")
        result = load_fixture_file(tmp_path / "abs.parquet", Path("/unused"))
        assert result.equals(_TABLE)


class TestErrors:
    def test_missing_file_rvt901(self, tmp_path):
        with pytest.raises(FixtureError) as exc_info:
            load_fixture_file(Path("nope.parquet"), tmp_path)
        assert exc_info.value.error.code == "RVT-901"

    def test_corrupt_file_rvt902(self, tmp_path):
        (tmp_path / "bad.parquet").write_text("not parquet")
        with pytest.raises(FixtureError) as exc_info:
            load_fixture_file(Path("bad.parquet"), tmp_path)
        assert exc_info.value.error.code == "RVT-902"

    def test_unsupported_extension_rvt902(self, tmp_path):
        (tmp_path / "data.xlsx").write_text("nope")
        with pytest.raises(FixtureError) as exc_info:
            load_fixture_file(Path("data.xlsx"), tmp_path)
        assert exc_info.value.error.code == "RVT-902"
