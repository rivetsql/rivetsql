"""Smoke tests for the E2E test harness infrastructure."""

from __future__ import annotations

from pathlib import Path

from tests.e2e.conftest import (
    CLIResult,
    read_sink_csv,
    run_cli,
    write_joint,
    write_sink,
    write_source,
)


class TestRivetProjectFixture:
    """Verify the rivet_project fixture creates a valid scaffold."""

    def test_creates_rivet_yaml(self, rivet_project: Path) -> None:
        assert (rivet_project / "rivet.yaml").exists()

    def test_creates_profiles_yaml(self, rivet_project: Path) -> None:
        assert (rivet_project / "profiles.yaml").exists()

    def test_creates_standard_directories(self, rivet_project: Path) -> None:
        for d in ("sources", "joints", "sinks", "tests", "quality", "data"):
            assert (rivet_project / d).is_dir()

    def test_profiles_has_two_engines(self, rivet_project: Path) -> None:
        content = (rivet_project / "profiles.yaml").read_text()
        assert "duckdb_primary" in content
        assert "duckdb_secondary" in content

    def test_profiles_has_filesystem_catalog(self, rivet_project: Path) -> None:
        content = (rivet_project / "profiles.yaml").read_text()
        assert "type: filesystem" in content
        assert "local" in content


class TestRunCli:
    """Verify run_cli invokes _main and captures output."""

    def test_returns_cli_result(self, rivet_project: Path, capsys) -> None:
        result = run_cli(rivet_project, ["compile"], capsys)
        assert isinstance(result, CLIResult)
        assert isinstance(result.exit_code, int)
        assert isinstance(result.stdout, str)
        assert isinstance(result.stderr, str)


class TestWriteSource:
    def test_creates_source_file(self, rivet_project: Path) -> None:
        write_source(rivet_project, "raw_orders", catalog="local", table="raw_orders.csv")
        path = rivet_project / "sources" / "raw_orders.sql"
        assert path.exists()
        content = path.read_text()
        assert "-- rivet:name: raw_orders" in content
        assert "-- rivet:type: source" in content
        assert "-- rivet:catalog: local" in content
        assert "-- rivet:table: raw_orders.csv" in content


class TestWriteJoint:
    def test_creates_joint_file(self, rivet_project: Path) -> None:
        write_joint(rivet_project, "transform", "SELECT id FROM raw_orders WHERE amount > 0")
        path = rivet_project / "joints" / "transform.sql"
        assert path.exists()
        content = path.read_text()
        assert "-- rivet:name: transform" in content
        assert "-- rivet:type: sql" in content
        assert "SELECT id FROM raw_orders WHERE amount > 0" in content

    def test_includes_engine_annotation(self, rivet_project: Path) -> None:
        write_joint(rivet_project, "j1", "SELECT 1", engine="duckdb_secondary")
        content = (rivet_project / "joints" / "j1.sql").read_text()
        assert "-- rivet:engine: duckdb_secondary" in content

    def test_omits_engine_when_none(self, rivet_project: Path) -> None:
        write_joint(rivet_project, "j2", "SELECT 1")
        content = (rivet_project / "joints" / "j2.sql").read_text()
        assert "rivet:engine" not in content


class TestWriteSink:
    def test_creates_sink_file(self, rivet_project: Path) -> None:
        write_sink(rivet_project, "out", catalog="local", table="out", upstream=["transform"])
        path = rivet_project / "sinks" / "out.sql"
        assert path.exists()
        content = path.read_text()
        assert "-- rivet:name: out" in content
        assert "-- rivet:type: sink" in content
        assert "-- rivet:catalog: local" in content
        assert "-- rivet:table: out" in content
        assert "-- rivet:upstream: [transform]" in content

    def test_multiple_upstreams(self, rivet_project: Path) -> None:
        write_sink(rivet_project, "s1", catalog="local", table="s1", upstream=["a", "b"])
        content = (rivet_project / "sinks" / "s1.sql").read_text()
        assert "-- rivet:upstream: [a, b]" in content


class TestReadSinkCsv:
    def test_reads_csv_from_data_dir(self, rivet_project: Path) -> None:
        csv_content = "id,name,amount\n1,Alice,100\n2,Bob,200\n"
        (rivet_project / "data" / "output.csv").write_text(csv_content)
        table = read_sink_csv(rivet_project, "output")
        assert table.num_rows == 2
        assert table.column_names == ["id", "name", "amount"]
