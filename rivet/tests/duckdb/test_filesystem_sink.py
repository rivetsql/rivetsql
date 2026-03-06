"""Tests for task 6.3: DuckDB Sink — filesystem write strategies (append, replace, partition)."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from rivet_core.errors import ExecutionError
from rivet_core.models import Catalog, Joint, Material
from rivet_core.plugins import PluginRegistry, SinkPlugin
from rivet_core.strategies import _ArrowMaterializedRef
from rivet_duckdb.filesystem_sink import FILESYSTEM_SUPPORTED_STRATEGIES, FilesystemSink

# ── Helpers ─────────────────────────────────────────────────────────────────


def _make_material(table: pa.Table) -> Material:
    ref = _ArrowMaterializedRef(table)
    return Material(name="j1", catalog="fs_cat", materialized_ref=ref, state="materialized")


def _make_catalog(path: str, fmt: str = "parquet") -> Catalog:
    return Catalog(name="fs_cat", type="filesystem", options={"path": path, "format": fmt})


def _make_joint(name: str = "output", table: str | None = "output", strategy: str = "replace", config: dict | None = None) -> Joint:
    j = Joint(name=name, joint_type="sink", catalog="fs_cat", table=table, write_strategy=strategy)
    if config:
        j.write_strategy_config = config  # type: ignore[attr-defined]
    return j


# ── Registration ─────────────────────────────────────────────────────────────


def test_filesystem_sink_catalog_type():
    assert FilesystemSink.catalog_type == "filesystem"


def test_filesystem_sink_is_sink_plugin():
    assert isinstance(FilesystemSink(), SinkPlugin)


def test_filesystem_sink_supported_strategies():
    assert {"append", "replace", "partition"} == FILESYSTEM_SUPPORTED_STRATEGIES


def test_registry_can_register_filesystem_sink():
    registry = PluginRegistry()
    registry.register_sink(FilesystemSink())
    assert registry._sinks.get("filesystem") is not None


# ── Unsupported strategy ──────────────────────────────────────────────────────


def test_unsupported_strategy_raises():
    sink = FilesystemSink()
    with tempfile.TemporaryDirectory() as tmpdir:
        mat = _make_material(pa.table({"x": [1]}))
        with pytest.raises(ExecutionError) as exc_info:
            sink.write(_make_catalog(tmpdir), _make_joint(), mat, "merge")
        assert exc_info.value.error.code == "RVT-501"


# ── Replace strategy ──────────────────────────────────────────────────────────


def test_replace_creates_parquet_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = FilesystemSink()
        data = pa.table({"id": [1, 2], "val": ["a", "b"]})
        mat = _make_material(data)
        sink.write(_make_catalog(tmpdir), _make_joint(), mat, "replace")

        out = Path(tmpdir) / "output.parquet"
        assert out.exists()
        result = pq.read_table(str(out))
        assert result.num_rows == 2
        assert set(result.column("id").to_pylist()) == {1, 2}


def test_replace_overwrites_existing_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "output.parquet"
        pq.write_table(pa.table({"id": [99]}), str(out))

        sink = FilesystemSink()
        data = pa.table({"id": [1]})
        mat = _make_material(data)
        sink.write(_make_catalog(tmpdir), _make_joint(), mat, "replace")

        result = pq.read_table(str(out))
        assert result.num_rows == 1
        assert result.column("id").to_pylist() == [1]


def test_replace_with_explicit_file_path():
    with tempfile.TemporaryDirectory() as tmpdir:
        out_path = str(Path(tmpdir) / "myfile.parquet")
        sink = FilesystemSink()
        data = pa.table({"x": [10, 20]})
        mat = _make_material(data)
        # catalog path points directly to a file
        sink.write(_make_catalog(out_path), _make_joint(), mat, "replace")

        result = pq.read_table(out_path)
        assert result.num_rows == 2


# ── Append strategy ───────────────────────────────────────────────────────────


def test_append_creates_file_if_missing():
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = FilesystemSink()
        data = pa.table({"id": [1]})
        mat = _make_material(data)
        sink.write(_make_catalog(tmpdir), _make_joint(), mat, "append")

        out = Path(tmpdir) / "output.parquet"
        assert out.exists()
        result = pq.read_table(str(out))
        assert result.num_rows == 1


def test_append_adds_to_existing_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        out = Path(tmpdir) / "output.parquet"
        pq.write_table(pa.table({"id": [1, 2]}), str(out))

        sink = FilesystemSink()
        data = pa.table({"id": [3, 4]})
        mat = _make_material(data)
        sink.write(_make_catalog(tmpdir), _make_joint(), mat, "append")

        result = pq.read_table(str(out))
        assert result.num_rows == 4
        assert sorted(result.column("id").to_pylist()) == [1, 2, 3, 4]


def test_append_multiple_times():
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = FilesystemSink()
        for i in range(3):
            data = pa.table({"id": [i]})
            mat = _make_material(data)
            sink.write(_make_catalog(tmpdir), _make_joint(), mat, "append")

        out = Path(tmpdir) / "output.parquet"
        result = pq.read_table(str(out))
        assert result.num_rows == 3


# ── Partition strategy ────────────────────────────────────────────────────────


def _read_parquet_file(path: Path) -> pa.Table:
    """Read a single parquet file directly (avoids dataset directory scan)."""
    return pq.ParquetFile(str(path)).read()


def test_partition_creates_subdirectories():
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = FilesystemSink()
        data = pa.table({"region": ["us", "eu", "us"], "id": [1, 2, 3]})
        mat = _make_material(data)
        joint = _make_joint(config={"partition_by": ["region"]})
        sink.write(_make_catalog(tmpdir), joint, mat, "partition")

        us_dir = Path(tmpdir) / "region=us"
        eu_dir = Path(tmpdir) / "region=eu"
        assert us_dir.exists()
        assert eu_dir.exists()

        us_data = _read_parquet_file(us_dir / "data.parquet")
        eu_data = _read_parquet_file(eu_dir / "data.parquet")
        assert us_data.num_rows == 2
        assert eu_data.num_rows == 1


def test_partition_replaces_existing_partition():
    with tempfile.TemporaryDirectory() as tmpdir:
        # Write initial data
        sink = FilesystemSink()
        data1 = pa.table({"region": ["us", "eu"], "id": [1, 2]})
        mat1 = _make_material(data1)
        joint = _make_joint(config={"partition_by": ["region"]})
        sink.write(_make_catalog(tmpdir), joint, mat1, "partition")

        # Overwrite only 'us' partition
        data2 = pa.table({"region": ["us"], "id": [99]})
        mat2 = _make_material(data2)
        sink.write(_make_catalog(tmpdir), joint, mat2, "partition")

        us_data = _read_parquet_file(Path(tmpdir) / "region=us" / "data.parquet")
        eu_data = _read_parquet_file(Path(tmpdir) / "region=eu" / "data.parquet")
        assert us_data.column("id").to_pylist() == [99]
        assert eu_data.column("id").to_pylist() == [2]


def test_partition_without_columns_falls_back_to_replace():
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = FilesystemSink()
        data = pa.table({"id": [1, 2]})
        mat = _make_material(data)
        # No partition_by config
        sink.write(_make_catalog(tmpdir), _make_joint(), mat, "partition")

        out = Path(tmpdir) / "output.parquet"
        assert out.exists()
        result = pq.read_table(str(out))
        assert result.num_rows == 2


def test_partition_multi_column():
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = FilesystemSink()
        data = pa.table({
            "region": ["us", "us", "eu"],
            "year": [2023, 2024, 2023],
            "val": [1, 2, 3],
        })
        mat = _make_material(data)
        joint = _make_joint(config={"partition_by": ["region", "year"]})
        sink.write(_make_catalog(tmpdir), joint, mat, "partition")

        us_2023 = Path(tmpdir) / "region=us" / "year=2023" / "data.parquet"
        us_2024 = Path(tmpdir) / "region=us" / "year=2024" / "data.parquet"
        eu_2023 = Path(tmpdir) / "region=eu" / "year=2023" / "data.parquet"
        assert us_2023.exists()
        assert us_2024.exists()
        assert eu_2023.exists()


# ── CSV format ────────────────────────────────────────────────────────────────


def test_replace_csv_format():
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = FilesystemSink()
        data = pa.table({"id": [1, 2], "name": ["alice", "bob"]})
        mat = _make_material(data)
        sink.write(_make_catalog(tmpdir, fmt="csv"), _make_joint(), mat, "replace")

        out = Path(tmpdir) / "output.csv"
        assert out.exists()
        import pyarrow.csv as pcsv
        result = pcsv.read_csv(str(out))
        assert result.num_rows == 2


def test_append_csv_format():
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = FilesystemSink()
        data1 = pa.table({"id": [1]})
        mat1 = _make_material(data1)
        sink.write(_make_catalog(tmpdir, fmt="csv"), _make_joint(), mat1, "append")

        data2 = pa.table({"id": [2]})
        mat2 = _make_material(data2)
        sink.write(_make_catalog(tmpdir, fmt="csv"), _make_joint(), mat2, "append")

        import pyarrow.csv as pcsv
        result = pcsv.read_csv(str(Path(tmpdir) / "output.csv"))
        assert result.num_rows == 2


# ── JSON format ───────────────────────────────────────────────────────────────


def test_replace_json_format():
    with tempfile.TemporaryDirectory() as tmpdir:
        sink = FilesystemSink()
        data = pa.table({"id": [1, 2], "val": ["x", "y"]})
        mat = _make_material(data)
        sink.write(_make_catalog(tmpdir, fmt="json"), _make_joint(), mat, "replace")

        out = Path(tmpdir) / "output.json"
        assert out.exists()
        lines = out.read_text().strip().split("\n")
        assert len(lines) == 2
        row = json.loads(lines[0])
        assert "id" in row
