"""Tests for FilesystemCatalogPlugin and FilesystemSource."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest

from rivet_core.builtins.filesystem_catalog import FilesystemCatalogPlugin, FilesystemSource
from rivet_core.models import Catalog, Joint


@pytest.fixture
def sample_table() -> pa.Table:
    return pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})


@pytest.fixture
def tmp_parquet(sample_table: pa.Table, tmp_path: Path) -> Path:
    p = tmp_path / "data.parquet"
    pq.write_table(sample_table, str(p))
    return p


@pytest.fixture
def tmp_csv(tmp_path: Path) -> Path:
    p = tmp_path / "data.csv"
    pcsv.write_csv(pa.table({"x": [10, 20], "y": ["foo", "bar"]}), str(p))
    return p


@pytest.fixture
def tmp_json(tmp_path: Path) -> Path:
    p = tmp_path / "data.json"
    rows = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    with open(p, "w") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    return p


@pytest.fixture
def tmp_ipc(sample_table: pa.Table, tmp_path: Path) -> Path:
    p = tmp_path / "data.arrow"
    writer = ipc.new_file(str(p), sample_table.schema)
    writer.write_table(sample_table)
    writer.close()
    return p


class TestFilesystemCatalogPlugin:
    def test_type(self) -> None:
        plugin = FilesystemCatalogPlugin()
        assert plugin.type == "filesystem"

    def test_required_options(self) -> None:
        plugin = FilesystemCatalogPlugin()
        assert "path" in plugin.required_options

    def test_validate_missing_path(self) -> None:
        plugin = FilesystemCatalogPlugin()
        with pytest.raises(Exception):  # noqa: B017
            plugin.validate({})

    def test_validate_ok(self, tmp_path: Path) -> None:
        plugin = FilesystemCatalogPlugin()
        plugin.validate({"path": str(tmp_path)})

    def test_instantiate(self, tmp_path: Path) -> None:
        plugin = FilesystemCatalogPlugin()
        cat = plugin.instantiate("test_fs", {"path": str(tmp_path)})
        assert cat.name == "test_fs"
        assert cat.type == "filesystem"
        assert cat.options["path"] == str(tmp_path)

    def test_default_table_reference(self) -> None:
        plugin = FilesystemCatalogPlugin()
        assert plugin.default_table_reference("my_table", {}) == "my_table.parquet"
        assert plugin.default_table_reference("my_table", {"format": "csv"}) == "my_table.csv"

    def test_list_tables_directory(self, tmp_parquet: Path) -> None:
        plugin = FilesystemCatalogPlugin()
        cat = Catalog(name="fs", type="filesystem", options={"path": str(tmp_parquet.parent)})
        nodes = plugin.list_tables(cat)
        names = [n.name for n in nodes]
        assert "data" in names
        node = [n for n in nodes if n.name == "data"][0]
        assert node.node_type == "file"
        assert node.summary is not None
        assert node.summary.format == "parquet"
        assert node.summary.size_bytes is not None and node.summary.size_bytes > 0

    def test_list_tables_single_file(self, tmp_parquet: Path) -> None:
        plugin = FilesystemCatalogPlugin()
        cat = Catalog(name="fs", type="filesystem", options={"path": str(tmp_parquet)})
        nodes = plugin.list_tables(cat)
        assert len(nodes) == 1
        assert nodes[0].name == "data"

    def test_list_tables_nonexistent(self, tmp_path: Path) -> None:
        plugin = FilesystemCatalogPlugin()
        cat = Catalog(name="fs", type="filesystem", options={"path": str(tmp_path / "nope")})
        assert plugin.list_tables(cat) == []

    def test_get_schema_parquet(self, tmp_parquet: Path) -> None:
        plugin = FilesystemCatalogPlugin()
        cat = Catalog(name="fs", type="filesystem", options={"path": str(tmp_parquet.parent)})
        schema = plugin.get_schema(cat, "data.parquet")
        col_names = [c.name for c in schema.columns]
        assert "id" in col_names
        assert "name" in col_names

    def test_get_schema_by_stem(self, tmp_parquet: Path) -> None:
        plugin = FilesystemCatalogPlugin()
        cat = Catalog(name="fs", type="filesystem", options={"path": str(tmp_parquet.parent)})
        schema = plugin.get_schema(cat, "data")
        col_names = [c.name for c in schema.columns]
        assert "id" in col_names

    def test_get_metadata_parquet(self, tmp_parquet: Path) -> None:
        plugin = FilesystemCatalogPlugin()
        cat = Catalog(name="fs", type="filesystem", options={"path": str(tmp_parquet.parent)})
        meta = plugin.get_metadata(cat, "data.parquet")
        assert meta is not None
        assert meta.format == "parquet"
        assert meta.row_count == 3
        assert meta.size_bytes is not None and meta.size_bytes > 0
        assert meta.last_modified is not None

    def test_get_metadata_nonexistent(self, tmp_path: Path) -> None:
        plugin = FilesystemCatalogPlugin()
        cat = Catalog(name="fs", type="filesystem", options={"path": str(tmp_path)})
        assert plugin.get_metadata(cat, "nope.parquet") is None


class TestFilesystemSource:
    def test_read_parquet(self, tmp_parquet: Path) -> None:
        source = FilesystemSource()
        cat = Catalog(name="fs", type="filesystem", options={"path": str(tmp_parquet.parent)})
        joint = Joint(name="src", joint_type="source", catalog="fs", table="data.parquet")
        mat = source.read(cat, joint)
        assert mat.state == "materialized"
        tbl = mat.to_arrow()
        assert tbl.num_rows == 3
        assert "id" in tbl.column_names

    def test_read_csv(self, tmp_csv: Path) -> None:
        source = FilesystemSource()
        cat = Catalog(name="fs", type="filesystem", options={"path": str(tmp_csv.parent)})
        joint = Joint(name="src", joint_type="source", catalog="fs", table="data.csv")
        mat = source.read(cat, joint)
        tbl = mat.to_arrow()
        assert tbl.num_rows == 2
        assert "x" in tbl.column_names

    def test_read_json(self, tmp_json: Path) -> None:
        source = FilesystemSource()
        cat = Catalog(name="fs", type="filesystem", options={"path": str(tmp_json.parent)})
        joint = Joint(name="src", joint_type="source", catalog="fs", table="data.json")
        mat = source.read(cat, joint)
        tbl = mat.to_arrow()
        assert tbl.num_rows == 2
        assert "a" in tbl.column_names

    def test_read_ipc(self, tmp_ipc: Path) -> None:
        source = FilesystemSource()
        cat = Catalog(name="fs", type="filesystem", options={"path": str(tmp_ipc.parent)})
        joint = Joint(name="src", joint_type="source", catalog="fs", table="data.arrow")
        mat = source.read(cat, joint)
        tbl = mat.to_arrow()
        assert tbl.num_rows == 3

    def test_read_with_joint_path(self, tmp_parquet: Path) -> None:
        source = FilesystemSource()
        cat = Catalog(name="fs", type="filesystem", options={"path": str(tmp_parquet.parent)})
        joint = Joint(name="src", joint_type="source", catalog="fs", path=str(tmp_parquet))
        mat = source.read(cat, joint)
        assert mat.to_arrow().num_rows == 3

    def test_read_with_relative_joint_path(self, tmp_parquet: Path) -> None:
        source = FilesystemSource()
        cat = Catalog(name="fs", type="filesystem", options={"path": str(tmp_parquet.parent)})
        joint = Joint(name="src", joint_type="source", catalog="fs", path="data.parquet")
        mat = source.read(cat, joint)
        assert mat.to_arrow().num_rows == 3

    def test_read_directory(self, tmp_path: Path, sample_table: pa.Table) -> None:
        subdir = tmp_path / "parts"
        subdir.mkdir()
        pq.write_table(sample_table, str(subdir / "part1.parquet"))
        pq.write_table(sample_table, str(subdir / "part2.parquet"))
        source = FilesystemSource()
        cat = Catalog(name="fs", type="filesystem", options={"path": str(tmp_path)})
        joint = Joint(name="src", joint_type="source", catalog="fs", table="parts")
        mat = source.read(cat, joint)
        assert mat.to_arrow().num_rows == 6

    def test_read_csv_custom_delimiter(self, tmp_path: Path) -> None:
        p = tmp_path / "data.csv"
        with open(p, "w") as f:
            f.write("a|b\n1|x\n2|y\n")
        source = FilesystemSource()
        cat = Catalog(
            name="fs",
            type="filesystem",
            options={"path": str(tmp_path), "csv_delimiter": "|"},
        )
        joint = Joint(name="src", joint_type="source", catalog="fs", table="data.csv")
        mat = source.read(cat, joint)
        tbl = mat.to_arrow()
        assert tbl.num_rows == 2
        assert "a" in tbl.column_names
        assert "b" in tbl.column_names

    def test_format_auto_detection(self) -> None:
        """Verify format is detected from extension."""
        from rivet_core.builtins.filesystem_catalog import _detect_format

        assert _detect_format(Path("file.parquet"), {}) == "parquet"
        assert _detect_format(Path("file.csv"), {}) == "csv"
        assert _detect_format(Path("file.json"), {}) == "json"
        assert _detect_format(Path("file.arrow"), {}) == "ipc"
        assert _detect_format(Path("file.feather"), {}) == "ipc"
        assert _detect_format(Path("file.ipc"), {}) == "ipc"

    def test_format_override(self) -> None:
        from rivet_core.builtins.filesystem_catalog import _detect_format

        assert _detect_format(Path("file.txt"), {"format": "csv"}) == "csv"

    def test_schema_dict_populated(self, tmp_parquet: Path) -> None:
        source = FilesystemSource()
        cat = Catalog(name="fs", type="filesystem", options={"path": str(tmp_parquet.parent)})
        joint = Joint(name="src", joint_type="source", catalog="fs", table="data.parquet")
        mat = source.read(cat, joint)
        assert mat.schema is not None
        assert "id" in mat.schema
        assert "name" in mat.schema
