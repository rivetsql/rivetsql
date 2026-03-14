"""Integration tests for IPC write support in the filesystem sink.

Exercises the FilesystemSink writing IPC format with replace, append, and
partition strategies, and verifies round-trip with FilesystemSource.
Requirements: 10.1, 10.2, 10.3.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from rivet_core.builtins.filesystem_catalog import FilesystemSource
from rivet_core.models import Catalog, Joint, Material
from rivet_core.strategies import ArrowMaterialization, MaterializationContext
from rivet_duckdb.filesystem_sink import FilesystemSink


def _make_table() -> pa.Table:
    return pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})


def _make_material(table: pa.Table, name: str = "test_joint") -> Material:
    ref = ArrowMaterialization().materialize(
        table, MaterializationContext(joint_name=name, strategy_name="arrow", options={})
    )
    return Material(
        name=name,
        catalog="local",
        table=name,
        state="materialized",
        materialized_ref=ref,
    )


def _make_joint(name: str = "test_joint", **kwargs: object) -> Joint:
    return Joint(name=name, joint_type="sink", **kwargs)


def test_ipc_write_replace_strategy(tmp_path: Path):
    """Filesystem sink writes IPC with replace strategy, read back matches."""
    out_file = tmp_path / "output.arrow"
    catalog = Catalog(
        name="local", type="filesystem", options={"path": str(out_file), "format": "ipc"}
    )
    joint = _make_joint()
    table = _make_table()
    material = _make_material(table)

    sink = FilesystemSink()
    sink.write(catalog, joint, material, strategy="replace")

    assert out_file.exists()
    result = pa.ipc.open_file(str(out_file)).read_all()
    assert result.equals(table)


def test_ipc_write_append_strategy(tmp_path: Path):
    """Filesystem sink writes IPC with append strategy, read back matches."""
    out_file = tmp_path / "output.arrow"
    catalog = Catalog(
        name="local", type="filesystem", options={"path": str(out_file), "format": "ipc"}
    )
    joint = _make_joint()
    table = _make_table()

    sink = FilesystemSink()
    # First write
    sink.write(catalog, joint, _make_material(table), strategy="append")
    # Second write (append)
    sink.write(catalog, joint, _make_material(table), strategy="append")

    result = pa.ipc.open_file(str(out_file)).read_all()
    expected = pa.concat_tables([table, table])
    assert result.equals(expected)


def test_ipc_write_partition_strategy(tmp_path: Path):
    """Filesystem sink writes IPC with partition strategy, read back matches."""
    catalog = Catalog(
        name="local", type="filesystem", options={"path": str(tmp_path), "format": "ipc"}
    )
    joint = _make_joint(write_strategy="partition")
    joint.write_strategy_config = {"partition_by": ["name"]}  # type: ignore[attr-defined]
    table = _make_table()
    material = _make_material(table)

    sink = FilesystemSink()
    sink.write(catalog, joint, material, strategy="partition")

    # Each partition value should have its own subdirectory with a data.arrow file
    for name_val in ["a", "b", "c"]:
        part_file = tmp_path / f"name={name_val}" / "data.arrow"
        assert part_file.exists(), f"Missing partition file for name={name_val}"
        part_table = pa.ipc.open_file(str(part_file)).read_all()
        assert part_table.num_rows == 1


def test_ipc_round_trip_source_reads_sink_output(tmp_path: Path):
    """Round-trip: filesystem source reads what filesystem sink wrote in IPC format."""
    out_file = tmp_path / "data.arrow"
    catalog = Catalog(
        name="local", type="filesystem", options={"path": str(out_file), "format": "ipc"}
    )
    joint_sink = _make_joint(name="writer")
    table = _make_table()
    material = _make_material(table, name="writer")

    # Write via sink
    sink = FilesystemSink()
    sink.write(catalog, joint_sink, material, strategy="replace")

    # Read via source
    source = FilesystemSource()
    joint_source = Joint(name="reader", joint_type="source", table=None, path=str(out_file))
    read_material = source.read(catalog, joint_source)

    assert read_material.to_arrow().equals(table)
