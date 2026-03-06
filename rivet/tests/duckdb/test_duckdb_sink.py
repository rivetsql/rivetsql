"""Tests for task 6.2: DuckDB Sink — all 8 write strategies for duckdb catalog."""

from __future__ import annotations

import duckdb
import pyarrow as pa
import pytest

from rivet_core.errors import ExecutionError
from rivet_core.models import Catalog, Joint, Material
from rivet_core.plugins import PluginRegistry, SinkPlugin
from rivet_core.strategies import _ArrowMaterializedRef
from rivet_duckdb.sink import SUPPORTED_STRATEGIES, DuckDBSink

# ── Helpers ─────────────────────────────────────────────────────────────────


def _make_material(name: str, table: pa.Table) -> Material:
    ref = _ArrowMaterializedRef(table)
    return Material(name=name, catalog="testdb", materialized_ref=ref, state="materialized")


def _make_catalog(path: str = ":memory:", read_only: bool = False) -> Catalog:
    return Catalog(name="testdb", type="duckdb", options={"path": path, "read_only": read_only})


def _make_joint(name: str = "j1", table: str = "target", strategy: str = "replace", config: dict | None = None) -> Joint:
    j = Joint(name=name, joint_type="sink", catalog="testdb", table=table, write_strategy=strategy)
    if config:
        j.write_strategy_config = config  # type: ignore[attr-defined]
    return j


def _read_table(path: str, table: str) -> pa.Table:
    conn = duckdb.connect(path, read_only=True)
    try:
        return conn.execute(f"SELECT * FROM {table}").arrow().read_all()
    finally:
        conn.close()


# ── Registration ────────────────────────────────────────────────────────────


def test_sink_has_catalog_type():
    assert DuckDBSink.catalog_type == "duckdb"


def test_sink_is_sink_plugin():
    assert isinstance(DuckDBSink(), SinkPlugin)


def test_registry_can_register_sink():
    registry = PluginRegistry()
    registry.register_sink(DuckDBSink())
    assert registry._sinks.get("duckdb") is not None


def test_supported_strategies_has_all_8():
    expected = {"append", "replace", "truncate_insert", "merge", "delete_insert", "incremental_append", "scd2", "partition"}
    assert expected == SUPPORTED_STRATEGIES


# ── Unsupported strategy ───────────────────────────────────────────────────


def test_unsupported_strategy_raises():
    sink = DuckDBSink()
    data = pa.table({"x": [1]})
    mat = _make_material("j1", data)
    with pytest.raises(ExecutionError) as exc_info:
        sink.write(_make_catalog(), _make_joint(strategy="bogus"), mat, "bogus")
    assert exc_info.value.error.code == "RVT-501"


# ── Read-only catalog ──────────────────────────────────────────────────────


def test_read_only_catalog_raises():
    sink = DuckDBSink()
    data = pa.table({"x": [1]})
    mat = _make_material("j1", data)
    with pytest.raises(ExecutionError) as exc_info:
        sink.write(_make_catalog(read_only=True), _make_joint(), mat, "replace")
    assert exc_info.value.error.code == "RVT-201"


# ── Replace strategy ───────────────────────────────────────────────────────


def test_replace_creates_table():
    import os
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    os.unlink(db_path)
    try:
        sink = DuckDBSink()
        data = pa.table({"id": [1, 2], "val": ["a", "b"]})
        mat = _make_material("j1", data)
        sink.write(_make_catalog(db_path), _make_joint(strategy="replace"), mat, "replace")
        result = _read_table(db_path, "target")
        assert result.num_rows == 2
        assert set(result.column("id").to_pylist()) == {1, 2}
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_replace_overwrites_existing():
    import os
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    os.unlink(db_path)
    try:
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE target (id INTEGER, val VARCHAR)")
        conn.execute("INSERT INTO target VALUES (99, 'old')")
        conn.close()

        sink = DuckDBSink()
        data = pa.table({"id": [1], "val": ["new"]})
        mat = _make_material("j1", data)
        sink.write(_make_catalog(db_path), _make_joint(strategy="replace"), mat, "replace")
        result = _read_table(db_path, "target")
        assert result.num_rows == 1
        assert result.column("val").to_pylist() == ["new"]
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


# ── Append strategy ────────────────────────────────────────────────────────


def test_append_creates_table_if_missing():
    import os
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    os.unlink(db_path)
    try:
        sink = DuckDBSink()
        data = pa.table({"id": [1]})
        mat = _make_material("j1", data)
        sink.write(_make_catalog(db_path), _make_joint(strategy="append"), mat, "append")
        result = _read_table(db_path, "target")
        assert result.num_rows == 1
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_append_adds_to_existing():
    import os
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    os.unlink(db_path)
    try:
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE target (id INTEGER)")
        conn.execute("INSERT INTO target VALUES (1)")
        conn.close()

        sink = DuckDBSink()
        data = pa.table({"id": [2, 3]})
        mat = _make_material("j1", data)
        sink.write(_make_catalog(db_path), _make_joint(strategy="append"), mat, "append")
        result = _read_table(db_path, "target")
        assert result.num_rows == 3
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


# ── Truncate-insert strategy ──────────────────────────────────────────────


def test_truncate_insert():
    import os
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    os.unlink(db_path)
    try:
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE target (id INTEGER)")
        conn.execute("INSERT INTO target VALUES (1), (2), (3)")
        conn.close()

        sink = DuckDBSink()
        data = pa.table({"id": [10]})
        mat = _make_material("j1", data)
        sink.write(_make_catalog(db_path), _make_joint(strategy="truncate_insert"), mat, "truncate_insert")
        result = _read_table(db_path, "target")
        assert result.num_rows == 1
        assert result.column("id").to_pylist() == [10]
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


# ── Merge strategy ─────────────────────────────────────────────────────────


def test_merge_simple_insert_or_replace():
    import os
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    os.unlink(db_path)
    try:
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE target (id INTEGER PRIMARY KEY, val VARCHAR)")
        conn.execute("INSERT INTO target VALUES (1, 'old')")
        conn.close()

        sink = DuckDBSink()
        data = pa.table({"id": [1, 2], "val": ["updated", "new"]})
        mat = _make_material("j1", data)
        sink.write(_make_catalog(db_path), _make_joint(strategy="merge"), mat, "merge")
        result = _read_table(db_path, "target")
        assert result.num_rows == 2
        rows = {r["id"]: r["val"] for r in result.to_pydict().items()} if False else dict(zip(result.column("id").to_pylist(), result.column("val").to_pylist()))
        assert rows[1] == "updated"
        assert rows[2] == "new"
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_merge_with_key_columns():
    import os
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    os.unlink(db_path)
    try:
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE target (id INTEGER, val VARCHAR)")
        conn.execute("INSERT INTO target VALUES (1, 'old'), (2, 'keep')")
        conn.close()

        sink = DuckDBSink()
        data = pa.table({"id": [1, 3], "val": ["updated", "new"]})
        mat = _make_material("j1", data)
        joint = _make_joint(strategy="merge", config={"merge_key": ["id"]})
        sink.write(_make_catalog(db_path), joint, mat, "merge")
        result = _read_table(db_path, "target")
        assert result.num_rows == 3
        rows = dict(zip(result.column("id").to_pylist(), result.column("val").to_pylist()))
        assert rows[1] == "updated"
        assert rows[2] == "keep"
        assert rows[3] == "new"
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


# ── Delete-insert strategy ─────────────────────────────────────────────────


def test_delete_insert_with_keys():
    import os
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    os.unlink(db_path)
    try:
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE target (id INTEGER, val VARCHAR)")
        conn.execute("INSERT INTO target VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        conn.close()

        sink = DuckDBSink()
        data = pa.table({"id": [1, 4], "val": ["updated", "new"]})
        mat = _make_material("j1", data)
        joint = _make_joint(strategy="delete_insert", config={"merge_key": ["id"]})
        sink.write(_make_catalog(db_path), joint, mat, "delete_insert")
        result = _read_table(db_path, "target")
        rows = dict(zip(result.column("id").to_pylist(), result.column("val").to_pylist()))
        assert rows[1] == "updated"
        assert rows[2] == "b"
        assert rows[3] == "c"
        assert rows[4] == "new"
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_delete_insert_without_keys_replaces_all():
    import os
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    os.unlink(db_path)
    try:
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE target (id INTEGER)")
        conn.execute("INSERT INTO target VALUES (1), (2)")
        conn.close()

        sink = DuckDBSink()
        data = pa.table({"id": [10]})
        mat = _make_material("j1", data)
        sink.write(_make_catalog(db_path), _make_joint(strategy="delete_insert"), mat, "delete_insert")
        result = _read_table(db_path, "target")
        assert result.num_rows == 1
        assert result.column("id").to_pylist() == [10]
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


# ── Incremental append strategy ────────────────────────────────────────────


def test_incremental_append_skips_duplicates():
    import os
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    os.unlink(db_path)
    try:
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE target (id INTEGER, val VARCHAR)")
        conn.execute("INSERT INTO target VALUES (1, 'a')")
        conn.close()

        sink = DuckDBSink()
        data = pa.table({"id": [1, 2], "val": ["a", "b"]})
        mat = _make_material("j1", data)
        joint = _make_joint(strategy="incremental_append", config={"merge_key": ["id"]})
        sink.write(_make_catalog(db_path), joint, mat, "incremental_append")
        result = _read_table(db_path, "target")
        assert result.num_rows == 2
        ids = sorted(result.column("id").to_pylist())
        assert ids == [1, 2]
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


# ── SCD2 strategy ──────────────────────────────────────────────────────────


def test_scd2_creates_table_with_tracking_columns():
    import os
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    os.unlink(db_path)
    try:
        sink = DuckDBSink()
        data = pa.table({"id": [1], "val": ["a"]})
        mat = _make_material("j1", data)
        joint = _make_joint(strategy="scd2", config={"merge_key": ["id"]})
        sink.write(_make_catalog(db_path), joint, mat, "scd2")

        conn = duckdb.connect(db_path, read_only=True)
        cols = [row[0] for row in conn.execute("SELECT column_name FROM duckdb_columns() WHERE table_name = 'target'").fetchall()]
        conn.close()
        assert "valid_from" in cols
        assert "valid_to" in cols
        assert "is_current" in cols
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_scd2_inserts_new_records_as_current():
    import os
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    os.unlink(db_path)
    try:
        sink = DuckDBSink()
        data = pa.table({"id": [1], "val": ["a"]})
        mat = _make_material("j1", data)
        joint = _make_joint(strategy="scd2", config={"merge_key": ["id"]})
        sink.write(_make_catalog(db_path), joint, mat, "scd2")

        conn = duckdb.connect(db_path, read_only=True)
        result = conn.execute("SELECT is_current FROM target WHERE id = 1").fetchone()
        conn.close()
        assert result[0] is True
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


# ── Partition strategy ──────────────────────────────────────────────────────


def test_partition_replaces_matching_partitions():
    import os
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    os.unlink(db_path)
    try:
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE target (region VARCHAR, id INTEGER)")
        conn.execute("INSERT INTO target VALUES ('us', 1), ('eu', 2)")
        conn.close()

        sink = DuckDBSink()
        data = pa.table({"region": ["us"], "id": [10]})
        mat = _make_material("j1", data)
        joint = _make_joint(strategy="partition", config={"partition_by": ["region"]})
        sink.write(_make_catalog(db_path), joint, mat, "partition")
        result = _read_table(db_path, "target")
        rows = dict(zip(result.column("region").to_pylist(), result.column("id").to_pylist()))
        assert rows["us"] == 10
        assert rows["eu"] == 2
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_partition_without_columns_falls_back_to_replace():
    import os
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    os.unlink(db_path)
    try:
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE target (id INTEGER)")
        conn.execute("INSERT INTO target VALUES (1), (2)")
        conn.close()

        sink = DuckDBSink()
        data = pa.table({"id": [10]})
        mat = _make_material("j1", data)
        sink.write(_make_catalog(db_path), _make_joint(strategy="partition"), mat, "partition")
        result = _read_table(db_path, "target")
        assert result.num_rows == 1
        assert result.column("id").to_pylist() == [10]
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


# ── Table name from joint ──────────────────────────────────────────────────


def test_uses_joint_name_when_no_table():
    import os
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    os.unlink(db_path)
    try:
        sink = DuckDBSink()
        data = pa.table({"x": [1]})
        mat = _make_material("j1", data)
        joint = Joint(name="my_output", joint_type="sink", catalog="testdb", write_strategy="replace")
        sink.write(_make_catalog(db_path), joint, mat, "replace")
        result = _read_table(db_path, "my_output")
        assert result.num_rows == 1
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)
