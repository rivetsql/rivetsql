"""Tests for task 6.1: DuckDB Source — execute SQL against DuckDB, return deferred Material."""

from __future__ import annotations

import duckdb
import pyarrow as pa
import pytest

from rivet_core.errors import ExecutionError
from rivet_core.models import Catalog, Joint, Material
from rivet_core.plugins import PluginRegistry, SourcePlugin
from rivet_duckdb.source import DuckDBDeferredMaterializedRef, DuckDBSource

# ── DuckDBSource registration ──────────────────────────────────────────────


def test_source_has_catalog_type():
    assert DuckDBSource.catalog_type == "duckdb"


def test_source_is_source_plugin():
    assert isinstance(DuckDBSource(), SourcePlugin)


def test_registry_can_register_source():
    registry = PluginRegistry()
    registry.register_source(DuckDBSource())
    assert registry._sources.get("duckdb") is not None


# ── DuckDBDeferredMaterializedRef ──────────────────────────────────────────


def test_deferred_ref_storage_type():
    ref = DuckDBDeferredMaterializedRef(":memory:", False, "SELECT 1 AS n")
    assert ref.storage_type == "duckdb"


def test_deferred_ref_size_bytes_is_none():
    ref = DuckDBDeferredMaterializedRef(":memory:", False, "SELECT 1 AS n")
    assert ref.size_bytes is None


def test_deferred_ref_to_arrow_executes_sql():
    ref = DuckDBDeferredMaterializedRef(":memory:", False, "SELECT 42 AS val")
    table = ref.to_arrow()
    assert isinstance(table, pa.Table)
    assert table.num_rows == 1
    assert table.column("val").to_pylist() == [42]


def test_deferred_ref_to_arrow_with_real_table():
    import os
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    os.unlink(db_path)  # remove so DuckDB can create a fresh database
    try:
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE items (id INTEGER, name VARCHAR)")
        conn.execute("INSERT INTO items VALUES (1, 'alpha'), (2, 'beta')")
        conn.close()

        ref = DuckDBDeferredMaterializedRef(db_path, False, "SELECT * FROM items ORDER BY id")
        table = ref.to_arrow()
        assert table.num_rows == 2
        assert table.column("id").to_pylist() == [1, 2]
        assert table.column("name").to_pylist() == ["alpha", "beta"]
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_deferred_ref_row_count():
    ref = DuckDBDeferredMaterializedRef(":memory:", False, "SELECT unnest([1,2,3]) AS n")
    assert ref.row_count == 3


def test_deferred_ref_schema():
    ref = DuckDBDeferredMaterializedRef(":memory:", False, "SELECT 1::INTEGER AS a, 'x'::VARCHAR AS b")
    schema = ref.schema
    names = [c.name for c in schema.columns]
    assert "a" in names
    assert "b" in names


def test_deferred_ref_invalid_sql_raises_execution_error():
    ref = DuckDBDeferredMaterializedRef(":memory:", False, "SELECT * FROM nonexistent_table_xyz")
    with pytest.raises(ExecutionError) as exc_info:
        ref.to_arrow()
    assert exc_info.value.error.code == "RVT-501"


# ── DuckDBSource.read() ────────────────────────────────────────────────────


def test_read_returns_material():
    source = DuckDBSource()
    catalog = Catalog(name="mydb", type="duckdb", options={"path": ":memory:"})
    joint = Joint(name="j1", joint_type="source", catalog="mydb", sql="SELECT 1 AS n")
    result = source.read(catalog, joint, None)
    assert isinstance(result, Material)


def test_read_returns_deferred_state():
    source = DuckDBSource()
    catalog = Catalog(name="mydb", type="duckdb", options={"path": ":memory:"})
    joint = Joint(name="j1", joint_type="source", catalog="mydb", sql="SELECT 1 AS n")
    result = source.read(catalog, joint, None)
    assert result.state == "deferred"


def test_read_material_has_ref():
    source = DuckDBSource()
    catalog = Catalog(name="mydb", type="duckdb", options={"path": ":memory:"})
    joint = Joint(name="j1", joint_type="source", catalog="mydb", sql="SELECT 1 AS n")
    result = source.read(catalog, joint, None)
    assert result.materialized_ref is not None
    assert isinstance(result.materialized_ref, DuckDBDeferredMaterializedRef)


def test_read_material_name_matches_joint():
    source = DuckDBSource()
    catalog = Catalog(name="mydb", type="duckdb", options={"path": ":memory:"})
    joint = Joint(name="my_joint", joint_type="source", catalog="mydb", sql="SELECT 1 AS n")
    result = source.read(catalog, joint, None)
    assert result.name == "my_joint"


def test_read_material_catalog_matches():
    source = DuckDBSource()
    catalog = Catalog(name="mydb", type="duckdb", options={"path": ":memory:"})
    joint = Joint(name="j1", joint_type="source", catalog="mydb", sql="SELECT 1 AS n")
    result = source.read(catalog, joint, None)
    assert result.catalog == "mydb"


def test_read_to_arrow_executes_sql():
    source = DuckDBSource()
    catalog = Catalog(name="mydb", type="duckdb", options={"path": ":memory:"})
    joint = Joint(name="j1", joint_type="source", catalog="mydb", sql="SELECT 99 AS val")
    result = source.read(catalog, joint, None)
    table = result.to_arrow()
    assert isinstance(table, pa.Table)
    assert table.column("val").to_pylist() == [99]


def test_read_is_deferred_not_eager():
    """Material is deferred — no SQL execution until to_arrow() is called."""
    source = DuckDBSource()
    catalog = Catalog(name="mydb", type="duckdb", options={"path": ":memory:"})
    # Invalid SQL — should not raise until to_arrow() is called
    joint = Joint(name="j1", joint_type="source", catalog="mydb", sql="SELECT * FROM no_such_table")
    result = source.read(catalog, joint, None)
    # No exception yet — deferred
    assert result.state == "deferred"
    # Now trigger execution
    with pytest.raises(ExecutionError):
        result.to_arrow()


def test_read_with_file_backed_database():
    import os
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    os.unlink(db_path)
    try:
        conn = duckdb.connect(db_path)
        conn.execute("CREATE TABLE scores (player VARCHAR, score INTEGER)")
        conn.execute("INSERT INTO scores VALUES ('alice', 100), ('bob', 200)")
        conn.close()

        source = DuckDBSource()
        catalog = Catalog(name="gamedb", type="duckdb", options={"path": db_path})
        joint = Joint(
            name="top_scores",
            joint_type="source",
            catalog="gamedb",
            sql="SELECT player, score FROM scores ORDER BY score DESC",
        )
        result = source.read(catalog, joint, None)
        table = result.to_arrow()
        assert table.num_rows == 2
        assert table.column("player").to_pylist() == ["bob", "alice"]
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_read_with_default_path_is_memory():
    """Catalog with no path option defaults to :memory:."""
    source = DuckDBSource()
    catalog = Catalog(name="mydb", type="duckdb", options={})
    joint = Joint(name="j1", joint_type="source", catalog="mydb", sql="SELECT 7 AS x")
    result = source.read(catalog, joint, None)
    table = result.to_arrow()
    assert table.column("x").to_pylist() == [7]
