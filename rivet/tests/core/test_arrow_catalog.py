"""Tests for ArrowCatalogPlugin, ArrowComputeEnginePlugin, ArrowSource, and ArrowSink."""

from __future__ import annotations

import pyarrow as pa
import pytest

from rivet_core.builtins.arrow_catalog import (
    ArrowCatalogPlugin,
    ArrowComputeEnginePlugin,
    ArrowSink,
    ArrowSource,
    _get_shared_store,
)
from rivet_core.errors import PluginValidationError
from rivet_core.models import Catalog, Joint


@pytest.fixture(autouse=True)
def clear_arrow_tables():
    """Clear the shared in-memory store before each test."""
    _get_shared_store().clear()
    yield
    _get_shared_store().clear()


@pytest.fixture
def catalog() -> Catalog:
    return Catalog(name="mem", type="arrow", options={})


@pytest.fixture
def sample_table() -> pa.Table:
    return pa.table({"id": [1, 2, 3], "val": ["a", "b", "c"]})


# ── ArrowCatalogPlugin ────────────────────────────────────────────────


class TestArrowCatalogPlugin:
    def test_type(self) -> None:
        assert ArrowCatalogPlugin().type == "arrow"

    def test_validate_valid_options(self) -> None:
        ArrowCatalogPlugin().validate({"memory_limit": 1024, "spill_to_disk": True})

    def test_validate_unknown_option_raises(self) -> None:
        with pytest.raises(PluginValidationError):
            ArrowCatalogPlugin().validate({"unknown_key": "x"})

    def test_instantiate(self) -> None:
        cat = ArrowCatalogPlugin().instantiate("my_cat", {})
        assert cat.name == "my_cat"
        assert cat.type == "arrow"

    def test_default_table_reference(self) -> None:
        assert ArrowCatalogPlugin().default_table_reference("tbl", {}) == "tbl"

    def test_list_tables_empty(self, catalog: Catalog) -> None:
        nodes = ArrowCatalogPlugin().list_tables(catalog)
        assert nodes == []

    def test_list_tables_after_write(self, catalog: Catalog, sample_table: pa.Table) -> None:
        _get_shared_store()[("mem", "t1")] = sample_table
        nodes = ArrowCatalogPlugin().list_tables(catalog)
        assert len(nodes) == 1
        assert nodes[0].name == "t1"
        assert nodes[0].summary is not None
        assert nodes[0].summary.row_count == 3
        assert nodes[0].summary.size_bytes == sample_table.nbytes

    def test_list_tables_only_own_catalog(self, catalog: Catalog, sample_table: pa.Table) -> None:
        _get_shared_store()[("mem", "t1")] = sample_table
        _get_shared_store()[("other", "t2")] = sample_table
        nodes = ArrowCatalogPlugin().list_tables(catalog)
        assert len(nodes) == 1
        assert nodes[0].name == "t1"

    def test_get_schema(self, catalog: Catalog, sample_table: pa.Table) -> None:
        _get_shared_store()[("mem", "t1")] = sample_table
        schema = ArrowCatalogPlugin().get_schema(catalog, "t1")
        col_names = [c.name for c in schema.columns]
        assert "id" in col_names
        assert "val" in col_names

    def test_get_schema_missing_table_raises(self, catalog: Catalog) -> None:
        with pytest.raises(NotImplementedError):
            ArrowCatalogPlugin().get_schema(catalog, "nonexistent")

    def test_get_metadata(self, catalog: Catalog, sample_table: pa.Table) -> None:
        _get_shared_store()[("mem", "t1")] = sample_table
        meta = ArrowCatalogPlugin().get_metadata(catalog, "t1")
        assert meta is not None
        assert meta.row_count == 3
        assert meta.size_bytes == sample_table.nbytes
        assert meta.format == "arrow"

    def test_get_metadata_missing_returns_none(self, catalog: Catalog) -> None:
        assert ArrowCatalogPlugin().get_metadata(catalog, "nope") is None


# ── ArrowComputeEnginePlugin ──────────────────────────────────────────


class TestArrowComputeEnginePlugin:
    def test_engine_type(self) -> None:
        assert ArrowComputeEnginePlugin().engine_type == "arrow"

    def test_create_engine(self) -> None:
        engine = ArrowComputeEnginePlugin().create_engine("arrow", {})
        assert engine.name == "arrow"
        assert engine.engine_type == "arrow"

    def test_arrow_catalog_capabilities(self) -> None:
        caps = ArrowComputeEnginePlugin().supported_catalog_types["arrow"]
        assert "projection_pushdown" in caps
        assert "predicate_pushdown" in caps
        assert "limit_pushdown" in caps
        assert "cast_pushdown" in caps

    def test_filesystem_catalog_capabilities(self) -> None:
        caps = ArrowComputeEnginePlugin().supported_catalog_types["filesystem"]
        assert "projection_pushdown" in caps
        assert "predicate_pushdown" in caps
        assert "limit_pushdown" in caps
        assert "cast_pushdown" not in caps

    def test_validate_no_error(self) -> None:
        ArrowComputeEnginePlugin().validate({})


# ── ArrowSource ───────────────────────────────────────────────────────


class TestArrowSource:
    def test_read_existing_table(self, catalog: Catalog, sample_table: pa.Table) -> None:
        _get_shared_store()[("mem", "t1")] = sample_table
        joint = Joint(name="t1", joint_type="source", catalog="mem")
        mat = ArrowSource().read(catalog, joint)
        assert mat.state == "materialized"
        result = mat.to_arrow()
        assert result.num_rows == 3
        assert "id" in result.column_names

    def test_read_uses_joint_path(self, catalog: Catalog, sample_table: pa.Table) -> None:
        _get_shared_store()[("mem", "my_table")] = sample_table
        joint = Joint(name="src", joint_type="source", catalog="mem", path="my_table")
        mat = ArrowSource().read(catalog, joint)
        assert mat.to_arrow().num_rows == 3

    def test_read_missing_table_raises(self, catalog: Catalog) -> None:
        joint = Joint(name="missing", joint_type="source", catalog="mem")
        with pytest.raises(KeyError):
            ArrowSource().read(catalog, joint)

    def test_read_schema_populated(self, catalog: Catalog, sample_table: pa.Table) -> None:
        _get_shared_store()[("mem", "t1")] = sample_table
        joint = Joint(name="t1", joint_type="source", catalog="mem")
        mat = ArrowSource().read(catalog, joint)
        assert mat.schema is not None
        assert "id" in mat.schema
        assert "val" in mat.schema


# ── ArrowSink ─────────────────────────────────────────────────────────


class TestArrowSink:
    def _make_material(self, catalog: Catalog, table: pa.Table, name: str = "src") -> object:
        """Write a table into the store and return a Material backed by it."""
        _get_shared_store()[(catalog.name, name)] = table
        joint = Joint(name=name, joint_type="source", catalog=catalog.name)
        return ArrowSource().read(catalog, joint)

    def test_write_replace(self, catalog: Catalog, sample_table: pa.Table) -> None:
        mat = self._make_material(catalog, sample_table)
        sink_joint = Joint(name="out", joint_type="sink", catalog="mem")
        ArrowSink().write(catalog, sink_joint, mat, "replace")
        stored = _get_shared_store()[("mem", "out")]
        assert stored.num_rows == 3

    def test_write_replace_overwrites(self, catalog: Catalog, sample_table: pa.Table) -> None:
        old = pa.table({"id": [99]})
        _get_shared_store()[("mem", "out")] = old
        mat = self._make_material(catalog, sample_table)
        sink_joint = Joint(name="out", joint_type="sink", catalog="mem")
        ArrowSink().write(catalog, sink_joint, mat, "replace")
        assert _get_shared_store()[("mem", "out")].num_rows == 3

    def test_write_append_to_empty(self, catalog: Catalog, sample_table: pa.Table) -> None:
        mat = self._make_material(catalog, sample_table)
        sink_joint = Joint(name="out", joint_type="sink", catalog="mem")
        ArrowSink().write(catalog, sink_joint, mat, "append")
        assert _get_shared_store()[("mem", "out")].num_rows == 3

    def test_write_append_accumulates(self, catalog: Catalog, sample_table: pa.Table) -> None:
        mat = self._make_material(catalog, sample_table)
        sink_joint = Joint(name="out", joint_type="sink", catalog="mem")
        ArrowSink().write(catalog, sink_joint, mat, "append")
        # Append again
        mat2 = self._make_material(catalog, sample_table)
        ArrowSink().write(catalog, sink_joint, mat2, "append")
        assert _get_shared_store()[("mem", "out")].num_rows == 6

    def test_write_uses_joint_path(self, catalog: Catalog, sample_table: pa.Table) -> None:
        mat = self._make_material(catalog, sample_table)
        sink_joint = Joint(name="sink_node", joint_type="sink", catalog="mem", path="custom_name")
        ArrowSink().write(catalog, sink_joint, mat, "replace")
        assert ("mem", "custom_name") in _get_shared_store()
