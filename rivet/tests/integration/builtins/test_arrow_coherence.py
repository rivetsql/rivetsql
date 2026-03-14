"""Integration tests for Arrow plugin error handling and validation fixes."""

from __future__ import annotations

import pyarrow
import pytest

from rivet_core.builtins.arrow_catalog import (
    ArrowCatalogPlugin,
    ArrowComputeEnginePlugin,
    ArrowSink,
    ArrowSource,
)
from rivet_core.errors import ExecutionError, PluginValidationError
from rivet_core.models import Catalog, Joint, Material
from rivet_core.strategies import ArrowMaterialization, MaterializationContext


def _make_catalog(name: str = "test") -> Catalog:
    return Catalog(name=name, type="arrow", options={})


def _make_joint(name: str = "test_joint", path: str | None = None) -> Joint:
    return Joint(name=name, joint_type="sink", path=path)


def _make_material(name: str = "test_joint") -> Material:
    table = pyarrow.table({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    ref = ArrowMaterialization().materialize(
        table, MaterializationContext(joint_name=name, strategy_name="arrow", options={})
    )
    return Material(
        name=name,
        catalog="test",
        table="t",
        schema={"id": "int64", "value": "utf8"},
        state="materialized",
        materialized_ref=ref,
    )


# ── ArrowSink strategy validation ────────────────────────────────────


def test_arrow_sink_rejects_unsupported_strategy():
    store: dict[tuple[str, str], pyarrow.Table] = {}
    sink = ArrowSink(table_store=store)
    catalog = _make_catalog()
    joint = _make_joint()
    material = _make_material()

    with pytest.raises(ExecutionError) as exc_info:
        sink.write(catalog, joint, material, strategy="merge")

    assert exc_info.value.error.code == "RVT-501"
    assert "merge" in exc_info.value.error.message
    assert "append" in (exc_info.value.error.remediation or "")
    assert "replace" in (exc_info.value.error.remediation or "")


def test_arrow_sink_rejects_scd2_strategy():
    store: dict[tuple[str, str], pyarrow.Table] = {}
    sink = ArrowSink(table_store=store)

    with pytest.raises(ExecutionError) as exc_info:
        sink.write(_make_catalog(), _make_joint(), _make_material(), strategy="scd2")

    assert exc_info.value.error.code == "RVT-501"


def test_arrow_sink_append_works():
    store: dict[tuple[str, str], pyarrow.Table] = {}
    sink = ArrowSink(table_store=store)
    catalog = _make_catalog()
    joint = _make_joint(name="t1", path="t1")
    material = _make_material()

    sink.write(catalog, joint, material, strategy="append")
    assert ("test", "t1") in store
    assert store[("test", "t1")].num_rows == 3


def test_arrow_sink_replace_works():
    store: dict[tuple[str, str], pyarrow.Table] = {}
    sink = ArrowSink(table_store=store)
    catalog = _make_catalog()
    joint = _make_joint(name="t1", path="t1")
    material = _make_material()

    sink.write(catalog, joint, material, strategy="replace")
    assert store[("test", "t1")].num_rows == 3


# ── ArrowSource error handling ───────────────────────────────────────


def test_arrow_source_raises_execution_error_for_missing_table():
    store: dict[tuple[str, str], pyarrow.Table] = {}
    source = ArrowSource(table_store=store)
    catalog = _make_catalog()
    joint = Joint(name="missing_table", joint_type="source", path="missing_table")

    with pytest.raises(ExecutionError) as exc_info:
        source.read(catalog, joint)

    assert exc_info.value.error.code == "RVT-501"
    assert "missing_table" in exc_info.value.error.message
    assert exc_info.value.error.context.get("plugin_type") == "source"


def test_arrow_source_reads_existing_table():
    table = pyarrow.table({"x": [10, 20]})
    store: dict[tuple[str, str], pyarrow.Table] = {("test", "my_table"): table}
    source = ArrowSource(table_store=store)
    catalog = _make_catalog()
    joint = Joint(name="my_table", joint_type="source", path="my_table")

    material = source.read(catalog, joint)
    assert material.to_arrow().num_rows == 2


# ── ArrowCatalogPlugin.get_schema error handling ─────────────────────


def test_arrow_catalog_get_schema_raises_execution_error():
    plugin = ArrowCatalogPlugin()
    catalog = _make_catalog("empty")

    with pytest.raises(ExecutionError) as exc_info:
        plugin.get_schema(catalog, "nonexistent")

    assert exc_info.value.error.code == "RVT-501"
    assert "nonexistent" in exc_info.value.error.message
    assert exc_info.value.error.context.get("plugin_type") == "catalog"


# ── ArrowComputeEnginePlugin.validate ────────────────────────────────


def test_arrow_engine_validate_rejects_unrecognized_options():
    engine = ArrowComputeEnginePlugin()

    with pytest.raises(PluginValidationError) as exc_info:
        engine.validate({"bogus_option": True})

    assert exc_info.value.error.code == "RVT-201"
    assert "bogus_option" in exc_info.value.error.message


def test_arrow_engine_validate_accepts_empty_options():
    engine = ArrowComputeEnginePlugin()
    engine.validate({})  # should not raise
