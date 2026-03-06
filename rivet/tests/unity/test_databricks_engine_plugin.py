"""Tests for DatabricksComputeEnginePlugin (task 25.1)."""

from __future__ import annotations

import pytest

from rivet_core.errors import PluginValidationError
from rivet_core.models import ComputeEngine
from rivet_core.plugins import ComputeEnginePlugin
from rivet_databricks.engine import DatabricksComputeEnginePlugin

_VALID_OPTIONS = {"warehouse_id": "abc123", "workspace_url": "https://test.databricks.com", "token": "tok"}


def test_engine_type():
    assert DatabricksComputeEnginePlugin.engine_type == "databricks"


def test_dialect():
    assert DatabricksComputeEnginePlugin.dialect == "databricks"


def test_is_compute_engine_plugin():
    assert isinstance(DatabricksComputeEnginePlugin(), ComputeEnginePlugin)


def test_native_support_for_databricks_catalog():
    plugin = DatabricksComputeEnginePlugin()
    assert "databricks" in plugin.supported_catalog_types


def test_databricks_catalog_has_all_6_capabilities():
    plugin = DatabricksComputeEnginePlugin()
    caps = plugin.supported_catalog_types["databricks"]
    for cap in ["projection_pushdown", "predicate_pushdown", "limit_pushdown",
                "cast_pushdown", "join", "aggregation"]:
        assert cap in caps


def test_required_options_includes_warehouse_id():
    assert "warehouse_id" in DatabricksComputeEnginePlugin.required_options


def test_optional_options():
    plugin = DatabricksComputeEnginePlugin()
    assert "wait_timeout" in plugin.optional_options
    assert "max_rows_per_chunk" in plugin.optional_options
    assert "disposition" in plugin.optional_options


def test_create_engine_returns_compute_engine():
    plugin = DatabricksComputeEnginePlugin()
    engine = plugin.create_engine("my_engine", _VALID_OPTIONS)
    assert isinstance(engine, ComputeEngine)
    assert engine.name == "my_engine"
    assert engine.engine_type == "databricks"


def test_validate_accepts_valid_options():
    DatabricksComputeEnginePlugin().validate(_VALID_OPTIONS)  # should not raise


def test_validate_rejects_missing_warehouse_id():
    with pytest.raises(PluginValidationError) as exc_info:
        DatabricksComputeEnginePlugin().validate({})
    assert exc_info.value.error.code == "RVT-201"
    assert "warehouse_id" in exc_info.value.error.message


def test_validate_rejects_unknown_option():
    opts = {**_VALID_OPTIONS, "unknown_key": "value"}
    with pytest.raises(PluginValidationError) as exc_info:
        DatabricksComputeEnginePlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-201"
    assert "unknown_key" in exc_info.value.error.message


def test_validate_accepts_all_optional_options():
    opts = {
        **_VALID_OPTIONS,
        "wait_timeout": "60s",
        "max_rows_per_chunk": 50000,
        "disposition": "INLINE",
    }
    DatabricksComputeEnginePlugin().validate(opts)  # should not raise


def test_supported_write_strategies_for_databricks():
    plugin = DatabricksComputeEnginePlugin()
    strategies = plugin.supported_write_strategies["databricks"]
    for s in ["append", "replace", "truncate_insert", "merge",
              "delete_insert", "incremental_append", "scd2", "partition"]:
        assert s in strategies
