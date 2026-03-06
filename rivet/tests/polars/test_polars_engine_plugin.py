"""Tests for task 29.1: PolarsComputeEnginePlugin registration."""

from __future__ import annotations

import pytest

from rivet_core.errors import PluginValidationError
from rivet_core.models import ComputeEngine
from rivet_core.plugins import ComputeEnginePlugin, PluginRegistry
from rivet_polars.engine import PolarsComputeEnginePlugin


def test_engine_type():
    plugin = PolarsComputeEnginePlugin()
    assert plugin.engine_type == "polars"


def test_dialect():
    plugin = PolarsComputeEnginePlugin()
    assert plugin.dialect == "duckdb"


def test_is_compute_engine_plugin():
    assert isinstance(PolarsComputeEnginePlugin(), ComputeEnginePlugin)


def test_create_engine_returns_correct_type():
    plugin = PolarsComputeEnginePlugin()
    engine = plugin.create_engine("my_polars", {})
    assert isinstance(engine, ComputeEngine)
    assert engine.name == "my_polars"
    assert engine.engine_type == "polars"


def test_validate_accepts_valid_options():
    plugin = PolarsComputeEnginePlugin()
    plugin.validate({"streaming": True, "n_threads": 4})  # should not raise


def test_validate_rejects_unknown_option():
    plugin = PolarsComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"unknown_key": "value"})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_accepts_empty_options():
    plugin = PolarsComputeEnginePlugin()
    plugin.validate({})  # should not raise


def test_registry_can_register_plugin():
    registry = PluginRegistry()
    plugin = PolarsComputeEnginePlugin()
    registry.register_engine_plugin(plugin)
    assert registry.get_engine_plugin("polars") is plugin


def test_compiler_reads_dialect():
    plugin = PolarsComputeEnginePlugin()
    assert getattr(plugin, "dialect", None) == "duckdb"


# Task 29.2: Declare native support for arrow and filesystem with all 6 capabilities

ALL_6 = {
    "projection_pushdown",
    "predicate_pushdown",
    "limit_pushdown",
    "cast_pushdown",
    "join",
    "aggregation",
}


def test_supported_catalog_types_contains_arrow():
    plugin = PolarsComputeEnginePlugin()
    assert "arrow" in plugin.supported_catalog_types


def test_supported_catalog_types_contains_filesystem():
    plugin = PolarsComputeEnginePlugin()
    assert "filesystem" in plugin.supported_catalog_types


def test_arrow_catalog_has_all_6_capabilities():
    plugin = PolarsComputeEnginePlugin()
    assert set(plugin.supported_catalog_types["arrow"]) == ALL_6


def test_filesystem_catalog_has_all_6_capabilities():
    plugin = PolarsComputeEnginePlugin()
    assert set(plugin.supported_catalog_types["filesystem"]) == ALL_6


def test_resolve_capabilities_arrow():
    registry = PluginRegistry()
    registry.register_engine_plugin(PolarsComputeEnginePlugin())
    assert set(registry.resolve_capabilities("polars", "arrow")) == ALL_6


def test_resolve_capabilities_filesystem():
    registry = PluginRegistry()
    registry.register_engine_plugin(PolarsComputeEnginePlugin())
    assert set(registry.resolve_capabilities("polars", "filesystem")) == ALL_6


# Task 29.3: Accept options: streaming, n_threads, check_dtypes


def test_optional_options_declares_streaming():
    plugin = PolarsComputeEnginePlugin()
    assert "streaming" in plugin.optional_options
    assert plugin.optional_options["streaming"] is False


def test_optional_options_declares_n_threads():
    plugin = PolarsComputeEnginePlugin()
    assert "n_threads" in plugin.optional_options
    assert plugin.optional_options["n_threads"] is None


def test_optional_options_declares_check_dtypes():
    plugin = PolarsComputeEnginePlugin()
    assert "check_dtypes" in plugin.optional_options
    assert plugin.optional_options["check_dtypes"] is True


def test_validate_accepts_streaming_true():
    plugin = PolarsComputeEnginePlugin()
    plugin.validate({"streaming": True})  # should not raise


def test_validate_accepts_streaming_false():
    plugin = PolarsComputeEnginePlugin()
    plugin.validate({"streaming": False})  # should not raise


def test_validate_rejects_streaming_non_bool():
    plugin = PolarsComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"streaming": "yes"})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_accepts_n_threads_integer():
    plugin = PolarsComputeEnginePlugin()
    plugin.validate({"n_threads": 4})  # should not raise


def test_validate_accepts_n_threads_none():
    plugin = PolarsComputeEnginePlugin()
    plugin.validate({"n_threads": None})  # should not raise


def test_validate_rejects_n_threads_non_int():
    plugin = PolarsComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"n_threads": "4"})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_accepts_check_dtypes_true():
    plugin = PolarsComputeEnginePlugin()
    plugin.validate({"check_dtypes": True})  # should not raise


def test_validate_accepts_check_dtypes_false():
    plugin = PolarsComputeEnginePlugin()
    plugin.validate({"check_dtypes": False})  # should not raise


def test_validate_rejects_check_dtypes_non_bool():
    plugin = PolarsComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"check_dtypes": 1})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_accepts_all_three_options_together():
    plugin = PolarsComputeEnginePlugin()
    plugin.validate({"streaming": True, "n_threads": 8, "check_dtypes": False})  # should not raise
