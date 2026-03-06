"""Tests for task 32.1: PySparkComputeEnginePlugin registration."""

from __future__ import annotations

import pytest

from rivet_core.errors import PluginValidationError
from rivet_core.models import ComputeEngine
from rivet_core.plugins import ComputeEnginePlugin, PluginRegistry
from rivet_pyspark.engine import PySparkComputeEnginePlugin


def test_engine_type():
    plugin = PySparkComputeEnginePlugin()
    assert plugin.engine_type == "pyspark"


def test_dialect():
    plugin = PySparkComputeEnginePlugin()
    assert plugin.dialect == "spark"


def test_is_compute_engine_plugin():
    assert isinstance(PySparkComputeEnginePlugin(), ComputeEnginePlugin)


def test_create_engine_returns_correct_type():
    plugin = PySparkComputeEnginePlugin()
    engine = plugin.create_engine("my_spark", {})
    assert isinstance(engine, ComputeEngine)
    assert engine.name == "my_spark"
    assert engine.engine_type == "pyspark"


def test_validate_accepts_valid_options():
    plugin = PySparkComputeEnginePlugin()
    plugin.validate({"master": "local[4]", "app_name": "test"})  # should not raise


def test_validate_rejects_unknown_option():
    plugin = PySparkComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"unknown_key": "value"})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_accepts_empty_options():
    plugin = PySparkComputeEnginePlugin()
    plugin.validate({})  # should not raise


def test_registry_can_register_plugin():
    registry = PluginRegistry()
    plugin = PySparkComputeEnginePlugin()
    registry.register_engine_plugin(plugin)
    assert registry.get_engine_plugin("pyspark") is plugin


def test_compiler_reads_dialect():
    plugin = PySparkComputeEnginePlugin()
    assert getattr(plugin, "dialect", None) == "spark"


ALL_6 = [
    "projection_pushdown",
    "predicate_pushdown",
    "limit_pushdown",
    "cast_pushdown",
    "join",
    "aggregation",
]


def test_native_support_for_arrow():
    plugin = PySparkComputeEnginePlugin()
    assert plugin.supported_catalog_types.get("arrow") == ALL_6


def test_native_support_for_filesystem():
    plugin = PySparkComputeEnginePlugin()
    assert plugin.supported_catalog_types.get("filesystem") == ALL_6


def test_supported_catalog_types_only_arrow_and_filesystem():
    plugin = PySparkComputeEnginePlugin()
    assert set(plugin.supported_catalog_types.keys()) == {"arrow", "filesystem"}


# Task 32.3: Accept options: master, app_name, config, spark_home, packages, connect_url


def test_optional_options_declares_master():
    plugin = PySparkComputeEnginePlugin()
    assert "master" in plugin.optional_options
    assert plugin.optional_options["master"] == "local[*]"


def test_optional_options_declares_app_name():
    plugin = PySparkComputeEnginePlugin()
    assert "app_name" in plugin.optional_options
    assert plugin.optional_options["app_name"] == "rivet"


def test_optional_options_declares_config():
    plugin = PySparkComputeEnginePlugin()
    assert "config" in plugin.optional_options
    assert plugin.optional_options["config"] == {}


def test_optional_options_declares_spark_home():
    plugin = PySparkComputeEnginePlugin()
    assert "spark_home" in plugin.optional_options
    assert plugin.optional_options["spark_home"] is None


def test_optional_options_declares_packages():
    plugin = PySparkComputeEnginePlugin()
    assert "packages" in plugin.optional_options
    assert plugin.optional_options["packages"] == []


def test_optional_options_declares_connect_url():
    plugin = PySparkComputeEnginePlugin()
    assert "connect_url" in plugin.optional_options
    assert plugin.optional_options["connect_url"] is None


def test_validate_accepts_master_string():
    plugin = PySparkComputeEnginePlugin()
    plugin.validate({"master": "spark://host:7077"})


def test_validate_rejects_master_non_string():
    plugin = PySparkComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"master": 123})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_accepts_app_name_string():
    plugin = PySparkComputeEnginePlugin()
    plugin.validate({"app_name": "my_app"})


def test_validate_rejects_app_name_non_string():
    plugin = PySparkComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"app_name": 42})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_accepts_config_dict():
    plugin = PySparkComputeEnginePlugin()
    plugin.validate({"config": {"spark.executor.memory": "2g"}})


def test_validate_rejects_config_non_dict():
    plugin = PySparkComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"config": "spark.executor.memory=2g"})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_accepts_spark_home_string():
    plugin = PySparkComputeEnginePlugin()
    plugin.validate({"spark_home": "/opt/spark"})


def test_validate_accepts_spark_home_none():
    plugin = PySparkComputeEnginePlugin()
    plugin.validate({"spark_home": None})


def test_validate_rejects_spark_home_non_string():
    plugin = PySparkComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"spark_home": 123})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_accepts_packages_list():
    plugin = PySparkComputeEnginePlugin()
    plugin.validate({"packages": ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"]})


def test_validate_accepts_packages_empty_list():
    plugin = PySparkComputeEnginePlugin()
    plugin.validate({"packages": []})


def test_validate_rejects_packages_non_list():
    plugin = PySparkComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"packages": "org.apache.spark:spark-sql-kafka"})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_accepts_connect_url_string():
    plugin = PySparkComputeEnginePlugin()
    plugin.validate({"connect_url": "sc://localhost:15002"})


def test_validate_accepts_connect_url_none():
    plugin = PySparkComputeEnginePlugin()
    plugin.validate({"connect_url": None})


def test_validate_rejects_connect_url_non_string():
    plugin = PySparkComputeEnginePlugin()
    with pytest.raises(PluginValidationError) as exc_info:
        plugin.validate({"connect_url": 9090})
    assert exc_info.value.error.code == "RVT-201"


def test_validate_accepts_all_options_together():
    plugin = PySparkComputeEnginePlugin()
    plugin.validate({
        "master": "local[4]",
        "app_name": "test_app",
        "config": {"spark.sql.shuffle.partitions": "10"},
        "spark_home": "/opt/spark",
        "packages": ["com.example:lib:1.0"],
        "connect_url": None,
    })
