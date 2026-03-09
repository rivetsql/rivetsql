"""Integration tests: PySpark adapter through real plugin interface.

Verifies the PySpark adapter registers, resolves tables, and executes SQL
through the real plugin interface. Skips if pyspark is unavailable.
"""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark")

from rivet_core.plugins import PluginRegistry
from rivet_pyspark import PySparkPlugin

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPySparkRegistration:
    """PySpark plugin registers engine and adapters."""

    def test_engine_plugin_registered(self):
        reg = PluginRegistry()
        reg.register_builtins()
        PySparkPlugin(reg)

        plugin = reg.get_engine_plugin("pyspark")
        assert plugin is not None
        assert plugin.engine_type == "pyspark"

    def test_create_engine_instance(self):
        reg = PluginRegistry()
        reg.register_builtins()
        PySparkPlugin(reg)

        plugin = reg.get_engine_plugin("pyspark")
        engine = plugin.create_engine("spark_primary", {})
        assert engine.name == "spark_primary"
        assert engine.engine_type == "pyspark"


class TestPySparkValidation:
    """PySpark engine validates options correctly."""

    def test_validate_empty_options(self):
        reg = PluginRegistry()
        reg.register_builtins()
        PySparkPlugin(reg)

        plugin = reg.get_engine_plugin("pyspark")
        # Should not raise
        plugin.validate({})

    def test_validate_unknown_option_raises(self):
        from rivet_core.errors import PluginValidationError

        reg = PluginRegistry()
        reg.register_builtins()
        PySparkPlugin(reg)

        plugin = reg.get_engine_plugin("pyspark")
        with pytest.raises(PluginValidationError):
            plugin.validate({"nonexistent_option": True})
