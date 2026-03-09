"""Integration tests: Polars adapter through real plugin interface.

Verifies the Polars adapter registers, resolves tables, and executes SQL
through the real plugin interface. Skips if polars is unavailable.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

polars = pytest.importorskip("polars")

from rivet_core.plugins import PluginRegistry
from rivet_polars import PolarsPlugin

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPolarsRegistration:
    """Polars plugin registers engine and adapters."""

    def test_engine_plugin_registered(self):
        reg = PluginRegistry()
        reg.register_builtins()
        PolarsPlugin(reg)

        plugin = reg.get_engine_plugin("polars")
        assert plugin is not None
        assert plugin.engine_type == "polars"

    def test_create_engine_instance(self):
        reg = PluginRegistry()
        reg.register_builtins()
        PolarsPlugin(reg)

        plugin = reg.get_engine_plugin("polars")
        engine = plugin.create_engine("polars_primary", {})
        assert engine.name == "polars_primary"
        assert engine.engine_type == "polars"


class TestPolarsExecution:
    """Polars engine executes SQL through the real plugin interface."""

    def test_execute_simple_select(self):
        reg = PluginRegistry()
        reg.register_builtins()
        PolarsPlugin(reg)

        plugin = reg.get_engine_plugin("polars")
        engine = plugin.create_engine("polars_test", {})

        input_table = pa.table({"id": [1, 2, 3], "val": [10, 20, 30]})
        result = plugin.execute_sql(engine, "SELECT * FROM data", {"data": input_table})

        assert result.num_rows == 3
        assert sorted(result.column("id").to_pylist()) == [1, 2, 3]

    def test_execute_filter(self):
        reg = PluginRegistry()
        reg.register_builtins()
        PolarsPlugin(reg)

        plugin = reg.get_engine_plugin("polars")
        engine = plugin.create_engine("polars_test2", {})

        input_table = pa.table({"id": [1, 2, 3], "val": [10, 20, 30]})
        result = plugin.execute_sql(
            engine,
            "SELECT id, val FROM data WHERE val > 15",
            {"data": input_table},
        )

        assert result.num_rows == 2
        assert sorted(result.column("id").to_pylist()) == [2, 3]

    def test_execute_aggregation(self):
        reg = PluginRegistry()
        reg.register_builtins()
        PolarsPlugin(reg)

        plugin = reg.get_engine_plugin("polars")
        engine = plugin.create_engine("polars_test3", {})

        input_table = pa.table({"group": ["a", "b", "a", "b"], "val": [1, 2, 3, 4]})
        result = plugin.execute_sql(
            engine,
            "SELECT \"group\", SUM(val) AS total FROM data GROUP BY \"group\"",
            {"data": input_table},
        )

        assert result.num_rows == 2
        groups = result.column("group").to_pylist()
        totals = result.column("total").to_pylist()
        row_map = dict(zip(groups, totals))
        assert row_map["a"] == 4
        assert row_map["b"] == 6

    def test_execute_join(self):
        reg = PluginRegistry()
        reg.register_builtins()
        PolarsPlugin(reg)

        plugin = reg.get_engine_plugin("polars")
        engine = plugin.create_engine("polars_test4", {})

        left = pa.table({"id": [1, 2], "name": ["Alice", "Bob"]})
        right = pa.table({"id": [1, 2], "score": [90, 85]})
        result = plugin.execute_sql(
            engine,
            "SELECT l.name, r.score FROM left_t l JOIN right_t r ON l.id = r.id",
            {"left_t": left, "right_t": right},
        )

        assert result.num_rows == 2


class TestPolarsSupportedCatalogs:
    """Polars engine reports supported catalog types."""

    def test_supports_arrow(self):
        reg = PluginRegistry()
        reg.register_builtins()
        PolarsPlugin(reg)

        plugin = reg.get_engine_plugin("polars")
        assert "arrow" in plugin.supported_catalog_types

    def test_supports_filesystem(self):
        reg = PluginRegistry()
        reg.register_builtins()
        PolarsPlugin(reg)

        plugin = reg.get_engine_plugin("polars")
        assert "filesystem" in plugin.supported_catalog_types
