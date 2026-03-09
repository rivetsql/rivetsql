"""Integration tests: plugin registration and dispatch through real components.

Verifies plugins register correctly and dispatch to the right engine/catalog.
Uses real PluginRegistry, DuckDB plugin, and built-in plugins — no mocks.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from rivet_core.plugins import PluginRegistry
from rivet_duckdb import DuckDBPlugin

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDuckDBPluginRegistration:
    """DuckDBPlugin registers all expected components."""

    def test_engine_plugin_registered(self):
        reg = PluginRegistry()
        reg.register_builtins()
        DuckDBPlugin(reg)

        plugin = reg.get_engine_plugin("duckdb")
        assert plugin is not None
        assert plugin.engine_type == "duckdb"

    def test_catalog_plugin_registered(self):
        reg = PluginRegistry()
        reg.register_builtins()
        DuckDBPlugin(reg)

        plugin = reg.get_catalog_plugin("duckdb")
        assert plugin is not None

    def test_filesystem_catalog_registered_by_builtins(self):
        reg = PluginRegistry()
        reg.register_builtins()

        plugin = reg.get_catalog_plugin("filesystem")
        assert plugin is not None

    def test_arrow_catalog_registered_by_builtins(self):
        reg = PluginRegistry()
        reg.register_builtins()

        plugin = reg.get_catalog_plugin("arrow")
        assert plugin is not None

    def test_arrow_engine_registered_by_builtins(self):
        reg = PluginRegistry()
        reg.register_builtins()

        plugin = reg.get_engine_plugin("arrow")
        assert plugin is not None


class TestEngineCreationAndDispatch:
    """Engine plugins create engine instances and execute SQL."""

    def test_create_duckdb_engine_instance(self):
        reg = PluginRegistry()
        reg.register_builtins()
        DuckDBPlugin(reg)

        plugin = reg.get_engine_plugin("duckdb")
        engine = plugin.create_engine("test_engine", {})
        assert engine.name == "test_engine"
        assert engine.engine_type == "duckdb"

    def test_register_and_retrieve_compute_engine(self):
        reg = PluginRegistry()
        reg.register_builtins()
        DuckDBPlugin(reg)

        plugin = reg.get_engine_plugin("duckdb")
        engine = plugin.create_engine("my_engine", {})
        reg.register_compute_engine(engine)

        retrieved = reg.get_compute_engine("my_engine")
        assert retrieved is not None
        assert retrieved.name == "my_engine"

    def test_duckdb_execute_sql_with_arrow_input(self):
        reg = PluginRegistry()
        reg.register_builtins()
        DuckDBPlugin(reg)

        plugin = reg.get_engine_plugin("duckdb")
        engine = plugin.create_engine("test_eng", {})

        input_table = pa.table({"id": [1, 2, 3], "val": [10, 20, 30]})
        result = plugin.execute_sql(engine, "SELECT id, val FROM test_data", {"test_data": input_table})

        assert result.num_rows == 3
        assert result.column("id").to_pylist() == [1, 2, 3]

    def test_duckdb_execute_sql_with_filter(self):
        reg = PluginRegistry()
        reg.register_builtins()
        DuckDBPlugin(reg)

        plugin = reg.get_engine_plugin("duckdb")
        engine = plugin.create_engine("test_eng2", {})

        input_table = pa.table({"id": [1, 2, 3], "val": [10, 20, 30]})
        result = plugin.execute_sql(
            engine,
            "SELECT id, val FROM data WHERE val > 15",
            {"data": input_table},
        )

        assert result.num_rows == 2
        assert sorted(result.column("id").to_pylist()) == [2, 3]

    def test_duckdb_execute_sql_with_join(self):
        reg = PluginRegistry()
        reg.register_builtins()
        DuckDBPlugin(reg)

        plugin = reg.get_engine_plugin("duckdb")
        engine = plugin.create_engine("test_eng3", {})

        left = pa.table({"id": [1, 2], "name": ["Alice", "Bob"]})
        right = pa.table({"id": [1, 2], "score": [90, 85]})
        result = plugin.execute_sql(
            engine,
            "SELECT l.name, r.score FROM left_t l JOIN right_t r ON l.id = r.id",
            {"left_t": left, "right_t": right},
        )

        assert result.num_rows == 2
        assert sorted(result.column("name").to_pylist()) == ["Alice", "Bob"]


class TestAdapterResolution:
    """Adapter lookup resolves correctly for engine+catalog pairs."""

    def test_no_adapter_for_natively_supported_catalogs(self):
        """DuckDB natively supports filesystem and arrow — no adapter needed."""
        reg = PluginRegistry()
        reg.register_builtins()
        DuckDBPlugin(reg)

        # Native catalog types don't need adapters
        assert reg.get_adapter("duckdb", "filesystem") is None
        assert reg.get_adapter("duckdb", "arrow") is None

    def test_cross_catalog_adapters_registered(self):
        """DuckDB registers adapters for cross-catalog types (s3, glue, unity)."""
        reg = PluginRegistry()
        reg.register_builtins()
        DuckDBPlugin(reg)

        # These adapters are registered best-effort (depend on optional packages)
        registered_keys = set(reg._adapters.keys())
        # At least one cross-catalog adapter should be registered
        cross_catalog_keys = {k for k in registered_keys if k[0] == "duckdb"}
        assert len(cross_catalog_keys) >= 1


class TestDuplicateRegistration:
    """Duplicate plugin registration raises errors."""

    def test_duplicate_engine_plugin_raises(self):
        from rivet_core.plugins import PluginRegistrationError

        reg = PluginRegistry()
        reg.register_builtins()
        DuckDBPlugin(reg)

        # Attempting to register DuckDB again should fail
        with pytest.raises(PluginRegistrationError):
            DuckDBPlugin(reg)

    def test_duplicate_compute_engine_instance_raises(self):
        from rivet_core.plugins import PluginRegistrationError

        reg = PluginRegistry()
        reg.register_builtins()
        DuckDBPlugin(reg)

        plugin = reg.get_engine_plugin("duckdb")
        engine = plugin.create_engine("dup_engine", {})
        reg.register_compute_engine(engine)

        with pytest.raises(PluginRegistrationError):
            reg.register_compute_engine(engine)


class TestSupportedCatalogTypes:
    """Engine plugins report their supported catalog types."""

    def test_duckdb_supports_filesystem(self):
        reg = PluginRegistry()
        reg.register_builtins()
        DuckDBPlugin(reg)

        plugin = reg.get_engine_plugin("duckdb")
        assert "filesystem" in plugin.supported_catalog_types

    def test_duckdb_supports_arrow(self):
        reg = PluginRegistry()
        reg.register_builtins()
        DuckDBPlugin(reg)

        plugin = reg.get_engine_plugin("duckdb")
        assert "arrow" in plugin.supported_catalog_types
