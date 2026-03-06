"""Tests for task 12.1: PostgresDuckDBAdapter registration attributes.

Verifies that PostgresDuckDBAdapter registers with:
- target_engine_type="duckdb"
- catalog_type="postgres"
- source_plugin="rivet_postgres"
- source="catalog_plugin" (takes priority over engine_plugin adapters)
"""

from __future__ import annotations

from rivet_core.optimizer import EMPTY_RESIDUAL, AdapterPushdownResult
from rivet_core.plugins import ComputeEngineAdapter, PluginRegistry
from rivet_duckdb.engine import DuckDBComputeEnginePlugin
from rivet_postgres.adapters.duckdb import PostgresDuckDBAdapter


def test_adapter_is_compute_engine_adapter():
    adapter = PostgresDuckDBAdapter()
    assert isinstance(adapter, ComputeEngineAdapter)


def test_target_engine_type():
    adapter = PostgresDuckDBAdapter()
    assert adapter.target_engine_type == "duckdb"


def test_catalog_type():
    adapter = PostgresDuckDBAdapter()
    assert adapter.catalog_type == "postgres"


def test_source_plugin():
    adapter = PostgresDuckDBAdapter()
    assert adapter.source_plugin == "rivet_postgres"


def test_source_is_catalog_plugin():
    adapter = PostgresDuckDBAdapter()
    assert adapter.source == "catalog_plugin"


def test_registry_accepts_adapter():
    """PostgresDuckDBAdapter can be registered in the PluginRegistry."""
    registry = PluginRegistry()
    registry.register_engine_plugin(DuckDBComputeEnginePlugin())
    adapter = PostgresDuckDBAdapter()
    registry.register_adapter(adapter)
    resolved = registry.get_adapter("duckdb", "postgres")
    assert resolved is adapter


def test_catalog_plugin_adapter_overrides_engine_plugin():
    """catalog_plugin adapter takes priority over engine_plugin adapter."""
    registry = PluginRegistry()
    registry.register_engine_plugin(DuckDBComputeEnginePlugin())

    # Register a fake engine_plugin adapter first
    class FakeEngineAdapter(ComputeEngineAdapter):
        target_engine_type = "duckdb"
        catalog_type = "postgres"
        capabilities = ["projection_pushdown"]
        source = "engine_plugin"
        source_plugin = "rivet_duckdb"

        def read_dispatch(self, engine, catalog, joint, pushdown=None):
            return AdapterPushdownResult(material=None, residual=EMPTY_RESIDUAL)

        def write_dispatch(self, engine, catalog, joint, material):
            pass

    registry.register_adapter(FakeEngineAdapter())

    # Now register the catalog_plugin adapter — it should override
    pg_adapter = PostgresDuckDBAdapter()
    registry.register_adapter(pg_adapter)

    resolved = registry.get_adapter("duckdb", "postgres")
    assert resolved is pg_adapter
    assert resolved.source == "catalog_plugin"
    assert resolved.source_plugin == "rivet_postgres"
