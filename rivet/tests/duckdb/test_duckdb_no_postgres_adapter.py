"""Tests for task 7.4: rivet_duckdb does NOT register a postgres adapter.

rivet_postgres ships its own PostgresDuckDBAdapter override (catalog-plugin-contributed),
so rivet_duckdb must not register any adapter for catalog_type="postgres".
"""

from __future__ import annotations

import glob
import os

from rivet_core.plugins import PluginRegistry
from rivet_duckdb.engine import DuckDBComputeEnginePlugin


def test_duckdb_engine_does_not_declare_postgres_catalog_type():
    """DuckDB engine's supported_catalog_types must not include 'postgres'."""
    plugin = DuckDBComputeEnginePlugin()
    assert "postgres" not in plugin.supported_catalog_types


def test_no_postgres_adapter_file_in_rivet_duckdb():
    """There must be no postgres adapter module in rivet_duckdb/adapters/."""
    adapters_dir = os.path.join(
        os.path.dirname(__file__), "..", "src", "rivet_duckdb", "adapters"
    )
    adapter_files = glob.glob(os.path.join(adapters_dir, "*.py"))
    names = [os.path.basename(f) for f in adapter_files]
    assert "postgres.py" not in names


def test_registry_has_no_duckdb_postgres_adapter_after_registering_duckdb_plugin():
    """After registering only the DuckDB engine plugin, no (duckdb, postgres) adapter exists."""
    registry = PluginRegistry()
    registry.register_engine_plugin(DuckDBComputeEnginePlugin())
    adapter = registry.get_adapter("duckdb", "postgres")
    assert adapter is None


def test_duckdb_resolve_capabilities_postgres_returns_none():
    """resolve_capabilities for (duckdb, postgres) returns None when only DuckDB plugin registered."""
    registry = PluginRegistry()
    registry.register_engine_plugin(DuckDBComputeEnginePlugin())
    caps = registry.resolve_capabilities("duckdb", "postgres")
    assert caps is None
