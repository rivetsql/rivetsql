"""Task 38.1: Plugin registration tests for all 6 packages.

Verifies that each plugin's registration function registers the correct
catalog types, engine types, adapters, sources, and sinks into a fresh
PluginRegistry, with correct type identifiers, capabilities, and option contracts.
"""

from __future__ import annotations

import pytest

from rivet_core.plugins import (
    CatalogPlugin,
    ComputeEnginePlugin,
    PluginRegistrationError,
    PluginRegistry,
    SinkPlugin,
    SourcePlugin,
)

# ── Helpers ──────────────────────────────────────────────────────────────────

def _fresh_registry() -> PluginRegistry:
    return PluginRegistry()


# ── rivet_duckdb ─────────────────────────────────────────────────────────────

class TestDuckDBPluginRegistration:
    def setup_method(self):
        from rivet_duckdb import DuckDBPlugin
        self.registry = _fresh_registry()
        DuckDBPlugin(self.registry)

    def test_catalog_type_registered(self):
        plugin = self.registry.get_catalog_plugin("duckdb")
        assert plugin is not None
        assert isinstance(plugin, CatalogPlugin)
        assert plugin.type == "duckdb"

    def test_engine_type_registered(self):
        plugin = self.registry.get_engine_plugin("duckdb")
        assert plugin is not None
        assert isinstance(plugin, ComputeEnginePlugin)
        assert plugin.engine_type == "duckdb"

    def test_engine_dialect(self):
        plugin = self.registry.get_engine_plugin("duckdb")
        assert plugin.dialect == "duckdb"

    def test_engine_native_catalog_types(self):
        plugin = self.registry.get_engine_plugin("duckdb")
        assert "duckdb" in plugin.supported_catalog_types
        assert "arrow" in plugin.supported_catalog_types
        assert "filesystem" in plugin.supported_catalog_types

    def test_engine_all_6_capabilities_for_native_types(self):
        plugin = self.registry.get_engine_plugin("duckdb")
        expected = {
            "projection_pushdown", "predicate_pushdown", "limit_pushdown",
            "cast_pushdown", "join", "aggregation",
        }
        for cat_type in ("duckdb", "arrow", "filesystem"):
            assert set(plugin.supported_catalog_types[cat_type]) == expected

    def test_s3_adapter_registered(self):
        adapter = self.registry.get_adapter("duckdb", "s3")
        assert adapter is not None
        assert adapter.target_engine_type == "duckdb"
        assert adapter.catalog_type == "s3"
        assert adapter.source == "engine_plugin"

    def test_glue_adapter_registered(self):
        adapter = self.registry.get_adapter("duckdb", "glue")
        assert adapter is not None
        assert adapter.target_engine_type == "duckdb"
        assert adapter.catalog_type == "glue"

    def test_unity_adapter_registered(self):
        adapter = self.registry.get_adapter("duckdb", "unity")
        assert adapter is not None
        assert adapter.target_engine_type == "duckdb"
        assert adapter.catalog_type == "unity"

    def test_no_postgres_adapter(self):
        # rivet_duckdb must NOT register a postgres adapter (rivet_postgres owns it)
        adapter = self.registry.get_adapter("duckdb", "postgres")
        assert adapter is None

    def test_source_registered(self):
        source = self.registry._sources.get("duckdb")
        assert source is not None
        assert isinstance(source, SourcePlugin)

    def test_sink_registered(self):
        sink = self.registry._sinks.get("duckdb")
        assert sink is not None
        assert isinstance(sink, SinkPlugin)

    def test_filesystem_sink_registered(self):
        sink = self.registry._sinks.get("filesystem")
        assert sink is not None
        assert isinstance(sink, SinkPlugin)

    def test_catalog_optional_options(self):
        plugin = self.registry.get_catalog_plugin("duckdb")
        assert "path" in plugin.optional_options
        assert "read_only" in plugin.optional_options

    def test_engine_optional_options(self):
        plugin = self.registry.get_engine_plugin("duckdb")
        assert "threads" in plugin.optional_options
        assert "memory_limit" in plugin.optional_options

    def test_double_registration_raises(self):
        from rivet_duckdb import DuckDBPlugin
        with pytest.raises(PluginRegistrationError):
            DuckDBPlugin(self.registry)


# ── rivet_postgres ────────────────────────────────────────────────────────────

class TestPostgresPluginRegistration:
    def setup_method(self):
        from rivet_postgres import PostgresPlugin
        self.registry = _fresh_registry()
        PostgresPlugin(self.registry)

    def test_catalog_type_registered(self):
        plugin = self.registry.get_catalog_plugin("postgres")
        assert plugin is not None
        assert isinstance(plugin, CatalogPlugin)
        assert plugin.type == "postgres"

    def test_engine_type_registered(self):
        plugin = self.registry.get_engine_plugin("postgres")
        assert plugin is not None
        assert isinstance(plugin, ComputeEnginePlugin)
        assert plugin.engine_type == "postgres"

    def test_engine_dialect(self):
        plugin = self.registry.get_engine_plugin("postgres")
        assert plugin.dialect == "postgres"

    def test_engine_native_catalog_type(self):
        plugin = self.registry.get_engine_plugin("postgres")
        assert "postgres" in plugin.supported_catalog_types

    def test_engine_all_6_capabilities(self):
        plugin = self.registry.get_engine_plugin("postgres")
        expected = {
            "projection_pushdown", "predicate_pushdown", "limit_pushdown",
            "cast_pushdown", "join", "aggregation",
        }
        assert set(plugin.supported_catalog_types["postgres"]) == expected

    def test_duckdb_adapter_registered_as_catalog_plugin(self):
        adapter = self.registry.get_adapter("duckdb", "postgres")
        assert adapter is not None
        assert adapter.target_engine_type == "duckdb"
        assert adapter.catalog_type == "postgres"
        assert adapter.source == "catalog_plugin"
        assert adapter.source_plugin == "rivet_postgres"

    def test_pyspark_adapter_registered(self):
        adapter = self.registry.get_adapter("pyspark", "postgres")
        assert adapter is not None
        assert adapter.target_engine_type == "pyspark"
        assert adapter.catalog_type == "postgres"

    def test_source_registered(self):
        source = self.registry._sources.get("postgres")
        assert source is not None
        assert isinstance(source, SourcePlugin)

    def test_sink_registered(self):
        sink = self.registry._sinks.get("postgres")
        assert sink is not None
        assert isinstance(sink, SinkPlugin)

    def test_catalog_required_options(self):
        plugin = self.registry.get_catalog_plugin("postgres")
        assert "host" in plugin.required_options
        assert "database" in plugin.required_options

    def test_catalog_credential_options(self):
        plugin = self.registry.get_catalog_plugin("postgres")
        assert "user" in plugin.credential_options
        assert "password" in plugin.credential_options

    def test_engine_optional_options(self):
        plugin = self.registry.get_engine_plugin("postgres")
        assert "pool_min_size" in plugin.optional_options
        assert "pool_max_size" in plugin.optional_options

    def test_double_registration_raises(self):
        from rivet_postgres import PostgresPlugin
        with pytest.raises(PluginRegistrationError):
            PostgresPlugin(self.registry)


# ── rivet_aws ─────────────────────────────────────────────────────────────────

class TestAWSPluginRegistration:
    def setup_method(self):
        from rivet_aws import AWSPlugin
        self.registry = _fresh_registry()
        AWSPlugin(self.registry)

    def test_s3_catalog_registered(self):
        plugin = self.registry.get_catalog_plugin("s3")
        assert plugin is not None
        assert isinstance(plugin, CatalogPlugin)
        assert plugin.type == "s3"

    def test_glue_catalog_registered(self):
        plugin = self.registry.get_catalog_plugin("glue")
        assert plugin is not None
        assert isinstance(plugin, CatalogPlugin)
        assert plugin.type == "glue"

    def test_no_engine_registered(self):
        # rivet_aws registers no compute engine
        assert self.registry.get_engine_plugin("s3") is None
        assert self.registry.get_engine_plugin("glue") is None
        assert self.registry.get_engine_plugin("aws") is None

    def test_s3_source_registered(self):
        source = self.registry._sources.get("s3")
        assert source is not None
        assert isinstance(source, SourcePlugin)

    def test_s3_sink_registered(self):
        sink = self.registry._sinks.get("s3")
        assert sink is not None
        assert isinstance(sink, SinkPlugin)

    def test_glue_source_registered(self):
        source = self.registry._sources.get("glue")
        assert source is not None
        assert isinstance(source, SourcePlugin)

    def test_glue_sink_registered(self):
        sink = self.registry._sinks.get("glue")
        assert sink is not None
        assert isinstance(sink, SinkPlugin)

    def test_s3_catalog_required_options(self):
        plugin = self.registry.get_catalog_plugin("s3")
        assert "bucket" in plugin.required_options

    def test_s3_catalog_optional_options(self):
        plugin = self.registry.get_catalog_plugin("s3")
        assert "region" in plugin.optional_options
        assert "format" in plugin.optional_options

    def test_glue_catalog_required_options(self):
        plugin = self.registry.get_catalog_plugin("glue")
        # database is optional (defaults to None), no required options
        assert plugin.required_options == []

    def test_glue_catalog_optional_options(self):
        plugin = self.registry.get_catalog_plugin("glue")
        assert "region" in plugin.optional_options

    def test_double_registration_raises(self):
        from rivet_aws import AWSPlugin
        with pytest.raises(PluginRegistrationError):
            AWSPlugin(self.registry)


# ── rivet_databricks ───────────────────────────────────────────────────────────────

class TestDatabricksPluginRegistration:
    def setup_method(self):
        from rivet_databricks import DatabricksPlugin
        self.registry = _fresh_registry()
        DatabricksPlugin(self.registry)

    def test_unity_catalog_registered(self):
        plugin = self.registry.get_catalog_plugin("unity")
        assert plugin is not None
        assert isinstance(plugin, CatalogPlugin)
        assert plugin.type == "unity"

    def test_databricks_catalog_registered(self):
        plugin = self.registry.get_catalog_plugin("databricks")
        assert plugin is not None
        assert isinstance(plugin, CatalogPlugin)
        assert plugin.type == "databricks"

    def test_databricks_engine_registered(self):
        plugin = self.registry.get_engine_plugin("databricks")
        assert plugin is not None
        assert isinstance(plugin, ComputeEnginePlugin)
        assert plugin.engine_type == "databricks"

    def test_databricks_engine_dialect(self):
        plugin = self.registry.get_engine_plugin("databricks")
        assert plugin.dialect == "databricks"

    def test_databricks_engine_native_catalog_type(self):
        plugin = self.registry.get_engine_plugin("databricks")
        assert "databricks" in plugin.supported_catalog_types

    def test_databricks_engine_all_6_capabilities(self):
        plugin = self.registry.get_engine_plugin("databricks")
        expected = {
            "projection_pushdown", "predicate_pushdown", "limit_pushdown",
            "cast_pushdown", "join", "aggregation",
        }
        assert set(plugin.supported_catalog_types["databricks"]) == expected

    def test_duckdb_adapter_registered(self):
        """DatabricksPlugin registers a DuckDB adapter for databricks catalog."""
        adapter = self.registry.get_adapter("duckdb", "databricks")
        assert adapter is not None

    def test_unity_source_registered(self):
        source = self.registry._sources.get("unity")
        assert source is not None
        assert isinstance(source, SourcePlugin)

    def test_databricks_source_registered(self):
        source = self.registry._sources.get("databricks")
        assert source is not None
        assert isinstance(source, SourcePlugin)

    def test_unity_sink_registered(self):
        sink = self.registry._sinks.get("unity")
        assert sink is not None
        assert isinstance(sink, SinkPlugin)

    def test_databricks_sink_registered(self):
        sink = self.registry._sinks.get("databricks")
        assert sink is not None
        assert isinstance(sink, SinkPlugin)

    def test_unity_catalog_required_options(self):
        plugin = self.registry.get_catalog_plugin("unity")
        assert "host" in plugin.required_options
        assert "catalog_name" in plugin.required_options

    def test_databricks_catalog_required_options(self):
        plugin = self.registry.get_catalog_plugin("databricks")
        assert "workspace_url" in plugin.required_options
        assert "catalog" in plugin.required_options

    def test_databricks_engine_required_options(self):
        plugin = self.registry.get_engine_plugin("databricks")
        assert "warehouse_id" in plugin.required_options

    def test_double_registration_raises(self):
        from rivet_databricks import DatabricksPlugin
        with pytest.raises(PluginRegistrationError):
            DatabricksPlugin(self.registry)


# ── rivet_polars ──────────────────────────────────────────────────────────────

class TestPolarsPluginRegistration:
    def setup_method(self):
        from rivet_polars import PolarsPlugin
        self.registry = _fresh_registry()
        PolarsPlugin(self.registry)

    def test_no_catalog_registered(self):
        # Polars is engine-only; registers no catalog types
        assert self.registry.get_catalog_plugin("polars") is None

    def test_engine_type_registered(self):
        plugin = self.registry.get_engine_plugin("polars")
        assert plugin is not None
        assert isinstance(plugin, ComputeEnginePlugin)
        assert plugin.engine_type == "polars"

    def test_engine_dialect(self):
        plugin = self.registry.get_engine_plugin("polars")
        assert plugin.dialect == "duckdb"

    def test_engine_native_catalog_types(self):
        plugin = self.registry.get_engine_plugin("polars")
        assert "arrow" in plugin.supported_catalog_types
        assert "filesystem" in plugin.supported_catalog_types

    def test_engine_all_6_capabilities(self):
        plugin = self.registry.get_engine_plugin("polars")
        expected = {
            "projection_pushdown", "predicate_pushdown", "limit_pushdown",
            "cast_pushdown", "join", "aggregation",
        }
        for cat_type in ("arrow", "filesystem"):
            assert set(plugin.supported_catalog_types[cat_type]) == expected

    def test_s3_adapter_registered(self):
        adapter = self.registry.get_adapter("polars", "s3")
        assert adapter is not None
        assert adapter.target_engine_type == "polars"
        assert adapter.catalog_type == "s3"

    def test_glue_adapter_registered(self):
        adapter = self.registry.get_adapter("polars", "glue")
        assert adapter is not None
        assert adapter.target_engine_type == "polars"
        assert adapter.catalog_type == "glue"

    def test_unity_adapter_registered(self):
        adapter = self.registry.get_adapter("polars", "unity")
        assert adapter is not None
        assert adapter.target_engine_type == "polars"
        assert adapter.catalog_type == "unity"

    def test_engine_optional_options(self):
        plugin = self.registry.get_engine_plugin("polars")
        assert "streaming" in plugin.optional_options
        assert "n_threads" in plugin.optional_options

    def test_double_registration_raises(self):
        from rivet_polars import PolarsPlugin
        with pytest.raises(PluginRegistrationError):
            PolarsPlugin(self.registry)


# ── rivet_pyspark ─────────────────────────────────────────────────────────────

class TestPySparkPluginRegistration:
    def setup_method(self):
        from rivet_pyspark import PySparkPlugin
        self.registry = _fresh_registry()
        PySparkPlugin(self.registry)

    def test_no_catalog_registered(self):
        # PySpark is engine-only; registers no catalog types
        assert self.registry.get_catalog_plugin("pyspark") is None

    def test_engine_type_registered(self):
        plugin = self.registry.get_engine_plugin("pyspark")
        assert plugin is not None
        assert isinstance(plugin, ComputeEnginePlugin)
        assert plugin.engine_type == "pyspark"

    def test_engine_dialect(self):
        plugin = self.registry.get_engine_plugin("pyspark")
        assert plugin.dialect == "spark"

    def test_engine_native_catalog_types(self):
        plugin = self.registry.get_engine_plugin("pyspark")
        assert "arrow" in plugin.supported_catalog_types
        assert "filesystem" in plugin.supported_catalog_types

    def test_engine_all_6_capabilities(self):
        plugin = self.registry.get_engine_plugin("pyspark")
        expected = {
            "projection_pushdown", "predicate_pushdown", "limit_pushdown",
            "cast_pushdown", "join", "aggregation",
        }
        for cat_type in ("arrow", "filesystem"):
            assert set(plugin.supported_catalog_types[cat_type]) == expected

    def test_s3_adapter_registered(self):
        adapter = self.registry.get_adapter("pyspark", "s3")
        assert adapter is not None
        assert adapter.target_engine_type == "pyspark"
        assert adapter.catalog_type == "s3"

    def test_glue_adapter_registered(self):
        adapter = self.registry.get_adapter("pyspark", "glue")
        assert adapter is not None
        assert adapter.target_engine_type == "pyspark"
        assert adapter.catalog_type == "glue"

    def test_unity_adapter_registered(self):
        adapter = self.registry.get_adapter("pyspark", "unity")
        assert adapter is not None
        assert adapter.target_engine_type == "pyspark"
        assert adapter.catalog_type == "unity"

    def test_engine_optional_options(self):
        plugin = self.registry.get_engine_plugin("pyspark")
        assert "master" in plugin.optional_options
        assert "app_name" in plugin.optional_options
        assert "connect_url" in plugin.optional_options

    def test_double_registration_raises(self):
        from rivet_pyspark import PySparkPlugin
        with pytest.raises(PluginRegistrationError):
            PySparkPlugin(self.registry)


# ── Cross-package: all 6 in one registry ─────────────────────────────────────

class TestAllPluginsInOneRegistry:
    """Verify all 6 plugins can coexist in a single registry without conflicts."""

    def test_all_plugins_register_without_conflict(self):
        from rivet_aws import AWSPlugin
        from rivet_databricks import DatabricksPlugin
        from rivet_duckdb import DuckDBPlugin
        from rivet_polars import PolarsPlugin
        from rivet_postgres import PostgresPlugin
        from rivet_pyspark import PySparkPlugin

        registry = _fresh_registry()
        DuckDBPlugin(registry)
        PostgresPlugin(registry)
        AWSPlugin(registry)
        DatabricksPlugin(registry)
        PolarsPlugin(registry)
        PySparkPlugin(registry)

        # All catalog types present
        for ct in ("duckdb", "postgres", "s3", "glue", "unity", "databricks"):
            assert registry.get_catalog_plugin(ct) is not None, f"Missing catalog plugin: {ct}"

        # All engine types present
        for et in ("duckdb", "postgres", "databricks", "polars", "pyspark"):
            assert registry.get_engine_plugin(et) is not None, f"Missing engine plugin: {et}"

    def test_postgres_duckdb_adapter_overrides_duckdb_baseline(self):
        """PostgresDuckDBAdapter (catalog_plugin) must win over any engine_plugin adapter."""
        from rivet_duckdb import DuckDBPlugin
        from rivet_postgres import PostgresPlugin

        registry = _fresh_registry()
        DuckDBPlugin(registry)
        # Register postgres after duckdb; its catalog_plugin adapter should override
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            PostgresPlugin(registry)

        adapter = registry.get_adapter("duckdb", "postgres")
        assert adapter is not None
        assert adapter.source == "catalog_plugin"
        assert adapter.source_plugin == "rivet_postgres"
