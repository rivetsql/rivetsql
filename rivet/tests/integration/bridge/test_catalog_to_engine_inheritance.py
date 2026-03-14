"""Integration tests for catalog-to-engine connection parameter inheritance."""

from __future__ import annotations

from rivet_bridge.engines import EngineInstantiator, _merge_catalog_connection_params
from rivet_config import CatalogConfig, EngineConfig, ResolvedProfile
from rivet_core import PluginRegistry
from rivet_postgres.engine import PostgresComputeEnginePlugin


class TestCatalogToEngineInheritance:
    """Test that engines inherit connection params from matching catalogs."""

    def test_merge_catalog_connection_params_inherits_from_catalog(self) -> None:
        """Engine should inherit connection params from catalog when not specified."""
        catalog_options = {
            "host": "catalog-host",
            "port": 5432,
            "database": "catalog-db",
            "user": "catalog-user",
            "password": "catalog-pass",
            "schema": "public",
        }
        engine_options = {
            "pool_min_size": 2,
        }

        merged = _merge_catalog_connection_params(engine_options, catalog_options)

        assert merged["host"] == "catalog-host"
        assert merged["port"] == 5432
        assert merged["database"] == "catalog-db"
        assert merged["user"] == "catalog-user"
        assert merged["password"] == "catalog-pass"
        assert merged["schema"] == "public"
        assert merged["pool_min_size"] == 2

    def test_merge_catalog_connection_params_engine_overrides(self) -> None:
        """Engine options should override catalog options."""
        catalog_options = {
            "host": "catalog-host",
            "port": 5432,
            "database": "catalog-db",
            "user": "catalog-user",
            "password": "catalog-pass",
        }
        engine_options = {
            "host": "engine-host",
            "database": "engine-db",
            "pool_min_size": 2,
        }

        merged = _merge_catalog_connection_params(engine_options, catalog_options)

        # Engine overrides
        assert merged["host"] == "engine-host"
        assert merged["database"] == "engine-db"
        # Inherited from catalog
        assert merged["port"] == 5432
        assert merged["user"] == "catalog-user"
        assert merged["password"] == "catalog-pass"
        # Engine-specific
        assert merged["pool_min_size"] == 2

    def test_merge_catalog_connection_params_ignores_non_connection_params(self) -> None:
        """Should only merge recognized connection parameters."""
        catalog_options = {
            "host": "catalog-host",
            "read_only": True,
            "custom_option": "value",
        }
        engine_options = {}

        merged = _merge_catalog_connection_params(engine_options, catalog_options)

        assert merged["host"] == "catalog-host"
        assert "read_only" not in merged
        assert "custom_option" not in merged

    def test_engine_instantiator_inherits_from_matching_catalog(self) -> None:
        """EngineInstantiator should merge params when engine type matches catalog type."""
        registry = PluginRegistry()
        registry.register_engine_plugin(PostgresComputeEnginePlugin())

        catalog = CatalogConfig(
            name="pg_catalog",
            type="postgres",
            options={
                "host": "localhost",
                "port": 5432,
                "database": "testdb",
                "user": "testuser",
                "password": "testpass",
            },
        )

        engine = EngineConfig(
            name="pg_engine",
            type="postgres",
            catalogs=["pg_catalog"],
            options={
                "pool_min_size": 2,
            },
        )

        profile = ResolvedProfile(
            name="test",
            default_engine="pg_engine",
            catalogs={"pg_catalog": catalog},
            engines=[engine],
        )

        instantiator = EngineInstantiator()
        engines, errors = instantiator.instantiate_all(profile, registry)

        assert len(errors) == 0
        assert "pg_engine" in engines
        pg_engine = engines["pg_engine"]
        assert pg_engine.config["host"] == "localhost"
        assert pg_engine.config["port"] == 5432
        assert pg_engine.config["database"] == "testdb"
        assert pg_engine.config["user"] == "testuser"
        assert pg_engine.config["password"] == "testpass"
        assert pg_engine.config["pool_min_size"] == 2

    def test_engine_instantiator_no_inheritance_when_types_differ(self) -> None:
        """EngineInstantiator should not merge params when engine and catalog types differ."""
        registry = PluginRegistry()
        registry.register_engine_plugin(PostgresComputeEnginePlugin())

        catalog = CatalogConfig(
            name="duckdb_catalog",
            type="duckdb",
            options={
                "path": "/tmp/test.db",
            },
        )

        engine = EngineConfig(
            name="pg_engine",
            type="postgres",
            catalogs=["duckdb_catalog"],
            options={
                "host": "localhost",
                "port": 5432,
                "database": "testdb",
                "user": "testuser",
                "password": "testpass",
            },
        )

        profile = ResolvedProfile(
            name="test",
            default_engine="pg_engine",
            catalogs={"duckdb_catalog": catalog},
            engines=[engine],
        )

        instantiator = EngineInstantiator()
        engines, errors = instantiator.instantiate_all(profile, registry)

        assert len(errors) == 0
        assert "pg_engine" in engines
        pg_engine = engines["pg_engine"]
        # Should use engine options only, no inheritance
        assert pg_engine.config["host"] == "localhost"
        assert "path" not in pg_engine.config

    def test_engine_instantiator_uses_first_matching_catalog(self) -> None:
        """EngineInstantiator should use first catalog with matching type."""
        registry = PluginRegistry()
        registry.register_engine_plugin(PostgresComputeEnginePlugin())

        catalog1 = CatalogConfig(
            name="pg_catalog1",
            type="postgres",
            options={
                "host": "host1",
                "port": 5432,
                "database": "db1",
                "user": "user1",
                "password": "pass1",
            },
        )

        catalog2 = CatalogConfig(
            name="pg_catalog2",
            type="postgres",
            options={
                "host": "host2",
                "port": 5433,
                "database": "db2",
                "user": "user2",
                "password": "pass2",
            },
        )

        engine = EngineConfig(
            name="pg_engine",
            type="postgres",
            catalogs=["pg_catalog1", "pg_catalog2"],
            options={},
        )

        profile = ResolvedProfile(
            name="test",
            default_engine="pg_engine",
            catalogs={"pg_catalog1": catalog1, "pg_catalog2": catalog2},
            engines=[engine],
        )

        instantiator = EngineInstantiator()
        engines, errors = instantiator.instantiate_all(profile, registry)

        assert len(errors) == 0
        assert "pg_engine" in engines
        pg_engine = engines["pg_engine"]
        # Should use first matching catalog (pg_catalog1)
        assert pg_engine.config["host"] == "host1"
        assert pg_engine.config["database"] == "db1"
        assert pg_engine.config["user"] == "user1"
