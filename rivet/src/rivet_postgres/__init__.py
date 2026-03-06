"""rivet_postgres — PostgreSQL plugin for Rivet."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rivet_core.plugins import PluginRegistry


def PostgresPlugin(registry: PluginRegistry) -> None:
    """Register all rivet_postgres components into the plugin registry.

    Core components (catalog, engine, source, sink, cross-joint) are always
    registered. Cross-catalog adapters are registered best-effort since they
    depend on optional packages.
    """
    from rivet_postgres.catalog import PostgresCatalogPlugin
    from rivet_postgres.cross_joint import PostgresCrossJointAdapter
    from rivet_postgres.engine import PostgresComputeEnginePlugin
    from rivet_postgres.sink import PostgresSink
    from rivet_postgres.source import PostgresSource

    registry.register_catalog_plugin(PostgresCatalogPlugin())
    registry.register_engine_plugin(PostgresComputeEnginePlugin())
    registry.register_source(PostgresSource())
    registry.register_sink(PostgresSink())
    registry.register_cross_joint_adapter(PostgresCrossJointAdapter())

    # Adapters depend on optional packages — register best-effort
    try:
        from rivet_postgres.adapters.duckdb import PostgresDuckDBAdapter

        registry.register_adapter(PostgresDuckDBAdapter())
    except ImportError:
        pass
    try:
        from rivet_postgres.adapters.pyspark import PostgresPySparkAdapter

        registry.register_adapter(PostgresPySparkAdapter())
    except ImportError:
        pass
