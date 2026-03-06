"""rivet_databricks — Databricks plugin for Rivet."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rivet_core.plugins import PluginRegistry


def DatabricksPlugin(registry: PluginRegistry) -> None:
    """Register all rivet_databricks components into the plugin registry.

    Core components (catalogs, sources, sinks, cross-joint) are always
    registered. Engine and cross-catalog adapters are registered best-effort
    since they depend on optional packages.
    """
    # Core components — always registered
    from rivet_databricks.databricks_catalog import DatabricksCatalogPlugin
    from rivet_databricks.databricks_cross_joint import DatabricksCrossJointAdapter
    from rivet_databricks.databricks_sink import DatabricksSink
    from rivet_databricks.databricks_source import DatabricksSource
    from rivet_databricks.unity_catalog import UnityCatalogPlugin
    from rivet_databricks.unity_sink import UnitySink
    from rivet_databricks.unity_source import UnitySource

    registry.register_catalog_plugin(UnityCatalogPlugin())
    registry.register_catalog_plugin(DatabricksCatalogPlugin())
    registry.register_source(UnitySource())
    registry.register_source(DatabricksSource())
    registry.register_sink(UnitySink())
    registry.register_sink(DatabricksSink())
    registry.register_cross_joint_adapter(DatabricksCrossJointAdapter())

    # Engine depends on databricks-sql-connector — register best-effort
    try:
        from rivet_databricks.engine import DatabricksComputeEnginePlugin

        registry.register_engine_plugin(DatabricksComputeEnginePlugin())
    except ImportError:
        pass

    # Adapters depend on optional packages — register best-effort
    try:
        from rivet_databricks.adapters.unity import DatabricksUnityAdapter

        registry.register_adapter(DatabricksUnityAdapter())
    except ImportError:
        pass
    try:
        from rivet_databricks.adapters.duckdb import DatabricksDuckDBAdapter

        registry.register_adapter(DatabricksDuckDBAdapter())
    except ImportError:
        pass
