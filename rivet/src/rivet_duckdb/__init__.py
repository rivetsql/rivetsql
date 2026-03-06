"""rivet_duckdb — DuckDB plugin for Rivet."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rivet_core.plugins import PluginRegistry


def DuckDBPlugin(registry: PluginRegistry) -> None:
    """Register all rivet_duckdb components into the plugin registry.

    Core components (catalog, engine, source, sink) are always registered.
    Cross-catalog adapters (glue, s3, unity) are registered best-effort
    since they depend on optional packages (boto3, requests).
    """
    from rivet_duckdb.catalog import DuckDBCatalogPlugin
    from rivet_duckdb.engine import DuckDBComputeEnginePlugin
    from rivet_duckdb.filesystem_sink import FilesystemSink
    from rivet_duckdb.sink import DuckDBSink
    from rivet_duckdb.source import DuckDBSource

    registry.register_catalog_plugin(DuckDBCatalogPlugin())
    registry.register_engine_plugin(DuckDBComputeEnginePlugin())
    registry.register_source(DuckDBSource())
    registry.register_sink(DuckDBSink())
    registry.register_sink(FilesystemSink())

    # Adapters depend on optional packages — register best-effort
    try:
        from rivet_duckdb.adapters.s3 import S3DuckDBAdapter

        registry.register_adapter(S3DuckDBAdapter())
    except ImportError:
        pass
    try:
        from rivet_duckdb.adapters.glue import GlueDuckDBAdapter

        registry.register_adapter(GlueDuckDBAdapter())
    except ImportError:
        pass
    try:
        from rivet_duckdb.adapters.unity import UnityDuckDBAdapter

        registry.register_adapter(UnityDuckDBAdapter())
    except ImportError:
        pass
