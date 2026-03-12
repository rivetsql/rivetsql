"""rivet_rest — REST API catalog plugin for Rivet."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rivet_core.plugins import PluginRegistry


def RestPlugin(registry: PluginRegistry) -> None:
    """Register all rivet_rest components into the plugin registry."""
    from rivet_rest.adapter import RestApiAdapter
    from rivet_rest.catalog import RestApiCatalogPlugin
    from rivet_rest.sink import RestApiSink
    from rivet_rest.source import RestApiSource

    registry.register_catalog_plugin(RestApiCatalogPlugin())
    registry.register_source(RestApiSource())
    registry.register_sink(RestApiSink())
    registry.register_adapter(RestApiAdapter())
