"""rivet_aws — AWS plugin for Rivet."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rivet_core.plugins import PluginRegistry


def AWSPlugin(registry: PluginRegistry) -> None:
    """Register all rivet_aws components into the plugin registry."""
    from rivet_aws.glue_catalog import GlueCatalogPlugin
    from rivet_aws.glue_sink import GlueSink
    from rivet_aws.glue_source import GlueSource
    from rivet_aws.s3_catalog import S3CatalogPlugin
    from rivet_aws.s3_sink import S3Sink
    from rivet_aws.s3_source import S3Source

    registry.register_catalog_plugin(S3CatalogPlugin())
    registry.register_catalog_plugin(GlueCatalogPlugin())
    registry.register_source(S3Source())
    registry.register_sink(S3Sink())
    registry.register_source(GlueSource())
    registry.register_sink(GlueSink())
