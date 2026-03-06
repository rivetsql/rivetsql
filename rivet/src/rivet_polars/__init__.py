"""rivet_polars — Polars plugin for Rivet."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rivet_core.plugins import PluginRegistry


def PolarsPlugin(registry: PluginRegistry) -> None:
    """Register all rivet_polars components into the plugin registry.

    Core engine plugin is always registered. Cross-catalog adapters
    are registered best-effort since they depend on optional packages.
    """
    from rivet_polars.engine import PolarsComputeEnginePlugin

    registry.register_engine_plugin(PolarsComputeEnginePlugin())

    try:
        from rivet_polars.adapters.s3 import S3PolarsAdapter

        registry.register_adapter(S3PolarsAdapter())
    except ImportError:
        pass
    try:
        from rivet_polars.adapters.glue import GluePolarsAdapter

        registry.register_adapter(GluePolarsAdapter())
    except ImportError:
        pass
    try:
        from rivet_polars.adapters.unity import UnityPolarsAdapter

        registry.register_adapter(UnityPolarsAdapter())
    except ImportError:
        pass
