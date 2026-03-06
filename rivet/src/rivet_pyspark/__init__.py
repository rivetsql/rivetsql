"""rivet_pyspark — PySpark plugin for Rivet."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rivet_core.plugins import PluginRegistry


def PySparkPlugin(registry: PluginRegistry) -> None:
    """Register all rivet_pyspark components into the plugin registry.

    Core components (engine) are always registered.
    Cross-catalog adapters (s3, glue, unity) are registered best-effort
    since they depend on optional packages.
    """
    from rivet_pyspark.engine import PySparkComputeEnginePlugin

    registry.register_engine_plugin(PySparkComputeEnginePlugin())

    # Adapters depend on optional packages — register best-effort
    try:
        from rivet_pyspark.adapters.s3 import S3PySparkAdapter

        registry.register_adapter(S3PySparkAdapter())
    except ImportError:
        pass
    try:
        from rivet_pyspark.adapters.glue import GluePySparkAdapter

        registry.register_adapter(GluePySparkAdapter())
    except ImportError:
        pass
    try:
        from rivet_pyspark.adapters.unity import UnityPySparkAdapter

        registry.register_adapter(UnityPySparkAdapter())
    except ImportError:
        pass
