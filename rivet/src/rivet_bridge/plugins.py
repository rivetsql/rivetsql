"""Optional plugin registration helpers for rivet-bridge.

Centralizes optional plugin imports so rivet_cli does not need to import
plugin packages directly (module boundary enforcement).
"""

from __future__ import annotations

from rivet_core import PluginRegistry


def register_optional_plugins(registry: PluginRegistry) -> None:
    """Register optional in-tree plugins if their packages are installed."""
    import importlib
    import logging
    import sys

    log = logging.getLogger("rivet.bridge.plugins")

    _optional = [
        ("rivet_duckdb", "DuckDBPlugin"),
        ("rivet_postgres", "PostgresPlugin"),
        ("rivet_aws", "AWSPlugin"),
        ("rivet_databricks", "DatabricksPlugin"),
        ("rivet_polars", "PolarsPlugin"),
        ("rivet_pyspark", "PySparkPlugin"),
    ]
    for module_name, fn_name in _optional:
        try:
            mod = importlib.import_module(module_name)
            plugin_fn = getattr(mod, fn_name)
            plugin_fn(registry)
        except ImportError as exc:
            log.debug("Skipping %s: %s", module_name, exc)
        except Exception as exc:  # noqa: BLE001
            print(
                f"warning: failed to register plugin {module_name}: {exc}",
                file=sys.stderr,
            )
            log.warning("Failed to register %s: %s", module_name, exc, exc_info=True)
