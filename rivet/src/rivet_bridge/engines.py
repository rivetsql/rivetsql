"""Engine instantiation from resolved profile."""

from __future__ import annotations

from typing import Any

from rivet_bridge.errors import BridgeError
from rivet_config import ResolvedProfile
from rivet_core import ComputeEngine, PluginRegistry

# Keys handled by the framework (executor), not by individual engine plugins.
_FRAMEWORK_KEYS = frozenset({"concurrency_limit"})

# Connection parameters that can be inherited from catalog to engine
_CONNECTION_PARAMS = frozenset(
    {
        "host",
        "port",
        "database",
        "user",
        "password",
        "schema",
        "warehouse",
        "cluster_id",
        "http_path",
        "account",
        "region",
        "access_key_id",
        "secret_access_key",
    }
)


def _merge_catalog_connection_params(
    engine_options: dict[str, Any],
    catalog_options: dict[str, Any],
) -> dict[str, Any]:
    """Merge connection parameters from catalog into engine options.

    Engine options take precedence (allow overrides).
    Only merges recognized connection parameters.

    Args:
        engine_options: Engine configuration options
        catalog_options: Catalog configuration options

    Returns:
        Merged options dict with catalog params as defaults
    """
    merged = {}

    # First, add catalog connection params that aren't in engine options
    for key in _CONNECTION_PARAMS:
        if key in catalog_options and key not in engine_options:
            merged[key] = catalog_options[key]

    # Then overlay engine options (takes precedence)
    merged.update(engine_options)

    return merged


class EngineInstantiator:
    def instantiate_all(
        self,
        profile: ResolvedProfile,
        registry: PluginRegistry,
    ) -> tuple[dict[str, ComputeEngine], list[BridgeError]]:
        engines: dict[str, ComputeEngine] = {}
        errors: list[BridgeError] = []

        for eng_config in profile.engines:
            plugin = registry.get_engine_plugin(eng_config.type)
            if plugin is None:
                errors.append(
                    BridgeError(
                        code="BRG-203",
                        message=f"Unknown engine plugin type '{eng_config.type}' for engine '{eng_config.name}'.",
                        joint_name=None,
                        remediation=f"Register a ComputeEnginePlugin for type '{eng_config.type}'.",
                    )
                )
                continue

            # Merge connection params from matching catalogs
            merged_options = dict(eng_config.options)
            for catalog_name in eng_config.catalogs:
                catalog_config = profile.catalogs.get(catalog_name)
                if catalog_config and catalog_config.type == eng_config.type:
                    # Engine type matches catalog type - inherit connection params
                    merged_options = _merge_catalog_connection_params(
                        merged_options,
                        catalog_config.options,
                    )
                    break  # Use first matching catalog

            plugin_options = {k: v for k, v in merged_options.items() if k not in _FRAMEWORK_KEYS}
            try:
                plugin.validate(plugin_options)
            except Exception as exc:
                errors.append(
                    BridgeError(
                        code="BRG-204",
                        message=f"Engine '{eng_config.name}' validation failed: {exc}",
                        joint_name=None,
                        remediation="Fix the engine options in your profile.",
                    )
                )
                continue
            try:
                engine = plugin.create_engine(eng_config.name, merged_options)
                registry.register_compute_engine(engine)
                engines[eng_config.name] = engine
            except Exception as exc:
                errors.append(
                    BridgeError(
                        code="BRG-204",
                        message=f"Engine '{eng_config.name}' creation failed: {exc}",
                        joint_name=None,
                        remediation="Fix the engine options in your profile.",
                    )
                )

        return engines, errors
