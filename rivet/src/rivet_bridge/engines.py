"""Engine instantiation from resolved profile."""

from __future__ import annotations

from rivet_bridge.errors import BridgeError
from rivet_config import ResolvedProfile
from rivet_core import ComputeEngine, PluginRegistry

# Keys handled by the framework (executor), not by individual engine plugins.
_FRAMEWORK_KEYS = frozenset({"concurrency_limit"})


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
            plugin_options = {k: v for k, v in eng_config.options.items() if k not in _FRAMEWORK_KEYS}
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
                engine = plugin.create_engine(eng_config.name, eng_config.options)
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
