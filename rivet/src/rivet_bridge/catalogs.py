"""Catalog instantiation from resolved profile configurations."""

from __future__ import annotations

from rivet_bridge.errors import BridgeError
from rivet_config import ResolvedProfile
from rivet_core import Catalog, PluginRegistry


class CatalogInstantiator:
    """Creates core Catalog objects from ResolvedProfile catalog configurations."""

    def instantiate_all(
        self,
        profile: ResolvedProfile,
        registry: PluginRegistry,
    ) -> tuple[dict[str, Catalog], list[BridgeError]]:
        """Create Catalog objects for all catalogs in the profile.

        For each CatalogConfig:
        1. Look up CatalogPlugin by type → BRG-201 if not found
        2. Call plugin.validate(options) → BRG-202 on failure
        3. Call plugin.instantiate(name, options) → Catalog

        Returns (catalogs_map, errors).
        """
        catalogs: dict[str, Catalog] = {}
        errors: list[BridgeError] = []

        for name, config in profile.catalogs.items():
            plugin = registry.get_catalog_plugin(config.type)
            if plugin is None:
                errors.append(BridgeError(
                    code="BRG-201",
                    message=f"Unknown catalog plugin type '{config.type}' for catalog '{name}'.",
                    joint_name=name,
                    remediation=f"Register a CatalogPlugin for type '{config.type}' before building.",
                ))
                continue

            try:
                plugin.validate(config.options)
            except Exception as exc:
                errors.append(BridgeError(
                    code="BRG-202",
                    message=f"Catalog '{name}' validation failed: {exc}",
                    joint_name=name,
                    remediation="Fix the catalog options to satisfy plugin validation.",
                ))
                continue

            try:
                catalogs[name] = plugin.instantiate(name, config.options)
            except Exception as exc:
                errors.append(BridgeError(
                    code="BRG-202",
                    message=f"Catalog '{name}' instantiation failed: {exc}",
                    joint_name=name,
                    remediation="Fix the catalog options to satisfy plugin requirements.",
                ))

        return catalogs, errors
