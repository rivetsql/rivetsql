"""Shared fixtures for bridge tests."""

from __future__ import annotations

from typing import Any

from rivet_config import CatalogConfig, ResolvedProfile
from rivet_core import Catalog, CatalogPlugin, PluginRegistry


class MockCatalogPlugin(CatalogPlugin):
    """A mock catalog plugin for testing."""

    type = "mock"
    required_options: list[str] = []
    optional_options: dict[str, Any] = {}
    credential_options: list[str] = []

    def validate(self, options: dict[str, Any]) -> None:
        if options.get("invalid"):
            raise ValueError("invalid options")

    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog:
        return Catalog(name=name, type=self.type, options=options)

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        return logical_name


class FailingCatalogPlugin(CatalogPlugin):
    """A catalog plugin that always fails validation."""

    type = "failing"
    required_options: list[str] = []
    optional_options: dict[str, Any] = {}
    credential_options: list[str] = []

    def validate(self, options: dict[str, Any]) -> None:
        raise ValueError("always fails")

    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog:
        raise RuntimeError("should not be called")

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        return logical_name


def make_profile(
    catalogs: dict[str, CatalogConfig] | None = None,
) -> ResolvedProfile:
    """Create a minimal ResolvedProfile for testing."""
    return ResolvedProfile(
        name="test",
        default_engine="arrow",
        catalogs=catalogs or {},
        engines=[],
    )


def make_registry(*plugins: CatalogPlugin) -> PluginRegistry:
    """Create a PluginRegistry with the given catalog plugins registered."""
    registry = PluginRegistry()
    for p in plugins:
        registry.register_catalog_plugin(p)
    return registry
