"""Unit tests for wildcard adapter resolution in PluginRegistry.

Covers:
- Exact match returned when both exact and wildcard exist
- Wildcard returned for Arrow-compatible engine
- None returned for non-Arrow engine
- None returned when no wildcard registered
- resolve_capabilities with wildcard fallback
"""

from __future__ import annotations

from typing import Any

from rivet_core.models import ComputeEngine
from rivet_core.plugins import (
    ComputeEngineAdapter,
    ComputeEnginePlugin,
    PluginRegistry,
)

# ── Stubs ───────────────────────────────────────────────────────────────────────


class _StubEnginePlugin(ComputeEnginePlugin):
    def __init__(self, engine_type: str, catalog_types: dict[str, list[str]]) -> None:
        self.engine_type = engine_type  # type: ignore[assignment]
        self.supported_catalog_types = catalog_types

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type=self.engine_type)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(self, engine: Any, sql: Any, input_tables: Any) -> Any:
        raise NotImplementedError


class _StubAdapter(ComputeEngineAdapter):
    def __init__(self, engine_type: str, catalog_type: str, caps: list[str] | None = None) -> None:
        self.target_engine_type = engine_type  # type: ignore[assignment]
        self.catalog_type = catalog_type  # type: ignore[assignment]
        self.capabilities = caps or ["projection_pushdown"]  # type: ignore[assignment]
        self.source = "catalog_plugin"  # type: ignore[assignment]

    def read_dispatch(self, engine: Any, catalog: Any, joint: Any, pushdown: Any = None) -> Any:
        raise NotImplementedError

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        raise NotImplementedError


# ── Tests: get_adapter ──────────────────────────────────────────────────────────


def test_exact_match_returned_when_both_exist() -> None:
    reg = PluginRegistry()
    reg.register_engine_plugin(_StubEnginePlugin("duckdb", {"arrow": [], "duckdb": []}))
    exact = _StubAdapter("duckdb", "rest_api", ["predicate_pushdown"])
    wildcard = _StubAdapter("*", "rest_api", ["projection_pushdown"])
    reg.register_adapter(exact)
    reg.register_adapter(wildcard)

    result = reg.get_adapter("duckdb", "rest_api")
    assert result is exact


def test_wildcard_returned_for_arrow_compatible_engine() -> None:
    reg = PluginRegistry()
    reg.register_engine_plugin(_StubEnginePlugin("polars", {"arrow": []}))
    wildcard = _StubAdapter("*", "rest_api", ["projection_pushdown"])
    reg.register_adapter(wildcard)

    result = reg.get_adapter("polars", "rest_api")
    assert result is wildcard


def test_none_returned_for_non_arrow_engine() -> None:
    reg = PluginRegistry()
    reg.register_engine_plugin(_StubEnginePlugin("databricks", {"databricks": [], "unity": []}))
    wildcard = _StubAdapter("*", "rest_api")
    reg.register_adapter(wildcard)

    result = reg.get_adapter("databricks", "rest_api")
    assert result is None


def test_none_returned_when_no_wildcard_registered() -> None:
    reg = PluginRegistry()
    reg.register_engine_plugin(_StubEnginePlugin("duckdb", {"arrow": []}))

    result = reg.get_adapter("duckdb", "rest_api")
    assert result is None


def test_none_returned_when_engine_not_registered() -> None:
    reg = PluginRegistry()
    wildcard = _StubAdapter("*", "rest_api")
    reg.register_adapter(wildcard)

    result = reg.get_adapter("unknown_engine", "rest_api")
    assert result is None


# ── Tests: resolve_capabilities ─────────────────────────────────────────────────


def test_resolve_capabilities_wildcard_fallback() -> None:
    reg = PluginRegistry()
    reg.register_engine_plugin(_StubEnginePlugin("duckdb", {"arrow": []}))
    wildcard = _StubAdapter("*", "rest_api", ["projection_pushdown", "predicate_pushdown"])
    reg.register_adapter(wildcard)

    caps = reg.resolve_capabilities("duckdb", "rest_api")
    assert caps == ["projection_pushdown", "predicate_pushdown"]


def test_resolve_capabilities_exact_adapter_takes_precedence() -> None:
    reg = PluginRegistry()
    reg.register_engine_plugin(_StubEnginePlugin("duckdb", {"arrow": []}))
    exact = _StubAdapter("duckdb", "rest_api", ["limit_pushdown"])
    wildcard = _StubAdapter("*", "rest_api", ["projection_pushdown"])
    reg.register_adapter(exact)
    reg.register_adapter(wildcard)

    caps = reg.resolve_capabilities("duckdb", "rest_api")
    assert caps == ["limit_pushdown"]


def test_resolve_capabilities_none_for_non_arrow_engine() -> None:
    reg = PluginRegistry()
    reg.register_engine_plugin(_StubEnginePlugin("databricks", {"databricks": []}))
    wildcard = _StubAdapter("*", "rest_api", ["projection_pushdown"])
    reg.register_adapter(wildcard)

    caps = reg.resolve_capabilities("databricks", "rest_api")
    assert caps is None


def test_resolve_capabilities_engine_native_support_takes_precedence() -> None:
    """When the engine natively supports the catalog type, those caps are returned
    even if a wildcard adapter also exists."""
    reg = PluginRegistry()
    reg.register_engine_plugin(
        _StubEnginePlugin("duckdb", {"arrow": [], "rest_api": ["native_cap"]})
    )
    wildcard = _StubAdapter("*", "rest_api", ["wildcard_cap"])
    reg.register_adapter(wildcard)

    caps = reg.resolve_capabilities("duckdb", "rest_api")
    # Engine native support is checked before wildcard fallback
    assert caps == ["native_cap"]
