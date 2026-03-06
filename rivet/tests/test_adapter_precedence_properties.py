"""Property-based tests: adapter precedence and duplicate registration (Properties 5 & 6).

Property 5: For any (engine_type, catalog_type) pair, when both an engine_plugin adapter
and a catalog_plugin adapter are registered, get_adapter() returns the catalog_plugin adapter.

Property 6: For any two CatalogPlugin instances with the same type, or two ComputeEnginePlugin
instances with the same engine_type, registering the second raises PluginRegistrationError.
"""

from __future__ import annotations

import warnings
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.models import Catalog, ComputeEngine
from rivet_core.optimizer import EMPTY_RESIDUAL, AdapterPushdownResult
from rivet_core.plugins import (
    CatalogPlugin,
    ComputeEngineAdapter,
    ComputeEnginePlugin,
    PluginRegistrationError,
    PluginRegistry,
)

# ── Minimal concrete implementations ─────────────────────────────────


class _CatalogPlugin(CatalogPlugin):
    def __init__(self, catalog_type: str) -> None:
        self.type = catalog_type
        self.required_options: list[str] = []
        self.optional_options: dict[str, Any] = {}
        self.credential_options: list[str] = []

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog:
        return Catalog(name=name, type=self.type, options=options)

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        return logical_name


class _EnginePlugin(ComputeEnginePlugin):
    def __init__(self, engine_type: str) -> None:
        self.engine_type = engine_type
        self.supported_catalog_types: dict[str, list[str]] = {}

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type=self.engine_type)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(self, engine, sql, input_tables):
        raise NotImplementedError


class _Adapter(ComputeEngineAdapter):
    def __init__(
        self,
        engine_type: str,
        catalog_type: str,
        capabilities: list[str],
        source: str,
        source_plugin: str | None = None,
    ) -> None:
        self.target_engine_type = engine_type
        self.catalog_type = catalog_type
        self.capabilities = capabilities
        self.source = source
        self.source_plugin = source_plugin

    def read_dispatch(self, engine: Any, catalog: Any, joint: Any, pushdown: Any = None) -> AdapterPushdownResult:
        return AdapterPushdownResult(material=None, residual=EMPTY_RESIDUAL)

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        return None


# ── Strategies ────────────────────────────────────────────────────────

_identifier = st.from_regex(r"[a-z][a-z0-9_]{0,19}", fullmatch=True)
_capability = st.sampled_from([
    "projection_pushdown", "predicate_pushdown", "limit_pushdown",
    "cast_pushdown", "join", "aggregation",
])
_capabilities = st.lists(_capability, min_size=1, max_size=6, unique=True)


# ── Property 5: catalog_plugin adapter overrides engine_plugin ────────


@given(
    engine_type=_identifier,
    catalog_type=_identifier,
    engine_caps=_capabilities,
    catalog_caps=_capabilities,
)
@settings(max_examples=100)
def test_catalog_plugin_adapter_overrides_engine_plugin(
    engine_type: str, catalog_type: str, engine_caps: list[str], catalog_caps: list[str],
):
    """catalog_plugin adapter always wins over engine_plugin for same (engine_type, catalog_type)."""
    reg = PluginRegistry()
    engine_adapter = _Adapter(engine_type, catalog_type, engine_caps, "engine_plugin")
    catalog_adapter = _Adapter(engine_type, catalog_type, catalog_caps, "catalog_plugin")

    reg.register_adapter(engine_adapter)
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        reg.register_adapter(catalog_adapter)

    result = reg.get_adapter(engine_type, catalog_type)
    assert result is catalog_adapter
    assert result.source == "catalog_plugin"
    assert result.capabilities == catalog_caps


@given(
    engine_type=_identifier,
    catalog_type=_identifier,
    engine_caps=_capabilities,
    catalog_caps=_capabilities,
)
@settings(max_examples=100)
def test_engine_plugin_ignored_when_catalog_plugin_registered_first(
    engine_type: str, catalog_type: str, engine_caps: list[str], catalog_caps: list[str],
):
    """When catalog_plugin is registered first, engine_plugin adapter is ignored."""
    reg = PluginRegistry()
    catalog_adapter = _Adapter(engine_type, catalog_type, catalog_caps, "catalog_plugin")
    engine_adapter = _Adapter(engine_type, catalog_type, engine_caps, "engine_plugin")

    reg.register_adapter(catalog_adapter)
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        reg.register_adapter(engine_adapter)

    result = reg.get_adapter(engine_type, catalog_type)
    assert result is catalog_adapter


@given(
    engine_type=_identifier,
    catalog_type=_identifier,
    caps=_capabilities,
)
@settings(max_examples=100)
def test_same_source_conflict_raises_plugin_registration_error(
    engine_type: str, catalog_type: str, caps: list[str],
):
    """Two adapters from the same source for the same pair raise PluginRegistrationError."""
    for source in ("engine_plugin", "catalog_plugin"):
        reg = PluginRegistry()
        a1 = _Adapter(engine_type, catalog_type, caps, source)
        a2 = _Adapter(engine_type, catalog_type, caps, source)
        reg.register_adapter(a1)
        with pytest.raises(PluginRegistrationError):
            reg.register_adapter(a2)


@given(
    engine_type=_identifier,
    catalog_type=_identifier,
    caps=_capabilities,
)
@settings(max_examples=100)
def test_same_source_conflict_error_is_actionable(
    engine_type: str, catalog_type: str, caps: list[str],
):
    """Conflict error message names the engine_type and catalog_type."""
    reg = PluginRegistry()
    a1 = _Adapter(engine_type, catalog_type, caps, "engine_plugin")
    a2 = _Adapter(engine_type, catalog_type, caps, "engine_plugin")
    reg.register_adapter(a1)
    with pytest.raises(PluginRegistrationError) as exc_info:
        reg.register_adapter(a2)
    msg = str(exc_info.value)
    assert engine_type in msg
    assert catalog_type in msg


# ── Property 6: duplicate plugin type registration fails fast ─────────


@given(catalog_type=_identifier)
@settings(max_examples=100)
def test_duplicate_catalog_type_raises(catalog_type: str):
    """Registering two CatalogPlugins with the same type raises PluginRegistrationError."""
    reg = PluginRegistry()
    reg.register_catalog_plugin(_CatalogPlugin(catalog_type))
    with pytest.raises(PluginRegistrationError):
        reg.register_catalog_plugin(_CatalogPlugin(catalog_type))


@given(engine_type=_identifier)
@settings(max_examples=100)
def test_duplicate_engine_type_raises(engine_type: str):
    """Registering two ComputeEnginePlugins with the same engine_type raises PluginRegistrationError."""
    reg = PluginRegistry()
    reg.register_engine_plugin(_EnginePlugin(engine_type))
    with pytest.raises(PluginRegistrationError):
        reg.register_engine_plugin(_EnginePlugin(engine_type))


@given(catalog_type=_identifier)
@settings(max_examples=100)
def test_duplicate_catalog_error_names_type(catalog_type: str):
    """Duplicate catalog error message includes the conflicting type."""
    reg = PluginRegistry()
    reg.register_catalog_plugin(_CatalogPlugin(catalog_type))
    with pytest.raises(PluginRegistrationError) as exc_info:
        reg.register_catalog_plugin(_CatalogPlugin(catalog_type))
    assert catalog_type in str(exc_info.value)


@given(engine_type=_identifier)
@settings(max_examples=100)
def test_duplicate_engine_error_names_type(engine_type: str):
    """Duplicate engine error message includes the conflicting type."""
    reg = PluginRegistry()
    reg.register_engine_plugin(_EnginePlugin(engine_type))
    with pytest.raises(PluginRegistrationError) as exc_info:
        reg.register_engine_plugin(_EnginePlugin(engine_type))
    assert engine_type in str(exc_info.value)
