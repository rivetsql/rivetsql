"""Property-based tests for wildcard adapter resolution in PluginRegistry.

Property 5: Wildcard resolution with Arrow gate
  For any (engine_type, catalog_type) pair where no exact adapter exists but a
  wildcard ("*", catalog_type) adapter is registered, get_adapter() returns the
  wildcard adapter iff the engine declares "arrow" in supported_catalog_types.

Property 6: Exact match precedence
  For any (engine_type, catalog_type) pair where both an exact and wildcard
  adapter are registered, get_adapter() always returns the exact match.

Property 7: resolve_capabilities consistency
  For any (engine_type, catalog_type) pair where the wildcard adapter resolves,
  resolve_capabilities() returns the wildcard adapter's capabilities list.
"""

from __future__ import annotations

from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

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


# ── Strategies ──────────────────────────────────────────────────────────────────

_identifier = st.text(
    alphabet=st.characters(whitelist_categories=("Ll",), whitelist_characters="_"),
    min_size=2,
    max_size=10,
).filter(lambda s: s != "arrow")  # avoid collision with the "arrow" gate key

_capabilities = st.lists(
    st.sampled_from(["projection_pushdown", "predicate_pushdown", "limit_pushdown"]),
    min_size=1,
    max_size=3,
    unique=True,
)


# ── Property 5: Wildcard resolution with Arrow gate ────────────────────────────


@given(
    engine_type=_identifier,
    catalog_type=_identifier,
    has_arrow=st.booleans(),
    wildcard_caps=_capabilities,
)
@settings(max_examples=100)
def test_wildcard_resolution_arrow_gate(
    engine_type: str,
    catalog_type: str,
    has_arrow: bool,
    wildcard_caps: list[str],
) -> None:
    """Wildcard adapter returned iff engine declares 'arrow' support."""
    reg = PluginRegistry()

    cat_types: dict[str, list[str]] = {engine_type: []}
    if has_arrow:
        cat_types["arrow"] = []
    reg.register_engine_plugin(_StubEnginePlugin(engine_type, cat_types))

    wildcard = _StubAdapter("*", catalog_type, wildcard_caps)
    reg.register_adapter(wildcard)

    result = reg.get_adapter(engine_type, catalog_type)

    if has_arrow:
        assert result is wildcard
    else:
        assert result is None


# ── Property 6: Exact match precedence ─────────────────────────────────────────


@given(
    engine_type=_identifier,
    catalog_type=_identifier,
    exact_caps=_capabilities,
    wildcard_caps=_capabilities,
)
@settings(max_examples=100)
def test_exact_match_precedence_over_wildcard(
    engine_type: str,
    catalog_type: str,
    exact_caps: list[str],
    wildcard_caps: list[str],
) -> None:
    """Exact adapter always returned when both exact and wildcard exist."""
    reg = PluginRegistry()

    cat_types: dict[str, list[str]] = {engine_type: [], "arrow": []}
    reg.register_engine_plugin(_StubEnginePlugin(engine_type, cat_types))

    exact = _StubAdapter(engine_type, catalog_type, exact_caps)
    wildcard = _StubAdapter("*", catalog_type, wildcard_caps)
    reg.register_adapter(exact)
    reg.register_adapter(wildcard)

    result = reg.get_adapter(engine_type, catalog_type)
    assert result is exact


# ── Property 7: resolve_capabilities consistency ───────────────────────────────


@given(
    engine_type=_identifier,
    catalog_type=_identifier,
    wildcard_caps=_capabilities,
)
@settings(max_examples=100)
def test_resolve_capabilities_matches_wildcard_adapter(
    engine_type: str,
    catalog_type: str,
    wildcard_caps: list[str],
) -> None:
    """resolve_capabilities returns wildcard adapter's capabilities when it resolves."""
    reg = PluginRegistry()

    cat_types: dict[str, list[str]] = {"arrow": []}
    reg.register_engine_plugin(_StubEnginePlugin(engine_type, cat_types))

    wildcard = _StubAdapter("*", catalog_type, wildcard_caps)
    reg.register_adapter(wildcard)

    adapter = reg.get_adapter(engine_type, catalog_type)
    caps = reg.resolve_capabilities(engine_type, catalog_type)

    assert adapter is wildcard
    assert caps == wildcard_caps
