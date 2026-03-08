"""Unit tests for _resolve_concurrency_limits."""

from __future__ import annotations

import pytest

from rivet_core.errors import ExecutionError
from rivet_core.executor import _resolve_concurrency_limits
from rivet_core.models import ComputeEngine
from rivet_core.plugins import PluginRegistry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakePlugin:
    """Minimal fake engine plugin with configurable default_concurrency_limit."""

    def __init__(self, engine_type: str, default_limit: int = 1) -> None:
        self.engine_type = engine_type
        self._default_limit = default_limit

    @property
    def default_concurrency_limit(self) -> int:
        return self._default_limit


def _registry_with_plugin(plugin: _FakePlugin) -> PluginRegistry:
    """Create a PluginRegistry and manually inject a fake plugin."""
    reg = PluginRegistry()
    reg._engine_plugins[plugin.engine_type] = plugin  # type: ignore[assignment]
    return reg


# ---------------------------------------------------------------------------
# Tests: fallback chain
# ---------------------------------------------------------------------------


def test_uses_config_concurrency_limit() -> None:
    """config['concurrency_limit'] takes priority over plugin default."""
    engine = ComputeEngine(name="eng1", engine_type="duckdb", config={"concurrency_limit": 5})
    plugin = _FakePlugin("duckdb", default_limit=3)
    reg = _registry_with_plugin(plugin)

    result = _resolve_concurrency_limits([engine], reg)
    assert result == {"eng1": 5}


def test_falls_back_to_plugin_default() -> None:
    """When config has no concurrency_limit, use plugin.default_concurrency_limit."""
    engine = ComputeEngine(name="eng1", engine_type="duckdb", config={})
    plugin = _FakePlugin("duckdb", default_limit=4)
    reg = _registry_with_plugin(plugin)

    result = _resolve_concurrency_limits([engine], reg)
    assert result == {"eng1": 4}


def test_falls_back_to_1_when_no_plugin() -> None:
    """When no plugin is registered, default to 1."""
    engine = ComputeEngine(name="eng1", engine_type="unknown", config={})
    reg = PluginRegistry()

    result = _resolve_concurrency_limits([engine], reg)
    assert result == {"eng1": 1}


def test_falls_back_to_1_when_plugin_has_no_property() -> None:
    """When plugin exists but lacks default_concurrency_limit, default to 1."""

    class _BarePlugin:
        engine_type = "bare"

    reg = PluginRegistry()
    reg._engine_plugins["bare"] = _BarePlugin()  # type: ignore[assignment]
    engine = ComputeEngine(name="eng1", engine_type="bare", config={})

    result = _resolve_concurrency_limits([engine], reg)
    assert result == {"eng1": 1}


def test_multiple_engines() -> None:
    """Resolves limits for multiple engines independently."""
    e1 = ComputeEngine(name="a", engine_type="duckdb", config={"concurrency_limit": 2})
    e2 = ComputeEngine(name="b", engine_type="duckdb", config={})
    e3 = ComputeEngine(name="c", engine_type="spark", config={})
    plugin_duck = _FakePlugin("duckdb", default_limit=3)
    plugin_spark = _FakePlugin("spark", default_limit=8)
    reg = PluginRegistry()
    reg._engine_plugins["duckdb"] = plugin_duck  # type: ignore[assignment]
    reg._engine_plugins["spark"] = plugin_spark  # type: ignore[assignment]

    result = _resolve_concurrency_limits([e1, e2, e3], reg)
    assert result == {"a": 2, "b": 3, "c": 8}


# ---------------------------------------------------------------------------
# Tests: validation errors
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad_value",
    [0, -1, -100, 1.5, "abc", True, False],
    ids=["zero", "neg1", "neg100", "float", "string", "bool_true", "bool_false"],
)
def test_invalid_concurrency_limit_raises(bad_value: object) -> None:
    """Invalid concurrency_limit values raise ExecutionError with RVT-501."""
    config = {"concurrency_limit": bad_value}
    engine = ComputeEngine(name="eng1", engine_type="duckdb", config=config)
    reg = PluginRegistry()

    with pytest.raises(ExecutionError) as exc_info:
        _resolve_concurrency_limits([engine], reg)
    assert exc_info.value.error.code == "RVT-501"


def test_valid_concurrency_limit_1() -> None:
    """concurrency_limit=1 is valid (boundary)."""
    engine = ComputeEngine(name="eng1", engine_type="duckdb", config={"concurrency_limit": 1})
    reg = PluginRegistry()

    result = _resolve_concurrency_limits([engine], reg)
    assert result == {"eng1": 1}


def test_empty_engines_list() -> None:
    """Empty engine list returns empty dict."""
    reg = PluginRegistry()
    result = _resolve_concurrency_limits([], reg)
    assert result == {}
