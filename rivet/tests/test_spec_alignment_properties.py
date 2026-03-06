# Feature: spec-alignment-fixes, Property 1: ComputeEngine config round-trip
"""Property-based test: ComputeEngine config round-trip.

For any dictionary config, creating a ComputeEngine with that config (either
directly or via ArrowComputeEnginePlugin.create_engine) stores the exact same
dict in the config field. When no config is provided, the field is an empty dict.
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.builtins.arrow_catalog import ArrowComputeEnginePlugin
from rivet_core.models import ComputeEngine

# Strategy: arbitrary JSON-like dicts (str keys, values are str/int/float/bool/None)
_json_values = st.recursive(
    st.one_of(st.none(), st.booleans(), st.integers(), st.floats(allow_nan=False), st.text()),
    lambda children: st.lists(children) | st.dictionaries(st.text(), children),
    max_leaves=10,
)
_config = st.dictionaries(st.text(), _json_values, max_size=10)
_name = st.from_regex(r"[a-z][a-z0-9_]{0,20}", fullmatch=True)


@settings(max_examples=100)
@given(name=_name, config=_config)
def test_compute_engine_direct_construction_round_trip(name: str, config: dict) -> None:
    """Direct construction preserves config exactly."""
    engine = ComputeEngine(name=name, engine_type="arrow", config=config)
    assert engine.config == config


@settings(max_examples=100)
@given(name=_name, config=_config)
def test_compute_engine_plugin_create_engine_round_trip(name: str, config: dict) -> None:
    """ArrowComputeEnginePlugin.create_engine preserves config exactly."""
    plugin = ArrowComputeEnginePlugin()
    engine = plugin.create_engine(name=name, config=config)
    assert engine.config == config


def test_compute_engine_default_config_is_empty_dict() -> None:
    """When no config is provided, the field defaults to an empty dict."""
    engine = ComputeEngine(name="test", engine_type="arrow")
    assert engine.config == {}
