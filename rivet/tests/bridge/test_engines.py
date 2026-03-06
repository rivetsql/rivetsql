"""Tests for EngineInstantiator."""

from __future__ import annotations

from typing import Any

from rivet_bridge.engines import EngineInstantiator
from rivet_config import EngineConfig, ResolvedProfile
from rivet_core import ComputeEngine, ComputeEnginePlugin, PluginRegistry


class MockEnginePlugin(ComputeEnginePlugin):
    engine_type = "mock_engine"
    supported_catalog_types: dict[str, list[str]] = {}

    def validate(self, options: dict[str, Any]) -> None:
        if options.get("invalid"):
            raise ValueError("invalid options")

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type=self.engine_type)

    def execute_sql(self, engine, sql, input_tables):
        raise NotImplementedError


class FailingEnginePlugin(ComputeEnginePlugin):
    engine_type = "failing_engine"
    supported_catalog_types: dict[str, list[str]] = {}

    def validate(self, options: dict[str, Any]) -> None:
        raise ValueError("always fails")

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        raise RuntimeError("should not be called")

    def execute_sql(self, engine, sql, input_tables):
        raise NotImplementedError


def _profile(engines: list[EngineConfig] | None = None) -> ResolvedProfile:
    return ResolvedProfile(name="test", default_engine="arrow", catalogs={}, engines=engines or [])


def _registry(*plugins: ComputeEnginePlugin) -> PluginRegistry:
    reg = PluginRegistry()
    for p in plugins:
        reg.register_engine_plugin(p)
    return reg


class TestEngineInstantiator:
    def setup_method(self) -> None:
        self.inst = EngineInstantiator()

    def test_empty_profile(self) -> None:
        engines, errors = self.inst.instantiate_all(_profile(), _registry())
        assert engines == {}
        assert errors == []

    def test_single_engine_success(self) -> None:
        profile = _profile([EngineConfig(name="e1", type="mock_engine", catalogs=[], options={"k": "v"})])
        registry = _registry(MockEnginePlugin())
        engines, errors = self.inst.instantiate_all(profile, registry)
        assert len(engines) == 1
        assert engines["e1"].name == "e1"
        assert engines["e1"].engine_type == "mock_engine"
        assert errors == []

    def test_multiple_engines_success(self) -> None:
        profile = _profile([
            EngineConfig(name="a", type="mock_engine", catalogs=[], options={}),
            EngineConfig(name="b", type="mock_engine", catalogs=[], options={}),
        ])
        registry = _registry(MockEnginePlugin())
        engines, errors = self.inst.instantiate_all(profile, registry)
        assert set(engines.keys()) == {"a", "b"}
        assert errors == []

    def test_unknown_type_produces_brg203(self) -> None:
        profile = _profile([EngineConfig(name="bad", type="nonexistent", catalogs=[], options={})])
        engines, errors = self.inst.instantiate_all(profile, _registry())
        assert engines == {}
        assert len(errors) == 1
        assert errors[0].code == "BRG-203"
        assert "nonexistent" in errors[0].message

    def test_validation_failure_produces_brg204(self) -> None:
        profile = _profile([EngineConfig(name="fail", type="failing_engine", catalogs=[], options={})])
        registry = _registry(FailingEnginePlugin())
        engines, errors = self.inst.instantiate_all(profile, registry)
        assert engines == {}
        assert len(errors) == 1
        assert errors[0].code == "BRG-204"

    def test_validation_failure_via_options(self) -> None:
        profile = _profile([EngineConfig(name="inv", type="mock_engine", catalogs=[], options={"invalid": True})])
        registry = _registry(MockEnginePlugin())
        engines, errors = self.inst.instantiate_all(profile, registry)
        assert engines == {}
        assert len(errors) == 1
        assert errors[0].code == "BRG-204"

    def test_collects_multiple_errors(self) -> None:
        profile = _profile([
            EngineConfig(name="unknown", type="nope", catalogs=[], options={}),
            EngineConfig(name="bad_opts", type="failing_engine", catalogs=[], options={}),
        ])
        registry = _registry(FailingEnginePlugin())
        engines, errors = self.inst.instantiate_all(profile, registry)
        assert engines == {}
        assert len(errors) == 2
        codes = {e.code for e in errors}
        assert "BRG-203" in codes
        assert "BRG-204" in codes

    def test_partial_success(self) -> None:
        profile = _profile([
            EngineConfig(name="good", type="mock_engine", catalogs=[], options={}),
            EngineConfig(name="bad", type="nonexistent", catalogs=[], options={}),
        ])
        registry = _registry(MockEnginePlugin())
        engines, errors = self.inst.instantiate_all(profile, registry)
        assert len(engines) == 1
        assert "good" in engines
        assert len(errors) == 1
        assert errors[0].code == "BRG-203"

    def test_engine_registered_in_registry(self) -> None:
        """Requirement 2.7: register_compute_engine is called."""
        profile = _profile([EngineConfig(name="e1", type="mock_engine", catalogs=[], options={})])
        registry = _registry(MockEnginePlugin())
        self.inst.instantiate_all(profile, registry)
        assert registry.get_compute_engine("e1") is not None

    def test_error_has_remediation(self) -> None:
        profile = _profile([EngineConfig(name="x", type="missing", catalogs=[], options={})])
        _, errors = self.inst.instantiate_all(profile, _registry())
        assert errors[0].remediation is not None
