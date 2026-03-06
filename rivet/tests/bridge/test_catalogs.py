"""Tests for CatalogInstantiator."""

from __future__ import annotations

from rivet_bridge.catalogs import CatalogInstantiator
from rivet_config import CatalogConfig
from tests.bridge.conftest import (
    FailingCatalogPlugin,
    MockCatalogPlugin,
    make_profile,
    make_registry,
)


class TestCatalogInstantiator:
    def setup_method(self) -> None:
        self.instantiator = CatalogInstantiator()

    def test_empty_profile(self) -> None:
        profile = make_profile()
        registry = make_registry()
        catalogs, errors = self.instantiator.instantiate_all(profile, registry)
        assert catalogs == {}
        assert errors == []

    def test_single_catalog_success(self) -> None:
        profile = make_profile(catalogs={
            "my_cat": CatalogConfig(name="my_cat", type="mock", options={"key": "val"}),
        })
        registry = make_registry(MockCatalogPlugin())
        catalogs, errors = self.instantiator.instantiate_all(profile, registry)
        assert len(catalogs) == 1
        assert "my_cat" in catalogs
        assert catalogs["my_cat"].name == "my_cat"
        assert catalogs["my_cat"].type == "mock"
        assert catalogs["my_cat"].options == {"key": "val"}
        assert errors == []

    def test_multiple_catalogs_success(self) -> None:
        profile = make_profile(catalogs={
            "a": CatalogConfig(name="a", type="mock", options={}),
            "b": CatalogConfig(name="b", type="mock", options={}),
        })
        registry = make_registry(MockCatalogPlugin())
        catalogs, errors = self.instantiator.instantiate_all(profile, registry)
        assert len(catalogs) == 2
        assert set(catalogs.keys()) == {"a", "b"}
        assert errors == []

    def test_unknown_type_produces_brg201(self) -> None:
        profile = make_profile(catalogs={
            "bad": CatalogConfig(name="bad", type="nonexistent", options={}),
        })
        registry = make_registry()
        catalogs, errors = self.instantiator.instantiate_all(profile, registry)
        assert catalogs == {}
        assert len(errors) == 1
        assert errors[0].code == "BRG-201"
        assert "nonexistent" in errors[0].message
        assert errors[0].joint_name == "bad"

    def test_validation_failure_produces_brg202(self) -> None:
        profile = make_profile(catalogs={
            "fail": CatalogConfig(name="fail", type="failing", options={}),
        })
        registry = make_registry(FailingCatalogPlugin())
        catalogs, errors = self.instantiator.instantiate_all(profile, registry)
        assert catalogs == {}
        assert len(errors) == 1
        assert errors[0].code == "BRG-202"
        assert errors[0].joint_name == "fail"

    def test_validation_failure_via_options(self) -> None:
        profile = make_profile(catalogs={
            "inv": CatalogConfig(name="inv", type="mock", options={"invalid": True}),
        })
        registry = make_registry(MockCatalogPlugin())
        catalogs, errors = self.instantiator.instantiate_all(profile, registry)
        assert catalogs == {}
        assert len(errors) == 1
        assert errors[0].code == "BRG-202"

    def test_collects_multiple_errors(self) -> None:
        profile = make_profile(catalogs={
            "unknown": CatalogConfig(name="unknown", type="nope", options={}),
            "bad_opts": CatalogConfig(name="bad_opts", type="failing", options={}),
        })
        registry = make_registry(FailingCatalogPlugin())
        catalogs, errors = self.instantiator.instantiate_all(profile, registry)
        assert catalogs == {}
        assert len(errors) == 2
        codes = {e.code for e in errors}
        assert "BRG-201" in codes
        assert "BRG-202" in codes

    def test_partial_success(self) -> None:
        """One catalog succeeds, another fails — both results returned."""
        profile = make_profile(catalogs={
            "good": CatalogConfig(name="good", type="mock", options={}),
            "bad": CatalogConfig(name="bad", type="nonexistent", options={}),
        })
        registry = make_registry(MockCatalogPlugin())
        catalogs, errors = self.instantiator.instantiate_all(profile, registry)
        assert len(catalogs) == 1
        assert "good" in catalogs
        assert len(errors) == 1
        assert errors[0].code == "BRG-201"

    def test_error_has_remediation(self) -> None:
        profile = make_profile(catalogs={
            "x": CatalogConfig(name="x", type="missing", options={}),
        })
        registry = make_registry()
        _, errors = self.instantiator.instantiate_all(profile, registry)
        assert errors[0].remediation is not None
