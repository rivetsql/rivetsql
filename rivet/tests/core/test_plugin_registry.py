"""Unit tests for PluginRegistry (task 6.4).

Covers:
- Registration order (built-ins first)
- Uniqueness enforcement and fail-fast errors
- Adapter priority resolution (catalog_plugin > engine_plugin)
- Entry point discovery mocking
"""

from __future__ import annotations

import warnings
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from rivet_core.models import Catalog, ComputeEngine
from rivet_core.optimizer import EMPTY_RESIDUAL, AdapterPushdownResult
from rivet_core.plugins import (
    CatalogPlugin,
    ComputeEngineAdapter,
    ComputeEnginePlugin,
    PluginRegistrationError,
    PluginRegistry,
    SinkPlugin,
    SourcePlugin,
)

# ── Minimal concrete implementations ──────────────────────────────────────────


class _CatalogPlugin(CatalogPlugin):
    def __init__(self, catalog_type: str = "test_catalog") -> None:
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
    def __init__(
        self,
        engine_type: str = "test_engine",
        supported: dict[str, list[str]] | None = None,
    ) -> None:
        self.engine_type = engine_type
        self.supported_catalog_types: dict[str, list[str]] = supported or {}

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type=self.engine_type)

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def execute_sql(self, engine, sql, input_tables):
        raise NotImplementedError


class _Adapter(ComputeEngineAdapter):
    def __init__(
        self,
        engine_type: str = "test_engine",
        catalog_type: str = "test_catalog",
        capabilities: list[str] | None = None,
        source: str = "engine_plugin",
        source_plugin: str | None = None,
    ) -> None:
        self.target_engine_type = engine_type
        self.catalog_type = catalog_type
        self.capabilities = capabilities or ["read"]
        self.source = source
        self.source_plugin = source_plugin

    def read_dispatch(self, engine: Any, catalog: Any, joint: Any, pushdown: Any = None) -> AdapterPushdownResult:
        return AdapterPushdownResult(material=None, residual=EMPTY_RESIDUAL)

    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        return None


class _SourcePlugin(SourcePlugin):
    def __init__(self, catalog_type: str = "test_catalog") -> None:
        self.catalog_type = catalog_type

    def read(self, catalog: Any, joint: Any, pushdown: Any) -> Any:
        return None


class _SinkPlugin(SinkPlugin):
    def __init__(self, catalog_type: str = "test_catalog") -> None:
        self.catalog_type = catalog_type

    def write(self, catalog: Any, joint: Any, material: Any, strategy: str) -> None:
        pass


# ── Tests ──────────────────────────────────────────────────────────────────────


class TestRegistration:
    def test_register_catalog_plugin(self) -> None:
        reg = PluginRegistry()
        plugin = _CatalogPlugin("arrow")
        reg.register_catalog_plugin(plugin)
        assert reg.get_catalog_plugin("arrow") is plugin

    def test_register_engine_plugin(self) -> None:
        reg = PluginRegistry()
        plugin = _EnginePlugin("duckdb")
        reg.register_engine_plugin(plugin)
        assert reg.get_engine_plugin("duckdb") is plugin

    def test_register_compute_engine_requires_engine_plugin(self) -> None:
        reg = PluginRegistry()
        engine = ComputeEngine(name="my_engine", engine_type="duckdb")
        with pytest.raises(PluginRegistrationError, match="No engine plugin registered"):
            reg.register_compute_engine(engine)

    def test_register_compute_engine_success(self) -> None:
        reg = PluginRegistry()
        reg.register_engine_plugin(_EnginePlugin("duckdb"))
        engine = ComputeEngine(name="my_engine", engine_type="duckdb")
        reg.register_compute_engine(engine)
        assert reg.get_compute_engine("my_engine") is engine

    def test_register_source(self) -> None:
        reg = PluginRegistry()
        src = _SourcePlugin("arrow")
        reg.register_source(src)

    def test_register_sink(self) -> None:
        reg = PluginRegistry()
        sink = _SinkPlugin("arrow")
        reg.register_sink(sink)

    def test_source_without_catalog_type_raises(self) -> None:
        reg = PluginRegistry()

        class _BadSource(SourcePlugin):
            def read(self, catalog: Any, joint: Any, pushdown: Any) -> Any:
                return None

        with pytest.raises(PluginRegistrationError, match="catalog_type"):
            reg.register_source(_BadSource())

    def test_sink_without_catalog_type_raises(self) -> None:
        reg = PluginRegistry()

        class _BadSink(SinkPlugin):
            def write(self, catalog: Any, joint: Any, material: Any, strategy: str) -> None:
                pass

        with pytest.raises(PluginRegistrationError, match="catalog_type"):
            reg.register_sink(_BadSink())


class TestUniquenessEnforcement:
    def test_duplicate_catalog_plugin_raises(self) -> None:
        reg = PluginRegistry()
        reg.register_catalog_plugin(_CatalogPlugin("arrow"))
        with pytest.raises(PluginRegistrationError, match="already registered"):
            reg.register_catalog_plugin(_CatalogPlugin("arrow"))

    def test_duplicate_engine_plugin_raises(self) -> None:
        reg = PluginRegistry()
        reg.register_engine_plugin(_EnginePlugin("duckdb"))
        with pytest.raises(PluginRegistrationError, match="already registered"):
            reg.register_engine_plugin(_EnginePlugin("duckdb"))

    def test_duplicate_catalog_plugin_error_names_type(self) -> None:
        """Error message must identify the conflicting catalog_type."""
        reg = PluginRegistry()
        reg.register_catalog_plugin(_CatalogPlugin("my_catalog"))
        with pytest.raises(PluginRegistrationError) as exc_info:
            reg.register_catalog_plugin(_CatalogPlugin("my_catalog"))
        assert "my_catalog" in str(exc_info.value)

    def test_duplicate_catalog_plugin_error_names_existing_class(self) -> None:
        """Error message must name the already-registered plugin class."""
        reg = PluginRegistry()
        reg.register_catalog_plugin(_CatalogPlugin("my_catalog"))
        with pytest.raises(PluginRegistrationError) as exc_info:
            reg.register_catalog_plugin(_CatalogPlugin("my_catalog"))
        assert "_CatalogPlugin" in str(exc_info.value)

    def test_duplicate_catalog_plugin_error_includes_remediation(self) -> None:
        """Error message must include a remediation hint."""
        reg = PluginRegistry()
        reg.register_catalog_plugin(_CatalogPlugin("my_catalog"))
        with pytest.raises(PluginRegistrationError) as exc_info:
            reg.register_catalog_plugin(_CatalogPlugin("my_catalog"))
        msg = str(exc_info.value)
        # Should mention checking for duplicate packages or uninstalling
        assert any(word in msg.lower() for word in ("uninstall", "duplicate", "conflict", "package"))

    def test_duplicate_engine_plugin_error_names_type(self) -> None:
        """Error message must identify the conflicting engine_type."""
        reg = PluginRegistry()
        reg.register_engine_plugin(_EnginePlugin("my_engine"))
        with pytest.raises(PluginRegistrationError) as exc_info:
            reg.register_engine_plugin(_EnginePlugin("my_engine"))
        assert "my_engine" in str(exc_info.value)

    def test_duplicate_engine_plugin_error_names_existing_class(self) -> None:
        """Error message must name the already-registered engine plugin class."""
        reg = PluginRegistry()
        reg.register_engine_plugin(_EnginePlugin("my_engine"))
        with pytest.raises(PluginRegistrationError) as exc_info:
            reg.register_engine_plugin(_EnginePlugin("my_engine"))
        assert "_EnginePlugin" in str(exc_info.value)

    def test_duplicate_engine_plugin_error_includes_remediation(self) -> None:
        """Error message must include a remediation hint."""
        reg = PluginRegistry()
        reg.register_engine_plugin(_EnginePlugin("my_engine"))
        with pytest.raises(PluginRegistrationError) as exc_info:
            reg.register_engine_plugin(_EnginePlugin("my_engine"))
        msg = str(exc_info.value)
        assert any(word in msg.lower() for word in ("uninstall", "duplicate", "conflict", "package"))

    def test_duplicate_compute_engine_instance_raises(self) -> None:
        reg = PluginRegistry()
        reg.register_engine_plugin(_EnginePlugin("duckdb"))
        engine = ComputeEngine(name="my_engine", engine_type="duckdb")
        reg.register_compute_engine(engine)
        with pytest.raises(PluginRegistrationError, match="already registered"):
            reg.register_compute_engine(ComputeEngine(name="my_engine", engine_type="duckdb"))

    def test_duplicate_source_raises(self) -> None:
        reg = PluginRegistry()
        reg.register_source(_SourcePlugin("arrow"))
        with pytest.raises(PluginRegistrationError, match="already registered"):
            reg.register_source(_SourcePlugin("arrow"))

    def test_duplicate_sink_raises(self) -> None:
        reg = PluginRegistry()
        reg.register_sink(_SinkPlugin("arrow"))
        with pytest.raises(PluginRegistrationError, match="already registered"):
            reg.register_sink(_SinkPlugin("arrow"))


class TestComputeEngineTypeIndex:
    def test_type_to_instances_index(self) -> None:
        reg = PluginRegistry()
        reg.register_engine_plugin(_EnginePlugin("duckdb"))
        e1 = ComputeEngine(name="duck1", engine_type="duckdb")
        e2 = ComputeEngine(name="duck2", engine_type="duckdb")
        reg.register_compute_engine(e1)
        reg.register_compute_engine(e2)
        assert set(reg._compute_engine_types["duckdb"]) == {"duck1", "duck2"}


class TestAdapterPriority:
    def test_catalog_plugin_adapter_overrides_engine_plugin_adapter(self) -> None:
        reg = PluginRegistry()
        engine_adapter = _Adapter("eng", "cat", source="engine_plugin")
        catalog_adapter = _Adapter("eng", "cat", source="catalog_plugin")

        reg.register_adapter(engine_adapter)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            reg.register_adapter(catalog_adapter)
        assert reg.get_adapter("eng", "cat") is catalog_adapter
        assert any("catalog_plugin" in str(warning.message) for warning in w)

    def test_engine_plugin_adapter_ignored_when_catalog_plugin_exists(self) -> None:
        reg = PluginRegistry()
        catalog_adapter = _Adapter("eng", "cat", source="catalog_plugin")
        engine_adapter = _Adapter("eng", "cat", source="engine_plugin")

        reg.register_adapter(catalog_adapter)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            reg.register_adapter(engine_adapter)
        # catalog_plugin adapter must remain
        assert reg.get_adapter("eng", "cat") is catalog_adapter
        assert any("ignored" in str(warning.message) for warning in w)

    def test_same_source_conflict_raises(self) -> None:
        reg = PluginRegistry()
        a1 = _Adapter("eng", "cat", source="engine_plugin")
        a2 = _Adapter("eng", "cat", source="engine_plugin")

        reg.register_adapter(a1)
        with pytest.raises(PluginRegistrationError, match="conflict"):
            reg.register_adapter(a2)

    def test_same_source_conflict_error_names_engine_and_catalog_type(self) -> None:
        reg = PluginRegistry()
        a1 = _Adapter("eng", "cat", source="engine_plugin")
        a2 = _Adapter("eng", "cat", source="engine_plugin")

        reg.register_adapter(a1)
        with pytest.raises(PluginRegistrationError) as exc_info:
            reg.register_adapter(a2)
        msg = str(exc_info.value)
        assert "eng" in msg and "cat" in msg

    def test_same_source_conflict_error_includes_remediation(self) -> None:
        reg = PluginRegistry()
        a1 = _Adapter("eng", "cat", source="catalog_plugin")
        a2 = _Adapter("eng", "cat", source="catalog_plugin")

        reg.register_adapter(a1)
        with pytest.raises(PluginRegistrationError) as exc_info:
            reg.register_adapter(a2)
        msg = str(exc_info.value).lower()
        assert any(word in msg for word in ("uninstall", "duplicate", "conflict", "package"))

    def test_first_adapter_registered_without_conflict(self) -> None:
        reg = PluginRegistry()
        adapter = _Adapter("eng", "cat", source="engine_plugin")
        reg.register_adapter(adapter)
        assert reg.get_adapter("eng", "cat") is adapter

    def test_source_plugin_field_stored_on_adapter(self) -> None:
        """source_plugin field identifies the contributing plugin package."""
        adapter = _Adapter("eng", "cat", source="catalog_plugin", source_plugin="rivet_postgres")
        assert adapter.source_plugin == "rivet_postgres"

    def test_source_plugin_defaults_to_none(self) -> None:
        """source_plugin is optional; defaults to None when not set."""
        adapter = _Adapter("eng", "cat", source="engine_plugin")
        assert adapter.source_plugin is None

    def test_source_plugin_accessible_after_registration(self) -> None:
        """source_plugin is preserved through registry registration."""
        reg = PluginRegistry()
        adapter = _Adapter("eng", "cat", source="catalog_plugin", source_plugin="rivet_postgres")
        reg.register_adapter(adapter)
        registered = reg.get_adapter("eng", "cat")
        assert registered is not None
        assert registered.source_plugin == "rivet_postgres"

    def test_resolve_capabilities_uses_catalog_plugin_adapter_after_override(self) -> None:
        """After catalog_plugin overrides engine_plugin, resolve_capabilities returns catalog_plugin's capabilities."""
        reg = PluginRegistry()
        engine_adapter = _Adapter("duckdb", "postgres", capabilities=["read"], source="engine_plugin")
        catalog_adapter = _Adapter(
            "duckdb", "postgres",
            capabilities=["read", "write", "cast_pushdown"],
            source="catalog_plugin",
            source_plugin="rivet_postgres",
        )
        reg.register_adapter(engine_adapter)
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            reg.register_adapter(catalog_adapter)
        assert reg.resolve_capabilities("duckdb", "postgres") == ["read", "write", "cast_pushdown"]

    def test_precedence_requires_no_user_configuration(self) -> None:
        """Precedence is automatic — no extra calls or config needed beyond register_adapter."""
        reg = PluginRegistry()
        reg.register_adapter(_Adapter("eng", "cat", capabilities=["a"], source="engine_plugin"))
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            reg.register_adapter(_Adapter("eng", "cat", capabilities=["a", "b"], source="catalog_plugin"))
        # Just registering is enough; get_adapter returns the winner
        result = reg.get_adapter("eng", "cat")
        assert result is not None
        assert result.source == "catalog_plugin"


class TestLookup:
    def test_get_missing_catalog_plugin_returns_none(self) -> None:
        assert PluginRegistry().get_catalog_plugin("nonexistent") is None

    def test_get_missing_engine_plugin_returns_none(self) -> None:
        assert PluginRegistry().get_engine_plugin("nonexistent") is None

    def test_get_missing_compute_engine_returns_none(self) -> None:
        assert PluginRegistry().get_compute_engine("nonexistent") is None

    def test_get_missing_adapter_returns_none(self) -> None:
        assert PluginRegistry().get_adapter("eng", "cat") is None

    def test_resolve_capabilities_from_adapter(self) -> None:
        reg = PluginRegistry()
        adapter = _Adapter("eng", "cat", capabilities=["read", "write"])
        reg.register_adapter(adapter)
        assert reg.resolve_capabilities("eng", "cat") == ["read", "write"]

    def test_resolve_capabilities_from_engine_plugin(self) -> None:
        reg = PluginRegistry()
        reg.register_engine_plugin(_EnginePlugin("eng", supported={"cat": ["read"]}))
        assert reg.resolve_capabilities("eng", "cat") == ["read"]

    def test_resolve_capabilities_adapter_takes_precedence_over_engine_plugin(self) -> None:
        reg = PluginRegistry()
        reg.register_engine_plugin(_EnginePlugin("eng", supported={"cat": ["read"]}))
        adapter = _Adapter("eng", "cat", capabilities=["read", "write"])
        reg.register_adapter(adapter)
        assert reg.resolve_capabilities("eng", "cat") == ["read", "write"]

    def test_resolve_capabilities_returns_none_when_unknown(self) -> None:
        assert PluginRegistry().resolve_capabilities("eng", "cat") is None


class TestEntryPointDiscovery:
    """Tests for granular entry point discovery across five groups."""

    _GROUPS = [
        "rivet.catalogs",
        "rivet.compute_engines",
        "rivet.compute_engine_adapters",
        "rivet.sources",
        "rivet.sinks",
    ]

    def _make_ep(self, name: str, load_return: Any) -> MagicMock:
        ep = MagicMock()
        ep.name = name
        ep.load.return_value = load_return
        return ep

    def _mock_entry_points(self, group_eps: dict[str, list[MagicMock]]):
        """Return a side_effect function that dispatches by group kwarg."""
        def _side_effect(*, group: str):
            return group_eps.get(group, [])
        return patch("rivet_core.plugins.entry_points", side_effect=_side_effect)

    def test_discover_plugin_via_granular_groups(self) -> None:
        """discover_plugins queries granular groups and registers plugins."""
        reg = PluginRegistry()
        ep = self._make_ep("ext_cat", _CatalogPlugin)

        with self._mock_entry_points({"rivet.catalogs": [ep]}):
            reg.discover_plugins()

        assert reg.get_catalog_plugin("test_catalog") is not None

    def test_discover_plugins_queries_all_five_groups(self) -> None:
        """discover_plugins must query all five granular entry point groups (plus rivet.plugins)."""
        reg = PluginRegistry()
        queried_groups: list[str] = []

        def _side_effect(*, group: str):
            queried_groups.append(group)
            return []

        with patch("rivet_core.plugins.entry_points", side_effect=_side_effect):
            reg.discover_plugins()

        # Monolithic group is queried first, then all five granular groups
        for g in self._GROUPS:
            assert g in queried_groups, f"Expected group {g!r} to be queried"

    def test_discover_catalogs_group(self) -> None:
        reg = PluginRegistry()
        ep = self._make_ep("my_cat", _CatalogPlugin)
        with self._mock_entry_points({"rivet.catalogs": [ep]}):
            reg.discover_plugins()
        assert reg.get_catalog_plugin("test_catalog") is not None

    def test_discover_compute_engines_group(self) -> None:
        reg = PluginRegistry()
        ep = self._make_ep("my_eng", _EnginePlugin)
        with self._mock_entry_points({"rivet.compute_engines": [ep]}):
            reg.discover_plugins()
        assert reg.get_engine_plugin("test_engine") is not None

    def test_discover_compute_engine_adapters_group(self) -> None:
        reg = PluginRegistry()
        # Need engine plugin registered first for adapter to work
        reg.register_engine_plugin(_EnginePlugin())
        ep = self._make_ep("my_adapter", _Adapter)
        with self._mock_entry_points({"rivet.compute_engine_adapters": [ep]}):
            reg.discover_plugins()
        assert reg.get_adapter("test_engine", "test_catalog") is not None

    def test_discover_sources_group(self) -> None:
        reg = PluginRegistry()
        ep = self._make_ep("my_src", _SourcePlugin)
        with self._mock_entry_points({"rivet.sources": [ep]}):
            reg.discover_plugins()
        # _SourcePlugin has catalog_type="test_catalog"
        assert reg._sources.get("test_catalog") is not None

    def test_discover_sinks_group(self) -> None:
        reg = PluginRegistry()
        ep = self._make_ep("my_sink", _SinkPlugin)
        with self._mock_entry_points({"rivet.sinks": [ep]}):
            reg.discover_plugins()
        assert reg._sinks.get("test_catalog") is not None

    def test_discover_plugins_alphabetical_order(self) -> None:
        """Entry points within a group are processed in alphabetical order."""
        reg = PluginRegistry()
        order: list[str] = []

        def make_catalog_cls(name: str, cat_type: str):
            class _Cat(_CatalogPlugin):
                def __init__(self) -> None:
                    super().__init__(cat_type)
                    order.append(name)
            return _Cat

        ep_z = self._make_ep("zulu", make_catalog_cls("zulu", "cat_z"))
        ep_a = self._make_ep("alpha", make_catalog_cls("alpha", "cat_a"))
        ep_m = self._make_ep("mike", make_catalog_cls("mike", "cat_m"))

        with self._mock_entry_points({"rivet.catalogs": [ep_z, ep_a, ep_m]}):
            reg.discover_plugins()

        assert order == ["alpha", "mike", "zulu"]

    def test_failed_entry_point_raises_with_name_and_group(self) -> None:
        reg = PluginRegistry()
        ep = MagicMock()
        ep.name = "bad_plugin"
        ep.load.side_effect = ImportError("missing dep")

        with self._mock_entry_points({"rivet.catalogs": [ep]}):
            with pytest.raises(PluginRegistrationError, match="bad_plugin") as exc_info:
                reg.discover_plugins()
        assert "rivet.catalogs" in str(exc_info.value)

    def test_failed_entry_point_error_includes_underlying_exception(self) -> None:
        """Error message must include the underlying exception for actionability."""
        reg = PluginRegistry()
        ep = MagicMock()
        ep.name = "bad_plugin"
        ep.load.side_effect = ImportError("no module named rivet_missing")

        with self._mock_entry_points({"rivet.catalogs": [ep]}):
            with pytest.raises(PluginRegistrationError) as exc_info:
                reg.discover_plugins()
        assert "rivet_missing" in str(exc_info.value)

    def test_failed_entry_point_error_is_chained(self) -> None:
        """PluginRegistrationError must chain the original exception as __cause__."""
        reg = PluginRegistry()
        ep = MagicMock()
        ep.name = "bad_plugin"
        original = ImportError("missing dep")
        ep.load.side_effect = original

        with self._mock_entry_points({"rivet.catalogs": [ep]}):
            with pytest.raises(PluginRegistrationError) as exc_info:
                reg.discover_plugins()
        assert exc_info.value.__cause__ is original

    def test_missing_class_raises_actionable_error(self) -> None:
        """AttributeError (missing class) during load() produces actionable error with ep name."""
        reg = PluginRegistry()
        ep = MagicMock()
        ep.name = "my_plugin"
        ep.load.side_effect = AttributeError("module 'rivet_foo' has no attribute 'FooPlugin'")

        with self._mock_entry_points({"rivet.sources": [ep]}):
            with pytest.raises(PluginRegistrationError) as exc_info:
                reg.discover_plugins()
        msg = str(exc_info.value)
        assert "my_plugin" in msg
        assert "FooPlugin" in msg

    def test_non_callable_loaded_object_raises_actionable_error(self) -> None:
        """If ep.load() returns a non-callable, calling it raises TypeError → actionable error."""
        reg = PluginRegistry()
        ep = MagicMock()
        ep.name = "broken_plugin"
        ep.load.return_value = "not_a_callable"  # string is not callable

        with self._mock_entry_points({"rivet.sinks": [ep]}):
            with pytest.raises(PluginRegistrationError) as exc_info:
                reg.discover_plugins()
        assert "broken_plugin" in str(exc_info.value)

    def test_failed_entry_point_does_not_swallow_other_plugins(self) -> None:
        """A failing entry point raises immediately; subsequent plugins are not processed."""
        reg = PluginRegistry()
        order: list[str] = []

        def make_catalog_cls(name: str, cat_type: str):
            class _Cat(_CatalogPlugin):
                def __init__(self) -> None:
                    super().__init__(cat_type)
                    order.append(name)
            return _Cat

        ep_bad = MagicMock()
        ep_bad.name = "aaa_bad"
        ep_bad.load.side_effect = ImportError("boom")

        ep_ok = self._make_ep("zzz_ok", make_catalog_cls("ok", "cat_ok"))

        with self._mock_entry_points({"rivet.catalogs": [ep_bad, ep_ok]}):
            with pytest.raises(PluginRegistrationError, match="aaa_bad"):
                reg.discover_plugins()
        assert "ok" not in order

    def test_builtins_registered_before_external_plugins(self) -> None:
        reg = PluginRegistry()
        sentinel_plugin = _CatalogPlugin("builtin_sentinel")

        def fake_register_builtins() -> None:
            PluginRegistry.register_catalog_plugin(reg, sentinel_plugin)

        class _ExtCat(_CatalogPlugin):
            def __init__(self) -> None:
                super().__init__("ext_cat")

        ep = self._make_ep("ext", _ExtCat)

        with patch.object(reg, "register_builtins", side_effect=fake_register_builtins):
            with self._mock_entry_points({"rivet.catalogs": [ep]}):
                reg.register_builtins()
                assert reg.get_catalog_plugin("builtin_sentinel") is not None
                assert reg.get_catalog_plugin("ext_cat") is None

                reg.discover_plugins()
                assert reg.get_catalog_plugin("ext_cat") is not None
