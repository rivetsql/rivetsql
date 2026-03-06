"""Tests for filter_compatible_engines and update_engine_catalogs — task 2.2."""
from __future__ import annotations

from rivet_cli.commands.catalog_create import filter_compatible_engines, update_engine_catalogs
from rivet_config.models import EngineConfig, ResolvedProfile
from rivet_core.plugins import ComputeEngineAdapter, ComputeEnginePlugin, PluginRegistry


def _make_adapter(engine_type: str, catalog_type: str) -> ComputeEngineAdapter:
    A = type(
        "A",
        (ComputeEngineAdapter,),
        {
            "target_engine_type": engine_type,
            "catalog_type": catalog_type,
            "capabilities": ["read"],
            "source": "engine_plugin",
            "read_dispatch": lambda self, e, c, j: None,
            "write_dispatch": lambda self, e, c, j, m: None,
        },
    )
    return A()


def _make_engine_plugin(etype: str, supported: dict) -> ComputeEnginePlugin:
    class P(ComputeEnginePlugin):
        engine_type = etype
        supported_catalog_types = supported

        def create_engine(self, n, c): ...
        def validate(self, o): ...
        def execute_sql(self, engine, sql, input_tables):
            raise NotImplementedError

    return P()


# --- update_engine_catalogs ---


def test_update_appends_new_catalog() -> None:
    assert update_engine_catalogs(["a"], "b") == ["a", "b"]


def test_update_idempotent() -> None:
    assert update_engine_catalogs(["a", "b"], "a") == ["a", "b"]


def test_update_empty_list() -> None:
    assert update_engine_catalogs([], "x") == ["x"]


# --- filter_compatible_engines ---


def test_filter_via_adapter() -> None:
    registry = PluginRegistry()
    registry.register_adapter(_make_adapter("duckdb", "postgres"))
    profile = ResolvedProfile(
        "default", "e1", {},
        [EngineConfig("e1", "duckdb", [], {}), EngineConfig("e2", "spark", [], {})],
    )
    result = filter_compatible_engines(profile, "postgres", registry)
    assert [e.name for e in result] == ["e1"]


def test_filter_via_supported_catalog_types() -> None:
    registry = PluginRegistry()
    registry.register_engine_plugin(_make_engine_plugin("polars", {"s3": ["read"]}))
    profile = ResolvedProfile(
        "default", "e1", {},
        [EngineConfig("e1", "polars", [], {}), EngineConfig("e2", "duckdb", [], {})],
    )
    result = filter_compatible_engines(profile, "s3", registry)
    assert [e.name for e in result] == ["e1"]


def test_filter_no_match() -> None:
    registry = PluginRegistry()
    profile = ResolvedProfile("default", "e1", {}, [EngineConfig("e1", "duckdb", [], {})])
    assert filter_compatible_engines(profile, "glue", registry) == []


def test_filter_empty_engines() -> None:
    registry = PluginRegistry()
    profile = ResolvedProfile("default", "e1", {}, [])
    assert filter_compatible_engines(profile, "postgres", registry) == []


def test_filter_both_adapter_and_plugin_match() -> None:
    """Engine with both adapter and plugin support is included once."""
    registry = PluginRegistry()
    registry.register_adapter(_make_adapter("duckdb", "postgres"))
    registry.register_engine_plugin(_make_engine_plugin("duckdb", {"postgres": ["read"]}))
    profile = ResolvedProfile("default", "e1", {}, [EngineConfig("e1", "duckdb", [], {})])
    result = filter_compatible_engines(profile, "postgres", registry)
    assert len(result) == 1
    assert result[0].name == "e1"
