"""Integration tests: config loader + profile resolver + plugin registry.

Loads a real profiles.yaml, resolves profiles, and verifies the plugin
registry contains the expected engine and catalog instances. Uses real
config loader, profile resolver, and plugin registry — no mocks.
"""

from __future__ import annotations

from pathlib import Path

from rivet_config import load_config
from rivet_config.profiles import ProfileResolver
from rivet_core.plugins import PluginRegistry
from rivet_duckdb import DuckDBPlugin

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_RIVET_YAML = """\
profiles: profiles.yaml
sources: sources
joints: joints
sinks: sinks
tests: tests
quality: quality
"""

_PROFILES_YAML = """\
default:
  catalogs:
    local:
      type: filesystem
      path: ./data
      format: csv
  engines:
    - name: duckdb_primary
      type: duckdb
      catalogs: [local]
    - name: duckdb_secondary
      type: duckdb
      catalogs: [local]
  default_engine: duckdb_primary
"""

_PROFILES_MULTI_CATALOG = """\
default:
  catalogs:
    local:
      type: filesystem
      path: ./data
      format: csv
    warehouse:
      type: duckdb
      path: ./warehouse.duckdb
  engines:
    - name: duckdb_primary
      type: duckdb
      catalogs: [local, warehouse]
  default_engine: duckdb_primary
"""


def _scaffold_project(tmp_path: Path, profiles_yaml: str = _PROFILES_YAML) -> Path:
    """Create a minimal Rivet project scaffold."""
    (tmp_path / "rivet.yaml").write_text(_RIVET_YAML)
    (tmp_path / "profiles.yaml").write_text(profiles_yaml)
    for d in ("sources", "joints", "sinks", "tests", "quality", "data"):
        (tmp_path / d).mkdir(exist_ok=True)
    return tmp_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestConfigLoading:
    """load_config parses manifest and resolves profile correctly."""

    def test_load_config_succeeds(self, tmp_path):
        project = _scaffold_project(tmp_path)
        result = load_config(project, strict=False)

        assert result.success
        assert result.manifest is not None
        assert result.profile is not None

    def test_profile_has_expected_engines(self, tmp_path):
        project = _scaffold_project(tmp_path)
        result = load_config(project, strict=False)

        assert result.success
        profile = result.profile
        engine_names = [e.name for e in profile.engines]
        assert "duckdb_primary" in engine_names
        assert "duckdb_secondary" in engine_names

    def test_profile_has_expected_catalogs(self, tmp_path):
        project = _scaffold_project(tmp_path)
        result = load_config(project, strict=False)

        assert result.success
        profile = result.profile
        assert "local" in profile.catalogs
        assert profile.catalogs["local"].type == "filesystem"

    def test_profile_default_engine(self, tmp_path):
        project = _scaffold_project(tmp_path)
        result = load_config(project, strict=False)

        assert result.success
        assert result.profile.default_engine == "duckdb_primary"


class TestProfileResolution:
    """ProfileResolver resolves profiles from YAML files."""

    def test_resolve_default_profile(self, tmp_path):
        _scaffold_project(tmp_path)
        resolver = ProfileResolver()
        profile, errors, warnings = resolver.resolve(
            tmp_path / "profiles.yaml", None, tmp_path, strict=False,
        )

        assert len(errors) == 0
        assert profile is not None
        assert profile.name == "default"

    def test_resolve_multi_catalog_profile(self, tmp_path):
        _scaffold_project(tmp_path, _PROFILES_MULTI_CATALOG)
        resolver = ProfileResolver()
        profile, errors, warnings = resolver.resolve(
            tmp_path / "profiles.yaml", None, tmp_path, strict=False,
        )

        assert len(errors) == 0
        assert profile is not None
        assert "local" in profile.catalogs
        assert "warehouse" in profile.catalogs
        assert profile.catalogs["local"].type == "filesystem"
        assert profile.catalogs["warehouse"].type == "duckdb"


class TestProfileToRegistry:
    """Profile resolution feeds into plugin registry correctly."""

    def test_engines_register_from_profile(self, tmp_path):
        project = _scaffold_project(tmp_path)
        result = load_config(project, strict=False)
        assert result.success

        registry = PluginRegistry()
        registry.register_builtins()
        DuckDBPlugin(registry)

        # Register engines from profile
        profile = result.profile
        eng_plugin = registry.get_engine_plugin("duckdb")
        for eng_config in profile.engines:
            engine = eng_plugin.create_engine(eng_config.name, {})
            registry.register_compute_engine(engine)

        # Verify engines are in the registry
        assert registry.get_compute_engine("duckdb_primary") is not None
        assert registry.get_compute_engine("duckdb_secondary") is not None

    def test_catalog_plugins_available_for_profile_types(self, tmp_path):
        project = _scaffold_project(tmp_path, _PROFILES_MULTI_CATALOG)
        result = load_config(project, strict=False)
        assert result.success

        registry = PluginRegistry()
        registry.register_builtins()
        DuckDBPlugin(registry)

        # Verify catalog plugins exist for all catalog types in the profile
        for _cat_name, cat_config in result.profile.catalogs.items():
            plugin = registry.get_catalog_plugin(cat_config.type)
            assert plugin is not None, f"No catalog plugin for type '{cat_config.type}'"


class TestMissingConfig:
    """Config loading handles missing files gracefully."""

    def test_missing_rivet_yaml_produces_error(self, tmp_path):
        result = load_config(tmp_path, strict=False)
        assert not result.success
        assert len(result.errors) > 0

    def test_missing_profiles_yaml_produces_error(self, tmp_path):
        (tmp_path / "rivet.yaml").write_text(_RIVET_YAML)
        for d in ("sources", "joints", "sinks", "tests", "quality"):
            (tmp_path / d).mkdir(exist_ok=True)
        result = load_config(tmp_path, strict=False)
        assert not result.success
