"""Tests for ProfileResolver."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.models import ResolvedProfile
from rivet_config.profiles import ProfileResolver


@pytest.fixture
def resolver():
    return ProfileResolver()


# --- Single file loading ---


def test_load_single_file_profile(tmp_path, resolver):
    profiles = {
        "default": {
            "default_engine": "duckdb",
            "catalogs": {"warehouse": {"type": "duckdb"}},
            "engines": [{"name": "duckdb", "type": "duckdb", "catalogs": ["duckdb"]}],
        }
    }
    pf = tmp_path / "profiles.yaml"
    pf.write_text(yaml.dump(profiles))

    profile, errors, warnings = resolver.resolve(pf, None, tmp_path)
    assert not errors
    assert profile is not None
    assert profile.name == "default"
    assert profile.default_engine == "duckdb"
    assert "warehouse" in profile.catalogs
    assert profile.catalogs["warehouse"].type == "duckdb"
    assert len(profile.engines) == 1
    assert profile.engines[0].name == "duckdb"


# --- Directory loading ---


def test_load_directory_profiles(tmp_path, resolver):
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    data = {
        "default_engine": "spark",
        "catalogs": {"lake": {"type": "iceberg"}},
        "engines": [{"name": "spark", "type": "spark", "catalogs": ["iceberg"]}],
    }
    (profiles_dir / "dev.yaml").write_text(yaml.dump(data))

    profile, errors, warnings = resolver.resolve(profiles_dir, "dev", tmp_path)
    assert not errors
    assert profile is not None
    assert profile.name == "dev"
    assert profile.default_engine == "spark"


def test_directory_lexicographic_fallback(tmp_path, resolver):
    """When no profile_name and no 'default', pick first lexicographically."""
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    for name in ["beta", "alpha"]:
        data = {
            "default_engine": name,
            "catalogs": {"c": {"type": "t"}},
            "engines": [{"name": name, "type": "t", "catalogs": ["t"]}],
        }
        (profiles_dir / f"{name}.yaml").write_text(yaml.dump(data))

    profile, errors, warnings = resolver.resolve(profiles_dir, None, tmp_path)
    assert not errors
    assert profile is not None
    assert profile.name == "alpha"


# --- Profile selection ---


def test_select_default_profile(tmp_path, resolver):
    profiles = {
        "default": {
            "default_engine": "e",
            "catalogs": {"c": {"type": "t"}},
            "engines": [{"name": "e", "type": "t", "catalogs": ["t"]}],
        },
        "prod": {
            "default_engine": "e2",
            "catalogs": {"c": {"type": "t"}},
            "engines": [{"name": "e2", "type": "t", "catalogs": ["t"]}],
        },
    }
    pf = tmp_path / "profiles.yaml"
    pf.write_text(yaml.dump(profiles))

    profile, errors, _ = resolver.resolve(pf, None, tmp_path)
    assert not errors
    assert profile is not None
    assert profile.name == "default"


def test_select_explicit_profile(tmp_path, resolver):
    profiles = {
        "default": {
            "default_engine": "e",
            "catalogs": {"c": {"type": "t"}},
            "engines": [{"name": "e", "type": "t", "catalogs": ["t"]}],
        },
        "prod": {
            "default_engine": "e2",
            "catalogs": {"c2": {"type": "t2"}},
            "engines": [{"name": "e2", "type": "t2", "catalogs": ["t2"]}],
        },
    }
    pf = tmp_path / "profiles.yaml"
    pf.write_text(yaml.dump(profiles))

    profile, errors, _ = resolver.resolve(pf, "prod", tmp_path)
    assert not errors
    assert profile is not None
    assert profile.name == "prod"
    assert profile.default_engine == "e2"


def test_select_nonexistent_profile_error(tmp_path, resolver):
    profiles = {
        "default": {
            "default_engine": "e",
            "catalogs": {"c": {"type": "t"}},
            "engines": [{"name": "e", "type": "t", "catalogs": ["t"]}],
        },
    }
    pf = tmp_path / "profiles.yaml"
    pf.write_text(yaml.dump(profiles))

    profile, errors, _ = resolver.resolve(pf, "nonexistent", tmp_path)
    assert profile is None
    assert any("nonexistent" in e.message for e in errors)
    assert any("default" in e.message for e in errors)


# --- Validation ---


def test_missing_default_engine(tmp_path, resolver):
    profiles = {
        "default": {
            "catalogs": {"c": {"type": "t"}},
            "engines": [{"name": "e", "type": "t", "catalogs": ["t"]}],
        }
    }
    pf = tmp_path / "profiles.yaml"
    pf.write_text(yaml.dump(profiles))

    profile, errors, _ = resolver.resolve(pf, None, tmp_path)
    assert profile is None
    assert any("default_engine" in e.message for e in errors)


def test_missing_catalogs(tmp_path, resolver):
    profiles = {
        "default": {
            "default_engine": "e",
            "engines": [{"name": "e", "type": "t", "catalogs": ["t"]}],
        }
    }
    pf = tmp_path / "profiles.yaml"
    pf.write_text(yaml.dump(profiles))

    profile, errors, _ = resolver.resolve(pf, None, tmp_path)
    assert profile is None
    assert any("catalogs" in e.message for e in errors)


def test_missing_engines(tmp_path, resolver):
    profiles = {
        "default": {
            "default_engine": "e",
            "catalogs": {"c": {"type": "t"}},
        }
    }
    pf = tmp_path / "profiles.yaml"
    pf.write_text(yaml.dump(profiles))

    profile, errors, _ = resolver.resolve(pf, None, tmp_path)
    assert profile is None
    assert any("engines" in e.message for e in errors)


def test_engine_missing_required_fields(tmp_path, resolver):
    profiles = {
        "default": {
            "default_engine": "e",
            "catalogs": {"c": {"type": "t"}},
            "engines": [{"name": "e"}],  # missing type and catalogs
        }
    }
    pf = tmp_path / "profiles.yaml"
    pf.write_text(yaml.dump(profiles))

    profile, errors, _ = resolver.resolve(pf, None, tmp_path)
    assert profile is None
    assert any("type" in e.message for e in errors)
    assert any("catalogs" in e.message and "engine" in e.message for e in errors)


# --- Merge ---


def test_merge_project_replaces_global_catalog(tmp_path, resolver, monkeypatch):
    """Project catalog fully replaces global catalog of same name."""
    global_dir = tmp_path / "global"
    global_dir.mkdir(parents=True)
    rivet_dir = tmp_path / ".rivet"
    rivet_dir.mkdir()

    global_profiles = {
        "default": {
            "default_engine": "e",
            "catalogs": {"warehouse": {"type": "postgres", "host": "global-host"}},
            "engines": [{"name": "e", "type": "t", "catalogs": ["t"]}],
        }
    }
    global_file = rivet_dir / "profiles.yaml"
    global_file.write_text(yaml.dump(global_profiles))
    monkeypatch.setenv("HOME", str(tmp_path))

    project_profiles = {
        "default": {
            "catalogs": {"warehouse": {"type": "duckdb"}},
        }
    }
    pf = tmp_path / "profiles.yaml"
    pf.write_text(yaml.dump(project_profiles))

    profile, errors, _ = resolver.resolve(pf, None, tmp_path)
    assert not errors
    assert profile is not None
    # Project catalog replaces global — no "host" from global
    assert profile.catalogs["warehouse"].type == "duckdb"
    assert "host" not in profile.catalogs["warehouse"].options


def test_merge_non_overlapping_catalogs_preserved(tmp_path, resolver, monkeypatch):
    """Catalogs from global that aren't in project are preserved."""
    rivet_dir = tmp_path / ".rivet"
    rivet_dir.mkdir()

    global_profiles = {
        "default": {
            "default_engine": "e",
            "catalogs": {"global_cat": {"type": "pg"}},
            "engines": [{"name": "e", "type": "t", "catalogs": ["t"]}],
        }
    }
    (rivet_dir / "profiles.yaml").write_text(yaml.dump(global_profiles))
    monkeypatch.setenv("HOME", str(tmp_path))

    project_profiles = {
        "default": {
            "catalogs": {"project_cat": {"type": "duckdb"}},
        }
    }
    pf = tmp_path / "profiles.yaml"
    pf.write_text(yaml.dump(project_profiles))

    profile, errors, _ = resolver.resolve(pf, None, tmp_path)
    assert not errors
    assert profile is not None
    assert "global_cat" in profile.catalogs
    assert "project_cat" in profile.catalogs


def test_merge_project_replaces_global_engine(tmp_path, resolver, monkeypatch):
    """Project engine fully replaces global engine of same name."""
    rivet_dir = tmp_path / ".rivet"
    rivet_dir.mkdir()

    global_profiles = {
        "default": {
            "default_engine": "e",
            "catalogs": {"c": {"type": "t"}},
            "engines": [{"name": "e", "type": "spark", "catalogs": ["t"], "memory": "4g"}],
        }
    }
    (rivet_dir / "profiles.yaml").write_text(yaml.dump(global_profiles))
    monkeypatch.setenv("HOME", str(tmp_path))

    project_profiles = {
        "default": {
            "engines": [{"name": "e", "type": "duckdb", "catalogs": ["t"]}],
        }
    }
    pf = tmp_path / "profiles.yaml"
    pf.write_text(yaml.dump(project_profiles))

    profile, errors, _ = resolver.resolve(pf, None, tmp_path)
    assert not errors
    assert profile is not None
    assert profile.engines[0].type == "duckdb"
    assert "memory" not in profile.engines[0].options


def test_merge_new_engine_added(tmp_path, resolver, monkeypatch):
    """Project engine with new name is added to the list."""
    rivet_dir = tmp_path / ".rivet"
    rivet_dir.mkdir()

    global_profiles = {
        "default": {
            "default_engine": "e1",
            "catalogs": {"c": {"type": "t"}},
            "engines": [{"name": "e1", "type": "spark", "catalogs": ["t"]}],
        }
    }
    (rivet_dir / "profiles.yaml").write_text(yaml.dump(global_profiles))
    monkeypatch.setenv("HOME", str(tmp_path))

    project_profiles = {
        "default": {
            "engines": [{"name": "e2", "type": "duckdb", "catalogs": ["t"]}],
        }
    }
    pf = tmp_path / "profiles.yaml"
    pf.write_text(yaml.dump(project_profiles))

    profile, errors, _ = resolver.resolve(pf, None, tmp_path)
    assert not errors
    assert profile is not None
    engine_names = [e.name for e in profile.engines]
    assert "e1" in engine_names
    assert "e2" in engine_names


# --- Env resolution ---


def test_env_resolution(tmp_path, resolver, monkeypatch):
    monkeypatch.setenv("DB_HOST", "localhost")
    profiles = {
        "default": {
            "default_engine": "e",
            "catalogs": {"c": {"type": "pg", "host": "${DB_HOST}"}},
            "engines": [{"name": "e", "type": "t", "catalogs": ["t"]}],
        }
    }
    pf = tmp_path / "profiles.yaml"
    pf.write_text(yaml.dump(profiles))

    profile, errors, _ = resolver.resolve(pf, None, tmp_path)
    assert not errors
    assert profile is not None
    assert profile.catalogs["c"].options["host"] == "localhost"


def test_missing_env_var_error(tmp_path, resolver, monkeypatch):
    monkeypatch.delenv("MISSING_VAR", raising=False)
    profiles = {
        "default": {
            "default_engine": "e",
            "catalogs": {"c": {"type": "pg", "host": "${MISSING_VAR}"}},
            "engines": [{"name": "e", "type": "t", "catalogs": ["t"]}],
        }
    }
    pf = tmp_path / "profiles.yaml"
    pf.write_text(yaml.dump(profiles))

    profile, errors, _ = resolver.resolve(pf, None, tmp_path)
    assert profile is None
    assert any("MISSING_VAR" in e.message for e in errors)


def test_plaintext_credential_warning(tmp_path, resolver):
    profiles = {
        "default": {
            "default_engine": "e",
            "catalogs": {"c": {"type": "pg", "password": "hunter2"}},
            "engines": [{"name": "e", "type": "t", "catalogs": ["t"]}],
        }
    }
    pf = tmp_path / "profiles.yaml"
    pf.write_text(yaml.dump(profiles))

    profile, errors, warnings = resolver.resolve(pf, None, tmp_path)
    assert not errors
    assert profile is not None
    assert any("credential" in w.message.lower() or "plaintext" in w.message.lower() for w in warnings)
    # Profile still resolves despite warning
    assert profile.catalogs["c"].options["password"] == "hunter2"


# --- Edge cases ---


def test_profiles_path_not_found(tmp_path, resolver):
    missing = tmp_path / "nonexistent.yaml"
    profile, errors, _ = resolver.resolve(missing, None, tmp_path)
    assert profile is None
    assert any("not found" in e.message.lower() for e in errors)


def test_empty_directory_error(tmp_path, resolver):
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()

    profile, errors, _ = resolver.resolve(profiles_dir, None, tmp_path)
    assert profile is None
    assert len(errors) > 0


def test_invalid_yaml_file(tmp_path, resolver):
    pf = tmp_path / "profiles.yaml"
    pf.write_text(": invalid: yaml: [")

    profile, errors, _ = resolver.resolve(pf, None, tmp_path)
    assert profile is None
    assert len(errors) > 0


def test_non_mapping_file(tmp_path, resolver):
    pf = tmp_path / "profiles.yaml"
    pf.write_text("- just\n- a\n- list\n")

    profile, errors, _ = resolver.resolve(pf, None, tmp_path)
    assert profile is None
    assert any("mapping" in e.message.lower() for e in errors)


def test_profile_only_at_global_level(tmp_path, resolver, monkeypatch):
    """Profile exists only at global level — used without merge."""
    rivet_dir = tmp_path / ".rivet"
    rivet_dir.mkdir()

    global_profiles = {
        "default": {
            "default_engine": "e",
            "catalogs": {"c": {"type": "t"}},
            "engines": [{"name": "e", "type": "t", "catalogs": ["t"]}],
        }
    }
    (rivet_dir / "profiles.yaml").write_text(yaml.dump(global_profiles))
    monkeypatch.setenv("HOME", str(tmp_path))

    # Project profiles file exists but has different profile
    project_profiles = {
        "staging": {
            "default_engine": "e2",
            "catalogs": {"c2": {"type": "t2"}},
            "engines": [{"name": "e2", "type": "t2", "catalogs": ["t2"]}],
        }
    }
    pf = tmp_path / "profiles.yaml"
    pf.write_text(yaml.dump(project_profiles))

    profile, errors, _ = resolver.resolve(pf, "default", tmp_path)
    assert not errors
    assert profile is not None
    assert profile.name == "default"


def test_resolved_profile_completeness(tmp_path, resolver):
    """ResolvedProfile has all expected fields populated."""
    profiles = {
        "default": {
            "default_engine": "duckdb",
            "catalogs": {
                "warehouse": {"type": "duckdb", "path": "/data/db"},
                "lake": {"type": "iceberg", "uri": "s3://bucket"},
            },
            "engines": [
                {"name": "duckdb", "type": "duckdb", "catalogs": ["duckdb"], "threads": 4},
                {"name": "spark", "type": "spark", "catalogs": ["iceberg"], "memory": "8g"},
            ],
        }
    }
    pf = tmp_path / "profiles.yaml"
    pf.write_text(yaml.dump(profiles))

    profile, errors, _ = resolver.resolve(pf, None, tmp_path)
    assert not errors
    assert profile is not None
    assert isinstance(profile, ResolvedProfile)
    assert profile.default_engine == "duckdb"
    assert len(profile.catalogs) == 2
    assert len(profile.engines) == 2
    # Catalog options exclude 'type'
    assert profile.catalogs["warehouse"].options == {"path": "/data/db"}
    # Engine options exclude 'name', 'type', 'catalogs'
    assert profile.engines[0].options == {"threads": 4}


# --- Property 6: Profile merge — project replaces global ---
# Feature: rivet-config, Property 6: Profile merge — project replaces global
# Validates: Requirements 4.2, 4.3, 4.5

_catalog_name_st = st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True)
_engine_name_st = st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True)
_type_st = st.from_regex(r"[a-z][a-z0-9]{0,8}", fullmatch=True)
_option_key_st = st.from_regex(r"[a-z][a-z0-9_]{0,8}", fullmatch=True)
_option_val_st = st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789", min_size=1, max_size=10)


@st.composite
def _catalog_config_st(draw, name: str) -> dict:
    """Generate a catalog config dict with a given name."""
    cat_type = draw(_type_st)
    extra_keys = draw(st.lists(_option_key_st, min_size=0, max_size=3, unique=True))
    # Avoid 'type' as an extra key
    extra_keys = [k for k in extra_keys if k != "type"]
    config = {"type": cat_type}
    for k in extra_keys:
        config[k] = draw(_option_val_st)
    return config


@st.composite
def _engine_config_st(draw, name: str) -> dict:
    """Generate an engine config dict with a given name."""
    eng_type = draw(_type_st)
    extra_keys = draw(st.lists(_option_key_st, min_size=0, max_size=3, unique=True))
    extra_keys = [k for k in extra_keys if k not in ("name", "type", "catalogs")]
    config = {"name": name, "type": eng_type, "catalogs": [draw(_type_st)]}
    for k in extra_keys:
        config[k] = draw(_option_val_st)
    return config


@given(
    shared_catalog_name=_catalog_name_st,
    global_catalog=st.fixed_dictionaries({"type": _type_st, "host": _option_val_st}),
    project_catalog=st.fixed_dictionaries({"type": _type_st, "port": _option_val_st}),
    shared_engine_name=_engine_name_st,
    global_engine_extra=_option_val_st,
    project_engine_type=_type_st,
    default_engine=_engine_name_st,
)
@settings(max_examples=100, suppress_health_check=[])
def test_property6_merge_project_replaces_global(
    shared_catalog_name,
    global_catalog,
    project_catalog,
    shared_engine_name,
    global_engine_extra,
    project_engine_type,
    default_engine,
):
    """Feature: rivet-config, Property 6: Profile merge — project replaces global.

    For any global and project profile that both define a catalog or engine with
    the same name, the merged result must contain exactly the project-level
    definition — no fields from the global definition should leak through.

    Validates: Requirements 4.2, 4.3, 4.5
    """
    import tempfile

    # Ensure global and project catalog types differ so we can distinguish them
    global_cat_type = global_catalog["type"] + "_g"
    project_cat_type = project_catalog["type"] + "_p"
    global_engine_type = "spark_g"
    proj_engine_type = project_engine_type + "_p"

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        rivet_dir = tmp_path / ".rivet"
        rivet_dir.mkdir()

        global_profiles = {
            "default": {
                "default_engine": default_engine,
                "catalogs": {
                    shared_catalog_name: {"type": global_cat_type, "host": global_catalog["host"]},
                },
                "engines": [
                    {
                        "name": shared_engine_name,
                        "type": global_engine_type,
                        "catalogs": ["t"],
                        "extra": global_engine_extra,
                    }
                ],
            }
        }
        (rivet_dir / "profiles.yaml").write_text(yaml.dump(global_profiles))

        project_profiles = {
            "default": {
                "default_engine": default_engine,
                "catalogs": {
                    shared_catalog_name: {"type": project_cat_type, "port": project_catalog["port"]},
                },
                "engines": [
                    {
                        "name": shared_engine_name,
                        "type": proj_engine_type,
                        "catalogs": ["t"],
                    }
                ],
            }
        }
        pf = tmp_path / "profiles.yaml"
        pf.write_text(yaml.dump(project_profiles))

        original_home = os.environ.get("HOME")
        os.environ["HOME"] = str(tmp_path)
        try:
            resolver = ProfileResolver()
            profile, errors, _ = resolver.resolve(pf, None, tmp_path)
        finally:
            if original_home is None:
                os.environ.pop("HOME", None)
            else:
                os.environ["HOME"] = original_home

    assert not errors, f"Unexpected errors: {errors}"
    assert profile is not None

    # Catalog: project definition wins — type must be project's, global's 'host' must not appear
    merged_cat = profile.catalogs[shared_catalog_name]
    assert merged_cat.type == project_cat_type, (
        f"Expected project catalog type '{project_cat_type}', got '{merged_cat.type}'"
    )
    assert "host" not in merged_cat.options, (
        f"Global catalog field 'host' leaked into merged result: {merged_cat.options}"
    )
    assert "port" in merged_cat.options, (
        f"Project catalog field 'port' missing from merged result: {merged_cat.options}"
    )

    # Engine: project definition wins — type must be project's, global's 'extra' must not appear
    merged_engines = {e.name: e for e in profile.engines}
    assert shared_engine_name in merged_engines, (
        f"Engine '{shared_engine_name}' missing from merged result"
    )
    merged_eng = merged_engines[shared_engine_name]
    assert merged_eng.type == proj_engine_type, (
        f"Expected project engine type '{proj_engine_type}', got '{merged_eng.type}'"
    )
    assert "extra" not in merged_eng.options, (
        f"Global engine field 'extra' leaked into merged result: {merged_eng.options}"
    )
