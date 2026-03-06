"""Property test for ProfileResolver — resolved profile completeness.

Feature: rivet-config, Property 12: Resolved profile completeness
Validates: Requirements 7.1, 7.2, 7.3

For any valid profile after merge and env resolution, the ResolvedProfile should
contain: all catalogs with name, type, and resolved options; all engines with
name, type, catalogs list, and resolved options; and the default_engine name.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.models import CatalogConfig, EngineConfig, ResolvedProfile
from rivet_config.profiles import ProfileResolver

RESOLVER = ProfileResolver()

_identifier = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)
_option_key = st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True).filter(
    lambda k: k not in ("type", "name", "catalogs")
)
_option_val = st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789", min_size=1, max_size=12)


@st.composite
def _catalog_entry(draw) -> tuple[str, dict]:
    name = draw(_identifier)
    cat_type = draw(_identifier)
    extra_keys = draw(st.lists(_option_key, min_size=0, max_size=3, unique=True))
    config: dict = {"type": cat_type}
    for k in extra_keys:
        config[k] = draw(_option_val)
    return name, config


@st.composite
def _engine_entry(draw) -> dict:
    name = draw(_identifier)
    eng_type = draw(_identifier)
    cat_refs = draw(st.lists(_identifier, min_size=1, max_size=3))
    extra_keys = draw(st.lists(_option_key, min_size=0, max_size=3, unique=True))
    config: dict = {"name": name, "type": eng_type, "catalogs": cat_refs}
    for k in extra_keys:
        config[k] = draw(_option_val)
    return config


@st.composite
def _valid_profile(draw) -> tuple[str, dict]:
    """Generate a valid profile name and config dict."""
    profile_name = draw(_identifier)
    catalog_entries = draw(st.lists(_catalog_entry(), min_size=1, max_size=4))
    # Deduplicate catalog names
    catalogs: dict = {}
    for cat_name, cat_cfg in catalog_entries:
        catalogs[cat_name] = cat_cfg

    engine_entries = draw(st.lists(_engine_entry(), min_size=1, max_size=3))
    # Deduplicate engine names
    engines_map: dict = {}
    for eng in engine_entries:
        engines_map[eng["name"]] = eng
    engines = list(engines_map.values())

    default_engine = engines[0]["name"]

    config = {
        "default_engine": default_engine,
        "catalogs": catalogs,
        "engines": engines,
    }
    return profile_name, config


@settings(max_examples=100)
@given(profile_data=_valid_profile())
def test_resolved_profile_completeness(profile_data: tuple[str, dict]) -> None:
    """Property 12: Resolved profile completeness.

    For any valid profile, the ResolvedProfile must contain:
    - default_engine (non-empty string)
    - all catalogs with name, type, and options
    - all engines with name, type, catalogs list, and options
    """
    import tempfile

    profile_name, config = profile_data

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        pf = tmp_path / "profiles.yaml"
        pf.write_text(yaml.dump({profile_name: config}))

        profile, errors, _ = RESOLVER.resolve(pf, profile_name, tmp_path)

    assert not errors, f"Unexpected errors: {errors}"
    assert profile is not None

    # Must be a ResolvedProfile instance
    assert isinstance(profile, ResolvedProfile)

    # Requirement 7.1: default_engine is present and non-empty
    assert isinstance(profile.default_engine, str) and profile.default_engine, (
        f"default_engine must be a non-empty string, got: {profile.default_engine!r}"
    )
    assert profile.default_engine == config["default_engine"]

    # Requirement 7.2: all catalogs present with name, type, and options
    assert isinstance(profile.catalogs, dict)
    assert set(profile.catalogs.keys()) == set(config["catalogs"].keys()), (
        f"Catalog names mismatch: {set(profile.catalogs.keys())} != {set(config['catalogs'].keys())}"
    )
    for cat_name, cat_cfg in config["catalogs"].items():
        resolved_cat = profile.catalogs[cat_name]
        assert isinstance(resolved_cat, CatalogConfig)
        assert resolved_cat.name == cat_name
        assert resolved_cat.type == cat_cfg["type"]
        expected_options = {k: v for k, v in cat_cfg.items() if k != "type"}
        assert resolved_cat.options == expected_options, (
            f"Catalog '{cat_name}' options mismatch: {resolved_cat.options} != {expected_options}"
        )

    # Requirement 7.3: all engines present with name, type, catalogs list, and options
    assert isinstance(profile.engines, list)
    input_engines = {e["name"]: e for e in config["engines"]}
    resolved_engines = {e.name: e for e in profile.engines}
    assert set(resolved_engines.keys()) == set(input_engines.keys()), (
        f"Engine names mismatch: {set(resolved_engines.keys())} != {set(input_engines.keys())}"
    )
    for eng_name, eng_cfg in input_engines.items():
        resolved_eng = resolved_engines[eng_name]
        assert isinstance(resolved_eng, EngineConfig)
        assert resolved_eng.name == eng_name
        assert resolved_eng.type == eng_cfg["type"]
        assert resolved_eng.catalogs == eng_cfg["catalogs"]
        expected_options = {k: v for k, v in eng_cfg.items() if k not in ("name", "type", "catalogs")}
        assert resolved_eng.options == expected_options, (
            f"Engine '{eng_name}' options mismatch: {resolved_eng.options} != {expected_options}"
        )
