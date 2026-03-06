"""Property tests for ProfileResolver — required field validation.

Feature: rivet-config, Property 5: Profile required field validation
Validates: Requirements 2.2, 2.3, 2.4, 2.5
"""

from __future__ import annotations

from pathlib import Path

import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.profiles import ProfileResolver

RESOLVER = ProfileResolver()

_profile_name = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)
_engine_name = st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True)
_catalog_name = st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True)
_type_str = st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True)

_REQUIRED_TOP_LEVEL = ["default_engine", "catalogs", "engines"]
_REQUIRED_ENGINE_FIELDS = ["name", "type", "catalogs"]


def _write_profiles_file(tmp_path: Path, profiles: dict) -> Path:
    p = tmp_path / "profiles.yaml"
    p.write_text(yaml.dump(profiles))
    return p


def _valid_engine(name: str, type_: str) -> dict:
    return {"name": name, "type": type_, "catalogs": [type_]}


def _valid_profile(engine_name: str, engine_type: str, catalog_name: str) -> dict:
    return {
        "default_engine": engine_name,
        "catalogs": {catalog_name: {"type": engine_type}},
        "engines": [_valid_engine(engine_name, engine_type)],
    }


# --- Property 5a: missing top-level required fields produce errors ---

@settings(max_examples=100)
@given(
    pname=_profile_name,
    missing_field=st.sampled_from(_REQUIRED_TOP_LEVEL),
    engine_name=_engine_name,
    engine_type=_type_str,
    catalog_name=_catalog_name,
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop5_missing_top")),
)
def test_missing_top_level_required_field_produces_error(
    pname, missing_field, engine_name, engine_type, catalog_name, tmp_path
):
    """Property 5: profile missing any top-level required field produces an error identifying the field."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    profile_data = _valid_profile(engine_name, engine_type, catalog_name)
    del profile_data[missing_field]
    pf = _write_profiles_file(tmp_path, {pname: profile_data})

    profile, errors, _ = RESOLVER.resolve(pf, pname, tmp_path)

    assert profile is None, f"Expected None when '{missing_field}' is missing"
    assert errors, f"Expected errors when '{missing_field}' is missing"
    error_text = " ".join(e.message for e in errors)
    assert missing_field in error_text, (
        f"Expected error mentioning '{missing_field}', got: {error_text}"
    )


# --- Property 5b: missing engine-level required fields produce errors ---

@settings(max_examples=100)
@given(
    pname=_profile_name,
    missing_field=st.sampled_from(_REQUIRED_ENGINE_FIELDS),
    engine_name=_engine_name,
    engine_type=_type_str,
    catalog_name=_catalog_name,
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop5_missing_engine")),
)
def test_missing_engine_required_field_produces_error(
    pname, missing_field, engine_name, engine_type, catalog_name, tmp_path
):
    """Property 5: engine entry missing any required field produces an error identifying the field."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    engine = _valid_engine(engine_name, engine_type)
    del engine[missing_field]
    profile_data = {
        "default_engine": engine_name,
        "catalogs": {catalog_name: {"type": engine_type}},
        "engines": [engine],
    }
    pf = _write_profiles_file(tmp_path, {pname: profile_data})

    profile, errors, _ = RESOLVER.resolve(pf, pname, tmp_path)

    assert profile is None, f"Expected None when engine '{missing_field}' is missing"
    assert errors, f"Expected errors when engine '{missing_field}' is missing"
    error_text = " ".join(e.message for e in errors)
    assert missing_field in error_text, (
        f"Expected error mentioning '{missing_field}', got: {error_text}"
    )


# --- Property 5c: valid profiles with all required fields produce no validation errors ---

@settings(max_examples=100)
@given(
    pname=_profile_name,
    engine_name=_engine_name,
    engine_type=_type_str,
    catalog_name=_catalog_name,
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop5_valid")),
)
def test_valid_profile_with_all_required_fields_no_errors(
    pname, engine_name, engine_type, catalog_name, tmp_path
):
    """Property 5: profile with all required fields resolves without validation errors."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    profile_data = _valid_profile(engine_name, engine_type, catalog_name)
    pf = _write_profiles_file(tmp_path, {pname: profile_data})

    profile, errors, _ = RESOLVER.resolve(pf, pname, tmp_path)

    assert not errors, f"Unexpected errors for valid profile: {errors}"
    assert profile is not None
    assert profile.name == pname
    assert profile.default_engine == engine_name


# --- Property 5d: errors include profile name for identification ---

@settings(max_examples=100)
@given(
    pname=_profile_name,
    engine_name=_engine_name,
    engine_type=_type_str,
    catalog_name=_catalog_name,
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop5_error_context")),
)
def test_validation_error_identifies_profile(
    pname, engine_name, engine_type, catalog_name, tmp_path
):
    """Property 5: validation errors identify the profile name."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    # Remove default_engine to trigger error
    profile_data = _valid_profile(engine_name, engine_type, catalog_name)
    del profile_data["default_engine"]
    pf = _write_profiles_file(tmp_path, {pname: profile_data})

    _, errors, _ = RESOLVER.resolve(pf, pname, tmp_path)

    assert errors
    error_text = " ".join(e.message for e in errors)
    assert pname in error_text, (
        f"Expected error to mention profile name '{pname}', got: {error_text}"
    )
