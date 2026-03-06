"""Property tests for ProfileResolver — non-overlapping keys preserved.

Feature: rivet-config, Property 7: Profile merge — non-overlapping keys preserved
Validates: Requirements 4.4, 4.6, 4.7
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.profiles import ProfileResolver

RESOLVER = ProfileResolver()

_name = st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True)
_type_str = st.from_regex(r"[a-z][a-z0-9_]{0,8}", fullmatch=True)


def _valid_engine(name: str, type_: str) -> dict:
    return {"name": name, "type": type_, "catalogs": [type_]}


def _setup_global(tmp_path: Path, profiles: dict) -> str:
    """Write global profiles and set HOME so the resolver finds them.

    Returns the original HOME value for cleanup.
    """
    rivet_dir = tmp_path / ".rivet"
    rivet_dir.mkdir(parents=True, exist_ok=True)
    (rivet_dir / "profiles.yaml").write_text(yaml.dump(profiles))
    old_home = os.environ.get("HOME", "")
    os.environ["HOME"] = str(tmp_path)
    return old_home


def _write_project_profiles(tmp_path: Path, profiles: dict) -> Path:
    p = tmp_path / "profiles.yaml"
    p.write_text(yaml.dump(profiles))
    return p


# --- Property 7a: global-only catalogs are preserved in merged result ---

@settings(max_examples=100)
@given(
    pname=_name,
    engine_name=_name,
    engine_type=_type_str,
    global_cat=_name,
    project_cat=_name,
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop7_global_cat")),
)
def test_global_only_catalog_preserved(
    pname, engine_name, engine_type, global_cat, project_cat, tmp_path
):
    """Property 7: catalog defined only in global profile is preserved in merged result."""
    if global_cat == project_cat:
        project_cat = project_cat + "x"

    tmp_path.mkdir(parents=True, exist_ok=True)
    old_home = _setup_global(tmp_path, {
        pname: {
            "default_engine": engine_name,
            "catalogs": {global_cat: {"type": engine_type}},
            "engines": [_valid_engine(engine_name, engine_type)],
        }
    })

    try:
        pf = _write_project_profiles(tmp_path, {
            pname: {
                "catalogs": {project_cat: {"type": engine_type}},
            }
        })

        profile, errors, _ = RESOLVER.resolve(pf, pname, tmp_path)

        assert not errors, f"Unexpected errors: {errors}"
        assert profile is not None
        assert global_cat in profile.catalogs, (
            f"Global-only catalog '{global_cat}' should be preserved in merged result"
        )
    finally:
        os.environ["HOME"] = old_home


# --- Property 7b: project-only catalogs are present in merged result ---

@settings(max_examples=100)
@given(
    pname=_name,
    engine_name=_name,
    engine_type=_type_str,
    global_cat=_name,
    project_cat=_name,
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop7_project_cat")),
)
def test_project_only_catalog_present(
    pname, engine_name, engine_type, global_cat, project_cat, tmp_path
):
    """Property 7: catalog defined only in project profile is present in merged result."""
    if global_cat == project_cat:
        global_cat = global_cat + "g"

    tmp_path.mkdir(parents=True, exist_ok=True)
    old_home = _setup_global(tmp_path, {
        pname: {
            "default_engine": engine_name,
            "catalogs": {global_cat: {"type": engine_type}},
            "engines": [_valid_engine(engine_name, engine_type)],
        }
    })

    try:
        pf = _write_project_profiles(tmp_path, {
            pname: {
                "catalogs": {project_cat: {"type": engine_type}},
            }
        })

        profile, errors, _ = RESOLVER.resolve(pf, pname, tmp_path)

        assert not errors, f"Unexpected errors: {errors}"
        assert profile is not None
        assert project_cat in profile.catalogs, (
            f"Project-only catalog '{project_cat}' should be present in merged result"
        )
    finally:
        os.environ["HOME"] = old_home


# --- Property 7c: union of catalog names equals union from both levels ---

@settings(max_examples=100)
@given(
    pname=_name,
    engine_name=_name,
    engine_type=_type_str,
    global_cats=st.lists(_name, min_size=1, max_size=3, unique=True),
    project_cats=st.lists(_name, min_size=1, max_size=3, unique=True),
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop7_union")),
)
def test_merged_catalog_names_equal_union(
    pname, engine_name, engine_type, global_cats, project_cats, tmp_path
):
    """Property 7: merged catalog names equal the union of global and project catalog names."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    old_home = _setup_global(tmp_path, {
        pname: {
            "default_engine": engine_name,
            "catalogs": {c: {"type": engine_type} for c in global_cats},
            "engines": [_valid_engine(engine_name, engine_type)],
        }
    })

    try:
        pf = _write_project_profiles(tmp_path, {
            pname: {
                "catalogs": {c: {"type": engine_type} for c in project_cats},
            }
        })

        profile, errors, _ = RESOLVER.resolve(pf, pname, tmp_path)

        assert not errors, f"Unexpected errors: {errors}"
        assert profile is not None

        expected_names = set(global_cats) | set(project_cats)
        actual_names = set(profile.catalogs.keys())
        assert actual_names == expected_names, (
            f"Expected catalog names {expected_names}, got {actual_names}"
        )
    finally:
        os.environ["HOME"] = old_home


# --- Property 7d: global-only engine is preserved in merged result ---

@settings(max_examples=100)
@given(
    pname=_name,
    global_engine_name=_name,
    project_engine_name=_name,
    engine_type=_type_str,
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop7_global_engine")),
)
def test_global_only_engine_preserved(
    pname, global_engine_name, project_engine_name, engine_type, tmp_path
):
    """Property 7: engine defined only in global profile is preserved in merged result (Req 4.6)."""
    if global_engine_name == project_engine_name:
        project_engine_name = project_engine_name + "p"

    tmp_path.mkdir(parents=True, exist_ok=True)
    old_home = _setup_global(tmp_path, {
        pname: {
            "default_engine": global_engine_name,
            "catalogs": {"c": {"type": engine_type}},
            "engines": [_valid_engine(global_engine_name, engine_type)],
        }
    })

    try:
        pf = _write_project_profiles(tmp_path, {
            pname: {
                "engines": [_valid_engine(project_engine_name, engine_type)],
            }
        })

        profile, errors, _ = RESOLVER.resolve(pf, pname, tmp_path)

        assert not errors, f"Unexpected errors: {errors}"
        assert profile is not None
        engine_names = {e.name for e in profile.engines}
        assert global_engine_name in engine_names, (
            f"Global-only engine '{global_engine_name}' should be preserved in merged result"
        )
    finally:
        os.environ["HOME"] = old_home


# --- Property 7e: profile existing only at one level is used without merge ---

@settings(max_examples=100)
@given(
    global_pname=_name,
    project_pname=_name,
    engine_name=_name,
    engine_type=_type_str,
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop7_single_level")),
)
def test_profile_only_at_global_level_used_unchanged(
    global_pname, project_pname, engine_name, engine_type, tmp_path
):
    """Property 7: profile existing only at global level is used without merge (Req 4.7)."""
    if global_pname == project_pname:
        project_pname = project_pname + "p"

    tmp_path.mkdir(parents=True, exist_ok=True)
    old_home = _setup_global(tmp_path, {
        global_pname: {
            "default_engine": engine_name,
            "catalogs": {"c": {"type": engine_type}},
            "engines": [_valid_engine(engine_name, engine_type)],
        }
    })

    try:
        pf = _write_project_profiles(tmp_path, {
            project_pname: {
                "default_engine": engine_name,
                "catalogs": {"c": {"type": engine_type}},
                "engines": [_valid_engine(engine_name, engine_type)],
            }
        })

        # Select the global-only profile
        profile, errors, _ = RESOLVER.resolve(pf, global_pname, tmp_path)

        assert not errors, f"Unexpected errors: {errors}"
        assert profile is not None
        assert profile.name == global_pname
        assert profile.default_engine == engine_name
    finally:
        os.environ["HOME"] = old_home
