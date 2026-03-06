"""Property test for ProfileResolver — format equivalence.

Feature: rivet-config, Property 4: Profile format equivalence
Validates: Requirements 2.1, 3.1, 3.2

For any valid profile configuration, loading it from a single-file format
(profile name as top-level key mapping to config) and from a directory format
(filename stem as profile name, file content as config directly) should produce
equivalent ResolvedProfile objects.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.profiles import ProfileResolver

RESOLVER = ProfileResolver()

# --- Strategies ---

_identifier = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)

_catalog_config = st.fixed_dictionaries({"type": _identifier})

_engine_config = st.fixed_dictionaries({
    "name": _identifier,
    "type": _identifier,
    "catalogs": st.lists(_identifier, min_size=1, max_size=3),
})

_profile_config = st.fixed_dictionaries({
    "default_engine": _identifier,
    "catalogs": st.dictionaries(_identifier, _catalog_config, min_size=1, max_size=3),
    "engines": st.lists(_engine_config, min_size=1, max_size=3),
})


@settings(max_examples=100)
@given(
    profile_name=_identifier,
    config=_profile_config,
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop_profile_fmt")),
)
def test_profile_format_equivalence(profile_name: str, config: dict, tmp_path: Path) -> None:
    """Property 4: Loading a profile from single-file vs directory format produces equivalent results."""
    tmp_path.mkdir(parents=True, exist_ok=True)

    # Single-file format: {profile_name: config}
    single_file = tmp_path / "profiles.yaml"
    single_file.write_text(yaml.dump({profile_name: config}))

    # Directory format: profiles/<profile_name>.yaml with config directly
    profiles_dir = tmp_path / "profiles_dir"
    profiles_dir.mkdir(exist_ok=True)
    (profiles_dir / f"{profile_name}.yaml").write_text(yaml.dump(config))

    profile_file, errors_file, _ = RESOLVER.resolve(single_file, profile_name, tmp_path)
    profile_dir, errors_dir, _ = RESOLVER.resolve(profiles_dir, profile_name, tmp_path)

    assert not errors_file, f"Single-file errors: {errors_file}"
    assert not errors_dir, f"Directory errors: {errors_dir}"
    assert profile_file is not None
    assert profile_dir is not None

    # Both formats should produce equivalent ResolvedProfile
    assert profile_file.name == profile_dir.name == profile_name
    assert profile_file.default_engine == profile_dir.default_engine
    assert set(profile_file.catalogs.keys()) == set(profile_dir.catalogs.keys())
    for cat_name in profile_file.catalogs:
        assert profile_file.catalogs[cat_name].type == profile_dir.catalogs[cat_name].type
        assert profile_file.catalogs[cat_name].options == profile_dir.catalogs[cat_name].options

    file_engines = {e.name: e for e in profile_file.engines}
    dir_engines = {e.name: e for e in profile_dir.engines}
    assert set(file_engines.keys()) == set(dir_engines.keys())
    for eng_name in file_engines:
        assert file_engines[eng_name].type == dir_engines[eng_name].type
        assert file_engines[eng_name].catalogs == dir_engines[eng_name].catalogs
        assert file_engines[eng_name].options == dir_engines[eng_name].options
