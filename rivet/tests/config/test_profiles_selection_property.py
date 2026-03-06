"""Property test for ProfileResolver — selection by name.

Feature: rivet-config, Property 8: Profile selection by name
Validates: Requirements 5.2, 5.3

For any set of available profiles and any profile name that exists in that set,
selecting that profile should return the profile with that name. Selecting a name
that does not exist should produce an error listing all available profile names.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.profiles import ProfileResolver

RESOLVER = ProfileResolver()

_identifier = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)
_type_str = st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True)


def _valid_profile_data(engine_name: str, engine_type: str) -> dict:
    return {
        "default_engine": engine_name,
        "catalogs": {"c": {"type": engine_type}},
        "engines": [{"name": engine_name, "type": engine_type, "catalogs": [engine_type]}],
    }


@settings(max_examples=100)
@given(
    profile_names=st.lists(_identifier, min_size=1, max_size=5, unique=True),
    select_index=st.integers(min_value=0, max_value=4),
    engine_type=_type_str,
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop8_select_existing")),
)
def test_select_existing_profile_by_name(
    profile_names: list[str],
    select_index: int,
    engine_type: str,
    tmp_path: Path,
) -> None:
    """Property 8a: Selecting an existing profile by name returns that profile.

    Validates: Requirement 5.2
    """
    tmp_path.mkdir(parents=True, exist_ok=True)
    # Clamp index to valid range
    idx = select_index % len(profile_names)
    target = profile_names[idx]

    profiles = {
        name: _valid_profile_data(name, engine_type)
        for name in profile_names
    }
    pf = tmp_path / "profiles.yaml"
    pf.write_text(yaml.dump(profiles))

    profile, errors, _ = RESOLVER.resolve(pf, target, tmp_path)

    assert not errors, f"Unexpected errors selecting '{target}': {errors}"
    assert profile is not None
    assert profile.name == target


@settings(max_examples=100)
@given(
    profile_names=st.lists(_identifier, min_size=1, max_size=5, unique=True),
    missing_name=_identifier.filter(lambda n: True),  # filtered below in body
    engine_type=_type_str,
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop8_select_missing")),
)
def test_select_nonexistent_profile_produces_error_with_available_names(
    profile_names: list[str],
    missing_name: str,
    engine_type: str,
    tmp_path: Path,
) -> None:
    """Property 8b: Selecting a non-existent profile produces an error listing available names.

    Validates: Requirement 5.3
    """
    # Ensure missing_name is not in profile_names
    if missing_name in profile_names:
        return  # skip this example

    tmp_path.mkdir(parents=True, exist_ok=True)
    profiles = {
        name: _valid_profile_data(name, engine_type)
        for name in profile_names
    }
    pf = tmp_path / "profiles.yaml"
    pf.write_text(yaml.dump(profiles))

    profile, errors, _ = RESOLVER.resolve(pf, missing_name, tmp_path)

    assert profile is None, f"Expected None for missing profile '{missing_name}'"
    assert errors, f"Expected errors for missing profile '{missing_name}'"

    # Error must mention the requested name and at least one available profile name
    error_text = " ".join(e.message for e in errors)
    assert missing_name in error_text, (
        f"Error should mention requested name '{missing_name}': {error_text}"
    )
    assert any(name in error_text for name in profile_names), (
        f"Error should list available profile names {profile_names}: {error_text}"
    )
