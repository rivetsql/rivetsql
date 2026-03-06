"""Tests for ManifestParser."""

from pathlib import Path

import pytest
import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.manifest import ManifestParser


@pytest.fixture
def parser():
    return ManifestParser()


@pytest.fixture
def tmp_manifest(tmp_path):
    """Helper that writes a rivet.yaml and returns its path."""
    def _write(data: dict) -> Path:
        p = tmp_path / "rivet.yaml"
        p.write_text(yaml.dump(data))
        return p
    return _write


VALID_DATA = {
    "profiles": "./profiles.yaml",
    "sources": "./sources/",
    "joints": "./joints/",
    "sinks": "./sinks/",
}


class TestValidManifest:
    def test_parse_minimal(self, parser, tmp_manifest):
        path = tmp_manifest(VALID_DATA)
        manifest, errors, warnings = parser.parse(path)
        assert manifest is not None
        assert errors == []
        assert warnings == []
        assert manifest.project_root == path.parent
        assert manifest.profiles_path == path.parent / "profiles.yaml"
        assert manifest.sources_dir == path.parent / "sources"
        assert manifest.joints_dir == path.parent / "joints"
        assert manifest.sinks_dir == path.parent / "sinks"
        assert manifest.quality_dir is None
        assert manifest.tests_dir == path.parent / "tests"
        assert manifest.fixtures_dir == path.parent / "fixtures"

    def test_parse_all_optional_keys(self, parser, tmp_manifest):
        data = {**VALID_DATA, "quality": "./quality/", "tests": "./my_tests/", "fixtures": "./my_fixtures/"}
        path = tmp_manifest(data)
        manifest, errors, warnings = parser.parse(path)
        assert manifest is not None
        assert errors == []
        assert manifest.quality_dir == path.parent / "quality"
        assert manifest.tests_dir == path.parent / "my_tests"
        assert manifest.fixtures_dir == path.parent / "my_fixtures"

    def test_paths_relative_to_manifest_parent(self, parser, tmp_path):
        subdir = tmp_path / "project"
        subdir.mkdir()
        path = subdir / "rivet.yaml"
        path.write_text(yaml.dump(VALID_DATA))
        manifest, errors, _ = parser.parse(path)
        assert manifest is not None
        assert manifest.project_root == subdir
        assert manifest.sources_dir == subdir / "sources"


class TestMissingFile:
    def test_file_not_found(self, parser, tmp_path):
        path = tmp_path / "rivet.yaml"
        manifest, errors, _ = parser.parse(path)
        assert manifest is None
        assert len(errors) == 1
        assert "not found" in errors[0].message

    def test_invalid_yaml(self, parser, tmp_path):
        path = tmp_path / "rivet.yaml"
        path.write_text(": : : bad yaml [")
        manifest, errors, _ = parser.parse(path)
        assert manifest is None
        assert len(errors) == 1

    def test_non_mapping(self, parser, tmp_path):
        path = tmp_path / "rivet.yaml"
        path.write_text("- a list\n- not a mapping\n")
        manifest, errors, _ = parser.parse(path)
        assert manifest is None
        assert "mapping" in errors[0].message


class TestUnrecognizedKeys:
    def test_single_unrecognized(self, parser, tmp_manifest):
        data = {**VALID_DATA, "bogus": "value"}
        path = tmp_manifest(data)
        manifest, errors, _ = parser.parse(path)
        assert manifest is None
        assert any("bogus" in e.message for e in errors)

    def test_multiple_unrecognized(self, parser, tmp_manifest):
        data = {**VALID_DATA, "foo": 1, "bar": 2}
        path = tmp_manifest(data)
        manifest, errors, _ = parser.parse(path)
        assert manifest is None
        unrecognized_msgs = [e for e in errors if "Unrecognized" in e.message]
        assert len(unrecognized_msgs) == 2


class TestMissingRequiredKeys:
    def test_all_missing(self, parser, tmp_manifest):
        path = tmp_manifest({})
        manifest, errors, _ = parser.parse(path)
        assert manifest is None
        assert len(errors) == 4
        missing_names = {e.message.split("'")[1] for e in errors}
        assert missing_names == {"profiles", "sources", "joints", "sinks"}

    def test_one_missing(self, parser, tmp_manifest):
        data = {k: v for k, v in VALID_DATA.items() if k != "sinks"}
        path = tmp_manifest(data)
        manifest, errors, _ = parser.parse(path)
        assert manifest is None
        assert any("sinks" in e.message for e in errors)


class TestDeprecatedKeys:
    def test_deprecated_assertions_warning(self, parser, tmp_manifest):
        data = {**VALID_DATA, "assertions": "./assertions/"}
        path = tmp_manifest(data)
        manifest, errors, warnings = parser.parse(path)
        assert manifest is not None
        assert errors == []
        assert len(warnings) == 1
        assert "assertions" in warnings[0].message
        assert "quality" in warnings[0].remediation

    def test_deprecated_audits_warning(self, parser, tmp_manifest):
        data = {**VALID_DATA, "audits": "./audits/"}
        path = tmp_manifest(data)
        manifest, errors, warnings = parser.parse(path)
        assert manifest is not None
        assert len(warnings) == 1
        assert "audits" in warnings[0].message

    def test_deprecated_as_quality_fallback(self, parser, tmp_manifest):
        data = {**VALID_DATA, "assertions": "./old_quality/"}
        path = tmp_manifest(data)
        manifest, errors, _ = parser.parse(path)
        assert manifest is not None
        assert manifest.quality_dir == path.parent / "old_quality"

    def test_deprecated_and_quality_coexist_error(self, parser, tmp_manifest):
        data = {**VALID_DATA, "quality": "./quality/", "assertions": "./assertions/"}
        path = tmp_manifest(data)
        manifest, errors, warnings = parser.parse(path)
        assert manifest is None
        assert any("Cannot use deprecated" in e.message for e in errors)
        # Still get the deprecation warning
        assert any("assertions" in w.message for w in warnings)

    def test_both_deprecated_keys(self, parser, tmp_manifest):
        data = {**VALID_DATA, "assertions": "./a/", "audits": "./b/"}
        path = tmp_manifest(data)
        manifest, errors, warnings = parser.parse(path)
        assert manifest is not None
        assert len(warnings) == 2


class TestDefaults:
    def test_tests_default(self, parser, tmp_manifest):
        path = tmp_manifest(VALID_DATA)
        manifest, _, _ = parser.parse(path)
        assert manifest is not None
        assert manifest.tests_dir == path.parent / "tests"

    def test_fixtures_default(self, parser, tmp_manifest):
        path = tmp_manifest(VALID_DATA)
        manifest, _, _ = parser.parse(path)
        assert manifest is not None
        assert manifest.fixtures_dir == path.parent / "fixtures"

    def test_quality_default_none(self, parser, tmp_manifest):
        path = tmp_manifest(VALID_DATA)
        manifest, _, _ = parser.parse(path)
        assert manifest is not None
        assert manifest.quality_dir is None


class TestErrorFormat:
    def test_errors_have_source_file(self, parser, tmp_manifest):
        path = tmp_manifest({})
        _, errors, _ = parser.parse(path)
        assert all(e.source_file == path for e in errors)

    def test_errors_have_remediation(self, parser, tmp_manifest):
        path = tmp_manifest({})
        _, errors, _ = parser.parse(path)
        assert all(e.remediation for e in errors)


# --- Property 1: Manifest round-trip preserves paths ---

# Strategy: a simple relative path component (no slashes, no dots)
_path_component_st = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)

# Strategy: a relative path like "foo/bar" or just "foo"
_rel_path_st = st.builds(
    lambda parts: "/".join(parts) + "/",
    st.lists(_path_component_st, min_size=1, max_size=3),
)


@given(
    profiles=_rel_path_st,
    sources=_rel_path_st,
    joints=_rel_path_st,
    sinks=_rel_path_st,
    quality=st.one_of(st.none(), _rel_path_st),
    tests=st.one_of(st.none(), _rel_path_st),
    fixtures=st.one_of(st.none(), _rel_path_st),
)
@settings(max_examples=100)
def test_property_manifest_path_resolution(
    profiles, sources, joints, sinks, quality, tests, fixtures
):
    """Feature: rivet-config, Property 1: Manifest round-trip preserves paths.

    For any valid rivet.yaml content with arbitrary relative directory paths,
    parsing the manifest and reading back the resolved paths should produce
    paths that are all relative to the manifest file's parent directory, and
    every declared key should appear in the resulting ProjectManifest.

    Validates: Requirements 1.1, 1.3
    """
    import tempfile

    data: dict = {
        "profiles": profiles,
        "sources": sources,
        "joints": joints,
        "sinks": sinks,
    }
    if quality is not None:
        data["quality"] = quality
    if tests is not None:
        data["tests"] = tests
    if fixtures is not None:
        data["fixtures"] = fixtures

    with tempfile.TemporaryDirectory() as tmp_dir:
        manifest_path = Path(tmp_dir) / "rivet.yaml"
        manifest_path.write_text(yaml.dump(data))

        parser = ManifestParser()
        manifest, errors, _ = parser.parse(manifest_path)

        assert errors == [], f"Unexpected errors: {errors}"
        assert manifest is not None

        parent = manifest_path.parent

        # All resolved paths must be under (or equal to) the manifest parent
        assert manifest.project_root == parent
        assert manifest.profiles_path == parent / profiles
        assert manifest.sources_dir == parent / sources
        assert manifest.joints_dir == parent / joints
        assert manifest.sinks_dir == parent / sinks

        if quality is not None:
            assert manifest.quality_dir == parent / quality
        else:
            assert manifest.quality_dir is None

        expected_tests = parent / tests if tests is not None else parent / "tests"
        expected_fixtures = parent / fixtures if fixtures is not None else parent / "fixtures"
        assert manifest.tests_dir == expected_tests
        assert manifest.fixtures_dir == expected_fixtures

        # Every resolved path is absolute and starts with the parent directory
        for p in [manifest.profiles_path, manifest.sources_dir, manifest.joints_dir, manifest.sinks_dir]:
            assert p.is_absolute()
            assert str(p).startswith(str(parent))


# --- Property 2: Manifest rejects unrecognized keys ---

_ALL_KNOWN_MANIFEST_KEYS = {
    "profiles", "sources", "joints", "sinks",
    "quality", "tests", "fixtures",
    "assertions", "audits",
}

# Strategy: generate keys that are NOT in the known set
_unknown_key_st = st.from_regex(r"[a-z][a-z0-9_]{1,15}", fullmatch=True).filter(
    lambda k: k not in _ALL_KNOWN_MANIFEST_KEYS
)


@given(
    unknown_keys=st.lists(_unknown_key_st, min_size=1, max_size=5, unique=True),
)
@settings(max_examples=100)
def test_property_manifest_rejects_unrecognized_keys(unknown_keys):
    """Feature: rivet-config, Property 2: Manifest rejects unrecognized keys.

    For any rivet.yaml content that includes one or more keys not in the
    recognized set, the parser should produce errors listing exactly the
    unrecognized keys.

    Validates: Requirements 1.8
    """
    import tempfile

    data = {
        "profiles": "./profiles.yaml",
        "sources": "./sources/",
        "joints": "./joints/",
        "sinks": "./sinks/",
    }
    for key in unknown_keys:
        data[key] = "some_value"

    with tempfile.TemporaryDirectory() as tmp_dir:
        manifest_path = Path(tmp_dir) / "rivet.yaml"
        manifest_path.write_text(yaml.dump(data))

        parser = ManifestParser()
        manifest, errors, _ = parser.parse(manifest_path)

        assert manifest is None, "Parser should reject manifest with unrecognized keys"
        assert len(errors) >= len(unknown_keys), (
            f"Expected at least {len(unknown_keys)} errors for keys {unknown_keys}, got {errors}"
        )
        error_messages = " ".join(e.message for e in errors)
        for key in unknown_keys:
            assert key in error_messages, (
                f"Expected error mentioning unrecognized key '{key}', got: {errors}"
            )


# --- Property 3: Manifest rejects missing required keys ---

_REQUIRED_KEYS = ["profiles", "sources", "joints", "sinks"]
_REQUIRED_VALUES = {
    "profiles": "./profiles.yaml",
    "sources": "./sources/",
    "joints": "./joints/",
    "sinks": "./sinks/",
}


@given(
    present=st.lists(
        st.sampled_from(_REQUIRED_KEYS),
        min_size=0,
        max_size=3,
        unique=True,
    )
)
@settings(max_examples=100)
def test_property_manifest_missing_required_keys(present):
    """Feature: rivet-config, Property 3: Manifest rejects missing required keys.

    For any strict subset of the required keys present in rivet.yaml, the parser
    should produce errors listing exactly the missing required keys.

    Validates: Requirements 1.9
    """
    import tempfile

    data = {k: _REQUIRED_VALUES[k] for k in present}

    with tempfile.TemporaryDirectory() as tmp_dir:
        manifest_path = Path(tmp_dir) / "rivet.yaml"
        manifest_path.write_text(yaml.dump(data))

        parser = ManifestParser()
        manifest, errors, _ = parser.parse(manifest_path)

        missing = set(_REQUIRED_KEYS) - set(present)

        assert manifest is None
        assert len(missing) > 0  # strict subset means at least one missing

        error_messages = " ".join(e.message for e in errors)
        for key in missing:
            assert key in error_messages, f"Expected error for missing key '{key}'"

        # No false positives: only missing keys appear in missing-key errors
        missing_key_errors = [e for e in errors if "Missing required key" in e.message]
        assert len(missing_key_errors) == len(missing)
