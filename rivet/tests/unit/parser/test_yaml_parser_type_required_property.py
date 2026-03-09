"""Property tests for YAMLParser — type-specific required field validation.

Feature: rivet-config, Property 16: Joint type-specific required field validation
Validates: Requirements 9.5, 9.6, 9.7, 9.8, 9.10, 11.7
"""

from __future__ import annotations

from pathlib import Path

import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.yaml_parser import YAMLParser

PARSER = YAMLParser()

_name = st.from_regex(r"[a-z][a-z0-9_]{0,20}", fullmatch=True)
_identifier = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)
_nonempty_str = st.text(min_size=1, max_size=40).filter(str.strip)

# Required fields per type (must be absent to trigger errors)
_TYPE_REQUIRED: dict[str, list[str]] = {
    "source": ["catalog"],
    "sink": ["catalog", "table"],
    "sql": ["sql"],
    "python": ["function"],
}

# Minimal valid base for each type (all required fields present)
_VALID_BASE: dict[str, dict] = {
    "source": {"catalog": "pg"},
    "sink": {"catalog": "pg", "table": "t"},
    "sql": {"sql": "SELECT 1"},
    "python": {"function": "mod.fn"},
}


def _write_yaml_file(tmp_path: Path, data: dict) -> Path:
    p = tmp_path / "joint.yaml"
    p.write_text(yaml.dump(data))
    return p


# --- Property 16a: missing type-specific required fields produce errors ---

@settings(max_examples=100)
@given(
    name=_name,
    joint_type=st.sampled_from(["source", "sink", "sql", "python"]),
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop16_missing_required")),
)
def test_missing_type_required_fields_produce_errors(name, joint_type, tmp_path):
    """Property 16: missing type-specific required fields produce errors identifying the file and fields."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    required_fields = _TYPE_REQUIRED[joint_type]

    # Build a declaration with NO type-specific required fields
    data: dict = {"name": name, "type": joint_type}
    p = _write_yaml_file(tmp_path, data)
    _, errors = PARSER.parse(p)

    assert errors, f"Expected errors for {joint_type} missing {required_fields}, got none"
    error_messages = " ".join(e.message for e in errors)
    for field in required_fields:
        assert field in error_messages, (
            f"Expected error mentioning '{field}' for type '{joint_type}', "
            f"got: {error_messages}"
        )
    # All errors must reference the source file
    for e in errors:
        assert e.source_file == p


# --- Property 16b: valid declarations with all required fields produce no type-related errors ---

@settings(max_examples=100)
@given(
    name=_name,
    joint_type=st.sampled_from(["source", "sink", "sql", "python"]),
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop16_valid_required")),
)
def test_valid_type_required_fields_no_errors(name, joint_type, tmp_path):
    """Property 16: declarations with all required fields for their type parse without type-related errors."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    data: dict = {"name": name, "type": joint_type, **_VALID_BASE[joint_type]}
    p = _write_yaml_file(tmp_path, data)
    decl, errors = PARSER.parse(p)

    assert not errors, f"Unexpected errors for valid {joint_type}: {errors}"
    assert decl is not None
    assert decl.joint_type == joint_type


# --- Property 16c: partial omission — each required field independently triggers an error ---

@settings(max_examples=100)
@given(
    name=_name,
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop16_partial_sink")),
)
def test_sink_missing_catalog_only(name, tmp_path):
    """Property 16: sink missing only 'catalog' produces an error mentioning 'catalog'."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    data: dict = {"name": name, "type": "sink", "table": "t"}
    p = _write_yaml_file(tmp_path, data)
    _, errors = PARSER.parse(p)

    assert errors
    assert any("catalog" in e.message for e in errors)


@settings(max_examples=100)
@given(
    name=_name,
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop16_partial_sink_table")),
)
def test_sink_missing_table_only(name, tmp_path):
    """Property 16: sink missing only 'table' produces an error mentioning 'table'."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    data: dict = {"name": name, "type": "sink", "catalog": "pg"}
    p = _write_yaml_file(tmp_path, data)
    _, errors = PARSER.parse(p)

    assert errors
    assert any("table" in e.message for e in errors)
