"""Property tests for YAMLParser — name validation.

Feature: rivet-config, Property 17: Joint name validation
Validates: Requirements 9.3, 16.1, 16.2, 16.3
"""

from __future__ import annotations

from pathlib import Path

import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.models import JOINT_NAME_MAX_LENGTH
from rivet_config.yaml_parser import YAMLParser

PARSER = YAMLParser()

_valid_name = st.from_regex(r"[a-z][a-z0-9_]{0,127}", fullmatch=True)

# Invalid names: empty, starts with digit/uppercase/underscore, or contains invalid chars
_invalid_name = st.one_of(
    st.just(""),
    st.from_regex(r"[0-9][a-z0-9_]*", fullmatch=True),
    st.from_regex(r"[A-Z][a-zA-Z0-9_]*", fullmatch=True),
    st.from_regex(r"_[a-z0-9_]*", fullmatch=True),
    st.from_regex(r"[a-z][a-z0-9_]*[A-Z!@#$%^&*][a-z0-9_]*", fullmatch=True),
)

_too_long_name = st.from_regex(
    r"[a-z][a-z0-9_]{128,200}", fullmatch=True
)

_catalog = st.just("pg")


def _write_yaml(tmp_path: Path, data: dict) -> Path:
    p = tmp_path / "joint.yaml"
    p.write_text(yaml.dump(data))
    return p


# --- Property 17a: valid names are accepted ---

@settings(max_examples=100)
@given(
    name=_valid_name,
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop17_valid")),
)
def test_valid_name_accepted(name, tmp_path):
    """Property 17: names matching [a-z][a-z0-9_]* and ≤128 chars are accepted."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    data = {"name": name, "type": "source", "catalog": "pg"}
    p = _write_yaml(tmp_path, data)
    decl, errors = PARSER.parse(p)

    assert not errors, f"Valid name '{name}' produced errors: {errors}"
    assert decl is not None
    assert decl.name == name


# --- Property 17b: invalid names produce errors with file path and name ---

@settings(max_examples=100)
@given(
    name=_invalid_name,
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop17_invalid")),
)
def test_invalid_name_produces_error(name, tmp_path):
    """Property 17: names not matching [a-z][a-z0-9_]* produce an error with file and name."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    data = {"name": name, "type": "source", "catalog": "pg"}
    p = _write_yaml(tmp_path, data)
    _, errors = PARSER.parse(p)

    assert errors, f"Invalid name '{name}' should produce errors"
    messages = " ".join(e.message for e in errors)
    assert any(e.source_file == p for e in errors), "Error must reference the source file"
    # The invalid name should appear in at least one error message
    if name:
        assert name in messages, f"Invalid name '{name}' not mentioned in errors: {messages}"


# --- Property 17c: names exceeding max length produce errors ---

@settings(max_examples=100)
@given(
    name=_too_long_name,
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop17_toolong")),
)
def test_too_long_name_produces_error(name, tmp_path):
    """Property 17: names exceeding 128 characters produce an error."""
    assert len(name) > JOINT_NAME_MAX_LENGTH
    tmp_path.mkdir(parents=True, exist_ok=True)
    data = {"name": name, "type": "source", "catalog": "pg"}
    p = _write_yaml(tmp_path, data)
    _, errors = PARSER.parse(p)

    assert errors, f"Too-long name (len={len(name)}) should produce errors"
    assert any(e.source_file == p for e in errors), "Error must reference the source file"
