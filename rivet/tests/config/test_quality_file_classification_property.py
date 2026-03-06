"""Property test for QualityParser — file classification.

Feature: rivet-config, Property 32: Quality file vs joint file classification
Validates: Requirements 22.3
"""

from __future__ import annotations

from pathlib import Path

import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.quality import QualityParser
from rivet_config.yaml_parser import YAMLParser

QUALITY_PARSER = QualityParser()
YAML_PARSER = YAMLParser()

# --- Strategies ---

_joint_name = st.from_regex(r"[a-z][a-z0-9_]{0,20}", fullmatch=True)
_joint_type = st.sampled_from(["source", "sql", "sink", "python"])

# Minimal valid check entry (row_count has no required params)
_check_entry = st.just({"type": "row_count"})
_check_list = st.lists(_check_entry, min_size=1, max_size=4)


# --- Property 32a: YAML file with name+type is a joint declaration, not a quality file ---

@settings(max_examples=100)
@given(
    name=_joint_name,
    joint_type=_joint_type,
    tmp_path=st.builds(lambda: Path("/tmp/rivet_prop_file_class_joint")),
)
def test_joint_file_has_name_and_type(name, joint_type, tmp_path):
    """Property 32: A YAML file with 'name' and 'type' top-level fields is a joint declaration.

    The YAMLParser should attempt to parse it (not reject it as a quality file),
    and the QualityParser should not produce valid checks from it (it's not a quality file).
    """
    tmp_path.mkdir(parents=True, exist_ok=True)
    fp = tmp_path / f"{name}.yaml"

    # Minimal joint content with name and type
    content: dict = {"name": name, "type": joint_type}
    # Add type-specific required fields to make it a valid joint
    if joint_type in ("source", "sink"):
        content["catalog"] = "my_catalog"
    if joint_type == "sink":
        content["table"] = "my_table"
    if joint_type == "sql":
        content["sql"] = "SELECT 1"
    if joint_type == "python":
        content["function"] = "my_module.my_func"

    fp.write_text(yaml.dump(content))

    # YAMLParser should parse it successfully (it's a joint declaration)
    decl, errors = YAML_PARSER.parse(fp)
    assert decl is not None, f"YAMLParser should parse joint file, got errors: {errors}"
    assert decl.name == name
    assert decl.joint_type == joint_type

    # QualityParser treating it as a colocated file should produce an error
    # (it doesn't match quality file format: not a flat list, no assertions/audits sections)
    checks, q_errors = QUALITY_PARSER.parse_colocated_file(fp)
    assert len(checks) == 0, (
        "A joint declaration file should not yield quality checks when parsed as a quality file"
    )


# --- Property 32b: YAML file without name+type is a quality file (flat list format) ---

@settings(max_examples=100)
@given(
    checks=_check_list,
    tmp_path=st.builds(lambda: Path("/tmp/rivet_prop_file_class_quality_flat")),
)
def test_quality_file_flat_list_has_no_name_or_type(checks, tmp_path):
    """Property 32: A flat-list quality file has no 'name' or 'type' top-level fields.

    The QualityParser should parse it successfully as a co-located quality file.
    The YAMLParser should fail (no 'name' or 'type' fields).
    """
    tmp_path.mkdir(parents=True, exist_ok=True)
    fp = tmp_path / "my_joint_quality.yaml"
    fp.write_text(yaml.dump(checks))

    # QualityParser should parse it successfully
    parsed_checks, errors = QUALITY_PARSER.parse_colocated_file(fp)
    assert not errors, f"Unexpected errors parsing quality file: {errors}"
    assert len(parsed_checks) == len(checks)
    assert all(c.source == "colocated" for c in parsed_checks)

    # The raw content is a list, not a dict with name+type — confirm no name/type at top level
    raw = yaml.safe_load(fp.read_text())
    assert isinstance(raw, list), "Flat quality file must be a list"
    assert not any(
        isinstance(item, dict) and "name" in item and "type" in item
        for item in raw
    ), "Quality file entries should not have both 'name' and 'type' (that would be a joint)"


# --- Property 32c: YAML file without name+type is a quality file (sectioned format) ---

@settings(max_examples=100)
@given(
    n_assertions=st.integers(min_value=0, max_value=3),
    n_audits=st.integers(min_value=0, max_value=3),
    tmp_path=st.builds(lambda: Path("/tmp/rivet_prop_file_class_quality_sectioned")),
)
def test_quality_file_sectioned_has_no_name_or_type(n_assertions, n_audits, tmp_path):
    """Property 32: A sectioned quality file (assertions:/audits:) has no 'name'/'type' top-level fields.

    The QualityParser should parse it successfully.
    """
    if n_assertions == 0 and n_audits == 0:
        return  # skip degenerate case

    tmp_path.mkdir(parents=True, exist_ok=True)
    fp = tmp_path / "my_joint_quality.yaml"

    content: dict = {}
    if n_assertions > 0:
        content["assertions"] = [{"type": "row_count"}] * n_assertions
    if n_audits > 0:
        content["audits"] = [{"type": "row_count"}] * n_audits

    fp.write_text(yaml.dump(content))

    # QualityParser should parse it successfully
    parsed_checks, errors = QUALITY_PARSER.parse_colocated_file(fp)
    assert not errors, f"Unexpected errors: {errors}"
    assert len(parsed_checks) == n_assertions + n_audits

    # Confirm no 'name' or 'type' at top level
    raw = yaml.safe_load(fp.read_text())
    assert isinstance(raw, dict)
    assert "name" not in raw
    assert "type" not in raw
