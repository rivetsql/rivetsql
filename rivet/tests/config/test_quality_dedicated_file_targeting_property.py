"""Property test for QualityParser — dedicated file joint targeting.

Feature: rivet-config, Property 27: Dedicated quality file joint targeting
Validates: Requirements 14.2, 14.3, 14.4, 14.5
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.quality import QualityParser

PARSER = QualityParser()

# Strategy: valid joint names (used as filename stems and joint: values)
_joint_name = st.from_regex(r"[a-z][a-z0-9_]{0,19}", fullmatch=True)

# Simple check entry with no required params
_simple_check = st.just({"type": "row_count"})
_check_list = st.lists(_simple_check, min_size=1, max_size=5)


# --- Property 27a: Filename stem targeting (Req 14.2) ---
# The file is named <joint_name>.yaml; the parser returns checks successfully.
# The caller uses the stem for targeting — the parser just parses the file.

@given(joint_name=_joint_name, n_checks=st.integers(min_value=1, max_value=5))
@settings(max_examples=100)
def test_filename_stem_used_for_targeting(joint_name: str, n_checks: int) -> None:
    """Property 27 (14.2): A dedicated quality file named <joint>.yaml is parsed
    successfully; the filename stem is the natural targeting key for the caller."""
    with tempfile.TemporaryDirectory() as tmpdir:
        fp = Path(tmpdir) / f"{joint_name}.yaml"
        fp.write_text(yaml.dump([{"type": "row_count"}] * n_checks))

        checks, errors = PARSER.parse_dedicated_file(fp)

        assert not errors, f"Unexpected errors for stem '{joint_name}': {errors}"
        assert len(checks) == n_checks
        # All checks carry the source_file so the caller can derive the stem
        assert all(c.source_file == fp for c in checks)
        assert fp.stem == joint_name


# --- Property 27b: Flat list → all assertions (Req 14.3) ---

@given(n_checks=st.integers(min_value=1, max_value=8))
@settings(max_examples=100)
def test_flat_list_produces_all_assertions(n_checks: int) -> None:
    """Property 27 (14.3): A flat list in a dedicated quality file produces
    QualityCheck objects all with phase='assertion'."""
    with tempfile.TemporaryDirectory() as tmpdir:
        fp = Path(tmpdir) / "my_joint.yaml"
        fp.write_text(yaml.dump([{"type": "row_count"}] * n_checks))

        checks, errors = PARSER.parse_dedicated_file(fp)

        assert not errors, f"Unexpected errors: {errors}"
        assert len(checks) == n_checks
        assert all(c.phase == "assertion" for c in checks)
        assert all(c.source == "dedicated" for c in checks)


# --- Property 27c: Sectioned format → correct phases (Req 14.4) ---

@given(
    n_assertions=st.integers(min_value=0, max_value=4),
    n_audits=st.integers(min_value=0, max_value=4),
)
@settings(max_examples=100)
def test_sectioned_format_produces_correct_phases(n_assertions: int, n_audits: int) -> None:
    """Property 27 (14.4): A sectioned quality file with assertions:/audits: sections
    produces checks with the correct phase for each section."""
    if n_assertions == 0 and n_audits == 0:
        return  # Nothing to verify

    with tempfile.TemporaryDirectory() as tmpdir:
        fp = Path(tmpdir) / "my_joint.yaml"
        content: dict = {}
        if n_assertions > 0:
            content["assertions"] = [{"type": "row_count"}] * n_assertions
        if n_audits > 0:
            content["audits"] = [{"type": "row_count"}] * n_audits
        fp.write_text(yaml.dump(content))

        checks, errors = PARSER.parse_dedicated_file(fp)

        assert not errors, f"Unexpected errors: {errors}"
        assert len(checks) == n_assertions + n_audits
        assert all(c.phase == "assertion" for c in checks[:n_assertions])
        assert all(c.phase == "audit" for c in checks[n_assertions:])
        assert all(c.source == "dedicated" for c in checks)


# --- Property 27d: Explicit joint: field overrides filename stem (Req 14.5) ---
# The parser must handle files with a joint: field alongside assertions:/audits:.
# The joint: value is available for the caller to use for targeting.

@given(
    file_stem=_joint_name,
    explicit_joint=_joint_name,
    n_checks=st.integers(min_value=1, max_value=4),
)
@settings(max_examples=100)
def test_explicit_joint_field_parsed_without_error(
    file_stem: str, explicit_joint: str, n_checks: int
) -> None:
    """Property 27 (14.5): A dedicated quality file with an explicit 'joint:' field
    is parsed successfully. The joint: value is preserved in the file for the caller
    to use for targeting instead of the filename stem."""
    with tempfile.TemporaryDirectory() as tmpdir:
        fp = Path(tmpdir) / f"{file_stem}.yaml"
        content = {
            "joint": explicit_joint,
            "assertions": [{"type": "row_count"}] * n_checks,
        }
        fp.write_text(yaml.dump(content))

        checks, errors = PARSER.parse_dedicated_file(fp)

        assert not errors, (
            f"Unexpected errors when joint: '{explicit_joint}' differs from stem '{file_stem}': {errors}"
        )
        assert len(checks) == n_checks
        assert all(c.phase == "assertion" for c in checks)
        assert all(c.source == "dedicated" for c in checks)

        # The caller can read the joint: field from the raw file to determine targeting
        raw = yaml.safe_load(fp.read_text())
        assert raw.get("joint") == explicit_joint
