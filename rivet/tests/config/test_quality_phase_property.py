"""Property test for QualityParser — phase assignment.

Feature: rivet-config, Property 23: Quality check phase assignment
Validates: Requirements 12.1, 12.2, 13.1, 13.2
"""

from __future__ import annotations

from pathlib import Path

import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.annotations import ParsedAnnotation
from rivet_config.quality import QualityParser

PARSER = QualityParser()

# --- Strategies ---

_check_type = st.sampled_from(["not_null", "unique", "row_count", "schema"])

# Minimal valid check entries per type (no required params for row_count/schema)
_simple_check_entry = st.just({"type": "row_count"})

_check_list = st.lists(_simple_check_entry, min_size=1, max_size=5)


# --- Property 23a: parse_sql_annotations — assert → assertion, audit → audit ---

@settings(max_examples=100)
@given(
    n_asserts=st.integers(min_value=0, max_value=4),
    n_audits=st.integers(min_value=0, max_value=4),
    tmp_path=st.builds(lambda: Path("/tmp/rivet_prop_quality_phase_sql")),
)
def test_sql_annotation_phase_assignment(n_asserts, n_audits, tmp_path):
    """Property 23: assert annotations → phase=assertion; audit annotations → phase=audit."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    fp = tmp_path / "joint.sql"
    fp.touch()

    annotations = (
        [ParsedAnnotation(key="assert", value="row_count()", line_number=i + 1) for i in range(n_asserts)]
        + [ParsedAnnotation(key="audit", value="row_count()", line_number=n_asserts + i + 1) for i in range(n_audits)]
    )

    checks, errors = PARSER.parse_sql_annotations(annotations, fp)

    assert not errors, f"Unexpected errors: {errors}"
    assert len(checks) == n_asserts + n_audits

    assert all(c.phase == "assertion" for c in checks[:n_asserts])
    assert all(c.phase == "audit" for c in checks[n_asserts:])


# --- Property 23b: parse_inline — assertions key → assertion, audits key → audit ---

@settings(max_examples=100)
@given(
    assertions=_check_list,
    audits=_check_list,
    tmp_path=st.builds(lambda: Path("/tmp/rivet_prop_quality_phase_inline")),
)
def test_inline_phase_assignment(assertions, audits, tmp_path):
    """Property 23: inline assertions → phase=assertion; inline audits → phase=audit."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    fp = tmp_path / "joint.yaml"
    fp.touch()

    raw = {"assertions": assertions, "audits": audits}
    checks, errors = PARSER.parse_inline(raw, fp)

    assert not errors, f"Unexpected errors: {errors}"
    assert len(checks) == len(assertions) + len(audits)

    assert all(c.phase == "assertion" for c in checks[: len(assertions)])
    assert all(c.phase == "audit" for c in checks[len(assertions) :])


# --- Property 23c: dedicated file — assertions/audits sections → correct phases ---

@settings(max_examples=100)
@given(
    n_assertions=st.integers(min_value=0, max_value=4),
    n_audits=st.integers(min_value=0, max_value=4),
    tmp_path=st.builds(lambda: Path("/tmp/rivet_prop_quality_phase_dedicated")),
)
def test_dedicated_file_sectioned_phase_assignment(n_assertions, n_audits, tmp_path):
    """Property 23: dedicated file assertions: section → assertion; audits: section → audit."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    fp = tmp_path / "my_joint.yaml"

    content: dict = {}
    if n_assertions > 0:
        content["assertions"] = [{"type": "row_count"}] * n_assertions
    if n_audits > 0:
        content["audits"] = [{"type": "row_count"}] * n_audits

    if not content:
        # Empty file — skip (no checks to verify)
        return

    fp.write_text(yaml.dump(content))
    checks, errors = PARSER.parse_dedicated_file(fp)

    assert not errors, f"Unexpected errors: {errors}"
    assert len(checks) == n_assertions + n_audits

    assert all(c.phase == "assertion" for c in checks[:n_assertions])
    assert all(c.phase == "audit" for c in checks[n_assertions:])


# --- Property 23d: dedicated file flat list → all assertions ---

@settings(max_examples=100)
@given(
    n_checks=st.integers(min_value=1, max_value=6),
    tmp_path=st.builds(lambda: Path("/tmp/rivet_prop_quality_phase_flat")),
)
def test_dedicated_file_flat_list_all_assertions(n_checks, tmp_path):
    """Property 23: flat list in dedicated file → all checks have phase=assertion."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    fp = tmp_path / "my_joint.yaml"
    fp.write_text(yaml.dump([{"type": "row_count"}] * n_checks))

    checks, errors = PARSER.parse_dedicated_file(fp)

    assert not errors, f"Unexpected errors: {errors}"
    assert len(checks) == n_checks
    assert all(c.phase == "assertion" for c in checks)
