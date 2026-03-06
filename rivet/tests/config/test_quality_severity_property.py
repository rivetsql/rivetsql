"""Property tests for QualityParser — Property 25.

Feature: rivet-config, Property 25: Quality check severity defaults to error
Validates: Requirements 12.9, 13.5
"""

from __future__ import annotations

from pathlib import Path

import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.annotations import ParsedAnnotation
from rivet_config.quality import QualityParser

PARSER = QualityParser()

# Check types that require no mandatory params (so we can omit severity cleanly).
_PARAMLESS_TYPES = ["row_count", "schema"]

# Strategy: pick a check type that needs only 'columns' as required param.
_columns_required_types = st.sampled_from(["not_null", "unique"])

# Strategy: pick a check type with no required params.
_no_param_types = st.sampled_from(_PARAMLESS_TYPES)


@given(check_type=_columns_required_types, col=st.from_regex(r"[a-z][a-z0-9_]*", fullmatch=True).filter(lambda s: len(s) <= 20))
@settings(max_examples=100)
def test_inline_severity_defaults_to_error(check_type: str, col: str) -> None:
    """Property 25 (inline): When severity is absent from an inline check entry,
    the resulting QualityCheck has severity='error'."""
    fp = Path("joint.yaml")
    raw = {"assertions": [{"type": check_type, "columns": [col]}]}
    checks, errors = PARSER.parse_inline(raw, fp)
    assert not errors
    assert len(checks) == 1
    assert checks[0].severity == "error"


@given(check_type=_no_param_types)
@settings(max_examples=100)
def test_sql_annotation_severity_defaults_to_error(check_type: str) -> None:
    """Property 25 (sql_annotation): When severity is absent from a SQL quality
    annotation, the resulting QualityCheck has severity='error'."""
    fp = Path("joint.sql")
    annotations = [ParsedAnnotation(key="assert", value=f"{check_type}()", line_number=1)]
    checks, errors = PARSER.parse_sql_annotations(annotations, fp)
    assert not errors
    assert len(checks) == 1
    assert checks[0].severity == "error"


@given(check_type=_no_param_types)
@settings(max_examples=100)
def test_dedicated_file_severity_defaults_to_error(check_type: str) -> None:
    """Property 25 (dedicated): When severity is absent from a dedicated quality
    file check entry, the resulting QualityCheck has severity='error'."""
    import os
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
        yaml.dump([{"type": check_type}], f)
        fp = Path(f.name)
    try:
        checks, errors = PARSER.parse_dedicated_file(fp)
        assert not errors
        assert len(checks) == 1
        assert checks[0].severity == "error"
    finally:
        os.unlink(fp)


@given(check_type=_no_param_types)
@settings(max_examples=100)
def test_colocated_file_severity_defaults_to_error(check_type: str) -> None:
    """Property 25 (colocated): When severity is absent from a co-located quality
    file check entry, the resulting QualityCheck has severity='error'."""
    import os
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
        yaml.dump([{"type": check_type}], f)
        fp = Path(f.name)
    try:
        checks, errors = PARSER.parse_colocated_file(fp)
        assert not errors
        assert len(checks) == 1
        assert checks[0].severity == "error"
    finally:
        os.unlink(fp)
