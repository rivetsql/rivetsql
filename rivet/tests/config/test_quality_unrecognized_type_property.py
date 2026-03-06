"""Property tests for QualityParser — Property 26.

Feature: rivet-config, Property 26: Unrecognized quality check type produces error
Validates: Requirements 12.7, 13.3
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.annotations import ParsedAnnotation
from rivet_config.models import CHECK_TYPES
from rivet_config.quality import QualityParser

PARSER = QualityParser()

# Strategy: generate strings that are NOT in CHECK_TYPES
_unrecognized_type = st.from_regex(r"[a-z][a-z0-9_]{0,19}", fullmatch=True).filter(
    lambda t: t not in CHECK_TYPES
)


@given(check_type=_unrecognized_type)
@settings(max_examples=100)
def test_inline_unrecognized_type_produces_error(check_type: str) -> None:
    """Property 26: parse_inline with an unrecognized check type produces an error
    that identifies the unrecognized type."""
    fp = Path("/tmp/rivet_prop26_inline.yaml")
    raw = {"assertions": [{"type": check_type, "columns": ["id"]}]}
    checks, errors = PARSER.parse_inline(raw, fp)
    assert len(errors) >= 1
    assert checks == []
    assert any(check_type in e.message for e in errors)


@given(
    check_type=_unrecognized_type,
    line_number=st.integers(min_value=1, max_value=20),
)
@settings(max_examples=100)
def test_sql_annotation_unrecognized_type_produces_error(check_type: str, line_number: int) -> None:
    """Property 26: parse_sql_annotations with an unrecognized check type produces an error
    that identifies the unrecognized type and includes the line number."""
    fp = Path("/tmp/rivet_prop26_sql.sql")
    annotations = [
        ParsedAnnotation(key="assert", value=f"{check_type}(col)", line_number=line_number),
    ]
    checks, errors = PARSER.parse_sql_annotations(annotations, fp)
    assert len(errors) >= 1
    assert checks == []
    assert any(check_type in e.message for e in errors)
    assert any(e.line_number == line_number for e in errors)


@given(check_type=_unrecognized_type)
@settings(max_examples=100)
def test_dedicated_file_unrecognized_type_produces_error(check_type: str) -> None:
    """Property 26: parse_dedicated_file with an unrecognized check type produces an error
    that identifies the unrecognized type and the source file."""
    with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
        yaml.dump([{"type": check_type, "columns": ["id"]}], f)
        fp = Path(f.name)
    checks, errors = PARSER.parse_dedicated_file(fp)
    assert len(errors) >= 1
    assert checks == []
    assert any(check_type in e.message for e in errors)
    assert all(e.source_file == fp for e in errors)
