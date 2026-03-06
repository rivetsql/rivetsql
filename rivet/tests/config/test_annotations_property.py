"""Property tests for AnnotationParser — Property 22.

Feature: rivet-config, Property 22: Malformed or unrecognized annotations produce errors with line numbers
Validates: Requirements 11.8, 11.9
"""

from __future__ import annotations

from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.annotations import AnnotationParser

PARSER = AnnotationParser()

# Strategy: generate a valid annotation key (identifier)
_valid_key = st.from_regex(r"[A-Za-z_][A-Za-z0-9_]*", fullmatch=True).filter(
    lambda k: len(k) <= 20
)

# Strategy: generate a valid annotation line (non-malformed string value)
_valid_annotation_line = _valid_key.map(lambda k: f"-- rivet:{k}: some_value\n")

# Strategy: generate a malformed YAML value (duplicate keys in mapping = invalid YAML)
# "{a: 1: 2}" is reliably invalid YAML
_malformed_value = st.just("{bad: yaml: here}")


@given(
    prefix_count=st.integers(min_value=0, max_value=5),
    file_path=st.builds(Path, st.just("some/path/joint.sql")),
)
@settings(max_examples=100)
def test_malformed_annotation_error_has_line_number_and_file(prefix_count, file_path):
    """Property 22: A malformed annotation value produces an error with the correct
    file path and a line number pointing to the malformed line."""
    # Build prefix_count valid annotation lines, then one malformed line
    prefix_lines = [f"-- rivet:key{i}: value\n" for i in range(prefix_count)]
    malformed_line = "-- rivet:write_strategy: {bad: yaml: here}\n"
    lines = prefix_lines + [malformed_line]

    _, _, errors = PARSER.parse(lines, file_path)

    assert len(errors) >= 1, "Expected at least one error for malformed annotation"
    # The error for the malformed line should reference the correct line number
    malformed_line_number = prefix_count + 1  # 1-indexed
    error_line_numbers = [e.line_number for e in errors]
    assert malformed_line_number in error_line_numbers, (
        f"Expected error at line {malformed_line_number}, got {error_line_numbers}"
    )
    # Every error must include the source file path
    for error in errors:
        assert error.source_file == file_path, (
            f"Expected source_file={file_path}, got {error.source_file}"
        )


@given(
    prefix_count=st.integers(min_value=0, max_value=5),
    key=_valid_key,
)
@settings(max_examples=100)
def test_malformed_annotation_error_message_contains_key(prefix_count, key):
    """Property 22 (message content): The error message for a malformed annotation
    includes the annotation key that caused the problem."""
    prefix_lines = [f"-- rivet:key{i}: value\n" for i in range(prefix_count)]
    malformed_line = f"-- rivet:{key}: {{bad: yaml: here}}\n"
    lines = prefix_lines + [malformed_line]

    _, _, errors = PARSER.parse(lines, Path("test.sql"))

    assert len(errors) >= 1
    # The error message should mention the key
    assert any(key in e.message for e in errors), (
        f"Expected key '{key}' in error message, got: {[e.message for e in errors]}"
    )
