"""Property tests for AnnotationParser — value type parsing.

Feature: rivet-config, Property 21: Annotation value type parsing
Validates: Requirements 11.4, 11.5, 11.6
"""

from __future__ import annotations

from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.annotations import AnnotationParser

PARSER = AnnotationParser()
FILE = Path("test.sql")


def _parse_single(key: str, value_str: str):
    line = f"-- rivet:{key}: {value_str}\n"
    annotations, _, errors = PARSER.parse([line], FILE)
    return annotations, errors


# Strategies
# Keys must match [A-Za-z_][A-Za-z0-9_]* (ASCII only, per annotation regex)
_key = st.from_regex(r"[a-z][a-z0-9_]{0,14}", fullmatch=True)

# Identifiers safe for YAML list items (no commas, brackets, or special chars)
_identifier = st.from_regex(r"[a-z][a-z0-9_]{0,9}", fullmatch=True)


# --- Property 21a: Bracket list values produce Python lists (Requirement 11.4) ---

@settings(max_examples=100)
@given(key=_key, items=st.lists(_identifier, min_size=0, max_size=5))
def test_bracket_list_produces_python_list(key: str, items: list[str]) -> None:
    """Bracket-delimited annotation values are parsed as Python lists."""
    value_str = "[" + ", ".join(items) + "]"
    annotations, errors = _parse_single(key, value_str)
    assert errors == [], f"Unexpected errors for value '{value_str}': {errors}"
    assert len(annotations) == 1
    result = annotations[0].value
    assert isinstance(result, list), f"Expected list for '{value_str}', got {type(result)}"
    assert len(result) == len(items)


# --- Property 21b: 'true'/'false' produce Python bools (Requirement 11.5) ---

@settings(max_examples=100)
@given(key=_key, bool_val=st.booleans())
def test_bool_values_produce_python_bool(key: str, bool_val: bool) -> None:
    """'true' and 'false' annotation values are parsed as Python booleans."""
    value_str = "true" if bool_val else "false"
    annotations, errors = _parse_single(key, value_str)
    assert errors == []
    assert len(annotations) == 1
    result = annotations[0].value
    assert isinstance(result, bool), f"Expected bool, got {type(result)}"
    assert result is bool_val


# --- Property 21c: Dict syntax produces Python dicts (Requirement 11.6) ---

@settings(max_examples=100)
@given(
    key=_key,
    mode=st.sampled_from(["append", "replace", "merge", "truncate_insert"]),
)
def test_dict_syntax_produces_python_dict(key: str, mode: str) -> None:
    """{key: value} annotation values are parsed as Python dicts."""
    value_str = f"{{mode: {mode}}}"
    annotations, errors = _parse_single(key, value_str)
    assert errors == []
    assert len(annotations) == 1
    result = annotations[0].value
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"
    assert result.get("mode") == mode


# --- Property 21d: Plain strings remain strings (not misclassified) ---

@settings(max_examples=100)
@given(
    key=_key,
    value=st.from_regex(r"[a-z][a-z0-9_ ]{0,29}", fullmatch=True).filter(
        lambda s: s.strip()
        and not s.strip().startswith("[")
        and not s.strip().startswith("{")
        and s.strip() not in ("true", "false")
    ),
)
def test_plain_string_stays_string(key: str, value: str) -> None:
    """Plain string annotation values are not coerced to list, bool, or dict."""
    annotations, errors = _parse_single(key, value)
    assert errors == []
    assert len(annotations) == 1
    result = annotations[0].value
    assert isinstance(result, str), f"Expected str for '{value}', got {type(result)}"
