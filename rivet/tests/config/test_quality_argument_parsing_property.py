"""Property test for QualityParser — argument parsing.

Feature: rivet-config, Property 24: Quality check argument parsing
Validates: Requirements 12.3, 12.4, 12.5
"""

from __future__ import annotations

from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.annotations import ParsedAnnotation
from rivet_config.quality import QualityParser

PARSER = QualityParser()
FILE_PATH = Path("test.sql")

# Strategies for identifiers (column names, keyword keys)
_identifier = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)

# Strategies for simple string values (no commas, brackets, equals)
_simple_value = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)

# Strategy for a list of 1–4 identifiers
_id_list = st.lists(_identifier, min_size=1, max_size=4)

# Strategy for integer keyword values
_int_value = st.integers(min_value=0, max_value=9999)


@settings(max_examples=100)
@given(columns=_id_list)
def test_positional_args_become_columns(columns: list[str]) -> None:
    """Property 24: Positional arguments in a quality annotation become config['columns']."""
    args = ", ".join(columns)
    annotation = ParsedAnnotation(key="assert", value=f"not_null({args})", line_number=1)
    checks, errors = PARSER.parse_sql_annotations([annotation], FILE_PATH)
    assert not errors, f"Unexpected errors: {errors}"
    assert len(checks) == 1
    assert checks[0].config["columns"] == columns


@settings(max_examples=100)
@given(
    key=_identifier.filter(lambda k: k not in ("severity", "columns")),
    value=_simple_value,
)
def test_keyword_arg_appears_in_config(key: str, value: str) -> None:
    """Property 24: A keyword argument key=value appears in config[key] = value."""
    annotation = ParsedAnnotation(
        key="assert", value=f"row_count({key}={value})", line_number=1
    )
    checks, errors = PARSER.parse_sql_annotations([annotation], FILE_PATH)
    assert not errors, f"Unexpected errors: {errors}"
    assert len(checks) == 1
    assert checks[0].config[key] == value


@settings(max_examples=100)
@given(items=_id_list)
def test_list_valued_keyword_arg_parsed_as_list(items: list[str]) -> None:
    """Property 24: values=[a, b, c] keyword argument is parsed as a Python list."""
    items_str = ", ".join(items)
    annotation = ParsedAnnotation(
        key="assert",
        value=f"accepted_values(column=status, values=[{items_str}])",
        line_number=1,
    )
    checks, errors = PARSER.parse_sql_annotations([annotation], FILE_PATH)
    assert not errors, f"Unexpected errors: {errors}"
    assert len(checks) == 1
    assert checks[0].config["values"] == items


@settings(max_examples=100)
@given(
    columns=_id_list,
    min_val=_int_value,
    max_val=_int_value,
)
def test_mixed_positional_and_keyword_args(
    columns: list[str], min_val: int, max_val: int
) -> None:
    """Property 24: Positional and keyword args coexist correctly in config."""
    positional = ", ".join(columns)
    annotation = ParsedAnnotation(
        key="assert",
        value=f"not_null({positional}, min={min_val}, max={max_val})",
        line_number=1,
    )
    checks, errors = PARSER.parse_sql_annotations([annotation], FILE_PATH)
    assert not errors, f"Unexpected errors: {errors}"
    assert len(checks) == 1
    assert checks[0].config["columns"] == columns
    assert checks[0].config["min"] == min_val
    assert checks[0].config["max"] == max_val
