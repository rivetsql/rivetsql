"""Property test for SQLParser — annotation/body split.

Feature: rivet-config, Property 20: SQL annotation extraction splits annotations from body
Validates: Requirements 11.1, 11.2, 11.3, 11.10
"""

from __future__ import annotations

from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.sql_parser import SQLParser

PARSER = SQLParser()

# --- Strategies ---

_identifier = st.from_regex(r"[a-z][a-z0-9_]{0,20}", fullmatch=True)
_annotation_value = st.one_of(
    _identifier,
    st.just("true"),
    st.just("false"),
)
# Annotation keys that are recognized and don't require extra fields
_safe_annotation_keys = st.sampled_from(["engine", "description", "fusion_strategy", "materialization_strategy"])

# SQL body lines: non-empty, not starting with -- rivet:
_sql_line = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",), blacklist_characters="\n\r"),
    min_size=1,
    max_size=60,
).filter(lambda s: not s.strip().startswith("-- rivet:") and s.strip() != "")

_sql_body = st.lists(_sql_line, min_size=1, max_size=5).map(lambda lines: "\n".join(lines))


def _write_sql(tmp_path: Path, content: str, name: str = "joint.sql") -> Path:
    p = tmp_path / name
    p.write_text(content)
    return p


# --- Property 20: annotations appear before SQL body ---

@settings(max_examples=100)
@given(
    name=_identifier,
    extra_key=_safe_annotation_keys,
    extra_value=_annotation_value,
    sql_body=_sql_body,
    tmp_path=st.builds(lambda: Path("/tmp/rivet_prop_sql_split")),
)
def test_annotations_split_from_body(name, extra_key, extra_value, sql_body, tmp_path):
    """Property 20: annotations at top are extracted; everything after is the SQL body."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    content = (
        f"-- rivet:name: {name}\n"
        f"-- rivet:{extra_key}: {extra_value}\n"
        f"{sql_body}"
    )
    p = _write_sql(tmp_path, content)
    decl, errors = PARSER.parse(p)

    # No errors expected for valid input
    assert errors == [], f"Unexpected errors: {errors}"
    assert decl is not None

    # Name annotation is extracted correctly
    assert decl.name == name

    # SQL body is everything after the last annotation line, stripped
    assert decl.sql == sql_body.strip()


@settings(max_examples=100)
@given(
    name=_identifier,
    sql_body=_sql_body,
    tmp_path=st.builds(lambda: Path("/tmp/rivet_prop_sql_no_annotations")),
)
def test_no_annotations_entire_file_is_body(name, sql_body, tmp_path):
    """Property 20: when there are no annotations, the entire file is the SQL body (name from stem)."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    p = _write_sql(tmp_path, sql_body, name=f"{name}.sql")
    decl, errors = PARSER.parse(p)

    assert errors == [], f"Unexpected errors: {errors}"
    assert decl is not None
    assert decl.name == name
    assert decl.sql == sql_body.strip()


@settings(max_examples=100)
@given(
    name=_identifier,
    annotation_count=st.integers(min_value=1, max_value=5),
    sql_body=_sql_body,
    tmp_path=st.builds(lambda: Path("/tmp/rivet_prop_sql_multi_annotations")),
)
def test_multiple_annotations_all_extracted_before_body(name, annotation_count, sql_body, tmp_path):
    """Property 20: all annotation lines before SQL are extracted; SQL body starts after last annotation."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    # Build N extra engine annotations (safe, no side effects)
    extra_lines = "\n".join(
        f"-- rivet:engine: engine{i}" for i in range(annotation_count)
    )
    content = f"-- rivet:name: {name}\n{extra_lines}\n{sql_body}"
    p = _write_sql(tmp_path, content)
    decl, errors = PARSER.parse(p)

    assert errors == [], f"Unexpected errors: {errors}"
    assert decl is not None
    assert decl.name == name
    # SQL body is everything after annotations
    assert decl.sql == sql_body.strip()
    # The last engine annotation wins (overwritten in fields dict)
    assert decl.engine == f"engine{annotation_count - 1}"
