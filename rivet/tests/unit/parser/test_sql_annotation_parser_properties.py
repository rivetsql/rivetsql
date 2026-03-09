"""Property tests for SQLParser — annotation/body split and joint declaration.

Merged from:
- test_sql_parser_annotation_body_property.py (Property 20)
- test_sql_parser_joint_declaration_property.py (Property 34)

Property 20: SQL annotation extraction splits annotations from body
  Validates: Requirements 11.1, 11.2, 11.3, 11.10

Property 34: SQL declaration produces JointDeclaration with SQL body
  Validates: Requirements 18.3, 11.2
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
_safe_annotation_keys = st.sampled_from(["engine", "description", "fusion_strategy", "materialization_strategy"])

_sql_line = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",), blacklist_characters="\n\r"),
    min_size=1,
    max_size=60,
).filter(lambda s: not s.strip().startswith("-- rivet:") and s.strip() != "")

_sql_body = st.lists(_sql_line, min_size=1, max_size=5).map(lambda lines: "\n".join(lines))
_optional_str = st.one_of(st.none(), _identifier)


def _write_sql(tmp_path: Path, content: str, name: str = "joint.sql") -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    p = tmp_path / name
    p.write_text(content)
    return p


# ---------------------------------------------------------------------------
# Property 20: annotations appear before SQL body
# ---------------------------------------------------------------------------


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

    assert errors == [], f"Unexpected errors: {errors}"
    assert decl is not None
    assert decl.name == name
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
    extra_lines = "\n".join(
        f"-- rivet:engine: engine{i}" for i in range(annotation_count)
    )
    content = f"-- rivet:name: {name}\n{extra_lines}\n{sql_body}"
    p = _write_sql(tmp_path, content)
    decl, errors = PARSER.parse(p)

    assert errors == [], f"Unexpected errors: {errors}"
    assert decl is not None
    assert decl.name == name
    assert decl.sql == sql_body.strip()
    assert decl.engine == f"engine{annotation_count - 1}"


# ---------------------------------------------------------------------------
# Property 34: SQL joint produces JointDeclaration with sql field set
# ---------------------------------------------------------------------------


@settings(max_examples=100)
@given(
    name=_identifier,
    sql_body=_sql_body,
    engine=_optional_str,
    description=_optional_str,
    tmp_path=st.builds(lambda: Path("/tmp/rivet_prop34_sql_decl")),
)
def test_sql_declaration_has_sql_body(name, sql_body, engine, description, tmp_path):
    """Property 34: SQL file produces JointDeclaration with sql field = file body after annotations."""
    lines = [f"-- rivet:name: {name}"]
    if engine is not None:
        lines.append(f"-- rivet:engine: {engine}")
    if description is not None:
        lines.append(f"-- rivet:description: {description}")
    lines.append(sql_body)
    content = "\n".join(lines)

    p = _write_sql(tmp_path, content)
    decl, errors = PARSER.parse(p)

    assert errors == [], f"Unexpected errors: {errors}"
    assert decl is not None
    assert decl.sql == sql_body.strip()
    assert decl.name == name
    if engine is not None:
        assert decl.engine == engine
    if description is not None:
        assert decl.description == description
    assert decl.source_path == p


@settings(max_examples=100)
@given(
    name=_identifier,
    sql_body=_sql_body,
    upstream=st.lists(_identifier, min_size=1, max_size=4),
    tags=st.lists(_identifier, min_size=1, max_size=4),
    tmp_path=st.builds(lambda: Path("/tmp/rivet_prop34_sql_lists")),
)
def test_sql_declaration_list_annotations_mapped(name, sql_body, upstream, tags, tmp_path):
    """Property 34: list-valued annotations (upstream, tags) are mapped to declaration fields."""
    upstream_str = "[" + ", ".join(upstream) + "]"
    tags_str = "[" + ", ".join(tags) + "]"
    content = (
        f"-- rivet:name: {name}\n"
        f"-- rivet:upstream: {upstream_str}\n"
        f"-- rivet:tags: {tags_str}\n"
        f"{sql_body}"
    )
    p = _write_sql(tmp_path, content)
    decl, errors = PARSER.parse(p)

    assert errors == [], f"Unexpected errors: {errors}"
    assert decl is not None
    assert decl.sql == sql_body.strip()
    assert decl.upstream == upstream
    assert decl.tags == tags


@settings(max_examples=100)
@given(
    name=_identifier,
    sql_body=_sql_body,
    eager=st.booleans(),
    tmp_path=st.builds(lambda: Path("/tmp/rivet_prop34_sql_eager")),
)
def test_sql_declaration_bool_annotation_mapped(name, sql_body, eager, tmp_path):
    """Property 34: boolean annotation (eager) is mapped to declaration field."""
    eager_str = "true" if eager else "false"
    content = (
        f"-- rivet:name: {name}\n"
        f"-- rivet:eager: {eager_str}\n"
        f"{sql_body}"
    )
    p = _write_sql(tmp_path, content)
    decl, errors = PARSER.parse(p)

    assert errors == [], f"Unexpected errors: {errors}"
    assert decl is not None
    assert decl.sql == sql_body.strip()
    assert decl.eager == eager
