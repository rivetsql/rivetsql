"""Property test for SQLParser — produces JointDeclaration with SQL body.

Feature: rivet-config, Property 34: SQL declaration produces JointDeclaration with SQL body
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

_sql_line = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",), blacklist_characters="\n\r"),
    min_size=1,
    max_size=80,
).filter(lambda s: not s.strip().startswith("-- rivet:") and s.strip() != "")

_sql_body = st.lists(_sql_line, min_size=1, max_size=10).map(lambda lines: "\n".join(lines))

_optional_str = st.one_of(st.none(), _identifier)


def _write_sql(tmp_path: Path, content: str, name: str = "joint.sql") -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    p = tmp_path / name
    p.write_text(content)
    return p


# --- Property 34: SQL joint produces JointDeclaration with sql field set ---

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

    # sql field must be set to the SQL body
    assert decl.sql == sql_body.strip()

    # annotation fields are mapped to declaration fields
    assert decl.name == name
    if engine is not None:
        assert decl.engine == engine
    if description is not None:
        assert decl.description == description

    # source_path is set
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
