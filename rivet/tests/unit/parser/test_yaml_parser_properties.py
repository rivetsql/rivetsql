"""Property tests for YAMLParser — round-trip.

Feature: rivet-config, Property 15: YAML joint declaration round-trip
Validates: Requirements 9.1, 18.1, 18.2, 18.4
"""

from __future__ import annotations

from pathlib import Path

import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.models import WRITE_STRATEGY_MODES, ColumnDecl
from rivet_config.yaml_parser import YAMLParser

PARSER = YAMLParser()

# --- Strategies ---

_name = st.from_regex(r"[a-z][a-z0-9_]{0,30}", fullmatch=True)
_identifier = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)
_nonempty_str = st.text(min_size=1, max_size=40).filter(str.strip)

# Column entries: plain string or single-key mapping
_col_plain = _identifier.map(lambda s: s)
_col_mapping = st.fixed_dictionaries({"alias": _identifier, "expr": _nonempty_str}).map(
    lambda d: {d["alias"]: d["expr"]}
)
_column_entry = st.one_of(_col_plain, _col_mapping)
_columns = st.lists(_column_entry, min_size=1, max_size=5)

_write_strategy_mode = st.sampled_from(sorted(WRITE_STRATEGY_MODES))


def _write_yaml_file(tmp_path: Path, data: dict) -> Path:
    p = tmp_path / "joint.yaml"
    p.write_text(yaml.dump(data))
    return p


# --- Property 15a: source joint round-trip ---

@settings(max_examples=100)
@given(
    name=_name,
    catalog=_identifier,
    columns=st.one_of(st.none(), _columns),
    engine=st.one_of(st.none(), _identifier),
    eager=st.booleans(),
    upstream=st.one_of(st.none(), st.lists(_identifier, max_size=3)),
    tags=st.one_of(st.none(), st.lists(_identifier, max_size=3)),
    description=st.one_of(st.none(), _nonempty_str),
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop_test_source")),
)
def test_source_joint_round_trip(
    name, catalog, columns, engine, eager, upstream, tags, description, tmp_path
):
    """Property 15: source joint fields are preserved after parsing."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    data: dict = {"name": name, "type": "source", "catalog": catalog}
    if columns is not None:
        data["columns"] = columns
    if engine is not None:
        data["engine"] = engine
    if eager:
        data["eager"] = eager
    if upstream is not None:
        data["upstream"] = upstream
    if tags is not None:
        data["tags"] = tags
    if description is not None:
        data["description"] = description

    p = _write_yaml_file(tmp_path, data)
    decl, errors = PARSER.parse(p)

    assert not errors, f"Unexpected errors: {errors}"
    assert decl is not None
    assert decl.name == name
    assert decl.joint_type == "source"
    assert decl.catalog == catalog
    assert decl.source_path == p
    assert decl.eager is bool(eager)

    if columns is not None:
        assert decl.columns is not None
        assert len(decl.columns) == len(columns)
        for i, entry in enumerate(columns):
            if isinstance(entry, str):
                assert decl.columns[i] == ColumnDecl(name=entry, expression=None)
            else:
                col_name, expr = next(iter(entry.items()))
                assert decl.columns[i] == ColumnDecl(name=str(col_name), expression=str(expr))
    else:
        assert decl.columns is None

    if engine is not None:
        assert decl.engine == engine
    if upstream is not None:
        assert decl.upstream == upstream
    if tags is not None:
        assert decl.tags == tags
    if description is not None:
        assert decl.description == description


# --- Property 15b: sql joint round-trip ---

@settings(max_examples=100)
@given(
    name=_name,
    sql_body=_nonempty_str,
    engine=st.one_of(st.none(), _identifier),
    upstream=st.one_of(st.none(), st.lists(_identifier, max_size=3)),
    fusion_strategy=st.one_of(st.none(), _identifier),
    materialization_strategy=st.one_of(st.none(), _identifier),
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop_test_sql")),
)
def test_sql_joint_round_trip(
    name, sql_body, engine, upstream, fusion_strategy, materialization_strategy, tmp_path
):
    """Property 15: sql joint fields are preserved after parsing."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    data: dict = {"name": name, "type": "sql", "sql": sql_body}
    if engine is not None:
        data["engine"] = engine
    if upstream is not None:
        data["upstream"] = upstream
    if fusion_strategy is not None:
        data["fusion_strategy"] = fusion_strategy
    if materialization_strategy is not None:
        data["materialization_strategy"] = materialization_strategy

    p = _write_yaml_file(tmp_path, data)
    decl, errors = PARSER.parse(p)

    assert not errors, f"Unexpected errors: {errors}"
    assert decl is not None
    assert decl.name == name
    assert decl.joint_type == "sql"
    assert decl.sql == sql_body
    assert decl.source_path == p

    if engine is not None:
        assert decl.engine == engine
    if upstream is not None:
        assert decl.upstream == upstream
    if fusion_strategy is not None:
        assert decl.fusion_strategy == fusion_strategy
    if materialization_strategy is not None:
        assert decl.materialization_strategy == materialization_strategy


# --- Property 15c: sink joint round-trip (including write_strategy) ---

@settings(max_examples=100)
@given(
    name=_name,
    catalog=_identifier,
    table=_identifier,
    mode=st.one_of(st.none(), _write_strategy_mode),
    columns=st.one_of(st.none(), _columns),
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop_test_sink")),
)
def test_sink_joint_round_trip(name, catalog, table, mode, columns, tmp_path):
    """Property 15: sink joint fields are preserved after parsing, write_strategy mode preserved."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    data: dict = {"name": name, "type": "sink", "catalog": catalog, "table": table}
    if mode is not None:
        data["write_strategy"] = {"mode": mode}
    if columns is not None:
        data["columns"] = columns

    p = _write_yaml_file(tmp_path, data)
    decl, errors = PARSER.parse(p)

    assert not errors, f"Unexpected errors: {errors}"
    assert decl is not None
    assert decl.name == name
    assert decl.joint_type == "sink"
    assert decl.catalog == catalog
    assert decl.table == table
    assert decl.source_path == p

    expected_mode = mode if mode is not None else "append"
    assert decl.write_strategy is not None
    assert decl.write_strategy.mode == expected_mode

    if columns is not None:
        assert decl.columns is not None
        assert len(decl.columns) == len(columns)


# --- Property 15d: python joint round-trip ---

@settings(max_examples=100)
@given(
    name=_name,
    function=st.from_regex(r"[a-z][a-z0-9_]{0,10}\.[a-z][a-z0-9_]{0,10}", fullmatch=True),
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop_test_python")),
)
def test_python_joint_round_trip(name, function, tmp_path):
    """Property 15: python joint fields are preserved after parsing."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    data: dict = {"name": name, "type": "python", "function": function}

    p = _write_yaml_file(tmp_path, data)
    decl, errors = PARSER.parse(p)

    assert not errors, f"Unexpected errors: {errors}"
    assert decl is not None
    assert decl.name == name
    assert decl.joint_type == "python"
    assert decl.function == function
    assert decl.source_path == p


# --- Property 15e: column order is preserved ---

@settings(max_examples=100)
@given(
    name=_name,
    col_names=st.lists(_identifier, min_size=1, max_size=10),
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop_test_col_order")),
)
def test_column_order_preserved(name, col_names, tmp_path):
    """Property 15 (column order): column order is preserved exactly as declared."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    data: dict = {"name": name, "type": "source", "catalog": "pg", "columns": col_names}

    p = _write_yaml_file(tmp_path, data)
    decl, errors = PARSER.parse(p)

    assert not errors, f"Unexpected errors: {errors}"
    assert decl is not None
    assert decl.columns is not None
    assert [c.name for c in decl.columns] == col_names


# --- Property 18: Unrecognized YAML keys produce errors ---

@settings(max_examples=100)
@given(
    name=_name,
    unknown_keys=st.lists(
        st.from_regex(r"[a-z][a-z0-9_]{1,15}", fullmatch=True).filter(
            lambda k: k not in {
                "name", "type", "sql", "columns", "filter", "catalog", "engine",
                "eager", "upstream", "tags", "description", "table", "write_strategy",
                "function", "fusion_strategy", "materialization_strategy", "quality",
            }
        ),
        min_size=1,
        max_size=3,
        unique=True,
    ),
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop_test_unrecognized")),
)
def test_unrecognized_keys_produce_errors(name, unknown_keys, tmp_path):
    """Feature: rivet-config, Property 18: Unrecognized YAML keys produce errors.
    Validates: Requirements 9.9
    """
    tmp_path.mkdir(parents=True, exist_ok=True)
    data: dict = {"name": name, "type": "sql", "sql": "SELECT 1"}
    for k in unknown_keys:
        data[k] = "value"

    p = _write_yaml_file(tmp_path, data)
    _, errors = PARSER.parse(p)

    assert errors, "Expected errors for unrecognized keys but got none"
    all_messages = " ".join(e.message for e in errors)
    for k in unknown_keys:
        assert k in all_messages, f"Expected unrecognized key '{k}' to appear in errors"
