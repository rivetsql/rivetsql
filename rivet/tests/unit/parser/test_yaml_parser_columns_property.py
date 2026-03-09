"""Property tests for YAMLParser — column parsing order and types.

Feature: rivet-config, Property 19: Column parsing preserves order and distinguishes types
Validates: Requirements 10.1, 10.2, 10.5
"""

from __future__ import annotations

from pathlib import Path

import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.models import ColumnDecl
from rivet_config.yaml_parser import YAMLParser

PARSER = YAMLParser()

_identifier = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)
_expression = st.text(min_size=1, max_size=40).filter(lambda s: s.strip() and "\n" not in s)

# A column entry is either a plain string (pass-through) or a single-key mapping (alias→expr).
_plain_col = _identifier
_mapping_col = st.fixed_dictionaries({"name": _identifier, "expr": _expression}).map(
    lambda d: {d["name"]: d["expr"]}
)
_column_entry = st.one_of(_plain_col, _mapping_col)


def _write_yaml_file(tmp_path: Path, data: dict) -> Path:
    p = tmp_path / "joint.yaml"
    p.write_text(yaml.dump(data))
    return p


@settings(max_examples=100)
@given(
    name=st.from_regex(r"[a-z][a-z0-9_]{0,20}", fullmatch=True),
    columns=st.lists(_column_entry, min_size=1, max_size=10),
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop19_col")),
)
def test_column_order_and_types_preserved(name, columns, tmp_path):
    """Property 19: Column parsing preserves order and distinguishes plain strings from mappings.

    - Plain string entries produce ColumnDecl(name=s, expression=None)  [Req 10.1]
    - Single-key mapping entries produce ColumnDecl(name=key, expression=value)  [Req 10.2]
    - Output list length and order match the input  [Req 10.5]
    """
    tmp_path.mkdir(parents=True, exist_ok=True)
    data = {"name": name, "type": "source", "catalog": "pg", "columns": columns}
    p = _write_yaml_file(tmp_path, data)

    decl, errors = PARSER.parse(p)

    assert not errors, f"Unexpected errors: {errors}"
    assert decl is not None
    assert decl.columns is not None
    assert len(decl.columns) == len(columns), "Output length must match input length"

    for i, (entry, col_decl) in enumerate(zip(columns, decl.columns)):
        assert isinstance(col_decl, ColumnDecl), f"Entry {i} must be a ColumnDecl"
        if isinstance(entry, str):
            # Plain string → pass-through, expression must be None
            assert col_decl.name == entry, f"Entry {i}: name mismatch"
            assert col_decl.expression is None, f"Entry {i}: plain string must have expression=None"
        else:
            # Single-key mapping → alias + expression
            col_name, expr = next(iter(entry.items()))
            assert col_decl.name == str(col_name), f"Entry {i}: alias mismatch"
            assert col_decl.expression == str(expr), f"Entry {i}: expression mismatch"
