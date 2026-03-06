"""Property tests for write strategy parsing and validation.

Feature: rivet-config, Property 31: Write strategy parsing and validation
Validates: Requirements 19.1, 19.2, 19.4, 19.5
"""

from __future__ import annotations

from pathlib import Path

import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.models import WRITE_STRATEGY_MODES
from rivet_config.yaml_parser import YAMLParser

PARSER = YAMLParser()

_name = st.from_regex(r"[a-z][a-z0-9_]{0,20}", fullmatch=True)
_identifier = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)
_valid_mode = st.sampled_from(sorted(WRITE_STRATEGY_MODES))
# Invalid modes: non-empty strings that are not valid modes
_invalid_mode = st.text(min_size=1, max_size=20).filter(
    lambda s: s not in WRITE_STRATEGY_MODES and s.strip() == s and len(s) > 0
)


def _write_sink_yaml(tmp_path: Path, name: str, catalog: str, table: str, write_strategy: dict | None) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    data: dict = {"name": name, "type": "sink", "catalog": catalog, "table": table}
    if write_strategy is not None:
        data["write_strategy"] = write_strategy
    p = tmp_path / "joint.yaml"
    p.write_text(yaml.dump(data))
    return p


# --- Property 31a: valid write strategy modes are accepted ---

@settings(max_examples=100)
@given(
    name=_name,
    catalog=_identifier,
    table=_identifier,
    mode=_valid_mode,
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop_ws_valid")),
)
def test_valid_write_strategy_mode_accepted(name, catalog, table, mode, tmp_path):
    """Property 31: valid write strategy modes produce no errors and are preserved."""
    p = _write_sink_yaml(tmp_path, name, catalog, table, {"mode": mode})
    decl, errors = PARSER.parse(p)

    assert not errors, f"Unexpected errors for valid mode '{mode}': {errors}"
    assert decl is not None
    assert decl.write_strategy is not None
    assert decl.write_strategy.mode == mode


# --- Property 31b: invalid write strategy modes produce errors ---

@settings(max_examples=100)
@given(
    name=_name,
    catalog=_identifier,
    table=_identifier,
    mode=_invalid_mode,
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop_ws_invalid")),
)
def test_invalid_write_strategy_mode_rejected(name, catalog, table, mode, tmp_path):
    """Property 31: invalid write strategy modes produce an error identifying the file and mode."""
    p = _write_sink_yaml(tmp_path, name, catalog, table, {"mode": mode})
    decl, errors = PARSER.parse(p)

    assert decl is None
    assert len(errors) >= 1
    # Error must reference the source file and the invalid mode
    error_messages = " ".join(e.message for e in errors)
    assert str(p) in " ".join(str(e.source_file) for e in errors)
    assert mode in error_messages


# --- Property 31c: omitted write_strategy defaults to append for sinks ---

@settings(max_examples=100)
@given(
    name=_name,
    catalog=_identifier,
    table=_identifier,
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop_ws_default")),
)
def test_omitted_write_strategy_defaults_to_append(name, catalog, table, tmp_path):
    """Property 31: omitted write_strategy on a sink defaults to mode='append'."""
    p = _write_sink_yaml(tmp_path, name, catalog, table, None)
    decl, errors = PARSER.parse(p)

    assert not errors, f"Unexpected errors: {errors}"
    assert decl is not None
    assert decl.write_strategy is not None
    assert decl.write_strategy.mode == "append"


# --- Property 31d: write_strategy extra options are preserved ---

@settings(max_examples=100)
@given(
    name=_name,
    catalog=_identifier,
    table=_identifier,
    mode=_valid_mode,
    option_key=_identifier,
    option_value=st.text(min_size=1, max_size=20),
    tmp_path=st.builds(Path, st.just("/tmp/rivet_prop_ws_options")),
)
def test_write_strategy_extra_options_preserved(name, catalog, table, mode, option_key, option_value, tmp_path):
    """Property 31: extra options in write_strategy are preserved in WriteStrategyDecl.options."""
    ws = {"mode": mode, option_key: option_value}
    p = _write_sink_yaml(tmp_path, name, catalog, table, ws)
    decl, errors = PARSER.parse(p)

    assert not errors, f"Unexpected errors: {errors}"
    assert decl is not None
    assert decl.write_strategy is not None
    assert decl.write_strategy.mode == mode
    assert decl.write_strategy.options.get(option_key) == option_value
