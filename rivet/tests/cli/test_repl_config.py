"""Tests for ReplConfig and FormatterConfig parsing.

Property 22: REPL config parsing
Validates: Requirements 38.1
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_cli.repl.config import FormatterConfig, ReplConfig

# --- Hypothesis strategies ---

_theme = st.one_of(
    st.sampled_from(["rivet", "rivet-light", "harlequin", "monokai", "dracula", "nord"]),
    st.text(min_size=1, max_size=30),
)
_keymap = st.one_of(
    st.sampled_from(["vscode", "vim", "emacs", "custom"]),
    st.text(min_size=1, max_size=20),
)
_positive_int = st.integers(min_value=1, max_value=100_000)
_bool = st.booleans()

_formatter_dict = st.fixed_dictionaries(
    {},
    optional={
        "indent": st.integers(min_value=1, max_value=8),
        "uppercase_keywords": _bool,
        "trailing_commas": _bool,
        "max_line_length": st.integers(min_value=40, max_value=200),
    },
)

_repl_dict = st.fixed_dictionaries(
    {},
    optional={
        "theme": _theme,
        "keymap": _keymap,
        "max_results": _positive_int,
        "autocomplete": _bool,
        "file_watch": _bool,
        "debounce_ms": _positive_int,
        "editor_cache": _bool,
        "show_line_numbers": _bool,
        "show_minimap": _bool,
        "tab_size": st.integers(min_value=1, max_value=8),
        "word_wrap": _bool,
        "formatter": _formatter_dict,
    },
)


# --- Property 22: REPL config parsing ---

@settings(max_examples=200)
@given(raw=_repl_dict)
def test_property_repl_config_from_dict_preserves_provided_values(raw: dict) -> None:
    """Property 22: every key provided in the dict is reflected in the parsed config."""
    cfg = ReplConfig.from_dict(raw)

    if "theme" in raw:
        assert cfg.theme == str(raw["theme"])
    if "keymap" in raw:
        assert cfg.keymap == str(raw["keymap"])
    if "max_results" in raw:
        assert cfg.max_results == int(raw["max_results"])
    if "autocomplete" in raw:
        assert cfg.autocomplete == bool(raw["autocomplete"])
    if "file_watch" in raw:
        assert cfg.file_watch == bool(raw["file_watch"])
    if "debounce_ms" in raw:
        assert cfg.debounce_ms == int(raw["debounce_ms"])
    if "editor_cache" in raw:
        assert cfg.editor_cache == bool(raw["editor_cache"])
    if "show_line_numbers" in raw:
        assert cfg.show_line_numbers == bool(raw["show_line_numbers"])
    if "show_minimap" in raw:
        assert cfg.show_minimap == bool(raw["show_minimap"])
    if "tab_size" in raw:
        assert cfg.tab_size == int(raw["tab_size"])
    if "word_wrap" in raw:
        assert cfg.word_wrap == bool(raw["word_wrap"])


@settings(max_examples=200)
@given(raw=_repl_dict)
def test_property_repl_config_missing_keys_use_defaults(raw: dict) -> None:
    """Property 22: keys absent from the dict fall back to the documented defaults."""
    defaults = ReplConfig()
    cfg = ReplConfig.from_dict(raw)

    if "theme" not in raw:
        assert cfg.theme == defaults.theme
    if "keymap" not in raw:
        assert cfg.keymap == defaults.keymap
    if "max_results" not in raw:
        assert cfg.max_results == defaults.max_results
    if "autocomplete" not in raw:
        assert cfg.autocomplete == defaults.autocomplete
    if "file_watch" not in raw:
        assert cfg.file_watch == defaults.file_watch
    if "debounce_ms" not in raw:
        assert cfg.debounce_ms == defaults.debounce_ms
    if "editor_cache" not in raw:
        assert cfg.editor_cache == defaults.editor_cache
    if "show_line_numbers" not in raw:
        assert cfg.show_line_numbers == defaults.show_line_numbers
    if "show_minimap" not in raw:
        assert cfg.show_minimap == defaults.show_minimap
    if "tab_size" not in raw:
        assert cfg.tab_size == defaults.tab_size
    if "word_wrap" not in raw:
        assert cfg.word_wrap == defaults.word_wrap


@settings(max_examples=200)
@given(raw=_repl_dict)
def test_property_repl_config_is_frozen(raw: dict) -> None:
    """Property 22: ReplConfig is always a frozen dataclass (immutable)."""
    cfg = ReplConfig.from_dict(raw)
    with pytest.raises((AttributeError, TypeError)):
        cfg.theme = "mutated"  # type: ignore[misc]


@settings(max_examples=200)
@given(raw=_formatter_dict)
def test_property_formatter_config_preserves_provided_values(raw: dict) -> None:
    """Property 22: FormatterConfig.from_dict preserves all provided values."""
    cfg = FormatterConfig.from_dict(raw)

    if "indent" in raw:
        assert cfg.indent == int(raw["indent"])
    if "uppercase_keywords" in raw:
        assert cfg.uppercase_keywords == bool(raw["uppercase_keywords"])
    if "trailing_commas" in raw:
        assert cfg.trailing_commas == bool(raw["trailing_commas"])
    if "max_line_length" in raw:
        assert cfg.max_line_length == int(raw["max_line_length"])


@settings(max_examples=100)
@given(
    repl_raw=_repl_dict,
    extra_keys=st.dictionaries(
        st.text(min_size=1, max_size=20).filter(
            lambda k: k not in {
                "theme", "keymap", "max_results", "autocomplete", "file_watch",
                "debounce_ms", "editor_cache", "show_line_numbers", "show_minimap",
                "tab_size", "word_wrap", "formatter",
            }
        ),
        st.integers(),
        max_size=3,
    ),
)
def test_property_repl_config_unknown_keys_ignored(repl_raw: dict, extra_keys: dict) -> None:
    """Property 22: unknown keys in the dict are silently ignored."""
    combined = {**repl_raw, **extra_keys}
    cfg_with_extras = ReplConfig.from_dict(combined)
    cfg_without_extras = ReplConfig.from_dict(repl_raw)
    assert cfg_with_extras == cfg_without_extras


@settings(max_examples=100)
@given(raw=_repl_dict)
def test_property_repl_config_from_rivet_yaml_under_repl_key(raw: dict) -> None:
    """Property 22: from_rivet_yaml extracts the `repl` key and parses it identically to from_dict."""
    cfg_direct = ReplConfig.from_dict(raw)
    cfg_via_yaml = ReplConfig.from_rivet_yaml({"repl": raw})
    assert cfg_direct == cfg_via_yaml


class TestFormatterConfigDefaults:
    def test_defaults(self) -> None:
        cfg = FormatterConfig()
        assert cfg.indent == 2
        assert cfg.uppercase_keywords is True
        assert cfg.trailing_commas is True
        assert cfg.max_line_length == 80

    def test_from_empty_dict(self) -> None:
        cfg = FormatterConfig.from_dict({})
        assert cfg == FormatterConfig()

    def test_from_dict_overrides(self) -> None:
        cfg = FormatterConfig.from_dict({
            "indent": 4,
            "uppercase_keywords": False,
            "trailing_commas": False,
            "max_line_length": 120,
        })
        assert cfg.indent == 4
        assert cfg.uppercase_keywords is False
        assert cfg.trailing_commas is False
        assert cfg.max_line_length == 120

    def test_from_dict_partial(self) -> None:
        cfg = FormatterConfig.from_dict({"indent": 4})
        assert cfg.indent == 4
        assert cfg.uppercase_keywords is True  # default preserved


class TestReplConfigDefaults:
    def test_defaults(self) -> None:
        cfg = ReplConfig()
        assert cfg.theme == "rivet"
        assert cfg.keymap == "vscode"
        assert cfg.max_results == 10_000
        assert cfg.autocomplete is True
        assert cfg.file_watch is True
        assert cfg.debounce_ms == 500
        assert cfg.editor_cache is True
        assert cfg.show_line_numbers is True
        assert cfg.show_minimap is False
        assert cfg.tab_size == 2
        assert cfg.word_wrap is False
        assert cfg.formatter == FormatterConfig()

    def test_from_empty_dict(self) -> None:
        cfg = ReplConfig.from_dict({})
        assert cfg == ReplConfig()

    def test_from_dict_overrides_all(self) -> None:
        cfg = ReplConfig.from_dict({
            "theme": "rivet-light",
            "keymap": "vim",
            "max_results": 5000,
            "autocomplete": False,
            "file_watch": False,
            "debounce_ms": 300,
            "editor_cache": False,
            "show_line_numbers": False,
            "show_minimap": True,
            "tab_size": 4,
            "word_wrap": True,
            "formatter": {"indent": 4, "uppercase_keywords": False},
        })
        assert cfg.theme == "rivet-light"
        assert cfg.keymap == "vim"
        assert cfg.max_results == 5000
        assert cfg.autocomplete is False
        assert cfg.file_watch is False
        assert cfg.debounce_ms == 300
        assert cfg.editor_cache is False
        assert cfg.show_line_numbers is False
        assert cfg.show_minimap is True
        assert cfg.tab_size == 4
        assert cfg.word_wrap is True
        assert cfg.formatter.indent == 4
        assert cfg.formatter.uppercase_keywords is False

    def test_unknown_keys_ignored(self) -> None:
        cfg = ReplConfig.from_dict({"theme": "rivet", "unknown_key": "value"})
        assert cfg.theme == "rivet"

    def test_formatter_non_dict_falls_back_to_default(self) -> None:
        cfg = ReplConfig.from_dict({"formatter": "invalid"})
        assert cfg.formatter == FormatterConfig()


class TestReplConfigFromRivetYaml:
    def test_no_repl_key_returns_defaults(self) -> None:
        cfg = ReplConfig.from_rivet_yaml({})
        assert cfg == ReplConfig()

    def test_repl_key_parsed(self) -> None:
        cfg = ReplConfig.from_rivet_yaml({"repl": {"theme": "rivet-light", "tab_size": 4}})
        assert cfg.theme == "rivet-light"
        assert cfg.tab_size == 4
        assert cfg.keymap == "vscode"  # default

    def test_repl_key_non_dict_returns_defaults(self) -> None:
        cfg = ReplConfig.from_rivet_yaml({"repl": "invalid"})
        assert cfg == ReplConfig()

    def test_repl_key_with_formatter(self) -> None:
        cfg = ReplConfig.from_rivet_yaml({
            "repl": {
                "formatter": {"indent": 4, "max_line_length": 100},
            }
        })
        assert cfg.formatter.indent == 4
        assert cfg.formatter.max_line_length == 100
        assert cfg.formatter.uppercase_keywords is True  # default

    def test_frozen_dataclass(self) -> None:
        cfg = ReplConfig()
        with pytest.raises((AttributeError, TypeError)):
            cfg.theme = "other"  # type: ignore[misc]
