"""Tests for keymap support.

Validates: Requirements 28.1, 28.2, 28.3, 28.4
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_cli.repl.keymap import (
    _BUILTIN_KEYMAPS,
    _RVT_864,
    DEFAULT_KEYMAP,
    KeyBinding,
    Keymap,
    _keymap_from_dict,
    load_keymap,
)

# ---------------------------------------------------------------------------
# KeyBinding
# ---------------------------------------------------------------------------


class TestKeyBinding:
    def test_frozen(self):
        kb = KeyBinding(key="ctrl+q", action="quit")
        with pytest.raises((AttributeError, TypeError)):
            kb.key = "ctrl+x"  # type: ignore[misc]

    def test_defaults(self):
        kb = KeyBinding(key="ctrl+q", action="quit")
        assert kb.description == ""
        assert kb.show is True


# ---------------------------------------------------------------------------
# Keymap
# ---------------------------------------------------------------------------


class TestKeymap:
    def test_frozen(self):
        km = Keymap(name="test", bindings=())
        with pytest.raises((AttributeError, TypeError)):
            km.name = "other"  # type: ignore[misc]

    def test_get_key_found(self):
        km = Keymap(
            name="test",
            bindings=(KeyBinding(key="ctrl+q", action="quit"),),
        )
        assert km.get_key("quit") == "ctrl+q"

    def test_get_key_not_found(self):
        km = Keymap(name="test", bindings=())
        assert km.get_key("nonexistent") is None

    def test_as_textual_bindings(self):
        km = Keymap(
            name="test",
            bindings=(
                KeyBinding(key="ctrl+q", action="quit", description="Quit", show=True),
                KeyBinding(key="ctrl+b", action="toggle", description="Toggle", show=False),
            ),
        )
        result = km.as_textual_bindings()
        assert result == [
            ("ctrl+q", "quit", "Quit", True),
            ("ctrl+b", "toggle", "Toggle", False),
        ]


# ---------------------------------------------------------------------------
# Built-in keymaps
# ---------------------------------------------------------------------------


class TestBuiltinKeymaps:
    def test_vscode_is_default(self):
        assert DEFAULT_KEYMAP.name == "vscode"

    def test_all_builtin_names_present(self):
        assert set(_BUILTIN_KEYMAPS.keys()) == {"vscode", "vim", "emacs"}

    @pytest.mark.parametrize("name", ["vscode", "vim", "emacs"])
    def test_builtin_has_quit_binding(self, name):
        km = _BUILTIN_KEYMAPS[name]
        assert km.get_key("request_quit") is not None

    @pytest.mark.parametrize("name", ["vscode", "vim", "emacs"])
    def test_builtin_has_toggle_catalog(self, name):
        km = _BUILTIN_KEYMAPS[name]
        assert km.get_key("toggle_catalog") is not None

    @pytest.mark.parametrize("name", ["vscode", "vim", "emacs"])
    def test_builtin_has_run_all_sinks(self, name):
        km = _BUILTIN_KEYMAPS[name]
        assert km.get_key("run_all_sinks") is not None

    @pytest.mark.parametrize("name", ["vscode", "vim", "emacs"])
    def test_builtin_has_run_tests(self, name):
        km = _BUILTIN_KEYMAPS[name]
        assert km.get_key("run_tests") is not None

    @pytest.mark.parametrize("name", ["vscode", "vim", "emacs"])
    def test_builtin_has_save(self, name):
        km = _BUILTIN_KEYMAPS[name]
        assert km.get_key("save") is not None

    @pytest.mark.parametrize("name", ["vscode", "vim", "emacs"])
    def test_builtin_has_format_sql(self, name):
        km = _BUILTIN_KEYMAPS[name]
        assert km.get_key("format_sql") is not None

    @pytest.mark.parametrize("name", ["vscode", "vim", "emacs"])
    def test_builtin_has_diff_results(self, name):
        km = _BUILTIN_KEYMAPS[name]
        assert km.get_key("diff_results") is not None

    @pytest.mark.parametrize("name", ["vscode", "vim", "emacs"])
    def test_builtin_has_profile_data(self, name):
        km = _BUILTIN_KEYMAPS[name]
        assert km.get_key("profile_data") is not None

    @pytest.mark.parametrize("name", ["vscode", "vim", "emacs"])
    def test_builtin_has_show_query_plan(self, name):
        km = _BUILTIN_KEYMAPS[name]
        assert km.get_key("show_query_plan") is not None

    @pytest.mark.parametrize("name", ["vscode", "vim", "emacs"])
    def test_builtin_has_debug_step(self, name):
        km = _BUILTIN_KEYMAPS[name]
        assert km.get_key("debug_step") is not None

    @pytest.mark.parametrize("name", ["vscode", "vim", "emacs"])
    def test_builtin_has_debug_continue(self, name):
        km = _BUILTIN_KEYMAPS[name]
        assert km.get_key("debug_continue") is not None

    @pytest.mark.parametrize("name", ["vscode", "vim", "emacs"])
    def test_builtin_has_toggle_breakpoint(self, name):
        km = _BUILTIN_KEYMAPS[name]
        assert km.get_key("toggle_breakpoint") is not None


# ---------------------------------------------------------------------------
# load_keymap — built-in resolution
# ---------------------------------------------------------------------------


class TestLoadKeymapBuiltin:
    def test_vscode_no_error(self):
        km, err = load_keymap("vscode")
        assert km.name == "vscode"
        assert err is None

    def test_vim_no_error(self):
        km, err = load_keymap("vim")
        assert km.name == "vim"
        assert err is None

    def test_emacs_no_error(self):
        km, err = load_keymap("emacs")
        assert km.name == "emacs"
        assert err is None


# ---------------------------------------------------------------------------
# load_keymap — fallback with RVT-864
# ---------------------------------------------------------------------------


class TestLoadKeymapFallback:
    def test_unknown_name_returns_default(self):
        km, err = load_keymap("nonexistent_keymap_xyz")
        assert km is DEFAULT_KEYMAP
        assert err == _RVT_864

    def test_unknown_name_error_code_is_rvt_864(self):
        _, err = load_keymap("totally_unknown")
        assert err == "RVT-864"

    @given(
        name=st.text(min_size=1, max_size=30).filter(
            lambda s: s not in {"vscode", "vim", "emacs"}
        )
    )
    @settings(max_examples=50)
    def test_property_unknown_keymap_always_falls_back(self, name):
        """Property: any unknown keymap name → default keymap + RVT-864."""
        # Patch entry_points to return empty so no custom keymap is found
        with patch("importlib.metadata.entry_points", return_value=[]):
            km, err = load_keymap(name)
        assert km is DEFAULT_KEYMAP
        assert err == _RVT_864


# ---------------------------------------------------------------------------
# load_keymap — entry point loading
# ---------------------------------------------------------------------------


class TestLoadKeymapEntryPoint:
    def test_entry_point_keymap_loaded(self):
        """A valid Keymap returned by an entry point is used."""
        custom_km = Keymap(
            name="custom",
            bindings=(KeyBinding(key="ctrl+q", action="request_quit"),),
        )
        mock_ep = MagicMock()
        mock_ep.name = "custom"
        mock_ep.load.return_value = custom_km

        with patch("importlib.metadata.entry_points", return_value=[mock_ep]):
            km, err = load_keymap("custom")

        assert km is custom_km
        assert err is None

    def test_entry_point_callable_factory(self):
        """An entry point that is a callable factory returning a Keymap is supported."""
        custom_km = Keymap(
            name="custom",
            bindings=(KeyBinding(key="ctrl+q", action="request_quit"),),
        )

        def factory():
            return custom_km

        mock_ep = MagicMock()
        mock_ep.name = "custom"
        mock_ep.load.return_value = factory

        with patch("importlib.metadata.entry_points", return_value=[mock_ep]):
            km, err = load_keymap("custom")

        assert km is custom_km
        assert err is None

    def test_entry_point_dict_converted(self):
        """An entry point returning a dict is converted to a Keymap."""
        mock_ep = MagicMock()
        mock_ep.name = "custom"
        mock_ep.load.return_value = {"request_quit": "ctrl+q", "save": "ctrl+s"}

        with patch("importlib.metadata.entry_points", return_value=[mock_ep]):
            km, err = load_keymap("custom")

        assert km.name == "custom"
        assert err is None
        assert km.get_key("request_quit") == "ctrl+q"
        assert km.get_key("save") == "ctrl+s"

    def test_entry_point_load_failure_falls_back(self):
        """If entry point load() raises, fall back to default with RVT-864."""
        mock_ep = MagicMock()
        mock_ep.name = "broken"
        mock_ep.load.side_effect = RuntimeError("load failed")

        with patch("importlib.metadata.entry_points", return_value=[mock_ep]):
            km, err = load_keymap("broken")

        assert km is DEFAULT_KEYMAP
        assert err == _RVT_864

    def test_entry_point_wrong_name_not_used(self):
        """Entry point with a different name is not used."""
        mock_ep = MagicMock()
        mock_ep.name = "other_keymap"
        mock_ep.load.return_value = Keymap(name="other_keymap", bindings=())

        with patch("importlib.metadata.entry_points", return_value=[mock_ep]):
            km, err = load_keymap("wanted_keymap")

        assert km is DEFAULT_KEYMAP
        assert err == _RVT_864


# ---------------------------------------------------------------------------
# _keymap_from_dict
# ---------------------------------------------------------------------------


class TestKeymapFromDict:
    def test_basic_conversion(self):
        km = _keymap_from_dict("test", {"quit": "ctrl+q", "save": "ctrl+s"})
        assert km.name == "test"
        assert km.get_key("quit") == "ctrl+q"
        assert km.get_key("save") == "ctrl+s"

    def test_empty_dict(self):
        km = _keymap_from_dict("empty", {})
        assert km.name == "empty"
        assert km.bindings == ()

    def test_non_string_values_skipped(self):
        km = _keymap_from_dict("test", {"quit": "ctrl+q", "bad": 123})  # type: ignore[dict-item]
        assert km.get_key("quit") == "ctrl+q"
        assert km.get_key("bad") is None
