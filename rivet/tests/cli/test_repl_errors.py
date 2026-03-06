"""Tests for REPL error code registry and ErrorModal display.

Requirements: 34.1, 34.2, 34.3, 34.4, 34.5
"""

from __future__ import annotations

import dataclasses

import pytest

from rivet_cli.repl.errors import (
    REPL_ERROR_REGISTRY,
    RVT_860,
    RVT_861,
    RVT_862,
    RVT_863,
    RVT_864,
    RVT_865,
    RVT_866,
    RVT_867,
    ReplError,
    make_repl_error,
)

# ---------------------------------------------------------------------------
# ReplError dataclass
# ---------------------------------------------------------------------------


class TestReplError:
    def test_fields(self) -> None:
        err = ReplError(code="RVT-860", description="TUI init failure", remediation="Check terminal")
        assert err.code == "RVT-860"
        assert err.description == "TUI init failure"
        assert err.remediation == "Check terminal"

    def test_immutable(self) -> None:
        err = ReplError(code="RVT-860", description="desc", remediation="fix")
        with pytest.raises(dataclasses.FrozenInstanceError):
            err.code = "RVT-999"  # type: ignore[misc]

    def test_format_text(self) -> None:
        err = ReplError(code="RVT-860", description="TUI init failure", remediation="Check terminal")
        text = err.format_text()
        assert "RVT-860" in text
        assert "TUI init failure" in text
        assert "Check terminal" in text


# ---------------------------------------------------------------------------
# Error code constants — Requirement 34.2
# ---------------------------------------------------------------------------


class TestErrorCodeConstants:
    def test_all_codes_defined(self) -> None:
        assert RVT_860 == "RVT-860"
        assert RVT_861 == "RVT-861"
        assert RVT_862 == "RVT-862"
        assert RVT_863 == "RVT-863"
        assert RVT_864 == "RVT-864"
        assert RVT_865 == "RVT-865"
        assert RVT_866 == "RVT-866"
        assert RVT_867 == "RVT-867"

    def test_registry_covers_all_codes(self) -> None:
        for code in (RVT_860, RVT_861, RVT_862, RVT_863, RVT_864, RVT_865, RVT_866, RVT_867):
            assert code in REPL_ERROR_REGISTRY, f"{code} missing from registry"

    def test_registry_entries_have_description_and_remediation(self) -> None:
        for code, (description, remediation) in REPL_ERROR_REGISTRY.items():
            assert description, f"{code} has empty description"
            assert remediation, f"{code} has empty remediation"


# ---------------------------------------------------------------------------
# make_repl_error
# ---------------------------------------------------------------------------


class TestMakeReplError:
    def test_returns_repl_error(self) -> None:
        err = make_repl_error(RVT_860)
        assert isinstance(err, ReplError)
        assert err.code == RVT_860

    def test_description_from_registry(self) -> None:
        err = make_repl_error(RVT_860)
        expected_desc, _ = REPL_ERROR_REGISTRY[RVT_860]
        assert expected_desc in err.description

    def test_remediation_from_registry(self) -> None:
        err = make_repl_error(RVT_860)
        _, expected_rem = REPL_ERROR_REGISTRY[RVT_860]
        assert err.remediation == expected_rem

    def test_detail_appended_to_description(self) -> None:
        err = make_repl_error(RVT_865, detail="disk full")
        assert "disk full" in err.description

    def test_all_codes_constructable(self) -> None:
        for code in (RVT_860, RVT_861, RVT_862, RVT_863, RVT_864, RVT_865, RVT_866, RVT_867):
            err = make_repl_error(code)
            assert err.code == code


# ---------------------------------------------------------------------------
# Error code descriptions — Requirement 34.2
# ---------------------------------------------------------------------------


class TestErrorCodeDescriptions:
    def test_rvt_860_tui_init(self) -> None:
        err = make_repl_error(RVT_860)
        assert "initialization" in err.description.lower() or "TUI" in err.description

    def test_rvt_861_file_watcher(self) -> None:
        err = make_repl_error(RVT_861)
        assert "watcher" in err.description.lower() or "file" in err.description.lower()

    def test_rvt_862_editor_cache(self) -> None:
        err = make_repl_error(RVT_862)
        assert "cache" in err.description.lower() or "editor" in err.description.lower()

    def test_rvt_863_theme(self) -> None:
        err = make_repl_error(RVT_863)
        assert "theme" in err.description.lower()

    def test_rvt_864_keymap(self) -> None:
        err = make_repl_error(RVT_864)
        assert "keymap" in err.description.lower()

    def test_rvt_865_export(self) -> None:
        err = make_repl_error(RVT_865)
        assert "export" in err.description.lower()

    def test_rvt_866_debug(self) -> None:
        err = make_repl_error(RVT_866)
        assert "debug" in err.description.lower() or "breakpoint" in err.description.lower()

    def test_rvt_867_profile_switch(self) -> None:
        err = make_repl_error(RVT_867)
        assert "profile" in err.description.lower() or "switch" in err.description.lower()


# ---------------------------------------------------------------------------
# ErrorModalState — pure state, no Textual dependency
# ---------------------------------------------------------------------------


class TestErrorModalState:
    def test_header_text(self) -> None:
        from rivet_cli.repl.screens.error_modal import ErrorModalState

        err = ReplError(code="RVT-860", description="TUI init failure", remediation="Check terminal")
        state = ErrorModalState(error=err)
        assert "RVT-860" in state.header_text()

    def test_description_text(self) -> None:
        from rivet_cli.repl.screens.error_modal import ErrorModalState

        err = ReplError(code="RVT-860", description="TUI init failure", remediation="Check terminal")
        state = ErrorModalState(error=err)
        assert state.description_text() == "TUI init failure"

    def test_remediation_text(self) -> None:
        from rivet_cli.repl.screens.error_modal import ErrorModalState

        err = ReplError(code="RVT-860", description="TUI init failure", remediation="Check terminal")
        state = ErrorModalState(error=err)
        assert state.remediation_text() == "Check terminal"

    def test_copy_text_contains_all_fields(self) -> None:
        from rivet_cli.repl.screens.error_modal import ErrorModalState

        err = ReplError(code="RVT-860", description="TUI init failure", remediation="Check terminal")
        state = ErrorModalState(error=err)
        text = state.copy_text()
        assert "RVT-860" in text
        assert "TUI init failure" in text
        assert "Check terminal" in text

    def test_show_in_editor_default_false(self) -> None:
        from rivet_cli.repl.screens.error_modal import ErrorModalState

        err = ReplError(code="RVT-860", description="desc", remediation="fix")
        state = ErrorModalState(error=err)
        assert state.show_in_editor is False

    def test_show_in_editor_true(self) -> None:
        from rivet_cli.repl.screens.error_modal import ErrorModalState

        err = ReplError(code="RVT-860", description="desc", remediation="fix")
        state = ErrorModalState(error=err, show_in_editor=True)
        assert state.show_in_editor is True


# ---------------------------------------------------------------------------
# App wiring — show_repl_error and on_execution_complete / on_project_compiled
# ---------------------------------------------------------------------------


class TestAppErrorWiring:
    """Test that the app has the correct error wiring methods."""

    def test_show_repl_error_method_exists(self) -> None:
        from rivet_cli.repl.app import RivetRepl

        assert hasattr(RivetRepl, "show_repl_error")

    def test_on_execution_complete_method_exists(self) -> None:
        from rivet_cli.repl.app import RivetRepl

        assert hasattr(RivetRepl, "on_execution_complete")

    def test_on_project_compiled_method_exists(self) -> None:
        from rivet_cli.repl.app import RivetRepl

        assert hasattr(RivetRepl, "on_project_compiled")

    def test_on_execution_complete_no_modal_on_success(self) -> None:
        """Successful execution should not push an error modal."""
        from unittest.mock import MagicMock

        from rivet_cli.repl.app import RivetRepl
        from rivet_cli.repl.widgets.status_bar import ExecutionComplete

        app = RivetRepl.__new__(RivetRepl)
        app.push_screen = MagicMock()  # type: ignore[assignment]

        msg = ExecutionComplete(success=True, elapsed_ms=100.0)
        app.on_execution_complete(msg)

        app.push_screen.assert_not_called()

    def test_on_execution_complete_no_modal_on_cancel(self) -> None:
        """Canceled execution should not push an error modal."""
        from unittest.mock import MagicMock

        from rivet_cli.repl.app import RivetRepl
        from rivet_cli.repl.widgets.status_bar import ExecutionComplete

        app = RivetRepl.__new__(RivetRepl)
        app.push_screen = MagicMock()  # type: ignore[assignment]

        msg = ExecutionComplete(success=False, canceled=True, elapsed_ms=100.0)
        app.on_execution_complete(msg)

        app.push_screen.assert_not_called()

    def test_on_execution_complete_pushes_modal_on_error(self) -> None:
        """Failed execution with error message should push an ErrorModal."""
        from unittest.mock import MagicMock

        from rivet_cli.repl.app import RivetRepl
        from rivet_cli.repl.screens.error_modal import ErrorModal
        from rivet_cli.repl.widgets.status_bar import ExecutionComplete

        app = RivetRepl.__new__(RivetRepl)
        app.push_screen = MagicMock()  # type: ignore[assignment]

        msg = ExecutionComplete(success=False, error="Connection refused", elapsed_ms=100.0)
        app.on_execution_complete(msg)

        app.push_screen.assert_called_once()
        modal_arg = app.push_screen.call_args[0][0]
        assert isinstance(modal_arg, ErrorModal)

    def test_on_project_compiled_no_modal_on_success(self) -> None:
        """Successful compilation should not push an error modal."""
        from unittest.mock import MagicMock

        from rivet_cli.repl.app import RivetRepl
        from rivet_cli.repl.widgets.status_bar import ProjectCompiled

        app = RivetRepl.__new__(RivetRepl)
        app.push_screen = MagicMock()  # type: ignore[assignment]

        msg = ProjectCompiled(success=True, elapsed_ms=200.0)
        app.on_project_compiled(msg)

        app.push_screen.assert_not_called()

    def test_on_project_compiled_pushes_modal_on_error(self) -> None:
        """Failed compilation should push an ErrorModal."""
        from unittest.mock import MagicMock

        from rivet_cli.repl.app import RivetRepl
        from rivet_cli.repl.screens.error_modal import ErrorModal
        from rivet_cli.repl.widgets.status_bar import ProjectCompiled

        app = RivetRepl.__new__(RivetRepl)
        app.push_screen = MagicMock()  # type: ignore[assignment]

        msg = ProjectCompiled(success=False, error="Syntax error near SELECT")
        app.on_project_compiled(msg)

        app.push_screen.assert_called_once()
        modal_arg = app.push_screen.call_args[0][0]
        assert isinstance(modal_arg, ErrorModal)

    def test_show_repl_error_pushes_error_modal(self) -> None:
        """show_repl_error should push an ErrorModal with the correct code."""
        from unittest.mock import MagicMock

        from rivet_cli.repl.app import RivetRepl
        from rivet_cli.repl.screens.error_modal import ErrorModal

        app = RivetRepl.__new__(RivetRepl)
        app.push_screen = MagicMock()  # type: ignore[assignment]

        app.show_repl_error(RVT_865, detail="disk full")

        app.push_screen.assert_called_once()
        modal_arg = app.push_screen.call_args[0][0]
        assert isinstance(modal_arg, ErrorModal)
        assert modal_arg._state.error.code == RVT_865
        assert "disk full" in modal_arg._state.error.description
