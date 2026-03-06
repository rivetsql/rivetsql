"""Property 15: Exit code mapping for `run_repl`.

Validates: Requirement 2.4
  - 0   normal exit
  - 1   startup error (missing deps or session start failure)
  - 10  invalid arguments
  - 130 SIGINT interruption
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from rivet_cli.repl import run_repl

_EXIT_SUCCESS = 0
_EXIT_GENERAL_ERROR = 1
_EXIT_USAGE_ERROR = 10
_EXIT_INTERRUPTED = 130
_VALID_EXIT_CODES = frozenset({_EXIT_SUCCESS, _EXIT_GENERAL_ERROR, _EXIT_USAGE_ERROR, _EXIT_INTERRUPTED})


def _make_args(
    project: str = ".",
    profile: str = "default",
    theme: str = "rivet",
    no_watch: bool = False,
    read_only: bool = False,
    editor: str | None = None,
) -> argparse.Namespace:
    return argparse.Namespace(
        project=project,
        profile=profile,
        theme=theme,
        no_watch=no_watch,
        read_only=read_only,
        editor=editor,
    )


def _patch_imports_available() -> patch:
    """Patch builtins.__import__ so TUI deps appear available."""
    original = __import__

    def _fake(name: str, *a: object, **kw: object) -> object:
        if name in ("textual", "textual_textarea", "textual_fastdatatable"):
            return MagicMock()
        return original(name, *a, **kw)  # type: ignore[call-arg]

    return patch("builtins.__import__", side_effect=_fake)


# ---------------------------------------------------------------------------
# Exit code 10: invalid arguments (non-existent project path)
# ---------------------------------------------------------------------------


class TestExitCode10InvalidArgs:
    def test_nonexistent_project_returns_10(self, tmp_path: Path) -> None:
        args = _make_args(project=str(tmp_path / "does_not_exist"))
        assert run_repl(args) == _EXIT_USAGE_ERROR

    def test_existing_project_does_not_return_10(self, tmp_path: Path) -> None:
        """A valid project path should not produce exit code 10."""
        args = _make_args(project=str(tmp_path))

        mock_session = MagicMock()
        mock_session.start.return_value = None
        mock_session.stop.return_value = None

        mock_app = MagicMock()
        mock_app.run.return_value = None

        mock_repl_cls = MagicMock(return_value=mock_app)
        mock_app_module = MagicMock()
        mock_app_module.RivetRepl = mock_repl_cls

        with (
            _patch_imports_available(),
            patch("rivet_core.interactive.InteractiveSession", return_value=mock_session),
            patch.dict(sys.modules, {"rivet_cli.repl.app": mock_app_module}),
        ):
            code = run_repl(args)
        assert code != _EXIT_USAGE_ERROR


# ---------------------------------------------------------------------------
# Exit code 1: session start failure
# ---------------------------------------------------------------------------


class TestExitCode1SessionStartFailure:
    def test_session_start_exception_returns_1(self, tmp_path: Path) -> None:
        args = _make_args(project=str(tmp_path))

        mock_session = MagicMock()
        mock_session.start.side_effect = RuntimeError("catalog connection failed")

        with (
            _patch_imports_available(),
            patch("rivet_core.interactive.InteractiveSession", return_value=mock_session),
        ):
            code = run_repl(args)

        assert code == _EXIT_GENERAL_ERROR


# ---------------------------------------------------------------------------
# Exit code 130: SIGINT / KeyboardInterrupt
# ---------------------------------------------------------------------------


class TestExitCode130Sigint:
    def test_keyboard_interrupt_during_app_run_returns_130(self, tmp_path: Path) -> None:
        args = _make_args(project=str(tmp_path))

        mock_session = MagicMock()
        mock_session.start.return_value = None
        mock_session.stop.return_value = None

        mock_app = MagicMock()
        mock_app.run.side_effect = KeyboardInterrupt

        mock_repl_cls = MagicMock(return_value=mock_app)
        mock_app_module = MagicMock()
        mock_app_module.RivetRepl = mock_repl_cls

        with (
            _patch_imports_available(),
            patch("rivet_core.interactive.InteractiveSession", return_value=mock_session),
            patch.dict(sys.modules, {"rivet_cli.repl.app": mock_app_module}),
        ):
            code = run_repl(args)

        assert code == _EXIT_INTERRUPTED


# ---------------------------------------------------------------------------
# Exit code 0: normal exit
# ---------------------------------------------------------------------------


class TestExitCode0NormalExit:
    def test_normal_exit_returns_0(self, tmp_path: Path) -> None:
        args = _make_args(project=str(tmp_path))

        mock_session = MagicMock()
        mock_session.start.return_value = None
        mock_session.stop.return_value = None

        mock_app = MagicMock()
        mock_app.run.return_value = None

        mock_repl_cls = MagicMock(return_value=mock_app)
        mock_app_module = MagicMock()
        mock_app_module.RivetRepl = mock_repl_cls

        with (
            _patch_imports_available(),
            patch("rivet_core.interactive.InteractiveSession", return_value=mock_session),
            patch.dict(sys.modules, {"rivet_cli.repl.app": mock_app_module}),
        ):
            code = run_repl(args)

        assert code == _EXIT_SUCCESS


# ---------------------------------------------------------------------------
# Property 15: exit code is always one of the four documented values
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "project_exists,deps_available,session_raises,app_raises",
    [
        (False, True, False, False),   # invalid path → 10
        (True, True, True, False),     # session start fails → 1
        (True, True, False, True),     # KeyboardInterrupt → 130
        (True, True, False, False),    # normal → 0
    ],
)
def test_property15_exit_code_always_in_documented_set(
    tmp_path: Path,
    project_exists: bool,
    deps_available: bool,
    session_raises: bool,
    app_raises: bool,
) -> None:
    """Property 15: run_repl always returns one of {0, 1, 10, 130}."""
    project = str(tmp_path) if project_exists else str(tmp_path / "missing")
    args = _make_args(project=project)

    mock_session = MagicMock()
    if session_raises:
        mock_session.start.side_effect = RuntimeError("boom")
    else:
        mock_session.start.return_value = None
    mock_session.stop.return_value = None

    mock_app = MagicMock()
    if app_raises:
        mock_app.run.side_effect = KeyboardInterrupt
    else:
        mock_app.run.return_value = None

    mock_app_module = MagicMock()
    mock_app_module.RivetRepl = MagicMock(return_value=mock_app)

    with (
        _patch_imports_available(),
        patch("rivet_core.interactive.InteractiveSession", return_value=mock_session),
        patch.dict(sys.modules, {"rivet_cli.repl.app": mock_app_module}),
    ):
        code = run_repl(args)

    assert code in _VALID_EXIT_CODES, (
        f"run_repl returned undocumented exit code {code}; "
        f"expected one of {sorted(_VALID_EXIT_CODES)}"
    )
