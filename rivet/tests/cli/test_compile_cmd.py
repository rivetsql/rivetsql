"""Tests for the compile command (commands/compile.py).

Validates task 8.2 requirements: format validation, compilation flow,
output rendering, tag/sink filtering, and Property 7 (no execution).
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from rivet_cli.app import GlobalOptions
from rivet_cli.commands.compile import _VALID_FORMATS, run_compile
from rivet_cli.exit_codes import GENERAL_ERROR, SUCCESS, USAGE_ERROR

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _globals(**overrides) -> GlobalOptions:
    defaults = dict(profile="default", project_path=Path("."), verbosity=0, color=False)
    defaults.update(overrides)
    return GlobalOptions(**defaults)


def _make_compiled(success: bool = True, joints=None, errors=None):
    """Build a minimal mock CompiledAssembly."""
    compiled = MagicMock()
    compiled.success = success
    compiled.profile_name = "default"
    compiled.catalogs = []
    compiled.engines = []
    compiled.adapters = []
    compiled.joints = joints or []
    compiled.fused_groups = []
    compiled.materializations = []
    compiled.execution_order = []
    compiled.errors = errors or []
    compiled.warnings = []
    return compiled


def _make_config_result(success: bool = True, profile=True):
    """Build a minimal mock ConfigResult."""
    cr = MagicMock()
    cr.success = success
    cr.errors = [] if success else [MagicMock(message="bad config", remediation="fix it")]
    cr.warnings = []
    cr.profile = MagicMock() if profile else None
    return cr


def _make_bridge_result():
    """Build a minimal mock BridgeResult."""
    br = MagicMock()
    br.assembly = MagicMock()
    br.catalogs = {}
    br.engines = {}
    br.profile_snapshot = MagicMock()
    return br


def _make_joint(name="j1", type="sql"):
    j = MagicMock()
    j.name = name
    j.type = type
    j.upstream = []
    j.fused_group_id = None
    return j


def _patch_pipeline(mock_load, mock_build, mock_compile, joints=None, success=True, errors=None):
    """Wire up the standard load→build→compile mock chain."""
    mock_load.return_value = _make_config_result()
    mock_build.return_value = _make_bridge_result()
    mock_compile.return_value = _make_compiled(
        success=success, joints=joints or [_make_joint()], errors=errors,
    )


# ---------------------------------------------------------------------------
# Format validation (Property 8 — RVT-856)
# ---------------------------------------------------------------------------


class TestFormatValidation:
    def test_invalid_format_returns_usage_error(self, capsys):
        result = run_compile(
            sink_name=None, tags=[], tag_all=False,
            format="xml", output=None, globals=_globals(),
        )
        assert result == USAGE_ERROR
        captured = capsys.readouterr()
        assert "RVT-856" in captured.err
        assert "xml" in captured.err

    def test_valid_formats_accepted(self):
        for fmt in _VALID_FORMATS:
            assert fmt in ("visual", "json", "mermaid")


# ---------------------------------------------------------------------------
# Config errors
# ---------------------------------------------------------------------------


class TestConfigErrors:
    @patch("rivet_cli.commands.compile.load_config")
    def test_config_failure_returns_general_error(self, mock_load, capsys):
        mock_load.return_value = _make_config_result(success=False)
        result = run_compile(
            sink_name=None, tags=[], tag_all=False,
            format="visual", output=None, globals=_globals(),
        )
        assert result == GENERAL_ERROR

    @patch("rivet_cli.commands.compile.load_config")
    def test_missing_profile_returns_general_error(self, mock_load, capsys):
        mock_load.return_value = _make_config_result(success=True, profile=False)
        result = run_compile(
            sink_name=None, tags=[], tag_all=False,
            format="visual", output=None, globals=_globals(),
        )
        assert result == GENERAL_ERROR
        captured = capsys.readouterr()
        assert "RVT-853" in captured.err


# ---------------------------------------------------------------------------
# Bridge errors
# ---------------------------------------------------------------------------


class TestBridgeErrors:
    @patch("rivet_cli.commands.compile.build_assembly")
    @patch("rivet_cli.commands.compile.load_config")
    def test_bridge_error_returns_general_error(self, mock_load, mock_build, capsys):
        mock_load.return_value = _make_config_result()
        from rivet_bridge import BridgeValidationError
        from rivet_bridge.errors import BridgeError
        mock_build.side_effect = BridgeValidationError([
            BridgeError(code="BRG-100", message="fail", remediation="fix")
        ])
        result = run_compile(
            sink_name=None, tags=[], tag_all=False,
            format="visual", output=None, globals=_globals(),
        )
        assert result == GENERAL_ERROR


# ---------------------------------------------------------------------------
# Compilation failure
# ---------------------------------------------------------------------------


class TestCompilationFailure:
    @patch("rivet_cli.commands.compile.compile")
    @patch("rivet_cli.commands.compile.build_assembly")
    @patch("rivet_cli.commands.compile.load_config")
    def test_compile_failure_returns_general_error(self, mock_load, mock_build, mock_compile, capsys):
        mock_load.return_value = _make_config_result()
        mock_build.return_value = _make_bridge_result()
        from rivet_core.errors import RivetError
        mock_compile.return_value = _make_compiled(
            success=False,
            errors=[RivetError(code="RVT-306", message="cycle", remediation="fix dag")],
        )
        result = run_compile(
            sink_name=None, tags=[], tag_all=False,
            format="visual", output=None, globals=_globals(),
        )
        assert result == GENERAL_ERROR


# ---------------------------------------------------------------------------
# Tag filter matched no joints → RVT-854
# ---------------------------------------------------------------------------


class TestTagFilter:
    @patch("rivet_cli.commands.compile.compile")
    @patch("rivet_cli.commands.compile.build_assembly")
    @patch("rivet_cli.commands.compile.load_config")
    def test_tag_no_match_returns_general_error(self, mock_load, mock_build, mock_compile, capsys):
        mock_load.return_value = _make_config_result()
        mock_build.return_value = _make_bridge_result()
        mock_compile.return_value = _make_compiled(success=True, joints=[])
        result = run_compile(
            sink_name=None, tags=["nonexistent"], tag_all=False,
            format="visual", output=None, globals=_globals(),
        )
        assert result == GENERAL_ERROR
        captured = capsys.readouterr()
        assert "RVT-854" in captured.err

    @patch("rivet_cli.commands.compile.compile")
    @patch("rivet_cli.commands.compile.build_assembly")
    @patch("rivet_cli.commands.compile.load_config")
    def test_no_tags_empty_joints_is_success(self, mock_load, mock_build, mock_compile, capsys):
        """Without tags, empty joints is not an error (just an empty project)."""
        mock_load.return_value = _make_config_result()
        mock_build.return_value = _make_bridge_result()
        mock_compile.return_value = _make_compiled(success=True, joints=[])
        result = run_compile(
            sink_name=None, tags=[], tag_all=False,
            format="visual", output=None, globals=_globals(),
        )
        assert result == SUCCESS


# ---------------------------------------------------------------------------
# Successful compilation — visual output
# ---------------------------------------------------------------------------


class TestSuccessfulCompile:
    @patch("rivet_cli.commands.compile.compile")
    @patch("rivet_cli.commands.compile.build_assembly")
    @patch("rivet_cli.commands.compile.load_config")
    def test_success_returns_zero(self, mock_load, mock_build, mock_compile):
        _patch_pipeline(mock_load, mock_build, mock_compile)
        result = run_compile(
            sink_name=None, tags=[], tag_all=False,
            format="visual", output=None, globals=_globals(),
        )
        assert result == SUCCESS

    @patch("rivet_cli.commands.compile.compile")
    @patch("rivet_cli.commands.compile.build_assembly")
    @patch("rivet_cli.commands.compile.load_config")
    def test_visual_output_contains_profile(self, mock_load, mock_build, mock_compile, capsys):
        _patch_pipeline(mock_load, mock_build, mock_compile)
        run_compile(
            sink_name=None, tags=[], tag_all=False,
            format="visual", output=None, globals=_globals(),
        )
        assert "Profile:" in capsys.readouterr().out

    @patch("rivet_cli.commands.compile.compile")
    @patch("rivet_cli.commands.compile.build_assembly")
    @patch("rivet_cli.commands.compile.load_config")
    def test_tag_all_passes_and_mode(self, mock_load, mock_build, mock_compile):
        _patch_pipeline(mock_load, mock_build, mock_compile)
        run_compile(
            sink_name="my_sink", tags=["a", "b"], tag_all=True,
            format="visual", output=None, globals=_globals(),
        )
        _, kwargs = mock_compile.call_args
        assert kwargs["tag_mode"] == "and"
        assert kwargs["tags"] == ["a", "b"]
        assert kwargs["target_sink"] == "my_sink"

    @patch("rivet_cli.commands.compile.compile")
    @patch("rivet_cli.commands.compile.build_assembly")
    @patch("rivet_cli.commands.compile.load_config")
    def test_tag_or_mode_default(self, mock_load, mock_build, mock_compile):
        _patch_pipeline(mock_load, mock_build, mock_compile)
        run_compile(
            sink_name=None, tags=["x"], tag_all=False,
            format="visual", output=None, globals=_globals(),
        )
        _, kwargs = mock_compile.call_args
        assert kwargs["tag_mode"] == "or"


# ---------------------------------------------------------------------------
# JSON format output
# ---------------------------------------------------------------------------


class TestJsonFormat:
    @patch("rivet_cli.rendering.json_out.render_compile_json", return_value='{"fused_groups": []}')
    @patch("rivet_cli.commands.compile.compile")
    @patch("rivet_cli.commands.compile.build_assembly")
    @patch("rivet_cli.commands.compile.load_config")
    def test_json_format_produces_valid_json(self, mock_load, mock_build, mock_compile, mock_render, capsys):
        _patch_pipeline(mock_load, mock_build, mock_compile)
        result = run_compile(
            sink_name=None, tags=[], tag_all=False,
            format="json", output=None, globals=_globals(),
        )
        assert result == SUCCESS
        data = json.loads(capsys.readouterr().out)
        assert isinstance(data, dict)
        mock_render.assert_called_once()


# ---------------------------------------------------------------------------
# Mermaid format output
# ---------------------------------------------------------------------------


class TestMermaidFormat:
    @patch("rivet_cli.commands.compile.compile")
    @patch("rivet_cli.commands.compile.build_assembly")
    @patch("rivet_cli.commands.compile.load_config")
    def test_mermaid_format_produces_graph(self, mock_load, mock_build, mock_compile, capsys):
        _patch_pipeline(mock_load, mock_build, mock_compile)
        result = run_compile(
            sink_name=None, tags=[], tag_all=False,
            format="mermaid", output=None, globals=_globals(),
        )
        assert result == SUCCESS
        assert capsys.readouterr().out.startswith("graph TD")


# ---------------------------------------------------------------------------
# Output file
# ---------------------------------------------------------------------------


class TestOutputFile:
    @patch("rivet_cli.commands.compile.compile")
    @patch("rivet_cli.commands.compile.build_assembly")
    @patch("rivet_cli.commands.compile.load_config")
    def test_output_writes_json_file(self, mock_load, mock_build, mock_compile, tmp_path):
        _patch_pipeline(mock_load, mock_build, mock_compile)
        out_path = tmp_path / "output.json"
        with patch("rivet_cli.commands.compile._write_json") as mock_write:
            run_compile(
                sink_name=None, tags=[], tag_all=False,
                format="visual", output=str(out_path), globals=_globals(),
            )
            mock_write.assert_called_once()


# ---------------------------------------------------------------------------
# Sink name filtering
# ---------------------------------------------------------------------------


class TestSinkNameFilter:
    @patch("rivet_cli.commands.compile.compile")
    @patch("rivet_cli.commands.compile.build_assembly")
    @patch("rivet_cli.commands.compile.load_config")
    def test_sink_name_passed_to_compile(self, mock_load, mock_build, mock_compile):
        _patch_pipeline(mock_load, mock_build, mock_compile)
        run_compile(
            sink_name="my_sink", tags=[], tag_all=False,
            format="visual", output=None, globals=_globals(),
        )
        _, kwargs = mock_compile.call_args
        assert kwargs["target_sink"] == "my_sink"

    @patch("rivet_cli.commands.compile.compile")
    @patch("rivet_cli.commands.compile.build_assembly")
    @patch("rivet_cli.commands.compile.load_config")
    def test_no_sink_name_passes_none(self, mock_load, mock_build, mock_compile):
        _patch_pipeline(mock_load, mock_build, mock_compile)
        run_compile(
            sink_name=None, tags=[], tag_all=False,
            format="visual", output=None, globals=_globals(),
        )
        _, kwargs = mock_compile.call_args
        assert kwargs["target_sink"] is None


# ---------------------------------------------------------------------------
# Compile never calls Executor (Property 7)
# ---------------------------------------------------------------------------


class TestCompileNeverExecutes:
    @patch("rivet_cli.commands.compile.compile")
    @patch("rivet_cli.commands.compile.build_assembly")
    @patch("rivet_cli.commands.compile.load_config")
    def test_executor_never_called(self, mock_load, mock_build, mock_compile):
        _patch_pipeline(mock_load, mock_build, mock_compile)
        with patch("rivet_core.Executor") as mock_executor_cls:
            run_compile(
                sink_name=None, tags=[], tag_all=False,
                format="visual", output=None, globals=_globals(),
            )
            mock_executor_cls.assert_not_called()
            mock_executor_cls.return_value.run.assert_not_called()
