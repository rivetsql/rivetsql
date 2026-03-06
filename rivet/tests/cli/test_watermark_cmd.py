"""Tests for the watermark command."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from rivet_cli.app import GlobalOptions
from rivet_cli.commands.watermark import (
    run_watermark_list,
    run_watermark_reset,
    run_watermark_set,
)


def _globals(project_path: Path, profile: str = "default") -> GlobalOptions:
    return GlobalOptions(profile=profile, project_path=project_path, color=False)


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """Create a minimal valid project that load_config can parse."""
    d = tmp_path / "proj"
    d.mkdir()
    (d / "rivet.yaml").write_text(
        "profiles: profiles.yaml\nsources: sources\njoints: joints\nsinks: sinks\n"
    )
    (d / "profiles.yaml").write_text(
        "default:\n  catalogs:\n    local:\n      type: filesystem\n"
        "      config: {}\n  engines:\n    default:\n      type: arrow\n"
        "  default_engine: default\n"
    )
    for sub in ("sources", "joints", "sinks"):
        (d / sub).mkdir()
    # Create a joint so we have a known name
    (d / "joints" / "my_joint.sql").write_text(
        "-- @joint: my_joint\n-- @type: sql\nSELECT 1\n"
    )
    return d


class TestWatermarkList:
    def test_no_watermarks(self, project: Path, capsys: pytest.CaptureFixture[str]) -> None:
        result = run_watermark_list(_globals(project))
        assert result == 0
        assert "No watermarks found" in capsys.readouterr().out

    def test_lists_existing_watermarks(self, project: Path, capsys: pytest.CaptureFixture[str]) -> None:
        run_watermark_set("my_joint", "2024-01-01", _globals(project))
        capsys.readouterr()

        result = run_watermark_list(_globals(project))
        assert result == 0
        out = capsys.readouterr().out
        assert "my_joint" in out
        assert "2024-01-01" in out


class TestWatermarkReset:
    def test_reset_existing(self, project: Path, capsys: pytest.CaptureFixture[str]) -> None:
        run_watermark_set("my_joint", "2024-01-01", _globals(project))
        capsys.readouterr()

        result = run_watermark_reset("my_joint", _globals(project))
        assert result == 0
        assert "reset" in capsys.readouterr().out.lower()

        capsys.readouterr()
        run_watermark_list(_globals(project))
        assert "No watermarks found" in capsys.readouterr().out

    def test_reset_nonexistent_joint(self, project: Path, capsys: pytest.CaptureFixture[str]) -> None:
        result = run_watermark_reset("no_such_joint", _globals(project))
        assert result == 1
        assert "not found" in capsys.readouterr().err.lower()


class TestWatermarkSet:
    def test_set_value(self, project: Path, capsys: pytest.CaptureFixture[str]) -> None:
        result = run_watermark_set("my_joint", "2024-06-15", _globals(project))
        assert result == 0
        out = capsys.readouterr().out
        assert "2024-06-15" in out

        wm_file = project / ".rivet" / "watermarks" / "default" / "my_joint.json"
        assert wm_file.exists()
        data = json.loads(wm_file.read_text())
        assert data["value"] == "2024-06-15"

    def test_set_nonexistent_joint(self, project: Path, capsys: pytest.CaptureFixture[str]) -> None:
        result = run_watermark_set("no_such_joint", "val", _globals(project))
        assert result == 1
        err = capsys.readouterr().err
        assert "not found" in err.lower()


class TestWatermarkProfileScoping:
    def test_different_profiles_isolated(self, project: Path, capsys: pytest.CaptureFixture[str]) -> None:
        run_watermark_set("my_joint", "val-a", _globals(project, profile="alpha"))
        run_watermark_set("my_joint", "val-b", _globals(project, profile="beta"))
        capsys.readouterr()

        run_watermark_list(_globals(project, profile="alpha"))
        out = capsys.readouterr().out
        assert "val-a" in out

        run_watermark_list(_globals(project, profile="beta"))
        out = capsys.readouterr().out
        assert "val-b" in out
