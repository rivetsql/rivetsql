"""Watermark command: list, reset, set incremental watermarks."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from rivet_cli.app import GlobalOptions
from rivet_cli.errors import CLIError, format_cli_error
from rivet_cli.exit_codes import GENERAL_ERROR, SUCCESS
from rivet_config import load_config


def _watermark_dir(project_path: Path, profile: str) -> Path:
    return project_path / ".rivet" / "watermarks" / profile


def _watermark_file(project_path: Path, profile: str, joint_name: str) -> Path:
    return _watermark_dir(project_path, profile) / f"{joint_name}.json"


def _validate_joint(project_path: Path, profile: str, joint_name: str, color: bool) -> int | None:
    """Validate joint exists in project. Returns exit code on error, None on success."""
    config_result = load_config(project_path, profile)
    known = {d.name for d in config_result.declarations}
    if joint_name not in known:
        err = CLIError(
            code="RVT-852",
            message=f"Joint '{joint_name}' not found in project.",
            remediation=f"Available joints: {', '.join(sorted(known)) or '(none)'}",
        )
        print(format_cli_error(err, color), file=sys.stderr)
        return GENERAL_ERROR
    return None


def run_watermark_list(globals: GlobalOptions) -> int:
    """List all watermarks for the current project and profile."""
    wm_dir = _watermark_dir(globals.project_path, globals.profile)
    if not wm_dir.is_dir():
        print("No watermarks found.")
        return SUCCESS

    files = sorted(wm_dir.glob("*.json"))
    if not files:
        print("No watermarks found.")
        return SUCCESS

    for f in files:
        joint_name = f.stem
        try:
            data = json.loads(f.read_text())
            value = data.get("value", "(unknown)")
            print(f"  {joint_name}: {value}")
        except (json.JSONDecodeError, OSError):
            print(f"  {joint_name}: (corrupt)")
    return SUCCESS


def run_watermark_reset(joint_name: str, globals: GlobalOptions) -> int:
    """Reset watermark for a specific joint."""
    err = _validate_joint(globals.project_path, globals.profile, joint_name, globals.color)
    if err is not None:
        return err

    wm_file = _watermark_file(globals.project_path, globals.profile, joint_name)
    if wm_file.exists():
        wm_file.unlink()
    print(f"Watermark reset for joint '{joint_name}'.")
    return SUCCESS


def run_watermark_set(joint_name: str, value: str, globals: GlobalOptions) -> int:
    """Manually set a watermark value."""
    err = _validate_joint(globals.project_path, globals.profile, joint_name, globals.color)
    if err is not None:
        return err

    wm_dir = _watermark_dir(globals.project_path, globals.profile)
    wm_dir.mkdir(parents=True, exist_ok=True)
    wm_file = _watermark_file(globals.project_path, globals.profile, joint_name)
    wm_file.write_text(json.dumps({"value": value}))
    print(f"Watermark for joint '{joint_name}' set to '{value}'.")
    return SUCCESS
