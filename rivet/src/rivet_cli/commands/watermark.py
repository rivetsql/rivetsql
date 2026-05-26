"""Watermark command: list, reset, set incremental watermarks.

Backed by ``rivet_core.watermark.LocalFileWatermarkBackend`` so the on-disk
format is the same one any future executor-side state reader will use.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime

from rivet_cli.app import GlobalOptions
from rivet_cli.errors import CLIError, format_cli_error
from rivet_cli.exit_codes import GENERAL_ERROR, SUCCESS
from rivet_config import load_config
from rivet_core.watermark import LocalFileWatermarkBackend, WatermarkState


def _backend(globals: GlobalOptions) -> LocalFileWatermarkBackend:
    return LocalFileWatermarkBackend(globals.project_path)


def _validate_joint(globals: GlobalOptions, joint_name: str) -> int | None:
    """Validate joint exists in project. Returns exit code on error, None on success."""
    config_result = load_config(globals.project_path, globals.profile)
    known = {d.name for d in config_result.declarations}
    if joint_name not in known:
        err = CLIError(
            code="RVT-852",
            message=f"Joint '{joint_name}' not found in project.",
            remediation=f"Available joints: {', '.join(sorted(known)) or '(none)'}",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return GENERAL_ERROR
    return None


def run_watermark_list(globals: GlobalOptions) -> int:
    """List all watermarks for the current project and profile."""
    backend = _backend(globals)
    sinks = backend.list(globals.profile)
    if not sinks:
        print("No watermarks found.")
        return SUCCESS

    for sink_name in sinks:
        state = backend.read(sink_name, globals.profile)
        value = state.value if state is not None else "(unknown)"
        print(f"  {sink_name}: {value}")
    return SUCCESS


def run_watermark_reset(joint_name: str, globals: GlobalOptions) -> int:
    """Reset watermark for a specific joint."""
    err = _validate_joint(globals, joint_name)
    if err is not None:
        return err

    _backend(globals).delete(joint_name, globals.profile)
    print(f"Watermark reset for joint '{joint_name}'.")
    return SUCCESS


def run_watermark_set(joint_name: str, value: str, globals: GlobalOptions) -> int:
    """Manually set a watermark value."""
    err = _validate_joint(globals, joint_name)
    if err is not None:
        return err

    state = WatermarkState(
        column="",
        value=value,
        value_type="string",
        last_run=datetime.now(UTC).isoformat(),
        rows_loaded=0,
        metadata={"source": "manual"},
    )
    _backend(globals).write(joint_name, globals.profile, state)
    print(f"Watermark for joint '{joint_name}' set to '{value}'.")
    return SUCCESS
