"""Run command: compile and execute the pipeline."""

from __future__ import annotations

import sys

from rivet_bridge import BridgeValidationError, build_assembly, register_optional_plugins
from rivet_cli.app import GlobalOptions
from rivet_cli.errors import RVT_856, CLIError, format_cli_error, format_upstream_error
from rivet_cli.exit_codes import GENERAL_ERROR, USAGE_ERROR, resolve_exit_code
from rivet_config import load_config
from rivet_core import Executor, PluginRegistry, compile

_VALID_FORMATS = ("text", "json", "quiet")


def run_run(
    sink_name: str | None,
    tags: list[str],
    tag_all: bool,
    fail_fast: bool,
    format: str,
    globals: GlobalOptions,
) -> int:
    """Compile and execute the pipeline."""
    # Validate format
    if format not in _VALID_FORMATS:
        err = CLIError(
            code=RVT_856,
            message=f"Format '{format}' is not supported for the run command.",
            remediation=f"Supported formats: {', '.join(_VALID_FORMATS)}.",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return USAGE_ERROR

    # Load config
    config_result = load_config(globals.project_path, globals.profile)
    if not config_result.success:
        for e in config_result.errors:
            print(
                format_upstream_error(
                    "RVT-850" if "rivet.yaml" in e.message.lower() else e.message[:7] if len(e.message) > 7 else "CFG",
                    e.message,
                    e.remediation,
                    globals.color,
                ),
                file=sys.stderr,
            )
        return GENERAL_ERROR

    # Build assembly
    registry = PluginRegistry()
    registry.register_builtins()
    register_optional_plugins(registry)
    try:
        bridge_result = build_assembly(config_result, registry)
    except BridgeValidationError as exc:
        for e in exc.errors:  # type: ignore[assignment]
            print(
                format_upstream_error(e.code, e.message, e.remediation or "", globals.color),  # type: ignore[attr-defined]
                file=sys.stderr,
            )
        return GENERAL_ERROR

    # Compile
    compiled = compile(
        bridge_result.assembly,
        list(bridge_result.catalogs.values()),
        list(bridge_result.engines.values()),
        registry,
        target_sink=sink_name,
        tags=tags or None,
        tag_mode="and" if tag_all else "or",
        default_engine=config_result.profile.default_engine if config_result.profile else None,
    )
    if not compiled.success:
        for e in compiled.errors:  # type: ignore[assignment]
            print(
                format_upstream_error(e.code, e.message, e.remediation or "", globals.color),  # type: ignore[attr-defined]
                file=sys.stderr,
            )
        return GENERAL_ERROR

    # Execute
    result = Executor(registry).run_sync(compiled, fail_fast=fail_fast)

    # Determine exit code from execution result
    has_assertion = any(
        not cr.passed and cr.phase == "assertion" and cr.severity == "error"
        for jr in result.joint_results
        for cr in jr.check_results
    )
    has_audit = any(
        not cr.passed and cr.phase == "audit"
        for jr in result.joint_results
        for cr in jr.check_results
    )
    has_partial = result.status == "partial_failure"
    exit_code = resolve_exit_code(has_assertion, has_audit, has_partial)

    # Render output
    if format == "json":
        from rivet_cli.rendering.json_out import render_run_json

        print(render_run_json(result, compiled))
    elif format == "quiet":
        # Quiet mode: errors only
        for jr in result.joint_results:
            if not jr.success and jr.error:
                print(
                    format_upstream_error(jr.error.code, jr.error.message, jr.error.remediation or "", globals.color),
                    file=sys.stderr,
                )
            for cr in jr.check_results:
                if not cr.passed:
                    print(
                        format_upstream_error(cr.phase.upper(), cr.message, "", globals.color),
                        file=sys.stderr,
                    )
    else:
        # text format
        from rivet_cli.rendering.run_text import render_run_text

        print(render_run_text(result, compiled, globals.verbosity, globals.color))

    return exit_code
