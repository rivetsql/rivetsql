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
    engine: str | None = None,
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
                    "RVT-850"
                    if "rivet.yaml" in e.message.lower()
                    else e.message[:7]
                    if len(e.message) > 7
                    else "CFG",
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
        default_engine=engine
        or (config_result.profile.default_engine if config_result.profile else None),
        project_root=globals.project_path,
    )
    if not compiled.success:
        for compile_error in compiled.diagnostics.errors:
            print(
                format_upstream_error(
                    compile_error.code,
                    compile_error.message,
                    compile_error.remediation or "",
                    globals.color,
                ),
                file=sys.stderr,
            )
        return GENERAL_ERROR

    # Execute — wire LiveRunRenderer as progress callback for text/quiet formats
    if format in ("text", "quiet"):
        from rivet_cli.rendering.formatter import AssemblyFormatter
        from rivet_cli.rendering.run_text import LiveRunRenderer

        verbosity = -1 if format == "quiet" else globals.verbosity
        renderer = LiveRunRenderer(compiled, verbosity, globals.color)

        # Print compilation summary to stderr before execution
        if verbosity >= 0:
            fmt = AssemblyFormatter(color=globals.color, verbosity=verbosity)
            summary_line = fmt.render_summary_line(compiled)
            if summary_line:
                print(summary_line, file=sys.stderr)

        renderer.print_execution_plan()
        result = Executor(registry, project_root=globals.project_path).run_sync(
            compiled, fail_fast=fail_fast, progress=renderer
        )
        summary = renderer.render_summary(result)
        if summary:
            print(summary)
    else:
        # JSON format: no callback
        result = Executor(registry, project_root=globals.project_path).run_sync(
            compiled, fail_fast=fail_fast
        )

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

    # Render JSON output (text/quiet already handled above via LiveRunRenderer)
    if format == "json":
        from rivet_cli.rendering.json_out import render_run_json

        print(render_run_json(result, compiled))

    return exit_code
