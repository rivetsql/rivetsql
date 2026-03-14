"""Compile command: build and render the CompiledAssembly."""

from __future__ import annotations

import sys
from pathlib import Path

from rivet_bridge import (
    BridgeResult,
    BridgeValidationError,
    build_assembly,
    register_optional_plugins,
)
from rivet_cli.app import GlobalOptions
from rivet_cli.errors import (
    RVT_853,
    RVT_854,
    RVT_856,
    CLIError,
    format_cli_error,
    format_cli_warning,
    format_upstream_error,
)
from rivet_cli.exit_codes import GENERAL_ERROR, SUCCESS, USAGE_ERROR
from rivet_config import load_config
from rivet_core import PluginRegistry, compile
from rivet_core.compiler import CompiledAssembly

_VALID_FORMATS = ("visual", "json", "mermaid")


def run_compile(
    sink_name: str | None,
    tags: list[str],
    tag_all: bool,
    format: str,
    output: str | None,
    globals: GlobalOptions,
) -> int:
    """Compile the project and render the CompiledAssembly.

    Returns exit code 0 on success, 1 on compilation failure, 10 on usage error.
    """
    # 1. Validate format
    if format not in _VALID_FORMATS:
        err = CLIError(
            code=RVT_856,
            message=f"Output format '{format}' is not supported for compile.",
            remediation=f"Supported formats: {', '.join(_VALID_FORMATS)}.",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return USAGE_ERROR

    # 2. Load config
    config_result = load_config(globals.project_path, globals.profile, strict=False)
    if not config_result.success:
        for e in config_result.errors:
            print(
                format_upstream_error("RVT-850", e.message, e.remediation, globals.color),
                file=sys.stderr,
            )
        return GENERAL_ERROR

    # Print config warnings (e.g. unset env vars)
    for w in config_result.warnings:
        print(
            format_cli_warning(w.message, w.remediation, globals.color),
            file=sys.stderr,
        )

    # 3. Check profile
    if config_result.profile is None:
        err = CLIError(
            code=RVT_853,
            message=f"Profile '{globals.profile}' not found.",
            remediation="Check available profiles in profiles.yaml.",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return GENERAL_ERROR

    # 4. Build assembly via bridge
    registry = PluginRegistry()
    registry.register_builtins()
    register_optional_plugins(registry)
    try:
        bridge_result: BridgeResult = build_assembly(config_result, registry)
    except BridgeValidationError as exc:
        for be in exc.errors:
            print(
                format_upstream_error(be.code, be.message, be.remediation, globals.color),
                file=sys.stderr,
            )
        return GENERAL_ERROR

    # 5. Compile
    tag_mode = "and" if tag_all else "or"
    compiled: CompiledAssembly = compile(
        assembly=bridge_result.assembly,
        catalogs=list(bridge_result.catalogs.values()),
        engines=list(bridge_result.engines.values()),
        registry=registry,
        target_sink=sink_name,
        tags=tags if tags else None,
        tag_mode=tag_mode,
        default_engine=config_result.profile.default_engine if config_result.profile else None,
        project_root=globals.project_path,
    )

    if not compiled.success:
        for ce in compiled.errors:
            print(
                format_upstream_error(
                    ce.code,
                    ce.message,
                    ce.remediation,
                    globals.color,
                ),
                file=sys.stderr,
            )
        return GENERAL_ERROR

    # 6. Check tag filter matched something
    if tags and not compiled.joints:
        err = CLIError(
            code=RVT_854,
            message="Tag filter matched no joints.",
            remediation="Check that the specified tags exist on at least one joint.",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return GENERAL_ERROR

    # 7. If --output: serialize to JSON file
    if output:
        _write_json(compiled, output)

    # 8. Dispatch to renderer
    rendered = _render(compiled, format, globals)
    if rendered:
        print(rendered)

    return SUCCESS


def _render(compiled: CompiledAssembly, format: str, globals: GlobalOptions) -> str:
    """Dispatch to the appropriate renderer."""
    if format == "json":
        from rivet_cli.rendering.json_out import render_compile_json

        return render_compile_json(compiled)
    elif format == "mermaid":
        from rivet_cli.rendering.mermaid import render_mermaid

        return render_mermaid(compiled)
    else:
        from rivet_cli.rendering.visual import render_visual

        return render_visual(compiled, globals.verbosity, globals.color)


def _write_json(compiled: CompiledAssembly, path: str) -> None:
    """Serialize CompiledAssembly to a JSON file."""
    from rivet_cli.rendering.json_out import render_compile_json

    data = render_compile_json(compiled)
    Path(path).write_text(data, encoding="utf-8")
