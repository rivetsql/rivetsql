"""Engine commands: list engines in the current profile."""

from __future__ import annotations

import json
import sys
from typing import TYPE_CHECKING

from rivet_bridge import register_optional_plugins
from rivet_cli.errors import format_upstream_error
from rivet_cli.exit_codes import GENERAL_ERROR, SUCCESS, USAGE_ERROR
from rivet_config import load_config
from rivet_core import PluginRegistry

if TYPE_CHECKING:
    from rivet_cli.app import GlobalOptions
    from rivet_config.models import EngineConfig, ResolvedProfile


def _load_profile(globals_: GlobalOptions) -> tuple[ResolvedProfile | None, PluginRegistry | None, int | None]:
    """Load config, resolve profile, and set up the plugin registry.

    Returns (profile, registry, error_code). error_code is None on success.
    """
    config_result = load_config(globals_.project_path, globals_.profile)
    if not config_result.success:
        for e in config_result.errors:
            print(
                format_upstream_error(
                    e.code if hasattr(e, "code") else "CFG",
                    e.message,
                    getattr(e, "remediation", None),
                    globals_.color,
                ),
                file=sys.stderr,
            )
        return None, None, GENERAL_ERROR

    profile = config_result.profile
    if profile is None:
        print(
            format_upstream_error(
                "RVT-853",
                "No profile resolved.",
                "Check profiles.yaml and --profile flag.",
                globals_.color,
            ),
            file=sys.stderr,
        )
        return None, None, GENERAL_ERROR

    registry = PluginRegistry()
    registry.register_builtins()
    register_optional_plugins(registry)

    return profile, registry, None


def engine_list(
    *,
    globals: GlobalOptions,
    engine_name: str | None = None,
    format: str = "text",
) -> int:
    """Handle ``rivet engine list``.

    - No arguments: list all engines with type, catalogs, and plugin availability.
    - engine_name: show details for a single engine.
    - format: text | json.
    """
    globals_ = globals
    profile, registry, err = _load_profile(globals_)
    if err is not None:
        return err
    assert profile is not None and registry is not None

    engines = profile.engines

    if engine_name is not None:
        matching = [e for e in engines if e.name == engine_name]
        if not matching:
            from rivet_cli.errors import CLIError, format_cli_error

            err_obj = CLIError(
                code="RVT-890",
                message=f"Engine '{engine_name}' not found.",
                remediation="Run 'rivet engine list' to see available engines.",
            )
            print(format_cli_error(err_obj, globals_.color), file=sys.stderr)
            return USAGE_ERROR
        engines = matching

    if format == "json":
        print(_render_json(engines, registry))
        return SUCCESS

    print(_render_text(engines, registry, profile, globals_.color))
    return SUCCESS


def _render_json(engines: list[EngineConfig], registry: PluginRegistry) -> str:
    rows = []
    for eng in engines:
        plugin = registry.get_engine_plugin(eng.type)
        rows.append({
            "name": eng.name,
            "type": eng.type,
            "catalogs": eng.catalogs,
            "plugin_available": plugin is not None,
            "supported_catalog_types": list(plugin.supported_catalog_types.keys()) if plugin else [],
            "options": eng.options,
        })
    return json.dumps(rows, indent=2)


def _render_text(
    engines: list[EngineConfig],
    registry: PluginRegistry,
    profile: ResolvedProfile,
    color: bool,
) -> str:
    if not engines:
        return "No engines configured in this profile."

    lines: list[str] = []
    default_engine = profile.default_engine

    for eng in engines:
        plugin = registry.get_engine_plugin(eng.type)
        status = "✓" if plugin is not None else "✗ plugin not found"
        if color and plugin is None:
            status = f"\033[31m{status}\033[0m"
        elif color and plugin is not None:
            status = f"\033[32m{status}\033[0m"

        default_marker = " (default)" if eng.name == default_engine else ""
        lines.append(f"  {eng.name}{default_marker}")
        lines.append(f"    type:     {eng.type}")
        lines.append(f"    plugin:   {status}")
        lines.append(f"    catalogs: {', '.join(eng.catalogs) if eng.catalogs else '(none)'}")
        if plugin is not None:
            supported = ", ".join(sorted(plugin.supported_catalog_types.keys()))
            lines.append(f"    supports: {supported}")
        if eng.options:
            for k, v in eng.options.items():
                lines.append(f"    {k}: {v}")

    header = f"Engines in profile '{profile.name}':"
    return header + "\n" + "\n".join(lines)
