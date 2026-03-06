"""Engine creation wizard — interactive and non-interactive flows."""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from rivet_bridge.plugins import register_optional_plugins
from rivet_cli.commands.catalog_create import (
    prompt_choice,
    prompt_confirm,
    prompt_value,
    suggest_env_var_name,
)
from rivet_cli.errors import (
    RVT_880,
    RVT_881,
    RVT_882,
    RVT_883,
    RVT_885,
    CLIError,
    format_cli_error,
)
from rivet_config import load_config
from rivet_core.plugins import PluginRegistry

ENGINE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
ENGINE_NAME_MAX_LEN = 64


@dataclass
class EngineWizardState:
    """Accumulated user choices as the engine wizard progresses."""

    engine_type: str = ""
    engine_name: str = ""
    catalogs: list[str] = field(default_factory=list)
    required_opts: dict[str, str] = field(default_factory=dict)
    optional_opts: dict[str, Any] = field(default_factory=dict)
    credential_opts: dict[str, str] = field(default_factory=dict)
    set_as_default: bool = False


def validate_engine_name(name: str, existing_names: set[str]) -> str | None:
    """Return an error message if *name* is invalid, or ``None`` if valid."""
    if not name:
        return "Engine name must not be empty."
    if len(name) > ENGINE_NAME_MAX_LEN:
        return f"Engine name must be at most {ENGINE_NAME_MAX_LEN} characters."
    if not ENGINE_NAME_PATTERN.match(name):
        return "Engine name must match [a-z][a-z0-9_]* (lowercase, start with letter)."
    if name in existing_names:
        return f"Engine name '{name}' already exists in this profile."
    return None


def suggest_default_engine_name(engine_type: str) -> str:
    return f"my_{engine_type}"


def build_engine_block(state: EngineWizardState) -> dict[str, Any]:
    """Build the engine dict for YAML serialization."""
    block: dict[str, Any] = {
        "name": state.engine_name,
        "type": state.engine_type,
        "catalogs": state.catalogs,
    }
    block.update(state.required_opts)
    block.update(state.optional_opts)
    block.update(state.credential_opts)
    return block


class EngineWriteError(Exception):
    def __init__(self, cli_error: CLIError) -> None:
        self.cli_error = cli_error
        super().__init__(cli_error.message)


def write_engine_to_profile(
    profiles_path: Path,
    profile_name: str,
    engine_block: dict[str, Any],
    set_as_default: bool,
) -> None:
    """Write an engine block into the profiles YAML.

    Handles both single-file and per-profile-directory layouts.
    """
    try:
        if profiles_path.is_dir():
            _write_directory_layout(profiles_path, profile_name, engine_block, set_as_default)
        else:
            _write_single_file(profiles_path, profile_name, engine_block, set_as_default)
    except EngineWriteError:
        raise
    except Exception as exc:
        raise EngineWriteError(CLIError(
            code=RVT_883,
            message=f"Failed to write engine to {profiles_path}: {exc}",
            remediation="Check file permissions and available disk space.",
        )) from exc


def _write_single_file(
    path: Path,
    profile_name: str,
    engine_block: dict[str, Any],
    set_as_default: bool,
) -> None:
    data = yaml.safe_load(path.read_text()) if path.exists() else {}
    if not isinstance(data, dict):
        data = {}
    profile = data.setdefault(profile_name, {})
    if not isinstance(profile, dict):
        profile = {}
        data[profile_name] = profile
    engines = profile.setdefault("engines", [])
    if not isinstance(engines, list):
        engines = []
        profile["engines"] = engines
    # Replace existing engine with same name, or append
    replaced = False
    for i, eng in enumerate(engines):
        if isinstance(eng, dict) and eng.get("name") == engine_block["name"]:
            engines[i] = engine_block
            replaced = True
            break
    if not replaced:
        engines.append(engine_block)
    if set_as_default:
        profile["default_engine"] = engine_block["name"]
    path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))


def _write_directory_layout(
    dir_path: Path,
    profile_name: str,
    engine_block: dict[str, Any],
    set_as_default: bool,
) -> None:
    file_path = dir_path / f"{profile_name}.yaml"
    data = yaml.safe_load(file_path.read_text()) if file_path.exists() else {}
    if not isinstance(data, dict):
        data = {}
    engines = data.setdefault("engines", [])
    if not isinstance(engines, list):
        engines = []
        data["engines"] = engines
    replaced = False
    for i, eng in enumerate(engines):
        if isinstance(eng, dict) and eng.get("name") == engine_block["name"]:
            engines[i] = engine_block
            replaced = True
            break
    if not replaced:
        engines.append(engine_block)
    if set_as_default:
        data["default_engine"] = engine_block["name"]
    file_path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))


def parse_key_value_pairs(pairs: list[str]) -> dict[str, str]:
    """Parse ``key=value`` pairs from CLI ``--option`` flags."""
    result: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"Invalid option format: '{pair}'. Expected key=value.")
        key, _, value = pair.partition("=")
        result[key.strip()] = value.strip()
    return result


def run_engine_create(
    *,
    engine_type: str | None,
    engine_name: str | None,
    catalogs: list[str],
    options: list[str],
    credentials: list[str],
    set_default: bool,
    dry_run: bool,
    globals: Any,
) -> int:
    """Entry point for ``rivet engine create``."""
    from rivet_cli.exit_codes import GENERAL_ERROR

    globals_ = globals

    # ── Validate project and profile ──────────────────────────────
    config_result = load_config(globals_.project_path, globals_.profile)
    if not config_result.success:
        for e in config_result.errors:
            from rivet_cli.errors import format_upstream_error
            print(format_upstream_error(
                e.code if hasattr(e, "code") else "CFG",
                e.message,
                getattr(e, "remediation", None),
                globals_.color,
            ), file=sys.stderr)
        return GENERAL_ERROR

    if config_result.manifest is None:
        err = CLIError(code=RVT_880, message="No rivet.yaml found.", remediation="Run 'rivet init' first.")
        print(format_cli_error(err, globals_.color), file=sys.stderr)
        return GENERAL_ERROR

    # ── Discover plugins ──────────────────────────────────────────
    registry = PluginRegistry()
    registry.register_builtins()
    register_optional_plugins(registry)

    available_types = sorted(registry._engine_plugins.keys())
    if not available_types:
        err = CLIError(code=RVT_882, message="No engine plugins discovered.", remediation="Install an engine plugin package (e.g. rivet-duckdb).")
        print(format_cli_error(err, globals_.color), file=sys.stderr)
        return GENERAL_ERROR

    # ── Non-interactive mode ──────────────────────────────────────
    if engine_type is not None and engine_name is not None:
        return _run_non_interactive(
            engine_type=engine_type,
            engine_name=engine_name,
            catalogs=catalogs,
            options=options,
            credentials=credentials,
            set_default=set_default,
            dry_run=dry_run,
            globals_=globals_,
            registry=registry,
            config_result=config_result,
            available_types=available_types,
        )

    # ── Interactive mode ──────────────────────────────────────────
    return _run_interactive(
        globals_=globals_,
        registry=registry,
        config_result=config_result,
        available_types=available_types,
        dry_run=dry_run,
    )


def _run_non_interactive(
    *,
    engine_type: str,
    engine_name: str,
    catalogs: list[str],
    options: list[str],
    credentials: list[str],
    set_default: bool,
    dry_run: bool,
    globals_: Any,
    registry: PluginRegistry,
    config_result: Any,
    available_types: list[str],
) -> int:
    from rivet_cli.exit_codes import GENERAL_ERROR, SUCCESS, USAGE_ERROR

    if engine_type not in available_types:
        err = CLIError(
            code=RVT_885,
            message=f"Unknown engine type '{engine_type}'.",
            remediation=f"Available types: {', '.join(available_types)}.",
        )
        print(format_cli_error(err, globals_.color), file=sys.stderr)
        return USAGE_ERROR

    existing_names: set[str] = set()
    if config_result.profile is not None:
        existing_names = {e.name for e in config_result.profile.engines}

    name_err = validate_engine_name(engine_name, existing_names)
    if name_err is not None:
        err = CLIError(code="RVT-884", message=name_err, remediation="Choose a valid engine name.")
        print(format_cli_error(err, globals_.color), file=sys.stderr)
        return USAGE_ERROR

    plugin = registry.get_engine_plugin(engine_type)
    assert plugin is not None

    try:
        parsed_opts = parse_key_value_pairs(options)
    except ValueError as exc:
        err = CLIError(code=RVT_881, message=str(exc), remediation="Use --option key=value format.")
        print(format_cli_error(err, globals_.color), file=sys.stderr)
        return USAGE_ERROR

    try:
        parsed_creds = parse_key_value_pairs(credentials)
    except ValueError as exc:
        err = CLIError(code=RVT_881, message=str(exc), remediation="Use --credential key=value format.")
        print(format_cli_error(err, globals_.color), file=sys.stderr)
        return USAGE_ERROR

    # Check required options
    required = getattr(plugin, "required_options", [])
    all_opts = {**parsed_opts, **parsed_creds}
    missing = [r for r in required if r not in all_opts]
    if missing:
        err = CLIError(
            code=RVT_881,
            message=f"Missing required options: {', '.join(missing)}.",
            remediation=f"Provide: {' '.join(f'--option {m}=VALUE' for m in missing)}",
        )
        print(format_cli_error(err, globals_.color), file=sys.stderr)
        return USAGE_ERROR

    # Validate via plugin
    try:
        plugin.validate(all_opts)
    except Exception as exc:
        err = CLIError(code=RVT_885, message=f"Plugin validation failed: {exc}", remediation="Fix the engine options.")
        print(format_cli_error(err, globals_.color), file=sys.stderr)
        return GENERAL_ERROR

    state = EngineWizardState(
        engine_type=engine_type,
        engine_name=engine_name,
        catalogs=catalogs,
        required_opts=parsed_opts,
        credential_opts=parsed_creds,
        set_as_default=set_default,
    )
    block = build_engine_block(state)

    if dry_run:
        preview = yaml.dump([block], default_flow_style=False, sort_keys=False)
        print("Engine configuration preview (dry-run):")
        print(preview)
        return SUCCESS

    profiles_path = config_result.manifest.profiles_path if config_result.manifest else (globals_.project_path / "profiles.yaml")
    try:
        write_engine_to_profile(profiles_path, globals_.profile, block, set_default)
    except EngineWriteError as exc:
        print(format_cli_error(exc.cli_error, globals_.color), file=sys.stderr)
        return GENERAL_ERROR

    print(f"\nEngine '{engine_name}' ({engine_type}) written to {profiles_path}")
    if set_default:
        print("Set as default engine.")
    _print_next_steps()
    return SUCCESS


def _run_interactive(
    *,
    globals_: Any,
    registry: PluginRegistry,
    config_result: Any,
    available_types: list[str],
    dry_run: bool,
) -> int:
    from rivet_cli.exit_codes import GENERAL_ERROR, SUCCESS

    # ── Type selection ────────────────────────────────────────────
    print("Available engine types:")
    selected_type = prompt_choice("Select engine type", available_types)
    plugin = registry.get_engine_plugin(selected_type)
    assert plugin is not None

    # ── Name prompt ───────────────────────────────────────────────
    existing_names: set[str] = set()
    if config_result.profile is not None:
        existing_names = {e.name for e in config_result.profile.engines}

    default_name = suggest_default_engine_name(selected_type)
    state = EngineWizardState(engine_type=selected_type)

    while True:
        name = prompt_value("Engine name", default=default_name, required=True)
        error = validate_engine_name(name, existing_names)
        if error is not None:
            if name in existing_names:
                print(f"  {error}")
                if prompt_confirm(f"Overwrite existing engine '{name}'?", default=False):
                    break
                continue
            print(f"  {error}")
            continue
        break
    state.engine_name = name

    # ── Catalog association ───────────────────────────────────────
    _prompt_catalog_association(state, plugin, config_result.profile)

    # ── Required options ──────────────────────────────────────────
    required = getattr(plugin, "required_options", [])
    for opt in required:
        value = prompt_value(opt, required=True)
        state.required_opts[opt] = value

    # ── Credential options ────────────────────────────────────────
    cred_opts = getattr(plugin, "credential_options", [])
    if cred_opts:
        print("\nCredential options (use ${ENV_VAR} for environment variable references):")
        for opt in cred_opts:
            env_suggestion = suggest_env_var_name(state.engine_name, opt)
            from rivet_cli.commands.catalog_create import prompt_credential
            value = prompt_credential(opt, env_suggestion)
            if value:
                state.credential_opts[opt] = value

    # ── Optional options ──────────────────────────────────────────
    optional = getattr(plugin, "optional_options", {})
    if optional and prompt_confirm("Configure optional settings?", default=False):
        for opt, default in optional.items():
            default_str = str(default) if default is not None else None
            value = prompt_value(opt, default=default_str, required=False)
            if value and value != default_str:
                # Try to parse as appropriate type
                state.optional_opts[opt] = _coerce_value(value, default)

    # ── Default engine ────────────────────────────────────────────
    state.set_as_default = prompt_confirm("Set as default engine?", default=len(existing_names) == 0)

    # ── Preview and confirm ───────────────────────────────────────
    block = build_engine_block(state)
    preview = yaml.dump([block], default_flow_style=False, sort_keys=False)
    print("\nEngine configuration preview:")
    print(preview)

    if dry_run:
        print("(dry-run — not written)")
        return SUCCESS

    if not prompt_confirm("Write this engine configuration?", default=True):
        print("Aborted.")
        return SUCCESS

    profiles_path = config_result.manifest.profiles_path if config_result.manifest else (globals_.project_path / "profiles.yaml")
    try:
        write_engine_to_profile(profiles_path, globals_.profile, block, state.set_as_default)
    except EngineWriteError as exc:
        print(format_cli_error(exc.cli_error, globals_.color), file=sys.stderr)
        return GENERAL_ERROR

    print(f"\nEngine '{state.engine_name}' ({state.engine_type}) written to {profiles_path}")
    if state.set_as_default:
        print("Set as default engine.")
    _print_next_steps()
    return SUCCESS


def _prompt_catalog_association(
    state: EngineWizardState,
    plugin: Any,
    profile: Any,
) -> None:
    """Prompt user to associate catalogs with the new engine."""
    if profile is None:
        return

    supported_types = set(plugin.supported_catalog_types.keys())
    available_catalogs = [
        cat for cat in profile.catalogs.values()
        if cat.type in supported_types
    ]

    if not available_catalogs:
        print(f"\nNo catalogs in this profile are compatible with engine type '{state.engine_type}'.")
        print(f"  Supported catalog types: {', '.join(sorted(supported_types))}")
        return

    print(f"\nCompatible catalogs for '{state.engine_type}' engine:")
    for i, cat in enumerate(available_catalogs, start=1):
        print(f"  {i}. {cat.name} ({cat.type})")

    if not prompt_confirm("Associate catalogs with this engine?", default=True):
        return

    while True:
        raw = input(f"Enter catalog numbers [1-{len(available_catalogs)}, comma-separated, or 'all']: ").strip()
        if raw.lower() == "all":
            state.catalogs = [cat.name for cat in available_catalogs]
            break
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        indices = []
        valid = True
        for p in parts:
            if p.isdigit() and 1 <= int(p) <= len(available_catalogs):
                indices.append(int(p) - 1)
            else:
                print(f"  Invalid selection '{p}'. Enter numbers between 1 and {len(available_catalogs)}.")
                valid = False
                break
        if valid and indices:
            state.catalogs = [available_catalogs[i].name for i in indices]
            break
        if valid:
            print("  Please enter at least one catalog number.")

    print(f"  Selected: {', '.join(state.catalogs)}")


def _coerce_value(value: str, default: Any) -> Any:
    """Try to coerce a string value to match the type of the default."""
    if default is None:
        return value
    if isinstance(default, bool):
        return value.lower() in ("true", "1", "yes")
    if isinstance(default, int):
        try:
            return int(value)
        except ValueError:
            return value
    if isinstance(default, list):
        return [v.strip() for v in value.split(",") if v.strip()]
    return value


def _print_next_steps() -> None:
    print("\nNext steps:")
    print("  rivet engine list")
    print("  rivet doctor")
