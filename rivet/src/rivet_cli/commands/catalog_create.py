"""Catalog creation wizard — pure helpers, prompt helpers, and interactive flow."""

from __future__ import annotations

import concurrent.futures
import getpass
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from rivet_bridge.plugins import register_optional_plugins
from rivet_cli.errors import (
    RVT_850,
    RVT_880,
    RVT_881,
    RVT_882,
    RVT_883,
    RVT_884,
    RVT_885,
    RVT_886,
    CLIError,
    format_cli_error,
)
from rivet_config import load_config
from rivet_core.plugins import PluginRegistry

CATALOG_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
CATALOG_NAME_MAX_LEN = 64


@dataclass
class WizardState:
    """Accumulated user choices as the wizard progresses."""

    catalog_type: str = ""
    catalog_name: str = ""
    required_opts: dict[str, str] = field(default_factory=dict)
    optional_opts: dict[str, Any] = field(default_factory=dict)
    credential_opts: dict[str, str] = field(default_factory=dict)


def validate_catalog_name(name: str, existing_names: set[str]) -> str | None:
    """Return an error message if *name* is invalid, or ``None`` if valid."""
    if not name:
        return "Catalog name must not be empty."
    if len(name) > CATALOG_NAME_MAX_LEN:
        return f"Catalog name must be at most {CATALOG_NAME_MAX_LEN} characters."
    if not CATALOG_NAME_PATTERN.match(name):
        return "Catalog name must match [a-z][a-z0-9_]* (lowercase, start with letter)."
    if name in existing_names:
        return f"Catalog name '{name}' already exists in this profile."
    return None


def suggest_default_name(catalog_type: str) -> str:
    """Return a default catalog name derived from the plugin type."""
    return f"my_{catalog_type}"


def suggest_env_var_name(catalog_name: str, option_name: str) -> str:
    """Return a ``${CATALOG_NAME_OPTION_NAME}`` style env-var reference."""
    return f"${{{catalog_name.upper()}_{option_name.upper()}}}"


def is_env_var_ref(value: str) -> bool:
    """Return ``True`` if *value* looks like a ``${...}`` env-var reference."""
    return value.startswith("${") and value.endswith("}")


_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def resolve_env_vars(options: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Resolve ``${VAR}`` references in option values from the environment.

    Returns (resolved_options, list_of_missing_var_names).
    """
    resolved = {}
    missing: list[str] = []
    for key, value in options.items():
        if isinstance(value, str) and _ENV_PATTERN.search(value):

            def _replace(m: re.Match[str]) -> str:
                var = m.group(1)
                val = os.environ.get(var)
                if val is None:
                    missing.append(var)
                    return m.group(0)
                return val

            resolved[key] = _ENV_PATTERN.sub(_replace, value)
        else:
            resolved[key] = value
    return resolved, missing


def find_missing_required_options(
    required_options: list[str], provided_keys: set[str]
) -> list[str]:
    """Return required option names not present in *provided_keys*."""
    return [opt for opt in required_options if opt not in provided_keys]


def merge_optional_defaults(
    optional_options: dict[str, Any], user_provided: dict[str, Any]
) -> dict[str, Any]:
    """Fill missing keys in *user_provided* with non-None defaults from *optional_options*."""
    merged = {k: v for k, v in optional_options.items() if v is not None}
    merged.update(user_provided)
    return merged


def build_catalog_block(state: WizardState) -> dict[str, Any]:
    """Merge all collected options with ``type`` into a flat dict."""
    block: dict[str, Any] = {"type": state.catalog_type}
    block.update(state.required_opts)
    block.update(state.optional_opts)
    block.update(state.credential_opts)
    return block


def mask_credentials_for_preview(
    block: dict[str, Any],
    credential_keys: list[str],
    catalog_name: str,
) -> dict[str, Any]:
    """Replace plaintext credential values with ``${ENV_VAR}`` references."""
    masked = dict(block)
    for key in credential_keys:
        if key in masked and not is_env_var_ref(str(masked[key])):
            masked[key] = suggest_env_var_name(catalog_name, key)
    return masked


class CatalogWriteError(Exception):
    """Raised when writing a catalog to the profiles file fails."""

    def __init__(self, cli_error: CLIError) -> None:
        self.cli_error = cli_error
        super().__init__(cli_error.message)


def write_catalog_to_profile(
    profiles_path: Path,
    profile_name: str,
    catalog_name: str,
    catalog_block: dict[str, Any] | None,
    engine_updates: dict[str, list[str]] | None,
) -> None:
    """Write a catalog block into the profiles YAML and optionally update engine catalog lists.

    Handles both single-file (``profiles.yaml``) and per-profile-directory layouts.
    Raises ``CatalogWriteError`` wrapping a ``CLIError`` with ``RVT-883`` on any I/O failure.
    """
    try:
        if profiles_path.is_dir():
            _write_directory_layout(
                profiles_path, profile_name, catalog_name, catalog_block, engine_updates
            )
        else:
            _write_single_file(
                profiles_path, profile_name, catalog_name, catalog_block, engine_updates
            )
    except CatalogWriteError:
        raise
    except Exception as exc:
        raise CatalogWriteError(
            CLIError(
                code=RVT_883,
                message=f"Failed to write catalog to {profiles_path}: {exc}",
                remediation="Check file permissions and available disk space.",
            )
        ) from exc


def _write_single_file(
    path: Path,
    profile_name: str,
    catalog_name: str,
    catalog_block: dict[str, Any] | None,
    engine_updates: dict[str, list[str]] | None,
) -> None:
    data = yaml.safe_load(path.read_text()) if path.exists() else {}
    if not isinstance(data, dict):
        data = {}
    profile = data.setdefault(profile_name, {})
    if not isinstance(profile, dict):
        profile = {}
        data[profile_name] = profile
    catalogs = profile.setdefault("catalogs", {})
    if not isinstance(catalogs, dict):
        catalogs = {}
        profile["catalogs"] = catalogs
    if catalog_block is not None:
        catalogs[catalog_name] = catalog_block
    _apply_engine_updates(profile, engine_updates)
    path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))


def _write_directory_layout(
    dir_path: Path,
    profile_name: str,
    catalog_name: str,
    catalog_block: dict[str, Any] | None,
    engine_updates: dict[str, list[str]] | None,
) -> None:
    file_path = dir_path / f"{profile_name}.yaml"
    data = yaml.safe_load(file_path.read_text()) if file_path.exists() else {}
    if not isinstance(data, dict):
        data = {}
    catalogs = data.setdefault("catalogs", {})
    if not isinstance(catalogs, dict):
        catalogs = {}
        data["catalogs"] = catalogs
    if catalog_block is not None:
        catalogs[catalog_name] = catalog_block
    _apply_engine_updates(data, engine_updates)
    file_path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))


def filter_compatible_engines(
    profile: Any,
    catalog_type: str,
    registry: Any,
) -> list[Any]:
    """Return engines from *profile* that support *catalog_type*.

    An engine is compatible if:
    - the registry has an adapter for ``(engine.type, catalog_type)``, OR
    - the engine plugin's ``supported_catalog_types`` includes ``catalog_type``.
    """
    compatible = []
    for engine in profile.engines:
        if registry.get_adapter(engine.type, catalog_type) is not None:
            compatible.append(engine)
            continue
        plugin = registry.get_engine_plugin(engine.type)
        if plugin is not None and catalog_type in plugin.supported_catalog_types:
            compatible.append(engine)
    return compatible


def update_engine_catalogs(engine_catalogs_list: list[str], catalog_name: str) -> list[str]:
    """Return a new list with *catalog_name* appended if not already present."""
    if catalog_name in engine_catalogs_list:
        return list(engine_catalogs_list)
    return list(engine_catalogs_list) + [catalog_name]


def test_connection(
    plugin: Any,
    catalog_name: str,
    options: dict[str, Any],
    timeout: float = 30.0,
) -> tuple[bool, float, str | None]:
    """Test catalog connectivity using the plugin's test_connection method.

    Returns ``(success, elapsed_seconds, error_message)``.
    On failure or timeout, the error_message describes the problem (RVT-886).
    Requirements: 9.2, 9.3, 9.4, 9.6, 9.7, 13.7
    """
    start = time.monotonic()
    try:
        catalog = plugin.instantiate(catalog_name, options)
        check = getattr(plugin, "test_connection", None)
        target = check if check is not None else lambda c: plugin.list_tables(c)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(target, catalog)
            try:
                future.result(timeout=timeout)
            except concurrent.futures.TimeoutError:
                elapsed = time.monotonic() - start
                return False, elapsed, f"Connection timed out after {timeout:.0f}s (RVT-886)"
    except Exception as exc:
        elapsed = time.monotonic() - start
        return False, elapsed, f"Connection failed: {exc} (RVT-886)"
    elapsed = time.monotonic() - start
    return True, elapsed, None


test_connection.__test__ = False  # type: ignore[attr-defined]  # prevent pytest collection


def _apply_engine_updates(
    profile_data: dict[str, Any],
    engine_updates: dict[str, list[str]] | None,
) -> None:
    if not engine_updates:
        return
    engines = profile_data.get("engines", [])
    if not isinstance(engines, list):
        return
    for eng in engines:
        if not isinstance(eng, dict):
            continue
        eng_name = eng.get("name", "")
        if eng_name in engine_updates:
            existing = eng.get("catalogs", [])
            if not isinstance(existing, list):
                existing = []
            for cat in engine_updates[eng_name]:
                if cat not in existing:
                    existing.append(cat)
            eng["catalogs"] = existing


def parse_key_value_pairs(pairs: list[str]) -> dict[str, str]:
    """Parse ``["key=value", ...]`` into a dict. Ignores malformed entries."""
    result: dict[str, str] = {}
    for pair in pairs:
        if "=" in pair:
            key, _, value = pair.partition("=")
            result[key.strip()] = value.strip()
    return result


# ── Prompt helpers ─────────────────────────────────────────────────────────────


def prompt_choice(prompt: str, choices: list[str]) -> str:
    """Display a numbered list and return the selected choice string.

    Keeps re-prompting until the user enters a valid number.
    Requirements: 3.2, 5.1
    """
    for i, choice in enumerate(choices, start=1):
        print(f"  {i}. {choice}")
    while True:
        raw = input(f"{prompt} [1-{len(choices)}]: ").strip()
        if raw.isdigit():
            idx = int(raw)
            if 1 <= idx <= len(choices):
                return choices[idx - 1]
        print(f"  Please enter a number between 1 and {len(choices)}.")


def prompt_value(prompt: str, default: str | None = None, required: bool = False) -> str:
    """Prompt for a text value, showing the default if provided.

    Re-prompts when the user provides an empty value for a required field with no default.
    Requirements: 5.1, 5.2, 6.1, 6.2
    """
    display = f"{prompt} [{default}]: " if default is not None else f"{prompt}: "
    while True:
        raw = input(display).strip()
        if raw:
            return raw
        if default is not None:
            return default
        if not required:
            return ""
        print("  This field is required.")


def prompt_credential(prompt: str, env_var_suggestion: str) -> str:
    """Prompt for a credential value using masked input.

    Displays a recommendation to use the suggested env-var reference.
    Empty input defaults to the env var reference.
    Requirements: 7.1, 7.2, 7.5
    """
    print(f"  Press enter to use {env_var_suggestion} from environment.")
    return getpass.getpass(f"{prompt} [{env_var_suggestion}]: ")


def prompt_confirm(prompt: str, default: bool = True) -> bool:
    """Prompt for a yes/no confirmation.

    Returns *default* when the user presses Enter without input.
    Requirements: 6.1, 6.2, 7.4
    """
    hint = "[Y/n]" if default else "[y/N]"
    while True:
        raw = input(f"{prompt} {hint}: ").strip().lower()
        if not raw:
            return default
        if raw in ("y", "yes"):
            return True
        if raw in ("n", "no"):
            return False
        print("  Please enter 'y' or 'n'.")


# ── Wizard flow (interactive + non-interactive) ───────────────────────────────


def _is_non_interactive(catalog_type: str | None, catalog_name: str | None) -> bool:
    """Non-interactive mode when both --type and --name are provided."""
    return catalog_type is not None and catalog_name is not None


def _run_non_interactive(
    *,
    catalog_type: str,
    catalog_name: str,
    options: list[str],
    credentials: list[str],
    no_test: bool,
    dry_run: bool,
    globals: Any,
    registry: Any,
    config_result: Any,
) -> int:
    """Non-interactive catalog creation — all values from CLI flags.

    Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8
    """
    from rivet_cli.exit_codes import GENERAL_ERROR, SUCCESS, USAGE_ERROR

    # ── Resolve plugin ────────────────────────────────────────────────
    plugin = registry.get_catalog_plugin(catalog_type)
    if plugin is None:
        err = CLIError(
            code=RVT_882,
            message=f"Unknown catalog type '{catalog_type}'.",
            remediation=f"Available types: {', '.join(sorted(registry._catalog_plugins.keys()))}",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return USAGE_ERROR

    # ── Validate catalog name ─────────────────────────────────────────
    existing_names: set[str] = set()
    if config_result.profile is not None:
        existing_names = set(config_result.profile.catalogs.keys())

    name_error = validate_catalog_name(catalog_name, existing_names)
    if name_error is not None and catalog_name not in existing_names:
        err = CLIError(
            code=RVT_884,
            message=name_error,
            remediation="Provide a valid catalog name matching [a-z][a-z0-9_]* (max 64 chars).",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return USAGE_ERROR

    # ── Parse key=value flags ─────────────────────────────────────────
    parsed_options = parse_key_value_pairs(options)
    parsed_credentials = parse_key_value_pairs(credentials)

    # ── Check required options present (Req 2.8) ──────────────────────
    provided_keys = set(parsed_options.keys()) | set(parsed_credentials.keys())
    missing = find_missing_required_options(plugin.required_options, provided_keys)
    if missing:
        err = CLIError(
            code=RVT_881,
            message=f"Missing required options: {', '.join(missing)}",
            remediation="Provide all required options via --option key=value flags.",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return USAGE_ERROR

    # ── Build state ───────────────────────────────────────────────────
    state = WizardState(catalog_type=catalog_type, catalog_name=catalog_name)
    for opt in plugin.required_options:
        state.required_opts[opt] = parsed_options.get(opt, parsed_credentials.get(opt, ""))
    for opt in plugin.credential_options:
        if opt in parsed_credentials:
            state.credential_opts[opt] = parsed_credentials[opt]
        elif opt in parsed_options:
            state.credential_opts[opt] = parsed_options[opt]
    # Remaining parsed options go to optional
    for key, val in parsed_options.items():
        if key not in state.required_opts and key not in state.credential_opts:
            state.optional_opts[key] = val

    state.optional_opts = merge_optional_defaults(
        {k: v for k, v in plugin.optional_options.items() if k not in state.required_opts},
        state.optional_opts,
    )

    # ── Plugin validation ─────────────────────────────────────────────
    all_options: dict[str, Any] = {}
    all_options.update(state.required_opts)
    all_options.update(state.optional_opts)
    all_options.update(state.credential_opts)

    try:
        plugin.validate(all_options)
    except Exception as exc:
        err = CLIError(
            code=RVT_885,
            message=f"Plugin validation failed for '{catalog_type}': {exc}",
            remediation="Check your --option and --credential values.",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return GENERAL_ERROR

    # ── Connection test (unless --no-test) ────────────────────────────
    if not no_test:
        test_options, missing_vars = resolve_env_vars(all_options)
        if missing_vars:
            err = CLIError(
                code=RVT_886,
                message=f"Missing environment variables: {', '.join(missing_vars)}",
                remediation="Set the required environment variables and retry.",
            )
            print(format_cli_error(err, globals.color), file=sys.stderr)
            return GENERAL_ERROR
        print("Testing connection...")
        import traceback

        try:
            success, elapsed, error_msg = test_connection(plugin, state.catalog_name, test_options)
        except Exception:
            print(f"TRACEBACK: {traceback.format_exc()}")
            raise
        if success:
            print(f"Connection successful ({elapsed:.2f}s).")
        else:
            err = CLIError(
                code=RVT_886,
                message=f"Connection test failed for '{state.catalog_name}': {error_msg}",
                remediation="Check your connection options and try again.",
            )
            print(format_cli_error(err, globals.color), file=sys.stderr)
            return GENERAL_ERROR

    # ── Preview (Req 10.1, 10.2, 10.6) — non-interactive: no confirmation prompt ──
    block = build_catalog_block(state)
    masked = mask_credentials_for_preview(
        block, list(state.credential_opts.keys()), state.catalog_name
    )
    preview_yaml = yaml.dump(
        {state.catalog_name: masked}, default_flow_style=False, sort_keys=False
    )
    print("\nConfiguration preview:")
    print(preview_yaml)

    if dry_run:
        return SUCCESS

    # ── Write (Req 11.1, 11.6, 11.7) ─────────────────────────────────
    profiles_path = (
        config_result.manifest.profiles_path
        if config_result.manifest
        else (globals.project_path / "profiles.yaml")
    )
    try:
        write_catalog_to_profile(profiles_path, globals.profile, state.catalog_name, block, None)
    except CatalogWriteError as exc:
        print(format_cli_error(exc.cli_error, globals.color), file=sys.stderr)
        return GENERAL_ERROR

    print(f"\nCatalog '{state.catalog_name}' ({state.catalog_type}) written to {profiles_path}")
    print("\nNext steps:")
    print("  rivet catalog list")
    print("  rivet doctor --check-connections")
    return SUCCESS


def _prompt_engine_association(
    *,
    profile: Any,
    catalog_name: str,
    catalog_type: str,
    profiles_path: Path,
    profile_name: str,
    registry: Any,
    color: bool,
) -> None:
    """Prompt user to associate the new catalog with compatible engines (Req 12.1–12.5)."""
    if profile is None:
        return
    compatible = filter_compatible_engines(profile, catalog_type, registry)
    if not compatible:
        print("No engines in this profile currently support this catalog type.")
        return
    print("\nCompatible engines found:")
    for i, eng in enumerate(compatible, start=1):
        print(f"  {i}. {eng.name} ({eng.type})")
    if not prompt_confirm("Add this catalog to one or more engines?", default=False):
        return
    # Collect engine selections (comma-separated numbers or "all")
    while True:
        raw = input(
            f"Enter engine numbers to associate [1-{len(compatible)}, comma-separated, or 'all']: "
        ).strip()
        if raw.lower() == "all":
            selected = compatible
            break
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        indices = []
        valid = True
        for p in parts:
            if p.isdigit() and 1 <= int(p) <= len(compatible):
                indices.append(int(p) - 1)
            else:
                print(f"  Invalid selection '{p}'. Enter numbers between 1 and {len(compatible)}.")
                valid = False
                break
        if valid and indices:
            selected = [compatible[i] for i in indices]
            break
        if valid:
            print("  Please enter at least one engine number.")
    engine_updates = {eng.name: [catalog_name] for eng in selected}
    write_catalog_to_profile(profiles_path, profile_name, catalog_name, None, engine_updates)
    names = ", ".join(eng.name for eng in selected)
    print(f"Catalog '{catalog_name}' added to engine(s): {names}")


def run_catalog_create(
    *,
    catalog_type: str | None,
    catalog_name: str | None,
    options: list[str],
    credentials: list[str],
    no_test: bool,
    dry_run: bool,
    globals: Any,
) -> int:
    """Catalog creation wizard — interactive or non-interactive.

    Non-interactive mode is activated when both ``--type`` and ``--name`` are provided.
    Returns an exit code.
    """
    from rivet_cli.exit_codes import INTERRUPTED

    try:
        return _run_catalog_create_inner(
            catalog_type=catalog_type,
            catalog_name=catalog_name,
            options=options,
            credentials=credentials,
            no_test=no_test,
            dry_run=dry_run,
            globals=globals,
        )
    except KeyboardInterrupt:
        return INTERRUPTED


def _validate_catalog_input(
    globals: Any,
) -> tuple[Any, int | None]:
    """Validate project manifest and profile exist.

    Returns ``(config_result, error_exit_code)``.  When ``error_exit_code`` is
    not ``None`` the caller should return it immediately.
    """
    from rivet_cli.exit_codes import USAGE_ERROR

    manifest_path = globals.project_path / "rivet.yaml"
    if not manifest_path.exists():
        err = CLIError(
            code=RVT_850,
            message=f"No rivet.yaml found in {globals.project_path}.",
            remediation="Run 'rivet init' to create a project, or use --project to specify the project directory.",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return None, USAGE_ERROR

    config_result = load_config(globals.project_path, globals.profile, strict=False)
    if config_result.profile is None:
        err = CLIError(
            code=RVT_880,
            message=f"Profile '{globals.profile}' not found.",
            remediation="Available profiles can be found in your profiles file. Use --profile to specify a valid profile.",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return None, USAGE_ERROR

    return config_result, None


def _resolve_catalog_type(
    globals: Any,
) -> tuple[Any, list[str], int | None]:
    """Discover plugins and return ``(registry, available_types, error_exit_code)``."""
    from rivet_cli.exit_codes import GENERAL_ERROR

    registry = PluginRegistry()
    registry.register_builtins()
    register_optional_plugins(registry)

    available_types = sorted(registry._catalog_plugins.keys())
    if not available_types:
        err = CLIError(
            code=RVT_882,
            message="No catalog plugins are registered.",
            remediation="Install a catalog plugin package (e.g. rivet-duckdb, rivet-postgres) and retry.",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return registry, [], GENERAL_ERROR

    return registry, available_types, None


def _prompt_credentials(plugin: Any, state: WizardState) -> None:
    """Prompt for credential options, handling auth groups and plaintext warnings.

    Mutates *state* in place.
    """
    cred_groups = getattr(plugin, "credential_groups", {})
    env_hints = getattr(plugin, "env_var_hints", {})
    if cred_groups:
        auth_types = sorted(cred_groups.keys())
        print("Authentication method:")
        auth_type = prompt_choice("Select auth type", auth_types)
        state.optional_opts["auth_type"] = auth_type
        cred_keys = cred_groups[auth_type]
    else:
        cred_keys = plugin.credential_options

    for opt in cred_keys:
        env_hint = env_hints.get(opt)
        env_var = f"${{{env_hint}}}" if env_hint else suggest_env_var_name(state.catalog_name, opt)
        value = prompt_credential(opt, env_var)
        if not value:
            value = env_var
        if value and not is_env_var_ref(value):
            print(f"  Warning: plaintext credential for '{opt}' will be stored insecurely.")
            if not prompt_confirm("Continue with plaintext value?", default=False):
                value = prompt_credential(opt, env_var)
                if not value:
                    value = env_var
        state.credential_opts[opt] = value


def _prompt_catalog_config(
    plugin: Any,
    state: WizardState,
    existing_names: set[str],
    selected_type: str,
    globals: Any,
) -> int | None:
    """Run the interactive prompts for options, credentials, and validation.

    Mutates *state* in place.  Returns an error exit code on abort, or ``None``
    on success.
    """
    from rivet_cli.exit_codes import GENERAL_ERROR

    # ── Required options ──────────────────────────────────────────────
    for opt in plugin.required_options:
        default = plugin.optional_options.get(opt)
        default_str = str(default) if default is not None else None
        state.required_opts[opt] = prompt_value(opt, default=default_str, required=True)

    # ── Optional options ──────────────────────────────────────────────
    optional_keys = [k for k in plugin.optional_options if k not in state.required_opts]
    # Skip 'endpoints' for rest_api - handle separately below
    if selected_type == "rest_api":
        optional_keys = [k for k in optional_keys if k != "endpoints"]

    if optional_keys and prompt_confirm("Configure optional settings?", default=False):
        import json

        for opt in optional_keys:
            default = plugin.optional_options[opt]
            default_str = str(default) if default is not None else None
            value = prompt_value(opt, default=default_str)
            if value:
                # If user pressed Enter, use the original default value
                if value == default_str:
                    state.optional_opts[opt] = default
                else:
                    # Parse the user input to match the default type
                    if isinstance(default, dict):
                        try:
                            state.optional_opts[opt] = json.loads(value)
                        except json.JSONDecodeError:
                            state.optional_opts[opt] = value
                    elif isinstance(default, int):
                        try:
                            state.optional_opts[opt] = int(value)
                        except ValueError:
                            state.optional_opts[opt] = value
                    elif isinstance(default, float):
                        try:
                            state.optional_opts[opt] = float(value)
                        except ValueError:
                            state.optional_opts[opt] = value
                    elif isinstance(default, bool):
                        state.optional_opts[opt] = value.lower() in ("true", "yes", "1", "y")
                    else:
                        state.optional_opts[opt] = value

    # ── REST API endpoint configuration ───────────────────────────────
    if selected_type == "rest_api":
        if prompt_confirm("Configure endpoints interactively?", default=False):
            endpoints = {}
            print("  Enter endpoint configurations (leave name empty to finish):")
            while True:
                endpoint_name = prompt_value("  Endpoint name", default=None, required=False)
                if not endpoint_name:
                    break
                endpoint_path = prompt_value("  Path", default=None, required=True)
                endpoint_method = prompt_value("  Method", default="GET", required=False) or "GET"
                endpoints[endpoint_name] = {
                    "path": endpoint_path,
                    "method": endpoint_method.upper(),
                }
                print(
                    f"  Added endpoint '{endpoint_name}': {endpoint_method.upper()} {endpoint_path}"
                )

            if endpoints:
                state.optional_opts["endpoints"] = endpoints
            else:
                state.optional_opts["endpoints"] = {}
        else:
            state.optional_opts["endpoints"] = {}

    # ── Credential options ────────────────────────────────────────────
    _prompt_credentials(plugin, state)

    # ── Plugin validation loop ────────────────────────────────────────
    all_options = dict(state.required_opts)
    all_options.update(state.optional_opts)
    all_options.update(state.credential_opts)

    while True:
        try:
            plugin.validate(all_options)
            print("Configuration validated successfully.")
            break
        except Exception as exc:
            err = CLIError(
                code=RVT_885,
                message=f"Plugin validation failed for '{selected_type}': {exc}",
                remediation="Re-enter the failing option or abort.",
            )
            print(format_cli_error(err, globals.color), file=sys.stderr)
            if not prompt_confirm("Re-enter options?", default=True):
                return GENERAL_ERROR
            for opt in plugin.required_options:
                default = all_options.get(opt)
                default_str = str(default) if default is not None else None
                value = prompt_value(opt, default=default_str, required=True)
                state.required_opts[opt] = value
                all_options[opt] = value

    state.optional_opts = merge_optional_defaults(
        {k: v for k, v in plugin.optional_options.items() if k not in state.required_opts},
        state.optional_opts,
    )
    return None


def _test_connection_interactive(
    plugin: Any,
    state: WizardState,
    all_options: dict[str, Any],
    globals: Any,
) -> int | None:
    """Run the interactive connection-test loop.

    Returns an error exit code on abort, or ``None`` to continue.
    """
    from rivet_cli.exit_codes import GENERAL_ERROR

    if not prompt_confirm("Test connection now?", default=True):
        return None

    while True:
        test_options, missing_vars = resolve_env_vars(all_options)
        if missing_vars:
            print(f"  Missing environment variables: {', '.join(missing_vars)}")
            print("  Set them and retry, or skip the test.")
            print("Options: (r) retry, (s) skip test, (a) abort")
            choice = input("Choice [r/s/a]: ").strip().lower()
            if choice == "s":
                return None
            if choice == "a":
                return GENERAL_ERROR
            continue
        print("Testing connection...")
        import traceback

        try:
            success, elapsed, error_msg = test_connection(plugin, state.catalog_name, test_options)
        except Exception:
            print(f"TRACEBACK: {traceback.format_exc()}")
            raise
        if success:
            print(f"Connection successful ({elapsed:.2f}s).")
            return None
        err = CLIError(
            code=RVT_886,
            message=f"Connection test failed for '{state.catalog_name}': {error_msg}",
            remediation="Check your connection options and try again.",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        print("Options: (r) re-enter options, (s) skip test, (a) abort")
        choice = input("Choice [r/s/a]: ").strip().lower()
        if choice == "s":
            return None
        if choice == "a":
            return GENERAL_ERROR
        for opt in plugin.required_options:
            default = all_options.get(opt)
            default_str = str(default) if default is not None else None
            value = prompt_value(opt, default=default_str, required=True)
            state.required_opts[opt] = value
            all_options[opt] = value


def _create_catalog(
    state: WizardState,
    plugin: Any,
    all_options: dict[str, Any],
    config_result: Any,
    registry: Any,
    globals: Any,
    dry_run: bool,
) -> int:
    """Preview, confirm, write the catalog, and prompt for engine association."""
    from rivet_cli.exit_codes import GENERAL_ERROR, SUCCESS

    block = build_catalog_block(state)
    masked = mask_credentials_for_preview(
        block, list(state.credential_opts.keys()), state.catalog_name
    )
    preview_yaml = yaml.dump(
        {state.catalog_name: masked}, default_flow_style=False, sort_keys=False
    )
    print("\nConfiguration preview:")
    print(preview_yaml)

    if dry_run:
        return SUCCESS

    while True:
        if prompt_confirm("Write this configuration?", default=True):
            break
        print("Options: (r) re-enter options, (a) abort")
        choice = input("Choice [r/a]: ").strip().lower()
        if choice == "a":
            return GENERAL_ERROR
        for opt in plugin.required_options:
            default = all_options.get(opt)
            default_str = str(default) if default is not None else None
            value = prompt_value(opt, default=default_str, required=True)
            state.required_opts[opt] = value
            all_options[opt] = value
        block = build_catalog_block(state)
        masked = mask_credentials_for_preview(
            block, list(state.credential_opts.keys()), state.catalog_name
        )
        preview_yaml = yaml.dump(
            {state.catalog_name: masked}, default_flow_style=False, sort_keys=False
        )
        print("\nConfiguration preview:")
        print(preview_yaml)

    profiles_path = (
        config_result.manifest.profiles_path
        if config_result.manifest
        else (globals.project_path / "profiles.yaml")
    )
    try:
        write_catalog_to_profile(profiles_path, globals.profile, state.catalog_name, block, None)
    except CatalogWriteError as exc:
        print(format_cli_error(exc.cli_error, globals.color), file=sys.stderr)
        return GENERAL_ERROR

    print(f"\nCatalog '{state.catalog_name}' ({state.catalog_type}) written to {profiles_path}")

    _prompt_engine_association(
        profile=config_result.profile,
        catalog_name=state.catalog_name,
        catalog_type=state.catalog_type,
        profiles_path=profiles_path,
        profile_name=globals.profile,
        registry=registry,
        color=globals.color,
    )

    print("\nNext steps:")
    print("  rivet catalog list")
    print("  rivet doctor --check-connections")
    return SUCCESS


def _run_catalog_create_inner(
    *,
    catalog_type: str | None,
    catalog_name: str | None,
    options: list[str],
    credentials: list[str],
    no_test: bool,
    dry_run: bool,
    globals: Any,
) -> int:

    # ── Validate project and profile ──────────────────────────────────
    config_result, err_code = _validate_catalog_input(globals)
    if err_code is not None:
        return err_code

    # ── Discover plugins ──────────────────────────────────────────────
    registry, available_types, err_code = _resolve_catalog_type(globals)
    if err_code is not None:
        return err_code

    # ── Non-interactive mode ──────────────────────────────────────────
    if _is_non_interactive(catalog_type, catalog_name):
        return _run_non_interactive(
            catalog_type=catalog_type,  # type: ignore[arg-type]
            catalog_name=catalog_name,  # type: ignore[arg-type]
            options=options,
            credentials=credentials,
            no_test=no_test,
            dry_run=dry_run,
            globals=globals,
            registry=registry,
            config_result=config_result,
        )

    # ── Interactive: type selection ───────────────────────────────────
    print("Available catalog types:")
    selected_type = prompt_choice("Select catalog type", available_types)
    plugin = registry.get_catalog_plugin(selected_type)
    assert plugin is not None

    # ── Name prompt with validation ───────────────────────────────────
    existing_names: set[str] = set()
    if config_result.profile is not None:
        existing_names = set(config_result.profile.catalogs.keys())

    default_name = suggest_default_name(selected_type)
    state = WizardState(catalog_type=selected_type)

    while True:
        name = prompt_value("Catalog name", default=default_name, required=True)
        error = validate_catalog_name(name, existing_names)
        if error is not None:
            if name in existing_names:
                print(f"  {error}")
                if prompt_confirm(f"Overwrite existing catalog '{name}'?", default=False):
                    break
                continue
            print(f"  {error}")
            continue
        break
    state.catalog_name = name

    # ── Prompt for config (options, credentials, validation) ──────────
    err_code = _prompt_catalog_config(plugin, state, existing_names, selected_type, globals)
    if err_code is not None:
        return err_code

    # ── Connection test ───────────────────────────────────────────────
    all_options: dict[str, Any] = {}
    all_options.update(state.required_opts)
    all_options.update(state.optional_opts)
    all_options.update(state.credential_opts)

    if not no_test:
        err_code = _test_connection_interactive(plugin, state, all_options, globals)
        if err_code is not None:
            return err_code

    # ── Preview, confirm, write ───────────────────────────────────────
    return _create_catalog(state, plugin, all_options, config_result, registry, globals, dry_run)
