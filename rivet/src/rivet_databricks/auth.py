"""Shared credential resolution for Unity Catalog and Databricks.

Supports: PAT, OAuth M2M, Azure Entra ID, env vars, ~/.databrickscfg, cloud-native auth.

Resolution order:
  1. Explicit options (token, client_id+client_secret, azure_* triple)
  2. Environment variables (DATABRICKS_TOKEN, DATABRICKS_CLIENT_ID, etc.)
  3. Databricks config file (~/.databrickscfg)
  4. Cloud-native auth (azure_cli, gcp_login)
"""

from __future__ import annotations

import configparser
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from rivet_core.errors import PluginValidationError, plugin_error

# Auth types
AUTH_TYPE_PAT = "pat"
AUTH_TYPE_OAUTH_M2M = "oauth_m2m"
AUTH_TYPE_AZURE_CLI = "azure_cli"
AUTH_TYPE_GCP_LOGIN = "gcp_login"

VALID_AUTH_TYPES = frozenset({AUTH_TYPE_PAT, AUTH_TYPE_OAUTH_M2M, AUTH_TYPE_AZURE_CLI, AUTH_TYPE_GCP_LOGIN})

# Environment variable names
ENV_DATABRICKS_TOKEN = "DATABRICKS_TOKEN"
ENV_DATABRICKS_CLIENT_ID = "DATABRICKS_CLIENT_ID"
ENV_DATABRICKS_CLIENT_SECRET = "DATABRICKS_CLIENT_SECRET"

# Default config file path
DEFAULT_CONFIG_PATH = Path.home() / ".databrickscfg"


@dataclass(frozen=True)
class ResolvedCredential:
    """Resolved credential with auth type and token/secret material."""

    auth_type: str
    token: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    azure_tenant_id: str | None = None
    azure_client_id: str | None = None
    azure_client_secret: str | None = None
    source: str = "unknown"


def _check_partial_oauth_m2m(options: dict[str, Any]) -> None:
    """Raise RVT-205 if only one of client_id/client_secret is provided."""
    has_id = bool(options.get("client_id"))
    has_secret = bool(options.get("client_secret"))
    if has_id != has_secret:
        missing = "client_secret" if has_id else "client_id"
        raise PluginValidationError(
            plugin_error(
                "RVT-205",
                f"Partial OAuth M2M credentials: '{missing}' is missing.",
                plugin_name="rivet_databricks",
                plugin_type="auth",
                remediation="Provide both 'client_id' and 'client_secret', or remove both.",
                auth_type=AUTH_TYPE_OAUTH_M2M,
            )
        )


def _check_partial_azure(options: dict[str, Any]) -> None:
    """Raise RVT-205 if Azure Entra ID credentials are partially provided."""
    azure_keys = ["azure_tenant_id", "azure_client_id", "azure_client_secret"]
    provided = [k for k in azure_keys if options.get(k)]
    if 0 < len(provided) < 3:
        missing = [k for k in azure_keys if k not in provided]
        raise PluginValidationError(
            plugin_error(
                "RVT-205",
                f"Partial Azure Entra ID credentials: missing {', '.join(missing)}.",
                plugin_name="rivet_databricks",
                plugin_type="auth",
                remediation="Provide all three: azure_tenant_id, azure_client_id, azure_client_secret.",
                auth_type=AUTH_TYPE_AZURE_CLI,
            )
        )


def _resolve_from_explicit(options: dict[str, Any]) -> ResolvedCredential | None:
    """Step 1: Resolve from explicit options."""
    if options.get("token"):
        return ResolvedCredential(
            auth_type=AUTH_TYPE_PAT,
            token=options["token"],
            source="explicit_options",
        )
    if options.get("client_id") and options.get("client_secret"):
        return ResolvedCredential(
            auth_type=AUTH_TYPE_OAUTH_M2M,
            client_id=options["client_id"],
            client_secret=options["client_secret"],
            source="explicit_options",
        )
    if (
        options.get("azure_tenant_id")
        and options.get("azure_client_id")
        and options.get("azure_client_secret")
    ):
        return ResolvedCredential(
            auth_type=AUTH_TYPE_AZURE_CLI,
            azure_tenant_id=options["azure_tenant_id"],
            azure_client_id=options["azure_client_id"],
            azure_client_secret=options["azure_client_secret"],
            source="explicit_options",
        )
    return None


def _resolve_from_env() -> ResolvedCredential | None:
    """Step 2: Resolve from environment variables."""
    token = os.environ.get(ENV_DATABRICKS_TOKEN)
    if token:
        return ResolvedCredential(
            auth_type=AUTH_TYPE_PAT,
            token=token,
            source="environment_variables",
        )
    client_id = os.environ.get(ENV_DATABRICKS_CLIENT_ID)
    client_secret = os.environ.get(ENV_DATABRICKS_CLIENT_SECRET)
    if client_id and client_secret:
        return ResolvedCredential(
            auth_type=AUTH_TYPE_OAUTH_M2M,
            client_id=client_id,
            client_secret=client_secret,
            source="environment_variables",
        )
    return None


def _resolve_from_config(
    host: str | None = None,
    profile: str = "DEFAULT",
    config_path: Path | None = None,
) -> ResolvedCredential | None:
    """Step 3: Resolve from ~/.databrickscfg."""
    path = config_path or DEFAULT_CONFIG_PATH
    if not path.exists():
        return None

    config = configparser.ConfigParser()
    config.read(str(path))

    # Try matching profile first, then match by host
    section = None
    if config.has_section(profile):
        section = profile
    elif profile == "DEFAULT" and host:
        for sec in config.sections():
            cfg_host = config.get(sec, "host", fallback=None)
            if cfg_host and _hosts_match(cfg_host, host):
                section = sec
                break

    if section is None and config.defaults():
        section = "DEFAULT"

    if section is None:
        return None

    get = lambda key: config.get(section, key, fallback=None)  # noqa: E731

    token = get("token")  # type: ignore[no-untyped-call]
    if token:
        return ResolvedCredential(
            auth_type=AUTH_TYPE_PAT,
            token=token,
            source="databrickscfg",
        )

    client_id = get("client_id")  # type: ignore[no-untyped-call]
    client_secret = get("client_secret")  # type: ignore[no-untyped-call]
    if client_id and client_secret:
        return ResolvedCredential(
            auth_type=AUTH_TYPE_OAUTH_M2M,
            client_id=client_id,
            client_secret=client_secret,
            source="databrickscfg",
        )

    return None


def _hosts_match(cfg_host: str, target_host: str) -> bool:
    """Compare hosts ignoring scheme and trailing slash."""
    def normalize(h: str) -> str:
        h = h.strip().rstrip("/")
        for prefix in ("https://", "http://"):
            if h.lower().startswith(prefix):
                h = h[len(prefix):]
        return h.lower()

    return normalize(cfg_host) == normalize(target_host)


def _resolve_cloud_native(auth_type: str | None) -> ResolvedCredential | None:
    """Step 4: Resolve via cloud-native auth (azure_cli, gcp_login).

    Returns a placeholder credential indicating cloud-native auth should be used.
    Actual token acquisition happens at request time.
    """
    if auth_type == AUTH_TYPE_AZURE_CLI:
        return ResolvedCredential(
            auth_type=AUTH_TYPE_AZURE_CLI,
            source="cloud_native_auth",
        )
    if auth_type == AUTH_TYPE_GCP_LOGIN:
        return ResolvedCredential(
            auth_type=AUTH_TYPE_GCP_LOGIN,
            source="cloud_native_auth",
        )
    return None


def resolve_credentials(
    options: dict[str, Any],
    host: str | None = None,
    config_path: Path | None = None,
) -> ResolvedCredential:
    """Resolve credentials using the 4-step chain.

    Resolution order:
      1. Explicit options (token, client_id+client_secret, azure_* triple)
      2. Environment variables (DATABRICKS_TOKEN, DATABRICKS_CLIENT_ID+SECRET)
      3. Databricks config file (~/.databrickscfg)
      4. Cloud-native auth (azure_cli, gcp_login) if auth_type is set

    Raises PluginValidationError (RVT-205) for partial credential sets.
    Raises PluginValidationError (RVT-201) if no credential source resolves.
    """
    # Validate partial credentials first
    _check_partial_oauth_m2m(options)
    _check_partial_azure(options)

    # Validate auth_type if provided
    auth_type = options.get("auth_type")
    if auth_type and auth_type not in VALID_AUTH_TYPES:
        raise PluginValidationError(
            plugin_error(
                "RVT-201",
                f"Invalid auth_type '{auth_type}'.",
                plugin_name="rivet_databricks",
                plugin_type="auth",
                remediation=f"Valid auth_type values: {', '.join(sorted(VALID_AUTH_TYPES))}",
                auth_type=auth_type,
            )
        )

    # Step 1: Explicit options
    cred = _resolve_from_explicit(options)
    if cred is not None:
        return cred

    # Step 2: Environment variables
    cred = _resolve_from_env()
    if cred is not None:
        return cred

    # Step 3: Databricks config file
    cred = _resolve_from_config(host=host, config_path=config_path)
    if cred is not None:
        return cred

    # Step 4: Cloud-native auth
    cred = _resolve_cloud_native(auth_type)
    if cred is not None:
        return cred

    raise PluginValidationError(
        plugin_error(
            "RVT-201",
            "No credentials resolved for Unity/Databricks.",
            plugin_name="rivet_databricks",
            plugin_type="auth",
            remediation="Provide 'token', 'client_id'+'client_secret', or "
                "Azure Entra ID credentials in options; or set DATABRICKS_TOKEN env var; "
                "or configure ~/.databrickscfg; or set auth_type to 'azure_cli' or 'gcp_login'.",
            sources_attempted=[
                    "explicit_options",
                    "environment_variables",
                    "databrickscfg",
                    "cloud_native_auth",
                ],
        )
    )
