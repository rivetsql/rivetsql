"""EnvResolver: resolve ${VAR} references in profile config values."""

from __future__ import annotations

import os
import re
from typing import Any

from rivet_config.errors import ConfigError, ConfigWarning

ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

_CREDENTIAL_KEYS = {"password", "secret", "token", "key", "credential"}


def _is_credential_key(key: str) -> bool:
    lower = key.lower()
    return any(ck in lower for ck in _CREDENTIAL_KEYS)


def _resolve_value(
    value: Any,
    path: str,
    errors: list[ConfigError],
    warnings: list[ConfigWarning],
    parent_key: str | None = None,
    strict: bool = True,
) -> Any:
    if isinstance(value, str):
        if parent_key and _is_credential_key(parent_key) and not ENV_PATTERN.fullmatch(value):
            warnings.append(
                ConfigWarning(
                    source_file=None,
                    message=f"Plaintext credential at '{path}'",
                    remediation="Use a ${{VAR}} environment variable reference instead.",
                )
            )

        def replace_match(m: re.Match[str]) -> str:
            var = m.group(1)
            val = os.environ.get(var)
            if val is None:
                if strict:
                    errors.append(
                        ConfigError(
                            source_file=None,
                            message=f"Environment variable '{var}' is not set (referenced at '{path}')",
                            remediation=f"Set the environment variable: export {var}=<value>",
                        )
                    )
                else:
                    warnings.append(
                        ConfigWarning(
                            source_file=None,
                            message=f"Environment variable '{var}' is not set (referenced at '{path}')",
                            remediation=f"Set the environment variable: export {var}=<value>",
                        )
                    )
                return m.group(0)  # leave unresolved
            return val

        return ENV_PATTERN.sub(replace_match, value)

    if isinstance(value, dict):
        return {
            k: _resolve_value(v, f"{path}.{k}", errors, warnings, parent_key=k, strict=strict)
            for k, v in value.items()
        }

    if isinstance(value, list):
        return [
            _resolve_value(item, f"{path}[{i}]", errors, warnings, strict=strict)
            for i, item in enumerate(value)
        ]

    return value


def resolve(
    data: dict[str, Any],
    strict: bool = True,
) -> tuple[dict[str, Any], list[ConfigError], list[ConfigWarning]]:
    """Resolve all ${VAR} references in *data*.

    Returns (resolved_data, errors, warnings).
    When strict=True (default), missing env vars produce errors.
    When strict=False, missing env vars produce warnings instead.
    """
    errors: list[ConfigError] = []
    warnings: list[ConfigWarning] = []
    resolved = _resolve_value(data, "", errors, warnings, strict=strict)
    return resolved, errors, warnings
