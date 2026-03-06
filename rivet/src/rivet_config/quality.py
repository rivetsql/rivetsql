"""QualityParser: parse quality checks from inline YAML, SQL annotations, and dedicated/co-located files."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from rivet_config.annotations import ParsedAnnotation
from rivet_config.errors import ConfigError
from rivet_config.models import CHECK_TYPES, QualityCheck

# Pattern for TYPE(ARGS) in SQL annotations — uses greedy match to handle nested parens.
_CHECK_RE = re.compile(r"^(\w+)\((.*)\)$", re.DOTALL)

# Required parameters per check type.
_REQUIRED_PARAMS: dict[str, list[str]] = {
    "not_null": ["columns"],
    "unique": ["columns"],
    "accepted_values": ["column", "values"],
    "expression": ["sql"],
    "freshness": ["column", "max_age"],
    "relationship": ["column", "to"],
    "row_count": [],
    "custom": ["sql"],
    "schema": [],
}


def _parse_check_args(raw: str) -> dict[str, Any]:
    """Parse positional and keyword arguments from a check annotation.

    Positional args become config["columns"] (list).
    key=value becomes config[key] = value.
    values=[a, b] becomes config["values"] = ["a", "b"].
    """
    config: dict[str, Any] = {}
    positional: list[str] = []
    if not raw.strip():
        return config

    for token in _split_args(raw):
        token = token.strip()
        if not token:
            continue
        if "=" in token:
            key, val = token.split("=", 1)
            key = key.strip()
            val = val.strip()
            if val.startswith("[") and val.endswith("]"):
                items = [v.strip() for v in val[1:-1].split(",") if v.strip()]
                config[key] = items
            else:
                # Try numeric conversion
                config[key] = _coerce_value(val)
        else:
            positional.append(token)

    if positional:
        config["columns"] = positional
    return config


def _split_args(raw: str) -> list[str]:
    """Split arguments respecting bracket groups."""
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in raw:
        if ch == "[":
            depth += 1
            current.append(ch)
        elif ch == "]":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current))
    return parts


def _coerce_value(val: str) -> Any:
    """Coerce string value to int/float if possible."""
    try:
        return int(val)
    except ValueError:
        pass
    try:
        return float(val)
    except ValueError:
        pass
    return val


def _parse_check_entry(
    entry: dict[str, Any],
    phase: str,
    source: str,
    file_path: Path,
    errors: list[ConfigError],
) -> QualityCheck | None:
    """Parse a single check entry dict into a QualityCheck."""
    check_type = entry.get("type")
    if check_type is None:
        errors.append(ConfigError(
            source_file=file_path,
            message="Quality check entry missing required 'type' field.",
            remediation="Add a 'type' field to the quality check entry.",
        ))
        return None

    if check_type not in CHECK_TYPES:
        errors.append(ConfigError(
            source_file=file_path,
            message=f"Unrecognized quality check type '{check_type}'.",
            remediation=f"Use one of: {sorted(CHECK_TYPES)}",
        ))
        return None

    severity = entry.get("severity", "error")
    config = {k: v for k, v in entry.items() if k not in ("type", "severity")}

    # Validate required params.
    for req in _REQUIRED_PARAMS.get(check_type, []):
        if req not in config:
            errors.append(ConfigError(
                source_file=file_path,
                message=f"Quality check '{check_type}' missing required parameter '{req}'.",
                remediation=f"Add '{req}' to the quality check entry.",
            ))
            return None

    return QualityCheck(
        check_type=check_type,
        phase=phase,
        severity=severity,
        config=config,
        source=source,
        source_file=file_path,
    )


class QualityParser:
    """Parses quality checks from all four sources."""

    def parse_inline(
        self, raw_quality: dict[str, Any], file_path: Path
    ) -> tuple[list[QualityCheck], list[ConfigError]]:
        """Parse inline quality block from a YAML declaration."""
        checks: list[QualityCheck] = []
        errors: list[ConfigError] = []

        for phase, key in [("assertion", "assertions"), ("audit", "audits")]:
            entries = raw_quality.get(key, [])
            if not isinstance(entries, list):
                errors.append(ConfigError(
                    source_file=file_path,
                    message=f"'quality.{key}' must be a list.",
                    remediation=f"Provide 'quality.{key}' as a YAML list.",
                ))
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    errors.append(ConfigError(
                        source_file=file_path,
                        message=f"Each entry in 'quality.{key}' must be a mapping.",
                        remediation="Provide each check as a YAML mapping with at least a 'type' field.",
                    ))
                    continue
                check = _parse_check_entry(entry, phase, "inline", file_path, errors)
                if check is not None:
                    checks.append(check)

        return checks, errors

    def parse_sql_annotations(
        self, annotations: list[ParsedAnnotation], file_path: Path
    ) -> tuple[list[QualityCheck], list[ConfigError]]:
        """Parse -- rivet:assert: and -- rivet:audit: annotations."""
        checks: list[QualityCheck] = []
        errors: list[ConfigError] = []

        for ann in annotations:
            if ann.key == "assert":
                phase = "assertion"
            elif ann.key == "audit":
                phase = "audit"
            else:
                continue

            raw = str(ann.value).strip()
            m = _CHECK_RE.match(raw)
            if m is None:
                errors.append(ConfigError(
                    source_file=file_path,
                    message=f"Malformed quality annotation: '{raw}'. Expected TYPE(ARGS).",
                    remediation="Use format: TYPE(arg1, arg2, key=value)",
                    line_number=ann.line_number,
                ))
                continue

            check_type = m.group(1)
            args_str = m.group(2)

            if check_type not in CHECK_TYPES:
                errors.append(ConfigError(
                    source_file=file_path,
                    message=f"Unrecognized quality check type '{check_type}'.",
                    remediation=f"Use one of: {sorted(CHECK_TYPES)}",
                    line_number=ann.line_number,
                ))
                continue

            config = _parse_check_args(args_str)

            # Extract severity from config if present, default to "error".
            severity = str(config.pop("severity", "error"))

            # Validate required params.
            missing = [r for r in _REQUIRED_PARAMS.get(check_type, []) if r not in config]
            if missing:
                errors.append(ConfigError(
                    source_file=file_path,
                    message=f"Quality check '{check_type}' missing required parameter(s): {missing}.",
                    remediation=f"Add {missing} to the annotation arguments.",
                    line_number=ann.line_number,
                ))
                continue

            checks.append(QualityCheck(
                check_type=check_type,
                phase=phase,
                severity=severity,
                config=config,
                source="sql_annotation",
                source_file=file_path,
            ))

        return checks, errors

    def parse_dedicated_file(
        self, file_path: Path, target_joint: str | None = None
    ) -> tuple[list[QualityCheck], list[ConfigError]]:
        """Parse a dedicated quality YAML file."""
        return self._parse_quality_file(file_path, "dedicated")

    def parse_colocated_file(
        self, file_path: Path
    ) -> tuple[list[QualityCheck], list[ConfigError]]:
        """Parse a co-located quality YAML file."""
        return self._parse_quality_file(file_path, "colocated")

    def _parse_quality_file(
        self, file_path: Path, source: str
    ) -> tuple[list[QualityCheck], list[ConfigError]]:
        """Parse a quality YAML file (dedicated or co-located)."""
        errors: list[ConfigError] = []

        try:
            raw = yaml.safe_load(file_path.read_text())
        except (OSError, yaml.YAMLError) as exc:
            errors.append(ConfigError(
                source_file=file_path,
                message=f"Failed to read or parse quality file: {exc}",
                remediation="Ensure the file exists and contains valid YAML.",
            ))
            return [], errors

        if isinstance(raw, list):
            # Flat list → all assertions.
            return self._parse_check_list(raw, "assertion", source, file_path, errors)

        if isinstance(raw, dict):
            checks: list[QualityCheck] = []
            # Sectioned format with optional joint: field.
            for phase, key in [("assertion", "assertions"), ("audit", "audits")]:
                entries = raw.get(key)
                if entries is None:
                    continue
                if not isinstance(entries, list):
                    errors.append(ConfigError(
                        source_file=file_path,
                        message=f"'{key}' must be a list.",
                        remediation=f"Provide '{key}' as a YAML list.",
                    ))
                    continue
                parsed, errs = self._parse_check_list(entries, phase, source, file_path, [])
                checks.extend(parsed)
                errors.extend(errs)

            # If no assertions/audits keys, treat as flat list if it looks like check entries
            if not checks and not errors and "assertions" not in raw and "audits" not in raw:
                # Not a recognized quality file format
                errors.append(ConfigError(
                    source_file=file_path,
                    message="Quality file must be a list of checks or contain 'assertions'/'audits' sections.",
                    remediation="Use a flat list of check entries or 'assertions:'/'audits:' sections.",
                ))
            return checks, errors

        errors.append(ConfigError(
            source_file=file_path,
            message="Quality file must be a list or mapping.",
            remediation="Use a flat list of check entries or a mapping with 'assertions:'/'audits:' sections.",
        ))
        return [], errors

    def _parse_check_list(
        self,
        entries: list[Any],
        phase: str,
        source: str,
        file_path: Path,
        errors: list[ConfigError],
    ) -> tuple[list[QualityCheck], list[ConfigError]]:
        """Parse a list of check entry dicts."""
        checks: list[QualityCheck] = []
        for entry in entries:
            if not isinstance(entry, dict):
                errors.append(ConfigError(
                    source_file=file_path,
                    message=f"Each quality check entry must be a mapping, got {type(entry).__name__}.",
                    remediation="Provide each check as a YAML mapping with at least a 'type' field.",
                ))
                continue
            check = _parse_check_entry(entry, phase, source, file_path, errors)
            if check is not None:
                checks.append(check)
        return checks, errors
