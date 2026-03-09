"""AnnotationParser: parse rivet:key: value annotations from SQL and Python files."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from rivet_config.errors import ConfigError

_SQL_ANNOTATION_RE = re.compile(r"^--\s*rivet:([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*):\s*(.*?)\s*$")
_PY_ANNOTATION_RE = re.compile(r"^#\s*rivet:([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*):\s*(.*?)\s*$")

# Keep backward-compatible alias
_ANNOTATION_RE = _SQL_ANNOTATION_RE


@dataclass
class ParsedAnnotation:
    key: str
    value: str | bool | list[Any] | dict[str, Any]
    line_number: int


class _StringSafeLoader(yaml.SafeLoader):
    """SafeLoader that treats YAML boolean literals as plain strings.

    yaml.safe_load coerces 'yes', 'no', 'on', 'off', 'true', 'false' to
    Python bools. Rivet annotation values (joint names, tags, etc.) are
    always strings, so we override the bool resolver to keep them as-is.
    """


# Remove the implicit bool resolver so yes/no/on/off stay as strings.
_StringSafeLoader.add_constructor(
    "tag:yaml.org,2002:bool",
    lambda loader, node: loader.construct_scalar(node),  # type: ignore[arg-type]
)


def _parse_value(raw: str) -> str | bool | list[Any] | dict[str, Any]:
    """Parse annotation value: list, bool, dict, or string."""
    stripped = raw.strip()
    if stripped in ("true", "false"):
        return stripped == "true"
    if stripped.startswith("[") or stripped.startswith("{"):
        return yaml.load(stripped, Loader=_StringSafeLoader)  # type: ignore[no-any-return]  # noqa: S506
    return stripped


class AnnotationParser:
    def parse(
        self, lines: list[str], file_path: Path, comment_prefix: str = "sql",
    ) -> tuple[list[ParsedAnnotation], int, list[ConfigError]]:
        """Parse annotations from file lines.

        Returns (annotations, first_code_line_index, errors).
        Stops at the first line that is not a rivet: annotation or blank.

        comment_prefix: "sql" for -- rivet: or "python" for # rivet:
        """
        regex = _PY_ANNOTATION_RE if comment_prefix == "python" else _SQL_ANNOTATION_RE
        annotations: list[ParsedAnnotation] = []
        errors: list[ConfigError] = []
        first_code_line = 0

        for i, line in enumerate(lines):
            stripped = line.rstrip("\n")
            # Blank lines before code are skipped but don't count as annotations
            if stripped.strip() == "":
                continue
            m = regex.match(stripped)
            if m is None:
                first_code_line = i
                break
            key = m.group(1)
            raw_value = m.group(2)
            try:
                value = _parse_value(raw_value)
            except Exception as exc:
                errors.append(
                    ConfigError(
                        source_file=file_path,
                        message=f"Malformed annotation value for key '{key}': {exc}",
                        remediation="Ensure the annotation value is valid YAML/JSON syntax.",
                        line_number=i + 1,
                    )
                )
                first_code_line = i + 1
                continue
            annotations.append(ParsedAnnotation(key=key, value=value, line_number=i + 1))
            first_code_line = i + 1
        else:
            first_code_line = len(lines)

        return annotations, first_code_line, errors
