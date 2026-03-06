"""Error and warning data models for configuration parsing."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ConfigError:
    source_file: Path | None
    message: str
    remediation: str
    line_number: int | None = None


@dataclass(frozen=True)
class ConfigWarning:
    source_file: Path | None
    message: str
    remediation: str | None = None
