"""Bridge data models."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from rivet_config import ResolvedProfile
from rivet_core import Assembly, Catalog, ComputeEngine


@dataclass(frozen=True)
class FileOutput:
    """A single generated file with its relative path and content."""

    relative_path: str
    content: str
    joint_name: str | None = None


@dataclass(frozen=True)
class BridgeResult:
    """Output of the forward path: a ready-to-compile Assembly with context."""

    assembly: Assembly
    catalogs: dict[str, Catalog]
    engines: dict[str, ComputeEngine]
    profile_snapshot: ResolvedProfile
    source_formats: dict[str, str]


@dataclass(frozen=True)
class ProjectOutput:
    """Complete output of generate_project: all files for a project scaffold."""

    rivet_yaml: FileOutput
    profile: FileOutput
    declarations: list[FileOutput]
    quality_files: list[FileOutput]
    output_dir: Path


@dataclass(frozen=True)
class RoundtripDifference:
    """A single semantic difference between two project declarations."""

    joint_name: str
    field: str
    description: str


@dataclass(frozen=True)
class RoundtripResult:
    """Result of roundtrip verification."""

    equivalent: bool
    differences: list[RoundtripDifference]
