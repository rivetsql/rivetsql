"""Data models for rivet-config: frozen dataclasses and validation constants."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# --- Validation Constants ---

JOINT_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
JOINT_NAME_MAX_LENGTH = 128

MANIFEST_REQUIRED_KEYS = frozenset({"profiles", "sources", "joints", "sinks"})
MANIFEST_OPTIONAL_KEYS = frozenset({"quality", "tests", "fixtures"})
MANIFEST_DEPRECATED_KEYS = frozenset({"assertions", "audits"})

YAML_JOINT_FIELDS = frozenset({
    "name", "type", "sql", "columns", "filter", "catalog", "engine",
    "eager", "upstream", "tags", "description", "table", "write_strategy",
    "function", "fusion_strategy", "materialization_strategy", "quality",
})

JOINT_TYPES = frozenset({"source", "sql", "sink", "python"})

WRITE_STRATEGY_MODES = frozenset({
    "append", "replace", "truncate_insert", "merge",
    "delete_insert", "incremental_append", "scd2",
})

CHECK_TYPES = frozenset({
    "not_null", "unique", "row_count", "accepted_values", "expression",
    "custom", "schema", "freshness", "relationship",
})


# --- Data Models ---


@dataclass(frozen=True)
class ProjectManifest:
    """Parsed representation of rivet.yaml."""

    project_root: Path
    profiles_path: Path
    sources_dir: Path
    joints_dir: Path
    sinks_dir: Path
    quality_dir: Path | None
    tests_dir: Path
    fixtures_dir: Path


@dataclass(frozen=True)
class CatalogConfig:
    """Fully resolved catalog configuration."""

    name: str
    type: str
    options: dict[str, Any]


@dataclass(frozen=True)
class EngineConfig:
    """Fully resolved engine configuration."""

    name: str
    type: str
    catalogs: list[str]
    options: dict[str, Any]


@dataclass(frozen=True)
class ResolvedProfile:
    """Fully merged and env-resolved profile ready for bridge consumption."""

    name: str
    default_engine: str
    catalogs: dict[str, CatalogConfig]
    engines: list[EngineConfig]


@dataclass(frozen=True)
class ColumnDecl:
    """A column declaration: either a pass-through name or an alias→expression mapping."""

    name: str
    expression: str | None


@dataclass(frozen=True)
class WriteStrategyDecl:
    """Parsed write strategy declaration."""

    mode: str
    options: dict[str, Any]


@dataclass(frozen=True)
class QualityCheck:
    """A parsed quality check (assertion or audit)."""

    check_type: str
    phase: str
    severity: str
    config: dict[str, Any]
    source: str
    source_file: Path


@dataclass(frozen=True)
class JointDeclaration:
    """Structured intermediate representation of a joint parsed from YAML or SQL."""

    name: str
    joint_type: str
    source_path: Path

    sql: str | None = None
    catalog: str | None = None
    table: str | None = None
    columns: list[ColumnDecl] | None = None
    filter: str | None = None
    write_strategy: WriteStrategyDecl | None = None
    function: str | None = None
    engine: str | None = None
    eager: bool = False
    upstream: list[str] | None = None
    tags: list[str] | None = None
    description: str | None = None
    dialect: str | None = None
    path: str | None = None
    source_format: str | None = None
    fusion_strategy: str | None = None
    materialization_strategy: str | None = None
    quality_checks: list[QualityCheck] = field(default_factory=list)


@dataclass(frozen=True)
class ProjectDeclaration:
    """Bundled output of project parsing: profile + declarations + manifest path."""

    profile: ResolvedProfile
    joints: list[JointDeclaration]
    rivet_yaml_path: Path
