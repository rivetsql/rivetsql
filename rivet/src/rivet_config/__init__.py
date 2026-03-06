"""rivet-config: configuration parsing layer for Rivet.

Public API: load_config(), ConfigResult, and all data model classes.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from rivet_config.declarations import DeclarationLoader
from rivet_config.errors import ConfigError, ConfigWarning
from rivet_config.manifest import ManifestParser
from rivet_config.models import (
    CHECK_TYPES,
    JOINT_NAME_MAX_LENGTH,
    JOINT_NAME_PATTERN,
    JOINT_TYPES,
    MANIFEST_DEPRECATED_KEYS,
    MANIFEST_OPTIONAL_KEYS,
    MANIFEST_REQUIRED_KEYS,
    WRITE_STRATEGY_MODES,
    YAML_JOINT_FIELDS,
    CatalogConfig,
    ColumnDecl,
    EngineConfig,
    JointDeclaration,
    ProjectDeclaration,
    ProjectManifest,
    QualityCheck,
    ResolvedProfile,
    WriteStrategyDecl,
)
from rivet_config.profiles import ProfileResolver


@dataclass
class ConfigResult:
    """Complete output of rivet-config parsing."""

    manifest: ProjectManifest | None
    profile: ResolvedProfile | None
    declarations: list[JointDeclaration]
    errors: list[ConfigError]
    warnings: list[ConfigWarning]

    @property
    def success(self) -> bool:
        return len(self.errors) == 0


def load_config(
    project_root: Path,
    profile_name: str | None = None,
    strict: bool = True,
) -> ConfigResult:
    """Parse project manifest, resolve profile, load all declarations.

    Collects all errors and returns them in ConfigResult.
    Never raises on validation errors — caller checks result.success.
    When strict=False, missing env vars produce warnings instead of errors.
    """
    errors: list[ConfigError] = []
    warnings: list[ConfigWarning] = []

    # Phase 1: Parse manifest
    manifest_path = project_root / "rivet.yaml"
    manifest_parser = ManifestParser()
    manifest, m_errors, m_warnings = manifest_parser.parse(manifest_path)
    errors.extend(m_errors)
    warnings.extend(m_warnings)

    if manifest is None:
        return ConfigResult(
            manifest=None,
            profile=None,
            declarations=[],
            errors=errors,
            warnings=warnings,
        )

    # Phase 2 & 3: Profile resolution and declaration loading are independent
    profile_resolver = ProfileResolver()
    profile, p_errors, p_warnings = profile_resolver.resolve(
        manifest.profiles_path, profile_name, project_root, strict=strict
    )
    errors.extend(p_errors)
    warnings.extend(p_warnings)

    declaration_loader = DeclarationLoader(project_root)
    declarations, d_errors = declaration_loader.load(manifest)
    errors.extend(d_errors)

    return ConfigResult(
        manifest=manifest,
        profile=profile,
        declarations=declarations,
        errors=errors,
        warnings=warnings,
    )


__all__ = [
    "load_config",
    "ConfigResult",
    "ConfigError",
    "ConfigWarning",
    "ProjectManifest",
    "CatalogConfig",
    "EngineConfig",
    "ResolvedProfile",
    "ColumnDecl",
    "WriteStrategyDecl",
    "QualityCheck",
    "JointDeclaration",
    "ProjectDeclaration",
    "JOINT_NAME_PATTERN",
    "JOINT_NAME_MAX_LENGTH",
    "MANIFEST_REQUIRED_KEYS",
    "MANIFEST_OPTIONAL_KEYS",
    "MANIFEST_DEPRECATED_KEYS",
    "YAML_JOINT_FIELDS",
    "JOINT_TYPES",
    "WRITE_STRATEGY_MODES",
    "CHECK_TYPES",
]
