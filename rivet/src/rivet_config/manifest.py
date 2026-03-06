"""ManifestParser: parse rivet.yaml → ProjectManifest."""

from __future__ import annotations

from pathlib import Path

import yaml

from rivet_config.errors import ConfigError, ConfigWarning
from rivet_config.models import (
    MANIFEST_DEPRECATED_KEYS,
    MANIFEST_OPTIONAL_KEYS,
    MANIFEST_REQUIRED_KEYS,
    ProjectManifest,
)

_ALL_KEYS = MANIFEST_REQUIRED_KEYS | MANIFEST_OPTIONAL_KEYS | MANIFEST_DEPRECATED_KEYS


class ManifestParser:
    def parse(
        self, manifest_path: Path
    ) -> tuple[ProjectManifest | None, list[ConfigError], list[ConfigWarning]]:
        errors: list[ConfigError] = []
        warnings: list[ConfigWarning] = []

        # Read and parse YAML
        try:
            raw = yaml.safe_load(manifest_path.read_text())
        except FileNotFoundError:
            errors.append(ConfigError(
                source_file=manifest_path,
                message=f"Manifest file not found: {manifest_path}",
                remediation="Create a rivet.yaml file at the project root.",
            ))
            return None, errors, warnings
        except Exception as e:
            errors.append(ConfigError(
                source_file=manifest_path,
                message=f"Cannot read manifest: {e}",
                remediation="Ensure rivet.yaml is valid YAML and readable.",
            ))
            return None, errors, warnings

        if not isinstance(raw, dict):
            errors.append(ConfigError(
                source_file=manifest_path,
                message="Manifest must be a YAML mapping.",
                remediation="Ensure rivet.yaml contains key-value pairs at the top level.",
            ))
            return None, errors, warnings

        keys = set(raw.keys())

        # Reject unrecognized keys
        unrecognized = keys - _ALL_KEYS
        for key in sorted(unrecognized):
            errors.append(ConfigError(
                source_file=manifest_path,
                message=f"Unrecognized key: '{key}'",
                remediation=f"Remove '{key}' from rivet.yaml. Recognized keys: {sorted(_ALL_KEYS)}",
            ))

        # Check required keys
        missing = MANIFEST_REQUIRED_KEYS - keys
        for key in sorted(missing):
            errors.append(ConfigError(
                source_file=manifest_path,
                message=f"Missing required key: '{key}'",
                remediation=f"Add '{key}' to rivet.yaml.",
            ))

        # Handle deprecated keys
        deprecated_present = keys & MANIFEST_DEPRECATED_KEYS
        for key in sorted(deprecated_present):
            warnings.append(ConfigWarning(
                source_file=manifest_path,
                message=f"Deprecated key: '{key}'",
                remediation="Use 'quality' instead.",
            ))

        if deprecated_present and "quality" in keys:
            errors.append(ConfigError(
                source_file=manifest_path,
                message="Cannot use deprecated keys ('assertions'/'audits') together with 'quality'.",
                remediation="Remove the deprecated keys and use 'quality' only.",
            ))

        if errors:
            return None, errors, warnings

        parent = manifest_path.parent

        # Determine quality_dir
        quality_dir: Path | None = None
        if "quality" in raw:
            quality_dir = parent / raw["quality"]
        elif deprecated_present:
            # Use first deprecated key as fallback
            quality_dir = parent / raw[sorted(deprecated_present)[0]]

        return (
            ProjectManifest(
                project_root=parent,
                profiles_path=parent / raw["profiles"],
                sources_dir=parent / raw["sources"],
                joints_dir=parent / raw["joints"],
                sinks_dir=parent / raw["sinks"],
                quality_dir=quality_dir,
                tests_dir=parent / raw.get("tests", "./tests/"),
                fixtures_dir=parent / raw.get("fixtures", "./fixtures/"),
            ),
            errors,
            warnings,
        )
