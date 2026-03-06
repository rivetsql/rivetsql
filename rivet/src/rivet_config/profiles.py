"""ProfileResolver: load, merge, and env-resolve profiles."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from rivet_config import env as env_resolver
from rivet_config.errors import ConfigError, ConfigWarning
from rivet_config.models import CatalogConfig, EngineConfig, ResolvedProfile


class ProfileResolver:
    def resolve(
        self,
        profiles_path: Path,
        profile_name: str | None,
        project_root: Path,
        strict: bool = True,
    ) -> tuple[ResolvedProfile | None, list[ConfigError], list[ConfigWarning]]:
        """Load, merge, and resolve a profile. Returns (profile, errors, warnings)."""
        errors: list[ConfigError] = []
        warnings: list[ConfigWarning] = []

        # Load project profiles
        project_profiles = self._load_profiles(profiles_path, errors)

        # Load global profiles
        global_profiles = self._load_global_profiles(errors)

        # Determine available profile names (union of both levels)
        all_names = sorted(set(global_profiles) | set(project_profiles))

        if not all_names:
            errors.append(ConfigError(
                source_file=profiles_path,
                message="No profiles found.",
                remediation="Define at least one profile in your profiles file or directory.",
            ))
            return None, errors, warnings

        # Select profile
        selected = self._select_profile(profile_name, all_names, profiles_path, errors)
        if selected is None:
            return None, errors, warnings

        # Merge global and project
        global_data = global_profiles.get(selected, {})
        project_data = project_profiles.get(selected, {})
        merged = self._merge_profiles(global_data, project_data)

        # Validate required fields
        self._validate_profile(merged, selected, profiles_path, errors)

        # Env-resolve
        resolved_data, env_errors, env_warnings = env_resolver.resolve(merged, strict=strict)
        errors.extend(env_errors)
        warnings.extend(env_warnings)

        if errors:
            return None, errors, warnings

        # Build ResolvedProfile
        profile = self._build_profile(selected, resolved_data, project_root)
        return profile, errors, warnings

    def _load_profiles(
        self, profiles_path: Path, errors: list[ConfigError]
    ) -> dict[str, dict[str, Any]]:
        if profiles_path.is_dir():
            return self._load_from_directory(profiles_path, errors)
        if profiles_path.is_file():
            return self._load_from_file(profiles_path, errors)
        errors.append(ConfigError(
            source_file=profiles_path,
            message=f"Profiles path not found: {profiles_path}",
            remediation="Create a profiles file or directory at the declared path.",
        ))
        return {}

    def _load_global_profiles(
        self, errors: list[ConfigError]
    ) -> dict[str, dict[str, Any]]:
        home = Path.home()
        file_path = home / ".rivet" / "profiles.yaml"
        dir_path = home / ".rivet" / "profiles"
        if file_path.is_file():
            return self._load_from_file(file_path, errors)
        if dir_path.is_dir():
            return self._load_from_directory(dir_path, errors)
        return {}

    def _load_from_file(
        self, path: Path, errors: list[ConfigError]
    ) -> dict[str, dict[str, Any]]:
        try:
            raw = yaml.safe_load(path.read_text())
        except Exception as e:
            errors.append(ConfigError(
                source_file=path,
                message=f"Cannot read profiles file: {e}",
                remediation="Ensure the file is valid YAML and readable.",
            ))
            return {}
        if not isinstance(raw, dict):
            errors.append(ConfigError(
                source_file=path,
                message="Profiles file must be a YAML mapping of profile names to configs.",
                remediation="Structure the file as: profile_name: {default_engine: ..., catalogs: ..., engines: ...}",
            ))
            return {}
        return {k: (v if isinstance(v, dict) else {}) for k, v in raw.items()}

    def _load_from_directory(
        self, path: Path, errors: list[ConfigError]
    ) -> dict[str, dict[str, Any]]:
        profiles: dict[str, dict[str, Any]] = {}
        files = sorted(
            f for f in path.iterdir()
            if f.is_file() and f.suffix in (".yaml", ".yml")
        )
        if not files:
            errors.append(ConfigError(
                source_file=path,
                message=f"No profile files found in directory: {path}",
                remediation="Add .yaml or .yml profile files to the directory.",
            ))
            return profiles
        for f in files:
            try:
                raw = yaml.safe_load(f.read_text())
            except Exception as e:
                errors.append(ConfigError(
                    source_file=f,
                    message=f"Cannot read profile file: {e}",
                    remediation="Ensure the file is valid YAML.",
                ))
                continue
            name = f.stem
            profiles[name] = raw if isinstance(raw, dict) else {}
        return profiles

    def _merge_profiles(
        self,
        global_data: dict[str, Any],
        project_data: dict[str, Any],
    ) -> dict[str, Any]:
        if not global_data:
            return dict(project_data)
        if not project_data:
            return dict(global_data)

        merged = dict(global_data)

        # Top-level keys from project replace global
        for key, value in project_data.items():
            if key == "catalogs" and isinstance(value, dict) and isinstance(merged.get("catalogs"), dict):
                # Merge catalogs: project replaces by name, global-only preserved
                cat = dict(merged["catalogs"])
                cat.update(value)
                merged["catalogs"] = cat
            elif key == "engines" and isinstance(value, list) and isinstance(merged.get("engines"), list):
                # Merge engines: project replaces by name, global-only preserved, new added
                engine_map: dict[str, Any] = {}
                for eng in merged["engines"]:
                    if isinstance(eng, dict) and "name" in eng:
                        engine_map[eng["name"]] = eng
                for eng in value:
                    if isinstance(eng, dict) and "name" in eng:
                        engine_map[eng["name"]] = eng
                merged["engines"] = list(engine_map.values())
            else:
                merged[key] = value

        return merged

    def _select_profile(
        self,
        profile_name: str | None,
        available: list[str],
        profiles_path: Path,
        errors: list[ConfigError],
    ) -> str | None:
        if profile_name is not None:
            if profile_name in available:
                return profile_name
            errors.append(ConfigError(
                source_file=profiles_path,
                message=f"Profile '{profile_name}' not found. Available: {available}",
                remediation=f"Use one of: {', '.join(available)}",
            ))
            return None
        if "default" in available:
            return "default"
        return available[0]

    def _validate_profile(
        self,
        data: dict[str, Any],
        name: str,
        source: Path,
        errors: list[ConfigError],
    ) -> None:
        if "default_engine" not in data:
            errors.append(ConfigError(
                source_file=source,
                message=f"Profile '{name}' missing required field: 'default_engine'",
                remediation=f"Add 'default_engine' to profile '{name}'.",
            ))
        if "catalogs" not in data:
            errors.append(ConfigError(
                source_file=source,
                message=f"Profile '{name}' missing required field: 'catalogs'",
                remediation=f"Add 'catalogs' mapping to profile '{name}'.",
            ))
        if "engines" not in data:
            errors.append(ConfigError(
                source_file=source,
                message=f"Profile '{name}' missing required field: 'engines'",
                remediation=f"Add 'engines' list to profile '{name}'.",
            ))
        for i, eng in enumerate(data.get("engines", [])):
            if not isinstance(eng, dict):
                continue
            for field in ("name", "type", "catalogs"):
                if field not in eng:
                    errors.append(ConfigError(
                        source_file=source,
                        message=f"Profile '{name}', engine #{i + 1} missing required field: '{field}'",
                        remediation=f"Add '{field}' to engine entry in profile '{name}'.",
                    ))

    def _build_profile(
        self, name: str, data: dict[str, Any], project_root: Path | None = None
    ) -> ResolvedProfile:
        catalogs: dict[str, CatalogConfig] = {}
        for cat_name, cat_data in data.get("catalogs", {}).items():
            if not isinstance(cat_data, dict):
                cat_data = {}
            options = {k: v for k, v in cat_data.items() if k != "type"}
            # Resolve relative 'path' options against project_root
            if project_root and "path" in options and isinstance(options["path"], str):
                p = Path(options["path"])
                if not p.is_absolute():
                    options["path"] = str((project_root / p).resolve())
            catalogs[cat_name] = CatalogConfig(
                name=cat_name,
                type=cat_data.get("type", ""),
                options=options,
            )

        engines: list[EngineConfig] = []
        for eng_data in data.get("engines", []):
            if not isinstance(eng_data, dict):
                continue
            eng_name = eng_data.get("name", "")
            engines.append(EngineConfig(
                name=eng_name,
                type=eng_data.get("type", ""),
                catalogs=eng_data.get("catalogs", []),
                options={k: v for k, v in eng_data.items() if k not in ("name", "type", "catalogs")},
            ))

        return ResolvedProfile(
            name=name,
            default_engine=data.get("default_engine", ""),
            catalogs=catalogs,
            engines=engines,
        )
