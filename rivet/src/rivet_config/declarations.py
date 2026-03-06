"""DeclarationLoader: discover joint files, dispatch to parsers, enforce uniqueness."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import yaml

from rivet_config.annotations import AnnotationParser
from rivet_config.errors import ConfigError
from rivet_config.models import JointDeclaration, ProjectManifest
from rivet_config.python_parser import PythonParser
from rivet_config.quality import QualityParser
from rivet_config.sql_parser import SQLParser
from rivet_config.yaml_parser import YAMLParser

_YAML_EXTENSIONS = frozenset({".yaml", ".yml"})
_SQL_EXTENSIONS = frozenset({".sql"})
_PY_EXTENSIONS = frozenset({".py"})
_ALL_EXTENSIONS = _YAML_EXTENSIONS | _SQL_EXTENSIONS | _PY_EXTENSIONS


def _is_hidden(name: str) -> bool:
    return name.startswith(".")


def _is_excluded_dir(name: str) -> bool:
    return name.startswith("_") or _is_hidden(name)


def _discover_files(directory: Path) -> list[Path]:
    """Recursively discover files with recognized extensions, following symlinks."""
    results: list[Path] = []
    if not directory.is_dir():
        return results
    for item in sorted(directory.iterdir()):
        if item.is_dir():
            if not _is_excluded_dir(item.name):
                results.extend(_discover_files(item))
        elif item.is_file():
            if not _is_hidden(item.name) and item.suffix in _ALL_EXTENSIONS:
                results.append(item)
    return results


def _is_joint_yaml(file_path: Path) -> bool:
    """Check if a YAML file is a joint declaration (has 'name' and 'type' top-level keys)."""
    try:
        raw = yaml.safe_load(file_path.read_text())
    except Exception:
        return False
    return isinstance(raw, dict) and "name" in raw and "type" in raw


class DeclarationLoader:
    """Discovers joint files, dispatches to parsers, enforces name uniqueness."""

    def __init__(self, project_root: Path | None = None) -> None:
        self._yaml_parser = YAMLParser()
        self._sql_parser = SQLParser()
        self._python_parser = PythonParser(project_root or Path("."))
        self._quality_parser = QualityParser()
        self._annotation_parser = AnnotationParser()

    def _parse_joint_file(
        self, f: Path, errors: list[ConfigError],
    ) -> JointDeclaration | None:
        """Parse a single joint file and attach inline quality checks."""
        if f.suffix in _SQL_EXTENSIONS:
            decl, errs = self._sql_parser.parse(f)
        elif f.suffix in _PY_EXTENSIONS:
            decl, errs = self._python_parser.parse(f)
        else:
            decl, errs = self._yaml_parser.parse(f)
        errors.extend(errs)
        if decl is None:
            return None

        quality_checks = list(decl.quality_checks)

        if f.suffix in _YAML_EXTENSIONS:
            try:
                raw = yaml.safe_load(f.read_text())
            except Exception:
                raw = None
            if isinstance(raw, dict) and "quality" in raw and isinstance(raw["quality"], dict):
                inline_checks, q_errs = self._quality_parser.parse_inline(raw["quality"], f)
                errors.extend(q_errs)
                quality_checks.extend(inline_checks)

        if f.suffix in _SQL_EXTENSIONS:
            try:
                text = f.read_text()
                lines = text.splitlines(keepends=True)
                annotations, _, _ = self._annotation_parser.parse(lines, f)
                sql_checks, q_errs = self._quality_parser.parse_sql_annotations(annotations, f)
                errors.extend(q_errs)
                quality_checks.extend(sql_checks)
            except Exception:
                pass

        if f.suffix in _PY_EXTENSIONS:
            try:
                text = f.read_text()
                lines = text.splitlines(keepends=True)
                annotations, _, _ = self._annotation_parser.parse(
                    lines, f, comment_prefix="python",
                )
                py_checks, q_errs = self._quality_parser.parse_sql_annotations(annotations, f)
                errors.extend(q_errs)
                quality_checks.extend(py_checks)
            except Exception:
                pass

        if quality_checks:
            decl = replace(decl, quality_checks=quality_checks)
        return decl

    def _attach_quality_files(
        self,
        declarations: list[JointDeclaration],
        manifest: ProjectManifest,
        colocated_quality_files: list[Path],
        errors: list[ConfigError],
    ) -> list[JointDeclaration]:
        """Attach dedicated and co-located quality checks to declarations."""
        dedicated_quality: dict[str, list[Path]] = {}
        if manifest.quality_dir is not None and manifest.quality_dir.is_dir():
            quality_files = sorted(
                f for f in _discover_files(manifest.quality_dir)
                if f.suffix in _YAML_EXTENSIONS
            )
            for qf in quality_files:
                try:
                    raw = yaml.safe_load(qf.read_text())
                except Exception:
                    raw = None
                target = None
                if isinstance(raw, dict):
                    target = raw.get("joint")
                if target is None:
                    target = qf.stem
                dedicated_quality.setdefault(str(target), []).append(qf)

        colocated_quality: dict[str, list[Path]] = {}
        for qf in colocated_quality_files:
            colocated_quality.setdefault(qf.stem, []).append(qf)

        result = list(declarations)
        for i, decl in enumerate(result):
            extra_checks: list = []  # type: ignore[type-arg]
            for qf in dedicated_quality.get(decl.name, []):
                checks, q_errs = self._quality_parser.parse_dedicated_file(qf)
                errors.extend(q_errs)
                extra_checks.extend(checks)
            for qf in sorted(colocated_quality.get(decl.name, [])):
                checks, q_errs = self._quality_parser.parse_colocated_file(qf)
                errors.extend(q_errs)
                extra_checks.extend(checks)
            if extra_checks:
                result[i] = replace(decl, quality_checks=list(decl.quality_checks) + extra_checks)
        return result

    @staticmethod
    def _validate_directories(
        manifest: ProjectManifest,
    ) -> tuple[list[Path], list[ConfigError]]:
        """Validate that declared directories exist."""
        errors: list[ConfigError] = []
        valid_dirs: list[Path] = []
        for dir_path, key in [
            (manifest.sources_dir, "sources"),
            (manifest.joints_dir, "joints"),
            (manifest.sinks_dir, "sinks"),
        ]:
            if not dir_path.is_dir():
                errors.append(ConfigError(
                    source_file=None,
                    message=f"Directory '{dir_path}' declared in '{key}' does not exist.",
                    remediation=f"Create the directory or update the '{key}' path in rivet.yaml.",
                ))
            else:
                valid_dirs.append(dir_path)
        return valid_dirs, errors

    @staticmethod
    def _classify_files(
        files: list[Path],
    ) -> tuple[list[Path], list[Path]]:
        """Classify files into joint files and co-located quality files."""
        joint_files: list[Path] = []
        colocated_quality_files: list[Path] = []
        for f in files:
            if f.suffix in _SQL_EXTENSIONS or f.suffix in _PY_EXTENSIONS:
                joint_files.append(f)
            elif f.suffix in _YAML_EXTENSIONS:
                if _is_joint_yaml(f):
                    joint_files.append(f)
                else:
                    colocated_quality_files.append(f)
        return joint_files, colocated_quality_files

    def _parse_all_joints(
        self, joint_files: list[Path], errors: list[ConfigError],
    ) -> list[JointDeclaration]:
        """Parse all joint files into declarations."""
        declarations: list[JointDeclaration] = []
        for f in joint_files:
            decl = self._parse_joint_file(f, errors)
            if decl is not None:
                declarations.append(decl)
        return declarations

    @staticmethod
    def _validate_uniqueness(
        declarations: list[JointDeclaration], errors: list[ConfigError],
    ) -> None:
        """Check for duplicate joint names."""
        seen: dict[str, Path] = {}
        for decl in declarations:
            if decl.name in seen:
                errors.append(ConfigError(
                    source_file=decl.source_path,
                    message=f"Duplicate joint name '{decl.name}' (also declared in '{seen[decl.name]}').",
                    remediation="Rename one of the joints to ensure all names are unique.",
                ))
            else:
                seen[decl.name] = decl.source_path

    def load(
        self, manifest: ProjectManifest,
    ) -> tuple[list[JointDeclaration], list[ConfigError]]:
        errors: list[ConfigError] = []

        valid_dirs, dir_errors = self._validate_directories(manifest)
        errors.extend(dir_errors)

        all_files: list[Path] = []
        for d in valid_dirs:
            all_files.extend(_discover_files(d))
        all_files.sort()

        joint_files, colocated_quality_files = self._classify_files(all_files)
        declarations = self._parse_all_joints(joint_files, errors)
        declarations = self._attach_quality_files(
            declarations, manifest, colocated_quality_files, errors,
        )
        self._validate_uniqueness(declarations, errors)

        declarations.sort(key=lambda d: d.source_path)
        return declarations, errors
