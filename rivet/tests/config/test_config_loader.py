"""Property test for ConfigLoader — collect-all-errors before failing.

Feature: rivet-config, Property 30: Collect-all-errors before failing
Validates: Requirements 17.1, 17.2, 17.3, 17.4
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from rivet_config import load_config


@pytest.fixture()
def project_dir(tmp_path: Path) -> Path:
    return tmp_path


def _write_manifest(root: Path, content: dict) -> None:
    (root / "rivet.yaml").write_text(yaml.dump(content))


def _make_valid_project(root: Path) -> None:
    """Create a minimal valid project structure."""
    _write_manifest(root, {
        "profiles": "profiles.yaml",
        "sources": "sources",
        "joints": "joints",
        "sinks": "sinks",
    })
    (root / "sources").mkdir()
    (root / "joints").mkdir()
    (root / "sinks").mkdir()
    (root / "profiles.yaml").write_text(yaml.dump({
        "default": {
            "default_engine": "duckdb",
            "catalogs": {"main": {"type": "duckdb"}},
            "engines": [{"name": "duckdb", "type": "duckdb", "catalogs": ["duckdb"]}],
        }
    }))


class TestCollectAllErrors:
    """Property 30: ConfigResult collects all errors from all phases."""

    def test_valid_project_succeeds(self, project_dir: Path) -> None:
        _make_valid_project(project_dir)
        result = load_config(project_dir)
        assert result.success
        assert result.manifest is not None
        assert result.profile is not None
        assert result.errors == []

    def test_missing_manifest_returns_errors(self, project_dir: Path) -> None:
        result = load_config(project_dir)
        assert not result.success
        assert len(result.errors) >= 1
        assert result.manifest is None

    def test_manifest_failure_skips_subsequent_phases(self, project_dir: Path) -> None:
        """If manifest parsing fails completely, profile and declarations are skipped."""
        result = load_config(project_dir)
        assert not result.success
        assert result.manifest is None
        assert result.profile is None
        assert result.declarations == []

    def test_profile_errors_and_declaration_errors_both_collected(self, project_dir: Path) -> None:
        """Profile resolution and declaration loading are independent — errors from both collected."""
        _write_manifest(project_dir, {
            "profiles": "profiles.yaml",
            "sources": "sources",
            "joints": "joints",
            "sinks": "sinks",
        })
        root = project_dir
        (root / "sources").mkdir()
        (root / "joints").mkdir()
        (root / "sinks").mkdir()
        # Profile with missing required fields
        (root / "profiles.yaml").write_text(yaml.dump({"default": {}}))
        # Invalid joint file
        (root / "joints" / "bad.yaml").write_text(yaml.dump({"name": "INVALID", "type": "sql"}))

        result = load_config(project_dir)
        assert not result.success
        # Should have profile errors (missing default_engine, catalogs, engines)
        # AND declaration errors (invalid name)
        messages = [e.message for e in result.errors]
        has_profile_error = any("default_engine" in m for m in messages)
        has_decl_error = any("INVALID" in m or "name" in m.lower() for m in messages)
        assert has_profile_error, f"Expected profile error, got: {messages}"
        assert has_decl_error, f"Expected declaration error, got: {messages}"

    def test_errors_have_required_fields(self, project_dir: Path) -> None:
        """Each error includes source_file (where applicable), message, and remediation."""
        _write_manifest(project_dir, {
            "profiles": "profiles.yaml",
            "sources": "sources",
            "joints": "joints",
            "sinks": "sinks",
            "unknown_key": "value",
        })
        result = load_config(project_dir)
        assert not result.success
        for error in result.errors:
            assert isinstance(error.message, str) and error.message
            assert isinstance(error.remediation, str) and error.remediation

    def test_multiple_manifest_errors_collected(self, project_dir: Path) -> None:
        """Multiple errors in manifest are all collected."""
        _write_manifest(project_dir, {"unknown1": "x", "unknown2": "y"})
        result = load_config(project_dir)
        assert not result.success
        # Should have errors for missing required keys AND unrecognized keys
        assert len(result.errors) >= 4 + 2  # 4 missing required + 2 unrecognized

    def test_config_result_success_property(self, project_dir: Path) -> None:
        """ConfigResult.success is True iff errors list is empty."""
        _make_valid_project(project_dir)
        result = load_config(project_dir)
        assert result.success == (len(result.errors) == 0)

    def test_missing_directories_produce_errors(self, project_dir: Path) -> None:
        """Missing declared directories produce errors."""
        _write_manifest(project_dir, {
            "profiles": "profiles.yaml",
            "sources": "nonexistent_sources",
            "joints": "nonexistent_joints",
            "sinks": "nonexistent_sinks",
        })
        (project_dir / "profiles.yaml").write_text(yaml.dump({
            "default": {
                "default_engine": "duckdb",
                "catalogs": {"main": {"type": "duckdb"}},
                "engines": [{"name": "duckdb", "type": "duckdb", "catalogs": ["duckdb"]}],
            }
        }))
        result = load_config(project_dir)
        assert not result.success
        dir_errors = [e for e in result.errors if "does not exist" in e.message]
        assert len(dir_errors) >= 3

    def test_warnings_collected_alongside_errors(self, project_dir: Path) -> None:
        """Warnings are collected even when errors exist."""
        _write_manifest(project_dir, {
            "profiles": "profiles.yaml",
            "sources": "sources",
            "joints": "joints",
            "sinks": "sinks",
            "assertions": "quality",  # deprecated key
        })
        (project_dir / "sources").mkdir()
        (project_dir / "joints").mkdir()
        (project_dir / "sinks").mkdir()
        (project_dir / "profiles.yaml").write_text(yaml.dump({"default": {}}))
        result = load_config(project_dir)
        # Should have deprecation warning
        assert any("eprecated" in w.message for w in result.warnings)

    def test_profile_name_selection(self, project_dir: Path) -> None:
        """Explicit profile name is passed through to resolver."""
        _make_valid_project(project_dir)
        result = load_config(project_dir, profile_name="nonexistent")
        assert not result.success
        assert any("nonexistent" in e.message for e in result.errors)
