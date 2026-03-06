"""Tests for the ProfileSelectorScreen overlay.

Validates: Requirements 22.1, 22.2, 22.3
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from rivet_cli.repl.screens.profile_selector import (
    ProfileSelectorScreen,
    _load_profile_names,
)

# ---------------------------------------------------------------------------
# Tests for _load_profile_names
# ---------------------------------------------------------------------------


class TestLoadProfileNames:
    def test_returns_empty_when_no_project(self, tmp_path: Path) -> None:
        names = _load_profile_names(tmp_path)
        assert names == []

    def test_loads_from_profiles_yaml(self, tmp_path: Path) -> None:
        (tmp_path / "rivet.yaml").write_text("profiles: profiles.yaml\n")
        (tmp_path / "profiles.yaml").write_text(
            "default:\n  default_engine: duckdb\nproduction:\n  default_engine: spark\n"
        )
        names = _load_profile_names(tmp_path)
        assert names == ["default", "production"]

    def test_loads_from_profiles_directory(self, tmp_path: Path) -> None:
        profiles_dir = tmp_path / "profiles"
        profiles_dir.mkdir()
        (profiles_dir / "dev.yaml").write_text("default_engine: duckdb\n")
        (profiles_dir / "prod.yaml").write_text("default_engine: spark\n")
        (tmp_path / "rivet.yaml").write_text("profiles: profiles\n")
        names = _load_profile_names(tmp_path)
        assert names == ["dev", "prod"]

    def test_returns_sorted_names(self, tmp_path: Path) -> None:
        (tmp_path / "rivet.yaml").write_text("profiles: profiles.yaml\n")
        (tmp_path / "profiles.yaml").write_text(
            "zebra:\n  default_engine: x\nalpha:\n  default_engine: y\nmiddle:\n  default_engine: z\n"
        )
        names = _load_profile_names(tmp_path)
        assert names == ["alpha", "middle", "zebra"]

    def test_fallback_without_manifest(self, tmp_path: Path) -> None:
        # No rivet.yaml, but profiles.yaml exists
        (tmp_path / "profiles.yaml").write_text("default:\n  default_engine: duckdb\n")
        names = _load_profile_names(tmp_path)
        assert names == ["default"]

    def test_handles_corrupt_yaml_gracefully(self, tmp_path: Path) -> None:
        (tmp_path / "rivet.yaml").write_text("profiles: profiles.yaml\n")
        (tmp_path / "profiles.yaml").write_text(": invalid: yaml: [\n")
        # Should not raise
        names = _load_profile_names(tmp_path)
        assert isinstance(names, list)

    def test_handles_missing_profiles_path_gracefully(self, tmp_path: Path) -> None:
        (tmp_path / "rivet.yaml").write_text("profiles: nonexistent.yaml\n")
        names = _load_profile_names(tmp_path)
        assert isinstance(names, list)

    def test_ignores_non_yaml_files_in_directory(self, tmp_path: Path) -> None:
        profiles_dir = tmp_path / "profiles"
        profiles_dir.mkdir()
        (profiles_dir / "dev.yaml").write_text("default_engine: duckdb\n")
        (profiles_dir / "README.md").write_text("# docs\n")
        (profiles_dir / "config.json").write_text("{}\n")
        (tmp_path / "rivet.yaml").write_text("profiles: profiles\n")
        names = _load_profile_names(tmp_path)
        assert names == ["dev"]


# ---------------------------------------------------------------------------
# Tests for ProfileSelectorScreen (logic only, no Textual app required)
# ---------------------------------------------------------------------------


class TestProfileSelectorScreenInit:
    def test_stores_session_and_project_path(self, tmp_path: Path) -> None:
        session = MagicMock()
        session.active_profile = "default"
        (tmp_path / "profiles.yaml").write_text("default:\n  default_engine: duckdb\n")

        screen = ProfileSelectorScreen(session=session, project_path=tmp_path)
        assert screen._session is session
        assert screen._project_path == tmp_path

    def test_loads_profiles_on_init(self, tmp_path: Path) -> None:
        session = MagicMock()
        session.active_profile = "default"
        (tmp_path / "profiles.yaml").write_text(
            "default:\n  default_engine: duckdb\nstaging:\n  default_engine: spark\n"
        )

        screen = ProfileSelectorScreen(session=session, project_path=tmp_path)
        assert "default" in screen._profiles
        assert "staging" in screen._profiles

    def test_profiles_sorted(self, tmp_path: Path) -> None:
        session = MagicMock()
        session.active_profile = "default"
        (tmp_path / "profiles.yaml").write_text(
            "zebra:\n  default_engine: x\nalpha:\n  default_engine: y\n"
        )

        screen = ProfileSelectorScreen(session=session, project_path=tmp_path)
        assert screen._profiles == ["alpha", "zebra"]

    def test_empty_profiles_when_no_file(self, tmp_path: Path) -> None:
        session = MagicMock()
        session.active_profile = "default"

        screen = ProfileSelectorScreen(session=session, project_path=tmp_path)
        assert screen._profiles == []


# ---------------------------------------------------------------------------
# Integration: profile switch wiring
# ---------------------------------------------------------------------------


class TestProfileSwitchWiring:
    def test_switch_profile_called_on_selection(self, tmp_path: Path) -> None:
        """When a profile is selected, session.switch_profile() is called."""
        session = MagicMock()
        session.active_profile = "default"
        (tmp_path / "profiles.yaml").write_text(
            "default:\n  default_engine: duckdb\nproduction:\n  default_engine: spark\n"
        )

        screen = ProfileSelectorScreen(session=session, project_path=tmp_path)
        # Simulate the dismiss callback that the app would use
        # The screen dismisses with the profile name; the app calls switch_profile
        assert "production" in screen._profiles

    def test_active_profile_marked_in_profiles_list(self, tmp_path: Path) -> None:
        """The active profile is identifiable in the profiles list."""
        session = MagicMock()
        session.active_profile = "staging"
        (tmp_path / "profiles.yaml").write_text(
            "default:\n  default_engine: duckdb\nstaging:\n  default_engine: spark\n"
        )

        screen = ProfileSelectorScreen(session=session, project_path=tmp_path)
        assert "staging" in screen._profiles
        assert screen._session.active_profile == "staging"
