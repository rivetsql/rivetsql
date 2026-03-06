"""Tests for write_catalog_to_profile — task 2.1."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from rivet_cli.commands.catalog_create import CatalogWriteError, write_catalog_to_profile
from rivet_cli.errors import RVT_883

# --- Single-file layout tests ---


def test_write_new_catalog_to_single_file(tmp_path: Path) -> None:
    """Writing a new catalog into an existing single-file profile."""
    profiles = tmp_path / "profiles.yaml"
    profiles.write_text(yaml.dump({
        "default": {
            "catalogs": {"existing": {"type": "duckdb", "path": "./data"}},
            "engines": [{"name": "eng1", "type": "duckdb", "catalogs": ["existing"]}],
            "default_engine": "eng1",
        }
    }))

    write_catalog_to_profile(
        profiles, "default", "my_pg",
        {"type": "postgres", "host": "localhost"},
        None,
    )

    data = yaml.safe_load(profiles.read_text())
    assert data["default"]["catalogs"]["my_pg"] == {"type": "postgres", "host": "localhost"}
    assert data["default"]["catalogs"]["existing"] == {"type": "duckdb", "path": "./data"}


def test_overwrite_existing_catalog(tmp_path: Path) -> None:
    """Overwriting an existing catalog replaces the block entirely."""
    profiles = tmp_path / "profiles.yaml"
    profiles.write_text(yaml.dump({
        "default": {
            "catalogs": {"my_pg": {"type": "postgres", "host": "old"}},
            "engines": [],
            "default_engine": "eng1",
        }
    }))

    write_catalog_to_profile(
        profiles, "default", "my_pg",
        {"type": "postgres", "host": "new", "port": 5432},
        None,
    )

    data = yaml.safe_load(profiles.read_text())
    assert data["default"]["catalogs"]["my_pg"] == {"type": "postgres", "host": "new", "port": 5432}


def test_engine_updates_idempotent(tmp_path: Path) -> None:
    """Engine catalog list update is idempotent — no duplicates."""
    profiles = tmp_path / "profiles.yaml"
    profiles.write_text(yaml.dump({
        "default": {
            "catalogs": {},
            "engines": [{"name": "eng1", "type": "duckdb", "catalogs": ["my_pg"]}],
            "default_engine": "eng1",
        }
    }))

    write_catalog_to_profile(
        profiles, "default", "my_pg",
        {"type": "postgres"},
        {"eng1": ["my_pg"]},
    )

    data = yaml.safe_load(profiles.read_text())
    assert data["default"]["engines"][0]["catalogs"] == ["my_pg"]


def test_engine_updates_adds_new_catalog(tmp_path: Path) -> None:
    """Engine catalog list gets the new catalog appended."""
    profiles = tmp_path / "profiles.yaml"
    profiles.write_text(yaml.dump({
        "default": {
            "catalogs": {},
            "engines": [{"name": "eng1", "type": "duckdb", "catalogs": ["existing"]}],
            "default_engine": "eng1",
        }
    }))

    write_catalog_to_profile(
        profiles, "default", "new_cat",
        {"type": "postgres"},
        {"eng1": ["new_cat"]},
    )

    data = yaml.safe_load(profiles.read_text())
    assert data["default"]["engines"][0]["catalogs"] == ["existing", "new_cat"]


# --- Per-profile directory layout tests ---


def test_write_to_directory_layout(tmp_path: Path) -> None:
    """Writing to a per-profile directory creates/updates the profile file."""
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()
    profile_file = profiles_dir / "dev.yaml"
    profile_file.write_text(yaml.dump({
        "catalogs": {"old": {"type": "duckdb"}},
        "engines": [{"name": "eng1", "type": "duckdb", "catalogs": ["old"]}],
        "default_engine": "eng1",
    }))

    write_catalog_to_profile(
        profiles_dir, "dev", "my_s3",
        {"type": "s3", "bucket": "test"},
        None,
    )

    data = yaml.safe_load(profile_file.read_text())
    assert data["catalogs"]["my_s3"] == {"type": "s3", "bucket": "test"}
    assert data["catalogs"]["old"] == {"type": "duckdb"}


def test_write_to_directory_new_profile(tmp_path: Path) -> None:
    """Writing to a directory for a profile that doesn't exist yet creates the file."""
    profiles_dir = tmp_path / "profiles"
    profiles_dir.mkdir()

    write_catalog_to_profile(
        profiles_dir, "staging", "my_cat",
        {"type": "postgres"},
        None,
    )

    data = yaml.safe_load((profiles_dir / "staging.yaml").read_text())
    assert data["catalogs"]["my_cat"] == {"type": "postgres"}


# --- Error handling ---


def test_raises_cli_error_on_write_failure(tmp_path: Path) -> None:
    """Unwritable path raises CatalogWriteError with RVT-883."""
    bad_path = tmp_path / "nonexistent_dir" / "profiles.yaml"

    with pytest.raises(CatalogWriteError) as exc_info:
        write_catalog_to_profile(
            bad_path, "default", "cat",
            {"type": "test"},
            None,
        )

    assert exc_info.value.cli_error.code == RVT_883


def test_engine_updates_multiple_engines(tmp_path: Path) -> None:
    """Engine updates can target multiple engines at once."""
    profiles = tmp_path / "profiles.yaml"
    profiles.write_text(yaml.dump({
        "default": {
            "catalogs": {},
            "engines": [
                {"name": "eng1", "type": "duckdb", "catalogs": []},
                {"name": "eng2", "type": "polars", "catalogs": ["other"]},
            ],
            "default_engine": "eng1",
        }
    }))

    write_catalog_to_profile(
        profiles, "default", "my_cat",
        {"type": "postgres"},
        {"eng1": ["my_cat"], "eng2": ["my_cat"]},
    )

    data = yaml.safe_load(profiles.read_text())
    assert data["default"]["engines"][0]["catalogs"] == ["my_cat"]
    assert data["default"]["engines"][1]["catalogs"] == ["other", "my_cat"]


def test_write_creates_file_if_not_exists(tmp_path: Path) -> None:
    """Single-file layout creates the file if it doesn't exist."""
    profiles = tmp_path / "profiles.yaml"

    write_catalog_to_profile(
        profiles, "default", "my_cat",
        {"type": "duckdb"},
        None,
    )

    data = yaml.safe_load(profiles.read_text())
    assert data["default"]["catalogs"]["my_cat"] == {"type": "duckdb"}
