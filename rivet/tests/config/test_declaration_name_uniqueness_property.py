"""Property test for DeclarationLoader — name uniqueness.

Feature: rivet-config, Property 29: Joint name uniqueness enforcement
Validates: Requirements 15.1, 15.2

For any set of joint declaration files where two or more files declare the same
joint name, the loader should produce an error identifying both file paths and
the duplicate name.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.declarations import DeclarationLoader
from rivet_config.models import ProjectManifest

_joint_name = st.from_regex(r"[a-z][a-z0-9_]{0,19}", fullmatch=True)
_dir_choice = st.sampled_from(["sources", "joints", "sinks"])


def _make_manifest(root: Path) -> ProjectManifest:
    sources = root / "sources"
    joints = root / "joints"
    sinks = root / "sinks"
    for d in (sources, joints, sinks):
        d.mkdir(exist_ok=True)
    return ProjectManifest(
        project_root=root,
        profiles_path=root / "profiles.yaml",
        sources_dir=sources,
        joints_dir=joints,
        sinks_dir=sinks,
        quality_dir=None,
        tests_dir=root / "tests",
        fixtures_dir=root / "fixtures",
    )


def _write_source(directory: Path, name: str, suffix: str = ".yaml") -> Path:
    """Write a minimal valid joint declaration file."""
    fp = directory / f"{name}{suffix}"
    fp.write_text(f"name: '{name}'\ntype: source\ncatalog: cat\n")
    return fp


@given(
    dup_name=_joint_name,
    dir_a=_dir_choice,
    dir_b=_dir_choice,
)
@settings(max_examples=100)
def test_duplicate_name_produces_error_with_both_paths(
    dup_name: str,
    dir_a: str,
    dir_b: str,
) -> None:
    """Property 29a: Two files with the same joint name produce an error identifying
    both file paths and the duplicate name.

    Validates: Requirements 15.1, 15.2
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        manifest = _make_manifest(root)

        # Write the same name in two different files (use unique filenames to avoid
        # overwriting the same file when dir_a == dir_b).
        fp_a = root / dir_a / f"{dup_name}_a.yaml"
        fp_b = root / dir_b / f"{dup_name}_b.yaml"
        fp_a.write_text(f"name: '{dup_name}'\ntype: source\ncatalog: cat\n")
        fp_b.write_text(f"name: '{dup_name}'\ntype: source\ncatalog: cat\n")

        loader = DeclarationLoader()
        _, errors = loader.load(manifest)

        dup_errors = [e for e in errors if "Duplicate" in e.message or dup_name in e.message]
        assert dup_errors, f"Expected a duplicate-name error for '{dup_name}', got errors: {errors}"
        # The error must mention the duplicate name.
        assert any(dup_name in e.message for e in dup_errors), (
            f"Error should mention duplicate name '{dup_name}': {dup_errors}"
        )
        # The error must reference at least one of the conflicting file paths.
        combined = " ".join(e.message + (str(e.source_file) or "") for e in dup_errors)
        assert str(fp_a) in combined or str(fp_b) in combined, (
            f"Error should reference one of the conflicting files: {dup_errors}"
        )


@given(
    names=st.lists(_joint_name, min_size=2, max_size=6, unique=True),
)
@settings(max_examples=100)
def test_unique_names_produce_no_uniqueness_errors(names: list[str]) -> None:
    """Property 29b: When all joint names are unique, no uniqueness errors are produced.

    Validates: Requirement 15.1
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        manifest = _make_manifest(root)

        for name in names:
            _write_source(root / "sources", name)

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        dup_errors = [e for e in errors if "Duplicate" in e.message]
        assert not dup_errors, f"Unexpected duplicate errors for unique names {names}: {dup_errors}"
        assert len(decls) == len(names), f"Expected {len(names)} declarations, got {len(decls)}"
