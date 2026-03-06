"""Property test for DeclarationLoader — quality check attachment ordering.

Feature: rivet-config, Property 33: Quality check attachment ordering
Validates: Requirements 23.2
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import yaml
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.declarations import DeclarationLoader
from rivet_config.models import ProjectManifest

# Strategy: valid joint names
_joint_name = st.from_regex(r"[a-z][a-z0-9_]{1,15}", fullmatch=True)

# Number of checks per source (0 means that source is absent)
_check_count = st.integers(min_value=0, max_value=3)


def _make_manifest(
    project_root: Path,
    sources_dir: Path,
    joints_dir: Path,
    sinks_dir: Path,
    quality_dir: Path | None,
) -> ProjectManifest:
    return ProjectManifest(
        project_root=project_root,
        profiles_path=project_root / "profiles.yaml",
        sources_dir=sources_dir,
        joints_dir=joints_dir,
        sinks_dir=sinks_dir,
        quality_dir=quality_dir,
        tests_dir=project_root / "tests",
        fixtures_dir=project_root / "fixtures",
    )


@given(
    joint_name=_joint_name,
    n_inline=_check_count,
    n_dedicated=_check_count,
    n_colocated=_check_count,
)
@settings(max_examples=100)
def test_yaml_joint_quality_attachment_order(
    joint_name: str,
    n_inline: int,
    n_dedicated: int,
    n_colocated: int,
) -> None:
    """Property 33: For a YAML joint, quality checks are attached in order:
    inline → dedicated → co-located. (SQL annotations don't apply to YAML joints.)

    Validates: Requirements 23.2
    """
    if n_inline == 0 and n_dedicated == 0 and n_colocated == 0:
        return  # Nothing to verify

    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        sources_dir = root / "sources"
        joints_dir = root / "joints"
        sinks_dir = root / "sinks"
        quality_dir = root / "quality"
        for d in (sources_dir, joints_dir, sinks_dir, quality_dir):
            d.mkdir()

        # Write YAML joint with optional inline quality
        joint_content: dict = {"name": joint_name, "type": "source", "catalog": "cat"}
        if n_inline > 0:
            joint_content["quality"] = {
                "assertions": [{"type": "row_count"}] * n_inline
            }
        (sources_dir / f"{joint_name}.yaml").write_text(yaml.dump(joint_content))

        # Write dedicated quality file
        if n_dedicated > 0:
            (quality_dir / f"{joint_name}.yaml").write_text(
                yaml.dump({"assertions": [{"type": "row_count"}] * n_dedicated})
            )

        # Write co-located quality file: same stem as joint but .yml extension,
        # no name/type fields so it's classified as a quality file, not a joint.
        if n_colocated > 0:
            (sources_dir / f"{joint_name}.yml").write_text(
                yaml.dump({"assertions": [{"type": "row_count"}] * n_colocated})
            )

        manifest = _make_manifest(root, sources_dir, joints_dir, sinks_dir, quality_dir)
        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        assert not errors, f"Unexpected errors: {errors}"
        assert len(decls) == 1

        checks = decls[0].quality_checks
        expected_total = n_inline + n_dedicated + n_colocated
        assert len(checks) == expected_total, (
            f"Expected {expected_total} checks, got {len(checks)}: {checks}"
        )

        # Verify ordering: inline first, then dedicated, then colocated
        idx = 0
        if n_inline > 0:
            for c in checks[idx : idx + n_inline]:
                assert c.source == "inline", f"Expected 'inline', got '{c.source}'"
            idx += n_inline
        if n_dedicated > 0:
            for c in checks[idx : idx + n_dedicated]:
                assert c.source == "dedicated", f"Expected 'dedicated', got '{c.source}'"
            idx += n_dedicated
        if n_colocated > 0:
            for c in checks[idx : idx + n_colocated]:
                assert c.source == "colocated", f"Expected 'colocated', got '{c.source}'"


@given(
    joint_name=_joint_name,
    n_sql_annotation=_check_count,
    n_dedicated=_check_count,
    n_colocated=_check_count,
)
@settings(max_examples=100)
def test_sql_joint_quality_attachment_order(
    joint_name: str,
    n_sql_annotation: int,
    n_dedicated: int,
    n_colocated: int,
) -> None:
    """Property 33: For a SQL joint, quality checks are attached in order:
    SQL annotations → dedicated → co-located. (Inline doesn't apply to SQL joints.)

    Validates: Requirements 23.2
    """
    if n_sql_annotation == 0 and n_dedicated == 0 and n_colocated == 0:
        return  # Nothing to verify

    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        sources_dir = root / "sources"
        joints_dir = root / "joints"
        sinks_dir = root / "sinks"
        quality_dir = root / "quality"
        for d in (sources_dir, joints_dir, sinks_dir, quality_dir):
            d.mkdir()

        # Write SQL joint with optional annotation-based quality checks
        annotation_lines = [f"-- rivet:name: {joint_name}"]
        for _ in range(n_sql_annotation):
            annotation_lines.append("-- rivet:assert: not_null(col1)")
        annotation_lines.append("SELECT 1")
        (joints_dir / f"{joint_name}.sql").write_text("\n".join(annotation_lines))

        # Write dedicated quality file
        if n_dedicated > 0:
            (quality_dir / f"{joint_name}.yaml").write_text(
                yaml.dump({"assertions": [{"type": "row_count"}] * n_dedicated})
            )

        # Write co-located quality file: same stem as joint but .yml extension,
        # no name/type fields so it's classified as a quality file, not a joint.
        if n_colocated > 0:
            (joints_dir / f"{joint_name}.yml").write_text(
                yaml.dump({"assertions": [{"type": "row_count"}] * n_colocated})
            )

        manifest = _make_manifest(root, sources_dir, joints_dir, sinks_dir, quality_dir)
        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        assert not errors, f"Unexpected errors: {errors}"
        assert len(decls) == 1

        checks = decls[0].quality_checks
        expected_total = n_sql_annotation + n_dedicated + n_colocated
        assert len(checks) == expected_total, (
            f"Expected {expected_total} checks, got {len(checks)}: {checks}"
        )

        # Verify ordering: sql_annotation first, then dedicated, then colocated
        idx = 0
        if n_sql_annotation > 0:
            for c in checks[idx : idx + n_sql_annotation]:
                assert c.source == "sql_annotation", f"Expected 'sql_annotation', got '{c.source}'"
            idx += n_sql_annotation
        if n_dedicated > 0:
            for c in checks[idx : idx + n_dedicated]:
                assert c.source == "dedicated", f"Expected 'dedicated', got '{c.source}'"
            idx += n_dedicated
        if n_colocated > 0:
            for c in checks[idx : idx + n_colocated]:
                assert c.source == "colocated", f"Expected 'colocated', got '{c.source}'"
