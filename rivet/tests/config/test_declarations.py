"""Tests for DeclarationLoader."""

from __future__ import annotations

from pathlib import Path

from rivet_config.declarations import DeclarationLoader
from rivet_config.models import ProjectManifest


def _make_manifest(tmp_path: Path, **overrides) -> ProjectManifest:
    """Create a ProjectManifest with directories under tmp_path."""
    sources = tmp_path / "sources"
    joints = tmp_path / "joints"
    sinks = tmp_path / "sinks"
    for d in (sources, joints, sinks):
        d.mkdir(exist_ok=True)
    return ProjectManifest(
        project_root=tmp_path,
        profiles_path=tmp_path / "profiles.yaml",
        sources_dir=overrides.get("sources_dir", sources),
        joints_dir=overrides.get("joints_dir", joints),
        sinks_dir=overrides.get("sinks_dir", sinks),
        quality_dir=overrides.get("quality_dir"),
        tests_dir=tmp_path / "tests",
        fixtures_dir=tmp_path / "fixtures",
    )


class TestFileDiscovery:
    """Test file discovery rules."""

    def test_discovers_yaml_and_sql_files(self, tmp_path: Path) -> None:
        manifest = _make_manifest(tmp_path)
        (tmp_path / "sources" / "a.yaml").write_text("name: a\ntype: source\ncatalog: cat")
        (tmp_path / "joints" / "b.sql").write_text("-- rivet:name: b\nSELECT 1")
        (tmp_path / "sinks" / "c.yml").write_text("name: c\ntype: sink\ncatalog: cat\ntable: t")

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        assert len(errors) == 0
        assert len(decls) == 3
        names = {d.name for d in decls}
        assert names == {"a", "b", "c"}

    def test_ignores_hidden_files(self, tmp_path: Path) -> None:
        manifest = _make_manifest(tmp_path)
        (tmp_path / "sources" / ".hidden.yaml").write_text("name: hidden\ntype: source\ncatalog: c")
        (tmp_path / "sources" / "visible.yaml").write_text("name: visible\ntype: source\ncatalog: c")

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        names = {d.name for d in decls}
        assert "hidden" not in names
        assert "visible" in names

    def test_ignores_underscore_prefixed_dirs(self, tmp_path: Path) -> None:
        manifest = _make_manifest(tmp_path)
        subdir = tmp_path / "sources" / "_internal"
        subdir.mkdir()
        (subdir / "x.yaml").write_text("name: x\ntype: source\ncatalog: c")
        (tmp_path / "sources" / "y.yaml").write_text("name: y\ntype: source\ncatalog: c")

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        names = {d.name for d in decls}
        assert "x" not in names
        assert "y" in names

    def test_ignores_unrecognized_extensions(self, tmp_path: Path) -> None:
        manifest = _make_manifest(tmp_path)
        (tmp_path / "sources" / "readme.md").write_text("# readme")
        (tmp_path / "sources" / "data.csv").write_text("a,b,c")
        (tmp_path / "sources" / "a.yaml").write_text("name: a\ntype: source\ncatalog: c")

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        assert len(decls) == 1
        assert decls[0].name == "a"

    def test_scans_subdirectories_recursively(self, tmp_path: Path) -> None:
        manifest = _make_manifest(tmp_path)
        subdir = tmp_path / "sources" / "nested" / "deep"
        subdir.mkdir(parents=True)
        (subdir / "a.yaml").write_text("name: a\ntype: source\ncatalog: c")

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        assert len(decls) == 1
        assert decls[0].name == "a"

    def test_follows_symlinks(self, tmp_path: Path) -> None:
        manifest = _make_manifest(tmp_path)
        real_dir = tmp_path / "real_sources"
        real_dir.mkdir()
        (real_dir / "a.yaml").write_text("name: a\ntype: source\ncatalog: c")
        link = tmp_path / "sources" / "linked"
        link.symlink_to(real_dir)

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        assert len(decls) == 1
        assert decls[0].name == "a"

    def test_error_on_missing_directory(self, tmp_path: Path) -> None:
        manifest = _make_manifest(
            tmp_path,
            sources_dir=tmp_path / "nonexistent",
        )

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        assert any("nonexistent" in e.message for e in errors)


class TestDispatch:
    """Test parser dispatch based on file extension."""

    def test_yaml_dispatched_to_yaml_parser(self, tmp_path: Path) -> None:
        manifest = _make_manifest(tmp_path)
        (tmp_path / "sources" / "src.yaml").write_text("name: src\ntype: source\ncatalog: c")

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        assert len(decls) == 1
        assert decls[0].joint_type == "source"

    def test_sql_dispatched_to_sql_parser(self, tmp_path: Path) -> None:
        manifest = _make_manifest(tmp_path)
        (tmp_path / "joints" / "j.sql").write_text("-- rivet:name: j\nSELECT 1")

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        assert len(decls) == 1
        assert decls[0].joint_type == "sql"
        assert decls[0].sql == "SELECT 1"


class TestYAMLClassification:
    """Test YAML file classification: joint vs co-located quality."""

    def test_yaml_with_name_and_type_is_joint(self, tmp_path: Path) -> None:
        manifest = _make_manifest(tmp_path)
        (tmp_path / "sources" / "a.yaml").write_text("name: a\ntype: source\ncatalog: c")

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        assert len(decls) == 1

    def test_yaml_without_name_type_is_quality(self, tmp_path: Path) -> None:
        manifest = _make_manifest(tmp_path)
        # Joint file
        (tmp_path / "sources" / "a.yaml").write_text("name: a\ntype: source\ncatalog: c")
        # Co-located quality file targeting joint 'a'
        (tmp_path / "sources" / "a_quality.yaml").write_text(
            "assertions:\n  - type: not_null\n    columns: [col1]"
        )

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        # Only the joint file should be a declaration
        assert len(decls) == 1
        assert decls[0].name == "a"


class TestNameUniqueness:
    """Test joint name uniqueness enforcement."""

    def test_duplicate_name_produces_error(self, tmp_path: Path) -> None:
        manifest = _make_manifest(tmp_path)
        (tmp_path / "sources" / "a.yaml").write_text("name: dup\ntype: source\ncatalog: c")
        (tmp_path / "joints" / "b.yaml").write_text("name: dup\ntype: sql\nsql: SELECT 1")

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        dup_errors = [e for e in errors if "Duplicate joint name" in e.message]
        assert len(dup_errors) == 1
        assert "dup" in dup_errors[0].message

    def test_unique_names_no_error(self, tmp_path: Path) -> None:
        manifest = _make_manifest(tmp_path)
        (tmp_path / "sources" / "a.yaml").write_text("name: alpha\ntype: source\ncatalog: c")
        (tmp_path / "joints" / "b.yaml").write_text("name: beta\ntype: sql\nsql: SELECT 1")

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        assert len(errors) == 0
        assert len(decls) == 2


class TestOutputOrdering:
    """Test deterministic output ordering."""

    def test_declarations_sorted_by_source_file(self, tmp_path: Path) -> None:
        manifest = _make_manifest(tmp_path)
        # Create files that would sort differently than creation order.
        (tmp_path / "sources" / "z.yaml").write_text("name: z\ntype: source\ncatalog: c")
        (tmp_path / "sources" / "a.yaml").write_text("name: a\ntype: source\ncatalog: c")
        (tmp_path / "joints" / "m.sql").write_text("-- rivet:name: m\nSELECT 1")

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        paths = [d.source_path for d in decls]
        assert paths == sorted(paths)


class TestQualityAttachment:
    """Test quality check attachment from all sources."""

    def test_inline_quality_attached(self, tmp_path: Path) -> None:
        manifest = _make_manifest(tmp_path)
        (tmp_path / "sources" / "a.yaml").write_text(
            "name: a\ntype: source\ncatalog: c\n"
            "quality:\n  assertions:\n    - type: not_null\n      columns: [col1]"
        )

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        assert len(decls) == 1
        assert len(decls[0].quality_checks) == 1
        assert decls[0].quality_checks[0].source == "inline"

    def test_sql_annotation_quality_attached(self, tmp_path: Path) -> None:
        manifest = _make_manifest(tmp_path)
        (tmp_path / "joints" / "j.sql").write_text(
            "-- rivet:name: j\n-- rivet:assert: not_null(col1)\nSELECT 1"
        )

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        assert len(decls) == 1
        assert len(decls[0].quality_checks) == 1
        assert decls[0].quality_checks[0].source == "sql_annotation"

    def test_dedicated_quality_attached(self, tmp_path: Path) -> None:
        quality_dir = tmp_path / "quality"
        quality_dir.mkdir()
        manifest = _make_manifest(tmp_path, quality_dir=quality_dir)
        (tmp_path / "sources" / "a.yaml").write_text("name: a\ntype: source\ncatalog: c")
        (quality_dir / "a.yaml").write_text(
            "assertions:\n  - type: not_null\n    columns: [col1]"
        )

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        assert len(decls) == 1
        assert len(decls[0].quality_checks) == 1
        assert decls[0].quality_checks[0].source == "dedicated"

    def test_colocated_quality_attached(self, tmp_path: Path) -> None:
        manifest = _make_manifest(tmp_path)
        (tmp_path / "sources" / "a.yaml").write_text("name: a\ntype: source\ncatalog: c")
        # Co-located quality file with same stem as joint
        (tmp_path / "sources" / "a.yml").write_text(
            "assertions:\n  - type: not_null\n    columns: [col1]"
        )

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        # 'a.yml' doesn't have name+type, so it's classified as co-located quality
        assert len(decls) == 1
        assert decls[0].name == "a"
        # But the stem is 'a' which matches the joint name
        colocated = [c for c in decls[0].quality_checks if c.source == "colocated"]
        assert len(colocated) == 1

    def test_quality_attachment_order(self, tmp_path: Path) -> None:
        """Quality checks should be attached in order: inline → SQL annotations → dedicated → co-located."""
        quality_dir = tmp_path / "quality"
        quality_dir.mkdir()
        manifest = _make_manifest(tmp_path, quality_dir=quality_dir)

        # Joint with inline quality
        (tmp_path / "sources" / "a.yaml").write_text(
            "name: a\ntype: source\ncatalog: c\n"
            "quality:\n  assertions:\n    - type: not_null\n      columns: [inline_col]"
        )
        # Dedicated quality
        (quality_dir / "a.yaml").write_text(
            "assertions:\n  - type: unique\n    columns: [dedicated_col]"
        )
        # Co-located quality (different extension, no name/type)
        (tmp_path / "sources" / "a.yml").write_text(
            "assertions:\n  - type: row_count\n    min: 1"
        )

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        assert len(decls) == 1
        checks = decls[0].quality_checks
        assert len(checks) == 3
        assert checks[0].source == "inline"
        assert checks[1].source == "dedicated"
        assert checks[2].source == "colocated"

    def test_empty_project_no_errors(self, tmp_path: Path) -> None:
        manifest = _make_manifest(tmp_path)

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        assert len(decls) == 0
        assert len(errors) == 0

    def test_parse_errors_collected(self, tmp_path: Path) -> None:
        manifest = _make_manifest(tmp_path)
        # Invalid YAML joint (missing required fields)
        (tmp_path / "sources" / "bad.yaml").write_text("name: bad\ntype: source")

        loader = DeclarationLoader()
        decls, errors = loader.load(manifest)

        assert len(errors) > 0
        assert len(decls) == 0
