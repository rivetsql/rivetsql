"""Tests for generate_project reverse path orchestrator."""

from __future__ import annotations

from pathlib import Path

import pytest

from rivet_bridge.errors import BridgeValidationError
from rivet_bridge.models import BridgeResult, ProjectOutput
from rivet_bridge.reverse import generate_project
from rivet_config import CatalogConfig, EngineConfig, ResolvedProfile
from rivet_core import Assembly, Catalog, ComputeEngine, Joint


def _make_bridge_result(joints: list[Joint] | None = None) -> BridgeResult:
    """Create a minimal BridgeResult for testing."""
    if joints is None:
        joints = [
            Joint(name="raw_users", joint_type="source", sql="SELECT * FROM raw_users"),
            Joint(name="output_users", joint_type="sink", upstream=["raw_users"], sql="SELECT * FROM raw_users"),
        ]
    return BridgeResult(
        assembly=Assembly(joints),
        catalogs={"test_cat": Catalog(name="test_cat", type="arrow")},
        engines={"test_eng": ComputeEngine(name="test_eng", engine_type="arrow")},
        profile_snapshot=ResolvedProfile(
            name="test",
            default_engine="test_eng",
            catalogs={"test_cat": CatalogConfig(name="test_cat", type="arrow", options={})},
            engines=[EngineConfig(name="test_eng", type="arrow", catalogs=["test_cat"], options={})],
        ),
        source_formats={"raw_users": "yaml", "output_users": "yaml"},
    )


class TestGenerateProjectEmptyDir:
    """Test generate_project with empty output dir → all files written."""

    def test_writes_all_files(self, tmp_path: Path):
        result = generate_project(_make_bridge_result(), output_dir=tmp_path)

        assert isinstance(result, ProjectOutput)
        assert (tmp_path / "rivet.yaml").exists()
        assert (tmp_path / "profiles.yaml").exists()
        assert len(result.declarations) == 2
        assert result.output_dir == tmp_path

    def test_creates_directory_structure(self, tmp_path: Path):
        result = generate_project(_make_bridge_result(), output_dir=tmp_path)

        # Source in sources/, sink in sinks/
        source_files = [d for d in result.declarations if d.relative_path.startswith("sources/")]
        sink_files = [d for d in result.declarations if d.relative_path.startswith("sinks/")]
        assert len(source_files) == 1
        assert len(sink_files) == 1
        assert (tmp_path / source_files[0].relative_path).exists()
        assert (tmp_path / sink_files[0].relative_path).exists()


class TestGenerateProjectNonEmptyDir:
    """Test generate_project with non-empty dir."""

    def test_overwrite_false_raises_brg_401(self, tmp_path: Path):
        (tmp_path / "existing.txt").write_text("existing")

        with pytest.raises(BridgeValidationError) as exc_info:
            generate_project(_make_bridge_result(), output_dir=tmp_path, overwrite=False)

        assert len(exc_info.value.errors) == 1
        assert exc_info.value.errors[0].code == "BRG-401"

    def test_overwrite_true_writes_files(self, tmp_path: Path):
        (tmp_path / "existing.txt").write_text("existing")

        result = generate_project(_make_bridge_result(), output_dir=tmp_path, overwrite=True)

        assert isinstance(result, ProjectOutput)
        assert (tmp_path / "rivet.yaml").exists()


class TestGenerateProjectRivetYaml:
    """Test generated rivet.yaml contains correct directory references."""

    def test_manifest_content(self, tmp_path: Path):
        result = generate_project(_make_bridge_result(), output_dir=tmp_path)

        content = result.rivet_yaml.content
        assert "sources/" in content
        assert "joints/" in content
        assert "sinks/" in content
        assert "quality/" in content


class TestGenerateProjectQualityFiles:
    """Test quality files placed in quality/ directory."""

    def test_quality_files_in_quality_dir(self, tmp_path: Path):
        joints = [
            Joint(
                name="raw_users",
                joint_type="source",
                sql="SELECT * FROM raw_users",
                assertions=[
                    pytest.importorskip("rivet_core.checks").Assertion(
                        type="not_null",
                        severity="error",
                        config={"column": "id"},
                    ),
                ],
            ),
            Joint(name="output_users", joint_type="sink", upstream=["raw_users"], sql="SELECT * FROM raw_users"),
        ]
        result = generate_project(_make_bridge_result(joints), output_dir=tmp_path)

        assert len(result.quality_files) == 1
        assert result.quality_files[0].relative_path == "quality/raw_users.yaml"
        assert (tmp_path / "quality" / "raw_users.yaml").exists()

    def test_no_quality_files_when_no_assertions(self, tmp_path: Path):
        result = generate_project(_make_bridge_result(), output_dir=tmp_path)

        assert len(result.quality_files) == 0


class TestGenerateProjectSQLFormat:
    """Test generate_project with SQL format as caller default."""

    def test_sql_format_used_when_no_source_format(self, tmp_path: Path):
        """When source_formats is empty, caller default format is used."""
        br = _make_bridge_result()
        # Override source_formats to empty so caller default kicks in
        br = BridgeResult(
            assembly=br.assembly,
            catalogs=br.catalogs,
            engines=br.engines,
            profile_snapshot=br.profile_snapshot,
            source_formats={},
        )
        result = generate_project(br, format="sql", output_dir=tmp_path)

        for decl in result.declarations:
            assert decl.relative_path.endswith(".sql")
            content = (tmp_path / decl.relative_path).read_text()
            assert "-- rivet:" in content
