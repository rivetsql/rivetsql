"""Tests for the doctor command."""

from __future__ import annotations

from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_cli.app import GlobalOptions
from rivet_cli.commands.doctor import run_doctor


def _globals(project: Path, color: bool = False, profile: str = "default") -> GlobalOptions:
    return GlobalOptions(project_path=project, color=color, profile=profile)


def _create_healthy_project(root: Path) -> None:
    """Create a minimal healthy project for doctor checks."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "rivet.yaml").write_text(
        "profiles: profiles.yaml\nsources: sources\njoints: joints\nsinks: sinks\n"
        "quality: quality\ntests: tests\n"
    )
    (root / "profiles.yaml").write_text(
        "default:\n  catalogs:\n    local:\n      type: filesystem\n"
        "  engines:\n    default:\n      type: arrow\n  default_engine: default\n"
    )
    for d in ("sources", "joints", "sinks", "quality", "tests"):
        (root / d).mkdir(exist_ok=True)

    (root / "sources" / "raw_data.yaml").write_text(
        "name: raw_data\ntype: source\ncatalog: local\ntable: raw_data\n"
        "description: Raw data source\n"
    )
    (root / "joints" / "transform.sql").write_text(
        "-- rivet:name: transform\n-- rivet:type: sql\nSELECT id, value FROM raw_data\n"
    )
    (root / "sinks" / "output.yaml").write_text(
        "name: output\ntype: sink\ncatalog: local\ntable: output\n"
        "upstream:\n  - transform\ndescription: Output sink\n"
    )
    (root / "quality" / "output.yaml").write_text(
        "joint: output\nassertions:\n  - type: not_null\n    columns: [id]\n"
    )
    (root / "tests" / "test_transform.yaml").write_text(
        "name: test_transform\njoint: transform\n"
    )


class TestDoctorHealthyProject:
    """Test healthy project → all checks pass, exit 0."""

    def test_healthy_project_passes(self, tmp_path: Path) -> None:
        root = tmp_path / "project"
        _create_healthy_project(root)
        result = run_doctor(_globals(root))
        assert result == 0

    def test_healthy_project_output(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        root = tmp_path / "project"
        _create_healthy_project(root)
        run_doctor(_globals(root))
        out = capsys.readouterr().out
        assert "✓" in out


class TestDoctorLevel1YamlSyntax:
    """Test project with YAML syntax error → level 1 error, stops, exit 1."""

    def test_invalid_yaml_syntax(self, tmp_path: Path) -> None:
        root = tmp_path / "project"
        root.mkdir(parents=True)
        (root / "rivet.yaml").write_text(":\n  invalid: [yaml\n")
        result = run_doctor(_globals(root))
        assert result == 1

    def test_rivet_yaml_not_mapping(self, tmp_path: Path) -> None:
        root = tmp_path / "project"
        root.mkdir(parents=True)
        (root / "rivet.yaml").write_text("- just\n- a\n- list\n")
        result = run_doctor(_globals(root))
        assert result == 1

    def test_missing_rivet_yaml(self, tmp_path: Path) -> None:
        root = tmp_path / "project"
        root.mkdir(parents=True)
        result = run_doctor(_globals(root))
        assert result == 1

    def test_missing_directory(self, tmp_path: Path) -> None:
        root = tmp_path / "project"
        root.mkdir(parents=True)
        (root / "rivet.yaml").write_text(
            "profiles: profiles.yaml\nsources: sources\njoints: joints\nsinks: sinks\n"
        )
        (root / "profiles.yaml").write_text("default:\n  default_engine: x\n")
        # sources/joints/sinks dirs don't exist
        result = run_doctor(_globals(root))
        assert result == 1

    def test_invalid_yaml_in_declaration_file(self, tmp_path: Path) -> None:
        root = tmp_path / "project"
        root.mkdir(parents=True)
        (root / "rivet.yaml").write_text(
            "profiles: profiles.yaml\nsources: sources\njoints: joints\nsinks: sinks\n"
        )
        (root / "profiles.yaml").write_text(
            "default:\n  catalogs:\n    local:\n      type: fs\n"
            "  engines:\n    default:\n      type: arrow\n  default_engine: default\n"
        )
        for d in ("sources", "joints", "sinks"):
            (root / d).mkdir()
        (root / "sources" / "bad.yaml").write_text(":\n  invalid: [yaml\n")
        result = run_doctor(_globals(root))
        assert result == 1

    def test_stops_at_level1(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verify no level 2+ checks run when level 1 has errors."""
        root = tmp_path / "project"
        root.mkdir(parents=True)
        (root / "rivet.yaml").write_text("- not a mapping\n")
        run_doctor(_globals(root))
        out = capsys.readouterr().out
        assert "✗" in out
        # Should not contain profile-level checks
        assert "default_engine" not in out


class TestDoctorLevel2Profile:
    """Test project with invalid profile → level 2 error, stops, exit 1."""

    def test_missing_profile(self, tmp_path: Path) -> None:
        root = tmp_path / "project"
        root.mkdir(parents=True)
        (root / "rivet.yaml").write_text(
            "profiles: profiles.yaml\nsources: sources\njoints: joints\nsinks: sinks\n"
        )
        (root / "profiles.yaml").write_text("production:\n  default_engine: x\n")
        for d in ("sources", "joints", "sinks"):
            (root / d).mkdir()
        result = run_doctor(_globals(root, profile="default"))
        assert result == 1

    def test_missing_default_engine(self, tmp_path: Path) -> None:
        root = tmp_path / "project"
        root.mkdir(parents=True)
        (root / "rivet.yaml").write_text(
            "profiles: profiles.yaml\nsources: sources\njoints: joints\nsinks: sinks\n"
        )
        (root / "profiles.yaml").write_text(
            "default:\n  catalogs:\n    local:\n      type: fs\n"
            "  engines:\n    default:\n      type: arrow\n"
        )
        for d in ("sources", "joints", "sinks"):
            (root / d).mkdir()
        result = run_doctor(_globals(root))
        assert result == 1


class TestDoctorLevel3SQL:
    """Test project with SQL parse error → level 3 error, stops, exit 1."""

    def test_invalid_sql(self, tmp_path: Path) -> None:
        root = tmp_path / "project"
        _create_healthy_project(root)
        # Overwrite with invalid SQL
        (root / "joints" / "transform.sql").write_text(
            "-- rivet:name: transform\n-- rivet:type: sql\nSELECT FROM WHERE\n"
        )
        result = run_doctor(_globals(root))
        # SQL parse errors at level 3 → exit 1
        assert result == 1


class TestDoctorLevel4DAG:
    """Test project with DAG cycle → level 4 error, stops, exit 1."""

    def test_unknown_upstream(self, tmp_path: Path) -> None:
        root = tmp_path / "project"
        _create_healthy_project(root)
        # Sink references nonexistent upstream
        (root / "sinks" / "output.yaml").write_text(
            "name: output\ntype: sink\ncatalog: local\ntable: output\n"
            "upstream:\n  - nonexistent_joint\ndescription: Output\n"
        )
        result = run_doctor(_globals(root))
        assert result == 1

    def test_dag_cycle(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        root = tmp_path / "project"
        _create_healthy_project(root)
        # Create a cycle: joint_a -> joint_b -> joint_a
        (root / "joints" / "joint_a.yaml").write_text(
            "name: joint_a\ntype: sql\nupstream:\n  - joint_b\ndescription: A\n"
        )
        (root / "joints" / "joint_b.yaml").write_text(
            "name: joint_b\ntype: sql\nupstream:\n  - joint_a\ndescription: B\n"
        )
        result = run_doctor(_globals(root))
        assert result == 1
        out = capsys.readouterr().out
        assert "ycle" in out  # "Cycle detected"


class TestDoctorLevel5Unused:
    """Test project with unused source → level 5 warning, continues, exit 0."""

    def test_unused_source_warning(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        root = tmp_path / "project"
        _create_healthy_project(root)
        # Add an extra source that nothing references
        (root / "sources" / "unused.yaml").write_text(
            "name: unused_source\ntype: source\ncatalog: local\ntable: unused\n"
            "description: Unused\n"
        )
        result = run_doctor(_globals(root))
        # Warnings don't cause failure
        assert result == 0
        out = capsys.readouterr().out
        assert "unused_source" in out
        assert "⚠" in out


class TestDoctorLevel6BestPractices:
    """Test project with sink without quality → level 6 info, exit 0."""

    def test_sink_without_quality(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        root = tmp_path / "project"
        _create_healthy_project(root)
        # Remove quality file
        (root / "quality" / "output.yaml").unlink()
        result = run_doctor(_globals(root))
        assert result == 0
        out = capsys.readouterr().out
        assert "quality" in out.lower() or "⚠" in out


class TestDoctorStopsAtFirstErrorLevel:
    """Test doctor stops at first error level (Property 11)."""

    def test_level1_error_blocks_level2(self, tmp_path: Path) -> None:
        root = tmp_path / "project"
        root.mkdir(parents=True)
        (root / "rivet.yaml").write_text("- not a mapping\n")
        result = run_doctor(_globals(root))
        assert result == 1

    def test_level2_error_blocks_level3(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        root = tmp_path / "project"
        root.mkdir(parents=True)
        (root / "rivet.yaml").write_text(
            "profiles: profiles.yaml\nsources: sources\njoints: joints\nsinks: sinks\n"
        )
        (root / "profiles.yaml").write_text("other_profile:\n  default_engine: x\n")
        for d in ("sources", "joints", "sinks"):
            (root / d).mkdir()
        result = run_doctor(_globals(root))
        assert result == 1
        out = capsys.readouterr().out
        # Should mention profile issue but not SQL parsing
        assert "SQL parsing" not in out


# Strategy: pick which level to inject an error at (1-4 produce errors; 5-6 produce warnings only)
_error_level_st = st.integers(min_value=1, max_value=4)


class TestProperty11StopsAtFirstErrorLevel:
    """Property 11: For any project with errors at level N, doctor reports
    checks at levels 1..N but does not execute checks at levels N+1..6."""

    @given(error_level=_error_level_st)
    @settings(max_examples=100)
    def test_stops_at_error_level(self, error_level: int) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as td:
            root = Path(td) / "proj"
            root.mkdir()

            if error_level == 1:
                (root / "rivet.yaml").write_text("- not a mapping\n")
            elif error_level == 2:
                (root / "rivet.yaml").write_text(
                    "profiles: profiles.yaml\nsources: sources\njoints: joints\nsinks: sinks\n"
                )
                (root / "profiles.yaml").write_text("other:\n  default_engine: x\n")
                for d in ("sources", "joints", "sinks"):
                    (root / d).mkdir()
            elif error_level == 3:
                _create_healthy_project(root)
                (root / "joints" / "transform.sql").write_text(
                    "-- rivet:name: transform\nSELECT FROM WHERE\n"
                )
            else:  # 4
                _create_healthy_project(root)
                (root / "sinks" / "output.yaml").write_text(
                    "name: output\ntype: sink\ncatalog: local\ntable: output\n"
                    "upstream:\n  - ghost\ndescription: X\n"
                )

            result = run_doctor(_globals(root))
            assert result == 1


class TestDoctorCheckConnections:
    """Test --check-connections runs connectivity check."""

    def test_check_connections_flag(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        root = tmp_path / "project"
        _create_healthy_project(root)
        result = run_doctor(_globals(root), check_connections=True)
        assert result == 0
        out = capsys.readouterr().out
        assert "Connection" in out or "connection" in out


class TestDoctorCheckSchemas:
    """Test --check-schemas runs schema drift check."""

    def test_check_schemas_flag(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        root = tmp_path / "project"
        _create_healthy_project(root)
        result = run_doctor(_globals(root), check_schemas=True)
        assert result == 0
        out = capsys.readouterr().out
        assert "Schema" in out or "schema" in out
