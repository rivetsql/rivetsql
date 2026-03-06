"""Tests for the init command."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from rivet_cli.app import GlobalOptions
from rivet_cli.commands.init import VALID_STYLES, run_init
from rivet_cli.exit_codes import GENERAL_ERROR, SUCCESS


def _globals(color: bool = False) -> GlobalOptions:
    return GlobalOptions(color=color)


class TestInitMixedStyle:
    """Mixed style (default): sources/sinks as YAML, joints as SQL."""

    def test_creates_rivet_yaml(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        result = run_init(str(target), bare=False, style="mixed", globals=_globals())
        assert result == SUCCESS
        assert (target / "rivet.yaml").exists()

    def test_creates_profiles_yaml(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=False, style="mixed", globals=_globals())
        assert (target / "profiles.yaml").exists()

    def test_creates_directories(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=False, style="mixed", globals=_globals())
        for d in ("sources", "joints", "sinks", "tests", "quality", "data"):
            assert (target / d).is_dir()

    def test_source_is_yaml(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=False, style="mixed", globals=_globals())
        assert (target / "sources" / "raw_orders.yaml").exists()
        assert not (target / "sources" / "raw_orders.sql").exists()

    def test_joint_is_sql(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=False, style="mixed", globals=_globals())
        assert (target / "joints" / "transform_orders.sql").exists()
        assert not (target / "joints" / "transform_orders.yaml").exists()

    def test_sink_is_yaml(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=False, style="mixed", globals=_globals())
        assert (target / "sinks" / "orders_clean.yaml").exists()
        assert not (target / "sinks" / "orders_clean.sql").exists()

    def test_sample_data_created(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=False, style="mixed", globals=_globals())
        csv = target / "data" / "raw_orders.csv"
        assert csv.exists()
        assert "Alice" in csv.read_text()

    def test_source_yaml_is_valid(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=False, style="mixed", globals=_globals())
        data = yaml.safe_load((target / "sources" / "raw_orders.yaml").read_text())
        assert data["name"] == "raw_orders"
        assert data["type"] == "source"

    def test_sink_yaml_is_valid(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=False, style="mixed", globals=_globals())
        data = yaml.safe_load((target / "sinks" / "orders_clean.yaml").read_text())
        assert data["name"] == "orders_clean"
        assert data["type"] == "sink"


class TestInitSQLStyle:
    """SQL style: sources, joints, and sinks all as .sql files."""

    def test_source_is_sql(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=False, style="sql", globals=_globals())
        assert (target / "sources" / "raw_orders.sql").exists()
        assert not (target / "sources" / "raw_orders.yaml").exists()

    def test_joint_is_sql(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=False, style="sql", globals=_globals())
        assert (target / "joints" / "transform_orders.sql").exists()

    def test_sink_is_sql(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=False, style="sql", globals=_globals())
        assert (target / "sinks" / "orders_clean.sql").exists()
        assert not (target / "sinks" / "orders_clean.yaml").exists()

    def test_sql_source_has_annotations(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=False, style="sql", globals=_globals())
        content = (target / "sources" / "raw_orders.sql").read_text()
        assert "-- rivet:name: raw_orders" in content
        assert "-- rivet:type: source" in content

    def test_sql_sink_has_annotations(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=False, style="sql", globals=_globals())
        content = (target / "sinks" / "orders_clean.sql").read_text()
        assert "-- rivet:name: orders_clean" in content
        assert "-- rivet:type: sink" in content

    def test_returns_success(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        result = run_init(str(target), bare=False, style="sql", globals=_globals())
        assert result == SUCCESS


class TestInitYAMLStyle:
    """YAML style: sources, joints, and sinks all as .yaml files."""

    def test_source_is_yaml(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=False, style="yaml", globals=_globals())
        assert (target / "sources" / "raw_orders.yaml").exists()

    def test_joint_is_yaml(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=False, style="yaml", globals=_globals())
        assert (target / "joints" / "transform_orders.yaml").exists()
        assert not (target / "joints" / "transform_orders.sql").exists()

    def test_sink_is_yaml(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=False, style="yaml", globals=_globals())
        assert (target / "sinks" / "orders_clean.yaml").exists()

    def test_joint_yaml_is_valid(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=False, style="yaml", globals=_globals())
        data = yaml.safe_load((target / "joints" / "transform_orders.yaml").read_text())
        assert data["name"] == "transform_orders"
        assert data["type"] == "sql"
        assert "sql" in data

    def test_returns_success(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        result = run_init(str(target), bare=False, style="yaml", globals=_globals())
        assert result == SUCCESS


class TestInitBare:
    """Bare mode creates structure only, no example files."""

    def test_bare_creates_directories(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        result = run_init(str(target), bare=True, style="mixed", globals=_globals())
        assert result == SUCCESS
        for d in ("sources", "joints", "sinks", "tests", "quality", "data"):
            assert (target / d).is_dir()

    def test_bare_creates_rivet_yaml(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=True, style="mixed", globals=_globals())
        assert (target / "rivet.yaml").exists()

    def test_bare_no_example_files(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=True, style="mixed", globals=_globals())
        assert not (target / "sources" / "raw_orders.yaml").exists()
        assert not (target / "joints" / "transform_orders.sql").exists()
        assert not (target / "sinks" / "orders_clean.yaml").exists()
        assert not (target / "data" / "raw_orders.csv").exists()


class TestInitErrors:
    """Error handling: existing project, invalid style."""

    def test_existing_project_rejected(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        target.mkdir()
        (target / "rivet.yaml").write_text("profiles: profiles.yaml\n")
        result = run_init(str(target), bare=False, style="mixed", globals=_globals())
        assert result == GENERAL_ERROR

    def test_existing_project_error_message(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        target = tmp_path / "proj"
        target.mkdir()
        (target / "rivet.yaml").write_text("profiles: profiles.yaml\n")
        run_init(str(target), bare=False, style="mixed", globals=_globals())
        err = capsys.readouterr().err
        assert "RVT-857" in err
        assert "not empty" in err

    def test_non_empty_directory_rejected(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        target.mkdir()
        (target / "some_file.txt").write_text("hello")
        result = run_init(str(target), bare=False, style="mixed", globals=_globals())
        assert result == GENERAL_ERROR

    def test_non_empty_directory_error_message(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        target = tmp_path / "proj"
        target.mkdir()
        (target / "some_file.txt").write_text("hello")
        run_init(str(target), bare=False, style="mixed", globals=_globals())
        err = capsys.readouterr().err
        assert "RVT-857" in err
        assert "not empty" in err

    def test_invalid_style_rejected(self, tmp_path: Path) -> None:
        target = tmp_path / "proj"
        result = run_init(str(target), bare=False, style="invalid", globals=_globals())
        assert result == GENERAL_ERROR

    def test_invalid_style_error_message(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=False, style="invalid", globals=_globals())
        err = capsys.readouterr().err
        assert "RVT-855" in err
        assert "invalid" in err


class TestInitDefaultDirectory:
    """When directory is None, init uses current directory."""

    def test_none_directory_uses_cwd(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        result = run_init(None, bare=False, style="mixed", globals=_globals())
        assert result == SUCCESS
        assert (tmp_path / "rivet.yaml").exists()


class TestInitOutput:
    """Verify CLI output messages."""

    def test_success_output(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        target = tmp_path / "proj"
        run_init(str(target), bare=False, style="mixed", globals=_globals())
        out = capsys.readouterr().out
        assert "Project initialized" in out
        assert "rivet compile" in out


class TestInitYAMLValidity:
    """All generated YAML files must be parseable."""

    @pytest.mark.parametrize("style", VALID_STYLES)
    def test_rivet_yaml_valid(self, tmp_path: Path, style: str) -> None:
        target = tmp_path / style
        run_init(str(target), bare=False, style=style, globals=_globals())
        data = yaml.safe_load((target / "rivet.yaml").read_text())
        assert isinstance(data, dict)
        assert "profiles" in data

    @pytest.mark.parametrize("style", VALID_STYLES)
    def test_profiles_yaml_valid(self, tmp_path: Path, style: str) -> None:
        target = tmp_path / style
        run_init(str(target), bare=False, style=style, globals=_globals())
        data = yaml.safe_load((target / "profiles.yaml").read_text())
        assert isinstance(data, dict)
        assert "default" in data

    @pytest.mark.parametrize("style", VALID_STYLES)
    def test_quality_yaml_valid(self, tmp_path: Path, style: str) -> None:
        target = tmp_path / style
        run_init(str(target), bare=False, style=style, globals=_globals())
        data = yaml.safe_load((target / "quality" / "orders_clean.yaml").read_text())
        assert "assertions" in data
