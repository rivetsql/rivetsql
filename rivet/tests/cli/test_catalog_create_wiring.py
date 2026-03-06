"""Unit tests for CLI wiring and error handling of catalog create command.

Task 8.5: CLI wiring and error handling tests.
Requirements: 1.5, 1.6, 14.1, 14.2, 14.3, 14.4, 14.5, 15.1, 15.2, 15.3, 15.4, 15.5, 15.6
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from rivet_cli import main
from rivet_cli.app import build_parser

# ── Stubs ─────────────────────────────────────────────────────────────────────


class StubPlugin:
    type = "stubdb"
    required_options: list[str] = []
    optional_options: dict[str, Any] = {}
    credential_options: list[str] = []

    def validate(self, options: dict[str, Any]) -> None:
        pass

    def instantiate(self, name: str, options: dict[str, Any]) -> Any:
        return MagicMock()

    def list_tables(self, catalog: Any) -> list[str]:
        return []


@dataclass
class FakeProfile:
    name: str = "default"
    catalogs: dict = field(default_factory=dict)
    engines: list = field(default_factory=list)


@dataclass
class FakeConfigResult:
    profile: FakeProfile | None = None
    manifest: Any = None
    success: bool = True
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)


def _make_registry(plugins: list[Any] | None = None) -> MagicMock:
    plugins = plugins or [StubPlugin()]
    reg = MagicMock()
    reg._catalog_plugins = {p.type: p for p in plugins}
    reg.get_catalog_plugin = lambda t: reg._catalog_plugins.get(t)
    reg.register_builtins = MagicMock()
    reg.discover_plugins = MagicMock()
    return reg


# ── Parser wiring: catalog create subcommand ──────────────────────────────────


class TestCatalogCreateParser:
    """Req 1.1, 1.2, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7"""

    def test_catalog_create_subcommand_recognized(self):
        parser = build_parser()
        args = parser.parse_args(["catalog", "create"])
        assert args.command == "catalog"
        assert args.catalog_action == "create"

    def test_catalog_create_type_flag(self):
        parser = build_parser()
        args = parser.parse_args(["catalog", "create", "--type", "postgres"])
        assert args.catalog_type == "postgres"

    def test_catalog_create_name_flag(self):
        parser = build_parser()
        args = parser.parse_args(["catalog", "create", "--name", "my_pg"])
        assert args.catalog_name == "my_pg"

    def test_catalog_create_option_flag_repeatable(self):
        parser = build_parser()
        args = parser.parse_args(["catalog", "create", "--option", "host=localhost", "--option", "port=5432"])
        assert args.option == ["host=localhost", "port=5432"]

    def test_catalog_create_credential_flag_repeatable(self):
        parser = build_parser()
        args = parser.parse_args(["catalog", "create", "--credential", "password=secret"])
        assert args.credential == ["password=secret"]

    def test_catalog_create_no_test_flag(self):
        parser = build_parser()
        args = parser.parse_args(["catalog", "create", "--no-test"])
        assert args.no_test is True

    def test_catalog_create_dry_run_flag(self):
        parser = build_parser()
        args = parser.parse_args(["catalog", "create", "--dry-run"])
        assert args.dry_run is True

    def test_catalog_create_global_options_inherited(self):
        parser = build_parser()
        args = parser.parse_args(["catalog", "create", "--profile", "prod", "--no-color"])
        assert args.profile == "prod"
        assert args.no_color is True

    def test_catalog_create_defaults(self):
        parser = build_parser()
        args = parser.parse_args(["catalog", "create"])
        assert args.catalog_type is None
        assert args.catalog_name is None
        assert args.option == []
        assert args.credential == []
        assert args.no_test is False
        assert args.dry_run is False


# ── Dispatch: catalog create invokes run_catalog_create ──────────────────────


class TestCatalogCreateDispatch:
    """Req 1.1: rivet catalog create invokes wizard."""

    def test_dispatch_calls_run_catalog_create(self, tmp_path):
        """catalog create dispatches to run_catalog_create."""
        (tmp_path / "rivet.yaml").write_text("sources: ./sources\n")

        with patch("rivet_cli.commands.catalog_create.run_catalog_create", return_value=0) as mock_run:
            code = main(["--project", str(tmp_path), "catalog", "create",
                         "--type", "stubdb", "--name", "my_cat",
                         "--no-test", "--dry-run"])
        mock_run.assert_called_once()
        assert code == 0

    def test_dispatch_passes_flags_to_run_catalog_create(self, tmp_path):
        """Flags are forwarded correctly to run_catalog_create."""
        (tmp_path / "rivet.yaml").write_text("sources: ./sources\n")

        with patch("rivet_cli.commands.catalog_create.run_catalog_create", return_value=0) as mock_run:
            main(["--project", str(tmp_path), "catalog", "create",
                  "--type", "postgres", "--name", "my_pg",
                  "--option", "host=localhost",
                  "--credential", "password=secret",
                  "--no-test", "--dry-run"])

        call_kwargs = mock_run.call_args.kwargs
        assert call_kwargs["catalog_type"] == "postgres"
        assert call_kwargs["catalog_name"] == "my_pg"
        assert call_kwargs["options"] == ["host=localhost"]
        assert call_kwargs["credentials"] == ["password=secret"]
        assert call_kwargs["no_test"] is True
        assert call_kwargs["dry_run"] is True


# ── Exit codes ────────────────────────────────────────────────────────────────


class TestExitCodes:
    """Req 14.1, 14.2, 14.3, 14.4, 14.5"""

    def test_exit_0_on_success(self, tmp_path):
        """Req 14.1: successful wizard exits 0."""
        (tmp_path / "rivet.yaml").write_text("sources: ./sources\n")
        with patch("rivet_cli.commands.catalog_create.run_catalog_create", return_value=0):
            code = main(["--project", str(tmp_path), "catalog", "create",
                         "--type", "stubdb", "--name", "my_cat", "--no-test", "--dry-run"])
        assert code == 0

    def test_exit_1_on_general_error(self, tmp_path):
        """Req 14.3: general error exits 1."""
        (tmp_path / "rivet.yaml").write_text("sources: ./sources\n")
        with patch("rivet_cli.commands.catalog_create.run_catalog_create", return_value=1):
            code = main(["--project", str(tmp_path), "catalog", "create",
                         "--type", "stubdb", "--name", "my_cat", "--no-test"])
        assert code == 1

    def test_exit_10_on_usage_error(self, tmp_path):
        """Req 14.4: usage error exits 10."""
        (tmp_path / "rivet.yaml").write_text("sources: ./sources\n")
        with patch("rivet_cli.commands.catalog_create.run_catalog_create", return_value=10):
            code = main(["--project", str(tmp_path), "catalog", "create",
                         "--type", "stubdb", "--name", "my_cat", "--no-test"])
        assert code == 10

    def test_exit_130_on_interrupt(self, tmp_path):
        """Req 14.2: KeyboardInterrupt exits 130."""
        (tmp_path / "rivet.yaml").write_text("sources: ./sources\n")
        with patch("rivet_cli.commands.catalog_create.run_catalog_create",
                   side_effect=KeyboardInterrupt):
            code = main(["--project", str(tmp_path), "catalog", "create",
                         "--type", "stubdb", "--name", "my_cat", "--no-test"])
        assert code == 130

    def test_exit_0_on_dry_run(self, tmp_path):
        """Req 14.5: --dry-run exits 0."""
        (tmp_path / "rivet.yaml").write_text("sources: ./sources\n")
        with patch("rivet_cli.commands.catalog_create.run_catalog_create", return_value=0):
            code = main(["--project", str(tmp_path), "catalog", "create",
                         "--type", "stubdb", "--name", "my_cat", "--no-test", "--dry-run"])
        assert code == 0


# ── RVT-850: no rivet.yaml ────────────────────────────────────────────────────


class TestRVT850NoRivetYaml:
    """Req 1.6, 13.1: RVT-850 when rivet.yaml is absent."""

    def test_rvt850_when_no_rivet_yaml(self, tmp_path, capsys):
        """No rivet.yaml → RVT-850, exit 10."""
        reg = _make_registry()
        config = FakeConfigResult(profile=FakeProfile())

        with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
             patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
             patch("rivet_cli.commands.catalog_create.load_config", return_value=config):
            from rivet_cli.app import GlobalOptions
            from rivet_cli.commands.catalog_create import run_catalog_create

            code = run_catalog_create(
                catalog_type="stubdb",
                catalog_name="my_cat",
                options=[],
                credentials=[],
                no_test=True,
                dry_run=True,
                globals=GlobalOptions(project_path=tmp_path),
            )

        assert code == 10
        err = capsys.readouterr().err
        assert "RVT-850" in err

    def test_rvt850_message_has_remediation(self, tmp_path, capsys):
        """RVT-850 error includes remediation text."""
        reg = _make_registry()
        config = FakeConfigResult(profile=FakeProfile())

        with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
             patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
             patch("rivet_cli.commands.catalog_create.load_config", return_value=config):
            from rivet_cli.app import GlobalOptions
            from rivet_cli.commands.catalog_create import run_catalog_create

            run_catalog_create(
                catalog_type="stubdb",
                catalog_name="my_cat",
                options=[],
                credentials=[],
                no_test=True,
                dry_run=True,
                globals=GlobalOptions(project_path=tmp_path),
            )

        err = capsys.readouterr().err
        assert "rivet init" in err or "init" in err


# ── RVT-880: profile not found ────────────────────────────────────────────────


class TestRVT880ProfileNotFound:
    """Req 1.5, 13.1: RVT-880 when profile does not exist."""

    def test_rvt880_when_profile_not_found(self, tmp_path, capsys):
        """Profile not found → RVT-880, exit 10."""
        (tmp_path / "rivet.yaml").write_text("sources: ./sources\n")
        reg = _make_registry()
        config = FakeConfigResult(profile=None)  # profile not found

        with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
             patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
             patch("rivet_cli.commands.catalog_create.load_config", return_value=config):
            from rivet_cli.app import GlobalOptions
            from rivet_cli.commands.catalog_create import run_catalog_create

            code = run_catalog_create(
                catalog_type="stubdb",
                catalog_name="my_cat",
                options=[],
                credentials=[],
                no_test=True,
                dry_run=True,
                globals=GlobalOptions(project_path=tmp_path, profile="nonexistent"),
            )

        assert code == 10
        err = capsys.readouterr().err
        assert "RVT-880" in err

    def test_rvt880_includes_profile_name(self, tmp_path, capsys):
        """RVT-880 error message includes the missing profile name."""
        (tmp_path / "rivet.yaml").write_text("sources: ./sources\n")
        reg = _make_registry()
        config = FakeConfigResult(profile=None)

        with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
             patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
             patch("rivet_cli.commands.catalog_create.load_config", return_value=config):
            from rivet_cli.app import GlobalOptions
            from rivet_cli.commands.catalog_create import run_catalog_create

            run_catalog_create(
                catalog_type="stubdb",
                catalog_name="my_cat",
                options=[],
                credentials=[],
                no_test=True,
                dry_run=True,
                globals=GlobalOptions(project_path=tmp_path, profile="missing_profile"),
            )

        err = capsys.readouterr().err
        assert "missing_profile" in err

    def test_rvt880_has_remediation(self, tmp_path, capsys):
        """RVT-880 error includes remediation text."""
        (tmp_path / "rivet.yaml").write_text("sources: ./sources\n")
        reg = _make_registry()
        config = FakeConfigResult(profile=None)

        with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
             patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
             patch("rivet_cli.commands.catalog_create.load_config", return_value=config):
            from rivet_cli.app import GlobalOptions
            from rivet_cli.commands.catalog_create import run_catalog_create

            run_catalog_create(
                catalog_type="stubdb",
                catalog_name="my_cat",
                options=[],
                credentials=[],
                no_test=True,
                dry_run=True,
                globals=GlobalOptions(project_path=tmp_path, profile="missing_profile"),
            )

        err = capsys.readouterr().err
        # Should have a remediation arrow
        assert "→" in err or "--profile" in err


# ── Module boundary compliance ────────────────────────────────────────────────


class TestModuleBoundaries:
    """Req 15.1–15.6: catalog_create.py must not import plugin packages directly."""

    def test_catalog_create_does_not_import_plugin_packages(self):
        """catalog_create.py must not import rivet_duckdb, rivet_postgres, etc."""
        source_path = Path(__file__).parent.parent.parent / "src" / "rivet_cli" / "commands" / "catalog_create.py"
        source = source_path.read_text()
        tree = ast.parse(source)

        forbidden_prefixes = ("rivet_duckdb", "rivet_postgres", "rivet_aws",
                              "rivet_databricks", "rivet_pyspark", "rivet_polars")

        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                if isinstance(node, ast.Import):
                    names = [alias.name for alias in node.names]
                else:
                    names = [node.module] if node.module else []
                for name in names:
                    for prefix in forbidden_prefixes:
                        assert not (name or "").startswith(prefix), (
                            f"catalog_create.py must not import '{name}' (plugin boundary violation)"
                        )

    def test_catalog_create_only_imports_allowed_modules(self):
        """catalog_create.py imports only rivet_core, rivet_config, rivet_bridge, stdlib."""
        source_path = Path(__file__).parent.parent.parent / "src" / "rivet_cli" / "commands" / "catalog_create.py"
        source = source_path.read_text()
        tree = ast.parse(source)

        allowed_prefixes = (
            "rivet_core", "rivet_config", "rivet_bridge", "rivet_cli",
            # stdlib modules
            "concurrent", "getpass", "re", "sys", "time", "dataclasses",
            "pathlib", "typing", "yaml", "__future__",
        )

        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                top_level = node.module.split(".")[0]
                assert any(top_level == p or top_level.startswith(p) for p in allowed_prefixes), (
                    f"catalog_create.py imports '{node.module}' which is outside allowed boundaries"
                )

    def test_catalog_create_command_in_commands_directory(self):
        """Wizard lives in rivet_cli/commands/ (Req 15.1)."""
        source_path = Path(__file__).parent.parent.parent / "src" / "rivet_cli" / "commands" / "catalog_create.py"
        assert source_path.exists(), "catalog_create.py must exist in rivet_cli/commands/"

    def test_no_new_external_dependencies(self):
        """catalog_create.py uses only stdlib + existing rivet deps (no new packages)."""
        source_path = Path(__file__).parent.parent.parent / "src" / "rivet_cli" / "commands" / "catalog_create.py"
        source = source_path.read_text()
        tree = ast.parse(source)

        # These are the only allowed third-party imports
        allowed_third_party = {"yaml"}  # PyYAML is already a rivet_cli dep

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    top = alias.name.split(".")[0]
                    if not top.startswith("rivet") and top not in {
                        "concurrent", "getpass", "os", "re", "sys", "time",
                        "dataclasses", "pathlib", "typing", "__future__",
                    }:
                        assert top in allowed_third_party, (
                            f"Unexpected third-party import '{top}' in catalog_create.py"
                        )


# ── Error message completeness ────────────────────────────────────────────────


class TestErrorMessageCompleteness:
    """Req 13.8: all wizard errors have message and remediation."""

    def test_rvt850_has_message_and_remediation(self, tmp_path, capsys):
        reg = _make_registry()
        config = FakeConfigResult(profile=FakeProfile())

        with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
             patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
             patch("rivet_cli.commands.catalog_create.load_config", return_value=config):
            from rivet_cli.app import GlobalOptions
            from rivet_cli.commands.catalog_create import run_catalog_create

            run_catalog_create(
                catalog_type="stubdb", catalog_name="my_cat",
                options=[], credentials=[], no_test=True, dry_run=True,
                globals=GlobalOptions(project_path=tmp_path),
            )

        err = capsys.readouterr().err
        assert "RVT-850" in err
        assert "→" in err  # remediation arrow present

    def test_rvt880_has_message_and_remediation(self, tmp_path, capsys):
        (tmp_path / "rivet.yaml").write_text("sources: ./sources\n")
        reg = _make_registry()
        config = FakeConfigResult(profile=None)

        with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
             patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
             patch("rivet_cli.commands.catalog_create.load_config", return_value=config):
            from rivet_cli.app import GlobalOptions
            from rivet_cli.commands.catalog_create import run_catalog_create

            run_catalog_create(
                catalog_type="stubdb", catalog_name="my_cat",
                options=[], credentials=[], no_test=True, dry_run=True,
                globals=GlobalOptions(project_path=tmp_path, profile="missing"),
            )

        err = capsys.readouterr().err
        assert "RVT-880" in err
        assert "→" in err

    def test_rvt881_has_message_and_remediation(self, tmp_path, capsys):
        (tmp_path / "rivet.yaml").write_text("sources: ./sources\n")

        class RequiredPlugin(StubPlugin):
            type = "reqplugin"
            required_options = ["host"]

        reg = _make_registry([RequiredPlugin()])
        config = FakeConfigResult(profile=FakeProfile())

        with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
             patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
             patch("rivet_cli.commands.catalog_create.load_config", return_value=config):
            from rivet_cli.app import GlobalOptions
            from rivet_cli.commands.catalog_create import run_catalog_create

            run_catalog_create(
                catalog_type="reqplugin", catalog_name="my_cat",
                options=[], credentials=[], no_test=True, dry_run=True,
                globals=GlobalOptions(project_path=tmp_path),
            )

        err = capsys.readouterr().err
        assert "RVT-881" in err
        assert "→" in err

    def test_rvt882_has_message_and_remediation(self, tmp_path, capsys):
        (tmp_path / "rivet.yaml").write_text("sources: ./sources\n")
        # Use a registry mock that explicitly returns empty catalog plugins
        reg = MagicMock()
        reg._catalog_plugins = {}
        reg.register_builtins = MagicMock()
        reg.discover_plugins = MagicMock()
        config = FakeConfigResult(profile=FakeProfile())

        with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
             patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
             patch("rivet_cli.commands.catalog_create.load_config", return_value=config):
            from rivet_cli.app import GlobalOptions
            from rivet_cli.commands.catalog_create import run_catalog_create

            run_catalog_create(
                catalog_type=None, catalog_name=None,
                options=[], credentials=[], no_test=True, dry_run=True,
                globals=GlobalOptions(project_path=tmp_path),
            )

        err = capsys.readouterr().err
        assert "RVT-882" in err
        assert "→" in err
