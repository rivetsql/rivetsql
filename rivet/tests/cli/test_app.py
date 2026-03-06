"""Tests for argument parsing, global options, and CLI entry point."""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_cli import main
from rivet_cli.app import GlobalOptions, build_parser, resolve_globals

# ---------------------------------------------------------------------------
# GlobalOptions dataclass
# ---------------------------------------------------------------------------


class TestGlobalOptions:
    def test_defaults(self):
        opts = GlobalOptions()
        assert opts.profile == "default"
        assert opts.project_path == Path(".")
        assert opts.verbosity == 0
        assert opts.color is True

    def test_frozen(self):
        opts = GlobalOptions()
        with pytest.raises(AttributeError):
            opts.profile = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# resolve_globals — helpers
# ---------------------------------------------------------------------------


def _ns(**kwargs) -> argparse.Namespace:
    """Build a Namespace with resolve_globals-expected attrs defaulted."""
    defaults = dict(profile=None, project=None, verbose=0, quiet=False, no_color=False)
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ---------------------------------------------------------------------------
# resolve_globals — defaults
# ---------------------------------------------------------------------------


class TestResolveGlobalsDefaults:
    def test_all_defaults(self, monkeypatch):
        monkeypatch.delenv("RIVET_PROFILE", raising=False)
        monkeypatch.delenv("RIVET_PROJECT", raising=False)
        monkeypatch.delenv("RIVET_NO_COLOR", raising=False)
        opts = resolve_globals(_ns())
        assert opts.profile == "default"
        assert opts.project_path == Path(".")
        assert opts.verbosity == 0
        assert opts.color is True


# ---------------------------------------------------------------------------
# resolve_globals — flag > env > default (Property 1)
# ---------------------------------------------------------------------------


class TestResolveGlobalsPriority:
    def test_profile_flag_overrides_env(self, monkeypatch):
        monkeypatch.setenv("RIVET_PROFILE", "env_profile")
        opts = resolve_globals(_ns(profile="flag_profile"))
        assert opts.profile == "flag_profile"

    def test_profile_env_used_when_no_flag(self, monkeypatch):
        monkeypatch.setenv("RIVET_PROFILE", "env_profile")
        opts = resolve_globals(_ns(profile=None))
        assert opts.profile == "env_profile"

    def test_project_flag_overrides_env(self, monkeypatch):
        monkeypatch.setenv("RIVET_PROJECT", "/env/path")
        opts = resolve_globals(_ns(project="/flag/path"))
        assert opts.project_path == Path("/flag/path")

    def test_project_env_used_when_no_flag(self, monkeypatch):
        monkeypatch.setenv("RIVET_PROJECT", "/env/path")
        opts = resolve_globals(_ns(project=None))
        assert opts.project_path == Path("/env/path")

    def test_no_color_flag_overrides_env(self, monkeypatch):
        monkeypatch.delenv("RIVET_NO_COLOR", raising=False)
        opts = resolve_globals(_ns(no_color=True))
        assert opts.color is False

    def test_no_color_env_used_when_no_flag(self, monkeypatch):
        monkeypatch.setenv("RIVET_NO_COLOR", "1")
        opts = resolve_globals(_ns(no_color=False))
        assert opts.color is False

    def test_no_color_env_ignored_when_not_1(self, monkeypatch):
        monkeypatch.setenv("RIVET_NO_COLOR", "0")
        opts = resolve_globals(_ns(no_color=False))
        assert opts.color is True

    @given(
        flag_profile=st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=("L", "N"))),
        env_profile=st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=("L", "N"))),
    )
    @settings(max_examples=100)
    def test_property_flag_always_overrides_env(self, flag_profile, env_profile):
        """Property 1: flags override env vars for all inputs."""
        import os
        old = os.environ.get("RIVET_PROFILE")
        try:
            os.environ["RIVET_PROFILE"] = env_profile
            opts = resolve_globals(_ns(profile=flag_profile))
            assert opts.profile == flag_profile
        finally:
            if old is None:
                os.environ.pop("RIVET_PROFILE", None)
            else:
                os.environ["RIVET_PROFILE"] = old


# ---------------------------------------------------------------------------
# resolve_globals — verbosity
# ---------------------------------------------------------------------------


class TestResolveGlobalsVerbosity:
    def test_quiet(self, monkeypatch):
        monkeypatch.delenv("RIVET_NO_COLOR", raising=False)
        opts = resolve_globals(_ns(quiet=True))
        assert opts.verbosity == -1

    def test_verbose_once(self, monkeypatch):
        monkeypatch.delenv("RIVET_NO_COLOR", raising=False)
        opts = resolve_globals(_ns(verbose=1))
        assert opts.verbosity == 1

    def test_verbose_twice(self, monkeypatch):
        monkeypatch.delenv("RIVET_NO_COLOR", raising=False)
        opts = resolve_globals(_ns(verbose=2))
        assert opts.verbosity == 2

    def test_verbose_capped_at_2(self, monkeypatch):
        monkeypatch.delenv("RIVET_NO_COLOR", raising=False)
        opts = resolve_globals(_ns(verbose=5))
        assert opts.verbosity == 2


# ---------------------------------------------------------------------------
# build_parser — recognizes all 6 commands
# ---------------------------------------------------------------------------

COMMANDS = ["init", "doctor", "compile", "run", "test", "watermark"]


class TestBuildParser:
    def test_recognizes_all_six_commands(self):
        parser = build_parser()
        for cmd in COMMANDS:
            args = parser.parse_args([cmd])
            assert args.command == cmd

    @pytest.mark.parametrize("cmd", COMMANDS)
    def test_global_options_on_each_command(self, cmd):
        parser = build_parser()
        argv = [cmd, "--profile", "prod", "--project", "/my/proj", "-v", "--no-color"]
        args = parser.parse_args(argv)
        assert args.command == cmd
        assert args.profile == "prod"
        assert args.project == "/my/proj"
        assert args.verbose >= 1
        assert args.no_color is True

    def test_compile_subparser_options(self):
        parser = build_parser()
        args = parser.parse_args(["compile", "my_sink", "-f", "json", "-t", "a", "-t", "b", "--tag-all", "-o", "out.json"])
        assert args.sink_name == "my_sink"
        assert args.format == "json"
        assert args.tag == ["a", "b"]
        assert args.tag_all is True
        assert args.output == "out.json"

    def test_run_subparser_options(self):
        parser = build_parser()
        args = parser.parse_args(["run", "my_sink", "--no-fail-fast", "-t", "x", "--format", "json"])
        assert args.sink_name == "my_sink"
        assert args.fail_fast is False
        assert args.tag == ["x"]
        assert args.format == "json"

    def test_test_subparser_options(self):
        parser = build_parser()
        args = parser.parse_args(["test", "tests/a.test.yaml", "--target", "j1", "--update-snapshots", "--fail-fast", "--format", "json"])
        assert args.file_paths == ["tests/a.test.yaml"]
        assert args.target == "j1"
        assert args.update_snapshots is True
        assert args.fail_fast is True
        assert args.format == "json"

    def test_doctor_subparser_options(self):
        parser = build_parser()
        args = parser.parse_args(["doctor", "--check-connections", "--check-schemas"])
        assert args.check_connections is True
        assert args.check_schemas is True

    def test_init_subparser_options(self):
        parser = build_parser()
        args = parser.parse_args(["init", "mydir", "--bare"])
        assert args.directory == "mydir"
        assert args.bare is True

    def test_watermark_list(self):
        parser = build_parser()
        args = parser.parse_args(["watermark", "list"])
        assert args.command == "watermark"
        assert args.watermark_action == "list"

    def test_watermark_reset(self):
        parser = build_parser()
        args = parser.parse_args(["watermark", "reset", "my_joint"])
        assert args.watermark_action == "reset"
        assert args.joint_name == "my_joint"

    def test_watermark_set(self):
        parser = build_parser()
        args = parser.parse_args(["watermark", "set", "my_joint", "2024-01-01"])
        assert args.watermark_action == "set"
        assert args.joint_name == "my_joint"
        assert args.value == "2024-01-01"


# ---------------------------------------------------------------------------
# CLI entry point — unknown command, invalid flag, no-args, --version
# ---------------------------------------------------------------------------


class TestMainEntryPoint:
    def test_unknown_command_exit_10(self, capsys):
        """Property 2: unknown command → exit code 10 with RVT-851."""
        code = main(["boguscmd"])
        assert code == 10
        err = capsys.readouterr().err
        assert "RVT-852" in err or "RVT-851" in err

    def test_invalid_flag_exit_10(self, capsys):
        code = main(["compile", "--nonexistent-flag"])
        assert code == 10
        err = capsys.readouterr().err
        assert "RVT-852" in err

    def test_no_args_shows_help(self, capsys):
        code = main([])
        assert code == 0
        out = capsys.readouterr().out
        assert "usage:" in out.lower() or "rivet" in out.lower()

    def test_version_exits_0(self, capsys):
        code = main(["--version"])
        assert code == 0
        out = capsys.readouterr().out
        assert "rivet " in out

    @given(cmd=st.text(min_size=1, max_size=30, alphabet=st.characters(whitelist_categories=("L",))).filter(lambda s: s not in COMMANDS))
    @settings(max_examples=100)
    def test_property_unknown_command_always_exit_10(self, cmd):
        """Property 2: any non-recognized command string → exit 10."""
        code = main([cmd])
        assert code == 10
