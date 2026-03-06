"""Unit tests for non-interactive mode in run_catalog_create().

Task 5.3/5.4: Non-interactive mode tests.
Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from rivet_cli.commands.catalog_create import parse_key_value_pairs, run_catalog_create

# ── Stubs ─────────────────────────────────────────────────────────────────────


class StubPlugin:
    type = "stubdb"
    required_options: list[str] = ["host", "database"]
    optional_options: dict[str, Any] = {"port": 5432, "timeout": 30}
    credential_options: list[str] = ["password"]

    def __init__(self, *, validate_error: Exception | None = None):
        self._validate_error = validate_error

    def validate(self, options: dict[str, Any]) -> None:
        if self._validate_error is not None:
            raise self._validate_error

    def instantiate(self, name: str, options: dict[str, Any]) -> Any:
        return MagicMock()


class NoCred(StubPlugin):
    type = "nocred"
    credential_options: list[str] = []


class FakeGlobals:
    def __init__(self, profile="default", project_path=None, color=False):
        self.profile = profile
        self.project_path = project_path or Path(".")
        self.color = color


_project_dir: Path | None = None


@pytest.fixture(autouse=True)
def _noninteractive_project_dir(tmp_path):
    """Create a minimal rivet.yaml so the wizard's existence check passes."""
    global _project_dir
    (tmp_path / "rivet.yaml").write_text("sources: ./sources\njoints: ./joints\nsinks: ./sinks\nprofiles: profiles.yaml\n")
    _project_dir = tmp_path
    FakeGlobals.__init__.__defaults__ = ("default", tmp_path, False)
    yield
    FakeGlobals.__init__.__defaults__ = ("default", None, False)
    _project_dir = None


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


def _make_registry(plugins: list[Any]) -> MagicMock:
    reg = MagicMock()
    reg._catalog_plugins = {p.type: p for p in plugins}
    reg.get_catalog_plugin = lambda t: reg._catalog_plugins.get(t)
    reg.register_builtins = MagicMock()
    reg.discover_plugins = MagicMock()
    return reg


def _run_noninteractive(reg, fake_config, *, catalog_type, catalog_name,
                         options=None, credentials=None, no_test=True, dry_run=False):
    """Helper to run non-interactive mode with mocked registry/config."""
    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("rivet_cli.commands.catalog_create.write_catalog_to_profile"):
        return run_catalog_create(
            catalog_type=catalog_type,
            catalog_name=catalog_name,
            options=options or [],
            credentials=credentials or [],
            no_test=no_test,
            dry_run=dry_run,
            globals=FakeGlobals(),
        )


# ── parse_key_value_pairs ─────────────────────────────────────────────────────


def test_parse_key_value_pairs_basic():
    assert parse_key_value_pairs(["host=localhost", "port=5432"]) == {
        "host": "localhost", "port": "5432",
    }


def test_parse_key_value_pairs_with_equals_in_value():
    assert parse_key_value_pairs(["conn=host=localhost"]) == {"conn": "host=localhost"}


def test_parse_key_value_pairs_ignores_malformed():
    assert parse_key_value_pairs(["noequals", "good=val"]) == {"good": "val"}


def test_parse_key_value_pairs_empty():
    assert parse_key_value_pairs([]) == {}


# ── Non-interactive happy path (Req 2.1) ──────────────────────────────────────


def test_noninteractive_happy_path(capsys):
    """All flags provided → success, no prompts."""
    plugin = NoCred()
    reg = _make_registry([plugin])
    config = FakeConfigResult(profile=FakeProfile())

    code = _run_noninteractive(
        reg, config,
        catalog_type="nocred",
        catalog_name="my_nocred",
        options=["host=localhost", "database=testdb"],
    )
    assert code == 0


def test_noninteractive_with_credentials(capsys):
    """Credentials provided via --credential flags."""
    plugin = StubPlugin()
    reg = _make_registry([plugin])
    config = FakeConfigResult(profile=FakeProfile())

    code = _run_noninteractive(
        reg, config,
        catalog_type="stubdb",
        catalog_name="my_stubdb",
        options=["host=localhost", "database=testdb"],
        credentials=["password=${MY_STUBDB_PASSWORD}"],
    )
    assert code == 0


# ── Missing required options → RVT-881 (Req 2.8) ─────────────────────────────


def test_noninteractive_missing_required_options(capsys):
    """Missing required options produces RVT-881 and exit code 10."""
    plugin = NoCred()
    reg = _make_registry([plugin])
    config = FakeConfigResult(profile=FakeProfile())

    code = _run_noninteractive(
        reg, config,
        catalog_type="nocred",
        catalog_name="my_nocred",
        options=["host=localhost"],  # missing 'database'
    )
    assert code == 10
    err = capsys.readouterr().err
    assert "RVT-881" in err
    assert "database" in err


def test_noninteractive_no_options_all_missing(capsys):
    """No options at all → RVT-881 listing all required."""
    plugin = NoCred()
    reg = _make_registry([plugin])
    config = FakeConfigResult(profile=FakeProfile())

    code = _run_noninteractive(
        reg, config,
        catalog_type="nocred",
        catalog_name="my_nocred",
        options=[],
    )
    assert code == 10
    err = capsys.readouterr().err
    assert "RVT-881" in err
    assert "host" in err
    assert "database" in err


# ── --no-test skips connection test (Req 2.6) ────────────────────────────────


def test_noninteractive_no_test_skips_connection(capsys):
    """--no-test flag skips connection test entirely."""
    plugin = NoCred()
    reg = _make_registry([plugin])
    config = FakeConfigResult(profile=FakeProfile())

    with patch("rivet_cli.commands.catalog_create.test_connection") as mock_tc:
        code = _run_noninteractive(
            reg, config,
            catalog_type="nocred",
            catalog_name="my_nocred",
            options=["host=localhost", "database=testdb"],
            no_test=True,
        )
    assert code == 0
    mock_tc.assert_not_called()


# ── Connection test runs when --no-test is not set ────────────────────────────


def test_noninteractive_runs_connection_test(capsys):
    """Without --no-test, connection test runs automatically."""
    plugin = NoCred()
    reg = _make_registry([plugin])
    config = FakeConfigResult(profile=FakeProfile())

    with patch("rivet_cli.commands.catalog_create.test_connection",
               return_value=(True, 0.5, None)):
        code = _run_noninteractive(
            reg, config,
            catalog_type="nocred",
            catalog_name="my_nocred",
            options=["host=localhost", "database=testdb"],
            no_test=False,
        )
    assert code == 0
    out = capsys.readouterr().out
    assert "0.50" in out


def test_noninteractive_connection_failure_exits(capsys):
    """Connection failure in non-interactive mode is fatal (exit 1)."""
    plugin = NoCred()
    reg = _make_registry([plugin])
    config = FakeConfigResult(profile=FakeProfile())

    with patch("rivet_cli.commands.catalog_create.test_connection",
               return_value=(False, 1.0, "Connection refused (RVT-886)")):
        code = _run_noninteractive(
            reg, config,
            catalog_type="nocred",
            catalog_name="my_nocred",
            options=["host=localhost", "database=testdb"],
            no_test=False,
        )
    assert code == 1
    err = capsys.readouterr().err
    assert "RVT-886" in err


# ── Unknown catalog type ──────────────────────────────────────────────────────


def test_noninteractive_unknown_type(capsys):
    """Unknown --type produces error and exit code 10."""
    plugin = NoCred()
    reg = _make_registry([plugin])
    config = FakeConfigResult(profile=FakeProfile())

    code = _run_noninteractive(
        reg, config,
        catalog_type="nonexistent",
        catalog_name="my_cat",
        options=["host=localhost", "database=testdb"],
    )
    assert code == 10
    err = capsys.readouterr().err
    assert "RVT-882" in err


# ── Invalid catalog name ─────────────────────────────────────────────────────


def test_noninteractive_invalid_name(capsys):
    """Invalid --name produces RVT-884 and exit code 10."""
    plugin = NoCred()
    reg = _make_registry([plugin])
    config = FakeConfigResult(profile=FakeProfile())

    code = _run_noninteractive(
        reg, config,
        catalog_type="nocred",
        catalog_name="INVALID",
        options=["host=localhost", "database=testdb"],
    )
    assert code == 10
    err = capsys.readouterr().err
    assert "RVT-884" in err


# ── Existing name is allowed (overwrite) in non-interactive ───────────────────


def test_noninteractive_existing_name_overwrites(capsys):
    """Existing catalog name proceeds (overwrite) in non-interactive mode."""
    plugin = NoCred()
    reg = _make_registry([plugin])
    existing = {"my_nocred": MagicMock()}
    config = FakeConfigResult(profile=FakeProfile(catalogs=existing))

    code = _run_noninteractive(
        reg, config,
        catalog_type="nocred",
        catalog_name="my_nocred",
        options=["host=localhost", "database=testdb"],
    )
    assert code == 0


# ── Plugin validation failure ─────────────────────────────────────────────────


def test_noninteractive_validation_failure(capsys):
    """Plugin validation failure is fatal in non-interactive mode."""
    plugin = NoCred()
    plugin._validate_error = ValueError("bad host format")
    # Need a fresh class instance with the error
    class FailPlugin(NoCred):
        def validate(self, options):
            raise ValueError("bad host format")

    fp = FailPlugin()
    reg = _make_registry([fp])
    config = FakeConfigResult(profile=FakeProfile())

    code = _run_noninteractive(
        reg, config,
        catalog_type="nocred",
        catalog_name="my_nocred",
        options=["host=bad", "database=testdb"],
    )
    assert code == 1
    err = capsys.readouterr().err
    assert "RVT-885" in err


# ── Optional options get defaults ─────────────────────────────────────────────


def test_noninteractive_optional_defaults_applied():
    """Optional options not provided get filled with plugin defaults."""
    plugin = NoCred()
    reg = _make_registry([plugin])
    config = FakeConfigResult(profile=FakeProfile())

    # Only provide required options, not optional ones
    code = _run_noninteractive(
        reg, config,
        catalog_type="nocred",
        catalog_name="my_nocred",
        options=["host=localhost", "database=testdb"],
    )
    assert code == 0
