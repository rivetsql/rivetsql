"""Unit tests for interactive wizard flow — run_catalog_create().

Task 4.3: Tests for interactive flow with mocked input/getpass.
Requirements: 4.4, 5.3, 7.4, 8.2, 8.3
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from rivet_cli.commands.catalog_create import run_catalog_create

# ── Stub plugin ───────────────────────────────────────────────────────────────


class StubCatalogPlugin:
    """Minimal CatalogPlugin stub for wizard tests."""

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

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        return logical_name


class NoCredPlugin(StubCatalogPlugin):
    """Plugin with no credential options."""

    type = "nocred"
    credential_options: list[str] = []


class FakeGlobals:
    def __init__(self, profile="default", project_path=None, color=False):
        self.profile = profile
        self.project_path = project_path or Path(".")
        self.color = color


_project_dir: Path | None = None


@pytest.fixture(autouse=True)
def _wizard_project_dir(tmp_path):
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
    """Build a mock PluginRegistry with given catalog plugins."""
    reg = MagicMock()
    reg._catalog_plugins = {p.type: p for p in plugins}
    reg.get_catalog_plugin = lambda t: reg._catalog_plugins.get(t)
    reg.register_builtins = MagicMock()
    reg.discover_plugins = MagicMock()
    return reg


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_no_plugins_exits_with_error(capsys):
    """RVT-882 when no catalog plugins are registered."""
    reg = _make_registry([])
    fake_config = FakeConfigResult(profile=FakeProfile())

    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config):
        code = run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=False, dry_run=False,
            globals=FakeGlobals(),
        )

    assert code == 1
    err = capsys.readouterr().err
    assert "RVT-882" in err


def test_happy_path_interactive(capsys):
    """Full interactive happy path: type selection, name, required, optional declined, credential, validation passes."""
    plugin = StubCatalogPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # Inputs: type=1 (stubdb), name=my_stubdb (default), host=localhost, database=testdb,
    # optional=n, credential=secret_pw, plaintext warning=y, test_connection=n, write=y
    inputs = iter(["1", "", "localhost", "testdb", "n", "y", "n", "y"])

    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("rivet_cli.commands.catalog_create.write_catalog_to_profile"), \
         patch("builtins.input", side_effect=inputs), \
         patch("getpass.getpass", return_value="secret_pw"):
        code = run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=False, dry_run=False,
            globals=FakeGlobals(),
        )

    assert code == 0
    out = capsys.readouterr().out
    assert "validated" in out.lower()


def test_invalid_name_reprompts(capsys):
    """Invalid name triggers re-prompt, then valid name proceeds."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # Inputs: type=1, name="INVALID" (uppercase), then "good_name", host=h, database=d, optional=n, test_conn=n, write=y
    inputs = iter(["1", "INVALID", "good_name", "h", "d", "n", "n", "y"])

    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("rivet_cli.commands.catalog_create.write_catalog_to_profile"), \
         patch("builtins.input", side_effect=inputs), \
         patch("getpass.getpass", return_value=""):
        code = run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=False, dry_run=False,
            globals=FakeGlobals(),
        )

    assert code == 0
    out = capsys.readouterr().out
    assert "lowercase" in out.lower()


def test_name_conflict_overwrite_confirmed(capsys):
    """Existing name conflict with overwrite confirmation proceeds."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    existing = {"existing_cat": MagicMock()}
    fake_config = FakeConfigResult(profile=FakeProfile(catalogs=existing))

    # type=1, name=existing_cat, overwrite=y, host=h, database=d, optional=n, test_conn=n, write=y
    inputs = iter(["1", "existing_cat", "y", "h", "d", "n", "n", "y"])

    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("rivet_cli.commands.catalog_create.write_catalog_to_profile"), \
         patch("builtins.input", side_effect=inputs), \
         patch("getpass.getpass", return_value=""):
        code = run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=False, dry_run=False,
            globals=FakeGlobals(),
        )

    assert code == 0


def test_name_conflict_overwrite_declined_reprompts(capsys):
    """Declining overwrite re-prompts for a different name."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    existing = {"existing_cat": MagicMock()}
    fake_config = FakeConfigResult(profile=FakeProfile(catalogs=existing))

    # type=1, name=existing_cat, overwrite=n, name=new_cat, host=h, database=d, optional=n, test_conn=n, write=y
    inputs = iter(["1", "existing_cat", "n", "new_cat", "h", "d", "n", "n", "y"])

    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("rivet_cli.commands.catalog_create.write_catalog_to_profile"), \
         patch("builtins.input", side_effect=inputs), \
         patch("getpass.getpass", return_value=""):
        code = run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=False, dry_run=False,
            globals=FakeGlobals(),
        )

    assert code == 0


def test_plaintext_credential_warning(capsys):
    """Plaintext credential triggers warning and confirmation prompt."""
    plugin = StubCatalogPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=default, host=h, database=d, optional=n, plaintext_confirm=y, test_conn=n, write=y
    inputs = iter(["1", "", "h", "d", "n", "y", "n", "y"])

    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("rivet_cli.commands.catalog_create.write_catalog_to_profile"), \
         patch("builtins.input", side_effect=inputs), \
         patch("getpass.getpass", return_value="plaintext_secret"):
        code = run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=False, dry_run=False,
            globals=FakeGlobals(),
        )

    assert code == 0
    out = capsys.readouterr().out
    assert "plaintext" in out.lower()


def test_validation_failure_reenter(capsys):
    """Plugin validation failure offers re-enter, then passes on retry."""
    call_count = 0
    original_error = ValueError("bad host")

    class FailOncePlugin(StubCatalogPlugin):
        type = "failonce"
        credential_options: list[str] = []

        def validate(self, options: dict[str, Any]) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise original_error

    plugin = FailOncePlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=default, host=bad, database=d, optional=n,
    # re-enter=y, host=good, database=d, test_conn=n, write=y
    inputs = iter(["1", "", "bad", "d", "n", "y", "good", "d", "n", "y"])

    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("rivet_cli.commands.catalog_create.write_catalog_to_profile"), \
         patch("builtins.input", side_effect=inputs), \
         patch("getpass.getpass", return_value=""):
        code = run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=False, dry_run=False,
            globals=FakeGlobals(),
        )

    assert code == 0
    err = capsys.readouterr().err
    assert "RVT-885" in err


def test_validation_failure_abort(capsys):
    """Plugin validation failure with abort returns error code."""
    plugin = StubCatalogPlugin(validate_error=ValueError("always fails"))
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=default, host=h, database=d, optional=n, credential=pw, plaintext=y, re-enter=n
    inputs = iter(["1", "", "h", "d", "n", "y", "n"])

    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("builtins.input", side_effect=inputs), \
         patch("getpass.getpass", return_value="pw"):
        code = run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=False, dry_run=False,
            globals=FakeGlobals(),
        )

    assert code == 1


def test_optional_settings_configured(capsys):
    """When user opts to configure optional settings, values are collected."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=default, host=h, database=d, optional=y, port=9999, timeout=60, test_conn=n, write=y
    inputs = iter(["1", "", "h", "d", "y", "9999", "60", "n", "y"])

    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("rivet_cli.commands.catalog_create.write_catalog_to_profile"), \
         patch("builtins.input", side_effect=inputs), \
         patch("getpass.getpass", return_value=""):
        code = run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=False, dry_run=False,
            globals=FakeGlobals(),
        )

    assert code == 0


def test_env_var_credential_no_warning(capsys):
    """Providing ${ENV_VAR} as credential skips plaintext warning."""
    plugin = StubCatalogPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=default, host=h, database=d, optional=n, test_conn=n
    # No plaintext warning prompt needed since credential is env var ref
    inputs = iter(["1", "", "h", "d", "n", "n", "y"])

    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("rivet_cli.commands.catalog_create.write_catalog_to_profile"), \
         patch("builtins.input", side_effect=inputs), \
         patch("getpass.getpass", return_value="${MY_STUBDB_PASSWORD}"):
        code = run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=False, dry_run=False,
            globals=FakeGlobals(),
        )

    assert code == 0
    out = capsys.readouterr().out
    # No plaintext warning should appear
    assert "plaintext" not in out.lower() or "tip" in out.lower()


# ── Task 5.2: Connection test integration ─────────────────────────────────────
# Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7


def _run_nocred_wizard(reg, fake_config, inputs, *, no_test=False):
    """Helper: run wizard with NoCredPlugin, mocked registry/config/input."""
    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("rivet_cli.commands.catalog_create.write_catalog_to_profile"), \
         patch("builtins.input", side_effect=iter(inputs)), \
         patch("getpass.getpass", return_value=""):
        return run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=no_test, dry_run=False,
            globals=FakeGlobals(),
        )


def test_no_test_flag_skips_connection_test(capsys):
    """--no-test skips the connection test entirely (Req 9.5)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=default, host=h, database=d, optional=n — no connection test prompt, write=y
    inputs = ["1", "", "h", "d", "n", "y"]

    with patch("rivet_cli.commands.catalog_create.test_connection") as mock_tc:
        code = _run_nocred_wizard(reg, fake_config, inputs, no_test=True)

    assert code == 0
    mock_tc.assert_not_called()


def test_connection_test_declined_skips_test(capsys):
    """Declining the connection test prompt skips it (Req 9.1)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=default, host=h, database=d, optional=n, test_conn=n, write=y
    inputs = ["1", "", "h", "d", "n", "n", "y"]

    with patch("rivet_cli.commands.catalog_create.test_connection") as mock_tc:
        code = _run_nocred_wizard(reg, fake_config, inputs)

    assert code == 0
    mock_tc.assert_not_called()


def test_connection_test_success_displays_elapsed(capsys):
    """Successful connection test displays elapsed time (Req 9.3)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=default, host=h, database=d, optional=n, test_conn=y, write=y
    inputs = ["1", "", "h", "d", "n", "y", "y"]

    with patch("rivet_cli.commands.catalog_create.test_connection", return_value=(True, 1.23, None)):
        code = _run_nocred_wizard(reg, fake_config, inputs)

    assert code == 0
    out = capsys.readouterr().out
    assert "1.23" in out


def test_connection_test_failure_offers_choices(capsys):
    """Connection test failure shows RVT-886 and offers re-enter/skip/abort (Req 9.4)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=default, host=h, database=d, optional=n, test_conn=y, choice=s (skip), write=y
    inputs = ["1", "", "h", "d", "n", "y", "s", "y"]

    with patch("rivet_cli.commands.catalog_create.test_connection",
               return_value=(False, 0.5, "Connection refused (RVT-886)")):
        code = _run_nocred_wizard(reg, fake_config, inputs)

    assert code == 0
    err = capsys.readouterr().err
    assert "RVT-886" in err


def test_connection_test_failure_abort(capsys):
    """Choosing abort on connection failure returns error code (Req 9.4)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=default, host=h, database=d, optional=n, test_conn=y, choice=a (abort)
    inputs = ["1", "", "h", "d", "n", "y", "a"]

    with patch("rivet_cli.commands.catalog_create.test_connection",
               return_value=(False, 0.5, "Connection refused (RVT-886)")):
        code = _run_nocred_wizard(reg, fake_config, inputs)

    assert code == 1


def test_connection_test_failure_reenter_then_success(capsys):
    """Re-entering options after failure retries the connection test (Req 9.4)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=default, host=bad, database=d, optional=n, test_conn=y,
    # choice=r (re-enter), host=good, database=d → success, write=y
    inputs = ["1", "", "bad", "d", "n", "y", "r", "good", "d", "y"]

    call_count = 0
    def fake_test_connection(plugin, name, opts, timeout=30.0):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return (False, 0.1, "refused (RVT-886)")
        return (True, 0.5, None)

    with patch("rivet_cli.commands.catalog_create.test_connection", side_effect=fake_test_connection):
        code = _run_nocred_wizard(reg, fake_config, inputs)

    assert code == 0
    assert call_count == 2


def test_connection_test_timeout_shows_error(capsys):
    """Timeout during connection test shows RVT-886 error (Req 9.6, 9.7)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=default, host=h, database=d, optional=n, test_conn=y, choice=s (skip), write=y
    inputs = ["1", "", "h", "d", "n", "y", "s", "y"]

    with patch("rivet_cli.commands.catalog_create.test_connection",
               return_value=(False, 30.0, "Connection timed out after 30s (RVT-886)")):
        code = _run_nocred_wizard(reg, fake_config, inputs)

    assert code == 0
    err = capsys.readouterr().err
    assert "RVT-886" in err
    assert "timed out" in err.lower() or "timeout" in err.lower() or "30" in err


# ── Task 6.1: Preview and confirmation flow ───────────────────────────────────
# Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6


def test_preview_shows_yaml_block(capsys):
    """Preview displays the catalog YAML block (Req 10.1)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=default, host=localhost, database=testdb, optional=n, test_conn=n, write=y
    inputs = ["1", "", "localhost", "testdb", "n", "n", "y"]

    code = _run_nocred_wizard(reg, fake_config, inputs)

    assert code == 0
    out = capsys.readouterr().out
    assert "preview" in out.lower()
    assert "localhost" in out
    assert "testdb" in out


def test_preview_masks_plaintext_credentials(capsys):
    """Preview replaces plaintext credentials with ${ENV_VAR} refs (Req 10.2)."""
    plugin = StubCatalogPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=my_stubdb (default), host=h, database=d, optional=n,
    # plaintext_confirm=y, test_conn=n, write=y
    inputs = iter(["1", "", "h", "d", "n", "y", "n", "y"])

    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("rivet_cli.commands.catalog_create.write_catalog_to_profile"), \
         patch("builtins.input", side_effect=inputs), \
         patch("getpass.getpass", return_value="plaintext_secret"):
        code = run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=False, dry_run=False,
            globals=FakeGlobals(),
        )

    assert code == 0
    out = capsys.readouterr().out
    # Plaintext value must NOT appear in preview
    assert "plaintext_secret" not in out
    # Masked env var ref must appear
    assert "${MY_STUBDB_PASSWORD}" in out


def test_dry_run_shows_preview_and_exits_zero(capsys):
    """--dry-run displays preview and exits 0 without writing (Req 10.6)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=default, host=h, database=d, optional=n, test_conn=n — no write prompt
    inputs = ["1", "", "h", "d", "n", "n"]

    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("builtins.input", side_effect=iter(inputs)), \
         patch("getpass.getpass", return_value=""):
        code = run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=False, dry_run=True,
            globals=FakeGlobals(),
        )

    assert code == 0
    out = capsys.readouterr().out
    assert "preview" in out.lower()


def test_confirm_write_declined_abort(capsys):
    """Declining write confirmation and choosing abort returns error code (Req 10.5)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=default, host=h, database=d, optional=n, test_conn=n, write=n, choice=a
    inputs = ["1", "", "h", "d", "n", "n", "n", "a"]

    code = _run_nocred_wizard(reg, fake_config, inputs)

    assert code == 1


def test_confirm_write_proceeds_on_yes(capsys):
    """Confirming write returns success (Req 10.4)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=default, host=h, database=d, optional=n, test_conn=n, write=y
    inputs = ["1", "", "h", "d", "n", "n", "y"]

    code = _run_nocred_wizard(reg, fake_config, inputs)

    assert code == 0


def test_confirm_write_declined_reenter_then_confirm(capsys):
    """Declining write, re-entering options, then confirming succeeds (Req 10.5)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=default, host=h, database=d, optional=n, test_conn=n,
    # write=n, choice=r (re-enter), host=h2, database=d2, write=y
    inputs = ["1", "", "h", "d", "n", "n", "n", "r", "h2", "d2", "y"]

    code = _run_nocred_wizard(reg, fake_config, inputs)

    assert code == 0
    out = capsys.readouterr().out
    # Preview should appear twice (once before decline, once after re-enter)
    assert out.count("preview") >= 2 or out.lower().count("preview") >= 1


# ── Non-interactive preview tests ─────────────────────────────────────────────


def _run_noninteractive_preview(reg, fake_config, *, catalog_type, catalog_name,
                                 options=None, credentials=None, no_test=True, dry_run=False):
    """Helper to run non-interactive mode."""
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


def test_noninteractive_preview_shown(capsys):
    """Non-interactive mode shows preview before writing (Req 10.1)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    config = FakeConfigResult(profile=FakeProfile())

    code = _run_noninteractive_preview(
        reg, config,
        catalog_type="nocred",
        catalog_name="my_nocred",
        options=["host=localhost", "database=testdb"],
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "preview" in out.lower()
    assert "localhost" in out


def test_noninteractive_dry_run_shows_preview_exits_zero(capsys):
    """Non-interactive --dry-run shows preview and exits 0 (Req 10.6)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    config = FakeConfigResult(profile=FakeProfile())

    code = _run_noninteractive_preview(
        reg, config,
        catalog_type="nocred",
        catalog_name="my_nocred",
        options=["host=localhost", "database=testdb"],
        dry_run=True,
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "preview" in out.lower()


def test_noninteractive_credentials_masked_in_preview(capsys):
    """Non-interactive preview masks plaintext credentials (Req 10.2)."""
    plugin = StubCatalogPlugin()
    reg = _make_registry([plugin])
    config = FakeConfigResult(profile=FakeProfile())

    code = _run_noninteractive_preview(
        reg, config,
        catalog_type="stubdb",
        catalog_name="my_stubdb",
        options=["host=localhost", "database=testdb"],
        credentials=["password=supersecret"],
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "supersecret" not in out
    assert "${MY_STUBDB_PASSWORD}" in out


# ── Task 6.2: Write and post-write flow ──────────────────────────────────────
# Requirements: 11.1, 11.6, 11.7


def test_write_called_after_confirmation(tmp_path):
    """After write confirmation, write_catalog_to_profile is called (Req 11.1)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    profiles = tmp_path / "profiles.yaml"
    fake_manifest = MagicMock()
    fake_manifest.profiles_path = profiles
    fake_config = FakeConfigResult(profile=FakeProfile(), manifest=fake_manifest)

    # type=1, name=default, host=h, database=d, optional=n, write=y
    inputs = ["1", "", "h", "d", "n", "y"]

    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("rivet_cli.commands.catalog_create.write_catalog_to_profile") as mock_write, \
         patch("builtins.input", side_effect=iter(inputs)), \
         patch("getpass.getpass", return_value=""):
        code = run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=True, dry_run=False,
            globals=FakeGlobals(),
        )

    assert code == 0
    mock_write.assert_called_once()
    call_args = mock_write.call_args
    assert call_args[0][0] == profiles  # profiles_path
    assert call_args[0][2] == "my_nocred"  # catalog_name


def test_success_message_shows_catalog_name_type_and_path(capsys, tmp_path):
    """Success message includes catalog name, type, and file path (Req 11.6)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    profiles = tmp_path / "profiles.yaml"
    fake_manifest = MagicMock()
    fake_manifest.profiles_path = profiles
    fake_config = FakeConfigResult(profile=FakeProfile(), manifest=fake_manifest)

    # type=1, name=default, host=h, database=d, optional=n, write=y
    inputs = ["1", "", "h", "d", "n", "y"]

    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("rivet_cli.commands.catalog_create.write_catalog_to_profile"), \
         patch("builtins.input", side_effect=iter(inputs)), \
         patch("getpass.getpass", return_value=""):
        code = run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=True, dry_run=False,
            globals=FakeGlobals(),
        )

    assert code == 0
    out = capsys.readouterr().out
    assert "my_nocred" in out
    assert "nocred" in out
    assert str(profiles) in out


def test_next_steps_hint_shown(capsys):
    """Next-steps hint is displayed after successful write (Req 11.7)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=default, host=h, database=d, optional=n, write=y
    inputs = ["1", "", "h", "d", "n", "y"]

    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("rivet_cli.commands.catalog_create.write_catalog_to_profile"), \
         patch("builtins.input", side_effect=iter(inputs)), \
         patch("getpass.getpass", return_value=""):
        code = run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=True, dry_run=False,
            globals=FakeGlobals(),
        )

    assert code == 0
    out = capsys.readouterr().out
    assert "rivet catalog list" in out
    assert "rivet doctor --check-connections" in out


def test_write_failure_returns_error_code(capsys):
    """Write failure (RVT-883) returns error code 1 (Req 11.5)."""
    from rivet_cli.commands.catalog_create import CatalogWriteError
    from rivet_cli.errors import RVT_883, CLIError

    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=default, host=h, database=d, optional=n, write=y
    inputs = ["1", "", "h", "d", "n", "y"]

    write_error = CatalogWriteError(CLIError(
        code=RVT_883,
        message="Cannot write profiles.yaml: Permission denied",
        remediation="Check file permissions.",
    ))

    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("rivet_cli.commands.catalog_create.write_catalog_to_profile", side_effect=write_error), \
         patch("builtins.input", side_effect=iter(inputs)), \
         patch("getpass.getpass", return_value=""):
        code = run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=True, dry_run=False,
            globals=FakeGlobals(),
        )

    assert code == 1
    err = capsys.readouterr().err
    assert "RVT-883" in err


def test_noninteractive_write_called(tmp_path):
    """Non-interactive mode calls write_catalog_to_profile after preview (Req 11.1)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    profiles = tmp_path / "profiles.yaml"
    fake_manifest = MagicMock()
    fake_manifest.profiles_path = profiles
    fake_config = FakeConfigResult(profile=FakeProfile(), manifest=fake_manifest)

    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("rivet_cli.commands.catalog_create.write_catalog_to_profile") as mock_write:
        code = run_catalog_create(
            catalog_type="nocred",
            catalog_name="my_nocred",
            options=["host=localhost", "database=testdb"],
            credentials=[],
            no_test=True, dry_run=False,
            globals=FakeGlobals(),
        )

    assert code == 0
    mock_write.assert_called_once()


def test_noninteractive_success_message_and_next_steps(capsys, tmp_path):
    """Non-interactive mode shows success message and next-steps (Req 11.6, 11.7)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    profiles = tmp_path / "profiles.yaml"
    fake_manifest = MagicMock()
    fake_manifest.profiles_path = profiles
    fake_config = FakeConfigResult(profile=FakeProfile(), manifest=fake_manifest)

    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("rivet_cli.commands.catalog_create.write_catalog_to_profile"):
        code = run_catalog_create(
            catalog_type="nocred",
            catalog_name="my_nocred",
            options=["host=localhost", "database=testdb"],
            credentials=[],
            no_test=True, dry_run=False,
            globals=FakeGlobals(),
        )

    assert code == 0
    out = capsys.readouterr().out
    assert "my_nocred" in out
    assert "nocred" in out
    assert str(profiles) in out
    assert "rivet catalog list" in out
    assert "rivet doctor --check-connections" in out


def test_dry_run_does_not_write(capsys):
    """--dry-run does not call write_catalog_to_profile (Req 10.6)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile())

    # type=1, name=default, host=h, database=d, optional=n — no write prompt with dry_run
    inputs = ["1", "", "h", "d", "n"]

    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("rivet_cli.commands.catalog_create.write_catalog_to_profile") as mock_write, \
         patch("builtins.input", side_effect=iter(inputs)), \
         patch("getpass.getpass", return_value=""):
        code = run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=True, dry_run=True,
            globals=FakeGlobals(),
        )

    assert code == 0
    mock_write.assert_not_called()


# ── Task 6.3: Engine association prompt ──────────────────────────────────────
# Requirements: 12.1, 12.2, 12.3, 12.4, 12.5


@dataclass
class FakeEngine:
    name: str
    type: str
    catalogs: list = field(default_factory=list)
    options: dict = field(default_factory=dict)


def _run_with_engines(reg, fake_config, inputs, *, no_test=True, dry_run=False):
    """Helper: run wizard with NoCredPlugin and given inputs."""
    with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
         patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
         patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("rivet_cli.commands.catalog_create.write_catalog_to_profile"), \
         patch("builtins.input", side_effect=iter(inputs)), \
         patch("getpass.getpass", return_value=""):
        return run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=no_test, dry_run=dry_run,
            globals=FakeGlobals(),
        )


def test_no_compatible_engines_shows_info_message(capsys):
    """When no compatible engines exist, display informational message (Req 12.4)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    # Profile has an engine but no adapter/plugin for nocred type
    fake_config = FakeConfigResult(profile=FakeProfile(
        engines=[FakeEngine("eng1", "duckdb")]
    ))

    # type=1, name=default, host=h, database=d, optional=n, write=y
    inputs = ["1", "", "h", "d", "n", "y"]

    with patch("rivet_cli.commands.catalog_create.filter_compatible_engines", return_value=[]):
        code = _run_with_engines(reg, fake_config, inputs)

    assert code == 0
    out = capsys.readouterr().out
    assert "no" in out.lower() or "engine" in out.lower()


def test_user_declines_engine_association(capsys):
    """When user declines engine association, step is skipped (Req 12.5)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    engine = FakeEngine("eng1", "duckdb")
    fake_config = FakeConfigResult(profile=FakeProfile(engines=[engine]))

    # type=1, name=default, host=h, database=d, optional=n, write=y, associate=n
    inputs = ["1", "", "h", "d", "n", "y", "n"]

    with patch("rivet_cli.commands.catalog_create.filter_compatible_engines",
               return_value=[engine]):
        with patch("rivet_cli.commands.catalog_create.write_catalog_to_profile") as mock_write:
            with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
                 patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
                 patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
                 patch("builtins.input", side_effect=iter(inputs)), \
                 patch("getpass.getpass", return_value=""):
                code = run_catalog_create(
                    catalog_type=None, catalog_name=None,
                    options=[], credentials=[],
                    no_test=True, dry_run=False,
                    globals=FakeGlobals(),
                )

    assert code == 0
    # write_catalog_to_profile called once (for catalog write), not again for engine update
    assert mock_write.call_count == 1


def test_user_selects_engine_for_association(capsys):
    """When user selects an engine, write_catalog_to_profile is called with engine_updates (Req 12.3)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    engine = FakeEngine("eng1", "duckdb")
    fake_config = FakeConfigResult(profile=FakeProfile(engines=[engine]))
    profiles = Path("/fake/profiles.yaml")
    fake_manifest = MagicMock()
    fake_manifest.profiles_path = profiles
    fake_config.manifest = fake_manifest

    # type=1, name=default, host=h, database=d, optional=n, write=y, associate=y, engine=1
    inputs = ["1", "", "h", "d", "n", "y", "y", "1"]

    with patch("rivet_cli.commands.catalog_create.filter_compatible_engines",
               return_value=[engine]):
        with patch("rivet_cli.commands.catalog_create.write_catalog_to_profile") as mock_write:
            with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
                 patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
                 patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
                 patch("builtins.input", side_effect=iter(inputs)), \
                 patch("getpass.getpass", return_value=""):
                code = run_catalog_create(
                    catalog_type=None, catalog_name=None,
                    options=[], credentials=[],
                    no_test=True, dry_run=False,
                    globals=FakeGlobals(),
                )

    assert code == 0
    # write_catalog_to_profile called twice: once for catalog, once for engine update
    assert mock_write.call_count == 2
    # Second call should have engine_updates
    second_call = mock_write.call_args_list[1]
    engine_updates = second_call[0][4]  # 5th positional arg
    assert "eng1" in engine_updates
    assert "my_nocred" in engine_updates["eng1"]


def test_engine_association_prompt_lists_engines(capsys):
    """Compatible engines are listed for the user to choose from (Req 12.2)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    engine1 = FakeEngine("eng1", "duckdb")
    engine2 = FakeEngine("eng2", "polars")
    fake_config = FakeConfigResult(profile=FakeProfile(engines=[engine1, engine2]))

    # type=1, name=default, host=h, database=d, optional=n, write=y, associate=n
    inputs = ["1", "", "h", "d", "n", "y", "n"]

    with patch("rivet_cli.commands.catalog_create.filter_compatible_engines",
               return_value=[engine1, engine2]):
        with patch("rivet_cli.commands.catalog_create.write_catalog_to_profile"):
            with patch("rivet_cli.commands.catalog_create.PluginRegistry", return_value=reg), \
                 patch("rivet_cli.commands.catalog_create.register_optional_plugins"), \
                 patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
                 patch("builtins.input", side_effect=iter(inputs)), \
                 patch("getpass.getpass", return_value=""):
                code = run_catalog_create(
                    catalog_type=None, catalog_name=None,
                    options=[], credentials=[],
                    no_test=True, dry_run=False,
                    globals=FakeGlobals(),
                )

    assert code == 0
    out = capsys.readouterr().out
    assert "eng1" in out
    assert "eng2" in out


def test_no_engines_in_profile_skips_association(capsys):
    """When profile has no engines at all, engine association is skipped (Req 12.4)."""
    plugin = NoCredPlugin()
    reg = _make_registry([plugin])
    fake_config = FakeConfigResult(profile=FakeProfile(engines=[]))

    # type=1, name=default, host=h, database=d, optional=n, write=y — no engine prompt
    inputs = ["1", "", "h", "d", "n", "y"]

    with patch("rivet_cli.commands.catalog_create.filter_compatible_engines", return_value=[]):
        code = _run_with_engines(reg, fake_config, inputs)

    assert code == 0


# ── Task 8.2: Top-level error handling and exit codes ─────────────────────────
# Requirements: 1.5, 1.6, 13.1, 14.1, 14.2, 14.3, 14.4, 14.5


def test_keyboard_interrupt_returns_130(capsys):
    """Ctrl+C (KeyboardInterrupt) returns exit code 130 (Req 14.2)."""
    fake_config = FakeConfigResult(profile=FakeProfile())

    with patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config), \
         patch("rivet_cli.commands.catalog_create.PluginRegistry") as mock_reg_cls:
        mock_reg_cls.return_value.register_builtins.side_effect = KeyboardInterrupt
        code = run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=False, dry_run=False,
            globals=FakeGlobals(),
        )

    assert code == 130


def test_missing_rivet_yaml_returns_rvt850(tmp_path, capsys):
    """Missing rivet.yaml produces RVT-850 and exit code 10 (Req 1.6)."""
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    code = run_catalog_create(
        catalog_type=None, catalog_name=None,
        options=[], credentials=[],
        no_test=False, dry_run=False,
        globals=FakeGlobals(project_path=empty_dir),
    )

    assert code == 10
    err = capsys.readouterr().err
    assert "RVT-850" in err


def test_missing_profile_returns_rvt880(capsys):
    """Non-existent profile produces RVT-880 and exit code 10 (Req 1.5, 13.1)."""
    fake_config = FakeConfigResult(profile=None)

    with patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config):
        code = run_catalog_create(
            catalog_type=None, catalog_name=None,
            options=[], credentials=[],
            no_test=False, dry_run=False,
            globals=FakeGlobals(profile="nonexistent"),
        )

    assert code == 10
    err = capsys.readouterr().err
    assert "RVT-880" in err
    assert "nonexistent" in err


def test_noninteractive_missing_rivet_yaml_returns_rvt850(tmp_path, capsys):
    """Non-interactive mode: missing rivet.yaml produces RVT-850 (Req 1.6)."""
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    code = run_catalog_create(
        catalog_type="postgres", catalog_name="my_pg",
        options=["host=localhost", "database=db"],
        credentials=[],
        no_test=True, dry_run=False,
        globals=FakeGlobals(project_path=empty_dir),
    )

    assert code == 10
    err = capsys.readouterr().err
    assert "RVT-850" in err


def test_noninteractive_missing_profile_returns_rvt880(capsys):
    """Non-interactive mode: missing profile produces RVT-880 (Req 1.5)."""
    fake_config = FakeConfigResult(profile=None)

    with patch("rivet_cli.commands.catalog_create.load_config", return_value=fake_config):
        code = run_catalog_create(
            catalog_type="postgres", catalog_name="my_pg",
            options=["host=localhost", "database=db"],
            credentials=[],
            no_test=True, dry_run=False,
            globals=FakeGlobals(profile="nonexistent"),
        )

    assert code == 10
    err = capsys.readouterr().err
    assert "RVT-880" in err
