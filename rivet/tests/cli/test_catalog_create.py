"""Unit tests for catalog creation wizard prompt helpers.

Task 4.1: prompt_choice, prompt_value, prompt_credential, prompt_confirm
Task 5.1: test_connection
Requirements: 3.2, 5.1, 5.2, 6.1, 6.2, 7.1, 7.2, 7.5, 9.2, 9.3, 9.4, 9.6, 9.7, 13.7
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from rivet_cli.commands.catalog_create import (
    prompt_choice,
    prompt_confirm,
    prompt_credential,
    prompt_value,
    test_connection,
)

# ── prompt_choice ──────────────────────────────────────────────────────────────


def test_prompt_choice_valid_selection(capsys):
    choices = ["postgres", "duckdb", "s3"]
    with patch("builtins.input", return_value="2"):
        result = prompt_choice("Select type", choices)
    assert result == "duckdb"


def test_prompt_choice_first_item(capsys):
    choices = ["a", "b", "c"]
    with patch("builtins.input", return_value="1"):
        result = prompt_choice("Pick", choices)
    assert result == "a"


def test_prompt_choice_last_item(capsys):
    choices = ["x", "y", "z"]
    with patch("builtins.input", return_value="3"):
        result = prompt_choice("Pick", choices)
    assert result == "z"


def test_prompt_choice_reprompts_on_invalid_then_valid(capsys):
    choices = ["alpha", "beta"]
    inputs = iter(["0", "5", "abc", "2"])
    with patch("builtins.input", side_effect=inputs):
        result = prompt_choice("Pick", choices)
    assert result == "beta"
    out = capsys.readouterr().out
    assert "1 and 2" in out


def test_prompt_choice_prints_numbered_list(capsys):
    choices = ["one", "two"]
    with patch("builtins.input", return_value="1"):
        prompt_choice("Choose", choices)
    out = capsys.readouterr().out
    assert "1. one" in out
    assert "2. two" in out


# ── prompt_value ───────────────────────────────────────────────────────────────


def test_prompt_value_returns_user_input():
    with patch("builtins.input", return_value="myvalue"):
        result = prompt_value("Enter host")
    assert result == "myvalue"


def test_prompt_value_uses_default_on_empty():
    with patch("builtins.input", return_value=""):
        result = prompt_value("Enter host", default="localhost")
    assert result == "localhost"


def test_prompt_value_user_overrides_default():
    with patch("builtins.input", return_value="remotehost"):
        result = prompt_value("Enter host", default="localhost")
    assert result == "remotehost"


def test_prompt_value_required_reprompts_on_empty(capsys):
    inputs = iter(["", "", "filled"])
    with patch("builtins.input", side_effect=inputs):
        result = prompt_value("Enter name", required=True)
    assert result == "filled"
    out = capsys.readouterr().out
    assert "required" in out.lower()


def test_prompt_value_not_required_returns_empty():
    with patch("builtins.input", return_value=""):
        result = prompt_value("Optional field", required=False)
    assert result == ""


def test_prompt_value_shows_default_in_prompt():
    prompts_seen = []
    def fake_input(p):
        prompts_seen.append(p)
        return ""
    with patch("builtins.input", side_effect=fake_input):
        prompt_value("Host", default="localhost")
    assert "localhost" in prompts_seen[0]


# ── prompt_credential ──────────────────────────────────────────────────────────


def test_prompt_credential_returns_value(capsys):
    with patch("getpass.getpass", return_value="s3cr3t"):
        result = prompt_credential("Password", "${MY_CATALOG_PASSWORD}")
    assert result == "s3cr3t"


def test_prompt_credential_prints_env_var_tip(capsys):
    with patch("getpass.getpass", return_value=""):
        prompt_credential("Token", "${MY_TOKEN}")
    out = capsys.readouterr().out
    assert "${MY_TOKEN}" in out


def test_prompt_credential_uses_getpass_not_input():
    """Credential input must be masked (getpass), not plain input()."""
    with patch("getpass.getpass", return_value="secret") as mock_gp:
        with patch("builtins.input") as mock_input:
            result = prompt_credential("Password", "${ENV}")
    mock_gp.assert_called_once()
    mock_input.assert_not_called()
    assert result == "secret"


# ── prompt_confirm ─────────────────────────────────────────────────────────────


def test_prompt_confirm_yes():
    with patch("builtins.input", return_value="y"):
        assert prompt_confirm("Continue?") is True


def test_prompt_confirm_no():
    with patch("builtins.input", return_value="n"):
        assert prompt_confirm("Continue?") is False


def test_prompt_confirm_yes_full_word():
    with patch("builtins.input", return_value="yes"):
        assert prompt_confirm("Proceed?") is True


def test_prompt_confirm_no_full_word():
    with patch("builtins.input", return_value="no"):
        assert prompt_confirm("Proceed?") is False


def test_prompt_confirm_default_true_on_enter():
    with patch("builtins.input", return_value=""):
        assert prompt_confirm("Continue?", default=True) is True


def test_prompt_confirm_default_false_on_enter():
    with patch("builtins.input", return_value=""):
        assert prompt_confirm("Continue?", default=False) is False


def test_prompt_confirm_reprompts_on_invalid(capsys):
    inputs = iter(["maybe", "x", "y"])
    with patch("builtins.input", side_effect=inputs):
        result = prompt_confirm("Sure?")
    assert result is True
    out = capsys.readouterr().out
    assert "y" in out.lower() or "n" in out.lower()


def test_prompt_confirm_case_insensitive():
    with patch("builtins.input", return_value="Y"):
        assert prompt_confirm("Ok?") is True
    with patch("builtins.input", return_value="N"):
        assert prompt_confirm("Ok?") is False


# ── test_connection ────────────────────────────────────────────────────────────
# Task 5.1: Requirements 9.2, 9.3, 9.4, 9.6, 9.7, 13.7


def _make_plugin(*, instantiate_raises=None, list_tables_raises=None, list_tables_result=None, test_connection_raises=None):
    plugin = MagicMock()
    if instantiate_raises is not None:
        plugin.instantiate.side_effect = instantiate_raises
    else:
        plugin.instantiate.return_value = MagicMock()
    if list_tables_raises is not None:
        plugin.list_tables.side_effect = list_tables_raises
    else:
        plugin.list_tables.return_value = list_tables_result or []
    if test_connection_raises is not None:
        plugin.test_connection.side_effect = test_connection_raises
    return plugin


def test_test_connection_success():
    """Successful connection returns (True, elapsed, None)."""
    plugin = _make_plugin()
    success, elapsed, error = test_connection(plugin, "my_cat", {"host": "localhost"})
    assert success is True
    assert elapsed >= 0.0
    assert error is None


def test_test_connection_calls_instantiate_and_test_connection():
    """test_connection calls plugin.instantiate then plugin.test_connection."""
    plugin = _make_plugin()
    catalog = plugin.instantiate.return_value
    test_connection(plugin, "my_cat", {"host": "localhost"})
    plugin.instantiate.assert_called_once_with("my_cat", {"host": "localhost"})
    plugin.test_connection.assert_called_once_with(catalog)


def test_test_connection_instantiate_failure():
    """Exception from instantiate returns (False, elapsed, error_message)."""
    plugin = _make_plugin(instantiate_raises=ConnectionError("refused"))
    success, elapsed, error = test_connection(plugin, "my_cat", {})
    assert success is False
    assert elapsed >= 0.0
    assert error is not None
    assert "RVT-886" in error
    assert "refused" in error


def test_test_connection_test_connection_failure():
    """Exception from plugin.test_connection returns (False, elapsed, error_message)."""
    plugin = _make_plugin(test_connection_raises=RuntimeError("query failed"))
    success, elapsed, error = test_connection(plugin, "my_cat", {})
    assert success is False
    assert error is not None
    assert "RVT-886" in error
    assert "query failed" in error


def test_test_connection_timeout():
    """plugin.test_connection that hangs past timeout returns (False, elapsed, timeout_message)."""
    import threading

    barrier = threading.Event()

    def slow_test_connection(catalog):
        barrier.wait(timeout=10)  # blocks until test releases it

    plugin = _make_plugin()
    plugin.test_connection.side_effect = slow_test_connection

    try:
        success, elapsed, error = test_connection(plugin, "my_cat", {}, timeout=0.05)
    finally:
        barrier.set()  # unblock the thread so it can exit cleanly

    assert success is False
    assert error is not None
    assert "RVT-886" in error
    assert "timed out" in error.lower()


def test_test_connection_elapsed_is_positive_on_success():
    """Elapsed time is a non-negative float on success."""
    plugin = _make_plugin()
    _, elapsed, _ = test_connection(plugin, "my_cat", {})
    assert isinstance(elapsed, float)
    assert elapsed >= 0.0


def test_test_connection_elapsed_is_positive_on_failure():
    """Elapsed time is a non-negative float on failure."""
    plugin = _make_plugin(list_tables_raises=ValueError("oops"))
    _, elapsed, _ = test_connection(plugin, "my_cat", {})
    assert isinstance(elapsed, float)
    assert elapsed >= 0.0
