"""Tests for EnvResolver (env.py)."""

from __future__ import annotations

import os

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_config.env import _CREDENTIAL_KEYS, ENV_PATTERN, resolve

# --- ENV_PATTERN ---

def test_env_pattern_matches_simple():
    assert ENV_PATTERN.search("${FOO}") is not None


def test_env_pattern_matches_underscore_start():
    assert ENV_PATTERN.search("${_VAR}") is not None


def test_env_pattern_no_match_digit_start():
    assert ENV_PATTERN.search("${1VAR}") is None


def test_env_pattern_no_match_plain():
    assert ENV_PATTERN.search("plain") is None


# --- resolve: basic substitution ---

def test_resolve_string_value(monkeypatch):
    monkeypatch.setenv("MY_VAR", "hello")
    result, errors, warnings = resolve({"key": "${MY_VAR}"})
    assert result == {"key": "hello"}
    assert errors == []
    assert warnings == []


def test_resolve_nested_dict(monkeypatch):
    monkeypatch.setenv("DB_HOST", "localhost")
    result, errors, warnings = resolve({"db": {"host": "${DB_HOST}"}})
    assert result["db"]["host"] == "localhost"
    assert errors == []


def test_resolve_list_values(monkeypatch):
    monkeypatch.setenv("A", "1")
    monkeypatch.setenv("B", "2")
    result, errors, warnings = resolve({"items": ["${A}", "${B}"]})
    assert result["items"] == ["1", "2"]
    assert errors == []


def test_resolve_non_string_passthrough():
    result, errors, warnings = resolve({"count": 42, "flag": True})
    assert result == {"count": 42, "flag": True}
    assert errors == []
    assert warnings == []


def test_resolve_partial_string(monkeypatch):
    monkeypatch.setenv("HOST", "db.example.com")
    result, errors, warnings = resolve({"url": "postgres://${HOST}/mydb"})
    assert result["url"] == "postgres://db.example.com/mydb"
    assert errors == []


# --- resolve: missing env var errors ---

def test_resolve_missing_var_produces_error(monkeypatch):
    monkeypatch.delenv("MISSING_VAR", raising=False)
    result, errors, warnings = resolve({"key": "${MISSING_VAR}"})
    assert len(errors) == 1
    assert "MISSING_VAR" in errors[0].message
    assert errors[0].remediation is not None


def test_resolve_missing_var_error_contains_path(monkeypatch):
    monkeypatch.delenv("MISSING_VAR", raising=False)
    _, errors, _ = resolve({"section": {"key": "${MISSING_VAR}"}})
    assert len(errors) == 1
    assert "section" in errors[0].message or "key" in errors[0].message


def test_resolve_collects_all_missing_errors(monkeypatch):
    monkeypatch.delenv("VAR_A", raising=False)
    monkeypatch.delenv("VAR_B", raising=False)
    _, errors, _ = resolve({"a": "${VAR_A}", "b": "${VAR_B}"})
    assert len(errors) == 2


# --- resolve: plaintext credential warnings ---

def test_resolve_plaintext_password_warns():
    _, errors, warnings = resolve({"password": "mysecret"})
    assert len(warnings) == 1
    assert errors == []


def test_resolve_plaintext_token_warns():
    _, errors, warnings = resolve({"api_token": "abc123"})
    assert len(warnings) == 1


def test_resolve_plaintext_secret_warns():
    _, errors, warnings = resolve({"client_secret": "xyz"})
    assert len(warnings) == 1


def test_resolve_plaintext_key_warns():
    _, errors, warnings = resolve({"access_key": "abc"})
    assert len(warnings) == 1


def test_resolve_plaintext_credential_warns():
    _, errors, warnings = resolve({"db_credential": "pass"})
    assert len(warnings) == 1


def test_resolve_env_ref_credential_no_warning(monkeypatch):
    monkeypatch.setenv("MY_PASSWORD", "secret")
    _, errors, warnings = resolve({"password": "${MY_PASSWORD}"})
    assert warnings == []
    assert errors == []


def test_resolve_non_credential_key_no_warning():
    _, errors, warnings = resolve({"host": "localhost"})
    assert warnings == []
    assert errors == []


def test_resolve_plaintext_credential_still_resolves():
    result, errors, warnings = resolve({"password": "mysecret"})
    assert result["password"] == "mysecret"
    assert len(warnings) == 1
    assert errors == []


# --- resolve: no ${} references ---

def test_resolve_no_refs_returns_unchanged():
    data = {"host": "localhost", "port": 5432}
    result, errors, warnings = resolve(data)
    assert result == data
    assert errors == []
    assert warnings == []


# --- Property 9: Environment variable resolution completeness ---
# Feature: rivet-config, Property 9: Environment variable resolution
# Validates: Requirements 6.1, 7.4

# Strategy: valid env var names (must match [A-Za-z_][A-Za-z0-9_]*)
_env_var_name = st.from_regex(r"[A-Za-z_][A-Za-z0-9_]{0,15}", fullmatch=True)

# Strategy: env var values (non-empty strings without null bytes)
_env_var_value = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",), blacklist_characters="\x00"),
    min_size=1,
    max_size=20,
)


def _has_unresolved_ref(data: object) -> bool:
    """Return True if any string in data still contains a ${...} pattern."""
    if isinstance(data, str):
        return bool(ENV_PATTERN.search(data))
    if isinstance(data, dict):
        return any(_has_unresolved_ref(v) for v in data.values())
    if isinstance(data, list):
        return any(_has_unresolved_ref(item) for item in data)
    return False


@st.composite
def _profile_with_refs(draw):
    """Generate (var_values dict, profile_data dict) where all ${VAR} refs are satisfiable."""
    var_names = draw(st.lists(_env_var_name, min_size=1, max_size=5, unique=True))
    var_values = {name: draw(_env_var_value) for name in var_names}

    plain_str = st.text(
        alphabet=st.characters(blacklist_categories=("Cs",), blacklist_characters="\x00$"),
        max_size=20,
    )
    ref_str = st.sampled_from([f"${{{n}}}" for n in var_names])
    leaf = st.one_of(plain_str, ref_str)

    keys = draw(st.lists(
        st.text(alphabet="abcdefghijklmnopqrstuvwxyz_", min_size=1, max_size=10),
        min_size=1,
        max_size=6,
        unique=True,
    ))
    profile_data = {k: draw(leaf) for k in keys}
    return var_values, profile_data


@given(drawn=_profile_with_refs())
@settings(max_examples=100)
def test_property9_resolution_completeness(drawn):
    """Property 9: After resolve(), no ${...} references remain when all vars are set."""
    var_values, profile_data = drawn

    original = {name: os.environ.get(name) for name in var_values}
    for name, value in var_values.items():
        os.environ[name] = value

    try:
        resolved, errors, _ = resolve(profile_data)

        # All referenced vars were set → no errors expected
        assert errors == [], f"Unexpected errors: {errors}"

        # No unresolved ${...} patterns should remain
        assert not _has_unresolved_ref(resolved), (
            f"Unresolved references remain in: {resolved}"
        )
    finally:
        for name, orig in original.items():
            if orig is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = orig


# --- Property 10: Missing environment variable produces error with context ---

# Strategy: simple string keys (no dots to keep path parsing simple)
_simple_key = st.from_regex(r"[a-z][a-z0-9]{0,10}", fullmatch=True)


@given(var_name=_env_var_name, key=_simple_key)
@settings(max_examples=100)
def test_property_missing_var_error_contains_var_name_and_path(var_name, key):
    """Feature: rivet-config, Property 10: Missing environment variable produces error with context.

    For any ${VAR} reference where VAR is not set, the error must include
    both the variable name and the config path where the reference appears.
    """
    clean_env = {k: v for k, v in os.environ.items() if k != var_name}
    original = os.environ.copy()
    os.environ.clear()
    os.environ.update(clean_env)
    try:
        _, errors, _ = resolve({key: f"${{{var_name}}}"})
    finally:
        os.environ.clear()
        os.environ.update(original)
    assert len(errors) == 1
    assert var_name in errors[0].message
    assert key in errors[0].message


@given(
    var_names=st.lists(_env_var_name, min_size=2, max_size=5, unique=True),
    keys=st.lists(_simple_key, min_size=2, max_size=5, unique=True),
)
@settings(max_examples=100)
def test_property_missing_vars_all_produce_errors(var_names, keys):
    """Feature: rivet-config, Property 10 (multi-var): every missing variable produces its own error."""
    pairs = list(zip(keys, var_names))
    clean_env = {k: v for k, v in os.environ.items() if k not in var_names}
    original = os.environ.copy()
    os.environ.clear()
    os.environ.update(clean_env)
    try:
        data = {k: f"${{{v}}}" for k, v in pairs}
        _, errors, _ = resolve(data)
    finally:
        os.environ.clear()
        os.environ.update(original)
    assert len(errors) == len(pairs)
    error_messages = " ".join(e.message for e in errors)
    for _, var in pairs:
        assert var in error_messages


# --- Property 11: Plaintext credential warning does not block resolution ---

# Strategy: generate a credential key (contains one of the credential keywords)
_credential_key_st = st.one_of(
    *[st.just(ck) for ck in _CREDENTIAL_KEYS],
    *[st.builds(lambda prefix, ck: f"{prefix}_{ck}", st.from_regex(r"[a-z]+", fullmatch=True), st.just(ck)) for ck in _CREDENTIAL_KEYS],
)

# Strategy: plaintext value — any non-empty string that is NOT a ${...} reference
_plaintext_value_st = st.text(
    alphabet=st.characters(blacklist_characters="${}", min_codepoint=32),
    min_size=1,
).filter(lambda s: not ENV_PATTERN.fullmatch(s))


@given(cred_key=_credential_key_st, plaintext=_plaintext_value_st)
@settings(max_examples=100)
def test_property_plaintext_credential_warning_does_not_block_resolution(cred_key, plaintext):
    """Feature: rivet-config, Property 11: Plaintext credential warning does not block resolution.

    For any profile containing plaintext credential values (not ${...} references),
    the resolver should emit a warning but still produce a valid resolved dict
    with the plaintext values intact.

    Validates: Requirements 6.5, 6.6
    """
    data = {cred_key: plaintext}
    result, errors, warnings = resolve(data)

    # Resolution must succeed (no errors from plaintext credentials)
    assert errors == [], f"Expected no errors for plaintext credential, got: {errors}"

    # The plaintext value must be preserved intact in the result
    assert result[cred_key] == plaintext, (
        f"Expected plaintext value to be preserved, got: {result[cred_key]!r}"
    )

    # A warning must be emitted
    assert len(warnings) >= 1, "Expected at least one warning for plaintext credential"
    assert any(cred_key in w.message or "plaintext" in w.message.lower() for w in warnings), (
        f"Expected warning to mention the credential key or 'plaintext', got: {[w.message for w in warnings]}"
    )
