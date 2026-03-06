"""Tests for rivet_databricks.auth — shared credential resolution."""

from __future__ import annotations

import pytest

from rivet_core.errors import PluginValidationError
from rivet_databricks.auth import (
    AUTH_TYPE_AZURE_CLI,
    AUTH_TYPE_GCP_LOGIN,
    AUTH_TYPE_OAUTH_M2M,
    AUTH_TYPE_PAT,
    ResolvedCredential,
    resolve_credentials,
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Remove Databricks env vars so tests are isolated."""
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
    monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)


# Use a nonexistent config path by default to avoid reading real ~/.databrickscfg
_NO_CFG = "/tmp/_rivet_test_nonexistent_cfg"


# ── Step 1: Explicit options ──────────────────────────────────────────


class TestExplicitOptions:
    def test_pat_token(self, tmp_path):
        cred = resolve_credentials({"token": "dapi_abc123"}, config_path=tmp_path / "x")
        assert cred.auth_type == AUTH_TYPE_PAT
        assert cred.token == "dapi_abc123"
        assert cred.source == "explicit_options"

    def test_oauth_m2m(self, tmp_path):
        cred = resolve_credentials(
            {"client_id": "cid", "client_secret": "csec"}, config_path=tmp_path / "x"
        )
        assert cred.auth_type == AUTH_TYPE_OAUTH_M2M
        assert cred.client_id == "cid"
        assert cred.client_secret == "csec"
        assert cred.source == "explicit_options"

    def test_azure_entra_id(self, tmp_path):
        cred = resolve_credentials(
            {
                "azure_tenant_id": "tid",
                "azure_client_id": "acid",
                "azure_client_secret": "asec",
            },
            config_path=tmp_path / "x",
        )
        assert cred.auth_type == AUTH_TYPE_AZURE_CLI
        assert cred.azure_tenant_id == "tid"
        assert cred.azure_client_id == "acid"
        assert cred.azure_client_secret == "asec"
        assert cred.source == "explicit_options"

    def test_token_takes_precedence_over_oauth(self, tmp_path):
        cred = resolve_credentials(
            {"token": "tok", "client_id": "cid", "client_secret": "csec"},
            config_path=tmp_path / "x",
        )
        assert cred.auth_type == AUTH_TYPE_PAT
        assert cred.token == "tok"


# ── Partial credential rejection (RVT-205) ───────────────────────────


class TestPartialCredentials:
    def test_partial_oauth_missing_secret(self):
        with pytest.raises(PluginValidationError) as exc_info:
            resolve_credentials({"client_id": "cid"})
        assert exc_info.value.error.code == "RVT-205"
        assert "client_secret" in exc_info.value.error.message

    def test_partial_oauth_missing_id(self):
        with pytest.raises(PluginValidationError) as exc_info:
            resolve_credentials({"client_secret": "csec"})
        assert exc_info.value.error.code == "RVT-205"
        assert "client_id" in exc_info.value.error.message

    def test_partial_azure_missing_one(self):
        with pytest.raises(PluginValidationError) as exc_info:
            resolve_credentials({"azure_tenant_id": "tid", "azure_client_id": "acid"})
        assert exc_info.value.error.code == "RVT-205"
        assert "azure_client_secret" in exc_info.value.error.message

    def test_partial_azure_missing_two(self):
        with pytest.raises(PluginValidationError) as exc_info:
            resolve_credentials({"azure_tenant_id": "tid"})
        assert exc_info.value.error.code == "RVT-205"


# ── Step 2: Environment variables ─────────────────────────────────────


class TestEnvVars:
    def test_databricks_token_env(self, monkeypatch, tmp_path):
        monkeypatch.setenv("DATABRICKS_TOKEN", "env_tok")
        cred = resolve_credentials({}, config_path=tmp_path / "x")
        assert cred.auth_type == AUTH_TYPE_PAT
        assert cred.token == "env_tok"
        assert cred.source == "environment_variables"

    def test_oauth_env(self, monkeypatch, tmp_path):
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "env_cid")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "env_csec")
        cred = resolve_credentials({}, config_path=tmp_path / "x")
        assert cred.auth_type == AUTH_TYPE_OAUTH_M2M
        assert cred.client_id == "env_cid"
        assert cred.source == "environment_variables"

    def test_explicit_takes_precedence_over_env(self, monkeypatch, tmp_path):
        monkeypatch.setenv("DATABRICKS_TOKEN", "env_tok")
        cred = resolve_credentials({"token": "explicit_tok"}, config_path=tmp_path / "x")
        assert cred.token == "explicit_tok"
        assert cred.source == "explicit_options"


# ── Step 3: ~/.databrickscfg ──────────────────────────────────────────


class TestDatabricksCfg:
    def test_token_from_default_profile(self, tmp_path):
        cfg = tmp_path / ".databrickscfg"
        cfg.write_text("[DEFAULT]\ntoken = cfg_tok\nhost = https://example.cloud.databricks.com\n")
        cred = resolve_credentials({}, config_path=cfg)
        assert cred.auth_type == AUTH_TYPE_PAT
        assert cred.token == "cfg_tok"
        assert cred.source == "databrickscfg"

    def test_oauth_from_config(self, tmp_path):
        cfg = tmp_path / ".databrickscfg"
        cfg.write_text("[DEFAULT]\nclient_id = cfg_cid\nclient_secret = cfg_csec\n")
        cred = resolve_credentials({}, config_path=cfg)
        assert cred.auth_type == AUTH_TYPE_OAUTH_M2M
        assert cred.client_id == "cfg_cid"
        assert cred.source == "databrickscfg"

    def test_host_matching(self, tmp_path):
        cfg = tmp_path / ".databrickscfg"
        cfg.write_text(
            "[workspace1]\nhost = https://ws1.cloud.databricks.com\ntoken = ws1_tok\n\n"
            "[workspace2]\nhost = https://ws2.cloud.databricks.com\ntoken = ws2_tok\n"
        )
        cred = resolve_credentials(
            {}, host="https://ws2.cloud.databricks.com", config_path=cfg
        )
        assert cred.token == "ws2_tok"

    def test_missing_config_falls_through(self, tmp_path):
        """Missing config file is not an error — resolution continues to next step."""
        cfg = tmp_path / "nonexistent"
        # No env, no config, no auth_type → should fail with RVT-201
        with pytest.raises(PluginValidationError) as exc_info:
            resolve_credentials({}, config_path=cfg)
        assert exc_info.value.error.code == "RVT-201"

    def test_env_takes_precedence_over_config(self, tmp_path, monkeypatch):
        cfg = tmp_path / ".databrickscfg"
        cfg.write_text("[DEFAULT]\ntoken = cfg_tok\n")
        monkeypatch.setenv("DATABRICKS_TOKEN", "env_tok")
        cred = resolve_credentials({}, config_path=cfg)
        assert cred.token == "env_tok"
        assert cred.source == "environment_variables"


# ── Step 4: Cloud-native auth ─────────────────────────────────────────


class TestCloudNativeAuth:
    def test_azure_cli(self, tmp_path):
        cred = resolve_credentials({"auth_type": "azure_cli"}, config_path=tmp_path / "x")
        assert cred.auth_type == AUTH_TYPE_AZURE_CLI
        assert cred.source == "cloud_native_auth"

    def test_gcp_login(self, tmp_path):
        cred = resolve_credentials({"auth_type": "gcp_login"}, config_path=tmp_path / "x")
        assert cred.auth_type == AUTH_TYPE_GCP_LOGIN
        assert cred.source == "cloud_native_auth"

    def test_invalid_auth_type(self):
        with pytest.raises(PluginValidationError) as exc_info:
            resolve_credentials({"auth_type": "invalid"})
        assert exc_info.value.error.code == "RVT-201"
        assert "invalid" in exc_info.value.error.message


# ── No credentials resolved (RVT-201) ────────────────────────────────


class TestNoCredentials:
    def test_empty_options_no_env_no_config(self, tmp_path):
        cfg = tmp_path / "nonexistent"
        with pytest.raises(PluginValidationError) as exc_info:
            resolve_credentials({}, config_path=cfg)
        err = exc_info.value.error
        assert err.code == "RVT-201"
        assert "No credentials resolved" in err.message
        assert err.remediation is not None


# ── ResolvedCredential dataclass ──────────────────────────────────────


class TestResolvedCredential:
    def test_frozen(self):
        cred = ResolvedCredential(auth_type=AUTH_TYPE_PAT, token="tok")
        with pytest.raises(AttributeError):
            cred.token = "new"  # type: ignore[misc]

    def test_defaults(self):
        cred = ResolvedCredential(auth_type=AUTH_TYPE_PAT)
        assert cred.token is None
        assert cred.client_id is None
        assert cred.source == "unknown"
