"""Tests for UnityCatalogPlugin (tasks 22.1, 22.3)."""

from __future__ import annotations

from datetime import UTC

import pytest

from rivet_core.errors import PluginValidationError
from rivet_core.models import Catalog
from rivet_core.plugins import CatalogPlugin
from rivet_databricks.auth import (
    AUTH_TYPE_AZURE_CLI,
    AUTH_TYPE_GCP_LOGIN,
    AUTH_TYPE_OAUTH_M2M,
    AUTH_TYPE_PAT,
)
from rivet_databricks.unity_catalog import UnityCatalogPlugin

_VALID_OPTIONS = {"host": "https://my.databricks.com", "catalog_name": "prod"}


def test_catalog_type():
    assert UnityCatalogPlugin().type == "unity"


def test_is_catalog_plugin():
    assert isinstance(UnityCatalogPlugin(), CatalogPlugin)


def test_required_options():
    plugin = UnityCatalogPlugin()
    assert "host" in plugin.required_options
    assert "catalog_name" in plugin.required_options


def test_credential_options():
    plugin = UnityCatalogPlugin()
    assert "token" in plugin.credential_options
    assert "client_id" in plugin.credential_options
    assert "client_secret" in plugin.credential_options


def test_credential_groups():
    plugin = UnityCatalogPlugin()
    assert "pat" in plugin.credential_groups
    assert "oauth_m2m" in plugin.credential_groups
    assert "azure_cli" in plugin.credential_groups
    assert "gcp_login" in plugin.credential_groups
    assert plugin.credential_groups["pat"] == ["token"]
    assert plugin.credential_groups["oauth_m2m"] == ["client_id", "client_secret"]
    assert plugin.credential_groups["azure_cli"] == []
    assert plugin.credential_groups["gcp_login"] == []


def test_validate_accepts_valid_options():
    UnityCatalogPlugin().validate(_VALID_OPTIONS)  # should not raise


def test_validate_rejects_missing_host():
    opts = {k: v for k, v in _VALID_OPTIONS.items() if k != "host"}
    with pytest.raises(PluginValidationError) as exc_info:
        UnityCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-201"
    assert "host" in exc_info.value.error.message


def test_validate_rejects_missing_catalog_name():
    opts = {k: v for k, v in _VALID_OPTIONS.items() if k != "catalog_name"}
    with pytest.raises(PluginValidationError) as exc_info:
        UnityCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-201"
    assert "catalog_name" in exc_info.value.error.message


def test_validate_rejects_unknown_option():
    opts = {**_VALID_OPTIONS, "unknown_key": "value"}
    with pytest.raises(PluginValidationError) as exc_info:
        UnityCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-201"
    assert "unknown_key" in exc_info.value.error.message


def test_validate_accepts_optional_schema():
    opts = {**_VALID_OPTIONS, "schema": "my_schema"}
    UnityCatalogPlugin().validate(opts)  # should not raise


def test_validate_accepts_credential_options():
    opts = {**_VALID_OPTIONS, "token": "dapi123", "auth_type": "pat"}
    UnityCatalogPlugin().validate(opts)  # should not raise


def test_validate_accepts_all_auth_types():
    for auth_type in ("pat", "oauth_m2m", "azure_cli", "gcp_login"):
        opts = {**_VALID_OPTIONS, "auth_type": auth_type}
        UnityCatalogPlugin().validate(opts)  # should not raise


def test_validate_rejects_invalid_auth_type():
    opts = {**_VALID_OPTIONS, "auth_type": "invalid_auth"}
    with pytest.raises(PluginValidationError) as exc_info:
        UnityCatalogPlugin().validate(opts)
    assert exc_info.value.error.code == "RVT-201"
    assert "auth_type" in exc_info.value.error.message


def test_validate_accepts_oauth_m2m_credentials():
    opts = {**_VALID_OPTIONS, "client_id": "my-client", "client_secret": "my-secret", "auth_type": "oauth_m2m"}
    UnityCatalogPlugin().validate(opts)  # should not raise


def test_validate_accepts_token_without_auth_type():
    opts = {**_VALID_OPTIONS, "token": "dapi123"}
    UnityCatalogPlugin().validate(opts)  # should not raise


def test_instantiate_returns_catalog():
    catalog = UnityCatalogPlugin().instantiate("my_unity", _VALID_OPTIONS)
    assert isinstance(catalog, Catalog)
    assert catalog.name == "my_unity"
    assert catalog.type == "unity"


def test_default_table_reference_uses_catalog_schema():
    plugin = UnityCatalogPlugin()
    ref = plugin.default_table_reference("users", {"catalog_name": "prod", "schema": "default"})
    assert ref == "prod.default.users"


def test_default_table_reference_custom_schema():
    plugin = UnityCatalogPlugin()
    ref = plugin.default_table_reference("orders", {"catalog_name": "dev", "schema": "sales"})
    assert ref == "dev.sales.orders"


# ── Task 22.3: Credential resolution ─────────────────────────────────


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Remove Databricks env vars so tests are isolated."""
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
    monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)


class TestCredentialResolution:
    _BASE = {"host": "https://my.databricks.com", "catalog_name": "prod"}

    def test_explicit_token(self, tmp_path):
        opts = {**self._BASE, "token": "dapi_abc"}
        cred = UnityCatalogPlugin().resolve_credentials(opts, config_path=tmp_path / "x")
        assert cred.auth_type == AUTH_TYPE_PAT
        assert cred.token == "dapi_abc"
        assert cred.source == "explicit_options"

    def test_explicit_oauth_m2m(self, tmp_path):
        opts = {**self._BASE, "client_id": "cid", "client_secret": "csec"}
        cred = UnityCatalogPlugin().resolve_credentials(opts, config_path=tmp_path / "x")
        assert cred.auth_type == AUTH_TYPE_OAUTH_M2M
        assert cred.source == "explicit_options"

    def test_env_databricks_token(self, monkeypatch, tmp_path):
        monkeypatch.setenv("DATABRICKS_TOKEN", "env_tok")
        cred = UnityCatalogPlugin().resolve_credentials(self._BASE, config_path=tmp_path / "x")
        assert cred.auth_type == AUTH_TYPE_PAT
        assert cred.token == "env_tok"
        assert cred.source == "environment_variables"

    def test_databrickscfg(self, tmp_path):
        cfg = tmp_path / ".databrickscfg"
        cfg.write_text("[DEFAULT]\ntoken = cfg_tok\n")
        cred = UnityCatalogPlugin().resolve_credentials(self._BASE, config_path=cfg)
        assert cred.auth_type == AUTH_TYPE_PAT
        assert cred.token == "cfg_tok"
        assert cred.source == "databrickscfg"

    def test_cloud_native_azure_cli(self, tmp_path):
        opts = {**self._BASE, "auth_type": "azure_cli"}
        cred = UnityCatalogPlugin().resolve_credentials(opts, config_path=tmp_path / "x")
        assert cred.auth_type == AUTH_TYPE_AZURE_CLI
        assert cred.source == "cloud_native_auth"

    def test_cloud_native_gcp_login(self, tmp_path):
        opts = {**self._BASE, "auth_type": "gcp_login"}
        cred = UnityCatalogPlugin().resolve_credentials(opts, config_path=tmp_path / "x")
        assert cred.auth_type == AUTH_TYPE_GCP_LOGIN
        assert cred.source == "cloud_native_auth"

    def test_no_credentials_raises_rvt201(self, tmp_path):
        with pytest.raises(PluginValidationError) as exc_info:
            UnityCatalogPlugin().resolve_credentials(self._BASE, config_path=tmp_path / "x")
        assert exc_info.value.error.code == "RVT-201"

    def test_partial_oauth_raises_rvt205(self):
        opts = {**self._BASE, "client_id": "cid"}
        with pytest.raises(PluginValidationError) as exc_info:
            UnityCatalogPlugin().resolve_credentials(opts)
        assert exc_info.value.error.code == "RVT-205"

    def test_explicit_takes_precedence_over_env(self, monkeypatch, tmp_path):
        monkeypatch.setenv("DATABRICKS_TOKEN", "env_tok")
        opts = {**self._BASE, "token": "explicit_tok"}
        cred = UnityCatalogPlugin().resolve_credentials(opts, config_path=tmp_path / "x")
        assert cred.token == "explicit_tok"
        assert cred.source == "explicit_options"

    def test_env_takes_precedence_over_cfg(self, monkeypatch, tmp_path):
        cfg = tmp_path / ".databrickscfg"
        cfg.write_text("[DEFAULT]\ntoken = cfg_tok\n")
        monkeypatch.setenv("DATABRICKS_TOKEN", "env_tok")
        cred = UnityCatalogPlugin().resolve_credentials(self._BASE, config_path=cfg)
        assert cred.token == "env_tok"
        assert cred.source == "environment_variables"


# ── Task 22.4: resolve_table_reference via REST API with per-table caching ───


class TestResolveTableReference:
    _BASE_OPTIONS = {"host": "https://my.databricks.com", "catalog_name": "prod", "token": "dapi123"}

    def _make_catalog(self, options=None):
        opts = options or self._BASE_OPTIONS
        return UnityCatalogPlugin().instantiate("my_unity", opts)

    def _raw_table_response(self):
        return {
            "name": "users",
            "full_name": "prod.default.users",
            "storage_location": "s3://my-bucket/prod/default/users",
            "data_source_format": "DELTA",
            "columns": [{"name": "id", "type_text": "bigint"}, {"name": "name", "type_text": "string"}],
            "partition_columns": ["id"],
            "table_type": "EXTERNAL",
        }

    def test_returns_required_fields(self, monkeypatch):
        raw = self._raw_table_response()
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, full_name: raw,
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.vend_credentials", lambda self, tid, operation="READ": None)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = UnityCatalogPlugin()
        catalog = self._make_catalog()
        result = plugin.resolve_table_reference("prod.default.users", catalog)

        assert result["storage_location"] == "s3://my-bucket/prod/default/users"
        assert result["file_format"] == "DELTA"
        assert result["columns"] == raw["columns"]
        assert result["partition_columns"] == ["id"]
        assert result["table_type"] == "EXTERNAL"
        assert "temporary_credentials" in result

    def test_caches_result_on_second_call(self, monkeypatch):
        raw = self._raw_table_response()
        call_count = {"n": 0}

        def fake_get_table(self, full_name):
            call_count["n"] += 1
            return raw

        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", fake_get_table)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.vend_credentials", lambda self, tid, operation="READ": None)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = UnityCatalogPlugin()
        catalog = self._make_catalog()

        plugin.resolve_table_reference("prod.default.users", catalog)
        plugin.resolve_table_reference("prod.default.users", catalog)

        assert call_count["n"] == 1  # API called only once

    def test_cache_is_per_table(self, monkeypatch):
        raw = self._raw_table_response()
        call_count = {"n": 0}

        def fake_get_table(self, full_name):
            call_count["n"] += 1
            return {**raw, "full_name": full_name}

        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", fake_get_table)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.vend_credentials", lambda self, tid, operation="READ": None)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = UnityCatalogPlugin()
        catalog = self._make_catalog()

        plugin.resolve_table_reference("prod.default.users", catalog)
        plugin.resolve_table_reference("prod.default.orders", catalog)

        assert call_count["n"] == 2  # Different tables → 2 API calls

    def test_cache_is_per_host(self, monkeypatch):
        raw = self._raw_table_response()
        call_count = {"n": 0}

        def fake_get_table(self, full_name):
            call_count["n"] += 1
            return raw

        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", fake_get_table)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.vend_credentials", lambda self, tid, operation="READ": None)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = UnityCatalogPlugin()
        catalog_a = plugin.instantiate("a", {**self._BASE_OPTIONS, "host": "https://host-a.databricks.com"})
        catalog_b = plugin.instantiate("b", {**self._BASE_OPTIONS, "host": "https://host-b.databricks.com"})

        plugin.resolve_table_reference("prod.default.users", catalog_a)
        plugin.resolve_table_reference("prod.default.users", catalog_b)

        assert call_count["n"] == 2  # Different hosts → 2 API calls

    def test_missing_optional_fields_default_to_empty(self, monkeypatch):
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, full_name: {"table_type": "MANAGED"},
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.vend_credentials", lambda self, tid, operation="READ": None)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = UnityCatalogPlugin()
        catalog = self._make_catalog()
        result = plugin.resolve_table_reference("prod.default.t", catalog)

        assert result["storage_location"] is None
        assert result["file_format"] is None
        assert result["columns"] == []
        assert result["partition_columns"] == []
        assert result["table_type"] == "MANAGED"
        assert result["temporary_credentials"] is None


# ── Task 22.5: Credential vending via POST /temporary-table-credentials ──


class TestCredentialVending:
    _BASE_OPTIONS = {"host": "https://my.databricks.com", "catalog_name": "prod", "token": "dapi123"}

    def _make_catalog(self, options=None):
        opts = options or self._BASE_OPTIONS
        return UnityCatalogPlugin().instantiate("my_unity", opts)

    def _raw_table_response(self, table_id="abc-123"):
        return {
            "name": "users",
            "full_name": "prod.default.users",
            "table_id": table_id,
            "storage_location": "s3://my-bucket/prod/default/users",
            "data_source_format": "DELTA",
            "columns": [],
            "partition_columns": [],
            "table_type": "EXTERNAL",
        }

    def _vend_response(self):
        return {
            "aws_temp_credentials": {
                "access_key_id": "AKIA...",
                "secret_access_key": "secret...",
                "session_token": "token...",
            },
            "expiration_time": "2026-02-28T10:00:00Z",
        }

    def test_resolve_table_reference_populates_temporary_credentials(self, monkeypatch):
        """resolve_table_reference calls vend_credentials and stores result."""
        raw = self._raw_table_response()
        vend = self._vend_response()

        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", lambda self, fn: raw)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.vend_credentials", lambda self, tid, operation="READ": vend)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = UnityCatalogPlugin()
        catalog = self._make_catalog()
        result = plugin.resolve_table_reference("prod.default.users", catalog)

        assert result["temporary_credentials"] == vend

    def test_resolve_table_reference_uses_table_id_for_vending(self, monkeypatch):
        """vend_credentials is called with the table_id from the API response."""
        raw = self._raw_table_response(table_id="my-table-id-xyz")
        vend = self._vend_response()
        vend_calls = []

        def fake_vend(self, table_id, operation="READ"):
            vend_calls.append((table_id, operation))
            return vend

        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", lambda self, fn: raw)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.vend_credentials", fake_vend)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = UnityCatalogPlugin()
        catalog = self._make_catalog()
        plugin.resolve_table_reference("prod.default.users", catalog)

        assert len(vend_calls) == 1
        assert vend_calls[0][0] == "my-table-id-xyz"
        assert vend_calls[0][1] == "READ"

    def test_resolve_table_reference_falls_back_on_403(self, monkeypatch):
        """When credential vending returns HTTP 403 (RVT-508), temporary_credentials is None."""
        from rivet_core.errors import ExecutionError, RivetError

        raw = self._raw_table_response()

        def fake_vend(self, table_id, operation="READ"):
            raise ExecutionError(
                RivetError(
                    code="RVT-508",
                    message="Credential vending denied (HTTP 403).",
                    context={},
                    remediation="Enable credential vending.",
                )
            )

        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", lambda self, fn: raw)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.vend_credentials", fake_vend)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = UnityCatalogPlugin()
        catalog = self._make_catalog()

        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = plugin.resolve_table_reference("prod.default.users", catalog)

        assert result["temporary_credentials"] is None
        assert len(w) == 1
        assert "Credential vending unavailable" in str(w[0].message)

    def test_resolve_table_reference_propagates_non_508_errors(self, monkeypatch):
        """Non-RVT-508 ExecutionErrors from vend_credentials are re-raised."""
        from rivet_core.errors import ExecutionError, RivetError

        raw = self._raw_table_response()

        def fake_vend(self, table_id, operation="READ"):
            raise ExecutionError(
                RivetError(
                    code="RVT-502",
                    message="Auth failed.",
                    context={},
                    remediation="Check credentials.",
                )
            )

        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", lambda self, fn: raw)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.vend_credentials", fake_vend)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = UnityCatalogPlugin()
        catalog = self._make_catalog()

        with pytest.raises(ExecutionError) as exc_info:
            plugin.resolve_table_reference("prod.default.users", catalog)
        assert exc_info.value.error.code == "RVT-502"

    def test_vend_credentials_method_returns_credentials(self, monkeypatch):
        """Standalone vend_credentials method returns vended credentials."""
        raw = self._raw_table_response()
        vend = self._vend_response()

        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", lambda self, fn: raw)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.vend_credentials", lambda self, tid, operation="READ": vend)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = UnityCatalogPlugin()
        catalog = self._make_catalog()
        result = plugin.vend_credentials("prod.default.users", catalog, operation="READ")

        assert result == vend

    def test_vend_credentials_method_returns_none_on_403(self, monkeypatch):
        """Standalone vend_credentials returns None when vending is unavailable."""
        from rivet_core.errors import ExecutionError, RivetError

        raw = self._raw_table_response()

        def fake_vend(self, table_id, operation="READ"):
            raise ExecutionError(
                RivetError(
                    code="RVT-508",
                    message="Credential vending denied (HTTP 403).",
                    context={},
                    remediation="Enable credential vending.",
                )
            )

        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", lambda self, fn: raw)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.vend_credentials", fake_vend)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = UnityCatalogPlugin()
        catalog = self._make_catalog()

        import warnings
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result = plugin.vend_credentials("prod.default.users", catalog)

        assert result is None

    def test_vend_credentials_scoped_to_table(self, monkeypatch):
        """Credential vending is scoped to the specific table (Property 22)."""
        raw_users = {**self._raw_table_response(table_id="id-users"), "full_name": "prod.default.users"}
        raw_orders = {**self._raw_table_response(table_id="id-orders"), "full_name": "prod.default.orders"}
        vend_calls = []

        def fake_get_table(self, full_name):
            return raw_users if "users" in full_name else raw_orders

        def fake_vend(self, table_id, operation="READ"):
            vend_calls.append(table_id)
            return {"table_id": table_id}

        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", fake_get_table)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.vend_credentials", fake_vend)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = UnityCatalogPlugin()
        catalog = self._make_catalog()

        plugin.resolve_table_reference("prod.default.users", catalog)
        plugin.resolve_table_reference("prod.default.orders", catalog)

        assert "id-users" in vend_calls
        assert "id-orders" in vend_calls
        assert vend_calls[0] != vend_calls[1]

    def test_fallback_uses_full_name_when_no_table_id(self, monkeypatch):
        """When table_id is absent, falls back to full_name for vending."""
        raw = {
            "full_name": "prod.default.users",
            "storage_location": "s3://bucket/path",
            "data_source_format": "DELTA",
            "table_type": "EXTERNAL",
        }
        vend_calls = []

        def fake_vend(self, table_id, operation="READ"):
            vend_calls.append(table_id)
            return {"vended": True}

        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", lambda self, fn: raw)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.vend_credentials", fake_vend)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = UnityCatalogPlugin()
        catalog = self._make_catalog()
        plugin.resolve_table_reference("prod.default.users", catalog)

        assert vend_calls[0] == "prod.default.users"


# ── Task 22.6: Introspection — 4-level hierarchy ──────────────────────────────


class TestListTables:
    _BASE_OPTIONS = {"host": "https://my.databricks.com", "catalog_name": "prod", "token": "dapi123"}

    def _make_catalog(self):
        return UnityCatalogPlugin().instantiate("my_unity", self._BASE_OPTIONS)

    def test_returns_catalog_schema_table_nodes(self, monkeypatch):
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_catalogs",
            lambda self: [{"name": "prod", "owner": "alice", "comment": "main"}],
        )
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_schemas",
            lambda self, cat: [{"name": "default", "owner": "bob", "comment": None}],
        )
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_tables",
            lambda self, cat, schema: [
                {"name": "users", "data_source_format": "DELTA", "owner": "carol", "comment": "user table"}
            ],
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        plugin = UnityCatalogPlugin()
        catalog = self._make_catalog()
        nodes = plugin.list_tables(catalog)

        node_types = [n.node_type for n in nodes]
        assert "catalog" in node_types
        assert "schema" in node_types
        assert "table" in node_types

    def test_catalog_node_has_correct_path(self, monkeypatch):
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_catalogs",
            lambda self: [{"name": "prod"}],
        )
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_schemas",
            lambda self, cat: [],
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        nodes = UnityCatalogPlugin().list_tables(self._make_catalog())
        cat_node = next(n for n in nodes if n.node_type == "catalog")
        assert cat_node.path == ["prod"]
        assert cat_node.is_container is True

    def test_schema_node_has_correct_path(self, monkeypatch):
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_catalogs",
            lambda self: [{"name": "prod"}],
        )
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_schemas",
            lambda self, cat: [{"name": "default"}],
        )
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_tables",
            lambda self, cat, schema: [],
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        nodes = UnityCatalogPlugin().list_tables(self._make_catalog())
        schema_node = next(n for n in nodes if n.node_type == "schema")
        assert schema_node.path == ["prod", "default"]
        assert schema_node.is_container is True

    def test_table_node_has_correct_path(self, monkeypatch):
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_catalogs",
            lambda self: [{"name": "prod"}],
        )
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_schemas",
            lambda self, cat: [{"name": "default"}],
        )
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_tables",
            lambda self, cat, schema: [{"name": "users", "data_source_format": "DELTA"}],
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        nodes = UnityCatalogPlugin().list_tables(self._make_catalog())
        table_node = next(n for n in nodes if n.node_type == "table")
        assert table_node.path == ["prod", "default", "users"]
        assert table_node.is_container is False

    def test_table_node_summary_has_format(self, monkeypatch):
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_catalogs",
            lambda self: [{"name": "prod"}],
        )
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_schemas",
            lambda self, cat: [{"name": "default"}],
        )
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_tables",
            lambda self, cat, schema: [{"name": "users", "data_source_format": "PARQUET", "owner": "alice"}],
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        nodes = UnityCatalogPlugin().list_tables(self._make_catalog())
        table_node = next(n for n in nodes if n.node_type == "table")
        assert table_node.summary.format == "PARQUET"
        assert table_node.summary.owner == "alice"

    def test_children_count_on_catalog_node(self, monkeypatch):
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_catalogs",
            lambda self: [{"name": "prod"}],
        )
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_schemas",
            lambda self, cat: [{"name": "s1"}, {"name": "s2"}],
        )
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_tables",
            lambda self, cat, schema: [],
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        nodes = UnityCatalogPlugin().list_tables(self._make_catalog())
        cat_node = next(n for n in nodes if n.node_type == "catalog")
        assert cat_node.children_count == 2

    def test_empty_catalog_returns_only_catalog_node(self, monkeypatch):
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_catalogs",
            lambda self: [{"name": "empty_cat"}],
        )
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_schemas",
            lambda self, cat: [],
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        nodes = UnityCatalogPlugin().list_tables(self._make_catalog())
        assert len(nodes) == 1
        assert nodes[0].node_type == "catalog"

    def test_read_only_no_mutations(self, monkeypatch):
        """list_tables must not call any mutating API methods."""
        mutating_calls = []

        def fake_create_table(self, *a, **kw):
            mutating_calls.append("create_table")

        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.list_catalogs",
            lambda self: [],
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.create_table", fake_create_table)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        UnityCatalogPlugin().list_tables(self._make_catalog())
        assert mutating_calls == []


class TestGetSchema:
    _BASE_OPTIONS = {"host": "https://my.databricks.com", "catalog_name": "prod", "token": "dapi123"}

    def _make_catalog(self):
        return UnityCatalogPlugin().instantiate("my_unity", self._BASE_OPTIONS)

    def _raw_table(self):
        return {
            "name": "users",
            "full_name": "prod.default.users",
            "table_type": "EXTERNAL",
            "comment": "user table",
            "columns": [
                {"name": "id", "type_text": "bigint", "nullable": False},
                {"name": "name", "type_text": "string", "nullable": True},
                {"name": "score", "type_text": "double", "nullable": True},
                {"name": "active", "type_text": "boolean", "nullable": True},
            ],
            "partition_columns": [{"name": "id"}],
        }

    def test_returns_object_schema(self, monkeypatch):
        from rivet_core.introspection import ObjectSchema

        raw = self._raw_table()
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, fn: raw,
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        result = UnityCatalogPlugin().get_schema(self._make_catalog(), "prod.default.users")
        assert isinstance(result, ObjectSchema)

    def test_columns_mapped_to_arrow_types(self, monkeypatch):
        raw = self._raw_table()
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, fn: raw,
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        result = UnityCatalogPlugin().get_schema(self._make_catalog(), "prod.default.users")
        col_map = {c.name: c.type for c in result.columns}
        assert col_map["id"] == "int64"
        assert col_map["name"] == "large_utf8"
        assert col_map["score"] == "float64"
        assert col_map["active"] == "bool"

    def test_partition_key_flagged(self, monkeypatch):
        raw = self._raw_table()
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, fn: raw,
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        result = UnityCatalogPlugin().get_schema(self._make_catalog(), "prod.default.users")
        id_col = next(c for c in result.columns if c.name == "id")
        name_col = next(c for c in result.columns if c.name == "name")
        assert id_col.is_partition_key is True
        assert name_col.is_partition_key is False

    def test_nullable_preserved(self, monkeypatch):
        raw = self._raw_table()
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, fn: raw,
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        result = UnityCatalogPlugin().get_schema(self._make_catalog(), "prod.default.users")
        id_col = next(c for c in result.columns if c.name == "id")
        name_col = next(c for c in result.columns if c.name == "name")
        assert id_col.nullable is False
        assert name_col.nullable is True

    def test_path_from_table_name(self, monkeypatch):
        raw = self._raw_table()
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, fn: raw,
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        result = UnityCatalogPlugin().get_schema(self._make_catalog(), "prod.default.users")
        assert result.path == ["prod", "default", "users"]

    def test_comment_preserved(self, monkeypatch):
        raw = self._raw_table()
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, fn: raw,
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        result = UnityCatalogPlugin().get_schema(self._make_catalog(), "prod.default.users")
        assert result.comment == "user table"

    def test_unknown_type_defaults_to_large_utf8_with_warning(self, monkeypatch):
        raw = {
            "name": "t",
            "table_type": "EXTERNAL",
            "columns": [{"name": "x", "type_text": "map<string,int>", "nullable": True}],
            "partition_columns": [],
        }
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, fn: raw,
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        import warnings as _warnings
        with _warnings.catch_warnings(record=True) as w:
            _warnings.simplefilter("always")
            result = UnityCatalogPlugin().get_schema(self._make_catalog(), "prod.default.t")

        assert result.columns[0].type == "large_utf8"
        assert any("no Arrow mapping" in str(warning.message) for warning in w)

    def test_native_type_preserved(self, monkeypatch):
        raw = {
            "name": "t",
            "table_type": "EXTERNAL",
            "columns": [{"name": "x", "type_text": "decimal(10,2)", "nullable": True}],
            "partition_columns": [],
        }
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, fn: raw,
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        result = UnityCatalogPlugin().get_schema(self._make_catalog(), "prod.default.t")
        assert result.columns[0].native_type == "decimal(10,2)"
        assert result.columns[0].type == "float64"

    def test_read_only_no_mutations(self, monkeypatch):
        """get_schema must not call any mutating API methods."""
        mutating_calls = []

        def fake_create_table(self, *a, **kw):
            mutating_calls.append("create_table")

        raw = self._raw_table()
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, fn: raw,
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.create_table", fake_create_table)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        UnityCatalogPlugin().get_schema(self._make_catalog(), "prod.default.users")
        assert mutating_calls == []


class TestGetMetadata:
    _BASE_OPTIONS = {"host": "https://my.databricks.com", "catalog_name": "prod", "token": "dapi123"}

    def _make_catalog(self):
        return UnityCatalogPlugin().instantiate("my_unity", self._BASE_OPTIONS)

    def _raw_table(self):
        return {
            "name": "users",
            "full_name": "prod.default.users",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "storage_location": "s3://bucket/prod/default/users",
            "owner": "alice",
            "comment": "user table",
            "updated_at": 1709118000000,
            "created_at": 1709000000000,
            "properties": {"numRows": "1000", "sizeInBytes": "512000"},
        }

    def test_returns_object_metadata(self, monkeypatch):
        from rivet_core.introspection import ObjectMetadata

        raw = self._raw_table()
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, fn: raw,
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        result = UnityCatalogPlugin().get_metadata(self._make_catalog(), "prod.default.users")
        assert isinstance(result, ObjectMetadata)

    def test_format_from_data_source_format(self, monkeypatch):
        raw = self._raw_table()
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, fn: raw,
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        result = UnityCatalogPlugin().get_metadata(self._make_catalog(), "prod.default.users")
        assert result.format == "DELTA"

    def test_location_from_storage_location(self, monkeypatch):
        raw = self._raw_table()
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, fn: raw,
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        result = UnityCatalogPlugin().get_metadata(self._make_catalog(), "prod.default.users")
        assert result.location == "s3://bucket/prod/default/users"

    def test_owner_and_comment(self, monkeypatch):
        raw = self._raw_table()
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, fn: raw,
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        result = UnityCatalogPlugin().get_metadata(self._make_catalog(), "prod.default.users")
        assert result.owner == "alice"
        assert result.comment == "user table"

    def test_updated_at_parsed_from_epoch_ms(self, monkeypatch):

        raw = self._raw_table()
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, fn: raw,
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        result = UnityCatalogPlugin().get_metadata(self._make_catalog(), "prod.default.users")
        assert result.last_modified is not None
        assert result.last_modified.tzinfo == UTC

    def test_node_type_from_table_type(self, monkeypatch):
        raw = self._raw_table()
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, fn: raw,
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        result = UnityCatalogPlugin().get_metadata(self._make_catalog(), "prod.default.users")
        assert result.node_type == "external"

    def test_path_from_table_name(self, monkeypatch):
        raw = self._raw_table()
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, fn: raw,
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        result = UnityCatalogPlugin().get_metadata(self._make_catalog(), "prod.default.users")
        assert result.path == ["prod", "default", "users"]

    def test_missing_optional_fields_are_none(self, monkeypatch):
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, fn: {"table_type": "MANAGED"},
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        result = UnityCatalogPlugin().get_metadata(self._make_catalog(), "prod.default.t")
        assert result.format is None
        assert result.location is None
        assert result.owner is None
        assert result.comment is None
        assert result.last_modified is None

    def test_read_only_no_mutations(self, monkeypatch):
        """get_metadata must not call any mutating API methods."""
        mutating_calls = []

        def fake_create_table(self, *a, **kw):
            mutating_calls.append("create_table")

        raw = self._raw_table()
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, fn: raw,
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.create_table", fake_create_table)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        UnityCatalogPlugin().get_metadata(self._make_catalog(), "prod.default.users")
        assert mutating_calls == []


class TestUnityTypeMapping:
    """Unit tests for _unity_type_to_arrow mapping function."""

    def test_bigint_maps_to_int64(self):
        from rivet_databricks.unity_catalog import _unity_type_to_arrow
        assert _unity_type_to_arrow("bigint") == "int64"

    def test_long_maps_to_int64(self):
        from rivet_databricks.unity_catalog import _unity_type_to_arrow
        assert _unity_type_to_arrow("long") == "int64"

    def test_int_maps_to_int32(self):
        from rivet_databricks.unity_catalog import _unity_type_to_arrow
        assert _unity_type_to_arrow("int") == "int32"

    def test_double_maps_to_float64(self):
        from rivet_databricks.unity_catalog import _unity_type_to_arrow
        assert _unity_type_to_arrow("double") == "float64"

    def test_decimal_with_params_maps_to_float64(self):
        from rivet_databricks.unity_catalog import _unity_type_to_arrow
        assert _unity_type_to_arrow("decimal(10,2)") == "float64"

    def test_string_maps_to_large_utf8(self):
        from rivet_databricks.unity_catalog import _unity_type_to_arrow
        assert _unity_type_to_arrow("string") == "large_utf8"

    def test_boolean_maps_to_bool(self):
        from rivet_databricks.unity_catalog import _unity_type_to_arrow
        assert _unity_type_to_arrow("boolean") == "bool"

    def test_timestamp_maps_to_timestamp_us(self):
        from rivet_databricks.unity_catalog import _unity_type_to_arrow
        assert _unity_type_to_arrow("timestamp") == "timestamp[us]"

    def test_date_maps_to_date32(self):
        from rivet_databricks.unity_catalog import _unity_type_to_arrow
        assert _unity_type_to_arrow("date") == "date32"

    def test_binary_maps_to_large_binary(self):
        from rivet_databricks.unity_catalog import _unity_type_to_arrow
        assert _unity_type_to_arrow("binary") == "large_binary"

    def test_unknown_type_defaults_to_large_utf8(self):
        from rivet_databricks.unity_catalog import _unity_type_to_arrow
        assert _unity_type_to_arrow("array<string>") == "large_utf8"

    def test_case_insensitive(self):
        from rivet_databricks.unity_catalog import _unity_type_to_arrow
        assert _unity_type_to_arrow("BIGINT") == "int64"
        assert _unity_type_to_arrow("String") == "large_utf8"
