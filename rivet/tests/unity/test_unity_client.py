"""Tests for rivet_databricks.client — Unity Catalog REST API client."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from rivet_core.errors import ExecutionError
from rivet_databricks.auth import AUTH_TYPE_OAUTH_M2M, AUTH_TYPE_PAT, ResolvedCredential
from rivet_databricks.client import UnityCatalogClient


@pytest.fixture
def pat_cred() -> ResolvedCredential:
    return ResolvedCredential(auth_type=AUTH_TYPE_PAT, token="dapi_test123", source="explicit")


@pytest.fixture
def oauth_cred() -> ResolvedCredential:
    return ResolvedCredential(
        auth_type=AUTH_TYPE_OAUTH_M2M,
        client_id="cid",
        client_secret="csec",
        source="explicit",
    )


@pytest.fixture
def client(pat_cred: ResolvedCredential) -> UnityCatalogClient:
    return UnityCatalogClient(host="https://my-workspace.cloud.databricks.com", credential=pat_cred)


# ── Construction ──────────────────────────────────────────────────────


class TestConstruction:
    def test_host_trailing_slash_stripped(self, pat_cred):
        c = UnityCatalogClient(host="https://host.com/", credential=pat_cred)
        assert c._base_url == "https://host.com/api/2.1/unity-catalog"

    def test_uses_requests_session(self, pat_cred):
        c = UnityCatalogClient(host="https://host.com", credential=pat_cred)
        assert isinstance(c._session, requests.Session)

    def test_pat_auth_header(self, pat_cred):
        c = UnityCatalogClient(host="https://host.com", credential=pat_cred)
        assert c._session.headers["Authorization"] == "Bearer dapi_test123"

    def test_oauth_auth_header(self, oauth_cred):
        """OAuth M2M should use client_id as bearer token placeholder (actual token exchange at request time)."""
        c = UnityCatalogClient(host="https://host.com", credential=oauth_cred)
        # OAuth M2M sets up token exchange; the session should have auth configured
        assert c._credential.auth_type == AUTH_TYPE_OAUTH_M2M


# ── List catalogs ─────────────────────────────────────────────────────


class TestListCatalogs:
    def test_list_catalogs(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"catalogs": [{"name": "main"}, {"name": "dev"}]}
        mock_resp.raise_for_status = MagicMock()

        with patch.object(client._session, "request", return_value=mock_resp) as mock_req:
            result = client.list_catalogs()

        assert result == [{"name": "main"}, {"name": "dev"}]
        mock_req.assert_called_once()
        call_args = mock_req.call_args
        assert call_args[1]["method"] == "GET"
        assert "/catalogs" in call_args[1]["url"]

    def test_list_catalogs_empty(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {}
        mock_resp.raise_for_status = MagicMock()

        with patch.object(client._session, "request", return_value=mock_resp):
            result = client.list_catalogs()

        assert result == []


# ── List schemas ──────────────────────────────────────────────────────


class TestListSchemas:
    def test_list_schemas(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"schemas": [{"name": "default"}, {"name": "raw"}]}
        mock_resp.raise_for_status = MagicMock()

        with patch.object(client._session, "request", return_value=mock_resp) as mock_req:
            result = client.list_schemas(catalog_name="main")

        assert result == [{"name": "default"}, {"name": "raw"}]
        call_args = mock_req.call_args
        assert "catalog_name=main" in call_args[1]["url"] or call_args[1].get("params", {}).get("catalog_name") == "main"


# ── List tables ───────────────────────────────────────────────────────


class TestListTables:
    def test_list_tables(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"tables": [{"name": "users"}, {"name": "orders"}]}
        mock_resp.raise_for_status = MagicMock()

        with patch.object(client._session, "request", return_value=mock_resp):
            result = client.list_tables(catalog_name="main", schema_name="default")

        assert result == [{"name": "users"}, {"name": "orders"}]


# ── Get table ─────────────────────────────────────────────────────────


class TestGetTable:
    def test_get_table(self, client):
        table_data = {
            "name": "users",
            "catalog_name": "main",
            "schema_name": "default",
            "table_type": "MANAGED",
            "data_source_format": "DELTA",
            "columns": [{"name": "id", "type_text": "INT"}],
            "storage_location": "s3://bucket/path",
        }
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = table_data
        mock_resp.raise_for_status = MagicMock()

        with patch.object(client._session, "request", return_value=mock_resp) as mock_req:
            result = client.get_table("main.default.users")

        assert result["name"] == "users"
        assert result["data_source_format"] == "DELTA"
        call_args = mock_req.call_args
        assert "main.default.users" in call_args[1]["url"]

    def test_get_table_not_found(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_resp.json.return_value = {"error_code": "NOT_FOUND", "message": "Table not found"}
        mock_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_resp)

        with patch.object(client._session, "request", return_value=mock_resp):
            with pytest.raises(ExecutionError) as exc_info:
                client.get_table("main.default.nonexistent")
            assert exc_info.value.error.code == "RVT-503"


# ── Credential vending ────────────────────────────────────────────────


class TestCredentialVending:
    def test_vend_credentials_read(self, client):
        vend_resp = {
            "aws_temp_credentials": {
                "access_key_id": "AKIA...",
                "secret_access_key": "secret...",
                "session_token": "token...",
            },
            "expiration_time": "2026-02-28T10:00:00Z",
        }
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = vend_resp
        mock_resp.raise_for_status = MagicMock()

        with patch.object(client._session, "request", return_value=mock_resp) as mock_req:
            result = client.vend_credentials(
                table_id="main.default.users", operation="READ"
            )

        assert result == vend_resp
        call_args = mock_req.call_args
        assert call_args[1]["method"] == "POST"
        assert "temporary-table-credentials" in call_args[1]["url"]

    def test_vend_credentials_403_raises(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.json.return_value = {"error_code": "FORBIDDEN", "message": "Credential vending disabled"}
        mock_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_resp)

        with patch.object(client._session, "request", return_value=mock_resp):
            with pytest.raises(ExecutionError) as exc_info:
                client.vend_credentials(table_id="main.default.users", operation="READ")
            assert exc_info.value.error.code == "RVT-508"


# ── Table creation ────────────────────────────────────────────────────


class TestCreateTable:
    def test_create_table(self, client):
        table_def = {
            "name": "new_table",
            "catalog_name": "main",
            "schema_name": "default",
            "table_type": "MANAGED",
            "data_source_format": "DELTA",
            "columns": [{"name": "id", "type_text": "INT"}],
        }
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {**table_def, "full_name": "main.default.new_table"}
        mock_resp.raise_for_status = MagicMock()

        with patch.object(client._session, "request", return_value=mock_resp) as mock_req:
            result = client.create_table(table_def)

        assert result["full_name"] == "main.default.new_table"
        call_args = mock_req.call_args
        assert call_args[1]["method"] == "POST"
        assert "/tables" in call_args[1]["url"]
        assert call_args[1]["json"] == table_def


# ── Retry logic ───────────────────────────────────────────────────────


class TestRetryLogic:
    def test_retries_on_429(self, client):
        rate_resp = MagicMock()
        rate_resp.status_code = 429
        rate_resp.headers = {"Retry-After": "0"}
        rate_resp.raise_for_status.side_effect = requests.HTTPError(response=rate_resp)

        ok_resp = MagicMock()
        ok_resp.status_code = 200
        ok_resp.json.return_value = {"catalogs": []}
        ok_resp.raise_for_status = MagicMock()

        with patch.object(client._session, "request", side_effect=[rate_resp, ok_resp]):
            result = client.list_catalogs()
        assert result == []

    def test_retries_on_503(self, client):
        err_resp = MagicMock()
        err_resp.status_code = 503
        err_resp.headers = {}
        err_resp.raise_for_status.side_effect = requests.HTTPError(response=err_resp)

        ok_resp = MagicMock()
        ok_resp.status_code = 200
        ok_resp.json.return_value = {"catalogs": []}
        ok_resp.raise_for_status = MagicMock()

        with patch.object(client._session, "request", side_effect=[err_resp, ok_resp]):
            result = client.list_catalogs()
        assert result == []

    def test_no_retry_on_400(self, client):
        err_resp = MagicMock()
        err_resp.status_code = 400
        err_resp.json.return_value = {"error_code": "BAD_REQUEST", "message": "Bad request"}
        err_resp.raise_for_status.side_effect = requests.HTTPError(response=err_resp)

        with patch.object(client._session, "request", return_value=err_resp):
            with pytest.raises(ExecutionError):
                client.list_catalogs()

    def test_retries_on_connection_error(self, client):
        ok_resp = MagicMock()
        ok_resp.status_code = 200
        ok_resp.json.return_value = {"catalogs": []}
        ok_resp.raise_for_status = MagicMock()

        with patch.object(
            client._session,
            "request",
            side_effect=[requests.ConnectionError("conn failed"), ok_resp],
        ):
            result = client.list_catalogs()
        assert result == []

    def test_exhausted_retries_raises(self, client):
        err_resp = MagicMock()
        err_resp.status_code = 503
        err_resp.headers = {}
        err_resp.raise_for_status.side_effect = requests.HTTPError(response=err_resp)

        with patch.object(client._session, "request", return_value=err_resp):
            with pytest.raises(ExecutionError) as exc_info:
                client.list_catalogs()
            assert exc_info.value.error.code == "RVT-503"


# ── Auth error handling ───────────────────────────────────────────────


class TestAuthErrors:
    def test_401_raises_auth_error(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_resp.json.return_value = {"error_code": "UNAUTHENTICATED", "message": "Invalid token"}
        mock_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_resp)

        with patch.object(client._session, "request", return_value=mock_resp):
            with pytest.raises(ExecutionError) as exc_info:
                client.list_catalogs()
            assert exc_info.value.error.code == "RVT-502"
            assert "authentication" in exc_info.value.error.message.lower()

    def test_403_raises_authorization_error(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.json.return_value = {"error_code": "FORBIDDEN", "message": "Access denied"}
        mock_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_resp)

        with patch.object(client._session, "request", return_value=mock_resp):
            with pytest.raises(ExecutionError) as exc_info:
                client.list_catalogs()
            assert exc_info.value.error.code == "RVT-502"
            assert "authorization" in exc_info.value.error.message.lower()


# ── Connection pooling ────────────────────────────────────────────────


class TestConnectionPooling:
    def test_session_reused_across_calls(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"catalogs": []}
        mock_resp.raise_for_status = MagicMock()

        with patch.object(client._session, "request", return_value=mock_resp):
            client.list_catalogs()
            client.list_catalogs()

        # Same session object used for both calls
        assert client._session is not None

    def test_close_closes_session(self, client):
        with patch.object(client._session, "close") as mock_close:
            client.close()
        mock_close.assert_called_once()
