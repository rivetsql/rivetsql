"""Tests for task 36.2: Distinguish authentication vs connectivity errors in connection failures.

Validates Requirement 34.3: WHEN a connection error occurs, THE Plugin SHALL distinguish
between authentication errors and connectivity errors in the error message.

Validates Requirement 34.5: WHEN a metadata-resolving adapter (Glue, Unity) cannot reach
its API, THE error SHALL distinguish between credential issues and connectivity issues.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import requests

from rivet_core.errors import ExecutionError
from rivet_postgres.errors import classify_pg_error, is_auth_error, is_connectivity_error

# ── Unit tests for classify_pg_error ──────────────────────────────────


class TestIsAuthError:
    def test_password_authentication_failed(self):
        assert is_auth_error(Exception("FATAL: password authentication failed for user \"alice\""))

    def test_role_does_not_exist(self):
        assert is_auth_error(Exception("FATAL: role \"baduser\" does not exist"))

    def test_no_pg_hba_conf_entry(self):
        assert is_auth_error(Exception("FATAL: no pg_hba.conf entry for host"))

    def test_peer_authentication_failed(self):
        assert is_auth_error(Exception("FATAL: peer authentication failed for user \"bob\""))

    def test_connection_refused_is_not_auth(self):
        assert not is_auth_error(Exception("could not connect to server: Connection refused"))

    def test_timeout_is_not_auth(self):
        assert not is_auth_error(Exception("connection timed out"))


class TestIsConnectivityError:
    def test_connection_refused(self):
        assert is_connectivity_error(Exception("could not connect to server: Connection refused"))

    def test_name_or_service_not_known(self):
        assert is_connectivity_error(Exception("could not connect to server: Name or service not known"))

    def test_connection_timed_out(self):
        assert is_connectivity_error(Exception("connection timed out"))

    def test_no_route_to_host(self):
        assert is_connectivity_error(Exception("No route to host"))

    def test_auth_error_is_not_connectivity(self):
        assert not is_connectivity_error(Exception("password authentication failed"))

    def test_generic_error_is_not_connectivity(self):
        assert not is_connectivity_error(Exception("syntax error at or near SELECT"))


class TestClassifyPgError:
    def test_auth_error_returns_rvt_502(self):
        exc = Exception("FATAL: password authentication failed for user \"alice\"")
        code, message, remediation = classify_pg_error(exc, plugin_type="source")
        assert code == "RVT-502"
        assert "authentication" in message.lower()
        assert remediation

    def test_connectivity_error_returns_rvt_501(self):
        exc = Exception("could not connect to server: Connection refused")
        code, message, remediation = classify_pg_error(exc, plugin_type="source")
        assert code == "RVT-501"
        assert "connectivity" in message.lower()
        assert remediation

    def test_generic_error_returns_rvt_501(self):
        exc = Exception("syntax error at or near SELECT")
        code, message, remediation = classify_pg_error(exc, plugin_type="source")
        assert code == "RVT-501"
        assert remediation

    def test_role_not_exist_is_auth(self):
        exc = Exception("FATAL: role \"nobody\" does not exist")
        code, _, _ = classify_pg_error(exc, plugin_type="catalog")
        assert code == "RVT-502"

    def test_network_unreachable_is_connectivity(self):
        exc = Exception("network is unreachable")
        code, _, _ = classify_pg_error(exc, plugin_type="sink")
        assert code == "RVT-501"


# ── PostgreSQL source: auth vs connectivity distinction ────────────────


class TestPostgresSourceErrorClassification:
    def _make_ref(self):
        from rivet_postgres.source import PostgresDeferredMaterializedRef

        return PostgresDeferredMaterializedRef("host=localhost dbname=test", "SELECT 1")

    def test_auth_error_raises_rvt_502(self):
        mock_psycopg = MagicMock()
        mock_psycopg.AsyncConnection.connect = AsyncMock(
            side_effect=Exception("FATAL: password authentication failed for user \"alice\"")
        )

        with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
            ref = self._make_ref()
            with pytest.raises(ExecutionError) as exc_info:
                ref.to_arrow()

        assert exc_info.value.error.code == "RVT-502"
        assert "authentication" in exc_info.value.error.message.lower()

    def test_connectivity_error_raises_rvt_501(self):
        mock_psycopg = MagicMock()
        mock_psycopg.AsyncConnection.connect = AsyncMock(
            side_effect=Exception("could not connect to server: Connection refused")
        )

        with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
            ref = self._make_ref()
            with pytest.raises(ExecutionError) as exc_info:
                ref.to_arrow()

        assert exc_info.value.error.code == "RVT-501"
        assert "connectivity" in exc_info.value.error.message.lower()

    def test_generic_error_raises_rvt_501(self):
        mock_psycopg = MagicMock()
        mock_psycopg.AsyncConnection.connect = AsyncMock(
            side_effect=Exception("syntax error at or near SELECT")
        )

        with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
            ref = self._make_ref()
            with pytest.raises(ExecutionError) as exc_info:
                ref.to_arrow()

        assert exc_info.value.error.code == "RVT-501"


# ── PostgreSQL sink: auth vs connectivity distinction ──────────────────


class TestPostgresSinkErrorClassification:
    def _make_sink_write(self, exc_msg: str):
        """Trigger a sink write that fails with the given exception message."""

        import pyarrow as pa

        from rivet_core.models import Catalog, Joint, Material
        from rivet_core.strategies import MaterializedRef
        from rivet_postgres.sink import PostgresSink

        class _Ref(MaterializedRef):
            def to_arrow(self):
                return pa.table({"id": [1, 2]})

            @property
            def schema(self):
                return None

            @property
            def row_count(self):
                return 2

            @property
            def size_bytes(self):
                return None

            @property
            def storage_type(self):
                return "test"

        catalog = Catalog(
            name="pg",
            type="postgres",
            options={
                "host": "localhost",
                "database": "testdb",
                "user": "user",
                "password": "pass",
            },
        )
        joint = Joint(name="j1", joint_type="sink", catalog="pg", table="public.t")
        material = Material(name="j1", catalog="pg", materialized_ref=_Ref(), state="materialized")

        mock_psycopg = MagicMock()
        mock_psycopg.AsyncConnection.connect = AsyncMock(side_effect=Exception(exc_msg))

        sink = PostgresSink()
        with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
            sink.write(catalog, joint, material, "append")

    def test_auth_error_raises_rvt_502(self):
        with pytest.raises(ExecutionError) as exc_info:
            self._make_sink_write("FATAL: password authentication failed for user \"alice\"")
        assert exc_info.value.error.code == "RVT-502"
        assert "authentication" in exc_info.value.error.message.lower()

    def test_connectivity_error_raises_rvt_501(self):
        with pytest.raises(ExecutionError) as exc_info:
            self._make_sink_write("could not connect to server: Connection refused")
        assert exc_info.value.error.code == "RVT-501"
        assert "connectivity" in exc_info.value.error.message.lower()


# ── PostgreSQL catalog: auth vs connectivity distinction ───────────────


class TestPostgresCatalogErrorClassification:
    def _make_catalog(self):
        from rivet_core.models import Catalog

        return Catalog(
            name="pg",
            type="postgres",
            options={
                "host": "localhost",
                "database": "testdb",
                "user": "user",
                "password": "pass",
            },
        )

    def test_auth_error_raises_rvt_502(self):
        from rivet_postgres.catalog import PostgresCatalogPlugin

        plugin = PostgresCatalogPlugin()
        catalog = self._make_catalog()

        mock_psycopg = MagicMock()
        mock_psycopg.connect.side_effect = Exception(
            "FATAL: password authentication failed for user \"alice\""
        )

        with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
            with pytest.raises(ExecutionError) as exc_info:
                plugin._connect(catalog)

        assert exc_info.value.error.code == "RVT-502"
        assert "authentication" in exc_info.value.error.message.lower()

    def test_connectivity_error_raises_rvt_501(self):
        from rivet_postgres.catalog import PostgresCatalogPlugin

        plugin = PostgresCatalogPlugin()
        catalog = self._make_catalog()

        mock_psycopg = MagicMock()
        mock_psycopg.connect.side_effect = Exception(
            "could not connect to server: Connection refused"
        )

        with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
            with pytest.raises(ExecutionError) as exc_info:
                plugin._connect(catalog)

        assert exc_info.value.error.code == "RVT-501"
        assert "connectivity" in exc_info.value.error.message.lower()

    def test_context_includes_host_and_database(self):
        from rivet_postgres.catalog import PostgresCatalogPlugin

        plugin = PostgresCatalogPlugin()
        catalog = self._make_catalog()

        mock_psycopg = MagicMock()
        mock_psycopg.connect.side_effect = Exception("could not connect to server: Connection refused")

        with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
            with pytest.raises(ExecutionError) as exc_info:
                plugin._connect(catalog)

        ctx = exc_info.value.error.context
        assert ctx.get("host") == "localhost"
        assert ctx.get("database") == "testdb"


# ── Unity client: auth vs connectivity distinction ─────────────────────


class TestUnityClientErrorClassification:
    """Unity client already distinguishes auth vs connectivity — verify the contract."""

    def _make_client(self):
        from rivet_databricks.auth import AUTH_TYPE_PAT, ResolvedCredential
        from rivet_databricks.client import UnityCatalogClient

        cred = ResolvedCredential(auth_type=AUTH_TYPE_PAT, token="tok", source="explicit")
        return UnityCatalogClient(host="https://host.databricks.com", credential=cred)

    def test_401_is_auth_error(self):
        client = self._make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_resp.json.return_value = {"message": "Invalid token"}
        mock_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_resp)

        with patch.object(client._session, "request", return_value=mock_resp):
            with pytest.raises(ExecutionError) as exc_info:
                client.list_catalogs()

        assert exc_info.value.error.code == "RVT-502"
        assert "authentication" in exc_info.value.error.message.lower()

    def test_403_is_authorization_error(self):
        client = self._make_client()
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.json.return_value = {"message": "Access denied"}
        mock_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_resp)

        with patch.object(client._session, "request", return_value=mock_resp):
            with pytest.raises(ExecutionError) as exc_info:
                client.list_catalogs()

        assert exc_info.value.error.code == "RVT-502"
        assert "authorization" in exc_info.value.error.message.lower()

    def test_connection_error_is_connectivity_error(self):
        client = self._make_client()

        with patch.object(
            client._session,
            "request",
            side_effect=requests.ConnectionError("Connection refused"),
        ), pytest.raises(ExecutionError) as exc_info:
            client.list_catalogs()

        # After exhausting retries, raises RVT-503 (connectivity)
        assert exc_info.value.error.code == "RVT-503"


# ── Databricks engine: auth vs connectivity distinction ────────────────


class TestDatabricksEngineErrorClassification:
    def _make_api(self):
        from rivet_databricks.engine import DatabricksStatementAPI

        return DatabricksStatementAPI(
            workspace_url="https://host.databricks.com",
            token="dapi_test",
            warehouse_id="wh123",
        )

    def test_401_is_auth_error(self):
        api = self._make_api()
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 401
        mock_resp.json.return_value = {"message": "Invalid token"}
        mock_resp.text = "Invalid token"

        with patch.object(api._session, "post", return_value=mock_resp):
            with pytest.raises(ExecutionError) as exc_info:
                api.execute("SELECT 1")

        assert exc_info.value.error.code == "RVT-502"
        assert "authentication" in exc_info.value.error.message.lower()

    def test_403_is_authorization_error(self):
        api = self._make_api()
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 403
        mock_resp.json.return_value = {"message": "Access denied"}
        mock_resp.text = "Access denied"

        with patch.object(api._session, "post", return_value=mock_resp):
            with pytest.raises(ExecutionError) as exc_info:
                api.execute("SELECT 1")

        assert exc_info.value.error.code == "RVT-502"
        assert "authorization" in exc_info.value.error.message.lower()

    def test_connection_error_is_connectivity_error(self):
        api = self._make_api()

        with patch.object(
            api._session,
            "post",
            side_effect=requests.ConnectionError("Connection refused"),
        ), pytest.raises(ExecutionError) as exc_info:
            api.execute("SELECT 1")

        assert exc_info.value.error.code == "RVT-501"
        assert "connect" in exc_info.value.error.message.lower()
