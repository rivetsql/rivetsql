"""Tests for task 36.3: Databricks errors include workspace URL and resource name.

Validates Requirement 40.3: THE Unity_Plugin error messages for Databricks components
SHALL include the workspace URL and the fully-qualified resource name that caused the failure.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from rivet_core.errors import ExecutionError
from rivet_databricks.engine import DatabricksStatementAPI

_WORKSPACE = "https://my-workspace.databricks.com"
_TOKEN = "dapi_test"
_WAREHOUSE_ID = "wh123"


def _make_api(**kwargs):
    defaults = {
        "workspace_url": _WORKSPACE,
        "token": _TOKEN,
        "warehouse_id": _WAREHOUSE_ID,
    }
    defaults.update(kwargs)
    return DatabricksStatementAPI(**defaults)


# ── DatabricksStatementAPI: workspace_url in error context ────────────


class TestStatementAPIErrorContext:
    def test_401_includes_workspace_url(self):
        api = _make_api()
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 401
        mock_resp.json.return_value = {"message": "Unauthorized"}
        mock_resp.text = "Unauthorized"

        with patch.object(api._session, "post", return_value=mock_resp):
            with pytest.raises(ExecutionError) as exc_info:
                api.execute("SELECT 1")

        ctx = exc_info.value.error.context
        assert ctx.get("workspace_url") == _WORKSPACE

    def test_403_includes_workspace_url(self):
        api = _make_api()
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 403
        mock_resp.json.return_value = {"message": "Forbidden"}
        mock_resp.text = "Forbidden"

        with patch.object(api._session, "post", return_value=mock_resp):
            with pytest.raises(ExecutionError) as exc_info:
                api.execute("SELECT 1")

        ctx = exc_info.value.error.context
        assert ctx.get("workspace_url") == _WORKSPACE

    def test_failed_statement_includes_workspace_url_and_statement_id(self):
        api = _make_api()
        submit_resp = MagicMock()
        submit_resp.ok = True
        submit_resp.json.return_value = {
            "statement_id": "stmt-fail-1",
            "status": {"state": "PENDING"},
        }
        poll_resp = MagicMock()
        poll_resp.ok = True
        poll_resp.json.return_value = {
            "statement_id": "stmt-fail-1",
            "status": {"state": "FAILED", "error": {"message": "Syntax error"}},
        }

        with patch.object(api._session, "post", return_value=submit_resp):
            with patch.object(api._session, "get", return_value=poll_resp):
                with pytest.raises(ExecutionError) as exc_info:
                    api.execute("SELECT bad syntax")

        ctx = exc_info.value.error.context
        assert ctx.get("workspace_url") == _WORKSPACE
        assert ctx.get("statement_id") == "stmt-fail-1"

    def test_canceled_statement_includes_workspace_url(self):
        api = _make_api()
        submit_resp = MagicMock()
        submit_resp.ok = True
        submit_resp.json.return_value = {
            "statement_id": "stmt-cancel-1",
            "status": {"state": "PENDING"},
        }
        poll_resp = MagicMock()
        poll_resp.ok = True
        poll_resp.json.return_value = {
            "statement_id": "stmt-cancel-1",
            "status": {"state": "CANCELED"},
        }

        with patch.object(api._session, "post", return_value=submit_resp):
            with patch.object(api._session, "get", return_value=poll_resp):
                with pytest.raises(ExecutionError) as exc_info:
                    api.execute("SELECT 1")

        ctx = exc_info.value.error.context
        assert ctx.get("workspace_url") == _WORKSPACE
        assert ctx.get("statement_id") == "stmt-cancel-1"

    def test_generic_http_error_includes_workspace_url(self):
        api = _make_api()
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 500
        mock_resp.json.return_value = {"message": "Internal Server Error"}
        mock_resp.text = "Internal Server Error"

        with patch.object(api._session, "post", return_value=mock_resp):
            with pytest.raises(ExecutionError) as exc_info:
                api.execute("SELECT 1")

        ctx = exc_info.value.error.context
        assert ctx.get("workspace_url") == _WORKSPACE


# ── DatabricksSink: workspace_url in execution errors ─────────────────


class TestDatabricksSinkErrorContext:
    def _make_catalog(self, workspace_url=_WORKSPACE):
        from rivet_core.models import Catalog
        return Catalog(
            name="db_cat",
            type="databricks",
            options={
                "workspace_url": workspace_url,
                "catalog": "main",
                "schema": "default",
                "warehouse_id": "wh123",
            },
        )

    def test_missing_warehouse_id_error_includes_workspace_url(self):
        """RVT-204 error for missing warehouse_id should include workspace_url."""
        import pyarrow as pa

        from rivet_core.models import Catalog, Material
        from rivet_core.strategies import MaterializedRef
        from rivet_databricks.databricks_sink import DatabricksSink

        class _Ref(MaterializedRef):
            def to_arrow(self): return pa.table({"id": [1]})
            @property
            def schema(self): return None
            @property
            def row_count(self): return 1
            @property
            def size_bytes(self): return None
            @property
            def storage_type(self): return "test"

        catalog = Catalog(
            name="db_cat",
            type="databricks",
            options={"workspace_url": _WORKSPACE, "catalog": "main", "schema": "default"},
            # no warehouse_id
        )
        joint = MagicMock()
        joint.name = "j1"
        joint.table = "main.default.orders"
        joint.sink_options = {}
        material = Material(name="j1", catalog="db_cat", materialized_ref=_Ref(), state="materialized")

        sink = DatabricksSink()

        from rivet_databricks.auth import AUTH_TYPE_PAT, ResolvedCredential
        fake_cred = ResolvedCredential(auth_type=AUTH_TYPE_PAT, token="tok", source="explicit")
        with patch("rivet_databricks.auth.resolve_credentials", return_value=fake_cred):
            with pytest.raises(ExecutionError) as exc_info:
                sink.write(catalog, joint, material, "append")

        ctx = exc_info.value.error.context
        assert ctx.get("workspace_url") == _WORKSPACE
