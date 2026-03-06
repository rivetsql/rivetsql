"""Tests for DatabricksStatementAPI (task 25.3).

Tests the Statement Execution API: submit, poll, Arrow Flight / JSONL result fetching.
All HTTP interactions are mocked — no network calls.
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pyarrow
import pyarrow.ipc
import pytest

from rivet_core.errors import ExecutionError
from rivet_databricks.engine import (
    DatabricksComputeEnginePlugin,
    DatabricksStatementAPI,
    _parse_wait_timeout,
)

_WORKSPACE = "https://my-workspace.databricks.com"
_TOKEN = "dapi_test_token"
_WAREHOUSE_ID = "abc123"


def _make_api(**kwargs):
    defaults = {
        "workspace_url": _WORKSPACE,
        "token": _TOKEN,
        "warehouse_id": _WAREHOUSE_ID,
    }
    defaults.update(kwargs)
    return DatabricksStatementAPI(**defaults)


def _arrow_ipc_bytes(table: pyarrow.Table) -> bytes:
    """Serialize a PyArrow Table to Arrow IPC stream bytes."""
    sink = io.BytesIO()
    writer = pyarrow.ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    writer.close()
    return sink.getvalue()


# ── parse_wait_timeout ────────────────────────────────────────────────


def test_parse_wait_timeout_with_suffix():
    assert _parse_wait_timeout("30s") == 30


def test_parse_wait_timeout_without_suffix():
    assert _parse_wait_timeout("60") == 60


def test_parse_wait_timeout_invalid():
    assert _parse_wait_timeout("abc") == 30


# ── Constructor ───────────────────────────────────────────────────────


def test_api_sets_auth_header():
    api = _make_api()
    assert api._session.headers["Authorization"] == f"Bearer {_TOKEN}"


def test_api_sets_base_url():
    api = _make_api()
    assert api._base_url == _WORKSPACE


def test_api_strips_trailing_slash():
    api = _make_api(workspace_url="https://example.com/")
    assert api._base_url == "https://example.com"


# ── Submit ────────────────────────────────────────────────────────────


def test_submit_posts_correct_payload():
    api = _make_api()
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.json.return_value = {
        "statement_id": "stmt-1",
        "status": {"state": "PENDING"},
    }
    with patch.object(api._session, "post", return_value=mock_resp) as mock_post:
        sid = api._submit("SELECT 1")
    assert sid == "stmt-1"
    call_args = mock_post.call_args
    body = call_args.kwargs.get("json") or call_args[1].get("json")
    assert body["warehouse_id"] == _WAREHOUSE_ID
    assert body["statement"] == "SELECT 1"
    assert body["format"] == "ARROW_STREAM"
    assert body["disposition"] == "EXTERNAL_LINKS"


def test_submit_with_catalog_and_schema():
    api = _make_api()
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.json.return_value = {
        "statement_id": "stmt-2",
        "status": {"state": "PENDING"},
    }
    with patch.object(api._session, "post", return_value=mock_resp) as mock_post:
        api._submit("SELECT 1", catalog="main", schema="default")
    body = mock_post.call_args.kwargs.get("json") or mock_post.call_args[1].get("json")
    assert body["catalog"] == "main"
    assert body["schema"] == "default"


def test_submit_raises_on_missing_statement_id():
    api = _make_api()
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.json.return_value = {"status": {"state": "PENDING"}}
    with patch.object(api._session, "post", return_value=mock_resp):
        with pytest.raises(ExecutionError) as exc_info:
            api._submit("SELECT 1")
    assert exc_info.value.error.code == "RVT-502"


def test_submit_raises_on_http_401():
    api = _make_api()
    mock_resp = MagicMock()
    mock_resp.ok = False
    mock_resp.status_code = 401
    mock_resp.json.return_value = {"message": "Unauthorized"}
    with patch.object(api._session, "post", return_value=mock_resp):
        with pytest.raises(ExecutionError) as exc_info:
            api._submit("SELECT 1")
    assert exc_info.value.error.code == "RVT-502"
    assert "401" in exc_info.value.error.message


def test_submit_raises_on_http_403():
    api = _make_api()
    mock_resp = MagicMock()
    mock_resp.ok = False
    mock_resp.status_code = 403
    mock_resp.json.return_value = {"message": "Forbidden"}
    with patch.object(api._session, "post", return_value=mock_resp):
        with pytest.raises(ExecutionError) as exc_info:
            api._submit("SELECT 1")
    assert exc_info.value.error.code == "RVT-502"
    assert "403" in exc_info.value.error.message


# ── Poll ──────────────────────────────────────────────────────────────


def test_poll_returns_on_succeeded():
    api = _make_api()
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.json.return_value = {
        "statement_id": "stmt-1",
        "status": {"state": "SUCCEEDED"},
        "manifest": {"format": "ARROW_STREAM"},
        "result": {},
    }
    with patch.object(api._session, "get", return_value=mock_resp):
        result = api._poll("stmt-1")
    assert result["status"]["state"] == "SUCCEEDED"


def test_poll_raises_on_failed():
    api = _make_api()
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.json.return_value = {
        "statement_id": "stmt-1",
        "status": {
            "state": "FAILED",
            "error": {"message": "Syntax error in SQL"},
        },
    }
    with patch.object(api._session, "get", return_value=mock_resp):
        with pytest.raises(ExecutionError) as exc_info:
            api._poll("stmt-1")
    err = exc_info.value.error
    assert err.code == "RVT-502"
    assert "stmt-1" in err.message
    assert "FAILED" in err.context["state"]
    assert "Syntax error" in err.message


def test_poll_raises_on_canceled():
    api = _make_api()
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.json.return_value = {
        "statement_id": "stmt-1",
        "status": {"state": "CANCELED"},
    }
    with patch.object(api._session, "get", return_value=mock_resp):
        with pytest.raises(ExecutionError) as exc_info:
            api._poll("stmt-1")
    err = exc_info.value.error
    assert err.code == "RVT-502"
    assert "CANCELED" in err.message
    assert err.context["statement_id"] == "stmt-1"


def test_poll_waits_for_pending_then_succeeds():
    api = _make_api()
    pending_resp = MagicMock()
    pending_resp.ok = True
    pending_resp.json.return_value = {
        "statement_id": "stmt-1",
        "status": {"state": "RUNNING"},
    }
    succeeded_resp = MagicMock()
    succeeded_resp.ok = True
    succeeded_resp.json.return_value = {
        "statement_id": "stmt-1",
        "status": {"state": "SUCCEEDED"},
        "result": {},
        "manifest": {},
    }
    with patch.object(api._session, "get", side_effect=[pending_resp, succeeded_resp]):
        with patch("rivet_databricks.engine.time.sleep"):
            result = api._poll("stmt-1")
    assert result["status"]["state"] == "SUCCEEDED"


# ── Fetch Arrow IPC results ──────────────────────────────────────────


def test_fetch_arrow_chunks():
    api = _make_api()
    expected = pyarrow.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    ipc_bytes = _arrow_ipc_bytes(expected)

    data = {
        "manifest": {"format": "ARROW_STREAM"},
        "result": {
            "external_links": [{"external_link": "https://storage.example.com/chunk0"}],
        },
    }
    mock_chunk_resp = MagicMock()
    mock_chunk_resp.status_code = 200
    mock_chunk_resp.content = ipc_bytes

    with patch("rivet_databricks.engine.requests.get", return_value=mock_chunk_resp):
        result = api._fetch_result(data, "stmt-1")

    assert result.num_rows == 3
    assert result.column_names == ["id", "name"]
    assert result.column("id").to_pylist() == [1, 2, 3]


def test_fetch_arrow_multiple_chunks():
    api = _make_api()
    t1 = pyarrow.table({"x": [1, 2]})
    t2 = pyarrow.table({"x": [3, 4]})

    data = {
        "manifest": {"format": "ARROW_STREAM"},
        "result": {
            "external_links": [
                {"external_link": "https://storage.example.com/chunk0"},
                {"external_link": "https://storage.example.com/chunk1"},
            ],
        },
    }

    resp1 = MagicMock()
    resp1.status_code = 200
    resp1.content = _arrow_ipc_bytes(t1)
    resp2 = MagicMock()
    resp2.status_code = 200
    resp2.content = _arrow_ipc_bytes(t2)

    with patch("rivet_databricks.engine.requests.get", side_effect=[resp1, resp2]):
        result = api._fetch_result(data, "stmt-1")

    assert result.num_rows == 4
    assert result.column("x").to_pylist() == [1, 2, 3, 4]


def test_fetch_arrow_chunk_failure_raises():
    api = _make_api()
    data = {
        "manifest": {"format": "ARROW_STREAM"},
        "result": {
            "external_links": [{"external_link": "https://storage.example.com/chunk0"}],
        },
    }
    mock_resp = MagicMock()
    mock_resp.status_code = 403
    with patch("rivet_databricks.engine.requests.get", return_value=mock_resp):
        with pytest.raises(ExecutionError) as exc_info:
            api._fetch_result(data, "stmt-1")
    assert exc_info.value.error.code == "RVT-502"


# ── Fetch JSONL results ──────────────────────────────────────────────


def test_fetch_jsonl_inline_result():
    api = _make_api()
    data = {
        "manifest": {
            "format": "JSON_ARRAY",
            "schema": {
                "columns": [
                    {"name": "id", "type_text": "INT"},
                    {"name": "val", "type_text": "STRING"},
                ],
            },
        },
        "result": {
            "data_array": [
                [1, "hello"],
                [2, "world"],
            ],
        },
    }
    result = api._fetch_result(data, "stmt-1")
    assert result.num_rows == 2
    assert result.column_names == ["id", "val"]
    assert result.column("id").to_pylist() == [1, 2]
    assert result.column("val").to_pylist() == ["hello", "world"]


def test_fetch_empty_jsonl_result():
    api = _make_api()
    data = {
        "manifest": {
            "format": "JSON_ARRAY",
            "schema": {"columns": []},
        },
        "result": {"data_array": []},
    }
    result = api._fetch_result(data, "stmt-1")
    assert result.num_rows == 0


# ── No result (DDL/DML) ──────────────────────────────────────────────


def test_fetch_no_result_returns_empty_table():
    api = _make_api()
    data = {"manifest": {"format": ""}, "result": {}}
    result = api._fetch_result(data, "stmt-1")
    assert result.num_rows == 0


# ── Full execute flow ─────────────────────────────────────────────────


def test_execute_full_flow_arrow():
    """End-to-end: submit → poll → fetch Arrow IPC."""
    api = _make_api()
    expected = pyarrow.table({"col": [10, 20]})
    ipc_bytes = _arrow_ipc_bytes(expected)

    submit_resp = MagicMock()
    submit_resp.ok = True
    submit_resp.json.return_value = {
        "statement_id": "stmt-99",
        "status": {"state": "PENDING"},
    }

    poll_resp = MagicMock()
    poll_resp.ok = True
    poll_resp.json.return_value = {
        "statement_id": "stmt-99",
        "status": {"state": "SUCCEEDED"},
        "manifest": {"format": "ARROW_STREAM"},
        "result": {
            "external_links": [{"external_link": "https://storage.example.com/chunk"}],
        },
    }

    chunk_resp = MagicMock()
    chunk_resp.status_code = 200
    chunk_resp.content = ipc_bytes

    with patch.object(api._session, "post", return_value=submit_resp):
        with patch.object(api._session, "get", return_value=poll_resp):
            with patch("rivet_databricks.engine.requests.get", return_value=chunk_resp):
                result = api.execute("SELECT col FROM t")

    assert result.num_rows == 2
    assert result.column("col").to_pylist() == [10, 20]


def test_execute_full_flow_jsonl():
    """End-to-end: submit → poll → fetch JSONL inline."""
    api = _make_api(disposition="INLINE")

    submit_resp = MagicMock()
    submit_resp.ok = True
    submit_resp.json.return_value = {
        "statement_id": "stmt-100",
        "status": {"state": "PENDING"},
    }

    poll_resp = MagicMock()
    poll_resp.ok = True
    poll_resp.json.return_value = {
        "statement_id": "stmt-100",
        "status": {"state": "SUCCEEDED"},
        "manifest": {
            "format": "JSON_ARRAY",
            "schema": {"columns": [{"name": "x", "type_text": "INT"}]},
        },
        "result": {"data_array": [[42]]},
    }

    with patch.object(api._session, "post", return_value=submit_resp):
        with patch.object(api._session, "get", return_value=poll_resp):
            result = api.execute("SELECT 42 AS x")

    assert result.num_rows == 1
    assert result.column("x").to_pylist() == [42]


# ── create_statement_api factory ──────────────────────────────────────


def test_create_statement_api():
    plugin = DatabricksComputeEnginePlugin()
    config = {
        "warehouse_id": "wh-1",
        "wait_timeout": "60s",
        "max_rows_per_chunk": 50000,
        "disposition": "INLINE",
    }
    api = plugin.create_statement_api(
        workspace_url="https://example.databricks.com",
        token="tok",
        config=config,
    )
    assert isinstance(api, DatabricksStatementAPI)
    assert api._warehouse_id == "wh-1"
    assert api._wait_timeout_s == 60
    assert api._max_rows_per_chunk == 50000
    assert api._disposition == "INLINE"


def test_create_statement_api_defaults():
    plugin = DatabricksComputeEnginePlugin()
    config = {"warehouse_id": "wh-2"}
    api = plugin.create_statement_api(
        workspace_url="https://example.databricks.com",
        token="tok",
        config=config,
    )
    assert api._wait_timeout_s == 30
    assert api._max_rows_per_chunk == 100_000
    assert api._disposition == "EXTERNAL_LINKS"


# ── Arrow chunk pagination ────────────────────────────────────────────


def test_fetch_arrow_with_next_chunk_pagination():
    """Test that next_chunk_internal_link pagination is followed."""
    api = _make_api()
    t1 = pyarrow.table({"v": [1]})
    t2 = pyarrow.table({"v": [2]})

    data = {
        "manifest": {"format": "ARROW_STREAM"},
        "result": {
            "external_links": [
                {
                    "external_link": "https://storage.example.com/chunk0",
                    "next_chunk_internal_link": "/api/2.0/sql/statements/stmt-1/result/chunks/1",
                },
            ],
        },
    }

    chunk0_resp = MagicMock()
    chunk0_resp.status_code = 200
    chunk0_resp.content = _arrow_ipc_bytes(t1)

    chunk1_ext_resp = MagicMock()
    chunk1_ext_resp.status_code = 200
    chunk1_ext_resp.content = _arrow_ipc_bytes(t2)

    # Internal pagination response
    page_resp = MagicMock()
    page_resp.ok = True
    page_resp.json.return_value = {
        "external_links": [{"external_link": "https://storage.example.com/chunk1"}],
    }

    with patch("rivet_databricks.engine.requests.get", side_effect=[chunk0_resp, chunk1_ext_resp]):
        with patch.object(api._session, "get", return_value=page_resp):
            result = api._fetch_result(data, "stmt-1")

    assert result.num_rows == 2
    assert result.column("v").to_pylist() == [1, 2]


# ── HTTP error handling ──────────────────────────────────────────────


def test_check_http_error_passes_on_ok():
    api = _make_api()
    resp = MagicMock()
    resp.ok = True
    api._check_http_error(resp)  # should not raise


def test_check_http_error_generic():
    api = _make_api()
    resp = MagicMock()
    resp.ok = False
    resp.status_code = 500
    resp.json.return_value = {"message": "Internal Server Error"}
    with pytest.raises(ExecutionError) as exc_info:
        api._check_http_error(resp)
    assert "500" in exc_info.value.error.message


# ── Close ─────────────────────────────────────────────────────────────


def test_close_closes_session():
    api = _make_api()
    with patch.object(api._session, "close") as mock_close:
        api.close()
    mock_close.assert_called_once()
