"""Integration tests for REST API sink strategy validation.

Verifies that RestApiSink rejects unsupported write strategies with
ExecutionError (RVT-501) and accepts supported strategies without
raising strategy validation errors.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from rivet_core.errors import ExecutionError
from rivet_core.models import Catalog, Joint, Material
from rivet_rest.sink import RestApiSink


def _make_catalog() -> MagicMock:
    catalog = MagicMock(spec=Catalog)
    catalog.name = "test_api"
    catalog.options = {
        "base_url": "https://api.example.com",
        "auth": "none",
        "endpoints": {"items": {"path": "/items", "batch_size": 1}},
        "max_retries": 0,
    }
    return catalog


def _make_joint() -> MagicMock:
    joint = MagicMock(spec=Joint)
    joint.name = "items"
    joint.table = "items"
    return joint


def _make_material() -> MagicMock:
    table = pa.table({"id": [1], "value": ["a"]})
    material = MagicMock(spec=Material)
    material.to_arrow.return_value = table
    return material


# ── Unsupported strategies raise ExecutionError ──────────────────────


@pytest.mark.parametrize(
    "strategy", ["merge", "scd2", "delete_insert", "truncate_insert", "incremental_append"]
)
def test_rest_sink_rejects_unsupported_strategy(strategy: str) -> None:
    sink = RestApiSink()

    with pytest.raises(ExecutionError) as exc_info:
        sink.write(_make_catalog(), _make_joint(), _make_material(), strategy=strategy)

    assert exc_info.value.error.code == "RVT-501"
    assert strategy in exc_info.value.error.message
    assert "append" in (exc_info.value.error.remediation or "")
    assert "replace" in (exc_info.value.error.remediation or "")


# ── Supported strategies do not raise strategy validation errors ─────


@pytest.mark.parametrize("strategy", ["append", "replace"])
def test_rest_sink_accepts_supported_strategy(strategy: str) -> None:
    sink = RestApiSink()

    with patch("rivet_rest.sink.requests.Session") as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.request.return_value = mock_response

        # Should not raise strategy validation error
        sink.write(_make_catalog(), _make_joint(), _make_material(), strategy=strategy)
