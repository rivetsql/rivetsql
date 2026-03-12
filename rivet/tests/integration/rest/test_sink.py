"""Integration tests for RestApiSink.

Tests write operations with different strategies, HTTP methods, batching,
and error handling.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
import requests

from rivet_core.errors import ExecutionError
from rivet_core.models import Catalog, Joint, Material
from rivet_rest.sink import RestApiSink


def test_append_strategy_sends_post_requests() -> None:
    """Test that append strategy uses POST method."""
    table = pa.table({"id": [1, 2], "name": ["Alice", "Bob"]})

    catalog = MagicMock(spec=Catalog)
    catalog.name = "test_api"
    catalog.options = {
        "base_url": "https://api.example.com",
        "auth": "none",
        "endpoints": {
            "users": {
                "path": "/users",
                "batch_size": 1,
            }
        },
        "max_retries": 0,
    }

    joint = MagicMock(spec=Joint)
    joint.name = "users"
    joint.table = "users"

    material = MagicMock(spec=Material)
    material.to_arrow.return_value = table

    with patch("rivet_rest.sink.requests.Session") as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.request.return_value = mock_response

        sink = RestApiSink()
        sink.write(catalog, joint, material, strategy="append")

        # Verify POST method used
        assert mock_session.request.call_count == 2
        for call in mock_session.request.call_args_list:
            args, kwargs = call
            assert args[0] == "POST"


def test_replace_strategy_sends_put_requests() -> None:
    """Test that replace strategy uses PUT method."""
    table = pa.table({"id": [1], "name": ["Alice"]})

    catalog = MagicMock(spec=Catalog)
    catalog.name = "test_api"
    catalog.options = {
        "base_url": "https://api.example.com",
        "auth": "none",
        "endpoints": {
            "users": {
                "path": "/users",
                "batch_size": 1,
            }
        },
        "max_retries": 0,
    }

    joint = MagicMock(spec=Joint)
    joint.name = "users"
    joint.table = "users"

    material = MagicMock(spec=Material)
    material.to_arrow.return_value = table

    with patch("rivet_rest.sink.requests.Session") as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.request.return_value = mock_response

        sink = RestApiSink()
        sink.write(catalog, joint, material, strategy="replace")

        # Verify PUT method used
        assert mock_session.request.call_count == 1
        args, kwargs = mock_session.request.call_args_list[0]
        assert args[0] == "PUT"


def test_write_method_patch_override() -> None:
    """Test that write_method=PATCH overrides the default method."""
    table = pa.table({"id": [1], "name": ["Alice"]})

    catalog = MagicMock(spec=Catalog)
    catalog.name = "test_api"
    catalog.options = {
        "base_url": "https://api.example.com",
        "auth": "none",
        "endpoints": {
            "users": {
                "path": "/users",
                "write_method": "PATCH",
                "batch_size": 1,
            }
        },
        "max_retries": 0,
    }

    joint = MagicMock(spec=Joint)
    joint.name = "users"
    joint.table = "users"

    material = MagicMock(spec=Material)
    material.to_arrow.return_value = table

    with patch("rivet_rest.sink.requests.Session") as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.request.return_value = mock_response

        sink = RestApiSink()
        sink.write(catalog, joint, material, strategy="replace")

        # Verify PATCH method used
        assert mock_session.request.call_count == 1
        args, kwargs = mock_session.request.call_args_list[0]
        assert args[0] == "PATCH"


def test_batch_size_grouping() -> None:
    """Test that rows are correctly grouped by batch_size."""
    table = pa.table({"id": [1, 2, 3, 4, 5], "name": ["A", "B", "C", "D", "E"]})

    catalog = MagicMock(spec=Catalog)
    catalog.name = "test_api"
    catalog.options = {
        "base_url": "https://api.example.com",
        "auth": "none",
        "endpoints": {
            "users": {
                "path": "/users",
                "batch_size": 2,
            }
        },
        "max_retries": 0,
    }

    joint = MagicMock(spec=Joint)
    joint.name = "users"
    joint.table = "users"

    material = MagicMock(spec=Material)
    material.to_arrow.return_value = table

    with patch("rivet_rest.sink.requests.Session") as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.request.return_value = mock_response

        sink = RestApiSink()
        sink.write(catalog, joint, material, strategy="append")

        # Verify 3 requests (2+2+1)
        assert mock_session.request.call_count == 3

        # Check batch sizes
        call_0_body = mock_session.request.call_args_list[0][1]["json"]
        call_1_body = mock_session.request.call_args_list[1][1]["json"]
        call_2_body = mock_session.request.call_args_list[2][1]["json"]

        assert isinstance(call_0_body, list) and len(call_0_body) == 2
        assert isinstance(call_1_body, list) and len(call_1_body) == 2
        assert isinstance(call_2_body, dict)  # Single row as object


def test_write_failure_includes_row_index_and_status() -> None:
    """Test that write failures include row index, HTTP status, and response body."""
    table = pa.table({"id": [1, 2, 3], "name": ["A", "B", "C"]})

    catalog = MagicMock(spec=Catalog)
    catalog.name = "test_api"
    catalog.options = {
        "base_url": "https://api.example.com",
        "auth": "none",
        "endpoints": {
            "users": {
                "path": "/users",
                "batch_size": 1,
            }
        },
        "max_retries": 0,
    }

    joint = MagicMock(spec=Joint)
    joint.name = "users"
    joint.table = "users"

    material = MagicMock(spec=Material)
    material.to_arrow.return_value = table

    with patch("rivet_rest.sink.requests.Session") as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # First request succeeds, second fails
        mock_response_ok = MagicMock()
        mock_response_ok.status_code = 200

        mock_response_fail = MagicMock()
        mock_response_fail.status_code = 400
        mock_response_fail.text = "Bad request: invalid data"
        mock_response_fail.raise_for_status.side_effect = requests.HTTPError(
            response=mock_response_fail
        )

        mock_session.request.side_effect = [
            mock_response_ok,
            mock_response_fail,
        ]

        sink = RestApiSink()

        with pytest.raises(ExecutionError) as exc_info:
            sink.write(catalog, joint, material, strategy="append")

        error = exc_info.value.error
        assert error.code == "RVT-501"
        assert "row 1" in error.message  # Second row (index 1)
        assert "400" in error.message
        assert "Bad request" in error.message
        assert error.context["row_index"] == 1
        assert error.context["status_code"] == 400


def test_auth_applied_to_session() -> None:
    """Test that authentication is applied to the session."""
    table = pa.table({"id": [1], "name": ["Alice"]})

    catalog = MagicMock(spec=Catalog)
    catalog.name = "test_api"
    catalog.options = {
        "base_url": "https://api.example.com",
        "auth": "bearer",
        "token": "test-token-123",
        "endpoints": {
            "users": {
                "path": "/users",
                "batch_size": 1,
            }
        },
        "max_retries": 0,
    }

    joint = MagicMock(spec=Joint)
    joint.name = "users"
    joint.table = "users"

    material = MagicMock(spec=Material)
    material.to_arrow.return_value = table

    with patch("rivet_rest.sink.requests.Session") as mock_session_class:
        mock_session = MagicMock()
        # Make headers a real dict so we can check it
        mock_session.headers = {}
        mock_session_class.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.request.return_value = mock_response

        sink = RestApiSink()
        sink.write(catalog, joint, material, strategy="append")

        # Verify Authorization header was set
        assert "Authorization" in mock_session.headers
        assert mock_session.headers["Authorization"] == "Bearer test-token-123"


def test_content_type_header_set() -> None:
    """Test that Content-Type: application/json header is set."""
    table = pa.table({"id": [1], "name": ["Alice"]})

    catalog = MagicMock(spec=Catalog)
    catalog.name = "test_api"
    catalog.options = {
        "base_url": "https://api.example.com",
        "auth": "none",
        "endpoints": {
            "users": {
                "path": "/users",
                "batch_size": 1,
            }
        },
        "max_retries": 0,
    }

    joint = MagicMock(spec=Joint)
    joint.name = "users"
    joint.table = "users"

    material = MagicMock(spec=Material)
    material.to_arrow.return_value = table

    with patch("rivet_rest.sink.requests.Session") as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.request.return_value = mock_response

        sink = RestApiSink()
        sink.write(catalog, joint, material, strategy="append")

        # Verify Content-Type header in request
        call_kwargs = mock_session.request.call_args_list[0][1]
        assert "headers" in call_kwargs
        assert call_kwargs["headers"]["Content-Type"] == "application/json"


def test_url_construction() -> None:
    """Test that URL is correctly constructed from base_url and path."""
    table = pa.table({"id": [1], "name": ["Alice"]})

    catalog = MagicMock(spec=Catalog)
    catalog.name = "test_api"
    catalog.options = {
        "base_url": "https://api.example.com/v1",
        "auth": "none",
        "endpoints": {
            "users": {
                "path": "/users",
                "batch_size": 1,
            }
        },
        "max_retries": 0,
    }

    joint = MagicMock(spec=Joint)
    joint.name = "users"
    joint.table = "users"

    material = MagicMock(spec=Material)
    material.to_arrow.return_value = table

    with patch("rivet_rest.sink.requests.Session") as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.request.return_value = mock_response

        sink = RestApiSink()
        sink.write(catalog, joint, material, strategy="append")

        # Verify URL
        args, kwargs = mock_session.request.call_args_list[0]
        assert args[1] == "https://api.example.com/v1/users"
