"""Integration tests for RestApiAdapter.

Tests read_dispatch, to_arrow, caching, response_path extraction,
CSV parsing, predicate pushdown, and error handling.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, Mock, patch

import pyarrow as pa
import pytest
import requests

from rivet_core.errors import ExecutionError
from rivet_core.models import Catalog, Joint
from rivet_core.optimizer import PredicatePushdownResult, PushdownPlan
from rivet_core.sql_parser import Predicate
from rivet_rest.adapter import RestApiAdapter

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_catalog() -> Catalog:
    """Create a mock REST API catalog."""
    return Catalog(
        name="test_api",
        type="rest_api",
        options={
            "base_url": "https://api.example.com",
            "auth": "none",
            "endpoints": {
                "users": {
                    "path": "/users",
                    "method": "GET",
                    "response_path": "data.users",
                },
                "orders": {
                    "path": "/orders",
                    "method": "GET",
                    "response_path": None,
                },
                "products": {
                    "path": "/products",
                    "method": "GET",
                    "response_path": "items",
                    "filter_params": {"status": "status", "category": "cat"},
                },
            },
            "max_flatten_depth": 3,
            "response_format": "json",
        },
    )


@pytest.fixture
def mock_engine() -> Mock:
    """Create a mock engine."""
    return Mock()


# ---------------------------------------------------------------------------
# Test read_dispatch
# ---------------------------------------------------------------------------


def test_read_dispatch_returns_deferred_material(
    mock_catalog: Catalog,
    mock_engine: Mock,
) -> None:
    """Test read_dispatch returns deferred Material without HTTP requests."""
    adapter = RestApiAdapter()
    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_api",
        table="users",
    )

    # Call read_dispatch - should NOT make HTTP requests
    result = adapter.read_dispatch(mock_engine, mock_catalog, joint, None)

    # Verify deferred state
    assert result.material.state == "deferred"
    assert result.material.materialized_ref is not None
    assert result.material.name == "test_joint"
    assert result.material.catalog == "test_api"


# ---------------------------------------------------------------------------
# Test to_arrow
# ---------------------------------------------------------------------------


def test_to_arrow_fetches_and_returns_arrow_table(mock_catalog: Catalog) -> None:
    """Test to_arrow() fetches data and returns Arrow table."""
    adapter = RestApiAdapter()
    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_api",
        table="users",
    )

    # Mock HTTP response
    mock_response = MagicMock()
    mock_response.ok = True
    mock_response.json.return_value = {
        "data": {"users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}
    }

    with patch("requests.Session.request", return_value=mock_response):
        result = adapter.read_dispatch(Mock(), mock_catalog, joint, None)
        table = result.material.to_arrow()

    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    assert "id" in table.column_names
    assert "name" in table.column_names


def test_to_arrow_caching(mock_catalog: Catalog) -> None:
    """Test to_arrow() caching - second call returns same table without re-fetch."""
    adapter = RestApiAdapter()
    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_api",
        table="users",
    )

    # Mock HTTP response
    mock_response = MagicMock()
    mock_response.ok = True
    mock_response.json.return_value = {"data": {"users": [{"id": 1, "name": "Alice"}]}}

    call_count = 0

    def mock_request(*args: object, **kwargs: object) -> MagicMock:
        nonlocal call_count
        call_count += 1
        return mock_response

    with patch("requests.Session.request", side_effect=mock_request):
        result = adapter.read_dispatch(Mock(), mock_catalog, joint, None)

        # First call
        table1 = result.material.to_arrow()
        assert call_count == 1

        # Second call - should use cache
        table2 = result.material.to_arrow()
        assert call_count == 1  # Still 1, not 2
        assert table2 is table1  # Same object


# ---------------------------------------------------------------------------
# Test response_path extraction
# ---------------------------------------------------------------------------


def test_response_path_extraction_from_nested_json(mock_catalog: Catalog) -> None:
    """Test response_path extraction from nested JSON."""
    adapter = RestApiAdapter()
    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_api",
        table="users",
    )

    # Mock nested response
    mock_response = MagicMock()
    mock_response.ok = True
    mock_response.json.return_value = {
        "status": "success",
        "data": {
            "users": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ],
            "total": 2,
        },
    }

    with patch("requests.Session.request", return_value=mock_response):
        result = adapter.read_dispatch(Mock(), mock_catalog, joint, None)
        table = result.material.to_arrow()

    assert table.num_rows == 2
    assert "id" in table.column_names


def test_single_json_object_wrapped_as_single_row_table(mock_catalog: Catalog) -> None:
    """Test single JSON object wrapped as single-row table."""
    adapter = RestApiAdapter()
    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_api",
        table="orders",  # No response_path configured
    )

    # Mock single object response
    mock_response = MagicMock()
    mock_response.ok = True
    mock_response.json.return_value = {"id": 123, "total": 99.99, "status": "completed"}

    with patch("requests.Session.request", return_value=mock_response):
        result = adapter.read_dispatch(Mock(), mock_catalog, joint, None)
        table = result.material.to_arrow()

    assert table.num_rows == 1
    assert "id" in table.column_names
    assert "total" in table.column_names
    assert "status" in table.column_names


# ---------------------------------------------------------------------------
# Test CSV response parsing
# ---------------------------------------------------------------------------


def test_csv_response_parsing() -> None:
    """Test CSV response parsing."""
    catalog = Catalog(
        name="test_api",
        type="rest_api",
        options={
            "base_url": "https://api.example.com",
            "auth": "none",
            "endpoints": {
                "data": {
                    "path": "/data.csv",
                    "method": "GET",
                },
            },
            "response_format": "csv",
        },
    )

    adapter = RestApiAdapter()
    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_api",
        table="data",
    )

    # Mock CSV response
    csv_data = b"id,name,value\n1,Alice,100\n2,Bob,200\n"
    mock_response = MagicMock()
    mock_response.ok = True
    mock_response.content = csv_data

    with patch("requests.Session.request", return_value=mock_response):
        result = adapter.read_dispatch(Mock(), catalog, joint, None)
        table = result.material.to_arrow()

    assert table.num_rows == 2
    assert "id" in table.column_names
    assert "name" in table.column_names
    assert "value" in table.column_names


# ---------------------------------------------------------------------------
# Test predicate pushdown
# ---------------------------------------------------------------------------


def test_predicate_pushdown_to_query_params(mock_catalog: Catalog) -> None:
    """Test predicate pushdown to query params via adapter."""
    adapter = RestApiAdapter()
    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_api",
        table="products",
    )

    # Create pushdown plan with predicates
    pushdown = PushdownPlan(
        predicates=PredicatePushdownResult(
            pushed=[
                Predicate(expression="status = 'active'", columns=["status"], location="where"),
                Predicate(
                    expression="category = 'electronics'", columns=["category"], location="where"
                ),
            ],
            residual=[],
        ),
        projections=[],
        limit=None,
        casts=[],
    )

    # Mock HTTP response
    mock_response = MagicMock()
    mock_response.ok = True
    mock_response.json.return_value = {
        "items": [{"id": 1, "name": "Laptop", "status": "active", "category": "electronics"}]
    }

    with patch("requests.Session.request", return_value=mock_response) as mock_request:
        result = adapter.read_dispatch(Mock(), mock_catalog, joint, pushdown)
        _ = result.material.to_arrow()

        # Verify query params were sent
        assert mock_request.called
        call_kwargs = mock_request.call_args[1]
        params = call_kwargs.get("params", {})
        assert params.get("status") == "active"
        assert params.get("cat") == "electronics"


# ---------------------------------------------------------------------------
# Test error handling
# ---------------------------------------------------------------------------


def test_error_401_authentication_failure(mock_catalog: Catalog) -> None:
    """Test 401 error raises authentication error."""
    adapter = RestApiAdapter()
    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_api",
        table="users",
    )

    mock_response = MagicMock()
    mock_response.ok = False
    mock_response.status_code = 401
    mock_response.json.return_value = {"error": "Unauthorized"}

    with patch("requests.Session.request", return_value=mock_response):
        result = adapter.read_dispatch(Mock(), mock_catalog, joint, None)

        with pytest.raises(ExecutionError) as exc_info:
            result.material.to_arrow()

    # The pagination module raises RVT-501 for HTTP errors during pagination
    assert "RVT-501" in str(exc_info.value)
    assert "401" in str(exc_info.value)


def test_error_404_endpoint_not_found(mock_catalog: Catalog) -> None:
    """Test 404 error raises endpoint not found error."""
    adapter = RestApiAdapter()
    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_api",
        table="users",
    )

    mock_response = MagicMock()
    mock_response.ok = False
    mock_response.status_code = 404
    mock_response.json.return_value = {"error": "Not Found"}

    with patch("requests.Session.request", return_value=mock_response):
        result = adapter.read_dispatch(Mock(), mock_catalog, joint, None)

        with pytest.raises(ExecutionError) as exc_info:
            result.material.to_arrow()

    assert "RVT-501" in str(exc_info.value)
    assert "404" in str(exc_info.value)


def test_error_json_parse_failure(mock_catalog: Catalog) -> None:
    """Test JSON parse failure includes first 200 chars of body."""
    adapter = RestApiAdapter()
    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_api",
        table="users",
    )

    mock_response = MagicMock()
    mock_response.ok = True
    mock_response.json.side_effect = json.JSONDecodeError("msg", "doc", 0)
    mock_response.text = "<html>Not JSON</html>"

    with patch("requests.Session.request", return_value=mock_response):
        result = adapter.read_dispatch(Mock(), mock_catalog, joint, None)

        with pytest.raises(ExecutionError) as exc_info:
            result.material.to_arrow()

    assert "RVT-501" in str(exc_info.value)
    assert "JSON parse error" in str(exc_info.value)


def test_error_missing_response_path(mock_catalog: Catalog) -> None:
    """Test missing response_path raises error with available keys."""
    adapter = RestApiAdapter()
    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_api",
        table="users",
    )

    # Response missing the expected path
    mock_response = MagicMock()
    mock_response.ok = True
    mock_response.json.return_value = {"status": "success", "results": []}  # Missing "data.users"

    with patch("requests.Session.request", return_value=mock_response):
        result = adapter.read_dispatch(Mock(), mock_catalog, joint, None)

        with pytest.raises(ExecutionError) as exc_info:
            result.material.to_arrow()

    assert "RVT-501" in str(exc_info.value)
    assert "response_path" in str(exc_info.value)
    assert "Available keys" in str(exc_info.value)


def test_error_network_failure(mock_catalog: Catalog) -> None:
    """Test network error raises error with URL."""
    adapter = RestApiAdapter()
    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_api",
        table="users",
    )

    with patch(
        "requests.Session.request", side_effect=requests.ConnectionError("Connection failed")
    ):
        result = adapter.read_dispatch(Mock(), mock_catalog, joint, None)

        with pytest.raises(ExecutionError) as exc_info:
            result.material.to_arrow()

    assert "RVT-501" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Test write_dispatch
# ---------------------------------------------------------------------------


def test_write_dispatch_append_strategy() -> None:
    """Test write_dispatch with append strategy sends POST requests."""
    catalog = Catalog(
        name="test_api",
        type="rest_api",
        options={
            "base_url": "https://api.example.com",
            "auth": "none",
            "endpoints": {
                "users": {
                    "path": "/users",
                    "method": "POST",
                },
            },
        },
    )

    adapter = RestApiAdapter()
    joint = Joint(
        name="test_joint",
        joint_type="sink",
        catalog="test_api",
        table="users",
        write_strategy="append",
    )

    # Mock successful POST
    mock_response = MagicMock()
    mock_response.ok = True
    mock_response.status_code = 201
    mock_response.json.return_value = {"success": True}

    # Create mock material with data
    mock_material = Mock()
    mock_material.to_arrow.return_value = pa.table(
        {
            "id": [1, 2],
            "name": ["Alice", "Bob"],
        }
    )

    with patch("requests.Session.request", return_value=mock_response) as mock_request:
        # Execute write
        adapter.write_dispatch(Mock(), catalog, joint, mock_material)

        # Verify POST was called (once per row with batch_size=1 default)
        assert mock_request.call_count == 2
        assert all(call[0][0] == "POST" for call in mock_request.call_args_list)


def test_write_dispatch_replace_strategy() -> None:
    """Test write_dispatch with replace strategy sends PUT requests."""
    catalog = Catalog(
        name="test_api",
        type="rest_api",
        options={
            "base_url": "https://api.example.com",
            "auth": "none",
            "endpoints": {
                "users": {
                    "path": "/users",
                    "method": "PUT",
                },
            },
        },
    )

    adapter = RestApiAdapter()
    joint = Joint(
        name="test_joint",
        joint_type="sink",
        catalog="test_api",
        table="users",
        write_strategy="replace",
    )

    # Mock successful PUT
    mock_response = MagicMock()
    mock_response.ok = True
    mock_response.status_code = 200
    mock_response.json.return_value = {"success": True}

    # Create mock material with data
    mock_material = Mock()
    mock_material.to_arrow.return_value = pa.table(
        {
            "id": [1],
            "name": ["Alice"],
        }
    )

    with patch("requests.Session.request", return_value=mock_response) as mock_request:
        # Execute write
        adapter.write_dispatch(Mock(), catalog, joint, mock_material)

        # Verify PUT was called
        assert mock_request.call_count == 1
        assert mock_request.call_args[0][0] == "PUT"
