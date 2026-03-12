"""Integration tests for RestApiSource.

Tests that the source plugin returns deferred Materials, handles joint.sql=None,
and resolves joint.table to endpoint lookup keys correctly.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from rivet_core.models import Catalog, Joint
from rivet_rest.source import RestApiSource

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
                },
            },
            "max_flatten_depth": 3,
            "response_format": "json",
        },
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_read_returns_deferred_material(mock_catalog: Catalog) -> None:
    """Test read() returns a Material in deferred state."""
    source = RestApiSource()
    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_api",
        table="users",
    )

    # Call read - should NOT make HTTP requests
    material = source.read(mock_catalog, joint, pushdown=None)

    # Verify deferred state
    assert material.state == "deferred"
    assert material.materialized_ref is not None
    assert material.name == "test_joint"
    assert material.catalog == "test_api"


def test_joint_sql_none_handled_gracefully(mock_catalog: Catalog) -> None:
    """Test that joint.sql=None is handled gracefully (REST APIs don't use SQL)."""
    source = RestApiSource()
    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_api",
        table="users",
        sql=None,  # Explicitly None
    )

    # Should not raise an error
    material = source.read(mock_catalog, joint, pushdown=None)

    assert material.state == "deferred"
    assert material.name == "test_joint"


def test_joint_table_resolves_to_endpoint_lookup_key(mock_catalog: Catalog) -> None:
    """Test that joint.table is used to look up the endpoint configuration."""
    source = RestApiSource()
    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_api",
        table="orders",  # Should resolve to endpoints["orders"]
    )

    material = source.read(mock_catalog, joint, pushdown=None)

    # Verify the material was created successfully
    assert material.state == "deferred"
    assert material.materialized_ref is not None

    # Verify the endpoint config was resolved correctly by checking the ref's config
    ref = material.materialized_ref
    assert hasattr(ref, "_endpoint_config")
    assert ref._endpoint_config["path"] == "/orders"
    assert ref._endpoint_config["method"] == "GET"


def test_joint_name_fallback_when_table_missing(mock_catalog: Catalog) -> None:
    """Test that joint.name is used as fallback when joint.table is not set."""
    source = RestApiSource()
    joint = Joint(
        name="users",  # Should be used as fallback for endpoint lookup
        joint_type="source",
        catalog="test_api",
        # table not set
    )

    material = source.read(mock_catalog, joint, pushdown=None)

    # Verify the material was created successfully
    assert material.state == "deferred"
    assert material.materialized_ref is not None

    # Verify the endpoint config was resolved using joint.name
    ref = material.materialized_ref
    assert hasattr(ref, "_endpoint_config")
    assert ref._endpoint_config["path"] == "/users"


def test_pushdown_ignored_in_source_fallback(mock_catalog: Catalog) -> None:
    """Test that pushdown parameter is ignored (source fallback doesn't support pushdown)."""
    source = RestApiSource()
    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_api",
        table="users",
    )

    # Pass a pushdown plan (would be used in adapter path, but ignored here)
    material = source.read(mock_catalog, joint, pushdown={"some": "pushdown"})

    # Should still create deferred material without error
    assert material.state == "deferred"

    # Verify no query params were added (pushdown ignored)
    ref = material.materialized_ref
    assert hasattr(ref, "_query_params")
    assert ref._query_params == {}  # Empty, no pushdown applied


def test_to_arrow_executes_http_request(mock_catalog: Catalog) -> None:
    """Test that calling to_arrow() on the deferred ref executes HTTP requests."""
    source = RestApiSource()
    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_api",
        table="users",
    )

    material = source.read(mock_catalog, joint, pushdown=None)

    # Mock the HTTP response
    mock_response = {
        "data": {
            "users": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ]
        }
    }

    with patch("requests.Session.request") as mock_request:
        mock_resp = mock_request.return_value
        mock_resp.ok = True
        mock_resp.json.return_value = mock_response

        # Call to_arrow() - should trigger HTTP request
        table = material.to_arrow()

        # Verify HTTP request was made
        assert mock_request.called
        assert mock_request.call_count == 1

        # Verify Arrow table was returned
        assert table.num_rows == 2
        assert "id" in table.column_names
        assert "name" in table.column_names


def test_endpoint_not_in_config_uses_default_path(mock_catalog: Catalog) -> None:
    """Test that endpoints not in config get a default path based on table name."""
    source = RestApiSource()
    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_api",
        table="unknown_endpoint",  # Not in endpoints config
    )

    material = source.read(mock_catalog, joint, pushdown=None)

    # Verify the material was created with default path
    assert material.state == "deferred"
    ref = material.materialized_ref
    assert hasattr(ref, "_endpoint_config")
    assert ref._endpoint_config["path"] == "/unknown_endpoint"
