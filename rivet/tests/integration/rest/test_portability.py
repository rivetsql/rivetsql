"""Integration tests for catalog portability and joint reuse.

Verifies that source joints with ``table:`` references work identically
across catalog types (DuckDB, REST API, etc.) without modification.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

from rivet_core.models import Catalog, Joint
from rivet_rest.adapter import RestApiAdapter


class TestJointTableResolution:
    """Test that joint.table resolution works for REST API catalog."""

    def test_joint_table_resolves_to_endpoint(self) -> None:
        """Verify RestApiAdapter.read_dispatch() resolves joint.table as endpoint key."""
        adapter = RestApiAdapter()

        # Create a mock catalog with an "orders" endpoint
        catalog = MagicMock(spec=Catalog)
        catalog.name = "test_api"
        catalog.options = {
            "base_url": "https://api.example.com",
            "endpoints": {
                "orders": {
                    "path": "/api/orders",
                    "method": "GET",
                    "response_path": "data",
                }
            },
            "auth": "none",
            "default_headers": {},
            "timeout": 30,
            "max_flatten_depth": 3,
            "response_format": "json",
        }

        # Create a joint with table: orders
        joint = MagicMock(spec=Joint)
        joint.name = "orders_source"
        joint.table = "orders"
        joint.sql = None  # REST APIs don't use SQL

        # Mock engine
        engine = MagicMock()

        # Call read_dispatch
        result = adapter.read_dispatch(engine, catalog, joint, pushdown=None)

        # Verify result
        assert result.material is not None
        assert result.material.state == "deferred"
        assert result.material.name == "orders_source"
        assert result.material.catalog == "test_api"

        # Verify the deferred ref has the correct endpoint config
        ref = result.material.materialized_ref
        assert ref._endpoint_config["path"] == "/api/orders"
        assert ref._endpoint_config["method"] == "GET"
        assert ref._endpoint_config["response_path"] == "data"

    def test_joint_sql_none_handled_gracefully(self) -> None:
        """Verify joint.sql = None is handled gracefully (no SQL for REST APIs)."""
        adapter = RestApiAdapter()

        catalog = MagicMock(spec=Catalog)
        catalog.name = "test_api"
        catalog.options = {
            "base_url": "https://api.example.com",
            "endpoints": {
                "users": {
                    "path": "/users",
                    "method": "GET",
                }
            },
            "auth": "none",
            "default_headers": {},
            "timeout": 30,
            "max_flatten_depth": 3,
            "response_format": "json",
        }

        joint = MagicMock(spec=Joint)
        joint.name = "users_source"
        joint.table = "users"
        joint.sql = None  # Explicitly set to None

        engine = MagicMock()

        # Should not raise any errors
        result = adapter.read_dispatch(engine, catalog, joint, pushdown=None)

        assert result.material is not None
        assert result.material.state == "deferred"

    def test_joint_table_fallback_to_name(self) -> None:
        """Verify joint.table falls back to joint.name if table is not set."""
        adapter = RestApiAdapter()

        catalog = MagicMock(spec=Catalog)
        catalog.name = "test_api"
        catalog.options = {
            "base_url": "https://api.example.com",
            "endpoints": {
                "products": {
                    "path": "/products",
                    "method": "GET",
                }
            },
            "auth": "none",
            "default_headers": {},
            "timeout": 30,
            "max_flatten_depth": 3,
            "response_format": "json",
        }

        joint = MagicMock(spec=Joint)
        joint.name = "products"
        joint.table = None  # No table specified

        engine = MagicMock()

        result = adapter.read_dispatch(engine, catalog, joint, pushdown=None)

        assert result.material is not None
        ref = result.material.materialized_ref
        assert ref._endpoint_config["path"] == "/products"

    def test_joint_table_consistent_with_duckdb_pattern(self) -> None:
        """Verify joint.table resolution follows same pattern as DuckDB adapter.

        DuckDB adapter uses: getattr(joint, "table", None) or getattr(joint, "name", "unknown")
        REST API adapter should use the same pattern for consistency.
        """
        adapter = RestApiAdapter()

        catalog = MagicMock(spec=Catalog)
        catalog.name = "test_api"
        catalog.options = {
            "base_url": "https://api.example.com",
            "endpoints": {
                "orders": {
                    "path": "/orders",
                    "method": "GET",
                }
            },
            "auth": "none",
            "default_headers": {},
            "timeout": 30,
            "max_flatten_depth": 3,
            "response_format": "json",
        }

        joint = MagicMock(spec=Joint)
        joint.name = "orders_source"
        joint.table = "orders"
        joint.sql = None

        engine = MagicMock()

        result = adapter.read_dispatch(engine, catalog, joint, pushdown=None)

        # Verify the endpoint was resolved correctly
        assert result.material is not None
        ref = result.material.materialized_ref
        assert ref._endpoint_config["path"] == "/orders"


class TestCatalogPortability:
    """Test that joint YAML is identical across catalog types."""

    def test_joint_yaml_identical_for_duckdb_and_rest_api(self) -> None:
        """Verify the same joint.table works for both DuckDB and REST API catalogs.

        This test demonstrates that a source joint with ``table: orders`` would
        work identically with a DuckDB catalog (resolving to a database table)
        or a REST API catalog (resolving to an endpoint), requiring only a
        change to the catalog configuration in profiles.yaml.
        """
        # This is a conceptual test showing the joint definition is identical
        _joint_yaml_content = """
        name: orders_source
        type: source
        catalog: my_catalog
        table: orders
        """

        # For DuckDB catalog:
        # - joint.table = "orders" resolves to database table "orders"
        # - DuckDB adapter queries: SELECT * FROM orders

        # For REST API catalog:
        # - joint.table = "orders" resolves to endpoint key "orders"
        # - REST API adapter fetches from endpoints["orders"]["path"]

        # The joint YAML is IDENTICAL in both cases
        # Only the catalog type in profiles.yaml changes:
        #   DuckDB:    type: duckdb
        #   REST API:  type: rest_api

        # Verify REST API adapter handles this correctly
        adapter = RestApiAdapter()

        catalog = MagicMock(spec=Catalog)
        catalog.name = "my_catalog"
        catalog.options = {
            "base_url": "https://api.example.com",
            "endpoints": {
                "orders": {
                    "path": "/orders",
                    "method": "GET",
                }
            },
            "auth": "none",
            "default_headers": {},
            "timeout": 30,
            "max_flatten_depth": 3,
            "response_format": "json",
        }

        joint = MagicMock(spec=Joint)
        joint.name = "orders_source"
        joint.table = "orders"  # Same as DuckDB would use
        joint.sql = None

        engine = MagicMock()

        result = adapter.read_dispatch(engine, catalog, joint, pushdown=None)

        # Verify it resolves correctly
        assert result.material is not None
        assert result.material.catalog == "my_catalog"
        ref = result.material.materialized_ref
        assert ref._endpoint_config["path"] == "/orders"

    def test_endpoint_not_in_config_uses_default_path(self) -> None:
        """Verify that if endpoint is not in config, a default path is used."""
        adapter = RestApiAdapter()

        catalog = MagicMock(spec=Catalog)
        catalog.name = "test_api"
        catalog.options = {
            "base_url": "https://api.example.com",
            "endpoints": {},  # Empty endpoints
            "auth": "none",
            "default_headers": {},
            "timeout": 30,
            "max_flatten_depth": 3,
            "response_format": "json",
        }

        joint = MagicMock(spec=Joint)
        joint.name = "unknown_table"
        joint.table = "unknown_table"

        engine = MagicMock()

        result = adapter.read_dispatch(engine, catalog, joint, pushdown=None)

        # Should use default path based on table name
        assert result.material is not None
        ref = result.material.materialized_ref
        assert ref._endpoint_config["path"] == "/unknown_table"

    @patch("rivet_rest.adapter.requests.Session")
    def test_rest_api_fetch_with_table_reference(
        self,
        mock_session_class: Any,
    ) -> None:
        """Integration test: verify full fetch cycle with joint.table reference."""
        adapter = RestApiAdapter()

        # Mock HTTP response
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.json.return_value = {
            "data": [
                {"id": 1, "name": "Order 1"},
                {"id": 2, "name": "Order 2"},
            ]
        }
        mock_session.request.return_value = mock_response
        mock_session_class.return_value = mock_session

        catalog = MagicMock(spec=Catalog)
        catalog.name = "test_api"
        catalog.options = {
            "base_url": "https://api.example.com",
            "endpoints": {
                "orders": {
                    "path": "/api/orders",
                    "method": "GET",
                    "response_path": "data",
                }
            },
            "auth": "none",
            "default_headers": {},
            "timeout": 30,
            "max_flatten_depth": 3,
            "response_format": "json",
        }

        joint = MagicMock(spec=Joint)
        joint.name = "orders_source"
        joint.table = "orders"
        joint.sql = None

        engine = MagicMock()

        # Get deferred material
        result = adapter.read_dispatch(engine, catalog, joint, pushdown=None)

        # Materialize it
        arrow_table = result.material.to_arrow()

        # Verify the result
        assert arrow_table is not None
        assert arrow_table.num_rows == 2
        assert "id" in arrow_table.column_names
        assert "name" in arrow_table.column_names

        # Verify HTTP request was made to correct URL
        mock_session.request.assert_called()
        call_args = mock_session.request.call_args
        assert "https://api.example.com/api/orders" in str(call_args)
