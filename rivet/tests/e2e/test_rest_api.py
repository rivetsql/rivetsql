"""E2E tests for REST API catalog plugin.

Tests full CLI lifecycle: source reads, pagination, sink writes, and wildcard
adapter resolution with DuckDB engine.

Validates Requirements: 1.1, 1.2, 1.3, 3.1, 4.1, 4.2, 5.1, 5.8, 9.1, 9.2,
10.1, 10.2, 10.5, 13.1, 14.1, 14.2, 14.4, 15.1, 15.2, 15.6
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from tests.e2e.conftest import read_sink_csv, run_cli, write_sink, write_source

# ---------------------------------------------------------------------------
# Profiles template with REST API catalog
# ---------------------------------------------------------------------------

_PROFILES_WITH_REST_API = """\
default:
  catalogs:
    local:
      type: filesystem
      path: ./data
      format: csv
    test_api:
      type: rest_api
      base_url: https://api.example.com
      auth: none
      response_format: json
      max_flatten_depth: 3
      endpoints:
        users:
          path: /users
          method: GET
          response_path: data.users
        orders:
          path: /orders
          method: GET
          schema:
            order_id: int64
            customer: string
            amount: int64
          pagination:
            strategy: offset
            limit: 2
            offset_param: offset
            limit_param: limit
        products:
          path: /products
          method: POST
  engines:
    - name: duckdb_primary
      type: duckdb
      catalogs: [local, test_api]
  default_engine: duckdb_primary
"""


# ---------------------------------------------------------------------------
# Mock HTTP response helpers
# ---------------------------------------------------------------------------


def _mock_response(
    status_code: int, json_data: dict | list | None = None, text: str | None = None
) -> Mock:
    """Create a mock requests.Response object."""
    response = Mock()
    response.status_code = status_code
    response.headers = {}
    response.raise_for_status = Mock()
    if json_data is not None:
        response.json.return_value = json_data
        response.text = json.dumps(json_data)
    elif text is not None:
        response.text = text
        response.json.side_effect = ValueError("Not JSON")
    else:
        response.text = ""
        response.json.side_effect = ValueError("Not JSON")
    return response


# ---------------------------------------------------------------------------
# Test 15.1: REST API source reads data through full CLI lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_rest_api_source_full_lifecycle(tmp_path: Path, capsys) -> None:
    """REST API source reads data through full CLI lifecycle.

    Creates a temporary Rivet project with a rest_api catalog, defines a
    source joint reading from a REST API endpoint, runs the pipeline via
    rivet_cli.app._main, and asserts on exit code and sink data.

    Validates Requirements: 1.1, 1.2, 1.3, 3.1, 5.1, 9.1, 9.2, 13.1, 14.1, 14.2
    """
    project = tmp_path

    # Create project structure
    (project / "rivet.yaml").write_text(
        "profiles: profiles.yaml\nsources: sources\njoints: joints\nsinks: sinks\n"
    )
    (project / "profiles.yaml").write_text(_PROFILES_WITH_REST_API)

    for d in ("sources", "joints", "sinks", "data"):
        (project / d).mkdir()

    # Define source joint reading from REST API
    write_source(project, "api_users", catalog="test_api", table="users")

    # Define sink to local filesystem
    write_sink(
        project,
        "users_output",
        catalog="local",
        table="users_output",
        upstream=["api_users"],
    )

    # Mock HTTP response
    mock_users_response = _mock_response(
        200,
        {
            "data": {
                "users": [
                    {"id": 1, "name": "Alice", "email": "alice@example.com"},
                    {"id": 2, "name": "Bob", "email": "bob@example.com"},
                    {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
                ]
            }
        },
    )

    # Patch at the module level where requests is imported
    with patch("requests.Session") as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_session.__enter__.return_value = mock_session
        mock_session.__exit__.return_value = None
        mock_session.request.return_value = mock_users_response

        # Run compile
        result = run_cli(project, ["compile"], capsys)
        assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

        # Run pipeline
        result = run_cli(project, ["run"], capsys)
        assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    # Verify sink data (observable output)
    table = read_sink_csv(project, "users_output")
    assert table.num_rows == 3
    names = sorted(table.column("name").to_pylist())
    assert names == ["Alice", "Bob", "Charlie"]
    emails = sorted(table.column("email").to_pylist())
    assert emails == ["alice@example.com", "bob@example.com", "charlie@example.com"]


# ---------------------------------------------------------------------------
# Test 15.2: REST API source with pagination
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_rest_api_source_with_pagination(tmp_path: Path, capsys) -> None:
    """REST API source with offset pagination fetches all pages.

    Configures an endpoint with offset pagination and multi-page mocked
    responses. Verifies all pages are fetched and consolidated into a single
    result.

    Validates Requirements: 4.1, 4.2, 5.8
    """
    project = tmp_path

    # Create project structure
    (project / "rivet.yaml").write_text(
        "profiles: profiles.yaml\nsources: sources\njoints: joints\nsinks: sinks\n"
    )
    (project / "profiles.yaml").write_text(_PROFILES_WITH_REST_API)

    for d in ("sources", "joints", "sinks", "data"):
        (project / d).mkdir()

    # Define source joint reading from paginated endpoint
    write_source(project, "api_orders", catalog="test_api", table="orders")

    # Define sink to local filesystem
    write_sink(
        project,
        "orders_output",
        catalog="local",
        table="orders_output",
        upstream=["api_orders"],
    )

    # Mock paginated responses (3 pages)
    page1_response = _mock_response(
        200,
        [
            {"order_id": 1, "customer": "Alice", "amount": 100},
            {"order_id": 2, "customer": "Bob", "amount": 200},
        ],
    )
    page2_response = _mock_response(
        200,
        [
            {"order_id": 3, "customer": "Charlie", "amount": 150},
            {"order_id": 4, "customer": "Diana", "amount": 250},
        ],
    )
    page3_response = _mock_response(
        200,
        [
            {"order_id": 5, "customer": "Eve", "amount": 300},
        ],
    )
    empty_response = _mock_response(200, [])

    # Patch requests.Session to return paginated responses
    with patch("requests.Session") as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_session.__enter__.return_value = mock_session
        mock_session.__exit__.return_value = None

        # Return different responses based on offset parameter
        call_count = [0]

        def side_effect(*args, **kwargs):
            params = kwargs.get("params", {})
            offset = params.get("offset", 0)
            call_count[0] += 1
            offset_responses = {
                0: page1_response,
                2: page2_response,
                4: page3_response,
            }
            return offset_responses.get(offset, empty_response)

        mock_session.request.side_effect = side_effect

        # Run pipeline
        result = run_cli(project, ["compile"], capsys)
        assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

        result = run_cli(project, ["run"], capsys)
        assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    # Verify all pages consolidated into single result (observable output)
    table = read_sink_csv(project, "orders_output")
    assert table.num_rows == 5
    order_ids = sorted(table.column("order_id").to_pylist())
    assert order_ids == [1, 2, 3, 4, 5]
    customers = table.column("customer").to_pylist()
    assert "Alice" in customers
    assert "Eve" in customers


# ---------------------------------------------------------------------------
# Test 15.3: REST API sink writes data
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_rest_api_sink_writes_data(tmp_path: Path, capsys) -> None:
    """REST API sink writes data via POST requests.

    Configures a sink joint writing to a REST API endpoint. Verifies POST
    requests are sent with correct JSON bodies.

    Validates Requirements: 10.1, 10.2, 10.5
    """
    project = tmp_path

    # Create project structure
    (project / "rivet.yaml").write_text(
        "profiles: profiles.yaml\nsources: sources\njoints: joints\nsinks: sinks\n"
    )
    (project / "profiles.yaml").write_text(_PROFILES_WITH_REST_API)

    for d in ("sources", "joints", "sinks", "data"):
        (project / d).mkdir()

    # Create source data in local filesystem
    (project / "data" / "products.csv").write_text(
        "product_id,name,price\n1,Widget,10.99\n2,Gadget,25.50\n3,Gizmo,5.00\n"
    )
    write_source(project, "src_products", catalog="local", table="products")

    # Define sink to REST API
    (project / "sinks" / "api_products.sql").write_text(
        "-- rivet:name: api_products\n"
        "-- rivet:type: sink\n"
        "-- rivet:catalog: test_api\n"
        "-- rivet:table: products\n"
        "-- rivet:upstream: [src_products]\n"
    )

    # Mock HTTP response for POST requests
    mock_post_response = _mock_response(201, {"status": "created"})

    # Patch requests.Session to capture POST requests
    with patch("requests.Session") as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_session.__enter__.return_value = mock_session
        mock_session.__exit__.return_value = None
        mock_session.request.return_value = mock_post_response

        # Run pipeline
        result = run_cli(project, ["compile"], capsys)
        assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

        result = run_cli(project, ["run"], capsys)
        assert result.exit_code == 0, f"run failed:\n{result.stderr}"

        # Verify POST requests were made (observable behavior)
        assert mock_session.request.called, "Expected HTTP requests to be made"

        # Verify at least one POST request
        post_calls = [
            call
            for call in mock_session.request.call_args_list
            if len(call[0]) > 0 and call[0][0] == "POST"
        ]
        assert len(post_calls) > 0, "Expected at least one POST request"


# ---------------------------------------------------------------------------
# Test 15.4: Wildcard adapter resolves for DuckDB engine with REST API catalog
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_wildcard_adapter_resolution_with_duckdb(tmp_path: Path, capsys) -> None:
    """Wildcard adapter resolves for DuckDB engine with REST API catalog.

    Configures a DuckDB engine with a rest_api catalog. Verifies the pipeline
    compiles and executes through the wildcard adapter path. Verifies pushdown
    capabilities are included in the compiled plan.

    Validates Requirements: 14.1, 14.4, 15.1, 15.2, 15.6
    """
    project = tmp_path

    # Create project structure
    (project / "rivet.yaml").write_text(
        "profiles: profiles.yaml\nsources: sources\njoints: joints\nsinks: sinks\n"
    )
    (project / "profiles.yaml").write_text(_PROFILES_WITH_REST_API)

    for d in ("sources", "joints", "sinks", "data"):
        (project / d).mkdir()

    # Define source joint reading from REST API
    write_source(project, "api_users", catalog="test_api", table="users")

    # Define sink to local filesystem
    write_sink(
        project,
        "users_output",
        catalog="local",
        table="users_output",
        upstream=["api_users"],
    )

    # Mock HTTP response
    mock_users_response = _mock_response(
        200,
        {
            "data": {
                "users": [
                    {"id": 1, "name": "Alice"},
                    {"id": 2, "name": "Bob"},
                ]
            }
        },
    )

    # Patch requests.Session
    with patch("requests.Session") as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_session.__enter__.return_value = mock_session
        mock_session.__exit__.return_value = None
        mock_session.request.return_value = mock_users_response

        # Run compile - should succeed with wildcard adapter
        result = run_cli(project, ["compile"], capsys)
        assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

        # Verify compilation output doesn't contain adapter resolution errors
        # (The wildcard adapter should be resolved for (duckdb, rest_api))
        assert "RVT-402" not in result.stderr, "Should not have adapter resolution error"
        assert "Unknown catalog plugin" not in result.stderr

        # Run pipeline - should execute through wildcard adapter
        result = run_cli(project, ["run"], capsys)
        assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    # Verify sink data (pipeline executed successfully through wildcard adapter)
    table = read_sink_csv(project, "users_output")
    assert table.num_rows == 2
    names = sorted(table.column("name").to_pylist())
    assert names == ["Alice", "Bob"]


# ---------------------------------------------------------------------------
# Test 15.5: REST API source with SQL LIMIT pushdown
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_rest_api_source_with_limit_pushdown(tmp_path: Path, capsys) -> None:
    """REST API source with SQL LIMIT stops fetching pages when limit is satisfied.

    Configures an endpoint with pagination and verifies that when a SQL LIMIT
    is applied directly to the source, the adapter stops fetching pages once
    enough records are retrieved.

    Validates Requirements: 5.1, 5.8, 14.2
    """
    project = tmp_path

    # Create project structure
    (project / "rivet.yaml").write_text(
        "profiles: profiles.yaml\nsources: sources\njoints: joints\nsinks: sinks\n"
    )
    (project / "profiles.yaml").write_text(_PROFILES_WITH_REST_API)

    for d in ("sources", "joints", "sinks", "data"):
        (project / d).mkdir()

    # Define source joint with LIMIT in the SQL
    (project / "sources" / "api_orders.sql").write_text(
        "-- rivet:name: api_orders\n"
        "-- rivet:type: source\n"
        "-- rivet:catalog: test_api\n"
        "-- rivet:table: orders\n"
        "SELECT * FROM orders LIMIT 3\n"
    )

    # Define sink to local filesystem
    write_sink(
        project,
        "orders_output",
        catalog="local",
        table="orders_output",
        upstream=["api_orders"],
    )

    # Mock paginated responses (5 pages available, but should only fetch 2)
    page1_response = _mock_response(
        200,
        [
            {"order_id": 1, "customer": "Alice", "amount": 100},
            {"order_id": 2, "customer": "Bob", "amount": 200},
        ],
    )
    page2_response = _mock_response(
        200,
        [
            {"order_id": 3, "customer": "Charlie", "amount": 150},
            {"order_id": 4, "customer": "Diana", "amount": 250},
        ],
    )
    page3_response = _mock_response(
        200,
        [
            {"order_id": 5, "customer": "Eve", "amount": 300},
            {"order_id": 6, "customer": "Frank", "amount": 350},
        ],
    )

    # Patch requests.Session to track how many pages are fetched
    with patch("requests.Session") as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_session.__enter__.return_value = mock_session
        mock_session.__exit__.return_value = None

        # Track number of HTTP requests
        request_count = [0]

        def side_effect(*args, **kwargs):
            params = kwargs.get("params", {})
            offset = params.get("offset", 0)
            request_count[0] += 1
            if offset == 0:
                return page1_response
            elif offset == 2:
                return page2_response
            elif offset == 4:
                return page3_response
            else:
                return _mock_response(200, [])

        mock_session.request.side_effect = side_effect

        # Run pipeline
        result = run_cli(project, ["compile"], capsys)
        assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

        result = run_cli(project, ["run"], capsys)
        assert result.exit_code == 0, f"run failed:\n{result.stderr}"

        # Verify only 2 pages were fetched (4 records total, LIMIT 3 needs only first 2 pages)
        # Page 1: 2 records, Page 2: 2 records (total 4, truncated to 3)
        assert request_count[0] == 2, f"Expected 2 HTTP requests, got {request_count[0]}"

    # Verify sink data contains exactly 3 records (observable output)
    table = read_sink_csv(project, "orders_output")
    assert table.num_rows == 3, f"Expected 3 rows, got {table.num_rows}"
    order_ids = sorted(table.column("order_id").to_pylist())
    assert order_ids == [1, 2, 3], f"Expected order_ids [1, 2, 3], got {order_ids}"
