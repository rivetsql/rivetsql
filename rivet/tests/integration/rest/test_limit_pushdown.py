"""Integration tests for REST API limit pushdown.

Tests that the REST API adapter correctly handles SQL LIMIT pushdown by
stopping pagination when enough records are fetched.
"""

from __future__ import annotations

from unittest.mock import MagicMock, Mock, patch

from rivet_core.optimizer import (
    CastPushdownResult,
    LimitPushdownResult,
    PredicatePushdownResult,
    ProjectionPushdownResult,
    PushdownPlan,
)
from rivet_rest.adapter import RestApiAdapter


def _mock_response(status_code: int, json_data: list | dict) -> Mock:
    """Create a mock requests.Response."""
    response = Mock()
    response.status_code = status_code
    response.ok = status_code < 400
    response.json.return_value = json_data
    response.headers = {}
    return response


def test_limit_pushdown_stops_pagination_early() -> None:
    """Adapter stops fetching pages when SQL LIMIT is satisfied.

    Configures a paginated endpoint and verifies that when a LIMIT is pushed
    down, the adapter stops fetching after collecting enough records.
    """
    # Create mock catalog with paginated endpoint
    catalog = Mock()
    catalog.name = "test_api"
    catalog.options = {
        "base_url": "https://api.example.com",
        "auth": "none",
        "response_format": "json",
        "max_flatten_depth": 3,
        "endpoints": {
            "orders": {
                "path": "/orders",
                "method": "GET",
                "pagination": {
                    "strategy": "offset",
                    "limit": 2,
                    "offset_param": "offset",
                    "limit_param": "limit",
                },
            }
        },
    }

    # Create mock joint
    joint = Mock()
    joint.name = "orders"
    joint.table = "orders"

    # Create mock engine
    engine = Mock()

    # Create pushdown plan with LIMIT 3
    pushdown = PushdownPlan(
        predicates=PredicatePushdownResult(pushed=[], residual=[]),
        projections=ProjectionPushdownResult(pushed_columns=None, reason=None),
        limit=LimitPushdownResult(pushed_limit=3, residual_limit=None, reason=None),
        casts=CastPushdownResult(pushed=[], residual=[]),
    )

    # Create adapter
    adapter = RestApiAdapter()

    # Mock HTTP responses (3 pages available, but should only fetch 2)
    page1 = _mock_response(
        200,
        [
            {"order_id": 1, "customer": "Alice"},
            {"order_id": 2, "customer": "Bob"},
        ],
    )
    page2 = _mock_response(
        200,
        [
            {"order_id": 3, "customer": "Charlie"},
            {"order_id": 4, "customer": "Diana"},
        ],
    )
    page3 = _mock_response(
        200,
        [
            {"order_id": 5, "customer": "Eve"},
        ],
    )

    request_count = [0]

    def side_effect(*args, **kwargs):
        params = kwargs.get("params", {})
        offset = params.get("offset", 0)
        request_count[0] += 1
        if offset == 0:
            return page1
        elif offset == 2:
            return page2
        elif offset == 4:
            return page3
        else:
            return _mock_response(200, [])

    # Patch requests.Session
    with patch("requests.Session") as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_session.__enter__.return_value = mock_session
        mock_session.__exit__.return_value = None
        mock_session.request.side_effect = side_effect

        # Call read_dispatch with limit pushdown
        result = adapter.read_dispatch(engine, catalog, joint, pushdown)

        # Debug: check if limit was set
        assert hasattr(result.material.materialized_ref, "_limit"), (
            "RestApiDeferredRef should have _limit attribute"
        )
        actual_limit = result.material.materialized_ref._limit
        assert actual_limit == 3, f"Expected limit=3, got {actual_limit}"

        # Materialize the result
        table = result.material.to_arrow()

        # Verify only 2-3 pages were fetched (stops early due to limit)
        # With offset pagination, may need one extra request to detect end
        assert request_count[0] <= 3, f"Expected at most 3 HTTP requests, got {request_count[0]}"
        assert request_count[0] >= 2, f"Expected at least 2 HTTP requests, got {request_count[0]}"

        # Verify exactly 3 records returned
        assert table.num_rows == 3
        order_ids = sorted(table.column("order_id").to_pylist())
        assert order_ids == [1, 2, 3]


def test_limit_pushdown_without_limit_fetches_all_pages() -> None:
    """Adapter fetches all pages when no LIMIT is pushed down.

    Verifies that without a limit, the adapter continues fetching until
    pagination is exhausted.
    """
    # Create mock catalog
    catalog = Mock()
    catalog.name = "test_api"
    catalog.options = {
        "base_url": "https://api.example.com",
        "auth": "none",
        "response_format": "json",
        "max_flatten_depth": 3,
        "endpoints": {
            "orders": {
                "path": "/orders",
                "method": "GET",
                "pagination": {
                    "strategy": "offset",
                    "limit": 2,
                    "offset_param": "offset",
                    "limit_param": "limit",
                },
            }
        },
    }

    joint = Mock()
    joint.name = "orders"
    joint.table = "orders"

    engine = Mock()

    # No pushdown plan (no LIMIT)
    pushdown = None

    adapter = RestApiAdapter()

    # Mock 3 pages of data
    page1 = _mock_response(200, [{"id": 1}, {"id": 2}])
    page2 = _mock_response(200, [{"id": 3}, {"id": 4}])
    page3 = _mock_response(200, [{"id": 5}])
    empty = _mock_response(200, [])

    request_count = [0]

    def side_effect(*args, **kwargs):
        params = kwargs.get("params", {})
        offset = params.get("offset", 0)
        request_count[0] += 1
        offset_responses = {
            0: page1,
            2: page2,
            4: page3,
        }
        return offset_responses.get(offset, empty)

    with patch("requests.Session") as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_session.__enter__.return_value = mock_session
        mock_session.__exit__.return_value = None
        mock_session.request.side_effect = side_effect

        result = adapter.read_dispatch(engine, catalog, joint, pushdown)
        table = result.material.to_arrow()

        # Should fetch all 3 pages with data (stops when page has fewer records than page size)
        assert request_count[0] == 3, f"Expected 3 HTTP requests, got {request_count[0]}"

        # Should have all 5 records
        assert table.num_rows == 5
