"""Property tests for RestApiAdapter behavior.

Property 11: Deferred materialization caching
Property 12: read_dispatch returns deferred without HTTP
Property 15: Pagination consolidation
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.models import Catalog, Joint, Material
from rivet_core.optimizer import (
    PredicatePushdownResult,
    PushdownPlan,
)
from rivet_rest.adapter import RestApiAdapter, RestApiDeferredRef

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------


@st.composite
def endpoint_config_strategy(draw: Any) -> dict[str, Any]:
    """Generate random endpoint configurations."""
    path = draw(
        st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=("L", "N")))
    )
    method = draw(st.sampled_from(["GET", "POST", "PUT"]))
    return {
        "path": f"/{path}",
        "method": method,
        "params": {},
        "headers": {},
        "response_path": None,
        "pagination": None,
    }


@st.composite
def pushdown_plan_strategy(draw: Any) -> PushdownPlan | None:
    """Generate random pushdown plans."""
    if draw(st.booleans()):
        return None
    return PushdownPlan(
        predicates=PredicatePushdownResult(pushed=[], residual=[]),
        projections=[],
        limit=draw(st.one_of(st.none(), st.integers(min_value=1, max_value=1000))),
        casts=[],
    )


# ---------------------------------------------------------------------------
# Property 11: Deferred materialization caching
# ---------------------------------------------------------------------------


@settings(max_examples=100)
@given(st.integers(min_value=1, max_value=100))
def test_property_deferred_materialization_caching(num_rows: int) -> None:
    """Property 11: Deferred materialization caching.

    Create a ref, call to_arrow() twice, verify same object returned
    and HTTP execution happens only once.
    """
    # Mock the HTTP execution
    mock_table = pa.table({"id": list(range(num_rows)), "value": ["test"] * num_rows})

    session_config = {
        "auth": "none",
        "default_headers": {},
        "timeout": 30,
    }
    endpoint_config = {
        "path": "/test",
        "method": "GET",
        "params": {},
        "headers": {},
        "response_path": None,
        "pagination": None,
    }

    ref = RestApiDeferredRef(
        session_config=session_config,
        endpoint_config=endpoint_config,
        query_params={},
        rate_limit_config=None,
        max_flatten_depth=3,
        response_format="json",
        base_url="https://api.example.com",
    )

    # Mock the _execute method to track calls
    execute_call_count = 0

    def mock_execute() -> pa.Table:
        nonlocal execute_call_count
        execute_call_count += 1
        return mock_table

    ref._execute = mock_execute  # type: ignore[method-assign]

    # First call should execute
    result1 = ref.to_arrow()
    assert execute_call_count == 1
    assert result1.num_rows == num_rows

    # Second call should return cached result without executing
    result2 = ref.to_arrow()
    assert execute_call_count == 1  # Still 1, not 2
    assert result2 is result1  # Same object


# ---------------------------------------------------------------------------
# Property 12: read_dispatch returns deferred without HTTP
# ---------------------------------------------------------------------------


@settings(max_examples=100)
@given(endpoint_config_strategy(), pushdown_plan_strategy())
def test_property_read_dispatch_returns_deferred_without_http(
    endpoint_config: dict[str, Any],
    pushdown: PushdownPlan | None,
) -> None:
    """Property 12: read_dispatch returns deferred without HTTP.

    Generate endpoint configs and pushdown plans, verify deferred state
    and correct residuals without making HTTP requests.
    """
    adapter = RestApiAdapter()

    # Create mock catalog and joint
    catalog = Catalog(
        name="test_api",
        type="rest_api",
        options={
            "base_url": "https://api.example.com",
            "auth": "none",
            "endpoints": {
                "test_table": endpoint_config,
            },
            "max_flatten_depth": 3,
            "response_format": "json",
        },
    )

    joint = Joint(
        name="test_joint",
        joint_type="source",
        catalog="test_api",
        table="test_table",
    )

    # Mock engine
    engine = Mock()

    # Call read_dispatch - should NOT make HTTP requests
    with patch("rivet_rest.adapter._create_session") as mock_session:
        result = adapter.read_dispatch(engine, catalog, joint, pushdown)

        # Verify no session was created (no HTTP requests)
        mock_session.assert_not_called()

    # Verify result structure
    assert isinstance(result.material, Material)
    assert result.material.state == "deferred"
    assert result.material.materialized_ref is not None
    assert isinstance(result.material.materialized_ref, RestApiDeferredRef)

    # Verify residual is present
    assert result.residual is not None


# ---------------------------------------------------------------------------
# Property 15: Pagination consolidation
# ---------------------------------------------------------------------------


@settings(max_examples=100)
@given(
    st.integers(min_value=2, max_value=10),  # num_pages
    st.integers(min_value=1, max_value=50),  # rows_per_page
)
def test_property_pagination_consolidation(num_pages: int, rows_per_page: int) -> None:
    """Property 15: Pagination consolidation.

    Generate multi-page responses, verify single consolidated table.
    """
    # Create mock responses for multiple pages
    mock_responses = []
    expected_total_rows = num_pages * rows_per_page

    for page_num in range(num_pages):
        start_id = page_num * rows_per_page
        records = [
            {"id": start_id + i, "value": f"page{page_num}_row{i}"} for i in range(rows_per_page)
        ]
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = records
        mock_responses.append(mock_resp)

    session_config = {
        "auth": "none",
        "default_headers": {},
        "timeout": 30,
    }
    endpoint_config = {
        "path": "/test",
        "method": "GET",
        "params": {},
        "headers": {},
        "response_path": None,
        "pagination": {
            "strategy": "offset",
            "limit": rows_per_page,
        },
    }

    ref = RestApiDeferredRef(
        session_config=session_config,
        endpoint_config=endpoint_config,
        query_params={},
        rate_limit_config=None,
        max_flatten_depth=3,
        response_format="json",
        base_url="https://api.example.com",
    )

    # Mock the paginator to return our mock responses
    with patch("rivet_rest.adapter.create_paginator") as mock_create_paginator:
        mock_paginator = MagicMock()
        mock_paginator.iterate.return_value = iter(mock_responses)
        mock_create_paginator.return_value = mock_paginator

        with patch("rivet_rest.adapter._create_session") as mock_create_session:
            mock_session = MagicMock()
            mock_create_session.return_value = mock_session

            # Execute and get consolidated table
            table = ref.to_arrow()

    # Verify single consolidated table
    assert isinstance(table, pa.Table)
    assert table.num_rows == expected_total_rows

    # Verify all IDs are present
    ids = table.column("id").to_pylist()
    assert len(ids) == expected_total_rows
    assert sorted(ids) == list(range(expected_total_rows))


# ---------------------------------------------------------------------------
# Additional property: Caching preserves schema
# ---------------------------------------------------------------------------


@settings(max_examples=50)
@given(st.integers(min_value=1, max_value=100))
def test_property_caching_preserves_schema(num_rows: int) -> None:
    """Verify that cached results preserve schema information."""
    mock_table = pa.table(
        {
            "id": list(range(num_rows)),
            "name": [f"name_{i}" for i in range(num_rows)],
            "value": [float(i) * 1.5 for i in range(num_rows)],
        }
    )

    session_config = {
        "auth": "none",
        "default_headers": {},
        "timeout": 30,
    }
    endpoint_config = {
        "path": "/test",
        "method": "GET",
        "params": {},
        "headers": {},
        "response_path": None,
        "pagination": None,
    }

    ref = RestApiDeferredRef(
        session_config=session_config,
        endpoint_config=endpoint_config,
        query_params={},
        rate_limit_config=None,
        max_flatten_depth=3,
        response_format="json",
        base_url="https://api.example.com",
    )

    ref._execute = lambda: mock_table  # type: ignore[method-assign]

    # Get schema from first call
    schema1 = ref.schema
    assert len(schema1.columns) == 3

    # Get schema from second call (should use cache)
    schema2 = ref.schema
    assert len(schema2.columns) == 3

    # Verify schemas are equivalent
    assert [c.name for c in schema1.columns] == [c.name for c in schema2.columns]
    assert [c.type for c in schema1.columns] == [c.type for c in schema2.columns]
