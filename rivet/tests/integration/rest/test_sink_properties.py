"""Property-based tests for RestApiSink batch grouping.

Property 13: Sink batch grouping — generate random Arrow tables and batch
sizes, verify correct batching.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.models import Catalog, Joint, Material
from rivet_rest.sink import RestApiSink


@st.composite
def arrow_table_strategy(draw: Any) -> pa.Table:
    """Generate random Arrow tables with varying row counts."""
    num_rows = draw(st.integers(min_value=1, max_value=100))
    num_cols = draw(st.integers(min_value=1, max_value=5))

    columns = {}
    for i in range(num_cols):
        col_name = f"col_{i}"
        # Generate simple integer columns
        columns[col_name] = pa.array(list(range(num_rows)), type=pa.int64())

    return pa.table(columns)


@given(
    table=arrow_table_strategy(),
    batch_size=st.integers(min_value=1, max_value=20),
)
@settings(max_examples=100, deadline=None)
def test_sink_batch_grouping_property(table: pa.Table, batch_size: int) -> None:
    """Property 13: Sink batch grouping correctness.

    For any Arrow table and batch size, verify that:
    1. The number of HTTP requests equals ceil(num_rows / batch_size)
    2. Each batch contains the correct number of rows (except possibly the last)
    3. All rows are written exactly once
    4. Single-row batches are sent as JSON objects, multi-row as arrays
    """
    num_rows = table.num_rows

    # Create mock catalog and joint
    catalog = MagicMock(spec=Catalog)
    catalog.name = "test_api"
    catalog.options = {
        "base_url": "https://api.example.com",
        "auth": "none",
        "endpoints": {
            "test_endpoint": {
                "path": "/data",
                "batch_size": batch_size,
            }
        },
        "max_retries": 0,  # No retries for property tests
    }

    joint = MagicMock(spec=Joint)
    joint.name = "test_endpoint"
    joint.table = "test_endpoint"

    material = MagicMock(spec=Material)
    material.to_arrow.return_value = table

    # Mock HTTP requests
    with patch("rivet_rest.sink.requests.Session") as mock_session_class:
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session

        # Mock successful responses
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_session.request.return_value = mock_response

        # Execute write
        sink = RestApiSink()
        sink.write(catalog, joint, material, strategy="append")

        # Verify number of requests
        expected_num_requests = (num_rows + batch_size - 1) // batch_size
        assert mock_session.request.call_count == expected_num_requests

        # Verify batch sizes and structure
        all_written_rows = []
        for call_idx, call in enumerate(mock_session.request.call_args_list):
            kwargs = call[1]
            body = kwargs["json"]

            # Check structure: single row = object, multiple rows = array
            if isinstance(body, list):
                batch_rows = body
                assert len(batch_rows) > 1 or call_idx == expected_num_requests - 1
            else:
                # Single row as object
                batch_rows = [body]
                assert len(batch_rows) == 1

            # Verify batch size (except possibly last batch)
            if call_idx < expected_num_requests - 1:
                assert len(batch_rows) == batch_size
            else:
                # Last batch may be smaller
                expected_last_batch = num_rows - (call_idx * batch_size)
                assert len(batch_rows) == expected_last_batch

            all_written_rows.extend(batch_rows)

        # Verify all rows written exactly once
        assert len(all_written_rows) == num_rows


def test_sink_single_row_as_object() -> None:
    """Verify that a single-row batch is sent as a JSON object, not an array."""
    table = pa.table({"id": [1], "name": ["Alice"]})

    catalog = MagicMock(spec=Catalog)
    catalog.name = "test_api"
    catalog.options = {
        "base_url": "https://api.example.com",
        "auth": "none",
        "endpoints": {
            "test_endpoint": {
                "path": "/data",
                "batch_size": 1,
            }
        },
        "max_retries": 0,
    }

    joint = MagicMock(spec=Joint)
    joint.name = "test_endpoint"
    joint.table = "test_endpoint"

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

        # Verify single call with object (not array)
        assert mock_session.request.call_count == 1
        call_kwargs = mock_session.request.call_args_list[0][1]
        body = call_kwargs["json"]

        assert isinstance(body, dict)
        assert body == {"id": 1, "name": "Alice"}


def test_sink_multiple_rows_as_array() -> None:
    """Verify that multi-row batches are sent as JSON arrays."""
    table = pa.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

    catalog = MagicMock(spec=Catalog)
    catalog.name = "test_api"
    catalog.options = {
        "base_url": "https://api.example.com",
        "auth": "none",
        "endpoints": {
            "test_endpoint": {
                "path": "/data",
                "batch_size": 3,
            }
        },
        "max_retries": 0,
    }

    joint = MagicMock(spec=Joint)
    joint.name = "test_endpoint"
    joint.table = "test_endpoint"

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

        # Verify single call with array
        assert mock_session.request.call_count == 1
        call_kwargs = mock_session.request.call_args_list[0][1]
        body = call_kwargs["json"]

        assert isinstance(body, list)
        assert len(body) == 3
        assert body[0] == {"id": 1, "name": "Alice"}
        assert body[1] == {"id": 2, "name": "Bob"}
        assert body[2] == {"id": 3, "name": "Charlie"}
