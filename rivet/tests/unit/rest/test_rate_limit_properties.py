"""Property-based tests for rate limiter interval enforcement.

- Property 14: Rate limiter interval enforcement — generate random rps
  values, verify minimum interval between requests.
  Validates: Requirement 8.2
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_rest.rate_limit import RateLimiter


def _ok_response() -> MagicMock:
    resp = MagicMock()
    resp.status_code = 200
    resp.ok = True
    return resp


# ---------------------------------------------------------------------------
# Feature: rest-api-catalog, Property 14: Rate limiter interval enforcement
# ---------------------------------------------------------------------------


@settings(max_examples=100)
@given(rps=st.floats(min_value=1.0, max_value=100.0, allow_nan=False, allow_infinity=False))
def test_property14_rate_limiter_interval_enforcement(rps: float) -> None:
    """For any requests_per_second value, the rate limiter shall enforce
    a minimum interval of 1/rps seconds between consecutive requests.
    """
    limiter = RateLimiter(requests_per_second=rps, max_retries=0)
    expected_interval = 1.0 / rps

    session = MagicMock()
    session.request.return_value = _ok_response()

    # Make two requests and measure the gap
    limiter.execute(session, "GET", "https://api.example.com/data")
    start = time.monotonic()
    limiter.execute(session, "GET", "https://api.example.com/data")
    elapsed = time.monotonic() - start

    # Allow a small tolerance for timing jitter (sleep is not perfectly precise)
    tolerance = 0.02
    assert elapsed >= expected_interval - tolerance, (
        f"Interval {elapsed:.4f}s < expected {expected_interval:.4f}s (rps={rps})"
    )
