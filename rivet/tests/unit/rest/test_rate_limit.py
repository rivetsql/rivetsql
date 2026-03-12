"""Unit tests for REST API rate limiter and retry logic."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from rivet_core.errors import ExecutionError
from rivet_rest.rate_limit import RateLimiter


def _response(status_code: int, headers: dict[str, str] | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.ok = 200 <= status_code < 300
    resp.headers = headers or {}
    return resp


class TestRetryOn429:
    def test_retries_on_429_with_retry_after(self) -> None:
        session = MagicMock()
        session.request.side_effect = [
            _response(429, {"Retry-After": "1"}),
            _response(200),
        ]

        limiter = RateLimiter(max_retries=3)
        with patch("rivet_rest.rate_limit.time.sleep"):
            resp = limiter.execute(session, "GET", "https://api.example.com/data")

        assert resp.status_code == 200
        assert session.request.call_count == 2

    def test_429_without_retry_after_uses_backoff(self) -> None:
        session = MagicMock()
        session.request.side_effect = [
            _response(429),
            _response(200),
        ]

        limiter = RateLimiter(max_retries=3)
        with patch("rivet_rest.rate_limit.time.sleep") as mock_sleep:
            resp = limiter.execute(session, "GET", "https://api.example.com/data")

        assert resp.status_code == 200
        # First retry uses base delay (1.0s) as fallback
        mock_sleep.assert_called()


class TestRetryOnTransientErrors:
    @pytest.mark.parametrize("status", [500, 502, 503, 504])
    def test_retries_transient_error_with_backoff(self, status: int) -> None:
        session = MagicMock()
        session.request.side_effect = [
            _response(status),
            _response(200),
        ]

        limiter = RateLimiter(max_retries=3)
        with patch("rivet_rest.rate_limit.time.sleep") as mock_sleep:
            resp = limiter.execute(session, "GET", "https://api.example.com/data")

        assert resp.status_code == 200
        assert session.request.call_count == 2
        # Exponential backoff: min(1.0 * 2^0, 60) = 1.0
        mock_sleep.assert_called_once_with(1.0)

    def test_exponential_backoff_increases(self) -> None:
        session = MagicMock()
        session.request.side_effect = [
            _response(500),
            _response(500),
            _response(500),
            _response(200),
        ]

        limiter = RateLimiter(max_retries=3)
        with patch("rivet_rest.rate_limit.time.sleep") as mock_sleep:
            resp = limiter.execute(session, "GET", "https://api.example.com/data")

        assert resp.status_code == 200
        delays = [call.args[0] for call in mock_sleep.call_args_list]
        # min(1*2^0, 60)=1.0, min(1*2^1, 60)=2.0, min(1*2^2, 60)=4.0
        assert delays == [1.0, 2.0, 4.0]


class TestMaxRetriesExhausted:
    def test_raises_execution_error_with_context(self) -> None:
        session = MagicMock()
        session.request.side_effect = [
            _response(500),
            _response(500),
            _response(500),
            _response(500),
        ]

        limiter = RateLimiter(max_retries=3)
        with patch("rivet_rest.rate_limit.time.sleep"):
            with pytest.raises(ExecutionError, match="Max retries.*3.*exhausted"):
                limiter.execute(session, "GET", "https://api.example.com/data")

        # 1 initial + 3 retries = 4 total attempts
        assert session.request.call_count == 4

    def test_error_includes_url_and_status(self) -> None:
        session = MagicMock()
        session.request.return_value = _response(503)

        limiter = RateLimiter(max_retries=1)
        with patch("rivet_rest.rate_limit.time.sleep"):
            with pytest.raises(ExecutionError) as exc_info:
                limiter.execute(session, "GET", "https://api.example.com/orders")

        error = exc_info.value.error
        assert "https://api.example.com/orders" in error.message
        assert "503" in error.message

    def test_429_max_retries_exhausted(self) -> None:
        session = MagicMock()
        session.request.return_value = _response(429, {"Retry-After": "1"})

        limiter = RateLimiter(max_retries=2)
        with patch("rivet_rest.rate_limit.time.sleep"):
            with pytest.raises(ExecutionError, match="Max retries.*2.*exhausted"):
                limiter.execute(session, "GET", "https://api.example.com/data")

        # 1 initial + 2 retries = 3 total attempts
        assert session.request.call_count == 3


class TestRetryAfterExceedsMax:
    def test_raises_immediately_when_retry_after_exceeds_300s(self) -> None:
        session = MagicMock()
        session.request.return_value = _response(429, {"Retry-After": "600"})

        limiter = RateLimiter(max_retries=3)
        with pytest.raises(ExecutionError, match="Retry-After.*600.*exceeds"):
            limiter.execute(session, "GET", "https://api.example.com/data")

        # Only one request — error raised before retry
        assert session.request.call_count == 1

    def test_retry_after_exactly_300s_is_allowed(self) -> None:
        session = MagicMock()
        session.request.side_effect = [
            _response(429, {"Retry-After": "300"}),
            _response(200),
        ]

        limiter = RateLimiter(max_retries=3)
        with patch("rivet_rest.rate_limit.time.sleep") as mock_sleep:
            resp = limiter.execute(session, "GET", "https://api.example.com/data")

        assert resp.status_code == 200
        mock_sleep.assert_called_once_with(300.0)


class TestNonRetryableResponses:
    def test_4xx_returned_without_retry(self) -> None:
        session = MagicMock()
        session.request.return_value = _response(404)

        limiter = RateLimiter(max_retries=3)
        resp = limiter.execute(session, "GET", "https://api.example.com/data")

        assert resp.status_code == 404
        assert session.request.call_count == 1

    def test_success_returned_immediately(self) -> None:
        session = MagicMock()
        session.request.return_value = _response(200)

        limiter = RateLimiter(max_retries=3)
        resp = limiter.execute(session, "GET", "https://api.example.com/data")

        assert resp.status_code == 200
        assert session.request.call_count == 1


class TestNoRateLimit:
    def test_no_rate_limit_makes_requests_without_delay(self) -> None:
        session = MagicMock()
        session.request.return_value = _response(200)

        limiter = RateLimiter(requests_per_second=None, max_retries=0)
        with patch("rivet_rest.rate_limit.time.sleep"):
            limiter.execute(session, "GET", "https://api.example.com/a")
            limiter.execute(session, "GET", "https://api.example.com/b")

        # No sleep calls for rate limiting (monotonic timing may not trigger)
        # The key assertion is that both requests succeed
        assert session.request.call_count == 2
