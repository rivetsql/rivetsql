"""Rate limiter and retry logic for REST API catalog plugin.

Enforces minimum intervals between requests, handles HTTP 429 (Too Many
Requests) with ``Retry-After`` support, and retries transient server errors
(500/502/503/504) with exponential backoff.
"""

from __future__ import annotations

import time
from typing import Any

import requests

from rivet_core.errors import ExecutionError, RivetError

_TRANSIENT_STATUS_CODES = frozenset({500, 502, 503, 504})
_MAX_RETRY_AFTER = 300  # seconds — refuse to wait longer than this
_MAX_BACKOFF = 60  # seconds — cap for exponential backoff
_BASE_DELAY = 1.0  # seconds — base for exponential backoff


class RateLimiter:
    """Rate limiter with retry and exponential backoff.

    Parameters:
        requests_per_second: Maximum sustained request rate.  ``None``
            disables rate limiting.
        burst: Burst allowance (currently used for documentation;
            the implementation enforces a simple minimum interval).
        max_retries: Maximum number of retry attempts for transient
            errors and 429 responses.
    """

    def __init__(
        self,
        requests_per_second: float | None = None,
        burst: int = 1,
        max_retries: int = 3,
    ) -> None:
        self._requests_per_second = requests_per_second
        self._burst = burst
        self._max_retries = max_retries
        self._min_interval: float = 1.0 / requests_per_second if requests_per_second else 0.0
        self._last_request_time: float = 0.0

    def execute(
        self,
        session: requests.Session,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> requests.Response:
        """Execute an HTTP request with rate limiting and retry.

        Enforces a minimum interval between consecutive requests when
        ``requests_per_second`` is configured.  Retries on HTTP 429
        (respecting ``Retry-After``) and transient 5xx errors with
        exponential backoff.

        Args:
            session: The ``requests.Session`` to use.
            method: HTTP method (GET, POST, etc.).
            url: Request URL.
            **kwargs: Additional keyword arguments forwarded to
                ``session.request``.

        Returns:
            The successful ``requests.Response``.

        Raises:
            ExecutionError: When max retries are exhausted or
                ``Retry-After`` exceeds 300 seconds.
        """
        last_status: int | None = None

        for attempt in range(self._max_retries + 1):
            self._wait_for_interval()
            resp = session.request(method, url, **kwargs)
            self._last_request_time = time.monotonic()
            last_status = resp.status_code

            if resp.status_code == 429:
                delay = self._parse_retry_after(resp, url)
                if attempt < self._max_retries:
                    time.sleep(delay)
                    continue
                # Max retries exhausted on 429
                break

            if resp.status_code in _TRANSIENT_STATUS_CODES:
                if attempt < self._max_retries:
                    delay = min(_BASE_DELAY * (2**attempt), _MAX_BACKOFF)
                    time.sleep(delay)
                    continue
                # Max retries exhausted on transient error
                break

            # Success or non-retryable error — return as-is
            return resp

        raise ExecutionError(
            RivetError(
                code="RVT-501",
                message=(
                    f"Max retries ({self._max_retries}) exhausted for {url} "
                    f"(last status: {last_status})"
                ),
                context={
                    "url": url,
                    "status_code": last_status,
                    "attempts": self._max_retries + 1,
                },
                remediation=(
                    "Check the API endpoint health and rate limit configuration. "
                    "Consider increasing max_retries or reducing requests_per_second."
                ),
            )
        )

    def _wait_for_interval(self) -> None:
        """Sleep if needed to enforce the minimum request interval."""
        if self._min_interval <= 0:
            return
        elapsed = time.monotonic() - self._last_request_time
        remaining = self._min_interval - elapsed
        if remaining > 0:
            time.sleep(remaining)

    @staticmethod
    def _parse_retry_after(resp: requests.Response, url: str) -> float:
        """Parse the ``Retry-After`` header, falling back to 1s.

        Raises:
            ExecutionError: If the ``Retry-After`` value exceeds 300 seconds.
        """
        header = resp.headers.get("Retry-After")
        if header is None:
            return _BASE_DELAY

        try:
            delay = float(header)
        except (ValueError, TypeError):
            return _BASE_DELAY

        if delay > _MAX_RETRY_AFTER:
            raise ExecutionError(
                RivetError(
                    code="RVT-501",
                    message=(
                        f"Retry-After header ({delay}s) exceeds maximum "
                        f"allowed wait ({_MAX_RETRY_AFTER}s) for {url}"
                    ),
                    context={"url": url, "retry_after": delay},
                    remediation=(
                        "The API is requesting an excessively long wait. "
                        "Check rate limit configuration or try again later."
                    ),
                )
            )

        return max(delay, 0.0)
