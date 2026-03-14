"""Pagination iterators for REST API catalog plugin.

Each strategy implements the ``Paginator`` protocol — an ``iterate`` method
that yields ``requests.Response`` objects, handling page advancement and
stop conditions automatically.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from typing import Any, Protocol

import requests

from rivet_core.errors import ExecutionError, PluginValidationError, RivetError, plugin_error


class RateLimiterLike(Protocol):
    """Minimal protocol for the rate limiter dependency."""

    def execute(
        self,
        session: requests.Session,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> requests.Response: ...


class Paginator(Protocol):
    """Protocol for pagination strategies."""

    def iterate(
        self,
        session: requests.Session,
        url: str,
        params: dict[str, Any],
        headers: dict[str, str],
        body: Any | None,
        method: str,
        timeout: int,
        rate_limiter: RateLimiterLike | None,
        response_path: str | None = None,
    ) -> Iterator[requests.Response]: ...


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_records_from_response(data: Any, response_path: str | None) -> list[Any]:
    """Extract records from response data using optional response_path.

    Args:
        data: The JSON response data
        response_path: Dot-separated path to records (e.g., "results" or "data.items")

    Returns:
        List of records, or empty list if extraction fails
    """
    if response_path:
        current = data
        for part in response_path.split("."):
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return []
        return current if isinstance(current, list) else []
    else:
        return data if isinstance(data, list) else []


def _do_request(
    session: requests.Session,
    method: str,
    url: str,
    rate_limiter: RateLimiterLike | None,
    **kwargs: Any,
) -> requests.Response:
    """Execute a single HTTP request, optionally through a rate limiter."""
    if rate_limiter is not None:
        return rate_limiter.execute(session, method, url, **kwargs)
    return session.request(method, url, **kwargs)


def _raise_page_error(
    url: str,
    status_code: int,
    page_context: str,
) -> None:
    """Raise an ``ExecutionError`` for an HTTP error during pagination."""
    raise ExecutionError(
        RivetError(
            code="RVT-501",
            message=(f"HTTP {status_code} during pagination ({page_context}) for {url}"),
            context={"url": url, "status_code": status_code, "page_context": page_context},
            remediation="Check API pagination behaviour and endpoint configuration.",
        )
    )


def _check_response(
    resp: requests.Response,
    url: str,
    page_context: str,
) -> None:
    """Raise on non-2xx status codes with pagination context."""
    if not resp.ok:
        _raise_page_error(url, resp.status_code, page_context)


# ---------------------------------------------------------------------------
# Strategy implementations
# ---------------------------------------------------------------------------


class NoPaginator:
    """Single request — no pagination."""

    def iterate(
        self,
        session: requests.Session,
        url: str,
        params: dict[str, Any],
        headers: dict[str, str],
        body: Any | None,
        method: str,
        timeout: int,
        rate_limiter: RateLimiterLike | None,
        response_path: str | None = None,
    ) -> Iterator[requests.Response]:
        kwargs: dict[str, Any] = {"params": params, "headers": headers, "timeout": timeout}
        if body is not None:
            kwargs["json"] = body
        resp = _do_request(session, method, url, rate_limiter, **kwargs)
        _check_response(resp, url, "single request")
        yield resp


class OffsetPaginator:
    """Offset/limit pagination.

    Increments ``offset`` by ``limit`` after each page.  Stops when a page
    returns fewer records than ``limit``, an empty result, or when an optional
    ``has_more_field`` indicates no more pages.
    """

    def __init__(
        self,
        limit: int = 100,
        offset_param: str = "offset",
        limit_param: str = "limit",
        has_more_field: str | None = None,
    ) -> None:
        self._limit = limit
        self._offset_param = offset_param
        self._limit_param = limit_param
        self._has_more_field = has_more_field

    def iterate(
        self,
        session: requests.Session,
        url: str,
        params: dict[str, Any],
        headers: dict[str, str],
        body: Any | None,
        method: str,
        timeout: int,
        rate_limiter: RateLimiterLike | None,
        response_path: str | None = None,
    ) -> Iterator[requests.Response]:
        offset = 0
        while True:
            page_params = {**params, self._offset_param: offset, self._limit_param: self._limit}
            kwargs: dict[str, Any] = {"params": page_params, "headers": headers, "timeout": timeout}
            if body is not None:
                kwargs["json"] = body
            resp = _do_request(session, method, url, rate_limiter, **kwargs)
            _check_response(resp, url, f"offset={offset}")
            yield resp

            # Determine record count from response to decide whether to continue
            try:
                data = resp.json()
            except (json.JSONDecodeError, ValueError):
                # Non-JSON response — can't determine page size, stop
                return

            records = _extract_records_from_response(data, response_path)

            # Check has_more_field if configured (e.g., "next" field in PokeAPI)
            if self._has_more_field and isinstance(data, dict):
                has_more = data.get(self._has_more_field)
                if not has_more:  # None, empty string, or falsy
                    return

            # Also stop if we got fewer records than requested
            if len(records) < self._limit:
                return
            offset += self._limit


class CursorPaginator:
    """Cursor-based pagination.

    Extracts the next cursor value from a configured response field and
    passes it as a query parameter.  Stops when the cursor is null or absent.
    """

    def __init__(
        self,
        cursor_field: str = "next_cursor",
        cursor_param: str = "cursor",
    ) -> None:
        self._cursor_field = cursor_field
        self._cursor_param = cursor_param

    def iterate(
        self,
        session: requests.Session,
        url: str,
        params: dict[str, Any],
        headers: dict[str, str],
        body: Any | None,
        method: str,
        timeout: int,
        rate_limiter: RateLimiterLike | None,
        response_path: str | None = None,
    ) -> Iterator[requests.Response]:
        cursor: str | None = None
        page_num = 0
        while True:
            page_params = dict(params)
            if cursor is not None:
                page_params[self._cursor_param] = cursor
            kwargs: dict[str, Any] = {"params": page_params, "headers": headers, "timeout": timeout}
            if body is not None:
                kwargs["json"] = body
            resp = _do_request(session, method, url, rate_limiter, **kwargs)
            context = f"cursor={cursor!r}" if cursor else "page=0"
            _check_response(resp, url, context)
            yield resp

            # Extract next cursor from response
            try:
                data = resp.json()
            except (json.JSONDecodeError, ValueError):
                return

            if not isinstance(data, dict):
                return

            cursor = data.get(self._cursor_field)
            if not cursor:  # Stop if cursor is None, empty string, or falsy
                return
            page_num += 1


class PageNumberPaginator:
    """Page-number pagination.

    Sends a ``page`` query parameter starting at ``start_page``, incrementing
    by 1 after each page.  Stops when a page returns fewer records than
    ``page_size`` or an empty result.
    """

    def __init__(
        self,
        page_size: int = 100,
        page_param: str = "page",
        start_page: int = 1,
        limit_param: str = "limit",
    ) -> None:
        self._page_size = page_size
        self._page_param = page_param
        self._start_page = start_page
        self._limit_param = limit_param

    def iterate(
        self,
        session: requests.Session,
        url: str,
        params: dict[str, Any],
        headers: dict[str, str],
        body: Any | None,
        method: str,
        timeout: int,
        rate_limiter: RateLimiterLike | None,
        response_path: str | None = None,
    ) -> Iterator[requests.Response]:
        page = self._start_page
        while True:
            page_params = {**params, self._page_param: page, self._limit_param: self._page_size}
            kwargs: dict[str, Any] = {"params": page_params, "headers": headers, "timeout": timeout}
            if body is not None:
                kwargs["json"] = body
            resp = _do_request(session, method, url, rate_limiter, **kwargs)
            _check_response(resp, url, f"page={page}")
            yield resp

            # Determine record count from response to decide whether to continue
            try:
                data = resp.json()
            except (json.JSONDecodeError, ValueError):
                return

            records = _extract_records_from_response(data, response_path)
            if len(records) < self._page_size:
                return
            page += 1


class LinkHeaderPaginator:
    """Link-header pagination (RFC 8288).

    Follows the ``next`` relation in the HTTP ``Link`` header.  Stops when
    no ``next`` link is present.
    """

    def iterate(
        self,
        session: requests.Session,
        url: str,
        params: dict[str, Any],
        headers: dict[str, str],
        body: Any | None,
        method: str,
        timeout: int,
        rate_limiter: RateLimiterLike | None,
        response_path: str | None = None,
    ) -> Iterator[requests.Response]:
        current_url: str | None = url
        page_num = 0
        while current_url:
            # Only use params on first request; subsequent URLs are fully qualified
            kwargs: dict[str, Any] = {"headers": headers, "timeout": timeout}
            if page_num == 0:
                kwargs["params"] = params
            if body is not None:
                kwargs["json"] = body
            resp = _do_request(session, method, current_url, rate_limiter, **kwargs)
            _check_response(resp, current_url, f"page={page_num}")
            yield resp

            # Extract next URL from resp.links (parsed by requests) or Link header
            current_url = None
            if hasattr(resp, "links") and resp.links and "next" in resp.links:
                current_url = resp.links["next"].get("url")
            else:
                # Fallback: parse Link header manually
                link_header = resp.headers.get("Link", "")
                for part in link_header.split(","):
                    if 'rel="next"' in part:
                        match = re.search(r"<([^>]+)>", part)
                        if match:
                            current_url = match.group(1)
                            break
            page_num += 1


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def create_paginator(config: dict[str, Any] | None) -> Paginator:
    """Create a paginator from an endpoint pagination configuration.

    Args:
        config: Pagination config dict with a ``strategy`` key and
            strategy-specific parameters.  ``None`` or missing ``strategy``
            defaults to :class:`NoPaginator`.

    Returns:
        A ``Paginator`` instance.

    Raises:
        PluginValidationError: If the strategy name is not recognised.
    """
    if config is None:
        return NoPaginator()

    strategy = config.get("strategy", "none")

    if strategy == "none":
        return NoPaginator()

    if strategy == "offset":
        return OffsetPaginator(
            limit=config.get("limit", 100),
            offset_param=config.get("offset_param", "offset"),
            limit_param=config.get("limit_param", "limit"),
        )

    if strategy == "cursor":
        return CursorPaginator(
            cursor_field=config.get("cursor_field", "next_cursor"),
            cursor_param=config.get("cursor_param", "cursor"),
        )

    if strategy == "page_number":
        return PageNumberPaginator(
            page_size=config.get("limit", 100),
            page_param=config.get("page_param", "page"),
            start_page=config.get("start_page", 1),
            limit_param=config.get("limit_param", "limit"),
        )

    if strategy == "link_header":
        return LinkHeaderPaginator()

    raise PluginValidationError(
        plugin_error(
            "RVT-201",
            f"Unrecognized pagination strategy: '{strategy}'",
            plugin_name="rivet_rest",
            plugin_type="catalog",
            remediation="Valid pagination strategies: none, offset, cursor, page_number, link_header",
        )
    )
