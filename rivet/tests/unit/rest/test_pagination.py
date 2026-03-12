"""Unit tests for REST API pagination strategies."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from rivet_core.errors import ExecutionError
from rivet_rest.pagination import (
    CursorPaginator,
    LinkHeaderPaginator,
    NoPaginator,
    OffsetPaginator,
    PageNumberPaginator,
    create_paginator,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_session() -> MagicMock:
    return MagicMock()


def _json_response(
    data: Any,
    status_code: int = 200,
    links: dict[str, dict[str, str]] | None = None,
) -> MagicMock:
    resp = MagicMock()
    resp.ok = 200 <= status_code < 300
    resp.status_code = status_code
    resp.json.return_value = data
    resp.links = links or {}
    return resp


def _error_response(status_code: int = 500) -> MagicMock:
    resp = MagicMock()
    resp.ok = False
    resp.status_code = status_code
    resp.links = {}
    return resp


_BASE = "https://api.example.com/v1/items"
_PARAMS: dict[str, Any] = {}
_HEADERS: dict[str, str] = {}


# ---------------------------------------------------------------------------
# NoPaginator
# ---------------------------------------------------------------------------


class TestNoPaginator:
    def test_single_request_yielded(self) -> None:
        session = _mock_session()
        resp = _json_response({"items": [1, 2, 3]})
        session.request.return_value = resp

        pages = list(
            NoPaginator().iterate(session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None)
        )
        assert len(pages) == 1
        assert pages[0] is resp

    def test_body_forwarded_as_json(self) -> None:
        session = _mock_session()
        session.request.return_value = _json_response([])

        body = {"query": "test"}
        list(NoPaginator().iterate(session, _BASE, _PARAMS, _HEADERS, body, "POST", 30, None))

        _, kwargs = session.request.call_args
        assert kwargs["json"] == body

    def test_http_error_raises(self) -> None:
        session = _mock_session()
        session.request.return_value = _error_response(403)

        with pytest.raises(ExecutionError, match="HTTP 403"):
            list(NoPaginator().iterate(session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None))


# ---------------------------------------------------------------------------
# OffsetPaginator
# ---------------------------------------------------------------------------


class TestOffsetPaginator:
    def test_iterates_multiple_pages(self) -> None:
        session = _mock_session()
        page1 = _json_response([{"id": i} for i in range(10)])
        page2 = _json_response([{"id": i} for i in range(10, 15)])  # fewer than limit
        session.request.side_effect = [page1, page2]

        paginator = OffsetPaginator(limit=10)
        pages = list(paginator.iterate(session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None))

        assert len(pages) == 2
        # Verify offset params sent
        call1_kwargs = session.request.call_args_list[0][1]
        assert call1_kwargs["params"]["offset"] == 0
        assert call1_kwargs["params"]["limit"] == 10
        call2_kwargs = session.request.call_args_list[1][1]
        assert call2_kwargs["params"]["offset"] == 10

    def test_stops_on_empty_result(self) -> None:
        session = _mock_session()
        session.request.return_value = _json_response([])

        pages = list(
            OffsetPaginator(limit=10).iterate(
                session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None
            )
        )
        assert len(pages) == 1

    def test_custom_param_names(self) -> None:
        session = _mock_session()
        session.request.return_value = _json_response([])

        paginator = OffsetPaginator(limit=50, offset_param="skip", limit_param="take")
        list(paginator.iterate(session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None))

        call_kwargs = session.request.call_args[1]
        assert "skip" in call_kwargs["params"]
        assert "take" in call_kwargs["params"]
        assert call_kwargs["params"]["skip"] == 0
        assert call_kwargs["params"]["take"] == 50

    def test_http_error_includes_offset_context(self) -> None:
        session = _mock_session()
        page1 = _json_response([{"id": i} for i in range(10)])
        page2 = _error_response(500)
        session.request.side_effect = [page1, page2]

        with pytest.raises(ExecutionError, match="offset=10"):
            list(
                OffsetPaginator(limit=10).iterate(
                    session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None
                )
            )

    def test_stops_on_non_json_response(self) -> None:
        session = _mock_session()
        resp = _json_response(None)
        resp.json.side_effect = ValueError("not json")
        session.request.return_value = resp

        pages = list(
            OffsetPaginator(limit=10).iterate(
                session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None
            )
        )
        assert len(pages) == 1


# ---------------------------------------------------------------------------
# CursorPaginator
# ---------------------------------------------------------------------------


class TestCursorPaginator:
    def test_iterates_until_cursor_absent(self) -> None:
        session = _mock_session()
        page1 = _json_response({"data": [1, 2], "next_cursor": "abc"})
        page2 = _json_response({"data": [3, 4], "next_cursor": "def"})
        page3 = _json_response({"data": [5]})  # no next_cursor → stop
        session.request.side_effect = [page1, page2, page3]

        paginator = CursorPaginator()
        pages = list(paginator.iterate(session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None))

        assert len(pages) == 3
        # First request has no cursor param
        call1_params = session.request.call_args_list[0][1]["params"]
        assert "cursor" not in call1_params
        # Second request has cursor=abc
        call2_params = session.request.call_args_list[1][1]["params"]
        assert call2_params["cursor"] == "abc"
        # Third request has cursor=def
        call3_params = session.request.call_args_list[2][1]["params"]
        assert call3_params["cursor"] == "def"

    def test_stops_on_null_cursor(self) -> None:
        session = _mock_session()
        session.request.return_value = _json_response({"data": [1], "next_cursor": None})

        pages = list(
            CursorPaginator().iterate(session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None)
        )
        assert len(pages) == 1

    def test_stops_on_empty_string_cursor(self) -> None:
        session = _mock_session()
        session.request.return_value = _json_response({"data": [1], "next_cursor": ""})

        pages = list(
            CursorPaginator().iterate(session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None)
        )
        assert len(pages) == 1

    def test_custom_param_names(self) -> None:
        session = _mock_session()
        page1 = _json_response({"items": [1], "continuation": "tok123"})
        page2 = _json_response({"items": [2]})
        session.request.side_effect = [page1, page2]

        paginator = CursorPaginator(cursor_field="continuation", cursor_param="after")
        list(paginator.iterate(session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None))

        call2_params = session.request.call_args_list[1][1]["params"]
        assert call2_params["after"] == "tok123"

    def test_http_error_includes_cursor_context(self) -> None:
        session = _mock_session()
        page1 = _json_response({"data": [1], "next_cursor": "abc"})
        page2 = _error_response(502)
        session.request.side_effect = [page1, page2]

        with pytest.raises(ExecutionError, match="cursor='abc'"):
            list(
                CursorPaginator().iterate(session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None)
            )

    def test_stops_on_non_dict_response(self) -> None:
        session = _mock_session()
        session.request.return_value = _json_response([1, 2, 3])

        pages = list(
            CursorPaginator().iterate(session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None)
        )
        assert len(pages) == 1


# ---------------------------------------------------------------------------
# PageNumberPaginator
# ---------------------------------------------------------------------------


class TestPageNumberPaginator:
    def test_iterates_multiple_pages(self) -> None:
        session = _mock_session()
        page1 = _json_response([{"id": i} for i in range(20)])
        page2 = _json_response([{"id": i} for i in range(20, 30)])  # fewer than page_size
        session.request.side_effect = [page1, page2]

        paginator = PageNumberPaginator(page_size=20)
        pages = list(paginator.iterate(session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None))

        assert len(pages) == 2
        call1_params = session.request.call_args_list[0][1]["params"]
        assert call1_params["page"] == 1
        assert call1_params["limit"] == 20
        call2_params = session.request.call_args_list[1][1]["params"]
        assert call2_params["page"] == 2

    def test_stops_on_empty_result(self) -> None:
        session = _mock_session()
        session.request.return_value = _json_response([])

        pages = list(
            PageNumberPaginator(page_size=10).iterate(
                session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None
            )
        )
        assert len(pages) == 1

    def test_custom_start_page_and_param_names(self) -> None:
        session = _mock_session()
        session.request.return_value = _json_response([])

        paginator = PageNumberPaginator(
            page_size=25, page_param="p", start_page=0, limit_param="per_page"
        )
        list(paginator.iterate(session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None))

        call_params = session.request.call_args[1]["params"]
        assert call_params["p"] == 0
        assert call_params["per_page"] == 25

    def test_http_error_includes_page_context(self) -> None:
        session = _mock_session()
        session.request.return_value = _error_response(503)

        with pytest.raises(ExecutionError, match="page=1"):
            list(
                PageNumberPaginator(page_size=10).iterate(
                    session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None
                )
            )


# ---------------------------------------------------------------------------
# LinkHeaderPaginator
# ---------------------------------------------------------------------------


class TestLinkHeaderPaginator:
    def test_follows_next_links(self) -> None:
        session = _mock_session()
        page1 = _json_response(
            [1, 2],
            links={"next": {"url": "https://api.example.com/v1/items?page=2"}},
        )
        page2 = _json_response(
            [3, 4],
            links={"next": {"url": "https://api.example.com/v1/items?page=3"}},
        )
        page3 = _json_response([5], links={})  # no next → stop
        session.request.side_effect = [page1, page2, page3]

        pages = list(
            LinkHeaderPaginator().iterate(session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None)
        )
        assert len(pages) == 3

    def test_stops_when_no_next_link(self) -> None:
        session = _mock_session()
        session.request.return_value = _json_response([1], links={})

        pages = list(
            LinkHeaderPaginator().iterate(session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None)
        )
        assert len(pages) == 1

    def test_params_only_on_first_request(self) -> None:
        session = _mock_session()
        page1 = _json_response(
            [1],
            links={"next": {"url": "https://api.example.com/v1/items?page=2"}},
        )
        page2 = _json_response([2], links={})
        session.request.side_effect = [page1, page2]

        initial_params = {"filter": "active"}
        list(
            LinkHeaderPaginator().iterate(
                session, _BASE, initial_params, _HEADERS, None, "GET", 30, None
            )
        )

        # First request includes params
        call1_kwargs = session.request.call_args_list[0][1]
        assert call1_kwargs["params"] == {"filter": "active"}
        # Second request does NOT include params (URL is fully qualified)
        call2_kwargs = session.request.call_args_list[1][1]
        assert "params" not in call2_kwargs

    def test_http_error_includes_page_context(self) -> None:
        session = _mock_session()
        session.request.return_value = _error_response(429)

        with pytest.raises(ExecutionError, match="page=0"):
            list(
                LinkHeaderPaginator().iterate(
                    session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None
                )
            )


# ---------------------------------------------------------------------------
# Rate limiter integration
# ---------------------------------------------------------------------------


class TestRateLimiterIntegration:
    def test_rate_limiter_used_when_provided(self) -> None:
        session = _mock_session()
        rate_limiter = MagicMock()
        resp = _json_response({"data": []})
        rate_limiter.execute.return_value = resp

        list(
            NoPaginator().iterate(session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, rate_limiter)
        )

        rate_limiter.execute.assert_called_once()
        # session.request should NOT be called directly
        session.request.assert_not_called()

    def test_session_used_when_no_rate_limiter(self) -> None:
        session = _mock_session()
        session.request.return_value = _json_response([])

        list(NoPaginator().iterate(session, _BASE, _PARAMS, _HEADERS, None, "GET", 30, None))

        session.request.assert_called_once()


# ---------------------------------------------------------------------------
# create_paginator factory
# ---------------------------------------------------------------------------


class TestCreatePaginator:
    def test_none_config_returns_no_paginator(self) -> None:
        assert isinstance(create_paginator(None), NoPaginator)

    def test_none_strategy_returns_no_paginator(self) -> None:
        assert isinstance(create_paginator({"strategy": "none"}), NoPaginator)

    def test_missing_strategy_returns_no_paginator(self) -> None:
        assert isinstance(create_paginator({}), NoPaginator)

    def test_offset_strategy(self) -> None:
        p = create_paginator({"strategy": "offset", "limit": 50})
        assert isinstance(p, OffsetPaginator)

    def test_offset_custom_params(self) -> None:
        p = create_paginator(
            {
                "strategy": "offset",
                "limit": 25,
                "offset_param": "skip",
                "limit_param": "take",
            }
        )
        assert isinstance(p, OffsetPaginator)
        assert p._limit == 25  # type: ignore[attr-defined]
        assert p._offset_param == "skip"  # type: ignore[attr-defined]
        assert p._limit_param == "take"  # type: ignore[attr-defined]

    def test_cursor_strategy(self) -> None:
        p = create_paginator({"strategy": "cursor", "cursor_field": "next", "cursor_param": "c"})
        assert isinstance(p, CursorPaginator)

    def test_page_number_strategy(self) -> None:
        p = create_paginator({"strategy": "page_number", "limit": 20, "start_page": 0})
        assert isinstance(p, PageNumberPaginator)

    def test_link_header_strategy(self) -> None:
        assert isinstance(create_paginator({"strategy": "link_header"}), LinkHeaderPaginator)

    def test_unknown_strategy_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown pagination strategy"):
            create_paginator({"strategy": "magic"})
