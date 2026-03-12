"""Unit tests for REST API auth strategies."""

from __future__ import annotations

import base64
import time
from unittest.mock import MagicMock, patch

import requests

from rivet_rest.auth import (
    ApiKeyAuth,
    BasicAuth,
    BearerAuth,
    NoAuth,
    OAuth2Auth,
    create_auth,
)


class TestNoAuth:
    def test_no_headers_added(self) -> None:
        session = requests.Session()
        original_headers = dict(session.headers)
        NoAuth().apply(session)
        assert dict(session.headers) == original_headers


class TestBearerAuth:
    def test_authorization_header_set(self) -> None:
        session = requests.Session()
        BearerAuth(token="my-token").apply(session)
        assert session.headers["Authorization"] == "Bearer my-token"


class TestBasicAuth:
    def test_authorization_header_set(self) -> None:
        session = requests.Session()
        BasicAuth(username="user", password="pass").apply(session)
        expected = base64.b64encode(b"user:pass").decode("ascii")
        assert session.headers["Authorization"] == f"Basic {expected}"

    def test_special_characters_in_credentials(self) -> None:
        session = requests.Session()
        BasicAuth(username="user@domain", password="p@ss:word!").apply(session)
        expected = base64.b64encode(b"user@domain:p@ss:word!").decode("ascii")
        assert session.headers["Authorization"] == f"Basic {expected}"


class TestApiKeyAuth:
    def test_header_mode(self) -> None:
        session = requests.Session()
        ApiKeyAuth(key_name="X-API-Key", key_value="secret", location="header").apply(session)
        assert session.headers["X-API-Key"] == "secret"

    def test_query_mode(self) -> None:
        session = requests.Session()
        ApiKeyAuth(key_name="api_key", key_value="secret", location="query").apply(session)
        assert session.params["api_key"] == "secret"  # type: ignore[index]

    def test_default_location_is_header(self) -> None:
        session = requests.Session()
        ApiKeyAuth(key_name="X-Key", key_value="val").apply(session)
        assert session.headers["X-Key"] == "val"


class TestOAuth2Auth:
    def test_manual_token_fetch(self) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "access_token": "fetched-token",
            "expires_in": 3600,
        }
        mock_response.raise_for_status = MagicMock()

        auth = OAuth2Auth(
            client_id="cid",
            client_secret="csecret",
            token_url="https://auth.example.com/token",
        )

        with patch("rivet_rest.auth.requests.post", return_value=mock_response):
            # Force manual path by making oauthlib import fail
            with patch.dict(
                "sys.modules",
                {"oauthlib": None, "oauthlib.oauth2": None, "requests_oauthlib": None},
            ):
                session = requests.Session()
                auth.apply(session)

        assert session.headers["Authorization"] == "Bearer fetched-token"

    def test_token_cached_on_second_apply(self) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "access_token": "cached-token",
            "expires_in": 3600,
        }
        mock_response.raise_for_status = MagicMock()

        auth = OAuth2Auth(
            client_id="cid",
            client_secret="csecret",
            token_url="https://auth.example.com/token",
        )

        with patch("rivet_rest.auth.requests.post", return_value=mock_response) as mock_post:
            with patch.dict(
                "sys.modules",
                {"oauthlib": None, "oauthlib.oauth2": None, "requests_oauthlib": None},
            ):
                s1 = requests.Session()
                auth.apply(s1)
                s2 = requests.Session()
                auth.apply(s2)

        # Only one HTTP call — token was cached
        assert mock_post.call_count == 1
        assert s2.headers["Authorization"] == "Bearer cached-token"

    def test_token_refreshed_when_near_expiry(self) -> None:
        mock_response_1 = MagicMock()
        mock_response_1.json.return_value = {
            "access_token": "token-1",
            "expires_in": 3600,
        }
        mock_response_1.raise_for_status = MagicMock()

        mock_response_2 = MagicMock()
        mock_response_2.json.return_value = {
            "access_token": "token-2",
            "expires_in": 3600,
        }
        mock_response_2.raise_for_status = MagicMock()

        auth = OAuth2Auth(
            client_id="cid",
            client_secret="csecret",
            token_url="https://auth.example.com/token",
        )

        with patch(
            "rivet_rest.auth.requests.post", side_effect=[mock_response_1, mock_response_2]
        ) as mock_post:
            with patch.dict(
                "sys.modules",
                {"oauthlib": None, "oauthlib.oauth2": None, "requests_oauthlib": None},
            ):
                session = requests.Session()
                auth.apply(session)
                assert session.headers["Authorization"] == "Bearer token-1"

                # Simulate token near expiry (within 30s)
                auth._expires_at = time.time() + 10

                session2 = requests.Session()
                auth.apply(session2)
                assert session2.headers["Authorization"] == "Bearer token-2"

        assert mock_post.call_count == 2


class TestCreateAuthFactory:
    def test_none_returns_no_auth(self) -> None:
        auth = create_auth("none", {})
        assert isinstance(auth, NoAuth)

    def test_bearer_returns_bearer_auth(self) -> None:
        auth = create_auth("bearer", {"token": "tok"})
        assert isinstance(auth, BearerAuth)

    def test_basic_returns_basic_auth(self) -> None:
        auth = create_auth("basic", {"username": "u", "password": "p"})
        assert isinstance(auth, BasicAuth)

    def test_api_key_returns_api_key_auth(self) -> None:
        auth = create_auth(
            "api_key",
            {"api_key_value": "k", "api_key_name": "X-Key", "api_key_location": "header"},
        )
        assert isinstance(auth, ApiKeyAuth)

    def test_api_key_default_name(self) -> None:
        auth = create_auth("api_key", {"api_key_value": "k"})
        assert isinstance(auth, ApiKeyAuth)
        session = requests.Session()
        auth.apply(session)
        assert session.headers["X-API-Key"] == "k"

    def test_oauth2_returns_oauth2_auth(self) -> None:
        auth = create_auth(
            "oauth2",
            {"client_id": "cid", "client_secret": "cs", "token_url": "https://t"},
        )
        assert isinstance(auth, OAuth2Auth)

    def test_unknown_type_raises_value_error(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="Unknown auth type"):
            create_auth("magic", {})
