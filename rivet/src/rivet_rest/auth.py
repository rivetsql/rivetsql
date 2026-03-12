"""Authentication strategies for REST API catalog plugin.

Each strategy implements the AuthStrategy protocol — a single `apply` method
that configures a `requests.Session` with the appropriate credentials.
"""

from __future__ import annotations

import base64
import time
from typing import Any, Protocol

import requests


class AuthStrategy(Protocol):
    """Protocol for authentication strategies."""

    def apply(self, session: requests.Session) -> None:
        """Apply authentication to the given session."""
        ...


class NoAuth:
    """No authentication — requests are sent without credentials."""

    def apply(self, session: requests.Session) -> None:
        pass


class BearerAuth:
    """Bearer token authentication via Authorization header."""

    def __init__(self, token: str) -> None:
        self._token = token

    def apply(self, session: requests.Session) -> None:
        session.headers["Authorization"] = f"Bearer {self._token}"


class BasicAuth:
    """HTTP Basic authentication via Authorization header."""

    def __init__(self, username: str, password: str) -> None:
        self._username = username
        self._password = password

    def apply(self, session: requests.Session) -> None:
        encoded = base64.b64encode(f"{self._username}:{self._password}".encode()).decode("ascii")
        session.headers["Authorization"] = f"Basic {encoded}"


class ApiKeyAuth:
    """API key authentication via header or query parameter."""

    def __init__(self, key_name: str, key_value: str, location: str = "header") -> None:
        self._key_name = key_name
        self._key_value = key_value
        self._location = location

    def apply(self, session: requests.Session) -> None:
        if self._location == "query":
            session.params = dict(session.params or {})  # type: ignore[arg-type]
            session.params[self._key_name] = self._key_value  # type: ignore[index]
        else:
            session.headers[self._key_name] = self._key_value


class OAuth2Auth:
    """OAuth2 client credentials grant with automatic token refresh.

    Tries ``requests_oauthlib`` first for the token exchange, falling back
    to a manual ``requests.post()`` if the library is not installed.
    Token is refreshed automatically when within 30 seconds of expiry.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_url: str,
    ) -> None:
        self._client_id = client_id
        self._client_secret = client_secret
        self._token_url = token_url
        self._access_token: str | None = None
        self._expires_at: float = 0.0

    def apply(self, session: requests.Session) -> None:
        if self._needs_refresh():
            self._fetch_token()
        session.headers["Authorization"] = f"Bearer {self._access_token}"

    def _needs_refresh(self) -> bool:
        if self._access_token is None:
            return True
        return time.time() >= (self._expires_at - 30)

    def _fetch_token(self) -> None:
        try:
            self._fetch_token_oauthlib()
        except ImportError:
            self._fetch_token_manual()

    def _fetch_token_oauthlib(self) -> None:
        from oauthlib.oauth2 import BackendApplicationClient  # type: ignore[import-untyped]
        from requests_oauthlib import OAuth2Session  # type: ignore[import-untyped]

        client = BackendApplicationClient(client_id=self._client_id)
        oauth = OAuth2Session(client=client)
        token = oauth.fetch_token(
            token_url=self._token_url,
            client_id=self._client_id,
            client_secret=self._client_secret,
        )
        self._access_token = token["access_token"]
        expires_in = token.get("expires_in", 3600)
        self._expires_at = time.time() + float(expires_in)

    def _fetch_token_manual(self) -> None:
        resp = requests.post(
            self._token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
            timeout=30,
        )
        resp.raise_for_status()
        token_data = resp.json()
        self._access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", 3600)
        self._expires_at = time.time() + float(expires_in)


def create_auth(auth_type: str, options: dict[str, Any]) -> AuthStrategy:
    """Factory: create an AuthStrategy from a type string and config options.

    Args:
        auth_type: One of ``"none"``, ``"bearer"``, ``"basic"``,
            ``"api_key"``, ``"oauth2"``.
        options: Catalog options dict containing credential values.

    Returns:
        An AuthStrategy instance ready to apply to a session.

    Raises:
        ValueError: If ``auth_type`` is not recognised.
    """
    if auth_type == "none":
        return NoAuth()
    if auth_type == "bearer":
        return BearerAuth(token=options["token"])
    if auth_type == "basic":
        return BasicAuth(
            username=options["username"],
            password=options["password"],
        )
    if auth_type == "api_key":
        return ApiKeyAuth(
            key_name=options.get("api_key_name", "X-API-Key"),
            key_value=options["api_key_value"],
            location=options.get("api_key_location", "header"),
        )
    if auth_type == "oauth2":
        return OAuth2Auth(
            client_id=options["client_id"],
            client_secret=options["client_secret"],
            token_url=options["token_url"],
        )
    raise ValueError(f"Unknown auth type: {auth_type!r}")
