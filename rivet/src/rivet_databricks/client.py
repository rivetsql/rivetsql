"""Shared Unity Catalog REST API client.

Provides authenticated requests, catalog/schema/table listing, credential vending,
table creation, retry logic with exponential backoff, and connection pooling via
requests.Session.
"""

from __future__ import annotations

import time
from typing import Any

import requests

from rivet_core.errors import ExecutionError, plugin_error
from rivet_databricks.auth import (
    AUTH_TYPE_OAUTH_M2M,
    AUTH_TYPE_PAT,
    ResolvedCredential,
)

_API_PREFIX = "/api/2.1/unity-catalog"
_MAX_RETRIES = 3
_BACKOFF_BASE = 0.5
_REQUEST_TIMEOUT = 10.0  # seconds per request
_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


class UnityCatalogClient:
    """REST client for Unity Catalog API with retry and connection pooling."""

    def __init__(self, host: str, credential: ResolvedCredential) -> None:
        self._host = host.rstrip("/")
        self._base_url = f"{self._host}{_API_PREFIX}"
        self._credential = credential
        self._session = requests.Session()
        self._session.headers["User-Agent"] = "rivet-databricks/0.1"
        self._session.headers["Content-Type"] = "application/json"
        self._session.headers["Accept"] = "application/json"
        if credential.auth_type == AUTH_TYPE_PAT and credential.token:
            self._session.headers["Authorization"] = f"Bearer {credential.token}"
        elif credential.auth_type == AUTH_TYPE_OAUTH_M2M and credential.client_id:
            # OAuth M2M: token exchange would happen here in production;
            # for now set client_id as bearer (real impl would call /oidc/v1/token)
            self._session.headers["Authorization"] = f"Bearer {credential.client_id}"
        elif credential.token:
            self._session.headers["Authorization"] = f"Bearer {credential.token}"

    # ── Public API ────────────────────────────────────────────────────

    def list_catalogs(self) -> list[dict[str, Any]]:
        resp = self._request("GET", "/catalogs")
        return resp.get("catalogs", [])  # type: ignore[no-any-return]

    def list_schemas(self, catalog_name: str) -> list[dict[str, Any]]:
        resp = self._request("GET", "/schemas", params={"catalog_name": catalog_name})
        return resp.get("schemas", [])  # type: ignore[no-any-return]

    def list_tables(self, catalog_name: str, schema_name: str) -> list[dict[str, Any]]:
        resp = self._request(
            "GET", "/tables", params={"catalog_name": catalog_name, "schema_name": schema_name}
        )
        return resp.get("tables", [])  # type: ignore[no-any-return]

    def get_table(self, full_name: str) -> dict[str, Any]:
        return self._request("GET", f"/tables/{full_name}")

    def create_table(self, table_def: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", "/tables", json=table_def)

    def vend_credentials(self, table_id: str, operation: str = "READ") -> dict[str, Any]:
        return self._request(
            "POST",
            "/temporary-table-credentials",
            json={"table_id": table_id, "operation": operation},
        )

    def close(self) -> None:
        self._session.close()

    # ── Internal request with retry ───────────────────────────────────

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{self._base_url}{path}"
        last_exc: Exception | None = None

        for attempt in range(_MAX_RETRIES + 1):
            try:
                resp = self._session.request(method=method, url=url, params=params, json=json, timeout=_REQUEST_TIMEOUT)
                resp.raise_for_status()
                return resp.json()  # type: ignore[no-any-return]
            except requests.ConnectionError as e:
                last_exc = e
                if attempt < _MAX_RETRIES:
                    time.sleep(_BACKOFF_BASE * (2**attempt))
                    continue
            except requests.HTTPError as e:
                resp = e.response
                status = resp.status_code if resp is not None else 0

                # Credential vending 403 → specific error
                if status == 403 and "temporary-table-credentials" in path:
                    raise ExecutionError(
                        plugin_error(
                            "RVT-508",
                            f"Credential vending denied (HTTP 403) for {url}.",
                            plugin_name="rivet_databricks",
                            plugin_type="client",
                            remediation="Enable credential vending on the Unity Catalog endpoint or use ambient credentials.",
                            host=self._host,
                            path=path,
                            status=403,
                        )
                    ) from e

                # Auth errors — no retry
                if status == 401:
                    raise ExecutionError(
                        plugin_error(
                            "RVT-502",
                            f"Authentication failed (HTTP 401) for {self._host}.",
                            plugin_name="rivet_databricks",
                            plugin_type="client",
                            remediation="Check your token or credentials. Ensure DATABRICKS_TOKEN is valid.",
                            host=self._host,
                            path=path,
                            status=401,
                        )
                    ) from e

                if status == 403:
                    raise ExecutionError(
                        plugin_error(
                            "RVT-502",
                            f"Authorization denied (HTTP 403) for {self._host}{path}.",
                            plugin_name="rivet_databricks",
                            plugin_type="client",
                            remediation="Check that your credentials have access to the requested resource.",
                            host=self._host,
                            path=path,
                            status=403,
                        )
                    ) from e

                if status == 404:
                    raise ExecutionError(
                        plugin_error(
                            "RVT-503",
                            f"Resource not found (HTTP 404): {path}.",
                            plugin_name="rivet_databricks",
                            plugin_type="client",
                            remediation="Verify the catalog, schema, or table name exists.",
                            host=self._host,
                            path=path,
                            status=404,
                        )
                    ) from e

                # Retryable status codes
                if status in _RETRYABLE_STATUS_CODES and attempt < _MAX_RETRIES:
                    wait = _BACKOFF_BASE * (2**attempt)
                    retry_after = resp.headers.get("Retry-After") if resp is not None else None
                    if retry_after is not None:
                        try:
                            wait = max(float(retry_after), 0)
                        except ValueError:
                            pass
                    time.sleep(wait)
                    last_exc = e
                    continue

                # Non-retryable error
                last_exc = e
                break

        # Exhausted retries or non-retryable error
        raise ExecutionError(
            plugin_error(
                "RVT-503",
                f"Unity Catalog API request failed: {method} {path}.",
                plugin_name="rivet_databricks",
                plugin_type="client",
                remediation="Check network connectivity and Unity Catalog host availability.",
                host=self._host,
                path=path,
                attempts=_MAX_RETRIES + 1,
            )
        ) from last_exc
