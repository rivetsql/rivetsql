"""REST API adapter: wildcard ComputeEngineAdapter for REST API catalogs.

Registered with ``target_engine_type = "*"`` so any Arrow-compatible engine
can execute REST API joints.  The ``PluginRegistry`` wildcard fallback gates
on the engine declaring ``"arrow"`` in ``supported_catalog_types``.

The adapter's ``read_dispatch`` builds a deferred ``Material`` backed by a
``RestApiDeferredRef`` that only performs HTTP requests on ``to_arrow()``.
Predicate pushdown is delegated to the ``PredicateMapper``.
"""

from __future__ import annotations

import io
import json
from typing import Any
from urllib.parse import urljoin

import pyarrow
import pyarrow.csv as pcsv
import requests

from rivet_core.errors import ExecutionError, RivetError
from rivet_core.models import Column, Material, Schema
from rivet_core.optimizer import AdapterPushdownResult, PushdownPlan
from rivet_core.plugins import ComputeEngineAdapter
from rivet_core.strategies import MaterializedRef
from rivet_rest.auth import create_auth
from rivet_rest.flatten import arrow_to_records, records_to_arrow
from rivet_rest.pagination import create_paginator
from rivet_rest.predicate_mapper import map_predicates
from rivet_rest.rate_limit import RateLimiter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_endpoint(catalog: Any, joint: Any) -> dict[str, Any]:
    """Look up the endpoint configuration for a joint's table reference."""
    options: dict[str, Any] = catalog.options if hasattr(catalog, "options") else {}
    endpoints: dict[str, Any] = options.get("endpoints", {})
    table: str | None = getattr(joint, "table", None) or getattr(joint, "name", None)
    if table and table in endpoints:
        return dict(endpoints[table])
    return {"path": f"/{table}" if table else "/"}


def _build_session_config(catalog: Any) -> dict[str, Any]:
    """Extract session configuration from catalog options."""
    options: dict[str, Any] = catalog.options if hasattr(catalog, "options") else {}
    return {
        "auth": options.get("auth", "none"),
        "default_headers": options.get("default_headers", {}),
        "timeout": options.get("timeout", 30),
        "token": options.get("token"),
        "username": options.get("username"),
        "password": options.get("password"),
        "api_key_name": options.get("api_key_name"),
        "api_key_value": options.get("api_key_value"),
        "api_key_location": options.get("api_key_location"),
        "client_id": options.get("client_id"),
        "client_secret": options.get("client_secret"),
        "token_url": options.get("token_url"),
    }


def _resolve_url(base_url: str, path: str) -> str:
    """Join base URL and endpoint path."""
    if not base_url:
        return path
    if not base_url.endswith("/"):
        base_url = base_url + "/"
    return urljoin(base_url, path.lstrip("/"))


def _create_session(session_config: dict[str, Any]) -> requests.Session:
    """Create a configured requests.Session from session config."""
    session = requests.Session()
    default_headers = session_config.get("default_headers", {})
    if default_headers:
        session.headers.update(default_headers)
    auth_type = session_config.get("auth", "none")
    auth_strategy = create_auth(auth_type, session_config)
    auth_strategy.apply(session)
    return session


def _extract_records(
    data: Any,
    response_path: str | None,
    url: str,
) -> list[dict[str, Any]]:
    """Extract records from a JSON response using the response_path."""
    if response_path:
        current = data
        for part in response_path.split("."):
            if isinstance(current, dict):
                if part not in current:
                    available = ", ".join(sorted(current.keys())) if current else "(empty)"
                    raise ExecutionError(
                        RivetError(
                            code="RVT-501",
                            message=(
                                f"response_path '{response_path}' not found "
                                f"in response. Available keys: {available}"
                            ),
                            context={"url": url, "response_path": response_path},
                            remediation=(
                                "Check the 'response_path' in your endpoint configuration."
                            ),
                        )
                    )
                current = current[part]
            else:
                raise ExecutionError(
                    RivetError(
                        code="RVT-501",
                        message=(
                            f"response_path '{response_path}' not found "
                            f"in response. Expected dict at '{part}', "
                            f"got {type(current).__name__}"
                        ),
                        context={"url": url, "response_path": response_path},
                        remediation=("Check the 'response_path' in your endpoint configuration."),
                    )
                )
        data = current

    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def _check_http_error(resp: requests.Response, url: str, auth_type: str) -> None:
    """Raise descriptive errors for common HTTP failure codes."""
    if resp.ok:
        return
    status = resp.status_code
    if status in (401, 403):
        raise ExecutionError(
            RivetError(
                code="RVT-201",
                message=(
                    f"Authentication/authorization failed (HTTP {status}) "
                    f"for {url} using auth strategy '{auth_type}'"
                ),
                context={"url": url, "status_code": status, "auth": auth_type},
                remediation="Check your credentials and auth configuration.",
            )
        )
    if status == 404:
        raise ExecutionError(
            RivetError(
                code="RVT-501",
                message=f"Endpoint not found (HTTP 404): {url}",
                context={"url": url, "status_code": 404},
                remediation="Verify the endpoint path exists in the API.",
            )
        )


# ---------------------------------------------------------------------------
# Deferred materialization ref
# ---------------------------------------------------------------------------


class RestApiDeferredRef(MaterializedRef):
    """Deferred HTTP fetch backed by a REST API endpoint.

    Created by ``RestApiAdapter.read_dispatch()``.  HTTP requests are only
    executed on the first ``to_arrow()`` call; subsequent calls return the
    cached result.  Consistent with ``GlueDuckDBMaterializedRef`` pattern.
    """

    def __init__(
        self,
        session_config: dict[str, Any],
        endpoint_config: dict[str, Any],
        query_params: dict[str, str],
        rate_limit_config: dict[str, Any] | None,
        max_flatten_depth: int,
        response_format: str,
        base_url: str = "",
        limit: int | None = None,
    ) -> None:
        self._session_config = session_config
        self._endpoint_config = endpoint_config
        self._query_params = query_params
        self._rate_limit_config = rate_limit_config
        self._max_flatten_depth = max_flatten_depth
        self._response_format = response_format
        self._base_url = base_url
        self._limit = limit
        self._cached: pyarrow.Table | None = None

    def _execute(self) -> pyarrow.Table:
        """Perform the HTTP fetch, paginate, parse, and return Arrow table."""
        path = self._endpoint_config.get("path", "/")
        url = _resolve_url(self._base_url, path)
        method: str = self._endpoint_config.get("method", "GET")
        timeout: int = self._session_config.get("timeout", 30)
        auth_type: str = self._session_config.get("auth", "none")

        params: dict[str, Any] = dict(self._endpoint_config.get("params", {}))
        params.update(self._query_params)

        headers: dict[str, str] = dict(self._endpoint_config.get("headers", {}))
        body: Any = self._endpoint_config.get("body")
        response_path: str | None = self._endpoint_config.get("response_path")

        session = _create_session(self._session_config)
        rate_limiter: RateLimiter | None = None
        if self._rate_limit_config:
            rate_limiter = RateLimiter(
                requests_per_second=self._rate_limit_config.get(
                    "requests_per_second",
                ),
                burst=self._rate_limit_config.get("burst", 1),
                max_retries=self._rate_limit_config.get("max_retries", 3),
            )

        paginator = create_paginator(self._endpoint_config.get("pagination"))

        try:
            return self._fetch_pages(
                session,
                url,
                method,
                params,
                headers,
                body,
                timeout,
                rate_limiter,
                paginator,
                response_path,
                auth_type,
            )
        except ExecutionError:
            raise
        except requests.ConnectionError as exc:
            raise ExecutionError(
                RivetError(
                    code="RVT-501",
                    message=f"Network error for {url}: {exc}",
                    context={"url": url},
                    remediation="Check network connectivity and the base_url.",
                )
            ) from exc
        except requests.RequestException as exc:
            raise ExecutionError(
                RivetError(
                    code="RVT-501",
                    message=f"HTTP request failed for {url}: {exc}",
                    context={"url": url},
                    remediation=("Check endpoint configuration and API availability."),
                )
            ) from exc
        finally:
            session.close()

    def _fetch_pages(
        self,
        session: requests.Session,
        url: str,
        method: str,
        params: dict[str, Any],
        headers: dict[str, str],
        body: Any,
        timeout: int,
        rate_limiter: RateLimiter | None,
        paginator: Any,
        response_path: str | None,
        auth_type: str,
    ) -> pyarrow.Table:
        """Iterate pages, extract records, consolidate into Arrow table."""
        if self._response_format == "csv":
            return self._fetch_csv(
                session,
                url,
                method,
                params,
                headers,
                body,
                timeout,
                rate_limiter,
                paginator,
                auth_type,
            )

        all_records: list[dict[str, Any]] = []

        for resp in paginator.iterate(
            session,
            url,
            params,
            headers,
            body,
            method,
            timeout,
            rate_limiter,
            response_path,
        ):
            _check_http_error(resp, url, auth_type)
            try:
                data = resp.json()
            except (json.JSONDecodeError, ValueError) as exc:
                body_preview = resp.text[:200] if resp.text else "(empty)"
                raise ExecutionError(
                    RivetError(
                        code="RVT-501",
                        message=f"JSON parse error for {url}: {body_preview}",
                        context={"url": url},
                        remediation="Check response_format and API response.",
                    )
                ) from exc

            records = _extract_records(data, response_path, url)
            if not records:
                break
            all_records.extend(records)

            # Stop fetching if we have enough records to satisfy the SQL LIMIT
            if self._limit is not None and len(all_records) >= self._limit:
                all_records = all_records[: self._limit]
                break
        if not all_records:
            return pyarrow.table({})

        return records_to_arrow(
            all_records,
            schema=None,
            max_depth=self._max_flatten_depth,
        )

    def _fetch_csv(
        self,
        session: requests.Session,
        url: str,
        method: str,
        params: dict[str, Any],
        headers: dict[str, str],
        body: Any,
        timeout: int,
        rate_limiter: RateLimiter | None,
        paginator: Any,
        auth_type: str,
    ) -> pyarrow.Table:
        """Fetch CSV response and parse via PyArrow CSV reader."""
        tables: list[pyarrow.Table] = []
        for resp in paginator.iterate(
            session,
            url,
            params,
            headers,
            body,
            method,
            timeout,
            rate_limiter,
        ):
            _check_http_error(resp, url, auth_type)
            csv_bytes = resp.content
            if not csv_bytes:
                break
            table = pcsv.read_csv(io.BytesIO(csv_bytes))
            tables.append(table)

        if not tables:
            return pyarrow.table({})
        return pyarrow.concat_tables(tables)

    def to_arrow(self) -> pyarrow.Table:
        """Execute HTTP request(s) on first call, return cached after."""
        if self._cached is not None:
            return self._cached
        self._cached = self._execute()
        return self._cached

    @property
    def schema(self) -> Schema:
        table = self.to_arrow()
        return Schema(
            columns=[
                Column(name=f.name, type=str(f.type), nullable=f.nullable) for f in table.schema
            ]
        )

    @property
    def row_count(self) -> int:
        return self.to_arrow().num_rows  # type: ignore[no-any-return]

    @property
    def size_bytes(self) -> int | None:
        if self._cached is not None:
            return self._cached.nbytes  # type: ignore[no-any-return]
        return None

    @property
    def storage_type(self) -> str:
        return "rest_api"


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class RestApiAdapter(ComputeEngineAdapter):
    """Wildcard adapter for REST API catalogs.

    Registered with ``target_engine_type = "*"`` so any Arrow-compatible
    engine can execute REST API joints.  The ``PluginRegistry`` wildcard
    fallback gates on the engine declaring ``"arrow"`` in
    ``supported_catalog_types``.
    """

    target_engine_type = "*"
    catalog_type = "rest_api"
    capabilities = ["projection_pushdown", "predicate_pushdown", "limit_pushdown"]
    source = "catalog_plugin"
    source_plugin = "rivet_rest"

    def read_dispatch(
        self,
        engine: Any,
        catalog: Any,
        joint: Any,
        pushdown: PushdownPlan | None = None,
    ) -> AdapterPushdownResult:
        """Build a deferred Material with pushdown-mapped query params.

        1. Resolve endpoint config from ``joint.table``
        2. Delegate predicate translation to PredicateMapper
        3. Build ``RestApiDeferredRef`` with session + endpoint config
        4. Return ``AdapterPushdownResult`` with deferred Material + residual
        """
        endpoint_config = _resolve_endpoint(catalog, joint)
        query_params, residual = map_predicates(
            pushdown,
            endpoint_config.get("filter_params"),
        )

        options: dict[str, Any] = catalog.options if hasattr(catalog, "options") else {}

        # Extract SQL LIMIT from pushdown plan
        limit: int | None = None
        if pushdown is not None and hasattr(pushdown, "limit"):
            limit_result = pushdown.limit
            if hasattr(limit_result, "pushed_limit"):
                limit = limit_result.pushed_limit
        ref = RestApiDeferredRef(
            session_config=_build_session_config(catalog),
            endpoint_config=endpoint_config,
            query_params=query_params,
            rate_limit_config=options.get("rate_limit"),
            max_flatten_depth=options.get("max_flatten_depth", 3),
            response_format=options.get("response_format", "json"),
            base_url=options.get("base_url", ""),
            limit=limit,
        )
        material = Material(
            name=getattr(joint, "name", "rest_api_read"),
            catalog=getattr(catalog, "name", "rest_api"),
            materialized_ref=ref,
            state="deferred",
        )
        return AdapterPushdownResult(material=material, residual=residual)

    def write_dispatch(
        self,
        engine: Any,
        catalog: Any,
        joint: Any,
        material: Any,
    ) -> Any:
        """Serialize Arrow to JSON and send HTTP requests.

        Strategy ``append`` -> POST, ``replace`` -> PUT (or PATCH if
        ``write_method=PATCH``).
        """
        endpoint_config = _resolve_endpoint(catalog, joint)
        options: dict[str, Any] = catalog.options if hasattr(catalog, "options") else {}
        session_config = _build_session_config(catalog)
        session = _create_session(session_config)

        base_url = options.get("base_url", "")
        path = endpoint_config.get("path", "/")
        url = _resolve_url(base_url, path)
        timeout: int = session_config.get("timeout", 30)

        rate_limiter: RateLimiter | None = None
        rate_limit_config = options.get("rate_limit")
        if rate_limit_config:
            rate_limiter = RateLimiter(
                requests_per_second=rate_limit_config.get("requests_per_second"),
                burst=rate_limit_config.get("burst", 1),
                max_retries=rate_limit_config.get("max_retries", 3),
            )

        write_strategy = getattr(joint, "write_strategy", "append") or "append"
        write_method_override = endpoint_config.get("write_method")
        batch_size: int = endpoint_config.get("batch_size", 1)

        if write_strategy == "replace":
            http_method = write_method_override or "PUT"
        else:
            http_method = "POST"

        arrow_table = material.to_arrow() if hasattr(material, "to_arrow") else material
        records = arrow_to_records(arrow_table)

        try:
            step = max(batch_size, 1)
            for i in range(0, max(len(records), 1), step):
                batch = records[i : i + step]
                if not batch:
                    break
                payload: Any = batch[0] if len(batch) == 1 else batch
                if rate_limiter is not None:
                    resp = rate_limiter.execute(
                        session,
                        http_method,
                        url,
                        json=payload,
                        timeout=timeout,
                    )
                else:
                    resp = session.request(
                        http_method,
                        url,
                        json=payload,
                        timeout=timeout,
                    )
                if not resp.ok:
                    raise ExecutionError(
                        RivetError(
                            code="RVT-501",
                            message=(
                                f"Write failed at row {i} "
                                f"(HTTP {resp.status_code}): "
                                f"{resp.text[:200]}"
                            ),
                            context={
                                "url": url,
                                "row_index": i,
                                "status_code": resp.status_code,
                            },
                            remediation=("Check the endpoint accepts the data format."),
                        )
                    )
        finally:
            session.close()

        return None
