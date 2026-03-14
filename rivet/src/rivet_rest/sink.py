"""REST API sink plugin for writing Arrow data to REST endpoints.

Serializes Arrow tables to JSON and sends them to REST API endpoints via
POST, PUT, or PATCH requests. Supports batching, authentication, and rate
limiting.
"""

from __future__ import annotations

from typing import Any

import requests

from rivet_core.errors import ExecutionError, RivetError, plugin_error
from rivet_core.models import Catalog, Joint, Material
from rivet_core.plugins import SinkPlugin

REST_SUPPORTED_STRATEGIES = frozenset({"append", "replace"})
from rivet_rest.adapter import _build_session_config, _resolve_endpoint
from rivet_rest.auth import create_auth
from rivet_rest.flatten import arrow_to_records
from rivet_rest.rate_limit import RateLimiter


class RestApiSink(SinkPlugin):
    """Sink plugin for writing Arrow data to REST API endpoints.

    Converts Arrow rows to JSON and sends them to the configured endpoint
    using POST (append strategy) or PUT/PATCH (replace strategy).

    Supports:
    - Batching multiple rows per request
    - Authentication strategies (bearer, basic, api_key, oauth2)
    - Rate limiting and retry with exponential backoff
    - Configurable HTTP methods per endpoint
    """

    catalog_type = "rest_api"
    supported_strategies = REST_SUPPORTED_STRATEGIES

    def write(
        self,
        catalog: Catalog,
        joint: Joint,
        material: Material,
        strategy: str,
    ) -> None:
        """Write Arrow data to a REST API endpoint.

        Args:
            catalog: The REST API catalog instance
            joint: The sink joint (table identifies the endpoint)
            material: The Arrow material to write
            strategy: Write strategy ("append" or "replace")

        Raises:
            ExecutionError: When the strategy is unsupported or write requests fail after retries
        """
        if strategy not in REST_SUPPORTED_STRATEGIES:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"Unsupported write strategy '{strategy}' for REST API sink.",
                    plugin_name="rest_api",
                    plugin_type="sink",
                    remediation=f"Supported strategies: {', '.join(sorted(REST_SUPPORTED_STRATEGIES))}",
                    strategy=strategy,
                    catalog=catalog.name,
                )
            )

        # Resolve endpoint configuration
        endpoint_config = _resolve_endpoint(catalog, joint)
        options: dict[str, Any] = catalog.options if hasattr(catalog, "options") else {}

        # Create session with auth
        session_config = _build_session_config(catalog)
        session = requests.Session()
        default_headers = session_config.get("default_headers", {})
        if default_headers:
            session.headers.update(default_headers)
        # Note: timeout is passed per-request, not set on session

        auth_type = session_config.get("auth", "none")
        auth_strategy = create_auth(auth_type, session_config)
        auth_strategy.apply(session)

        # Create rate limiter
        rate_limit_config = options.get("rate_limit")
        rate_limiter = RateLimiter(
            requests_per_second=(
                rate_limit_config.get("requests_per_second") if rate_limit_config else None
            ),
            burst=rate_limit_config.get("burst", 1) if rate_limit_config else 1,
            max_retries=options.get("max_retries", 3),
        )

        # Convert Arrow to JSON records
        table = material.to_arrow()
        records = arrow_to_records(table)

        # Determine HTTP method
        write_method = endpoint_config.get("write_method")
        if write_method:
            method = write_method
        elif strategy == "append":
            method = "POST"
        else:  # replace
            method = "PUT"

        # Build URL
        base_url = options.get("base_url", "")
        path = endpoint_config.get("path", "")
        url = f"{base_url.rstrip('/')}/{path.lstrip('/')}"

        # Batch and send
        batch_size = endpoint_config.get("batch_size", 1)
        self._write_batches(
            session=session,
            rate_limiter=rate_limiter,
            url=url,
            method=method,
            records=records,
            batch_size=batch_size,
            endpoint_config=endpoint_config,
        )

    def _write_batches(
        self,
        session: requests.Session,
        rate_limiter: RateLimiter,
        url: str,
        method: str,
        records: list[dict[str, Any]],
        batch_size: int,
        endpoint_config: dict[str, Any],
    ) -> None:
        """Write records in batches to the REST API endpoint.

        Args:
            session: Configured requests session
            rate_limiter: Rate limiter for request throttling
            url: Full endpoint URL
            method: HTTP method (POST, PUT, PATCH)
            records: List of JSON records to write
            batch_size: Number of records per request
            endpoint_config: Endpoint configuration dict

        Raises:
            ExecutionError: When a write request fails after retries
        """
        headers = endpoint_config.get("headers", {})
        headers.setdefault("Content-Type", "application/json")

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            row_index = i

            # Single row as JSON object, multiple rows as JSON array
            body: dict[str, Any] | list[dict[str, Any]]
            if len(batch) == 1:
                body = batch[0]
            else:
                body = batch

            try:
                resp = rate_limiter.execute(
                    session=session,
                    method=method,
                    url=url,
                    json=body,
                    headers=headers,
                )
                resp.raise_for_status()
            except requests.HTTPError as e:
                status = e.response.status_code if e.response else None
                body_text = e.response.text[:200] if e.response else "No response body"
                raise ExecutionError(
                    RivetError(
                        code="RVT-501",
                        message=(
                            f"Write request failed at row {row_index} (HTTP {status}): {body_text}"
                        ),
                        context={
                            "url": url,
                            "method": method,
                            "row_index": row_index,
                            "status_code": status,
                            "response_body": body_text,
                        },
                        remediation=(
                            "Check the API endpoint configuration and credentials. "
                            "Verify the request body format matches the API expectations."
                        ),
                    )
                ) from e
            except ExecutionError:
                # Re-raise ExecutionError from rate limiter (max retries exhausted)
                raise
            except Exception as e:
                raise ExecutionError(
                    RivetError(
                        code="RVT-501",
                        message=f"Write request failed at row {row_index}: {e}",
                        context={
                            "url": url,
                            "method": method,
                            "row_index": row_index,
                        },
                        remediation="Check network connectivity and API endpoint health.",
                    )
                ) from e
