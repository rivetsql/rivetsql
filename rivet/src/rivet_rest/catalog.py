"""REST API catalog plugin for Rivet.

Treats REST API endpoints as logical tables. Configured via ``profiles.yaml``
with ``type: rest_api``, a ``base_url``, and an ``endpoints`` mapping.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

import requests

from rivet_core.errors import PluginValidationError, plugin_error
from rivet_core.models import Catalog
from rivet_core.plugins import CatalogPlugin

if TYPE_CHECKING:
    from rivet_core.introspection import CatalogNode, ObjectMetadata, ObjectSchema

_VALID_AUTH_TYPES = frozenset({"none", "bearer", "basic", "api_key", "oauth2"})
_VALID_RESPONSE_FORMATS = frozenset({"json", "csv"})
_URL_PATTERN = re.compile(r"^https?://", re.IGNORECASE)


class RestApiCatalogPlugin(CatalogPlugin):
    """CatalogPlugin for REST API endpoints."""

    type = "rest_api"
    required_options: list[str] = ["base_url"]
    optional_options: dict[str, Any] = {
        "endpoints": {},
        "default_headers": {},
        "response_format": "json",
        "timeout": 30,
        "rate_limit": None,
        "max_retries": 3,
        "max_flatten_depth": 3,
    }
    credential_options: list[str] = [
        "token",
        "username",
        "password",
        "api_key_value",
        "client_id",
        "client_secret",
    ]
    credential_groups: dict[str, list[str]] = {
        "none": [],
        "bearer": ["token"],
        "basic": ["username", "password"],
        "api_key": ["api_key_value"],
        "oauth2": ["client_id", "client_secret", "token_url"],
    }
    env_var_hints: dict[str, str] = {
        "token": "REST_API_TOKEN",
        "username": "REST_API_USERNAME",
        "password": "REST_API_PASSWORD",
        "api_key_value": "REST_API_KEY",
        "client_id": "REST_API_CLIENT_ID",
        "client_secret": "REST_API_CLIENT_SECRET",
    }

    # ── Validation ────────────────────────────────────────────────

    def validate(self, options: dict[str, Any]) -> None:
        base_url = options.get("base_url")
        if not base_url:
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    "base_url is required for rest_api catalog.",
                    plugin_name="rivet_rest",
                    plugin_type="catalog",
                    remediation="Add a 'base_url' option (e.g. 'https://api.example.com/v1').",
                )
            )

        if not isinstance(base_url, str) or not _URL_PATTERN.match(base_url):
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"base_url must be an http:// or https:// URL, got: {base_url!r}",
                    plugin_name="rivet_rest",
                    plugin_type="catalog",
                    remediation="Use a URL starting with http:// or https://.",
                )
            )

        auth = options.get("auth", "none")
        if auth not in _VALID_AUTH_TYPES:
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"Unknown auth type: {auth!r}",
                    plugin_name="rivet_rest",
                    plugin_type="catalog",
                    remediation=f"Valid auth types: {', '.join(sorted(_VALID_AUTH_TYPES))}",
                )
            )

        response_format = options.get("response_format", "json")
        if response_format not in _VALID_RESPONSE_FORMATS:
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"Unsupported response_format: {response_format!r}",
                    plugin_name="rivet_rest",
                    plugin_type="catalog",
                    remediation="Use 'json' or 'csv'.",
                )
            )

        endpoints = options.get("endpoints", {})
        if not isinstance(endpoints, dict):
            raise PluginValidationError(
                plugin_error(
                    "RVT-201",
                    f"endpoints must be a dict, got {type(endpoints).__name__}: {endpoints!r}",
                    plugin_name="rivet_rest",
                    plugin_type="catalog",
                    remediation="Provide endpoints as a dict mapping endpoint names to configurations, or use {} for no endpoints.",
                )
            )
        for name, cfg in endpoints.items():
            if not isinstance(cfg, dict) or "path" not in cfg:
                raise PluginValidationError(
                    plugin_error(
                        "RVT-201",
                        f"Endpoint '{name}' is missing required 'path' field.",
                        plugin_name="rivet_rest",
                        plugin_type="catalog",
                        remediation=f"Add a 'path' field to the '{name}' endpoint configuration.",
                    )
                )

    # ── Instantiation ─────────────────────────────────────────────

    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog:
        self.validate(options)
        return Catalog(name=name, type="rest_api", options=options)

    # ── Table reference ───────────────────────────────────────────

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        return logical_name

    # ── Introspection ─────────────────────────────────────────────

    def list_tables(self, catalog: Catalog) -> list[CatalogNode]:
        from rivet_core.introspection import CatalogNode, NodeSummary

        endpoints: dict[str, Any] = catalog.options.get("endpoints", {})
        nodes: list[CatalogNode] = []
        base_url = catalog.options.get("base_url", "")
        for name, cfg in endpoints.items():
            path = cfg.get("path", "")
            full_url = _resolve_url(base_url, path)
            nodes.append(
                CatalogNode(
                    name=name,
                    node_type="endpoint",
                    path=[catalog.name, name],
                    is_container=False,
                    children_count=None,
                    summary=NodeSummary(
                        row_count=None,
                        size_bytes=None,
                        format=catalog.options.get("response_format", "json"),
                        last_modified=None,
                        owner=None,
                        comment=full_url,
                    ),
                )
            )
        return nodes

    def list_children(self, catalog: Catalog, path: list[str]) -> list[CatalogNode]:
        if not path:
            return self.list_tables(catalog)
        return []

    def get_schema(self, catalog: Catalog, table: str) -> ObjectSchema:
        from rivet_core.introspection import ColumnDetail, ObjectSchema

        endpoints: dict[str, Any] = catalog.options.get("endpoints", {})
        cfg = endpoints.get(table, {})

        # If explicit schema is declared, use it without HTTP request
        declared_schema = cfg.get("schema")
        if declared_schema and isinstance(declared_schema, dict):
            columns = [
                ColumnDetail(
                    name=col_name,
                    type=col_type,
                    native_type=col_type,
                    nullable=True,
                    default=None,
                    comment=None,
                    is_primary_key=False,
                    is_partition_key=False,
                )
                for col_name, col_type in declared_schema.items()
            ]
            return ObjectSchema(
                path=[catalog.name, table],
                node_type="endpoint",
                columns=columns,
                primary_key=None,
                comment=None,
            )

        # Sample request for schema inference
        return self._infer_schema_from_sample(catalog, table, cfg)

    def get_metadata(self, catalog: Catalog, table: str) -> ObjectMetadata | None:
        from rivet_core.introspection import ObjectMetadata

        endpoints: dict[str, Any] = catalog.options.get("endpoints", {})
        cfg = endpoints.get(table)
        if cfg is None:
            return None

        base_url = catalog.options.get("base_url", "")
        path = cfg.get("path", "")
        full_url = _resolve_url(base_url, path)

        return ObjectMetadata(
            path=[catalog.name, table],
            node_type="endpoint",
            row_count=None,
            size_bytes=None,
            last_modified=None,
            created_at=None,
            format=catalog.options.get("response_format", "json"),
            compression=None,
            owner=None,
            comment=None,
            location=full_url,
            column_statistics=[],
            partitioning=None,
        )

    def test_connection(self, catalog: Catalog) -> None:
        base_url = catalog.options.get("base_url", "")
        timeout = catalog.options.get("timeout", 30)
        session = _create_session(catalog.options)
        try:
            resp = session.head(base_url, timeout=timeout)
            # Some APIs don't support HEAD — fall back to GET
            if resp.status_code == 405:
                resp = session.get(base_url, timeout=timeout)
            resp.raise_for_status()
        except requests.RequestException as exc:
            from rivet_core.errors import ExecutionError, RivetError

            raise ExecutionError(
                RivetError(
                    code="RVT-501",
                    message=f"Connection test failed for {base_url}: {exc}",
                    context={"url": base_url},
                    remediation="Check the base_url, network connectivity, and authentication.",
                )
            ) from exc
        finally:
            session.close()

    # ── Private helpers ───────────────────────────────────────────

    def _infer_schema_from_sample(
        self, catalog: Catalog, table: str, cfg: dict[str, Any]
    ) -> ObjectSchema:
        from rivet_core.introspection import ColumnDetail, ObjectSchema
        from rivet_rest.flatten import flatten_records, infer_schema

        base_url = catalog.options.get("base_url", "")
        path = cfg.get("path", "")
        url = _resolve_url(base_url, path)
        method = cfg.get("method", "GET")
        timeout = catalog.options.get("timeout", 30)
        max_depth = catalog.options.get("max_flatten_depth", 3)

        session = _create_session(catalog.options)
        try:
            # Limit to a small sample
            params = dict(cfg.get("params", {}))
            params["limit"] = "1"

            resp = session.request(method, url, params=params, timeout=timeout)
            resp.raise_for_status()

            data = resp.json()
            response_path = cfg.get("response_path")
            records = _extract_records(data, response_path)

            if not records:
                return ObjectSchema(
                    path=[catalog.name, table],
                    node_type="endpoint",
                    columns=[],
                    primary_key=None,
                    comment=None,
                )

            flat = flatten_records(records, max_depth)
            arrow_schema = infer_schema(flat)

            columns = [
                ColumnDetail(
                    name=field.name,
                    type=str(field.type),
                    native_type="json",
                    nullable=True,
                    default=None,
                    comment=None,
                    is_primary_key=False,
                    is_partition_key=False,
                )
                for field in arrow_schema
            ]
            return ObjectSchema(
                path=[catalog.name, table],
                node_type="endpoint",
                columns=columns,
                primary_key=None,
                comment=None,
            )
        except requests.RequestException as exc:
            from rivet_core.errors import ExecutionError, RivetError

            raise ExecutionError(
                RivetError(
                    code="RVT-501",
                    message=f"Schema inference failed for endpoint '{table}': {exc}",
                    context={"url": url, "endpoint": table},
                    remediation="Check the endpoint path and authentication. "
                    "Alternatively, declare an explicit 'schema' in the endpoint config.",
                )
            ) from exc
        finally:
            session.close()


# ── Module-level helpers ──────────────────────────────────────────────


def _resolve_url(base_url: str, path: str) -> str:
    """Join base URL and endpoint path."""
    if not base_url:
        return path
    # Ensure base_url ends with / for proper urljoin behavior
    if not base_url.endswith("/"):
        base_url = base_url + "/"
    # Strip leading / from path to avoid replacing the base path
    return urljoin(base_url, path.lstrip("/"))


def _create_session(options: dict[str, Any]) -> requests.Session:
    """Create a configured requests.Session from catalog options."""
    from rivet_rest.auth import create_auth

    session = requests.Session()

    # Apply default headers
    default_headers = options.get("default_headers", {})
    if default_headers:
        session.headers.update(default_headers)

    # Apply auth
    auth_type = options.get("auth", "none")
    auth_strategy = create_auth(auth_type, options)
    auth_strategy.apply(session)

    return session


def _extract_records(data: Any, response_path: str | None) -> list[dict[str, Any]]:
    """Extract records from a JSON response using the response_path."""
    if response_path:
        for part in response_path.split("."):
            if isinstance(data, dict):
                data = data.get(part)
            else:
                return []
            if data is None:
                return []

    if isinstance(data, list):
        return data  # type: ignore[return-value]
    if isinstance(data, dict):
        return [data]
    return []
