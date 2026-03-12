"""REST API source plugin (fallback + introspection).

The primary read path goes through ``RestApiAdapter.read_dispatch()``.
This source is registered for catalog introspection (used by
``RestApiCatalogPlugin.get_schema()`` for sample requests) and as a fallback
when no adapter is resolved.
"""

from __future__ import annotations

from typing import Any

from rivet_core.models import Material
from rivet_core.plugins import SourcePlugin
from rivet_rest.adapter import (
    RestApiDeferredRef,
    _build_session_config,
    _resolve_endpoint,
)


class RestApiSource(SourcePlugin):
    """Fallback source for REST API catalogs.

    The primary read path goes through ``RestApiAdapter.read_dispatch()``.
    This source is registered for catalog introspection and as a fallback
    when no adapter is resolved.

    Unlike the adapter path, the source fallback does not receive pushdown
    plans, so no predicate-to-query-param translation occurs here.
    """

    catalog_type = "rest_api"

    def read(
        self,
        catalog: Any,
        joint: Any,
        pushdown: Any | None = None,
    ) -> Material:
        """Return a deferred Material for the REST API endpoint.

        Resolves the endpoint from ``joint.table`` (fallback to ``joint.name``),
        creates a ``RestApiDeferredRef`` with session config, and returns a
        deferred ``Material``.

        ``joint.sql`` is accepted as ``None`` — REST API sources don't use SQL.

        Args:
            catalog: The REST API catalog instance
            joint: The source joint (table or name identifies the endpoint)
            pushdown: Ignored (source fallback path doesn't support pushdown)

        Returns:
            Material in deferred state backed by RestApiDeferredRef
        """
        endpoint_config = _resolve_endpoint(catalog, joint)
        options: dict[str, Any] = catalog.options if hasattr(catalog, "options") else {}

        ref = RestApiDeferredRef(
            session_config=_build_session_config(catalog),
            endpoint_config=endpoint_config,
            query_params={},  # No pushdown in source fallback path
            rate_limit_config=options.get("rate_limit"),
            max_flatten_depth=options.get("max_flatten_depth", 3),
            response_format=options.get("response_format", "json"),
            base_url=options.get("base_url", ""),
        )

        return Material(
            name=getattr(joint, "name", "rest_api_read"),
            catalog=getattr(catalog, "name", "rest_api"),
            materialized_ref=ref,
            state="deferred",
        )
