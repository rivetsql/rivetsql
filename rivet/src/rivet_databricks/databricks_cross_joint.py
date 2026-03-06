"""CrossJointAdapter for Databricks engine boundaries.

Handles data flow when a Databricks engine consumes upstream results: resolves
whether to use a native Unity Catalog reference, reject unsupported Arrow data,
or fall through to arrow passthrough.

This module is distinct from ``adapter.py`` which contains the ComputeEngineAdapter
for DuckDB-based read/write dispatch against Databricks-managed storage.
"""

from __future__ import annotations

from typing import Any

from rivet_core.plugins import (
    CrossJointAdapter,
    CrossJointContext,
    UpstreamResolution,
)
from rivet_core.strategies import MaterializedRef


class DatabricksCrossJointAdapter(CrossJointAdapter):
    """CrossJointAdapter for resolving data flow at Databricks engine boundaries.

    Determines how a Databricks consumer engine accesses upstream materialized data:
    - Unity/Databricks catalog sources → native_reference (zero-copy table reference)
    - Local Arrow data → unsupported (cannot upload to remote cluster)
    - Other cases → arrow_passthrough

    Registered via ``registry.register_cross_joint_adapter()`` in ``__init__.py``.
    """

    consumer_engine_type = "databricks"
    producer_engine_type = "databricks"

    def resolve_upstream(
        self,
        producer_ref: MaterializedRef,
        consumer_engine: Any,
        joint_context: CrossJointContext,
    ) -> UpstreamResolution:
        if joint_context.producer_catalog_type in ("unity", "databricks") and joint_context.producer_table:
            return UpstreamResolution(
                strategy="native_reference",
                table_reference=joint_context.producer_table,
            )

        if producer_ref.storage_type == "arrow":
            return UpstreamResolution(
                strategy="unsupported",
                message=(
                    f"Cannot transfer local Arrow data to Databricks. "
                    f"Producer '{joint_context.producer_joint_name}' outputs "
                    f"storage_type='arrow'. Use a different engine or ensure "
                    f"upstream data is in Unity Catalog."
                ),
            )

        return UpstreamResolution(strategy="arrow_passthrough")
