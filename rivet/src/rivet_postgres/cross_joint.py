"""Cross-joint adapter for Postgres engine boundaries."""

from __future__ import annotations

from typing import Any

from rivet_core.plugins import (
    CrossJointAdapter,
    CrossJointContext,
    UpstreamResolution,
)
from rivet_core.strategies import MaterializedRef


class PostgresCrossJointAdapter(CrossJointAdapter):
    """Handles data flow at Postgres engine boundaries.

    - Same-server postgres catalog → native_reference (schema.table)
    - Non-postgres producer → unsupported (cannot consume local Arrow)
    """

    consumer_engine_type = "postgres"
    producer_engine_type = "postgres"

    def resolve_upstream(
        self,
        producer_ref: MaterializedRef,
        consumer_engine: Any,
        joint_context: CrossJointContext,
    ) -> UpstreamResolution:
        if joint_context.producer_catalog_type == "postgres" and joint_context.producer_table:
            return UpstreamResolution(
                strategy="native_reference",
                table_reference=joint_context.producer_table,
            )

        if producer_ref.storage_type == "arrow":
            return UpstreamResolution(
                strategy="unsupported",
                message=(
                    f"Cannot transfer local Arrow data to Postgres. "
                    f"Producer '{joint_context.producer_joint_name}' outputs "
                    f"storage_type='arrow'. Ensure upstream data is accessible "
                    f"as Postgres tables."
                ),
            )

        return UpstreamResolution(strategy="arrow_passthrough")
