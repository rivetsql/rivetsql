"""SQL resolution for execution.

This module provides shared SQL resolution logic used by both the Executor
and compilation output rendering. It determines the final SQL string that
will be executed on the engine after all optimizations and transformations.
"""

from __future__ import annotations

from rivet_core.compiler import CompiledJoint
from rivet_core.optimizer import FusedGroup


def resolve_execution_sql(
    group: FusedGroup,
    joint_map: dict[str, CompiledJoint],
    adapter_read_sources: set[str],
    has_materialized_inputs: bool = False,
) -> str | None:
    """Resolve the SQL string for execution.

    Rewrites adapter-backed source CTE bodies to ``SELECT * FROM <name>``
    so the engine reads from the registered Arrow table (input_tables).
    When a reference resolver has produced resolved SQL, that is returned
    directly since it already contains fully-qualified table references.
    Falls back through fusion_result and group-level SQL attributes.

    When *has_materialized_inputs* is True (upstream data was materialized
    across an engine boundary), resolved SQL is skipped because the
    receiving engine works with in-memory tables registered by joint name,
    not catalog-qualified references.

    Args:
        group: The fused group to resolve SQL for
        joint_map: Mapping of joint names to CompiledJoint objects
        adapter_read_sources: Set of joint names that read via adapters
        has_materialized_inputs: Whether upstream data was materialized

    Returns:
        The resolved SQL string, or None if no SQL is available
    """
    # Prefer resolved SQL (reference-resolver output) when available
    # AND the engine can natively resolve catalog references (no
    # materialized inputs from an engine boundary).
    if not has_materialized_inputs:
        resolved = None
        if group.fusion_result:
            resolved = group.fusion_result.resolved_fused_sql
        if resolved is None:
            resolved = group.resolved_sql
        if resolved is not None:
            return resolved

    # No reference resolver ran — rewrite adapter-read sources as CTEs
    # so the engine can resolve them from input_tables.
    sql: str | None = None
    if adapter_read_sources and len(group.joints) > 1:
        from rivet_core.optimizer import _compose_cte

        rewritten_joint_sql: dict[str, str | None] = {}
        for jn in group.joints:
            cj = joint_map.get(jn)
            if jn in adapter_read_sources:
                rewritten_joint_sql[jn] = f"SELECT * FROM {jn}"
            elif cj:
                rewritten_joint_sql[jn] = cj.sql_resolved or cj.sql_translated or cj.sql
            else:
                rewritten_joint_sql[jn] = None
        rewritten = _compose_cte(group.joints, rewritten_joint_sql)
        if rewritten:
            sql = rewritten.fused_sql
    if sql is None and group.fusion_result:
        sql = group.fusion_result.fused_sql
    if sql is None:
        sql = group.fused_sql
    return sql
