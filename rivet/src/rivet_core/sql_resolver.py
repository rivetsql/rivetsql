"""SQL resolution for execution.

This module provides shared SQL resolution logic used by both the Executor
and compilation output rendering. It determines the final SQL string that
will be executed on the engine after all optimizations and transformations.
"""

from __future__ import annotations

from rivet_core.compiler.models import CompiledJoint
from rivet_core.optimizer import FusedGroup


def resolve_execution_sql(
    group: FusedGroup,
    joint_map: dict[str, CompiledJoint],
    adapter_read_sources: set[str],
    has_materialized_inputs: bool = False,
) -> str | None:
    """Resolve the SQL string for execution.

    Rewrites source CTE bodies to ``SELECT * FROM <name>`` so the engine
    reads from the registered Arrow table (input_tables).  This applies to
    all source joints whose data was read into input_tables — whether via
    an adapter or the fallback source plugin — because the compiled SQL
    references the ``table_map``-resolved physical name while the engine
    registers the table under the joint name.

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
        adapter_read_sources: Set of source joint names read into input_tables
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

    # Determine the sqlglot dialect for this group so that CTE
    # extraction round-trips type names correctly (e.g. STRING stays
    # STRING for Spark instead of being normalised to TEXT).
    group_dialect: str | None = None
    for jn in group.joints:
        cj = joint_map.get(jn)
        if cj and cj.engine_dialect:
            group_dialect = cj.engine_dialect
            break

    # No reference resolver ran — re-compose from translated SQL so that
    # dialect-specific types (e.g. TEXT→STRING for Spark) are applied.
    # Source joints read into input_tables are rewritten to
    # ``SELECT * FROM <joint_name>`` so the engine resolves them by name.
    #
    # When has_materialized_inputs is True, upstream data lives in
    # input_tables keyed by joint name.  ``sql_resolved`` must be skipped
    # because it contains catalog-qualified references (e.g.
    # ``read_csv_auto(...)`` for DuckDB filesystem) that would bypass the
    # in-memory tables.  ``sql_translated`` references joint names and
    # carries the correct dialect types.
    sql: str | None = None
    if len(group.joints) > 1:
        from rivet_core.optimizer import _compose_cte

        rewritten_joint_sql: dict[str, str | None] = {}
        for jn in group.joints:
            cj = joint_map.get(jn)
            if jn in adapter_read_sources:
                rewritten_joint_sql[jn] = f"SELECT * FROM {jn}"
            elif cj:
                if has_materialized_inputs:
                    rewritten_joint_sql[jn] = cj.sql_translated or cj.sql
                else:
                    rewritten_joint_sql[jn] = cj.sql_resolved or cj.sql_translated or cj.sql
            else:
                rewritten_joint_sql[jn] = None
        rewritten = _compose_cte(
            group.joints,
            rewritten_joint_sql,
            dialect=group_dialect or "duckdb",
        )
        if rewritten:
            sql = rewritten.fused_sql
    elif len(group.joints) == 1:
        cj = joint_map.get(group.joints[0])
        if cj:
            if has_materialized_inputs:
                translated = cj.sql_translated
            else:
                translated = cj.sql_resolved or cj.sql_translated
            if translated:
                sql = translated
    if sql is None and group.fusion_result:
        sql = group.fusion_result.fused_sql
    if sql is None:
        sql = group.fused_sql
    return sql
