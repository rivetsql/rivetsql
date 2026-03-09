"""Optimizer: fusion and pushdown passes.

The optimizer runs two passes during compilation: fusion then pushdown.
Both passes are pure functions that produce metadata — no data operations.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any

from rivet_core.lineage import ColumnLineage, ColumnOrigin
from rivet_core.sql_parser import LogicalPlan, Predicate

if TYPE_CHECKING:
    from rivet_core.compiler import CompiledJoint, OptimizationResult
    from rivet_core.models import Material


# ---------------------------------------------------------------------------
# Data models (from task 11.1)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FusionResult:
    fused_sql: str
    statements: list[str]
    final_select: str
    resolved_fused_sql: str | None = None
    resolved_statements: list[str] | None = None
    resolved_final_select: str | None = None


@dataclass(frozen=True)
class Cast:
    column: str
    from_type: str
    to_type: str


@dataclass(frozen=True)
class PredicatePushdownResult:
    pushed: list[Predicate]
    residual: list[Predicate]


@dataclass(frozen=True)
class ProjectionPushdownResult:
    pushed_columns: list[str] | None  # None = all columns
    reason: str | None


@dataclass(frozen=True)
class LimitPushdownResult:
    pushed_limit: int | None
    residual_limit: int | None
    reason: str | None


@dataclass(frozen=True)
class CastPushdownResult:
    pushed: list[Cast]
    residual: list[Cast]


@dataclass(frozen=True)
class PushdownPlan:
    predicates: PredicatePushdownResult
    projections: ProjectionPushdownResult
    limit: LimitPushdownResult
    casts: CastPushdownResult


@dataclass(frozen=True)
class ResidualPlan:
    predicates: list[Predicate]
    limit: int | None
    casts: list[Cast]


EMPTY_RESIDUAL = ResidualPlan(predicates=[], limit=None, casts=[])


@dataclass(frozen=True)
class AdapterPushdownResult:
    """Return value from adapter.read_dispatch with pushdown support."""

    material: Material
    residual: ResidualPlan


@dataclass(frozen=True)
class FusedGroup:
    id: str
    joints: list[str]  # joint names in execution order
    engine: str  # engine instance name
    engine_type: str
    adapters: dict[str, str | None]  # joint name → adapter name
    fused_sql: str | None  # composed CTE SQL (user-facing)
    fusion_strategy: str = "cte"  # "cte" or "temp_view"
    fusion_result: FusionResult | None = None
    resolved_sql: str | None = None  # engine-native fused SQL
    entry_joints: list[str] = field(default_factory=list)
    exit_joints: list[str] = field(default_factory=list)
    pushdown: PushdownPlan | None = None
    residual: ResidualPlan | None = None
    materialization_strategy_name: str = "arrow"
    per_joint_predicates: dict[str, list[Predicate]] = field(default_factory=dict)
    per_joint_projections: dict[str, list[str]] = field(default_factory=dict)
    per_joint_limits: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class OptimizerResult:
    fused_groups: list[FusedGroup]


# ---------------------------------------------------------------------------
# Fusion input — lightweight view of a joint for the fusion pass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FusionJoint:
    """Minimal joint info needed by the fusion pass.

    The compiler constructs these from CompiledJoint before calling the optimizer.
    """

    name: str
    joint_type: str  # "source", "sql", "sink", "python"
    upstream: list[str]
    engine: str  # engine instance name
    engine_type: str
    adapter: str | None = None
    eager: bool = False
    has_assertions: bool = False
    sql: str | None = None


# ---------------------------------------------------------------------------
# Fusion pass implementation
# ---------------------------------------------------------------------------


def _downstream_counts(joints: list[FusionJoint]) -> dict[str, int]:
    """Count how many downstream consumers each joint has."""
    counts: dict[str, int] = {j.name: 0 for j in joints}
    for j in joints:
        for up in j.upstream:
            counts[up] = counts.get(up, 0) + 1
    return counts


def _can_fuse(
    joint: FusionJoint,
    upstream_name: str,
    upstream_joint: FusionJoint,
    downstream_counts: dict[str, int],
    *,
    all_joints: list[FusionJoint] | None = None,
) -> bool:
    """Return True when *joint* can merge into the upstream joint's group.

    All six conditions from Req 18.2 must hold:
    1. Same engine instance
    2. No eager flag on upstream
    3. No assertions on upstream
    4. Upstream has exactly one downstream consumer (single-consumer rule)
       — relaxed when *all_joints* is provided and every consumer of the
       upstream is the same multi-input *joint*.
    5. Downstream joint is not a PythonJoint
    6. Upstream joint is not a PythonJoint (Python joints must be standalone
       groups so the executor can dispatch them via _execute_python_joint)
    """
    if joint.engine != upstream_joint.engine:
        return False
    if upstream_joint.eager:
        return False
    if upstream_joint.has_assertions:
        return False
    if downstream_counts.get(upstream_name, 0) != 1:
        # Relaxation: if all downstream consumers of upstream are the same joint, allow fusion
        if all_joints is not None:
            consumers = [j for j in all_joints if upstream_name in j.upstream]
            if not all(c.name == joint.name for c in consumers):
                return False
        else:
            return False
    if joint.joint_type == "python":
        return False
    if upstream_joint.joint_type == "python":
        return False
    return True


def _compose_cte(
    group_joints: list[str],
    joint_sql: dict[str, str | None],
) -> FusionResult | None:
    """Compose CTE SQL for a fused group.

    Chain upstream joints as WITH clauses; the last joint's SQL is the final SELECT.
    """
    sqls: list[tuple[str, str]] = []
    for name in group_joints:
        sql = joint_sql.get(name)
        if sql is not None:
            sqls.append((name, sql))

    if not sqls:
        return None

    # Single joint — no CTE needed
    if len(sqls) == 1:
        _, final = sqls[0]
        return FusionResult(
            fused_sql=final,
            statements=[],
            final_select=final,
        )

    cte_parts: list[str] = []
    for name, sql in sqls[:-1]:
        cte_parts.append(f"{name} AS (\n    {sql}\n)")

    _, final_sql = sqls[-1]
    fused = "WITH " + ",\n".join(cte_parts) + "\n" + final_sql

    return FusionResult(
        fused_sql=fused,
        statements=[f"{n} AS (\n    {s}\n)" for n, s in sqls[:-1]],
        final_select=final_sql,
    )


def _compose_temp_view(
    group_joints: list[str],
    joint_sql: dict[str, str | None],
) -> FusionResult | None:
    """Compose TempView SQL for a fused group.

    Produce CREATE TEMPORARY VIEW for each intermediate, final SELECT for the last.
    """
    sqls: list[tuple[str, str]] = []
    for name in group_joints:
        sql = joint_sql.get(name)
        if sql is not None:
            sqls.append((name, sql))

    if not sqls:
        return None

    if len(sqls) == 1:
        _, final = sqls[0]
        return FusionResult(
            fused_sql=final,
            statements=[],
            final_select=final,
        )

    stmts: list[str] = []
    for name, sql in sqls[:-1]:
        stmts.append(f"CREATE TEMPORARY VIEW {name} AS ({sql})")

    _, final_sql = sqls[-1]
    all_parts = stmts + [final_sql]
    fused = ";\n".join(all_parts)

    return FusionResult(
        fused_sql=fused,
        statements=stmts,
        final_select=final_sql,
    )


def _compute_entry_exit(
    group_joints_set: set[str],
    joints_by_name: dict[str, FusionJoint],
    group_joint_names: list[str],
) -> tuple[list[str], list[str]]:
    """Compute entry and exit joints for a group.

    Entry joints: joints whose upstreams are all outside the group (or have no upstream).
    Exit joints: joints that have at least one downstream outside the group (or no downstream in group).
    """
    # Build set of all downstream within group
    downstream_in_group: dict[str, set[str]] = {n: set() for n in group_joint_names}
    for name in group_joint_names:
        j = joints_by_name[name]
        for up in j.upstream:
            if up in group_joints_set:
                downstream_in_group[up].add(name)

    entries: list[str] = []
    exits: list[str] = []

    for name in group_joint_names:
        j = joints_by_name[name]
        # Entry: no upstream within the group
        if not any(up in group_joints_set for up in j.upstream):
            entries.append(name)
        # Exit: no downstream within the group
        if not downstream_in_group[name]:
            exits.append(name)

    return entries, exits


def _assign_fusion_groups(
    joints: list[FusionJoint],
    joints_by_name: dict[str, FusionJoint],
    ds_counts: dict[str, int],
) -> tuple[dict[str, str], dict[str, list[str]], dict[str, str], dict[str, str]]:
    """Assign each joint to a fusion group. Returns (group_id_for, group_joints, group_engine, group_engine_type)."""
    import hashlib

    group_id_for: dict[str, str] = {}
    group_joints: dict[str, list[str]] = {}
    group_engine: dict[str, str] = {}
    group_engine_type: dict[str, str] = {}

    for joint in joints:
        candidate_gid: str | None = None

        if joint.joint_type != "python":
            if len(joint.upstream) == 1:
                up_name = joint.upstream[0]
                up_joint = joints_by_name[up_name]
                if _can_fuse(joint, up_name, up_joint, ds_counts):
                    candidate_gid = group_id_for[up_name]
            elif len(joint.upstream) > 1:
                eligible_gids: list[str] = []
                for up_name in joint.upstream:
                    up_joint = joints_by_name[up_name]
                    if _can_fuse(joint, up_name, up_joint, ds_counts, all_joints=joints):
                        gid = group_id_for[up_name]
                        if gid not in eligible_gids:
                            eligible_gids.append(gid)
                if eligible_gids:
                    target_gid = eligible_gids[0]
                    for other_gid in eligible_gids[1:]:
                        if other_gid != target_gid:
                            group_joints[target_gid].extend(group_joints[other_gid])
                            for moved_name in group_joints[other_gid]:
                                group_id_for[moved_name] = target_gid
                            del group_joints[other_gid]
                            del group_engine[other_gid]
                            del group_engine_type[other_gid]
                    candidate_gid = target_gid

        if candidate_gid is not None:
            group_joints[candidate_gid].append(joint.name)
            group_id_for[joint.name] = candidate_gid
        else:
            gid = str(uuid.UUID(hashlib.md5(joint.name.encode()).hexdigest()))
            group_joints[gid] = [joint.name]
            group_engine[gid] = joint.engine
            group_engine_type[gid] = joint.engine_type
            group_id_for[joint.name] = gid

    return group_id_for, group_joints, group_engine, group_engine_type


def fusion_pass(
    joints: list[FusionJoint],
    fusion_strategy: str = "cte",
) -> list[FusedGroup]:
    """Walk DAG in topological order and fuse eligible adjacent joints."""
    joints_by_name: dict[str, FusionJoint] = {j.name: j for j in joints}
    ds_counts = _downstream_counts(joints)

    group_id_for, group_joints, group_engine, group_engine_type = _assign_fusion_groups(
        joints, joints_by_name, ds_counts,
    )

    joint_sql: dict[str, str | None] = {j.name: j.sql for j in joints}

    seen_gids: list[str] = []
    for j in joints:
        gid = group_id_for[j.name]
        if gid not in seen_gids:
            seen_gids.append(gid)

    result: list[FusedGroup] = []
    for gid in seen_gids:
        names = group_joints[gid]
        group_set = set(names)
        composer = _compose_cte if fusion_strategy == "cte" else _compose_temp_view
        fusion_result = composer(names, joint_sql)
        entries, exits = _compute_entry_exit(group_set, joints_by_name, names)
        adapters: dict[str, str | None] = {
            n: joints_by_name[n].adapter for n in names
        }
        result.append(
            FusedGroup(
                id=gid, joints=names, engine=group_engine[gid],
                engine_type=group_engine_type[gid], adapters=adapters,
                fused_sql=fusion_result.fused_sql if fusion_result else None,
                fusion_strategy=fusion_strategy, fusion_result=fusion_result,
                entry_joints=entries, exit_joints=exits,
            )
        )
    return result


# ---------------------------------------------------------------------------
# Pushdown pass implementation
# ---------------------------------------------------------------------------

# Subquery indicators in predicate expressions
_SUBQUERY_RE = re.compile(r"\b(SELECT|EXISTS|IN\s*\()\b", re.IGNORECASE)

# Widening numeric casts that are always safe to push
_NUMERIC_WIDENING: dict[str, set[str]] = {
    "int8": {"int16", "int32", "int64", "float32", "float64"},
    "int16": {"int32", "int64", "float32", "float64"},
    "int32": {"int64", "float64"},
    "int64": {"float64"},
    "float32": {"float64"},
}

_STRING_TYPES = {"utf8", "large_utf8", "string"}


def _has_subquery(pred: Predicate) -> bool:
    return bool(_SUBQUERY_RE.search(pred.expression))


def _is_or_predicate(pred: Predicate) -> bool:
    """Check if predicate is an OR compound (top-level OR)."""
    expr = pred.expression.strip()
    # Simple heuristic: contains " OR " at top level (not inside parens)
    depth = 0
    upper = expr.upper()
    for i, ch in enumerate(expr):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0 and upper[i:i + 4] == " OR ":
            return True
    return False


def _split_and_conjuncts(pred: Predicate) -> list[Predicate]:
    """Split an AND predicate into independent conjuncts.

    OR predicates are atomic and not split.
    """
    if _is_or_predicate(pred):
        return [pred]

    expr = pred.expression.strip()
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    upper = expr.upper()

    i = 0
    while i < len(expr):
        ch = expr[i]
        if ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif depth == 0 and upper[i:i + 5] == " AND ":
            parts.append("".join(current).strip())
            current = []
            i += 5
            continue
        else:
            current.append(ch)
        i += 1

    remainder = "".join(current).strip()
    if remainder:
        parts.append(remainder)

    if len(parts) <= 1:
        return [pred]

    result: list[Predicate] = []
    for part in parts:
        # Extract column references from the part (best-effort: reuse columns
        # that appear in this sub-expression)
        cols = [c for c in pred.columns if c.lower() in part.lower()]
        result.append(Predicate(expression=part, columns=cols, location=pred.location))
    return result


def _pushdown_projections(
    logical_plan: LogicalPlan | None,
    capabilities: list[str],
) -> ProjectionPushdownResult:
    if logical_plan is None:
        return ProjectionPushdownResult(pushed_columns=None, reason="no_logical_plan")

    if "projection_pushdown" not in capabilities:
        return ProjectionPushdownResult(pushed_columns=None, reason="capability_gap")

    # Check for SELECT * — any projection with expression "*"
    for proj in logical_plan.projections:
        if proj.expression.strip() == "*":
            return ProjectionPushdownResult(pushed_columns=None, reason="not_applicable")

    # Collect all referenced columns from projections
    columns: set[str] = set()
    for proj in logical_plan.projections:
        columns.update(proj.source_columns)

    # Also include columns from predicates, joins, ordering, aggregations
    for pred in logical_plan.predicates:
        columns.update(pred.columns)
    for join in logical_plan.joins:
        columns.update(join.columns)
    if logical_plan.aggregations:
        columns.update(logical_plan.aggregations.group_by)
    if logical_plan.ordering:
        columns.update(col for col, _ in logical_plan.ordering.columns)

    if not columns:
        return ProjectionPushdownResult(pushed_columns=None, reason="no_columns_detected")

    return ProjectionPushdownResult(
        pushed_columns=sorted(columns),
        reason=None,
    )


def _pushdown_predicates(
    logical_plan: LogicalPlan | None,
    capabilities: list[str],
) -> PredicatePushdownResult:
    if logical_plan is None or not logical_plan.predicates:
        return PredicatePushdownResult(pushed=[], residual=[])

    has_cap = "predicate_pushdown" in capabilities

    pushed: list[Predicate] = []
    residual: list[Predicate] = []

    for pred in logical_plan.predicates:
        # HAVING predicates are never pushed to source
        if pred.location == "having":
            residual.append(pred)
            continue

        conjuncts = _split_and_conjuncts(pred)
        for conj in conjuncts:
            if _has_subquery(conj) or not has_cap:
                residual.append(conj)
            else:
                pushed.append(conj)

    return PredicatePushdownResult(pushed=pushed, residual=residual)


def _pushdown_limit(
    logical_plan: LogicalPlan | None,
    capabilities: list[str],
) -> LimitPushdownResult:
    if logical_plan is None or logical_plan.limit is None or logical_plan.limit.count is None:
        return LimitPushdownResult(pushed_limit=None, residual_limit=None, reason=None)

    limit_val = logical_plan.limit.count

    # Unsafe if aggregations, joins, or DISTINCT between source and LIMIT
    if logical_plan.aggregations:
        return LimitPushdownResult(
            pushed_limit=None, residual_limit=limit_val, reason="aggregation_present"
        )
    if logical_plan.joins:
        return LimitPushdownResult(
            pushed_limit=None, residual_limit=limit_val, reason="join_present"
        )
    if logical_plan.distinct:
        return LimitPushdownResult(
            pushed_limit=None, residual_limit=limit_val, reason="distinct_present"
        )

    if "limit_pushdown" not in capabilities:
        return LimitPushdownResult(
            pushed_limit=None, residual_limit=limit_val, reason="capability_gap"
        )

    return LimitPushdownResult(pushed_limit=limit_val, residual_limit=None, reason=None)


def _is_widening_or_to_string(from_type: str, to_type: str) -> bool:
    """Check if a cast is widening numeric or any-to-string (always safe)."""
    if to_type in _STRING_TYPES:
        return True
    widened = _NUMERIC_WIDENING.get(from_type)
    return widened is not None and to_type in widened


def _pushdown_casts(
    logical_plan: LogicalPlan | None,
    capabilities: list[str],
) -> CastPushdownResult:
    if logical_plan is None:
        return CastPushdownResult(pushed=[], residual=[])

    has_cap = "cast_pushdown" in capabilities

    # Extract casts from projections that look like CAST expressions
    casts = _extract_casts_from_plan(logical_plan)
    if not casts:
        return CastPushdownResult(pushed=[], residual=[])

    pushed: list[Cast] = []
    residual: list[Cast] = []

    for cast in casts:
        if not has_cap:
            residual.append(cast)
        elif _is_widening_or_to_string(cast.from_type, cast.to_type):
            pushed.append(cast)
        else:
            residual.append(cast)

    return CastPushdownResult(pushed=pushed, residual=residual)


_CAST_RE = re.compile(
    r"CAST\s*\(\s*(\w+)\s+AS\s+(\w+)\s*\)", re.IGNORECASE
)


def _extract_casts_from_plan(logical_plan: LogicalPlan) -> list[Cast]:
    """Extract Cast objects from projection expressions containing CAST(...)."""
    casts: list[Cast] = []
    for proj in logical_plan.projections:
        for m in _CAST_RE.finditer(proj.expression):
            col = m.group(1)
            to_type = m.group(2).lower()
            # Determine from_type from source columns (best-effort)
            from_type = "unknown"
            casts.append(Cast(column=col, from_type=from_type, to_type=to_type))
    return casts


def pushdown_pass(
    groups: list[FusedGroup],
    logical_plans: dict[str, LogicalPlan | None],
    capabilities: dict[str, list[str]],
    catalog_types: dict[str, str | None],
) -> list[FusedGroup]:
    """Apply pushdown rules to each fused group's entry points.

    Args:
        groups: Fused groups from the fusion pass.
        logical_plans: joint_name → LogicalPlan (from compiled joints).
        capabilities: (engine_type, catalog_type) key as "engine_type:catalog_type" → capabilities list.
        catalog_types: joint_name → catalog_type.

    Returns:
        New list of FusedGroup with pushdown and residual plans set.
    """
    result: list[FusedGroup] = []

    for group in groups:
        # Collect the logical plan from the exit joint (last joint in group)
        exit_joint = group.exit_joints[0] if group.exit_joints else group.joints[-1]
        plan = logical_plans.get(exit_joint)

        # Resolve capabilities for entry joints
        caps: list[str] = []
        for entry in group.entry_joints:
            ct = catalog_types.get(entry)
            key = f"{group.engine_type}:{ct}" if ct else group.engine_type
            entry_caps = capabilities.get(key, [])
            if entry_caps:
                caps = entry_caps
                break
        # Fallback: try engine_type alone
        if not caps:
            caps = capabilities.get(group.engine_type, [])

        proj_result = _pushdown_projections(plan, caps)
        pred_result = _pushdown_predicates(plan, caps)
        limit_result = _pushdown_limit(plan, caps)
        cast_result = _pushdown_casts(plan, caps)

        pushdown = PushdownPlan(
            predicates=pred_result,
            projections=proj_result,
            limit=limit_result,
            casts=cast_result,
        )

        residual = ResidualPlan(
            predicates=pred_result.residual,
            limit=limit_result.residual_limit,
            casts=cast_result.residual,
        )

        result.append(replace(group, pushdown=pushdown, residual=residual))

    return result

# ---------------------------------------------------------------------------
# Cross-group predicate pushdown helpers
# ---------------------------------------------------------------------------

_NON_PUSHABLE_TRANSFORMS = frozenset({
    "aggregation", "window", "expression", "multi_column", "opaque",
})


def _bare_column(col: str) -> str:
    """Strip a table alias/qualifier from a column name (e.g. ``t1.col`` → ``col``)."""
    return col.split(".")[-1] if "." in col else col


def _find_lineage(exit_cj: CompiledJoint, col: str) -> ColumnLineage | None:
    """Find the ColumnLineage record for *col* on *exit_cj*, matching bare names.

    Both the predicate column and the lineage output_column are stripped of
    table qualifiers before comparison so that ``ris.correlation_id`` matches
    a lineage entry with output_column ``correlation_id`` or vice-versa.

    When multiple lineage entries share the same bare name (e.g. both
    ``ris.correlation_id`` and ``rie.correlation_id``), an exact match on the
    full qualified name is preferred.
    """
    bare = _bare_column(col)
    # First pass: exact match (handles qualified names)
    for lin in exit_cj.column_lineage:
        if lin.output_column == col:
            return lin
    # Second pass: bare-name match
    for lin in exit_cj.column_lineage:
        if _bare_column(lin.output_column) == bare:
            return lin
    return None


def _is_cross_group_pushable(conj: Predicate, exit_cj: CompiledJoint) -> bool:
    """Check whether a conjunct can be pushed across group boundaries.

    Returns False for HAVING predicates, subquery predicates, and columns
    whose lineage transform is not direct or renamed.  Table-qualified columns
    resolvable via the logical plan's source_tables alias map are treated as
    pushable (direct pass-through) without consulting lineage — this avoids
    ambiguous bare-name matches when multiple upstream tables share a column.
    """
    if conj.location == "having":
        return False
    if _has_subquery(conj):
        return False

    alias_map = _build_alias_map(exit_cj)
    for col in conj.columns:
        # Table-qualified columns resolvable via alias map are always pushable
        if "." in col and col.split(".")[0] in alias_map:
            continue
        lin = _find_lineage(exit_cj, col)
        if lin is None:
            return False
        if lin.transform in _NON_PUSHABLE_TRANSFORMS:
            return False
    return True


def _build_alias_map(cj: CompiledJoint) -> dict[str, str]:
    """Build a table-alias → joint-name map from the logical plan's source_tables."""
    if cj.logical_plan is None or not cj.logical_plan.source_tables:
        return {}
    return {
        ref.alias: ref.name
        for ref in cj.logical_plan.source_tables
        if ref.alias
    }


def _resolve_conjunct_origins(
    conj: Predicate,
    exit_cj: CompiledJoint,
    compiled_joints: dict[str, CompiledJoint],
) -> list[ColumnOrigin] | None:
    """Trace each column in a conjunct backward through lineage to ultimate origins.

    Returns a list of ColumnOrigin objects, or None if any column has no lineage.

    When a predicate column is table-qualified (e.g. ``rie.correlation_id``)
    the function first attempts to resolve via the logical plan's alias map
    (table alias → joint name).  This avoids ambiguity when multiple upstream
    tables share the same bare column name (e.g. both ``ris.correlation_id``
    and ``rie.correlation_id`` exist but lineage only records one
    ``correlation_id`` entry).

    Falls back to lineage-based resolution when the column is not
    table-qualified or has no alias-map entry.
    """
    alias_map = _build_alias_map(exit_cj)
    all_origins: list[ColumnOrigin] = []
    for col in conj.columns:
        # Prefer alias-map resolution for table-qualified columns to avoid
        # ambiguous bare-name lineage matches.
        if "." in col:
            table_alias = col.split(".")[0]
            bare = _bare_column(col)
            joint_name = alias_map.get(table_alias)
            if joint_name:
                all_origins.append(ColumnOrigin(joint=joint_name, column=bare))
                continue

        lin = _find_lineage(exit_cj, col)
        if lin is None:
            return None

        # Walk backward through the lineage chain
        visited: set[tuple[str, str]] = set()
        stack: list[tuple[str, str]] = [(o.joint, o.column) for o in lin.origins]
        if not stack:
            # No origins (e.g. literal) — treat as terminal at exit joint
            all_origins.append(ColumnOrigin(joint=exit_cj.name, column=col))
            continue

        col_origins: list[ColumnOrigin] = []
        while stack:
            jname, cname = stack.pop()
            if (jname, cname) in visited:
                continue
            visited.add((jname, cname))

            upstream_cj = compiled_joints.get(jname)
            if upstream_cj is None:
                col_origins.append(ColumnOrigin(joint=jname, column=cname))
                continue

            upstream_lin = _find_lineage(upstream_cj, cname)
            if upstream_lin is None or not upstream_lin.origins:
                col_origins.append(ColumnOrigin(joint=jname, column=cname))
            else:
                for origin in upstream_lin.origins:
                    stack.append((origin.joint, origin.column))

        all_origins.extend(col_origins)

    return all_origins


# Regex matching a table-qualified column reference like ``t1.col_name``
_TABLE_QUAL_RE = re.compile(r"\b(\w+)\.(\w+)\b")


def _rewrite_predicate_for_source(
    conj: Predicate,
    exit_cj: CompiledJoint,
    origins: list[ColumnOrigin],
) -> Predicate:
    """Rewrite a predicate expression to use source-schema column names.

    Strips table aliases/qualifiers and applies column renames from lineage.
    """
    # Build rename map: bare consumer column -> source column
    rename_map: dict[str, str] = {}
    for col in conj.columns:
        lin = _find_lineage(exit_cj, col)
        if lin is not None and lin.origins:
            source_col = lin.origins[0].column
            rename_map[_bare_column(col)] = source_col

    expr = conj.expression

    # Step 1: Strip table aliases — replace ``alias.col`` with ``col``
    expr = _TABLE_QUAL_RE.sub(r"\2", expr)

    # Step 2: Apply column renames where source name differs
    for consumer_col, source_col in rename_map.items():
        if consumer_col != source_col:
            pattern = re.compile(r"\b" + re.escape(consumer_col) + r"\b")
            expr = pattern.sub(source_col, expr)

    # Build updated columns list using source names
    new_columns = [rename_map.get(_bare_column(c), _bare_column(c)) for c in conj.columns]

    return Predicate(expression=expr, columns=new_columns, location="where")


# Regex for simple column-equality in JOIN conditions: ``a.col = b.col`` or ``col1 = col2``
_SIMPLE_EQ_RE = re.compile(
    r"(\w+(?:\.\w+)?)\s*=\s*(\w+(?:\.\w+)?)"
)


def _has_predicate_capability(
    target_group: FusedGroup,
    target_joint: str,
    capabilities: dict[str, list[str]],
    catalog_types: dict[str, str | None],
) -> bool:
    """Check if a target source group's adapter supports predicate pushdown."""
    ct = catalog_types.get(target_joint)
    key = f"{target_group.engine_type}:{ct}" if ct else target_group.engine_type
    caps = capabilities.get(key, [])
    if not caps:
        caps = capabilities.get(target_group.engine_type, [])
    return "predicate_pushdown" in caps

def _has_projection_capability(
    target_group: FusedGroup,
    target_joint: str,
    capabilities: dict[str, list[str]],
    catalog_types: dict[str, str | None],
) -> bool:
    """Check if a target source group's adapter supports projection pushdown."""
    ct = catalog_types.get(target_joint)
    key = f"{target_group.engine_type}:{ct}" if ct else target_group.engine_type
    caps = capabilities.get(key, [])
    if not caps:
        caps = capabilities.get(target_group.engine_type, [])
    return "projection_pushdown" in caps

def _has_limit_capability(
    target_group: FusedGroup,
    target_joint: str,
    capabilities: dict[str, list[str]],
    catalog_types: dict[str, str | None],
) -> bool:
    """Check if a target source group's adapter supports limit pushdown."""
    ct = catalog_types.get(target_joint)
    key = f"{target_group.engine_type}:{ct}" if ct else target_group.engine_type
    caps = capabilities.get(key, [])
    if not caps:
        caps = capabilities.get(target_group.engine_type, [])
    return "limit_pushdown" in caps


def _get_upstream_source_joints(
    exit_cj: CompiledJoint,
    compiled_joints: dict[str, CompiledJoint],
    group_for_joint: dict[str, FusedGroup],
) -> list[str]:
    """Return upstream source joint names in other groups.

    Walks the exit joint's ``upstream`` references. Joints that belong to a
    different :class:`FusedGroup` than *exit_cj* are considered upstream source
    joints and are collected. Joints in the same group are traversed
    recursively so that we reach the true cross-group boundary.
    """
    exit_group = group_for_joint.get(exit_cj.name)
    result: list[str] = []
    visited: set[str] = set()

    def _walk(joint_name: str) -> None:
        if joint_name in visited:
            return
        visited.add(joint_name)
        jg = group_for_joint.get(joint_name)
        if jg is None or (exit_group is not None and jg.id != exit_group.id):
            result.append(joint_name)
            return
        cj = compiled_joints.get(joint_name)
        if cj is None:
            return
        for up in cj.upstream:
            _walk(up)

    for up in exit_cj.upstream:
        _walk(up)

    return result


def _derive_join_equality_predicates(
    conj: Predicate,
    exit_cj: CompiledJoint,
    compiled_joints: dict[str, CompiledJoint],
    group_for_joint: dict[str, FusedGroup],
    capabilities: dict[str, list[str]],
    catalog_types: dict[str, str | None],
) -> list[tuple[Predicate, str, FusedGroup]]:
    """Infer equivalent predicates through INNER JOIN equality conditions.

    For each predicate column that participates in an INNER JOIN equality
    ``A = B``, derives a new predicate by substituting B for A. Only simple
    column-reference equalities on INNER JOINs are eligible — LEFT/RIGHT/FULL/
    CROSS joins and expression-based conditions are skipped.

    Returns a list of (rewritten_predicate, target_joint_name, target_group).
    """
    if exit_cj.logical_plan is None or not exit_cj.logical_plan.joins:
        return []

    # Collect simple column equalities from INNER JOINs
    equalities: list[tuple[str, str]] = []
    for join in exit_cj.logical_plan.joins:
        if join.type != "inner" or join.condition is None:
            continue
        # Split on AND to handle compound conditions
        parts = re.split(r"\bAND\b", join.condition, flags=re.IGNORECASE)
        for part in parts:
            m = _SIMPLE_EQ_RE.fullmatch(part.strip())
            if m:
                equalities.append((m.group(1), m.group(2)))

    if not equalities:
        return []

    results: list[tuple[Predicate, str, FusedGroup]] = []

    for col in conj.columns:
        col_bare = _bare_column(col)
        for left, right in equalities:
            # Check if the predicate column matches either side (with or without alias)
            left_bare = left.split(".")[-1] if "." in left else left
            right_bare = right.split(".")[-1] if "." in right else right

            other_side: str | None = None
            # Prefer exact (table-qualified) matches first to avoid
            # ambiguity when both sides share the same bare column name.
            if col == left:
                other_side = right
            elif col == right:
                other_side = left
            elif col_bare == left_bare and col_bare == right_bare:
                # Both sides have the same bare column name (typical for
                # join keys).  Disambiguate using the table qualifier on
                # the predicate column: pick the side whose table alias
                # differs from the predicate's table alias.
                col_table = col.split(".")[0] if "." in col else None
                left_table = left.split(".")[0] if "." in left else None
                right_table = right.split(".")[0] if "." in right else None
                if col_table and col_table == left_table:
                    other_side = right
                elif col_table and col_table == right_table:
                    other_side = left
                else:
                    # No table qualifier on predicate — emit both sides
                    other_side = right
            elif col_bare == left_bare:
                other_side = right
            elif col_bare == right_bare:
                other_side = left
            else:
                continue

            other_bare = other_side.split(".")[-1] if "." in other_side else other_side

            # Derive a new predicate by substituting the other-side column
            derived_expr = conj.expression
            # Strip table qualifiers first
            derived_expr = _TABLE_QUAL_RE.sub(r"\2", derived_expr)
            # Replace the predicate column with the other-side column
            pattern = re.compile(r"\b" + re.escape(col_bare) + r"\b")
            derived_expr = pattern.sub(other_bare, derived_expr)

            # Keep the full qualified name so _resolve_conjunct_origins can
            # use the alias map to resolve to the correct source joint.
            derived_pred = Predicate(
                expression=derived_expr, columns=[other_side], location="where",
            )

            # Trace the other-side column through lineage to find its source
            derived_origins = _resolve_conjunct_origins(
                derived_pred, exit_cj, compiled_joints,
            )
            if derived_origins is None:
                continue

            # Must be single-origin
            target_joints = {o.joint for o in derived_origins}
            if len(target_joints) != 1:
                continue

            target_joint_name = derived_origins[0].joint
            target_group = group_for_joint.get(target_joint_name)
            if target_group is None:
                continue

            # Check capability
            if not _has_predicate_capability(
                target_group, target_joint_name, capabilities, catalog_types,
            ):
                continue

            # Rewrite for source schema
            rewritten = _rewrite_predicate_for_source(
                derived_pred, exit_cj, derived_origins,
            )
            results.append((rewritten, target_joint_name, target_group))

    return results

# ---------------------------------------------------------------------------
# Cross-group predicate pushdown pass
# ---------------------------------------------------------------------------


def cross_group_pushdown_pass(
    groups: list[FusedGroup],
    compiled_joints: dict[str, CompiledJoint],
    capabilities: dict[str, list[str]],
    catalog_types: dict[str, str | None],
) -> tuple[list[FusedGroup], list[OptimizationResult]]:
    """Propagate predicates, projections, and limits across fused-group boundaries.

    For each consumer group whose exit joint has a LogicalPlan, the pass:
    1. **Predicates** — Splits, classifies, traces, rewrites, and pushes WHERE
       predicates to upstream source groups (unchanged from original).
    2. **Projections** — Collects all columns referenced in the consumer's
       LogicalPlan, maps them through column lineage to source joints, and
       stores the mapped column lists in ``per_joint_projections`` on the
       upstream source groups.
    3. **Limits** — Extracts LIMIT clauses with safety guards
       and stores them in ``per_joint_limits``.

    Consumer-side predicates, projections, and limits are always retained for
    correctness — the pass can only make things faster, never incorrect.

    Args:
        groups: Fused groups (already processed by ``pushdown_pass``).
        compiled_joints: Joint name → ``CompiledJoint`` map for lineage access.
        capabilities: Capability map (engine_type or engine_type:catalog_type → list).
        catalog_types: Joint name → catalog type map.

    Returns:
        A tuple of (updated groups via ``replace()``, list of ``OptimizationResult``).
    """
    from rivet_core.compiler import OptimizationResult

    _RULE = "cross_group_predicate_pushdown"
    _PROJ_RULE = "cross_group_projection_pushdown"
    _LIM_RULE = "cross_group_limit_pushdown"

    # Build index: joint name → its FusedGroup
    group_for_joint: dict[str, FusedGroup] = {}
    for g in groups:
        for jn in g.joints:
            group_for_joint[jn] = g

    # Accumulate per-joint predicates keyed by group id, then joint name
    # group_id → {joint_name → [Predicate, ...]}
    pjp_updates: dict[str, dict[str, list[Predicate]]] = {}
    # Accumulate per-joint projections keyed by group id, then joint name
    # group_id → {joint_name → {col, ...}}
    pjproj_updates: dict[str, dict[str, set[str]]] = {}
    # Accumulate per-joint limits keyed by group id, then joint name
    # group_id → {joint_name → limit}
    pjlim_updates: dict[str, dict[str, int]] = {}
    results: list[OptimizationResult] = []

    for group in groups:
        if not group.exit_joints:
            continue
        exit_joint = group.exit_joints[0]
        cj = compiled_joints.get(exit_joint)
        if cj is None:
            continue

        # ── Predicate pushdown (only when predicates exist) ──────────
        pushed_predicate_cols: set[str] = set()
        if cj.logical_plan is not None and cj.logical_plan.predicates:
            for pred in cj.logical_plan.predicates:
                for conj in _split_and_conjuncts(pred):
                    # --- Classify ---
                    if not _is_cross_group_pushable(conj, cj):
                        results.append(OptimizationResult(
                            rule=_RULE,
                            status="skipped",
                            detail=(
                                f"Predicate '{conj.expression}' on exit joint "
                                f"'{exit_joint}' is non-pushable (HAVING, subquery, "
                                f"or non-direct lineage transform)"
                            ),
                        ))
                        continue

                    # --- Resolve origins ---
                    origins = _resolve_conjunct_origins(conj, cj, compiled_joints)
                    if origins is None:
                        results.append(OptimizationResult(
                            rule=_RULE,
                            status="skipped",
                            detail=(
                                f"Predicate '{conj.expression}' on exit joint "
                                f"'{exit_joint}' has no column lineage"
                            ),
                        ))
                        continue

                    target_joints = {o.joint for o in origins}
                    if len(target_joints) != 1:
                        results.append(OptimizationResult(
                            rule=_RULE,
                            status="skipped",
                            detail=(
                                f"Predicate '{conj.expression}' on exit joint "
                                f"'{exit_joint}' traces to multiple source joints: "
                                f"{sorted(target_joints)}"
                            ),
                        ))
                        continue

                    target_joint = origins[0].joint
                    target_group = group_for_joint.get(target_joint)
                    if target_group is None:
                        results.append(OptimizationResult(
                            rule=_RULE,
                            status="skipped",
                            detail=(
                                f"Predicate '{conj.expression}' target joint "
                                f"'{target_joint}' not found in any group"
                            ),
                        ))
                        continue

                    # --- Capability check ---
                    if not _has_predicate_capability(
                        target_group, target_joint, capabilities, catalog_types,
                    ):
                        results.append(OptimizationResult(
                            rule=_RULE,
                            status="not_applicable",
                            detail=(
                                f"Predicate '{conj.expression}' targets source joint "
                                f"'{target_joint}' in group '{target_group.id}' whose "
                                f"adapter lacks predicate_pushdown capability"
                            ),
                        ))
                        continue

                    # --- Rewrite and push ---
                    rewritten = _rewrite_predicate_for_source(conj, cj, origins)

                    gid = target_group.id
                    pjp_updates.setdefault(gid, {})
                    pjp_updates[gid].setdefault(target_joint, [])
                    pjp_updates[gid][target_joint].append(rewritten)

                    # Track columns from successfully pushed predicates (Req 10.2)
                    pushed_predicate_cols.update(conj.columns)

                    results.append(OptimizationResult(
                        rule=_RULE,
                        status="applied",
                        detail=(
                            f"Pushed predicate '{rewritten.expression}' to source "
                            f"joint '{target_joint}' in group '{target_group.id}'"
                        ),
                        pushed=rewritten.expression,
                    ))

                    # --- Join-equality propagation ---
                    derived = _derive_join_equality_predicates(
                        conj, cj, compiled_joints, group_for_joint,
                        capabilities, catalog_types,
                    )
                    for derived_pred, derived_joint, derived_group in derived:
                        dgid = derived_group.id
                        pjp_updates.setdefault(dgid, {})
                        pjp_updates[dgid].setdefault(derived_joint, [])
                        pjp_updates[dgid][derived_joint].append(derived_pred)

                        results.append(OptimizationResult(
                            rule=_RULE,
                            status="applied",
                            detail=(
                                f"Pushed join-equality inferred predicate "
                                f"'{derived_pred.expression}' to source joint "
                                f"'{derived_joint}' in group '{derived_group.id}' "
                                f"(derived from '{conj.expression}' via join equality)"
                            ),
                            pushed=derived_pred.expression,
                        ))

        # ── Projection pushdown ──────────────────────────────────────
        if cj.logical_plan is None:
            # No LogicalPlan → skip projection and limit pushdown (Req 1.3, 4.5)
            continue

        lp = cj.logical_plan

        # Skip if SELECT *  (Req 1.2)
        if any(p.expression.strip() == "*" for p in lp.projections):
            results.append(OptimizationResult(
                rule=_PROJ_RULE,
                status="skipped",
                detail=(
                    f"Consumer group '{group.id}' exit joint '{exit_joint}' "
                    f"uses SELECT * — skipping projection pushdown"
                ),
            ))
        else:
            # Collect all referenced columns from the consumer's LogicalPlan (Req 1.1)
            consumer_cols: set[str] = set()
            for proj in lp.projections:
                consumer_cols.update(proj.source_columns)
            for pred in lp.predicates:
                consumer_cols.update(pred.columns)
            for join in lp.joins:
                consumer_cols.update(join.columns)
            if lp.aggregations:
                consumer_cols.update(lp.aggregations.group_by)
            if lp.ordering:
                consumer_cols.update(col for col, _ in lp.ordering.columns)

            # Include columns from cross-group predicates pushed for this consumer (Req 10.2)
            consumer_cols.update(pushed_predicate_cols)

            # Map each consumer column to source joint via lineage (Req 2.1–2.4)
            source_projections: dict[str, set[str]] = {}
            skip_projection = False
            for col in consumer_cols:
                lineage = _find_lineage(cj, col)
                if lineage is None:
                    # No lineage → fall back to reading all columns (Req 2.3)
                    skip_projection = True
                    break
                for origin in lineage.origins:
                    source_projections.setdefault(origin.joint, set()).add(origin.column)

            if not skip_projection:
                # Check capability and store projections (Req 3.1, 3.2, 3.4)
                for source_joint, cols in source_projections.items():
                    target_group = group_for_joint.get(source_joint)
                    if target_group is None:
                        continue
                    if not _has_projection_capability(
                        target_group, source_joint, capabilities, catalog_types,
                    ):
                        results.append(OptimizationResult(
                            rule=_PROJ_RULE,
                            status="not_applicable",
                            detail=(
                                f"Source joint '{source_joint}' in group "
                                f"'{target_group.id}' lacks projection_pushdown capability"
                            ),
                        ))
                        continue
                    gid = target_group.id
                    pjproj_updates.setdefault(gid, {})
                    pjproj_updates[gid].setdefault(source_joint, set())
                    pjproj_updates[gid][source_joint] |= cols
                    results.append(OptimizationResult(
                        rule=_PROJ_RULE,
                        status="applied",
                        detail=(
                            f"Pushed projection {sorted(cols)} to source joint "
                            f"'{source_joint}' in group '{target_group.id}'"
                        ),
                    ))

                # Ensure columns from pushed predicates (including join-equality
                # derived predicates) are included in each source joint's projection.
                # Without this, a derived predicate like ``correlation_id = 'x'``
                # pushed to raw_ingestion_events would not cause correlation_id to
                # appear in that source's projection, leading to a runtime error.
                for src_gid, joint_preds in pjp_updates.items():
                    for src_joint, preds in joint_preds.items():
                        src_group = group_for_joint.get(src_joint)
                        if src_group is None:
                            continue
                        if not _has_projection_capability(
                            src_group, src_joint, capabilities, catalog_types,
                        ):
                            continue
                        pred_cols: set[str] = set()
                        for p in preds:
                            for c in p.columns:
                                pred_cols.add(_bare_column(c))
                        if pred_cols:
                            pjproj_updates.setdefault(src_gid, {})
                            pjproj_updates[src_gid].setdefault(src_joint, set())
                            pjproj_updates[src_gid][src_joint] |= pred_cols

        # ── Limit pushdown ──────────────────────────────────────────
        if lp.limit is not None and lp.limit.count is not None:
            limit_blocked = False

            if lp.aggregations:
                results.append(OptimizationResult(
                    rule=_LIM_RULE,
                    status="skipped",
                    detail=(
                        f"Consumer group '{group.id}' exit joint '{exit_joint}' "
                        f"has aggregations — skipping limit pushdown"
                    ),
                ))
                limit_blocked = True

            if not limit_blocked and lp.joins:
                results.append(OptimizationResult(
                    rule=_LIM_RULE,
                    status="skipped",
                    detail=(
                        f"Consumer group '{group.id}' exit joint '{exit_joint}' "
                        f"has joins — skipping limit pushdown"
                    ),
                ))
                limit_blocked = True

            if not limit_blocked and lp.distinct:
                results.append(OptimizationResult(
                    rule=_LIM_RULE,
                    status="skipped",
                    detail=(
                        f"Consumer group '{group.id}' exit joint '{exit_joint}' "
                        f"has DISTINCT — skipping limit pushdown"
                    ),
                ))
                limit_blocked = True

            if not limit_blocked:
                upstream_source_joints = _get_upstream_source_joints(
                    cj, compiled_joints, group_for_joint,
                )
                upstream_source_groups = {
                    group_for_joint[j].id
                    for j in upstream_source_joints
                    if j in group_for_joint
                }

                if len(upstream_source_groups) != 1:
                    results.append(OptimizationResult(
                        rule=_LIM_RULE,
                        status="skipped",
                        detail=(
                            f"Consumer group '{group.id}' exit joint '{exit_joint}' "
                            f"references multiple upstream source groups — "
                            f"skipping limit pushdown"
                        ),
                    ))
                    limit_blocked = True

            if not limit_blocked and group.residual and group.residual.predicates:
                results.append(OptimizationResult(
                    rule=_LIM_RULE,
                    status="skipped",
                    detail=(
                        f"Consumer group '{group.id}' has residual predicates — "
                        f"skipping limit pushdown"
                    ),
                ))
                limit_blocked = True

            if not limit_blocked:
                limit_val = lp.limit.count
                for source_joint in upstream_source_joints:
                    target_group = group_for_joint.get(source_joint)
                    if target_group is None:
                        continue
                    if not _has_limit_capability(
                        target_group, source_joint, capabilities, catalog_types,
                    ):
                        results.append(OptimizationResult(
                            rule=_LIM_RULE,
                            status="not_applicable",
                            detail=(
                                f"Source joint '{source_joint}' in group "
                                f"'{target_group.id}' lacks limit_pushdown capability"
                            ),
                        ))
                        continue
                    gid = target_group.id
                    pjlim_updates.setdefault(gid, {})
                    pjlim_updates[gid][source_joint] = max(
                        pjlim_updates[gid].get(source_joint, 0),
                        limit_val,
                    )
                    results.append(OptimizationResult(
                        rule=_LIM_RULE,
                        status="applied",
                        detail=(
                            f"Pushed limit {limit_val} to source joint "
                            f"'{source_joint}' in group '{target_group.id}'"
                        ),
                    ))

    # --- Build updated groups via replace() (immutable pattern) ---
    new_groups: list[FusedGroup] = []
    for group in groups:
        pred_updates = pjp_updates.get(group.id)
        proj_updates = pjproj_updates.get(group.id)
        lim_updates = pjlim_updates.get(group.id)
        kwargs: dict[str, Any] = {}

        if pred_updates is not None:
            # Merge with any existing per_joint_predicates, deduplicating
            merged = dict(group.per_joint_predicates)
            for jn, preds in pred_updates.items():
                existing = merged.get(jn, [])
                seen_exprs = {p.expression for p in existing}
                deduped = list(existing)
                for p in preds:
                    if p.expression not in seen_exprs:
                        seen_exprs.add(p.expression)
                        deduped.append(p)
                merged[jn] = deduped
            kwargs["per_joint_predicates"] = merged

        if proj_updates is not None:
            # Merge with any existing per_joint_projections (union, sorted)
            merged_proj = dict(group.per_joint_projections)
            for jn, cols in proj_updates.items():
                existing_cols = set(merged_proj.get(jn, []))
                existing_cols |= cols
                merged_proj[jn] = sorted(existing_cols)
            kwargs["per_joint_projections"] = merged_proj

        if lim_updates is not None:
            # Merge with any existing per_joint_limits (max)
            merged_lims = dict(group.per_joint_limits)
            for jn, lim in lim_updates.items():
                existing_lim = merged_lims.get(jn, 0)
                merged_lims[jn] = max(existing_lim, lim) if existing_lim else lim
            kwargs["per_joint_limits"] = merged_lims

        if kwargs:
            new_groups.append(replace(group, **kwargs))
        else:
            new_groups.append(group)

    return new_groups, results
