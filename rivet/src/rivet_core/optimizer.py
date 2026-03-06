"""Optimizer: fusion and pushdown passes.

The optimizer runs two passes during compilation: fusion then pushdown.
Both passes are pure functions that produce metadata — no data operations.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING

from rivet_core.sql_parser import LogicalPlan, Predicate

if TYPE_CHECKING:
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
) -> bool:
    """Return True when *joint* can merge into the upstream joint's group.

    All five conditions from Req 18.2 must hold:
    1. Same engine instance
    2. No eager flag on upstream
    3. No assertions on upstream
    4. Upstream has exactly one downstream consumer (single-consumer rule)
    5. Downstream joint is not a PythonJoint
    """
    if joint.engine != upstream_joint.engine:
        return False
    if upstream_joint.eager:
        return False
    if upstream_joint.has_assertions:
        return False
    if downstream_counts.get(upstream_name, 0) != 1:
        return False
    if joint.joint_type == "python":
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
                best_gid: str | None = None
                best_size = -1
                for up_name in joint.upstream:
                    up_joint = joints_by_name[up_name]
                    if _can_fuse(joint, up_name, up_joint, ds_counts):
                        gid = group_id_for[up_name]
                        size = len(group_joints[gid])
                        if size > best_size:
                            best_size = size
                            best_gid = gid
                candidate_gid = best_gid

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
