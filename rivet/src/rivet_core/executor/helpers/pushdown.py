"""Pushdown plan merging functions.

Functions that merge pushdown plans (predicates, projections, limits,
residuals, cross-group optimizations) for adapter-based source reading.
"""

from __future__ import annotations

from rivet_core.compiler import CompiledJoint
from rivet_core.optimizer import (
    CastPushdownResult,
    FusedGroup,
    LimitPushdownResult,
    PredicatePushdownResult,
    ProjectionPushdownResult,
    PushdownPlan,
    ResidualPlan,
)


def _merge_residuals(
    group_residual: ResidualPlan | None, adapter_residual: ResidualPlan
) -> ResidualPlan:
    """Merge compiler-computed residuals with adapter-returned residuals."""
    if group_residual is None:
        return adapter_residual
    return ResidualPlan(
        predicates=group_residual.predicates + adapter_residual.predicates,
        limit=adapter_residual.limit
        if adapter_residual.limit is not None
        else group_residual.limit,
        casts=group_residual.casts + adapter_residual.casts,
    )


def _merge_source_limit_into_pushdown(
    pushdown: PushdownPlan | None,
    source_joint: CompiledJoint,
) -> PushdownPlan | None:
    """If the source joint's SQL has a LIMIT, ensure it's in the pushdown for the adapter.

    When a source joint like ``SELECT * FROM table LIMIT 100`` is read via an adapter,
    the adapter ignores the SQL body and reads directly from storage. The LIMIT must be
    forwarded through the pushdown so the adapter can apply it at the storage level.
    """
    lp = source_joint.logical_plan
    if lp is None or lp.limit is None or lp.limit.count is None:
        return pushdown

    source_limit = lp.limit.count

    if pushdown is not None and pushdown.limit.pushed_limit is not None:
        # Already has a pushed limit — use the smaller of the two
        effective = min(pushdown.limit.pushed_limit, source_limit)
        if effective == pushdown.limit.pushed_limit:
            return pushdown
        return PushdownPlan(
            predicates=pushdown.predicates,
            projections=pushdown.projections,
            limit=LimitPushdownResult(pushed_limit=effective, residual_limit=None, reason=None),
            casts=pushdown.casts,
        )

    # No existing pushdown or no limit in it — create/augment with source limit
    if pushdown is None:
        return PushdownPlan(
            predicates=PredicatePushdownResult(pushed=[], residual=[]),
            projections=ProjectionPushdownResult(pushed_columns=None, reason=None),
            limit=LimitPushdownResult(pushed_limit=source_limit, residual_limit=None, reason=None),
            casts=CastPushdownResult(pushed=[], residual=[]),
        )

    return PushdownPlan(
        predicates=pushdown.predicates,
        projections=pushdown.projections,
        limit=LimitPushdownResult(pushed_limit=source_limit, residual_limit=None, reason=None),
        casts=pushdown.casts,
    )


def _merge_source_predicates_into_pushdown(
    pushdown: PushdownPlan | None,
    source_joint: CompiledJoint,
) -> PushdownPlan | None:
    """Merge WHERE predicates from the source joint's LogicalPlan into *pushdown*.

    If the source joint has a ``LogicalPlan`` with WHERE predicates, they are
    appended to the pushed predicate list.  If *pushdown* is ``None`` and there
    are source predicates, a new ``PushdownPlan`` is created with those
    predicates as the only pushed entries.  When no source predicates exist,
    *pushdown* is returned unchanged.
    """
    lp = source_joint.logical_plan
    if lp is None or not lp.predicates:
        return pushdown

    source_preds = list(lp.predicates)

    if pushdown is None:
        return PushdownPlan(
            predicates=PredicatePushdownResult(pushed=source_preds, residual=[]),
            projections=ProjectionPushdownResult(pushed_columns=None, reason=None),
            limit=LimitPushdownResult(pushed_limit=None, residual_limit=None, reason=None),
            casts=CastPushdownResult(pushed=[], residual=[]),
        )

    merged_pushed = list(pushdown.predicates.pushed) + source_preds
    return PushdownPlan(
        predicates=PredicatePushdownResult(
            pushed=merged_pushed, residual=pushdown.predicates.residual
        ),
        projections=pushdown.projections,
        limit=pushdown.limit,
        casts=pushdown.casts,
    )


def _merge_source_projections_into_pushdown(
    pushdown: PushdownPlan | None,
    source_joint: CompiledJoint,
) -> PushdownPlan | None:
    """Merge projection column references from the source joint's LogicalPlan into *pushdown*.

    For simple column references (no expression beyond the column itself), the
    column name is added to ``pushed_columns``.  For aliased expressions, the
    ``source_columns`` (base columns referenced by the expression) are added
    instead — the expression itself is applied post-read by
    ``_apply_source_expressions``.

    When the source has ``SELECT *`` (no explicit projections), no projection
    pushdown is applied and *pushdown* is returned unchanged.
    """
    lp = source_joint.logical_plan
    if lp is None or not lp.projections:
        return pushdown

    # Detect SELECT * — a single projection with expression "*" and no alias
    if (
        len(lp.projections) == 1
        and lp.projections[0].expression == "*"
        and lp.projections[0].alias is None
    ):
        return pushdown

    # Collect all base columns needed from storage
    source_cols: list[str] = []
    seen: set[str] = set()
    for proj in lp.projections:
        if proj.source_columns:
            for col in proj.source_columns:
                if col not in seen:
                    source_cols.append(col)
                    seen.add(col)
        elif proj.alias is None:
            # Simple column reference with no alias — expression is the column name
            col = proj.expression
            if col not in seen:
                source_cols.append(col)
                seen.add(col)

    if not source_cols:
        return pushdown

    if pushdown is None:
        return PushdownPlan(
            predicates=PredicatePushdownResult(pushed=[], residual=[]),
            projections=ProjectionPushdownResult(pushed_columns=source_cols, reason=None),
            limit=LimitPushdownResult(pushed_limit=None, residual_limit=None, reason=None),
            casts=CastPushdownResult(pushed=[], residual=[]),
        )

    existing = pushdown.projections.pushed_columns
    if existing is not None:
        merged = sorted(set(existing) & set(source_cols))
    else:
        merged = source_cols

    return PushdownPlan(
        predicates=pushdown.predicates,
        projections=ProjectionPushdownResult(
            pushed_columns=merged, reason=pushdown.projections.reason
        ),
        limit=pushdown.limit,
        casts=pushdown.casts,
    )


def _merge_cross_group_predicates(
    pushdown: PushdownPlan | None,
    group: FusedGroup,
    joint_name: str,
) -> PushdownPlan | None:
    """Merge cross-group predicates from ``group.per_joint_predicates`` into *pushdown*.

    If the group has cross-group predicates targeting *joint_name*, they are
    appended to the pushed predicate list.  If *pushdown* is ``None`` and there
    are cross-group predicates, a new ``PushdownPlan`` is created with those
    predicates as the only pushed entries.  When no cross-group predicates
    exist for the joint, *pushdown* is returned unchanged.
    """
    xg_preds = group.per_joint_predicates.get(joint_name)
    if not xg_preds:
        return pushdown

    if pushdown is None:
        return PushdownPlan(
            predicates=PredicatePushdownResult(pushed=list(xg_preds), residual=[]),
            projections=ProjectionPushdownResult(pushed_columns=None, reason=None),
            limit=LimitPushdownResult(pushed_limit=None, residual_limit=None, reason=None),
            casts=CastPushdownResult(pushed=[], residual=[]),
        )

    merged_pushed = list(pushdown.predicates.pushed) + list(xg_preds)
    return PushdownPlan(
        predicates=PredicatePushdownResult(
            pushed=merged_pushed, residual=pushdown.predicates.residual
        ),
        projections=pushdown.projections,
        limit=pushdown.limit,
        casts=pushdown.casts,
    )


def _merge_cross_group_projections(
    pushdown: PushdownPlan | None,
    group: FusedGroup,
    joint_name: str,
) -> PushdownPlan | None:
    """Merge cross-group projections from ``group.per_joint_projections`` into *pushdown*.

    If the group has cross-group projections targeting *joint_name*, they are
    merged with any existing intra-group projections.  When both exist, the
    intersection is used (only columns needed by both).  When only cross-group
    projections exist, they are used directly.  If *pushdown* is ``None`` and
    there are cross-group projections, a new ``PushdownPlan`` is created.
    When no cross-group projections exist for the joint, *pushdown* is returned
    unchanged.
    """
    xg_cols = group.per_joint_projections.get(joint_name)
    if not xg_cols:
        return pushdown

    if pushdown is None:
        return PushdownPlan(
            predicates=PredicatePushdownResult(pushed=[], residual=[]),
            projections=ProjectionPushdownResult(pushed_columns=list(xg_cols), reason=None),
            limit=LimitPushdownResult(pushed_limit=None, residual_limit=None, reason=None),
            casts=CastPushdownResult(pushed=[], residual=[]),
        )

    existing = pushdown.projections.pushed_columns
    if existing is not None:
        merged = sorted(set(existing) & set(xg_cols))
    else:
        merged = list(xg_cols)

    return PushdownPlan(
        predicates=pushdown.predicates,
        projections=ProjectionPushdownResult(
            pushed_columns=merged, reason=pushdown.projections.reason
        ),
        limit=pushdown.limit,
        casts=pushdown.casts,
    )


def _merge_cross_group_limits(
    pushdown: PushdownPlan | None,
    group: FusedGroup,
    joint_name: str,
) -> PushdownPlan | None:
    """Merge cross-group limits from ``group.per_joint_limits`` into *pushdown*.

    If the group has a cross-group limit targeting *joint_name*, it is merged
    with any existing pushed limit.  When both exist, the minimum is used (the
    tighter constraint wins).  When only a cross-group limit exists, it is used
    directly.  If *pushdown* is ``None`` and there is a cross-group limit, a
    new ``PushdownPlan`` is created.  When no cross-group limit exists for the
    joint, *pushdown* is returned unchanged.
    """
    xg_limit = group.per_joint_limits.get(joint_name)
    if xg_limit is None:
        return pushdown

    if pushdown is None:
        return PushdownPlan(
            predicates=PredicatePushdownResult(pushed=[], residual=[]),
            projections=ProjectionPushdownResult(pushed_columns=None, reason=None),
            limit=LimitPushdownResult(pushed_limit=xg_limit, residual_limit=None, reason=None),
            casts=CastPushdownResult(pushed=[], residual=[]),
        )

    existing = pushdown.limit.pushed_limit
    if existing is not None:
        effective = min(existing, xg_limit)
    else:
        effective = xg_limit

    return PushdownPlan(
        predicates=pushdown.predicates,
        projections=pushdown.projections,
        limit=LimitPushdownResult(
            pushed_limit=effective,
            residual_limit=pushdown.limit.residual_limit,
            reason=pushdown.limit.reason,
        ),
        casts=pushdown.casts,
    )
