"""Run text format renderer for execution progress and quality."""

from __future__ import annotations

from rivet_cli.rendering.colors import (
    BOLD,
    CYAN,
    DIM,
    GREEN,
    MAGENTA,
    RED,
    SYM_ASSERT,
    SYM_AUDIT,
    SYM_CHECK,
    SYM_ERROR,
    SYM_MATERIALIZE,
    SYM_NOT_APPLICABLE,
    YELLOW,
    colorize,
)
from rivet_core.compiler import CompiledAssembly, CompiledJoint
from rivet_core.executor import ExecutionResult, JointExecutionResult


def render_run_text(
    result: ExecutionResult,
    compiled: CompiledAssembly,
    verbosity: int,
    color: bool,
) -> str:
    """Render ExecutionResult as human-readable progress output."""
    lines: list[str] = []
    joint_map = {j.name: j for j in compiled.joints}
    group_map = {fg.id: fg for fg in compiled.fused_groups}

    for jr in result.joint_results:
        cj = joint_map.get(jr.name)
        _render_joint_result(lines, jr, cj, verbosity, color)

    lines.append("")
    _render_summary(lines, result, color)
    _render_quality_summary(lines, result, color)

    if verbosity >= 2:
        _render_fused_sql(lines, compiled, group_map, color)

    return "\n".join(lines)


def _render_joint_result(
    lines: list[str],
    jr: JointExecutionResult,
    cj: CompiledJoint | None,
    verbosity: int,
    color: bool,
) -> None:
    status = colorize(f"{SYM_CHECK} OK", GREEN, color) if jr.success else colorize(f"{SYM_ERROR} FAIL", RED, color)
    name = colorize(jr.name, BOLD, color)
    parts = [f"  {name} {status}"]
    if jr.rows_out is not None:
        parts.append(colorize(f"({jr.rows_out} rows)", DIM, color))
    if jr.timing:
        parts.append(colorize(f"{jr.timing.total_ms:.0f}ms", DIM, color))
    if jr.fused_group_id:
        parts.append(colorize(f"[fused:{jr.fused_group_id}]", DIM, color))
    if cj:
        parts.append(colorize(f"engine:{cj.engine}", DIM, color))
    lines.append(" ".join(parts))

    if jr.materialized:
        trigger = jr.materialization_trigger or ""
        lines.append(f"    {colorize(SYM_MATERIALIZE + ' materialized', YELLOW, color)} {trigger}")

    for cr in jr.check_results:
        if cr.phase == "assertion":
            sym = colorize(SYM_ASSERT, CYAN, color)
            label = colorize("PASS" if cr.passed else "FAIL", GREEN if cr.passed else RED, color)
            lines.append(f"    {sym} {cr.type} {label} {cr.message}")
            if not cr.passed:
                _render_assertion_failure(lines, jr.name, cr, cj, verbosity, color)
        elif cr.phase == "audit":
            sym = colorize(SYM_AUDIT, MAGENTA, color)
            label = colorize("PASS" if cr.passed else "FAIL", GREEN if cr.passed else RED, color)
            lines.append(f"    {sym} {cr.type} {label} {cr.message}")

    if not jr.success and jr.error:
        lines.append(f"    {colorize(f'[{jr.error.code}] {jr.error.message}', RED, color)}")

    # Verbosity 1: schemas, optimizations, lineage
    if verbosity >= 1 and cj:
        if cj.output_schema:
            cols = ", ".join(f"{c.name}: {c.type}" for c in cj.output_schema.columns)
            lines.append(f"    schema: [{cols}]")
        for opt in cj.optimizations:
            sym = colorize(SYM_CHECK, GREEN, color) if opt.status == "applied" else colorize(SYM_NOT_APPLICABLE, DIM, color)
            lines.append(f"    {sym} {opt.rule}: {opt.status} - {opt.detail}")
        for lin in cj.column_lineage:
            origins = ", ".join(f"{o.joint}.{o.column}" for o in lin.origins)
            lines.append(f"    lineage: {lin.output_column} <- {origins} ({lin.transform})")

    # Verbosity 2: logical plan, stack traces
    if verbosity >= 2 and cj and cj.logical_plan:
        lines.append(f"    logical plan: {cj.logical_plan}")

    if verbosity >= 2 and not jr.success and jr.error:
        ctx = jr.error.context
        if ctx:
            lines.append(f"    context: {ctx}")


def _render_assertion_failure(
    lines: list[str],
    joint_name: str,
    cr: object,
    cj: CompiledJoint | None,
    verbosity: int,
    color: bool,
) -> None:
    lines.append(f"      {colorize('[RVT-601]', RED, color)} joint={joint_name} type={cr.type} severity={cr.severity}")  # type: ignore[attr-defined]
    lines.append(f"      violation: {cr.message}")  # type: ignore[attr-defined]
    if verbosity >= 1 and cj:
        for lin in cj.column_lineage:
            origins = ", ".join(f"{o.joint}.{o.column}" for o in lin.origins)
            lines.append(f"      lineage: {lin.output_column} <- {origins}")


def _render_summary(lines: list[str], result: ExecutionResult, color: bool) -> None:
    time_str = colorize(f"{result.total_time_ms:.0f}ms", DIM, color)
    joint_count = len(result.joint_results)
    group_count = len(result.group_results)
    mat_count = result.total_materializations
    fail_count = result.total_failures
    lines.append(f"  {time_str} | {joint_count} joints | {group_count} groups | {mat_count} materializations | {fail_count} failures")


def _render_quality_summary(lines: list[str], result: ExecutionResult, color: bool) -> None:
    assertion_count = 0
    audit_count = 0
    assertion_failures = 0
    audit_failures = 0
    warnings = 0
    for jr in result.joint_results:
        for cr in jr.check_results:
            if cr.phase == "assertion":
                assertion_count += 1
                if not cr.passed:
                    if cr.severity == "warning":
                        warnings += 1
                    else:
                        assertion_failures += 1
            elif cr.phase == "audit":
                audit_count += 1
                if not cr.passed:
                    if cr.severity == "warning":
                        warnings += 1
                    else:
                        audit_failures += 1
    total = assertion_count + audit_count
    if total > 0:
        lines.append(
            f"  Quality: {total} checks (assertions: {assertion_count}, audits: {audit_count})"
            f" | {assertion_failures + audit_failures} failures | {warnings} warnings"
        )


def _render_fused_sql(lines: list[str], compiled: CompiledAssembly, group_map: dict, color: bool) -> None:  # type: ignore[type-arg]
    for fg in compiled.fused_groups:
        if fg.fused_sql:
            lines.append(f"  Fused SQL [{colorize(fg.id, BOLD, color)}]: {fg.fused_sql}")
            if fg.resolved_sql:
                lines.append(f"  Resolved: {fg.resolved_sql}")
