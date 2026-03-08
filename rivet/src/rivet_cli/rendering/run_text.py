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

    if result.run_stats is not None:
        _render_stats_summary(lines, result, verbosity, color)

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

def _render_stats_summary(
    lines: list[str],
    result: ExecutionResult,
    verbosity: int,
    color: bool,
) -> None:
    """Render per-group timing summary table and optional per-joint detail."""
    run_stats = result.run_stats
    if run_stats is None:
        return

    lines.append("")
    # Pipeline-level time breakdown
    engine_pct = (run_stats.total_engine_ms / run_stats.total_time_ms * 100) if run_stats.total_time_ms > 0 else 0
    rivet_pct = (run_stats.total_rivet_ms / run_stats.total_time_ms * 100) if run_stats.total_time_ms > 0 else 0
    lines.append(
        f"  Time: {run_stats.total_time_ms:.0f}ms total"
        f" | engine: {run_stats.total_engine_ms:.0f}ms ({engine_pct:.0f}%)"
        f" | rivet: {run_stats.total_rivet_ms:.0f}ms ({rivet_pct:.0f}%)"
    )

    lines.append("")
    lines.append(colorize("  Group Stats:", BOLD, color))
    # Header
    header = f"  {'GROUP':<20} {'JOINTS':>6} {'TOTAL_MS':>10} {'ENGINE_MS':>10} {'RIVET_MS':>10} {'ROWS_OUT':>10}"
    lines.append(colorize(header, DIM, color))

    for gs in run_stats.group_stats:
        # Sum rows_out for joints in this group
        rows_out = 0
        for jname in gs.joints:
            js = run_stats.joint_stats.get(jname)
            if js and js.rows_out is not None:
                rows_out += js.rows_out
        rivet_ms = gs.timing.total_ms - gs.timing.engine_ms
        row = (
            f"  {gs.group_id:<20} {len(gs.joints):>6} "
            f"{gs.timing.total_ms:>10.0f} {gs.timing.engine_ms:>10.0f} {rivet_ms:>10.0f} {rows_out:>10}"
        )
        lines.append(row)

        # Verbosity >= 2: per-joint detail rows
        if verbosity >= 2:
            for jname in gs.joints:
                js = run_stats.joint_stats.get(jname)
                if js is None:
                    continue
                ri = js.rows_in if js.rows_in is not None else "-"
                ro = js.rows_out if js.rows_out is not None else "-"
                mat = ""
                if js.materialization_stats is not None:
                    ms = js.materialization_stats
                    mat = f"mat: {ms.row_count} rows, {ms.byte_size} bytes"
                detail = f"    {jname}: rows_in={ri} rows_out={ro}"
                if mat:
                    detail += f" | {mat}"
                lines.append(colorize(detail, DIM, color))

            # Engine metadata at verbosity >= 2
            if gs.plugin_metrics and gs.plugin_metrics.well_known:
                for cat_name, cat in gs.plugin_metrics.well_known.items():
                    lines.append(colorize(f"    engine[{cat_name}]: {cat}", DIM, color))



def _render_fused_sql(lines: list[str], compiled: CompiledAssembly, group_map: dict, color: bool) -> None:  # type: ignore[type-arg]
    for fg in compiled.fused_groups:
        if fg.fused_sql:
            lines.append(f"  Fused SQL [{colorize(fg.id, BOLD, color)}]: {fg.fused_sql}")
            if fg.resolved_sql:
                lines.append(f"  Resolved: {fg.resolved_sql}")
