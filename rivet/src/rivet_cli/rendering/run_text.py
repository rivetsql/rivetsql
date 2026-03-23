"""Run text format renderer for execution progress and quality."""

from __future__ import annotations

import sys
import threading
import time
from typing import Any, TextIO

from rivet_cli.rendering.colors import (
    BLUE,
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
    joint_icon,
)
from rivet_core.compiler import CompiledAssembly, CompiledJoint
from rivet_core.errors import RivetError
from rivet_core.executor import ExecutionResult, JointExecutionResult
from rivet_core.optimizer import FusedGroup

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _group_label(group_id: str, joints: list[str]) -> str:
    """Return a human-friendly label for a fused group.

    Uses the last joint name when available, falls back to the UUID.
    """
    return joints[-1] if joints else group_id


# ---------------------------------------------------------------------------
# LiveRunRenderer — implements ProgressCallback for real-time run output
# ---------------------------------------------------------------------------


class LiveRunRenderer:
    """Real-time progress renderer for ``rivet run``.

    Implements the ``ProgressCallback`` protocol defined in
    ``rivet_core.executor``.  All live progress lines are written to
    *file* (defaults to ``sys.stderr``).  The final summary is returned
    as a string from :meth:`render_summary` for the caller to print to
    stdout.

    A ``threading.Lock`` serialises writes so concurrent callback
    invocations from the wavefront scheduler never interleave output.
    """

    def __init__(
        self,
        compiled: CompiledAssembly,
        verbosity: int,
        color: bool,
        file: TextIO | None = None,
    ) -> None:
        self.compiled = compiled
        self.verbosity = verbosity
        self.color = color
        self._file = file if file is not None else sys.stderr

        # Serialise writes from concurrent callbacks
        self._lock = threading.Lock()

        # Lookup maps built from the compiled assembly
        self._joint_map: dict[str, CompiledJoint] = {j.name: j for j in compiled.joints}
        self._group_map: dict[str, FusedGroup] = {fg.id: fg for fg in compiled.fused_groups}
        self._joint_to_group: dict[str, str] = {}
        for fg in compiled.fused_groups:
            for jname in fg.joints:
                self._joint_to_group[jname] = fg.id

        # Tracking state for the summary
        self._completed_groups: int = 0
        self._failed_groups: int = 0
        self._materialization_count: int = 0
        self._start_time: float = time.monotonic()

    # -- helpers -------------------------------------------------------------

    def _write(self, line: str) -> None:
        """Write a line to the output file under the lock."""
        with self._lock:
            self._file.write(line + "\n")
            self._file.flush()

    def _c(self, text: str, color: str) -> str:
        """Colorise *text* if color output is enabled."""
        return colorize(text, color, self.color)

    # -- execution plan (v>=1) -----------------------------------------------

    def print_execution_plan(self) -> None:
        """Print the parallel execution plan to stderr before execution.

        Called by the run command handler at verbosity >= 1.
        """
        if self.verbosity < 1:
            return

        plan = self.compiled.parallel_execution_plan
        if not plan:
            return

        self._write(self._c("─── Execution Plan ───", BOLD))
        for wave in plan:
            self._write(f"  Wave {wave.wave_number}:")
            for gid in wave.groups:
                fg = self._group_map.get(gid)
                if fg:
                    joint_count = len(fg.joints)
                    engine = self._c(fg.engine, BLUE)
                    self._write(f"    {gid} ({joint_count} joints) [engine: {engine}]")
                else:
                    self._write(f"    {gid}")
        self._write("")

    # -- ProgressCallback implementations ------------------------------------

    def on_group_start(self, group_id: str, engine: str) -> None:
        """Called when a fused group begins execution.

        At compact verbosity (v=0) this is a no-op.
        At verbose verbosity (v=2), prints fused SQL for multi-joint groups.
        """
        if self.verbosity >= 2:
            fg = self._group_map.get(group_id)
            if fg and fg.fused_sql and len(fg.joints) >= 2:
                label = _group_label(group_id, fg.joints)
                self._write(self._c(f"─── Group {label} ({engine}) ───", BOLD))
                self._write("Fused SQL:")
                self._write(f"  {fg.fused_sql}")

    def on_group_complete(
        self,
        group_id: str,
        success: bool,
        joint_results: list[JointExecutionResult],
        elapsed_ms: float,
    ) -> None:
        """Called when a fused group finishes execution."""
        if success:
            self._completed_groups += 1
        else:
            self._failed_groups += 1

        if self.verbosity < 0:
            return

        for jr in joint_results:
            cj = self._joint_map.get(jr.name)
            jtype = cj.type if cj else "sql"
            icon = joint_icon(jtype)
            # rows_out and timing are only set on the exit joint of a fused
            # group; non-exit joints show just their name and type.
            has_stats = jr.rows_out is not None
            rows = f"{jr.rows_out} rows" if has_stats else ""
            time_label = f"{elapsed_ms:.0f}ms" if has_stats else ""

            if self.verbosity >= 1:
                # v=1: add engine, schema column count, pushdown details
                engine_label = self._c(f"[{cj.engine}]", BLUE) if cj else ""
                parts = [f"{icon} {jr.name} ({jtype}) {engine_label}"]
                if rows:
                    parts.append(rows)
                if time_label:
                    parts.append(time_label)

                # Schema column count
                if cj and cj.output_schema:
                    col_count = len(cj.output_schema.columns)
                    parts.append(self._c(f"{col_count} cols", DIM))

                # Pushdown details from the fused group
                fg = self._group_map.get(group_id)
                if fg and fg.pushdown:
                    pushdown_parts: list[str] = []
                    if fg.pushdown.predicates.pushed:
                        pushdown_parts.append(
                            f"{len(fg.pushdown.predicates.pushed)} predicates pushed"
                        )
                    if fg.pushdown.projections.pushed_columns is not None:
                        pushdown_parts.append(
                            f"{len(fg.pushdown.projections.pushed_columns)} cols projected"
                        )
                    if fg.pushdown.limit.pushed_limit is not None:
                        pushdown_parts.append(f"limit {fg.pushdown.limit.pushed_limit}")
                    if pushdown_parts:
                        parts.append(self._c(f"({', '.join(pushdown_parts)})", DIM))

                self._write(" ".join(parts))

                # v=2: add logical plan
                if self.verbosity >= 2 and cj and cj.logical_plan:
                    self._write(f"  logical plan: {cj.logical_plan}")
            else:
                # v=0: compact line
                parts = [f"{icon} {jr.name} ({jtype})"]
                if rows:
                    parts.append(rows)
                if time_label:
                    parts.append(time_label)
                self._write(" ".join(parts))

    def on_materialization(
        self,
        source_joint: str,
        target_engine: str,
        strategy: str,
    ) -> None:
        """Called when a materialization occurs."""
        self._materialization_count += 1

        if self.verbosity < 0:
            return

        self._write(
            self._c(
                f"{SYM_MATERIALIZE} {source_joint} → {target_engine} ({strategy})",
                YELLOW,
            )
        )

    def on_check_result(
        self,
        joint_name: str,
        check_type: str,
        passed: bool,
        phase: str,
    ) -> None:
        """Called when a quality check completes."""
        if self.verbosity < 0:
            return

        label = self._c("PASS", GREEN) if passed else self._c("FAIL", RED)
        self._write(self._c(f"{SYM_ASSERT} {joint_name} {check_type} ", CYAN) + label)

    def on_error(self, group_id: str, error: RivetError) -> None:
        """Called when a group fails."""
        self._failed_groups += 1
        fg = self._group_map.get(group_id)
        label = _group_label(group_id, fg.joints if fg else [])
        self._write(
            self._c(
                f"{SYM_ERROR} {label}: [{error.code}] {error.message}",
                RED,
            )
        )

        if self.verbosity >= 2:
            if error.context:
                self._write(self._c(f"  context: {error.context}", DIM))
            if error.remediation:
                self._write(self._c(f"  remediation: {error.remediation}", DIM))
            if error.original_sql:
                self._write(self._c(f"  sql: {error.original_sql}", DIM))
            if error.error_position:
                line, col = error.error_position
                self._write(self._c(f"  position: line {line}, column {col}", DIM))

    def render_summary(self, result: ExecutionResult) -> str:
        """Return the final summary string for stdout.

        Compact (v=0): one-line counts.
        Normal  (v=1): timing breakdown + group stats table.
        Verbose (v=2): per-joint detail rows inside group stats.
        """
        elapsed = result.total_time_ms
        joints = len(result.joint_results)
        groups = self._completed_groups + self._failed_groups
        mats = self._materialization_count
        fails = self._failed_groups

        # Quiet mode: no stdout output at all
        if self.verbosity < 0:
            return ""

        compact = (
            f"{elapsed:.0f}ms | {joints} joints | {groups} groups"
            f" | {mats} materializations | {fails} failures"
        )

        if self.verbosity == 0:
            return compact

        # -- v>=1: timing breakdown + group stats table ----------------------
        lines: list[str] = [compact, ""]

        run_stats = result.run_stats
        if run_stats is not None:
            engine_pct = (
                (run_stats.total_engine_ms / run_stats.total_time_ms * 100)
                if run_stats.total_time_ms > 0
                else 0
            )
            rivet_pct = (
                (run_stats.total_rivet_ms / run_stats.total_time_ms * 100)
                if run_stats.total_time_ms > 0
                else 0
            )
            lines.append(
                f"Time: {run_stats.total_time_ms:.0f}ms total"
                f" | engine: {run_stats.total_engine_ms:.0f}ms ({engine_pct:.0f}%)"
                f" | rivet: {run_stats.total_rivet_ms:.0f}ms ({rivet_pct:.0f}%)"
            )
            lines.append("")

            # Group stats table
            header = (
                f"{'GROUP':<20} {'JOINTS':>6} {'TOTAL_MS':>10}"
                f" {'ENGINE_MS':>10} {'RIVET_MS':>10} {'ROWS_OUT':>10}"
            )
            lines.append(header)

            for gs in run_stats.group_stats:
                rows_out = 0
                for jname in gs.joints:
                    js = run_stats.joint_stats.get(jname)
                    if js and js.rows_out is not None:
                        rows_out += js.rows_out
                rivet_ms = gs.timing.total_ms - gs.timing.engine_ms
                label = _group_label(gs.group_id, gs.joints)
                lines.append(
                    f"{label:<20} {len(gs.joints):>6}"
                    f" {gs.timing.total_ms:>10.0f} {gs.timing.engine_ms:>10.0f}"
                    f" {rivet_ms:>10.0f} {rows_out:>10}"
                )

                # -- v>=2: per-joint detail rows -----------------------------
                if self.verbosity >= 2:
                    for jname in gs.joints:
                        js = run_stats.joint_stats.get(jname)
                        if js is None:
                            continue
                        ri: str = str(js.rows_in) if js.rows_in is not None else "-"
                        ro: str = str(js.rows_out) if js.rows_out is not None else "-"
                        detail = f"  {jname}: rows_in={ri} rows_out={ro}"
                        if js.materialization_stats is not None:
                            ms = js.materialization_stats
                            detail += f" | mat: {ms.row_count} rows, {ms.byte_size} bytes"
                        lines.append(detail)

        return "\n".join(lines)


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

    _render_execution_plan(lines, compiled, group_map, color)

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


def _render_execution_plan(
    lines: list[str],
    compiled: CompiledAssembly,
    group_map: dict[str, FusedGroup],
    color: bool,
) -> None:
    """Render the parallel execution plan grouped by wave."""
    if not compiled.parallel_execution_plan:
        return
    lines.append(colorize("Execution Plan:", BOLD, color))
    for wave in compiled.parallel_execution_plan:
        entries: list[str] = []
        for gid in wave.groups:
            fg = group_map.get(gid)
            engine = fg.engine if fg else "unknown"
            entries.append(f"{gid} (engine: {engine})")
        lines.append(f"  Wave {wave.wave_number}: [{', '.join(entries)}]")
    lines.append("")


def _render_joint_result(
    lines: list[str],
    jr: JointExecutionResult,
    cj: CompiledJoint | None,
    verbosity: int,
    color: bool,
) -> None:
    status = (
        colorize(f"{SYM_CHECK} OK", GREEN, color)
        if jr.success
        else colorize(f"{SYM_ERROR} FAIL", RED, color)
    )
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
            sym = (
                colorize(SYM_CHECK, GREEN, color)
                if opt.status == "applied"
                else colorize(SYM_NOT_APPLICABLE, DIM, color)
            )
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
    lines.append(
        f"      {colorize('[RVT-601]', RED, color)} joint={joint_name} type={cr.type} severity={cr.severity}"  # type: ignore[attr-defined]
    )
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
    lines.append(
        f"  {time_str} | {joint_count} joints | {group_count} groups | {mat_count} materializations | {fail_count} failures"
    )


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
    engine_pct = (
        (run_stats.total_engine_ms / run_stats.total_time_ms * 100)
        if run_stats.total_time_ms > 0
        else 0
    )
    rivet_pct = (
        (run_stats.total_rivet_ms / run_stats.total_time_ms * 100)
        if run_stats.total_time_ms > 0
        else 0
    )
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
        label = _group_label(gs.group_id, gs.joints)
        row = (
            f"  {label:<20} {len(gs.joints):>6} "
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


def _render_fused_sql(
    lines: list[str],
    compiled: CompiledAssembly,
    group_map: dict[str, Any],
    color: bool,
) -> None:
    for fg in compiled.fused_groups:
        if fg.fused_sql:
            lines.append(f"  Fused SQL [{colorize(fg.id, BOLD, color)}]: {fg.fused_sql}")
            if fg.resolved_sql:
                lines.append(f"  Resolved: {fg.resolved_sql}")
