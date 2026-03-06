"""Shared AssemblyFormatter for CLI and REPL rendering.

Renders CompiledAssembly as structured text with configurable verbosity
and optional ANSI color. Used by both CLI compile command and REPL views.
"""

from __future__ import annotations

import re

from rivet_cli.rendering.colors import (
    BLUE,
    BOLD,
    CYAN,
    DIM,
    GREEN,
    MAGENTA,
    RESET,
    SYM_ASSERT,
    SYM_AUDIT,
    SYM_CHECK,
    SYM_MATERIALIZE,
    SYM_NOT_APPLICABLE,
    YELLOW,
    colorize,
)
from rivet_core.compiler import CompiledAssembly, CompiledJoint, Materialization
from rivet_core.optimizer import FusedGroup

_TYPE_ICONS = {"source": "📥", "sql": "🔧", "sink": "📤", "python": "🐍"}

_SQL_KW = re.compile(
    r"\b(SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|FULL|CROSS|ON|AND|OR|NOT|"
    r"IN|EXISTS|BETWEEN|LIKE|IS|NULL|AS|WITH|UNION|ALL|INSERT|INTO|VALUES|"
    r"UPDATE|SET|DELETE|CREATE|TABLE|VIEW|DROP|ALTER|GROUP|BY|ORDER|HAVING|"
    r"LIMIT|OFFSET|DISTINCT|CASE|WHEN|THEN|ELSE|END|CAST|COALESCE|COUNT|"
    r"SUM|AVG|MIN|MAX|OVER|PARTITION|ROW_NUMBER|RANK|DENSE_RANK)\b",
    re.IGNORECASE,
)


class AssemblyFormatter:
    """Shared renderer for CompiledAssembly output.

    Verbosity levels:
        0 (compact): joint names and types only
        1 (normal):  engines, schemas, materializations
        2 (verbose): all metadata including SourceStats, logical plans, full column lists
    """

    def __init__(self, color: bool = True, verbosity: int = 1) -> None:
        self.color = color
        self.verbosity = verbosity

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def render(self, compiled: CompiledAssembly) -> str:
        """Full assembly render.

        Verbosity controls detail:
            0 (compact): header, DAG with names/types only, summary line
            1 (normal):  + execution plan, engine boundaries, full summary
            2 (verbose): + source stats, logical plans, full column lists
        """
        lines: list[str] = []
        self._render_header(lines, compiled)
        lines.append("")
        self._render_dag(lines, compiled)
        if self.verbosity >= 1:
            lines.append("")
            self._render_execution_plan(lines, compiled)
            if compiled.engine_boundaries:
                lines.append("")
                self._render_engine_boundaries(lines, compiled)
            lines.append("")
            self._render_summary(lines, compiled)
        summary = self.render_summary_line(compiled)
        if summary:
            lines.append("")
            lines.append(summary)
        return "\n".join(lines)

    def render_summary_line(self, compiled: CompiledAssembly) -> str:
        """One-line compilation summary.

        Example: "✓ compiled 12 joints (8/10 schemas) in 340ms [introspection: 6 ok, 2 failed, 2 skipped]"
        """
        total = len(compiled.joints)
        schema_count = sum(1 for j in compiled.joints if j.output_schema is not None)
        schema_total = total

        stats = getattr(compiled, "compilation_stats", None)
        if stats is not None:
            duration = f" in {stats.compile_duration_ms}ms"
            intro = (
                f" [introspection: {stats.introspection_succeeded} ok"
                f", {stats.introspection_failed} failed"
                f", {stats.introspection_skipped} skipped]"
            )
        else:
            duration = ""
            intro = ""

        sym = self._c(SYM_CHECK, GREEN)
        return f"{sym} compiled {total} joints ({schema_count}/{schema_total} schemas){duration}{intro}"

    def render_joint_detail(self, joint: CompiledJoint) -> str:
        """Detailed single-joint view.

        Respects verbosity:
            0 (compact): name, type, engine only
            1 (normal):  + catalog, resolution, adapter, upstream, SQL, schema, checks
            2 (verbose): + source stats, logical plan, full column lists
        """
        lines: list[str] = []
        icon = _TYPE_ICONS.get(joint.type, "·")
        name = self._c(joint.name, BOLD)
        engine_label = f" [{self._c(joint.engine, BLUE)}]"
        lines.append(f"{icon} {name} ({joint.type}){engine_label}")

        if self.verbosity < 1:
            return "\n".join(lines)

        if joint.catalog:
            lines.append(f"    catalog: {self._c(joint.catalog, BLUE)} ({joint.catalog_type})")
        lines.append(f"    engine resolution: {joint.engine_resolution}")
        if joint.adapter:
            lines.append(f"    adapter: {self._c(joint.adapter, BLUE)}")
        if joint.upstream:
            lines.append(f"    upstream: {', '.join(joint.upstream)}")
        if joint.table:
            lines.append(f"    table: {joint.table}")
        if joint.write_strategy:
            lines.append(f"    write strategy: {joint.write_strategy}")
        if joint.tags:
            lines.append(f"    tags: {', '.join(joint.tags)}")
        if joint.description:
            lines.append(f"    description: {joint.description}")

        # SQL variants
        sql_block = self.render_sql_variants(joint)
        if sql_block:
            lines.append(sql_block)

        # Schema (gated by confidence)
        self._append_schema(lines, joint, indent="    ")

        # Source stats (verbose only)
        if self.verbosity >= 2:
            self._append_source_stats(lines, joint, indent="    ")

        # Optimizations
        if joint.optimizations:
            for opt in joint.optimizations:
                if opt.status == "applied":
                    sym = self._c(SYM_CHECK, GREEN)
                else:
                    sym = self._c(SYM_NOT_APPLICABLE, DIM)
                lines.append(f"    {sym} {opt.rule}: {opt.status} - {opt.detail}")

        # Logical plan (verbose only)
        if self.verbosity >= 2 and joint.logical_plan:
            lines.append(f"    logical plan: {joint.logical_plan}")

        # Checks
        for chk in joint.checks:
            if chk.phase == "assertion":
                sym = self._c(SYM_ASSERT, CYAN)
            elif chk.phase == "audit":
                sym = self._c(SYM_AUDIT, MAGENTA)
            else:
                sym = "·"
            lines.append(f"    {sym} {chk.type} [{chk.severity}] {chk.config}")

        return "\n".join(lines)

    def render_sql_variants(self, joint: CompiledJoint) -> str:
        """Render original/translated/resolved SQL with labels.

        Shows three labeled variants when distinct and non-null.
        Shows SQL once without labels when all non-null variants are identical
        or only one exists.
        """
        variants: list[tuple[str, str]] = []
        if joint.sql:
            variants.append(("original", joint.sql))
        if joint.sql_translated and joint.sql_translated != joint.sql:
            variants.append(("translated", joint.sql_translated))
        if joint.sql_resolved and joint.sql_resolved not in (joint.sql, joint.sql_translated):
            variants.append(("resolved", joint.sql_resolved))

        if not variants:
            return ""

        lines: list[str] = []
        if len(variants) == 1:
            lines.append(f"    sql: {self._highlight_sql(variants[0][1])}")
        else:
            for label, sql in variants:
                lines.append(f"    sql ({label}): {self._highlight_sql(sql)}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Internal: header
    # ------------------------------------------------------------------

    def _render_header(self, lines: list[str], compiled: CompiledAssembly) -> None:
        lines.append(self._c("═══ Compilation Result ═══", BOLD))
        lines.append(f"Profile: {self._c(compiled.profile_name, BLUE)}")
        cat_names = ", ".join(f"{c.name} ({c.type})" for c in compiled.catalogs) or "none"
        lines.append(f"Catalogs ({len(compiled.catalogs)}): {cat_names}")
        eng_names = ", ".join(f"{e.name} ({e.engine_type})" for e in compiled.engines) or "none"
        lines.append(f"Engines ({len(compiled.engines)}): {eng_names}")
        adp_names = ", ".join(
            f"{a.engine_type} -> {a.catalog_type} ({a.source})" for a in compiled.adapters
        ) or "none"
        lines.append(f"Adapters ({len(compiled.adapters)}): {adp_names}")

    # ------------------------------------------------------------------
    # Internal: DAG rendering
    # ------------------------------------------------------------------

    def _render_dag(self, lines: list[str], compiled: CompiledAssembly) -> None:
        lines.append(self._c("─── Assembly ───", BOLD))

        joint_map = {j.name: j for j in compiled.joints}
        group_map = {fg.id: fg for fg in compiled.fused_groups}
        rendered_joints: set[str] = set()

        mat_between: dict[tuple[str, str], Materialization] = {}
        for m in compiled.materializations:
            mat_between[(m.from_joint, m.to_joint)] = m

        prev_group_exit_joints: list[str] = []

        for step in compiled.execution_order:
            fg = group_map.get(step)
            if fg:
                # Materializations from previous group
                for entry in fg.entry_joints or fg.joints[:1]:
                    for prev_exit in prev_group_exit_joints:
                        m = mat_between.get((prev_exit, entry))  # type: ignore[assignment]
                        if m:
                            self._render_materialization(lines, m)

                if len(fg.joints) >= 2:
                    self._render_fused_group(lines, fg, joint_map)
                else:
                    for jname in fg.joints:
                        j = joint_map.get(jname)
                        if j:
                            self._render_joint_line(lines, j)
                            rendered_joints.add(jname)

                for jname in fg.joints:
                    rendered_joints.add(jname)
                prev_group_exit_joints = fg.exit_joints or fg.joints[-1:]
            else:
                j = joint_map.get(step)
                if j and j.name not in rendered_joints:
                    for prev_exit in prev_group_exit_joints:
                        m = mat_between.get((prev_exit, j.name))  # type: ignore[assignment]
                        if m:
                            self._render_materialization(lines, m)
                    self._render_joint_line(lines, j)
                    rendered_joints.add(j.name)
                    prev_group_exit_joints = [j.name]

        for j in compiled.joints:
            if j.name not in rendered_joints:
                self._render_joint_line(lines, j)

    def _render_fused_group(
        self,
        lines: list[str],
        fg: FusedGroup,
        joint_map: dict[str, CompiledJoint],
    ) -> None:
        lines.append(
            f"╔══ Fused Group: {self._c(fg.id, BOLD)}"
            f" (engine: {fg.engine}, strategy: {fg.fusion_strategy}) ══╗"
        )
        for i, jname in enumerate(fg.joints):
            j = joint_map.get(jname)
            if j:
                self._render_joint_line(lines, j, indent="║ ")
                if i < len(fg.joints) - 1:
                    lines.append("║   ↓")
        if self.verbosity >= 2 and fg.fused_sql:
            lines.append(f"║ Fused SQL: {self._highlight_sql(fg.fused_sql)}")
            if fg.resolved_sql:
                lines.append(f"║ Resolved Fused SQL: {self._highlight_sql(fg.resolved_sql)}")
        lines.append("╚" + "═" * 40 + "╝")

    def _render_joint_line(
        self,
        lines: list[str],
        j: CompiledJoint,
        indent: str = "",
    ) -> None:
        icon = _TYPE_ICONS.get(j.type, "·")
        name = self._c(j.name, BOLD)
        engine_label = f" [{self._c(j.engine, BLUE)}]"

        if self.verbosity == 0:
            # Compact: name, type, engine
            lines.append(f"{indent}  {icon} {name} ({j.type}){engine_label}")
            return

        # Normal+
        lines.append(f"{indent}  {icon} {name} ({j.type}){engine_label}")
        if j.catalog:
            lines.append(f"{indent}    catalog: {self._c(j.catalog, BLUE)} ({j.catalog_type})")
        if self.verbosity >= 1:
            lines.append(f"{indent}    engine resolution: {j.engine_resolution}")
        if j.adapter:
            lines.append(f"{indent}    adapter: {self._c(j.adapter, BLUE)}")

        # SQL variants inline
        sql_block = self._sql_variants_indented(j, indent)
        if sql_block:
            lines.append(sql_block)

        # Schema (gated by confidence)
        self._append_schema(lines, j, indent=f"{indent}    ")

        # Source stats (verbose only)
        if self.verbosity >= 2:
            self._append_source_stats(lines, j, indent=f"{indent}    ")

        # Optimizations
        if self.verbosity >= 1 and j.optimizations:
            for opt in j.optimizations:
                if opt.status == "applied":
                    sym = self._c(SYM_CHECK, GREEN)
                else:
                    sym = self._c(SYM_NOT_APPLICABLE, DIM)
                lines.append(f"{indent}    {sym} {opt.rule}: {opt.status} - {opt.detail}")

        # Logical plan (verbose only)
        if self.verbosity >= 2 and j.logical_plan:
            lines.append(f"{indent}    logical plan: {j.logical_plan}")

        # Checks
        for chk in j.checks:
            if chk.phase == "assertion":
                sym = self._c(SYM_ASSERT, CYAN)
            elif chk.phase == "audit":
                sym = self._c(SYM_AUDIT, MAGENTA)
            else:
                sym = "·"
            lines.append(f"{indent}    {sym} {chk.type} [{chk.severity}] {chk.config}")

    def _render_materialization(self, lines: list[str], m: Materialization) -> None:
        sym = self._c(SYM_MATERIALIZE, YELLOW)
        lines.append(
            f"  {sym} materialization: {m.from_joint} -> {m.to_joint}"
            f" ({m.trigger}: {m.detail}) [strategy: {m.strategy}]"
        )

    # ------------------------------------------------------------------
    # Internal: execution plan
    # ------------------------------------------------------------------

    def _render_execution_plan(self, lines: list[str], compiled: CompiledAssembly) -> None:
        lines.append(self._c("─── Execution Plan ───", BOLD))
        group_map = {fg.id: fg for fg in compiled.fused_groups}

        for i, step in enumerate(compiled.execution_order, 1):
            fg = group_map.get(step)
            if fg:
                lines.append(
                    f"  {i}. [fused ({len(fg.joints)} joints)] {step} (engine: {fg.engine})"
                )
            else:
                lines.append(f"  {i}. {step}")

        if compiled.materializations:
            lines.append("  Materializations:")
            for m in compiled.materializations:
                sym = self._c(SYM_MATERIALIZE, YELLOW)
                lines.append(f"    {sym} {m.from_joint} -> {m.to_joint} ({m.trigger})")

    # ------------------------------------------------------------------
    # Internal: engine boundaries
    # ------------------------------------------------------------------

    def _render_engine_boundaries(self, lines: list[str], compiled: CompiledAssembly) -> None:
        lines.append(self._c("─── Engine Boundaries ───", BOLD))
        for eb in compiled.engine_boundaries:
            arrow = self._c("→", YELLOW)
            prod = self._c(eb.producer_engine_type, BLUE)
            cons = self._c(eb.consumer_engine_type, BLUE)
            joints = ", ".join(eb.boundary_joints)
            strategy = eb.adapter_strategy or "default: arrow_passthrough"
            lines.append(f"  {prod} {arrow} {cons}  joints: [{joints}]  strategy: {strategy}")

    # ------------------------------------------------------------------
    # Internal: summary
    # ------------------------------------------------------------------

    def _render_summary(self, lines: list[str], compiled: CompiledAssembly) -> None:
        lines.append(self._c("─── Summary ───", BOLD))

        type_counts: dict[str, int] = {}
        for j in compiled.joints:
            type_counts[j.type] = type_counts.get(j.type, 0) + 1
        type_str = ", ".join(f"{t}: {c}" for t, c in sorted(type_counts.items()))
        lines.append(f"  Joints: {len(compiled.joints)} ({type_str})")

        lines.append(f"  Fused groups: {len(compiled.fused_groups)}")
        lines.append(f"  Engine boundaries: {len(compiled.engine_boundaries)}")
        lines.append(f"  Materializations: {len(compiled.materializations)}")

        assertion_count = sum(
            1 for j in compiled.joints for chk in j.checks if chk.phase == "assertion"
        )
        audit_count = sum(
            1 for j in compiled.joints for chk in j.checks if chk.phase == "audit"
        )
        lines.append(
            f"  Quality checks: {assertion_count + audit_count}"
            f" (assertions: {assertion_count}, audits: {audit_count})"
        )

        schema_count = sum(1 for j in compiled.joints if j.output_schema is not None)
        lines.append(f"  Schemas resolved: {schema_count}/{len(compiled.joints)}")

        applied = sum(1 for j in compiled.joints for o in j.optimizations if o.status == "applied")
        not_applied = sum(
            1 for j in compiled.joints for o in j.optimizations if o.status != "applied"
        )
        lines.append(f"  Optimizations: {applied} applied, {not_applied} not applicable")

        status = (
            self._c(SYM_CHECK + " valid", GREEN)
            if compiled.success
            else self._c("invalid", "red")
        )
        lines.append(f"  Validation: {status}")

    # ------------------------------------------------------------------
    # Internal: schema display (gated by confidence)
    # ------------------------------------------------------------------

    def _append_schema(self, lines: list[str], j: CompiledJoint, indent: str) -> None:
        if self.verbosity < 1:
            return
        confidence = getattr(j, "schema_confidence", "none")
        if j.output_schema and confidence in ("introspected", "inferred"):
            if self.verbosity >= 2:
                cols = ", ".join(f"{c.name}: {c.type}" for c in j.output_schema.columns)
            else:
                cols = f"{len(j.output_schema.columns)} columns"
            lines.append(f"{indent}schema ({confidence}): [{cols}]")
        elif j.output_schema:
            lines.append(f"{indent}schema: {self._c('(unverified)', DIM)}")
        else:
            lines.append(f"{indent}schema: {self._c('(not available)', DIM)}")

    # ------------------------------------------------------------------
    # Internal: source stats (verbose only)
    # ------------------------------------------------------------------

    def _append_source_stats(self, lines: list[str], j: CompiledJoint, indent: str) -> None:
        stats = getattr(j, "source_stats", None)
        if stats is None:
            return
        parts: list[str] = []
        if stats.row_count is not None:
            parts.append(f"rows={stats.row_count}")
        if stats.size_bytes is not None:
            parts.append(f"size={stats.size_bytes}B")
        if stats.last_modified is not None:
            parts.append(f"modified={stats.last_modified.isoformat()}")
        if stats.partition_count is not None:
            parts.append(f"partitions={stats.partition_count}")
        if parts:
            lines.append(f"{indent}source stats: {', '.join(parts)}")

    # ------------------------------------------------------------------
    # Internal: SQL helpers
    # ------------------------------------------------------------------

    def _sql_variants_indented(self, j: CompiledJoint, indent: str) -> str:
        """SQL variant display for inline DAG rendering."""
        variants: list[tuple[str, str]] = []
        if j.sql:
            variants.append(("original", j.sql))
        if j.sql_translated and j.sql_translated != j.sql:
            variants.append(("translated", j.sql_translated))
        if j.sql_resolved and j.sql_resolved not in (j.sql, j.sql_translated):
            variants.append(("resolved", j.sql_resolved))

        if not variants:
            return ""

        parts: list[str] = []
        if len(variants) == 1:
            parts.append(f"{indent}    sql: {self._highlight_sql(variants[0][1])}")
        else:
            for label, sql in variants:
                parts.append(f"{indent}    sql ({label}): {self._highlight_sql(sql)}")
        return "\n".join(parts)

    def _highlight_sql(self, sql: str) -> str:
        if not self.color:
            return sql
        return _SQL_KW.sub(lambda m: f"{BLUE}{m.group(0)}{RESET}", sql)

    # ------------------------------------------------------------------
    # Internal: color helper
    # ------------------------------------------------------------------

    def _c(self, text: str, color: str) -> str:
        return colorize(text, color, self.color)
