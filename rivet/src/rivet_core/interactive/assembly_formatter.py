"""Formats CompiledAssembly contents for human inspection.

Headless — no TUI/CLI imports. Lives in rivet_core/interactive/.
"""

from __future__ import annotations

import re
from collections import defaultdict

from rivet_core.compiler import CompiledAssembly, CompiledJoint
from rivet_core.interactive.types import (
    AdapterInfo,
    AssemblyInspection,
    CatalogInfo,
    DagEdge,
    DagNode,
    DagSection,
    EngineInfo,
    ExecutionOrderSection,
    ExecutionStep,
    FusedGroupDetail,
    FusedGroupsSection,
    InspectFilter,
    JointInspection,
    MaterializationDetail,
    MaterializationsSection,
    OverviewSection,
    SchemaField,
    SourceStatsInfo,
    Verbosity,
)

_TYPE_ICONS = {"source": "⚪", "sql": "🔵", "python": "🟣", "sink": "🟢"}

# ANSI codes
_BOLD = "\x1b[1m"
_CYAN = "\x1b[36m"
_GREEN = "\x1b[32m"
_YELLOW = "\x1b[33m"
_RED = "\x1b[31m"
_DIM = "\x1b[2m"
_BLUE = "\x1b[34m"
_MAGENTA = "\x1b[35m"
_RESET = "\x1b[0m"

# SQL keyword pattern for highlighting
_SQL_KEYWORDS = re.compile(
    r"\b(SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|FULL|CROSS|ON|AND|OR|NOT|"
    r"IN|EXISTS|BETWEEN|LIKE|IS|NULL|AS|WITH|UNION|ALL|INSERT|INTO|VALUES|"
    r"UPDATE|SET|DELETE|CREATE|TABLE|VIEW|DROP|ALTER|GROUP|BY|ORDER|HAVING|"
    r"LIMIT|OFFSET|DISTINCT|CASE|WHEN|THEN|ELSE|END|CAST|COALESCE|COUNT|"
    r"SUM|AVG|MIN|MAX|OVER|PARTITION|ROW_NUMBER|RANK|DENSE_RANK)\b",
    re.IGNORECASE,
)


def _matches_filter(joint: CompiledJoint, f: InspectFilter) -> bool:
    if f.engine is not None and joint.engine != f.engine:
        return False
    if f.tag is not None and f.tag not in joint.tags:
        return False
    if f.joint_type is not None and joint.type != f.joint_type:
        return False
    return True


class AssemblyFormatter:
    """Formats CompiledAssembly contents for human inspection."""

    def format_assembly(
        self,
        assembly: CompiledAssembly,
        verbosity: Verbosity = Verbosity.NORMAL,
        filter: InspectFilter | None = None,
    ) -> AssemblyInspection:
        filtered = assembly.joints
        if filter is not None:
            filtered = [j for j in assembly.joints if _matches_filter(j, filter)]

        filtered_names = {j.name for j in filtered}
        overview = self._build_overview(assembly, filtered, filtered_names)
        exec_order = self._build_execution_order(assembly, filtered_names)

        fused = None
        mats = None
        if verbosity in (Verbosity.NORMAL, Verbosity.FULL):
            fused = self._build_fused_groups(assembly, filtered_names)
            mats = self._build_materializations(assembly, filtered_names)

        dag = None
        details = None
        if verbosity == Verbosity.FULL:
            dag = self._build_dag(assembly, filtered_names)
            details = [self._build_joint_inspection(j) for j in filtered]

        return AssemblyInspection(
            overview=overview,
            execution_order=exec_order,
            fused_groups=fused,
            materializations=mats,
            dag=dag,
            joint_details=details,
            filter_applied=filter,
            verbosity=verbosity,
        )

    def format_assembly_text(
        self,
        assembly: CompiledAssembly,
        verbosity: Verbosity = Verbosity.NORMAL,
        filter: InspectFilter | None = None,
        ansi: bool = True,
    ) -> str:
        inspection = self.format_assembly(assembly, verbosity, filter)
        cs = getattr(assembly, "compilation_stats", None)
        return self._render_text(inspection, ansi=ansi, compilation_stats=cs)

    def format_joint(
        self,
        joint: CompiledJoint,
        assembly: CompiledAssembly,
    ) -> JointInspection:
        return self._build_joint_inspection(joint)

    def format_joint_text(
        self,
        joint: CompiledJoint,
        assembly: CompiledAssembly,
        ansi: bool = True,
    ) -> str:
        insp = self.format_joint(joint, assembly)
        return self._render_joint_text(insp, ansi=ansi)

    # ------------------------------------------------------------------
    # Section builders
    # ------------------------------------------------------------------

    def _build_overview(
        self,
        assembly: CompiledAssembly,
        filtered: list[CompiledJoint],
        filtered_names: set[str],
    ) -> OverviewSection:
        joint_counts: dict[str, int] = defaultdict(int)
        for j in filtered:
            joint_counts[j.type] += 1

        engine_counts: dict[str, int] = defaultdict(int)
        for j in filtered:
            if j.engine:
                engine_counts[j.engine] += 1

        engine_map = {e.name: e for e in assembly.engines}
        engines = [
            EngineInfo(
                name=name,
                engine_type=engine_map[name].engine_type if name in engine_map else "",
                joint_count=count,
            )
            for name, count in engine_counts.items()
        ]

        fg_count = sum(
            1 for g in assembly.fused_groups if any(jn in filtered_names for jn in g.joints)
        )
        mat_count = sum(
            1
            for m in assembly.materializations
            if m.from_joint in filtered_names or m.to_joint in filtered_names
        )

        # Schema coverage from compilation_stats or computed from joints
        cs = getattr(assembly, "compilation_stats", None)
        if cs is not None:
            pass

        return OverviewSection(
            profile_name=assembly.profile_name,
            joint_counts=dict(joint_counts),
            total_joints=len(filtered),
            fused_group_count=fg_count,
            materialization_count=mat_count,
            engines=engines,
            catalogs=[CatalogInfo(name=c.name, type=c.type) for c in assembly.catalogs],
            adapters=[
                AdapterInfo(engine_type=a.engine_type, catalog_type=a.catalog_type, source=a.source)
                for a in assembly.adapters
            ],
            success=assembly.success,
            warnings=list(assembly.warnings),
            errors=[str(e) for e in assembly.errors],
        )

    def _build_execution_order(
        self, assembly: CompiledAssembly, filtered_names: set[str]
    ) -> ExecutionOrderSection:
        group_map = {g.id: g for g in assembly.fused_groups}
        joint_map = {j.name: j for j in assembly.joints}
        mat_joints = set()
        for m in assembly.materializations:
            mat_joints.add(m.from_joint)
            mat_joints.add(m.to_joint)

        # Build group_id → wave_number mapping from parallel execution plan
        wave_map: dict[str, int] = {}
        for wave in assembly.parallel_execution_plan:
            for gid in wave.groups:
                wave_map[gid] = wave.wave_number

        steps: list[ExecutionStep] = []
        step_num = 0
        for step_id in assembly.execution_order:
            if step_id in group_map:
                g = group_map[step_id]
                joints_in_step = [n for n in g.joints if n in filtered_names]
                if not joints_in_step:
                    continue
                step_num += 1
                steps.append(
                    ExecutionStep(
                        step_number=step_num,
                        id=step_id,
                        engine=g.engine,
                        joints=joints_in_step,
                        is_fused=len(g.joints) > 1,
                        has_materialization=any(n in mat_joints for n in g.joints),
                        wave_number=wave_map.get(step_id, 0),
                    )
                )
            elif step_id in joint_map and step_id in filtered_names:
                j = joint_map[step_id]
                step_num += 1
                steps.append(
                    ExecutionStep(
                        step_number=step_num,
                        id=step_id,
                        engine=j.engine,
                        joints=[step_id],
                        is_fused=False,
                        has_materialization=step_id in mat_joints,
                        wave_number=wave_map.get(step_id, 0),
                    )
                )
        return ExecutionOrderSection(steps=steps)

    def _build_fused_groups(
        self, assembly: CompiledAssembly, filtered_names: set[str]
    ) -> FusedGroupsSection:
        groups: list[FusedGroupDetail] = []
        for g in assembly.fused_groups:
            if not any(n in filtered_names for n in g.joints):
                continue
            pushdown_preds = None
            if g.pushdown is not None:
                preds = []
                for p in g.pushdown.predicates.pushed:
                    preds.append(str(p))
                if g.pushdown.projections.pushed_columns is not None:
                    preds.append(f"project: {g.pushdown.projections.pushed_columns}")
                if g.pushdown.limit.pushed_limit is not None:
                    preds.append(f"limit: {g.pushdown.limit.pushed_limit}")
                for c in g.pushdown.casts.pushed:
                    preds.append(f"cast: {c.column} {c.from_type}->{c.to_type}")
                pushdown_preds = preds if preds else None

            residual_ops = None
            if g.residual is not None:
                ops = []
                for p in g.residual.predicates:
                    ops.append(f"filter: {p}")
                if g.residual.limit is not None:
                    ops.append(f"limit: {g.residual.limit}")
                for c in g.residual.casts:
                    ops.append(f"cast: {c.column} {c.from_type}->{c.to_type}")
                residual_ops = ops if ops else None

            groups.append(
                FusedGroupDetail(
                    id=g.id,
                    engine=g.engine,
                    engine_type=g.engine_type,
                    fusion_strategy=g.fusion_strategy,
                    joints=list(g.joints),
                    entry_joints=list(g.entry_joints),
                    exit_joints=list(g.exit_joints),
                    adapters=dict(g.adapters),
                    fused_sql=g.fused_sql,
                    resolved_sql=g.resolved_sql,
                    pushdown_predicates=pushdown_preds,
                    residual_operations=residual_ops,
                )
            )
        return FusedGroupsSection(groups=groups)

    def _build_materializations(
        self, assembly: CompiledAssembly, filtered_names: set[str]
    ) -> MaterializationsSection:
        by_trigger: dict[str, list[MaterializationDetail]] = defaultdict(list)
        for m in assembly.materializations:
            if m.from_joint in filtered_names or m.to_joint in filtered_names:
                by_trigger[m.trigger].append(
                    MaterializationDetail(
                        from_joint=m.from_joint,
                        to_joint=m.to_joint,
                        trigger=m.trigger,
                        detail=m.detail,
                        strategy=m.strategy,
                    )
                )
        return MaterializationsSection(by_trigger=dict(by_trigger))

    def _build_dag(
        self, assembly: CompiledAssembly, filtered_names: set[str]
    ) -> DagSection:
        joint_map = {j.name: j for j in assembly.joints if j.name in filtered_names}
        mat_set = {
            (m.from_joint, m.to_joint) for m in assembly.materializations
        }

        # Topological sort via Kahn's algorithm
        in_degree: dict[str, int] = {n: 0 for n in joint_map}
        adj: dict[str, list[str]] = {n: [] for n in joint_map}
        edges: list[DagEdge] = []
        for name, j in joint_map.items():
            for up in j.upstream:
                if up in joint_map:
                    adj[up].append(name)
                    in_degree[name] += 1
                    edges.append(DagEdge(from_joint=up, to_joint=name))

        queue = [n for n in joint_map if in_degree[n] == 0]
        queue.sort()
        ordered: list[str] = []
        while queue:
            node = queue.pop(0)
            ordered.append(node)
            for child in sorted(adj[node]):
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)
                    queue.sort()

        nodes = [
            DagNode(
                name=n,
                joint_type=joint_map[n].type,
                engine=joint_map[n].engine,
                fused_group_id=joint_map[n].fused_group_id,
                icon=_TYPE_ICONS.get(joint_map[n].type, "⚪"),
            )
            for n in ordered
        ]

        # Render text DAG
        children: dict[str, list[str]] = defaultdict(list)
        for e in edges:
            children[e.from_joint].append(e.to_joint)
        roots = [n for n in ordered if not any(n in children[p] for p in ordered)]

        lines: list[str] = []
        visited: set[str] = set()

        def _render(name: str, prefix: str, is_last: bool, is_root: bool) -> None:
            if name in visited:
                return
            visited.add(name)
            j = joint_map[name]
            icon = _TYPE_ICONS.get(j.type, "⚪")
            group_str = f" [{j.fused_group_id}]" if j.fused_group_id else ""
            # Schema annotation gated by confidence
            schema_str = ""
            confidence = getattr(j, "schema_confidence", "none")
            if confidence in ("introspected", "inferred") and j.output_schema is not None:
                col_count = len(j.output_schema.columns)
                schema_str = f" ({col_count} cols)"
            if is_root:
                lines.append(f"{icon} {name} ({j.engine}){group_str}{schema_str}")
            else:
                connector = "└──▶ " if is_last else "├──▶ "
                lines.append(f"{prefix}{connector}{icon} {name} ({j.engine}){group_str}{schema_str}")

            # Check materialization annotation
            for up in j.upstream:
                if (up, name) in mat_set:
                    mat = next(
                        (m for m in assembly.materializations if m.from_joint == up and m.to_joint == name),
                        None,
                    )
                    if mat:
                        indent = prefix + ("     " if is_last else "│    ")
                        lines.append(f"{indent}⚡ materialization: {mat.trigger} → {mat.strategy}")

            kids = [c for c in children.get(name, []) if c in joint_map and c not in visited]
            child_prefix = prefix + ("     " if is_last else "│    ")
            for i, child in enumerate(kids):
                is_child_last = i == len(kids) - 1
                if kids:
                    lines.append(f"{child_prefix}│")
                _render(child, child_prefix, is_child_last, False)

        for i, root in enumerate(roots):
            if i > 0:
                lines.append("")
            _render(root, "", True, True)

        return DagSection(nodes=nodes, edges=edges, rendered_text="\n".join(lines))

    def _build_joint_inspection(self, joint: CompiledJoint) -> JointInspection:
        schema = None
        if joint.output_schema is not None:
            schema = [SchemaField(name=c.name, type=c.type) for c in joint.output_schema.columns]

        stats = None
        src = getattr(joint, "source_stats", None)
        if src is not None:
            stats = SourceStatsInfo(
                row_count=src.row_count,
                size_bytes=src.size_bytes,
                last_modified=src.last_modified,
                partition_count=src.partition_count,
            )

        return JointInspection(
            name=joint.name,
            type=joint.type,
            source_file=joint.source_file,
            engine=joint.engine,
            engine_resolution=joint.engine_resolution,  # type: ignore[arg-type]
            adapter=joint.adapter,
            catalog=joint.catalog,
            table=joint.table,
            fused_group_id=joint.fused_group_id,
            upstream=list(joint.upstream),
            output_schema=schema,
            sql_original=joint.sql,
            sql_translated=joint.sql_translated,
            sql_resolved=joint.sql_resolved,
            write_strategy=joint.write_strategy,
            tags=list(joint.tags),
            description=joint.description,
            checks=[f"{c.type}({c.severity}): {c.config}" for c in joint.checks],
            optimizations=[f"{o.rule}: {o.status} — {o.detail}" for o in joint.optimizations],
            schema_confidence=getattr(joint, "schema_confidence", "none"),
            source_stats=stats,
        )

    # ------------------------------------------------------------------
    # Text rendering
    # ------------------------------------------------------------------

    def _render_text(self, inspection: AssemblyInspection, *, ansi: bool, compilation_stats=None) -> str:  # type: ignore[no-untyped-def]
        sections: list[str] = []
        sections.append(self._render_overview_text(inspection.overview, ansi=ansi, compilation_stats=compilation_stats))

        if inspection.filter_applied is not None:
            parts = []
            f = inspection.filter_applied
            if f.engine is not None:
                parts.append(f"engine={f.engine}")
            if f.tag is not None:
                parts.append(f"tag={f.tag}")
            if f.joint_type is not None:
                parts.append(f"type={f.joint_type}")
            sections.append(self._section_header("Filter", ansi=ansi) + "  " + ", ".join(parts))

        if inspection.execution_order is not None:
            sections.append(self._render_execution_order_text(inspection.execution_order, ansi=ansi))

        if inspection.fused_groups is not None:
            sections.append(self._render_fused_groups_text(inspection.fused_groups, ansi=ansi))

        if inspection.materializations is not None:
            sections.append(self._render_materializations_text(inspection.materializations, ansi=ansi))

        if inspection.dag is not None:
            sections.append(self._render_dag_text(inspection.dag, ansi=ansi))

        if inspection.joint_details is not None:
            for jd in inspection.joint_details:
                sections.append(self._render_joint_text(jd, ansi=ansi))

        sep = "\n" + ("─" * 60) + "\n"
        return sep.join(sections)

    def _section_header(self, title: str, *, ansi: bool) -> str:
        if ansi:
            return f"{_BOLD}{_CYAN}═══ {title} ═══{_RESET}\n"
        return f"═══ {title} ═══\n"

    def _render_overview_text(self, ov: OverviewSection, *, ansi: bool, compilation_stats=None) -> str:  # type: ignore[no-untyped-def]
        lines: list[str] = [self._section_header("Assembly Overview", ansi=ansi)]
        lines.append(f"  Profile: {ov.profile_name}")
        status = "✓ Success" if ov.success else "✗ Failed"
        if ansi:
            status = f"{_GREEN}{status}{_RESET}" if ov.success else f"{_RED}{status}{_RESET}"
        lines.append(f"  Status:  {status}")
        lines.append(f"  Joints:  {ov.total_joints} total")
        for jtype, cnt in sorted(ov.joint_counts.items()):
            icon = _TYPE_ICONS.get(jtype, "")
            lines.append(f"    {icon} {jtype}: {cnt}")
        lines.append(f"  Fused Groups:      {ov.fused_group_count}")
        lines.append(f"  Materializations:  {ov.materialization_count}")

        if compilation_stats is not None:
            cs = compilation_stats
            lines.append(f"  Compilation:       {cs.compile_duration_ms}ms, {cs.joints_with_schema}/{cs.joints_total} schemas")
            lines.append(f"  Introspection:     {cs.introspection_succeeded} ok, {cs.introspection_failed} failed, {cs.introspection_skipped} skipped")

        if ov.engines:
            lines.append("  Engines:")
            for e in ov.engines:
                lines.append(f"    {e.name} ({e.engine_type}): {e.joint_count} joints")
        if ov.catalogs:
            lines.append("  Catalogs:")
            for c in ov.catalogs:
                lines.append(f"    {c.name} ({c.type})")
        if ov.adapters:
            lines.append("  Adapters:")
            for a in ov.adapters:
                lines.append(f"    {a.engine_type} ↔ {a.catalog_type} ({a.source})")
        if ov.warnings:
            label = "Warnings:" if not ansi else f"{_YELLOW}Warnings:{_RESET}"
            lines.append(f"  {label}")
            for w in ov.warnings:
                lines.append(f"    ⚠ {w}")
        if ov.errors:
            label = "Errors:" if not ansi else f"{_RED}Errors:{_RESET}"
            lines.append(f"  {label}")
            for e in ov.errors:  # type: ignore[assignment]
                lines.append(f"    ✗ {e}")
        return "\n".join(lines)

    def _render_execution_order_text(
        self, eo: ExecutionOrderSection, *, ansi: bool
    ) -> str:
        lines: list[str] = [self._section_header("Execution Order", ansi=ansi)]

        # Group steps by wave_number for parallel wave display
        waves: dict[int, list[ExecutionStep]] = {}
        for step in eo.steps:
            waves.setdefault(step.wave_number, []).append(step)

        has_waves = any(w != 0 for w in waves)

        if has_waves:
            for wave_num in sorted(waves):
                if wave_num == 0:
                    # Fallback steps without wave assignment
                    for step in waves[wave_num]:
                        lines.append(self._format_step_line(step, ansi=ansi))
                else:
                    wave_label = f"  Wave {wave_num}:"
                    if ansi:
                        wave_label = f"  {_BOLD}Wave {wave_num}:{_RESET}"
                    lines.append(wave_label)
                    for step in waves[wave_num]:
                        lines.append(self._format_step_line(step, ansi=ansi, indent=4))
        else:
            for step in eo.steps:
                lines.append(self._format_step_line(step, ansi=ansi))

        return "\n".join(lines)
    def _format_step_line(self, step: ExecutionStep, *, ansi: bool, indent: int = 2) -> str:
        """Format a single execution step as a text line."""
        prefix = " " * indent
        fused_marker = " [fused]" if step.is_fused else ""
        mat_marker = " ⚡" if step.has_materialization else ""
        step_label = f"{prefix}{step.step_number}. {step.id}"
        if ansi and step.is_fused:
            step_label = f"{prefix}{step.step_number}. {_BOLD}{step.id}{_RESET}"
        line = f"{step_label} ({step.engine}){fused_marker}{mat_marker}"
        if len(step.joints) > 1 or (len(step.joints) == 1 and step.joints[0] != step.id):
            line += f"\n{prefix}   joints: {', '.join(step.joints)}"
        return line

    def _render_fused_groups_text(
        self, fg: FusedGroupsSection, *, ansi: bool
    ) -> str:
        lines: list[str] = [self._section_header("Fused Groups", ansi=ansi)]
        for g in fg.groups:
            header = f"  {g.id} ({g.engine}, {g.engine_type}, {g.fusion_strategy})"
            if ansi:
                header = f"  {_BOLD}{g.id}{_RESET} ({g.engine}, {g.engine_type}, {g.fusion_strategy})"
            lines.append(header)
            lines.append(f"    joints: {', '.join(g.joints)}")
            lines.append(f"    entry:  {', '.join(g.entry_joints)}")
            lines.append(f"    exit:   {', '.join(g.exit_joints)}")
            if g.fused_sql:
                lines.append("    fused SQL:")
                lines.append(self._indent_sql(g.fused_sql, 6, ansi=ansi))
            if g.resolved_sql:
                lines.append("    resolved SQL:")
                lines.append(self._indent_sql(g.resolved_sql, 6, ansi=ansi))
            if g.pushdown_predicates:
                lines.append("    pushdown:")
                for p in g.pushdown_predicates:
                    lines.append(f"      {p}")
            if g.residual_operations:
                lines.append("    residual:")
                for r in g.residual_operations:
                    lines.append(f"      {r}")
        return "\n".join(lines)

    def _render_materializations_text(
        self, ms: MaterializationsSection, *, ansi: bool
    ) -> str:
        lines: list[str] = [self._section_header("Materializations", ansi=ansi)]
        for trigger, details in ms.by_trigger.items():
            trigger_label = trigger
            if ansi:
                trigger_label = f"{_YELLOW}{trigger}{_RESET}"
            lines.append(f"  {trigger_label}:")
            for d in details:
                lines.append(f"    {d.from_joint} → {d.to_joint} ({d.strategy})")
                if d.detail:
                    lines.append(f"      {d.detail}")
        return "\n".join(lines)

    def _render_dag_text(self, dag: DagSection, *, ansi: bool) -> str:
        lines: list[str] = [self._section_header("DAG", ansi=ansi)]
        lines.append(dag.rendered_text)
        return "\n".join(lines)

    def _render_joint_text(self, ji: JointInspection, *, ansi: bool) -> str:
        icon = _TYPE_ICONS.get(ji.type, "")
        header = f"{icon} {ji.name}"
        if ansi:
            header = f"{icon} {_BOLD}{ji.name}{_RESET}"
        lines: list[str] = [self._section_header(f"Joint: {ji.name}", ansi=ansi)]
        lines.append(f"  {header}")
        lines.append(f"  Type:              {ji.type}")
        lines.append(f"  Engine:            {ji.engine} ({ji.engine_resolution})")
        if ji.source_file:
            lines.append(f"  Source File:       {ji.source_file}")
        if ji.adapter:
            lines.append(f"  Adapter:           {ji.adapter}")
        if ji.catalog:
            lines.append(f"  Catalog:           {ji.catalog}")
        if ji.table:
            lines.append(f"  Table:             {ji.table}")
        if ji.fused_group_id:
            lines.append(f"  Fused Group:       {ji.fused_group_id}")
        if ji.upstream:
            lines.append(f"  Upstream:          {', '.join(ji.upstream)}")
        if ji.write_strategy:
            lines.append(f"  Write Strategy:    {ji.write_strategy}")
        if ji.tags:
            lines.append(f"  Tags:              {', '.join(ji.tags)}")
        if ji.description:
            lines.append(f"  Description:       {ji.description}")
        # Schema confidence always shown
        confidence = ji.schema_confidence
        conf_label = f"  Schema Confidence: {confidence}"
        if ansi:
            color = _GREEN if confidence in ("introspected", "inferred") else _DIM
            conf_label = f"  Schema Confidence: {color}{confidence}{_RESET}"
        lines.append(conf_label)
        # Schema gated by confidence
        if ji.output_schema and confidence in ("introspected", "inferred"):
            lines.append("  Schema:")
            for f in ji.output_schema:
                lines.append(f"    {f.name}: {f.type}")
        # Source stats (verbose detail for source joints)
        if ji.source_stats is not None:
            lines.append("  Source Stats:")
            ss = ji.source_stats
            if ss.row_count is not None:
                lines.append(f"    Row Count:       {ss.row_count:,}")
            if ss.size_bytes is not None:
                lines.append(f"    Size:            {ss.size_bytes:,} bytes")
            if ss.last_modified is not None:
                lines.append(f"    Last Modified:   {ss.last_modified.isoformat()}")
            if ss.partition_count is not None:
                lines.append(f"    Partitions:      {ss.partition_count}")
        if ji.sql_original:
            lines.append("  SQL (original):")
            lines.append(self._indent_sql(ji.sql_original, 4, ansi=ansi))
        if ji.sql_translated:
            lines.append("  SQL (translated):")
            lines.append(self._indent_sql(ji.sql_translated, 4, ansi=ansi))
        if ji.sql_resolved:
            lines.append("  SQL (resolved):")
            lines.append(self._indent_sql(ji.sql_resolved, 4, ansi=ansi))
        if ji.checks:
            lines.append("  Checks:")
            for c in ji.checks:
                lines.append(f"    {c}")
        if ji.optimizations:
            lines.append("  Optimizations:")
            for o in ji.optimizations:
                lines.append(f"    {o}")
        return "\n".join(lines)

    def _indent_sql(self, sql: str, indent: int, *, ansi: bool) -> str:
        pad = " " * indent
        if ansi:
            highlighted = _SQL_KEYWORDS.sub(lambda m: f"{_BLUE}{m.group(0)}{_RESET}", sql)
            return "\n".join(f"{pad}{line}" for line in highlighted.splitlines())
        return "\n".join(f"{pad}{line}" for line in sql.splitlines())
