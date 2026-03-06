"""Mermaid format renderer for compile output."""

from __future__ import annotations

from rivet_core.compiler import CompiledAssembly, CompiledJoint


def _sanitize_id(name: str) -> str:
    """Sanitize a name for use as a Mermaid node ID."""
    return name.replace("-", "_").replace(".", "_").replace(" ", "_")


def _joint_label(j: CompiledJoint) -> str:
    """Build a node label with name, type, catalog, and optimization summary."""
    parts = [j.name, f"type: {j.type}"]
    if j.catalog:
        parts.append(f"catalog: {j.catalog}")
    applied = sum(1 for o in j.optimizations if o.status == "applied")
    if applied:
        parts.append(f"optimizations: {applied}")
    return "<br/>".join(parts)


def render_mermaid(compiled: CompiledAssembly) -> str:
    """Render CompiledAssembly as Mermaid graph definition."""
    lines = ["graph TD"]

    joint_map = {j.name: j for j in compiled.joints}
    grouped: set[str] = set()

    for fg in compiled.fused_groups:
        sid = _sanitize_id(fg.id)
        lines.append(f"    subgraph {sid}[{fg.engine}]")
        for jname in fg.joints:
            j = joint_map.get(jname)
            nid = _sanitize_id(jname)
            label = _joint_label(j) if j else jname
            lines.append(f"        {nid}[\"{label}\"]")
            grouped.add(jname)
        lines.append("    end")

    for j in compiled.joints:
        if j.name not in grouped:
            nid = _sanitize_id(j.name)
            lines.append(f"    {nid}[\"{_joint_label(j)}\"]")

    for j in compiled.joints:
        nid = _sanitize_id(j.name)
        for up in j.upstream:
            uid = _sanitize_id(up)
            lines.append(f"    {uid} --> {nid}")

    for m in compiled.materializations:
        fid = _sanitize_id(m.from_joint)
        tid = _sanitize_id(m.to_joint)
        lines.append(f"    {fid} -->|⚡ materialize| {tid}")

    return "\n".join(lines)
