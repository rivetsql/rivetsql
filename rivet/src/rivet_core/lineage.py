"""Column-level lineage data models and traversal."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rivet_core.compiler import CompiledAssembly


@dataclass(frozen=True)
class ColumnOrigin:
    joint: str
    column: str


@dataclass(frozen=True)
class ColumnLineage:
    output_column: str
    transform: str  # "source", "direct", "renamed", "expression", "aggregation",
    #                  "window", "literal", "multi_column", "opaque"
    origins: list[ColumnOrigin]
    expression: str | None


def trace_column_backward(
    compiled: CompiledAssembly,
    joint_name: str,
    column_name: str,
) -> list[ColumnOrigin]:
    """Trace an output column backward to its ultimate source origins.

    Walks the lineage chain from the given joint/column through upstream joints
    until reaching joints with no further origins (source joints or literals).
    Preserves per-joint lineage within fused groups for full chain navigation.
    """
    joints_by_name = {j.name: j for j in compiled.joints}
    results: list[ColumnOrigin] = []
    visited: set[tuple[str, str]] = set()

    stack: list[tuple[str, str]] = [(joint_name, column_name)]
    while stack:
        jname, cname = stack.pop()
        if (jname, cname) in visited:
            continue
        visited.add((jname, cname))

        joint = joints_by_name.get(jname)
        if joint is None:
            results.append(ColumnOrigin(joint=jname, column=cname))
            continue

        lineage_for_col = [
            l for l in joint.column_lineage if l.output_column == cname
        ]
        if not lineage_for_col:
            # No lineage info — this is a terminal origin
            results.append(ColumnOrigin(joint=jname, column=cname))
            continue

        lin = lineage_for_col[0]
        if not lin.origins:
            # Literal or source with no upstream origins
            results.append(ColumnOrigin(joint=jname, column=cname))
        else:
            for origin in lin.origins:
                stack.append((origin.joint, origin.column))

    return results


def trace_column_forward(
    compiled: CompiledAssembly,
    joint_name: str,
    column_name: str,
) -> list[ColumnOrigin]:
    """Find all downstream columns affected by a source column.

    Walks forward from the given joint/column through downstream joints,
    collecting every output column that depends on the specified origin.
    Preserves per-joint lineage within fused groups for full chain navigation.
    """
    joints_by_name = {j.name: j for j in compiled.joints}
    # Build downstream index: joint_name -> list of downstream joint names
    downstream: dict[str, list[str]] = {}
    for j in compiled.joints:
        for up in j.upstream:
            downstream.setdefault(up, []).append(j.name)

    results: list[ColumnOrigin] = []
    visited: set[tuple[str, str]] = set()

    stack: list[tuple[str, str]] = [(joint_name, column_name)]
    while stack:
        jname, cname = stack.pop()
        if (jname, cname) in visited:
            continue
        visited.add((jname, cname))

        for ds_name in downstream.get(jname, []):
            ds_joint = joints_by_name.get(ds_name)
            if ds_joint is None:
                continue
            for lin in ds_joint.column_lineage:
                if any(o.joint == jname and o.column == cname for o in lin.origins):
                    result = ColumnOrigin(joint=ds_name, column=lin.output_column)
                    results.append(result)
                    stack.append((ds_name, lin.output_column))

    return results
