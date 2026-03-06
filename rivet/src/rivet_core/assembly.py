"""Assembly (DAG) construction and validation.

An Assembly is a Directed Acyclic Graph of Joints defining pipeline shape.
It validates structural integrity at construction time: unique names, acyclicity,
upstream reference existence, and upstream constraints per joint type.
"""

from __future__ import annotations

from rivet_core.errors import RivetError
from rivet_core.models import Joint


class AssemblyError(Exception):
    """Raised when Assembly construction fails due to structural violations."""

    def __init__(self, error: RivetError) -> None:
        self.error = error
        super().__init__(str(error))


class Assembly:
    """Directed Acyclic Graph of Joints defining pipeline shape.

    Validates at construction:
      - Joint names are globally unique
      - All upstream references exist
      - Upstream constraints per joint type are satisfied
      - The graph is acyclic
    """

    def __init__(self, joints: list[Joint]) -> None:
        self.joints: dict[str, Joint] = {}
        self.edges: dict[str, list[str]] = {}  # joint → downstream joints

        # Enforce unique names
        for joint in joints:
            if joint.name in self.joints:
                raise AssemblyError(
                    RivetError(
                        code="RVT-301",
                        message=f"Duplicate joint name: '{joint.name}'.",
                        context={"joint": joint.name},
                        remediation="Ensure all joint names are unique within the assembly.",
                    )
                )
            self.joints[joint.name] = joint
            self.edges[joint.name] = []

        # Validate upstream references and constraints
        for joint in joints:
            for up in joint.upstream:
                if up not in self.joints:
                    raise AssemblyError(
                        RivetError(
                            code="RVT-302",
                            message=f"Joint '{joint.name}' references unknown upstream '{up}'.",
                            context={"joint": joint.name, "upstream": up},
                            remediation=f"Add a joint named '{up}' to the assembly or fix the upstream reference.",
                        )
                    )
                self.edges[up].append(joint.name)

            if joint.joint_type == "source" and joint.upstream:
                raise AssemblyError(
                    RivetError(
                        code="RVT-303",
                        message=f"Source joint '{joint.name}' must not have upstream joints, but has: {joint.upstream}.",
                        context={"joint": joint.name, "upstream": joint.upstream},
                        remediation="Remove upstream references from the source joint.",
                    )
                )
            if joint.joint_type == "sink" and not joint.upstream:
                raise AssemblyError(
                    RivetError(
                        code="RVT-304",
                        message=f"Sink joint '{joint.name}' must have at least one upstream joint.",
                        context={"joint": joint.name},
                        remediation="Add at least one upstream reference to the sink joint.",
                    )
                )

        # Cycle detection via DFS
        self._detect_cycles()

    def _detect_cycles(self) -> None:
        WHITE, GRAY, BLACK = 0, 1, 2
        color: dict[str, int] = {name: WHITE for name in self.joints}
        parent: dict[str, str | None] = {name: None for name in self.joints}

        def dfs(node: str) -> list[str] | None:
            color[node] = GRAY
            for neighbor in self.edges[node]:
                if color[neighbor] == GRAY:
                    # Reconstruct cycle
                    cycle = [neighbor, node]
                    cur = node
                    while cur != neighbor:
                        cur = parent[cur]  # type: ignore[assignment]
                        if cur is None:
                            break
                        cycle.append(cur)
                    cycle.reverse()
                    return cycle
                if color[neighbor] == WHITE:
                    parent[neighbor] = node
                    result = dfs(neighbor)
                    if result is not None:
                        return result
            color[node] = BLACK
            return None

        for name in sorted(self.joints):
            if color[name] == WHITE:
                cycle = dfs(name)
                if cycle is not None:
                    cycle_str = " -> ".join(cycle)
                    raise AssemblyError(
                        RivetError(
                            code="RVT-305",
                            message=f"Cycle detected in assembly: {cycle_str}.",
                            context={"cycle": cycle},
                            remediation="Remove or redirect edges to break the cycle.",
                        )
                    )

    def topological_order(self) -> list[str]:
        """Return a deterministic topological sort of joint names.

        Joints are ordered so that every joint appears after all its upstreams.
        Determinism is achieved by sorting candidates alphabetically at each step.
        """
        in_degree: dict[str, int] = {name: 0 for name in self.joints}
        for joint in self.joints.values():
            for _up in joint.upstream:
                in_degree[joint.name] += 1  # noqa: SIM113

        queue = sorted(name for name, deg in in_degree.items() if deg == 0)
        result: list[str] = []

        while queue:
            node = queue.pop(0)
            result.append(node)
            for downstream in sorted(self.edges[node]):
                in_degree[downstream] -= 1
                if in_degree[downstream] == 0:
                    # Insert in sorted position
                    lo, hi = 0, len(queue)
                    while lo < hi:
                        mid = (lo + hi) // 2
                        if queue[mid] < downstream:
                            lo = mid + 1
                        else:
                            hi = mid
                    queue.insert(lo, downstream)

        return result

    def subgraph(
        self,
        target_sink: str | None = None,
        tags: list[str] | None = None,
        tag_mode: str = "or",
    ) -> Assembly:
        """Return a pruned Assembly based on target_sink and/or tag filtering.

        - target_sink: include only joints reachable upstream from this sink
        - tags: include joints matching the tag filter (plus their upstream deps)
        - tag_mode: "or" (match ANY tag) or "and" (match ALL tags)
        - When both are specified, compute the intersection plus upstream deps
        """
        selected: set[str] | None = None

        # Step 1: target_sink pruning — walk upstream from sink
        if target_sink is not None:
            if target_sink not in self.joints:
                raise AssemblyError(
                    RivetError(
                        code="RVT-306",
                        message=f"Target sink '{target_sink}' not found in assembly.",
                        context={"target_sink": target_sink},
                        remediation="Specify a valid joint name as target_sink.",
                    )
                )
            selected = self._upstream_closure({target_sink})

        # Step 2: tag filtering
        if tags:
            tag_set = set(tags)
            tag_matched: set[str] = set()
            for name, joint in self.joints.items():
                if selected is not None and name not in selected:
                    continue
                joint_tags = set(joint.tags)
                if tag_mode == "and":
                    if tag_set.issubset(joint_tags):
                        tag_matched.add(name)
                else:  # "or"
                    if tag_set & joint_tags:
                        tag_matched.add(name)
            # Include upstream dependencies of tag-matched joints
            selected = self._upstream_closure(tag_matched)
        elif selected is None:
            # No filtering at all
            return self

        assert selected is not None
        filtered_joints = [self.joints[name] for name in self.topological_order() if name in selected]
        return Assembly(filtered_joints)

    def _upstream_closure(self, seeds: set[str]) -> set[str]:
        """Return the set of all joints reachable by walking upstream from seeds."""
        visited: set[str] = set()
        stack = list(seeds)
        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            for up in self.joints[node].upstream:
                if up not in visited:
                    stack.append(up)
        return visited
