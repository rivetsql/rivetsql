"""Scheduling infrastructure for the executor.

Contains the ``DependencyGraph`` DAG, ``EngineConcurrencyPool`` async
context manager, the ``_nullcontext`` fallback, and
``_resolve_concurrency_limits``.

This is Level 3 of the executor package dependency hierarchy — it imports
from ``models`` (Level 1) only within the executor package.  It does NOT
import from ``pipeline.py`` or other phase modules.
"""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any

from rivet_core.compiler import CompiledJoint
from rivet_core.errors import ExecutionError, RivetError
from rivet_core.models import ComputeEngine
from rivet_core.optimizer import FusedGroup
from rivet_core.plugins import PluginRegistry


class DependencyGraph:
    """DAG of fused groups derived from upstream joint references."""

    _upstream: dict[str, set[str]]
    _downstream: dict[str, set[str]]
    _in_degree: dict[str, int]
    _submitted: set[str]
    _completed: set[str]

    def __init__(
        self,
        upstream: dict[str, set[str]],
        downstream: dict[str, set[str]],
        in_degree: dict[str, int],
    ) -> None:
        self._upstream = upstream
        self._downstream = downstream
        self._in_degree = in_degree
        self._submitted: set[str] = set()
        self._completed: set[str] = set()

    @staticmethod
    def build(
        fused_groups: list[FusedGroup],
        joint_map: dict[str, CompiledJoint],
    ) -> DependencyGraph:
        """Construct the graph from compiled assembly data.

        An edge A -> B exists iff any joint in B has an upstream joint
        whose output is produced by group A.
        """
        # Map each joint name to its owning fused group ID
        joint_to_group: dict[str, str] = {}
        for group in fused_groups:
            for joint_name in group.joints:
                joint_to_group[joint_name] = group.id

        upstream: dict[str, set[str]] = {g.id: set() for g in fused_groups}
        downstream: dict[str, set[str]] = {g.id: set() for g in fused_groups}

        for group in fused_groups:
            for joint_name in group.joints:
                compiled_joint = joint_map.get(joint_name)
                if compiled_joint is None:
                    continue
                for up_name in compiled_joint.upstream:
                    up_group_id = joint_to_group.get(up_name)
                    # Skip upstream refs not belonging to any group (Req 1.3)
                    if up_group_id is None:
                        continue
                    # Skip self-references (joints within the same group)
                    if up_group_id == group.id:
                        continue
                    upstream[group.id].add(up_group_id)
                    downstream[up_group_id].add(group.id)

        in_degree: dict[str, int] = {gid: len(ups) for gid, ups in upstream.items()}

        return DependencyGraph(
            upstream=upstream,
            downstream=downstream,
            in_degree=in_degree,
        )

    def ready_groups(self) -> list[str]:
        """Return group IDs with in-degree 0 that haven't been submitted."""
        return [
            gid for gid, deg in self._in_degree.items() if deg == 0 and gid not in self._submitted
        ]

    def mark_submitted(self, group_id: str) -> None:
        self._submitted.add(group_id)

    def is_submitted(self, group_id: str) -> bool:
        return group_id in self._submitted

    def submit_ready(self) -> list[str]:
        ready = self.ready_groups()
        for group_id in ready:
            self.mark_submitted(group_id)
        return ready

    def mark_submitted_many(self, group_ids: list[str]) -> None:
        for group_id in group_ids:
            self.mark_submitted(group_id)

    def mark_complete(self, group_id: str) -> list[str]:
        """Add to completed, decrement downstream in-degrees.

        Returns newly ready group IDs (in-degree just became 0 and not yet
        submitted).
        """
        self._completed.add(group_id)
        newly_ready: list[str] = []
        for ds_id in self._downstream.get(group_id, set()):
            self._in_degree[ds_id] -= 1
            if self._in_degree[ds_id] == 0 and ds_id not in self._submitted:
                newly_ready.append(ds_id)
        return newly_ready

    def mark_failed(self, group_id: str) -> list[str]:
        """BFS to collect all transitive downstream group IDs."""
        visited: set[str] = set()
        queue: deque[str] = deque()
        # Seed with direct downstream of the failed group
        for ds_id in self._downstream.get(group_id, set()):
            if ds_id not in visited:
                visited.add(ds_id)
                queue.append(ds_id)
        while queue:
            current = queue.popleft()
            for ds_id in self._downstream.get(current, set()):
                if ds_id not in visited:
                    visited.add(ds_id)
                    queue.append(ds_id)
        return list(visited)


class EngineConcurrencyPool:
    """Manages concurrency for a single engine instance."""

    def __init__(self, engine_name: str, concurrency_limit: int) -> None:
        self._semaphore = asyncio.Semaphore(concurrency_limit)
        self.engine_name = engine_name
        self.concurrency_limit = concurrency_limit

    async def __aenter__(self) -> EngineConcurrencyPool:
        """Acquire a slot (suspends coroutine if pool is full)."""
        await self._semaphore.acquire()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        """Release a slot."""
        self._semaphore.release()


def _resolve_concurrency_limits(
    engines: list[ComputeEngine],
    plugin_registry: PluginRegistry,
) -> dict[str, int]:
    """Resolve concurrency_limit for each engine.

    Priority: config["concurrency_limit"] (user override) > plugin.default_concurrency_limit > 1.
    Returns engine_name → concurrency_limit mapping.
    Raises ExecutionError if any resolved limit is invalid (< 1 or non-integer).
    """
    limits: dict[str, int] = {}
    for engine in engines:
        limit: Any = engine.config.get("concurrency_limit")
        if limit is None:
            plugin = plugin_registry.get_engine_plugin(engine.engine_type)
            if plugin is not None and hasattr(plugin, "default_concurrency_limit"):
                limit = plugin.default_concurrency_limit
            else:
                limit = 1
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ExecutionError(
                RivetError(
                    code="RVT-501",
                    message=(
                        f"Invalid concurrency_limit for engine '{engine.name}': "
                        f"{limit!r}. Must be a positive integer (>= 1)."
                    ),
                    context={"engine": engine.name, "concurrency_limit": limit},
                    remediation=(
                        "Set 'concurrency_limit' in the engine config to a positive integer, "
                        "or remove it to use the plugin default."
                    ),
                )
            )
        limits[engine.name] = limit
    return limits


class _nullcontext:
    """Minimal async context manager that does nothing (fallback when no pool)."""

    async def __aenter__(self) -> _nullcontext:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        pass
