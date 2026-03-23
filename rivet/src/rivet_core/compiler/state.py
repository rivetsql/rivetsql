"""Pipeline state, compile options, and compiler phase protocol.

This is a Level 1 module — it must NOT import from ``helpers/``, ``phases/``,
or ``pipeline.py`` within the compiler package.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from rivet_core.assembly import Assembly
from rivet_core.compiler.models import (
    AdapterDecision,
    CompiledAssembly,
    CompiledJoint,
    EngineBoundary,
    FusionStrategyName,
    IntrospectionRecord,
    Materialization,
    MaterializationStrategyName,
    PluginAnnotation,
    ResolvedJointMetadata,
    TagMode,
)
from rivet_core.errors import RivetError
from rivet_core.models import Catalog, ComputeEngine, Schema
from rivet_core.optimizer import FusedGroup
from rivet_core.plugins import PluginRegistry, ReferenceResolver


@dataclass(frozen=True)
class CompileOptions:
    profile_name: str = "default"
    target_sink: str | None = None
    tags: list[str] | None = None
    tag_mode: TagMode = "or"
    default_fusion_strategy: FusionStrategyName = "cte"
    default_materialization_strategy: MaterializationStrategyName = "arrow"
    resolve_references: ReferenceResolver | None = None
    default_engine: str | None = None
    introspect: bool = True
    introspect_timeout: float = 5.0
    project_root: Path | None = None


@dataclass(frozen=True)
class PhaseState:
    """Immutable accumulator threaded through all 10 compiler phases.

    ``PhaseState`` is a frozen dataclass that serves as both the input and
    output of every compiler phase.  Each phase reads the fields populated by
    prior phases and returns a *new* ``PhaseState`` (via ``replace()``) with
    its own fields set.  Fields belonging to phases that have not yet executed
    remain ``None``.

    The ``completed_phases`` tuple tracks which phases have run, and
    ``phase_timings`` records wall-clock milliseconds per phase.  Errors and
    warnings are accumulated as immutable tuples so that no phase can
    accidentally discard diagnostics from an earlier phase.

    Use ``compile_until()`` to obtain a ``PhaseState`` at any intermediate
    point in the pipeline.
    """

    # ── Inputs (set once at pipeline start) ──
    assembly: Assembly
    catalogs: list[Catalog]
    engines: list[ComputeEngine]
    registry: PluginRegistry
    options: CompileOptions
    catalog_map: dict[str, Catalog]
    engine_map: dict[str, ComputeEngine]

    # ── Phase 1: DAG Pruning ──
    pruned: Assembly | None = None

    # ── Phase 2: Metadata Resolution ──
    topo_order: list[str] | None = None
    joint_metadata: dict[str, ResolvedJointMetadata] | None = None
    adapter_decisions: list[AdapterDecision] | None = None

    # ── Phase 3: Source Introspection ──
    introspection_results: dict[str, IntrospectionRecord] | None = None

    # ── Phase 4: SQL Compilation ──
    compiled_joints: list[CompiledJoint] | None = None
    cj_map: dict[str, CompiledJoint] | None = None
    upstream_schemas: dict[str, Schema] | None = None

    # ── Phase 5: Fusion ──
    fused_groups: list[FusedGroup] | None = None
    joint_to_group: dict[str, str] | None = None

    # ── Phase 6–7: Optimization & Strategy Resolution ──
    # fused_groups / cj_map updated via replace()

    # ── Phase 8: Engine Boundaries ──
    engine_boundaries: list[EngineBoundary] | None = None

    # ── Phase 9: Materialization ──
    materializations: list[Materialization] | None = None

    # ── Phase 10: Finalization ──
    compiled_assembly: CompiledAssembly | None = None

    # ── Subgraph poisoning ──
    poisoned_joints: frozenset[str] = frozenset()

    # ── Accumulated diagnostics ──
    errors: tuple[RivetError, ...] = ()
    warnings: tuple[str, ...] = ()
    phase_timings: dict[str, float] = field(default_factory=dict)
    plugin_annotations: list[PluginAnnotation] = field(default_factory=list)

    # ── Introspection stats ──
    introspection_attempted: int = 0
    introspection_succeeded: int = 0
    introspection_failed: int = 0
    introspection_skipped: int = 0

    # ── Completed phase tracking ──
    completed_phases: tuple[str, ...] = ()


class CompilerPhase(Protocol):
    """Protocol that every compiler phase must satisfy.

    A phase is a callable that accepts a ``PhaseState`` and returns a new
    ``PhaseState`` with its own fields populated.  Phases must be pure
    functions: same input produces same output, with no shared mutable state.

    The ``name`` property returns the phase constant (e.g. ``PHASE_FUSION``)
    used for timing, logging, and ``compile_until()`` matching.
    """

    @property
    def name(self) -> str: ...

    def __call__(self, state: PhaseState) -> PhaseState: ...
