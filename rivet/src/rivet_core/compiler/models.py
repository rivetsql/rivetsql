"""Compilation data models, type aliases, and phase constants.

All compilation dataclasses, type aliases, and phase name constants live here.
This is a Level 1 module — it must NOT import from ``helpers/``, ``phases/``,
or ``pipeline.py`` within the compiler package.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, TypeAlias

from rivet_core.checks import CompiledCheck
from rivet_core.errors import RivetError
from rivet_core.lineage import ColumnLineage
from rivet_core.models import Catalog, ComputeEngine, Joint, JointType, Schema
from rivet_core.optimizer import FusedGroup
from rivet_core.plugins import CatalogPlugin, PluginRegistry
from rivet_core.sql_parser import LogicalPlan, SQLParser

# ---------------------------------------------------------------------------
# Data models (task 12.1)
# ---------------------------------------------------------------------------

OptimizationStatus: TypeAlias = Literal["applied", "not_applicable", "capability_gap", "skipped"]
MaterializationTrigger: TypeAlias = Literal[
    "eager",
    "engine_instance_change",
    "capability_gap",
    "python_boundary",
    "assertion_boundary",
    "multi_consumer",
    "checkpoint_boundary",
]
MaterializationStrategyName: TypeAlias = Literal["arrow", "temp_table"]
FusionStrategyName: TypeAlias = Literal["cte", "temp_view"]
SchemaConfidence: TypeAlias = Literal["introspected", "inferred", "partial", "none"]
EngineResolutionSource: TypeAlias = Literal[
    "joint_override", "catalog_default", "project_default", ""
]
TagMode: TypeAlias = Literal["or", "and"]

logger = logging.getLogger("rivet_core.compiler")

# ---------------------------------------------------------------------------
# Phase name constants
#
# Each constant identifies one of the 10 sequential phases in the compilation
# pipeline.  Use these with ``compile_until(stop_after=...)`` to obtain the
# intermediate ``PhaseState`` at any point in the pipeline.
# ---------------------------------------------------------------------------

PHASE_PRUNE_DAG = "prune_dag"
"""Phase 1 — prune the Assembly DAG to the target sink / tags."""

PHASE_RESOLVE_METADATA = "resolve_metadata"
"""Phase 2 — resolve engine, catalog, and adapter metadata per joint."""

PHASE_INTROSPECT_SOURCES = "introspect_sources"
"""Phase 3 — introspect source schemas via catalog plugins."""

PHASE_COMPILE_SQL = "compile_sql"
"""Phase 4 — parse and compile SQL/Python for every joint."""

PHASE_FUSION = "fusion"
"""Phase 5 — fuse compatible joints into ``FusedGroup`` instances."""

PHASE_OPTIMIZATION = "optimization"
"""Phase 6 — apply pushdown and cross-group optimization passes."""

PHASE_STRATEGY_RESOLUTION = "strategy_resolution"
"""Phase 7 — resolve execution strategies and reference resolution."""

PHASE_ENGINE_BOUNDARIES = "engine_boundaries"
"""Phase 8 — detect cross-engine boundaries and resolve adapters."""

PHASE_MATERIALIZATION = "materialization"
"""Phase 9 — determine which groups require materialization."""

PHASE_FINALIZATION = "finalization"
"""Phase 10 — build the final ``CompiledAssembly`` with execution plan."""


@dataclass(frozen=True)
class SourceStats:
    """Cheap table-level metadata from catalog introspection.

    All fields optional — catalogs report what they can.
    """

    row_count: int | None = None
    size_bytes: int | None = None
    last_modified: datetime | None = None
    partition_count: int | None = None


@dataclass(frozen=True)
class EngineBoundary:
    """Records an engine type change between adjacent fused groups."""

    producer_group_id: str
    consumer_group_id: str
    producer_engine_type: str
    consumer_engine_type: str
    boundary_joints: list[str]
    adapter_strategy: str | None = None


@dataclass(frozen=True)
class OptimizationResult:
    rule: str
    status: OptimizationStatus
    detail: str
    pushed: str | None = None
    residual: str | None = None
    target_joint: str | None = None
    target_group: str | None = None


@dataclass(frozen=True)
class Materialization:
    from_joint: str
    to_joint: str
    trigger: MaterializationTrigger
    detail: str
    strategy: MaterializationStrategyName


@dataclass(frozen=True)
class CompiledCatalog:
    name: str
    type: str
    options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CompiledEngine:
    name: str
    engine_type: str
    native_catalog_types: list[str]


@dataclass(frozen=True)
class CompiledAdapter:
    engine_type: str
    catalog_type: str
    source: str


@dataclass(frozen=True)
class CompiledJoint:
    """A compiled joint with all metadata resolved.

    Attributes:
        name: Joint name
        type: Joint type ("source", "sql", "sink", "python", "checkpoint")
        catalog: Catalog name if applicable
        catalog_type: Type of catalog plugin
        engine: Engine name for execution
        engine_resolution: How the engine was resolved
        adapter: Adapter name if applicable
        sql: Original user-written SQL
        sql_translated: SQL after dialect translation
        sql_resolved: SQL with catalog-qualified references
        sql_dialect: Dialect of the original SQL
        engine_dialect: Target engine's SQL dialect
        upstream: List of upstream joint names
        eager: Whether joint should be eagerly materialized
        table: Target table name for sinks
        write_strategy: Write strategy for sinks
        function: Python function name for python joints
        source_file: Path to source file for python joints
        logical_plan: Logical plan for the joint
        output_schema: Output schema if known
        column_lineage: Column lineage information
        optimizations: List of applied optimizations
        checks: List of compiled checks
        fused_group_id: ID of the fused group this joint belongs to
        tags: User-defined tags
        description: User-defined description
        fusion_strategy_override: Override for fusion strategy
        materialization_strategy_override: Override for materialization strategy
        source_stats: Statistics about the source data
        schema_confidence: Confidence level of schema inference
        execution_sql: Final SQL that will be executed on the engine after all
            optimizations and transformations. None for non-SQL joints or when
            SQL resolution is not applicable.
    """

    name: str
    type: JointType
    catalog: str | None
    catalog_type: str | None
    engine: str
    engine_resolution: EngineResolutionSource | None
    adapter: str | None
    sql: str | None
    sql_translated: str | None
    sql_resolved: str | None
    sql_dialect: str | None
    engine_dialect: str | None
    upstream: list[str]
    eager: bool
    table: str | None
    write_strategy: str | None
    function: str | None
    source_file: str | None
    logical_plan: LogicalPlan | None
    output_schema: Schema | None
    column_lineage: list[ColumnLineage]
    optimizations: list[OptimizationResult]
    checks: list[CompiledCheck]
    fused_group_id: str | None
    tags: list[str]
    description: str | None
    fusion_strategy_override: str | None
    materialization_strategy_override: str | None
    source_stats: SourceStats | None = None
    schema_confidence: SchemaConfidence = "none"
    execution_sql: str | None = None


@dataclass(frozen=True)
class CompilationStats:
    """Metrics about the compilation process itself."""

    compile_duration_ms: int
    joints_with_schema: int
    joints_total: int
    introspection_attempted: int
    introspection_succeeded: int
    introspection_failed: int
    introspection_skipped: int
    phase_durations_ms: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class CompilationWarning:
    """Structured warning metadata for compiler diagnostics."""

    message: str
    code: str | None = None
    context: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CompilationDiagnostics:
    """Structured diagnostics emitted during compilation."""

    errors: list[RivetError] = field(default_factory=list)
    warnings: list[CompilationWarning] = field(default_factory=list)
    stats: CompilationStats | None = None


@dataclass(frozen=True)
class ExecutionWave:
    """A set of fused groups that can execute concurrently."""

    wave_number: int
    groups: list[str]  # fused group IDs
    engines: dict[str, list[str]]  # engine_name → group_ids on that engine


@dataclass(frozen=True)
class AdapterDecision:
    """Traceability record explaining why a specific adapter was selected.

    One ``AdapterDecision`` is produced for every adapter lookup during
    metadata resolution (Phase 2) and engine-boundary detection (Phase 8).

    Attributes:
        joint_name: Name of the joint the lookup was performed for.
        engine_type: Engine type used in the lookup key.
        catalog_type: Catalog type used in the lookup key, or ``None``.
        adapter_found: Identifier of the adapter that was selected
            (e.g. ``"duckdb:filesystem"``), or ``None`` if no adapter matched.
        resolution_method: How the adapter was resolved — one of
            ``"exact_match"``, ``"wildcard_fallback"``, or ``"none"``.
        available_for_engine: Adapter identifiers registered for *engine_type*.
        available_for_catalog: Adapter identifiers registered for *catalog_type*.
        is_cross_joint: ``True`` when this decision was made for a
            cross-engine boundary (Phase 8).
        producer_engine_type: Engine type of the upstream group when
            *is_cross_joint* is ``True``.
        consumer_engine_type: Engine type of the downstream group when
            *is_cross_joint* is ``True``.
    """

    joint_name: str
    engine_type: str
    catalog_type: str | None
    adapter_found: str | None
    resolution_method: str  # "exact_match", "wildcard_fallback", "none"
    available_for_engine: list[str]
    available_for_catalog: list[str]
    is_cross_joint: bool = False
    producer_engine_type: str | None = None
    consumer_engine_type: str | None = None


@dataclass(frozen=True)
class IntrospectionRecord:
    """Per-source introspection trace produced during Phase 3.

    Each source joint that is a candidate for introspection gets exactly one
    ``IntrospectionRecord``, regardless of whether introspection succeeded.

    Attributes:
        joint_name: Name of the source joint.
        catalog_type: Catalog type backing this source, or ``None``.
        catalog_plugin_class: Fully-qualified class name of the catalog plugin
            used (e.g. ``"FilesystemCatalogPlugin"``), or ``None``.
        result: Outcome — one of ``"success"``, ``"failed"``, ``"timeout"``,
            or ``"skipped"``.
        duration_ms: Wall-clock time spent on this introspection call.
        schema_obtained: Whether a schema was successfully retrieved.
        stats_obtained: Whether table statistics were retrieved.
        error_message: Error details when *result* is ``"failed"`` or
            ``"timeout"``, otherwise ``None``.
    """

    joint_name: str
    catalog_type: str | None
    catalog_plugin_class: str | None
    result: str  # "success", "failed", "timeout", "skipped"
    duration_ms: float
    schema_obtained: bool
    stats_obtained: bool
    error_message: str | None = None


@dataclass(frozen=True)
class PluginAnnotation:
    """Marks a core/plugin boundary crossing during compilation.

    Every call from the compiler core into a plugin (catalog, engine, adapter,
    or reference resolver) is recorded as a ``PluginAnnotation``, making the
    boundary between core logic and plugin-contributed behaviour explicit.

    Attributes:
        phase: Phase constant (e.g. ``PHASE_RESOLVE_METADATA``) in which the
            plugin was invoked.
        joint_name: Joint the invocation relates to, or ``None`` for
            pipeline-wide operations.
        plugin_type: Category of plugin — one of ``"catalog_plugin"``,
            ``"engine_plugin"``, ``"adapter"``, or ``"reference_resolver"``.
        plugin_class: Fully-qualified class name of the plugin instance.
        operation: Method or operation invoked (e.g. ``"get_schema"``,
            ``"resolve_references"``).
        result: Outcome — one of ``"success"``, ``"failed"``, or
            ``"not_applicable"``.
        detail: Optional free-form detail (e.g. before/after SQL for
            reference resolution).
    """

    phase: str
    joint_name: str | None
    plugin_type: str  # "catalog_plugin", "engine_plugin", "adapter", "reference_resolver"
    plugin_class: str
    operation: str
    result: str  # "success", "failed", "not_applicable"
    detail: str | None = None


@dataclass(frozen=True)
class SourceSQLAnalysis:
    joint: Joint
    logical_plan: LogicalPlan | None
    parsed_ast: Any | None


@dataclass
class CompilationContext:
    catalog_map: dict[str, Catalog]
    engine_map: dict[str, ComputeEngine]
    registry: PluginRegistry
    default_engine: str | None
    parser: SQLParser
    upstream_schemas: dict[str, Schema]
    errors: list[RivetError]
    warnings: list[str]
    adapter_cache: dict[tuple[str, str], str | None] = field(default_factory=dict)
    project_root: Path | None = None


@dataclass(frozen=True)
class ResolvedJointMetadata:
    catalog: Catalog | None
    catalog_type: str | None
    catalog_plugin: CatalogPlugin | None
    engine_name: str
    engine_type: str
    resolution: EngineResolutionSource | None
    adapter_name: str | None


@dataclass(frozen=True)
class CompiledAssembly:
    success: bool
    profile_name: str
    catalogs: list[CompiledCatalog]
    engines: list[CompiledEngine]
    adapters: list[CompiledAdapter]
    joints: list[CompiledJoint]
    fused_groups: list[FusedGroup]
    materializations: list[Materialization]
    execution_order: list[str]  # fused group IDs in topological execution order
    diagnostics: CompilationDiagnostics = field(default_factory=CompilationDiagnostics)
    engine_boundaries: list[EngineBoundary] = field(default_factory=list)
    parallel_execution_plan: list[ExecutionWave] = field(default_factory=list)
    adapter_decisions: list[AdapterDecision] = field(default_factory=list)
    introspection_records: list[IntrospectionRecord] = field(default_factory=list)
    plugin_annotations: list[PluginAnnotation] = field(default_factory=list)
