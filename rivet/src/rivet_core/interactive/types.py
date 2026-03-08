"""Data models for the interactive service layer.

All types are frozen dataclasses — pure data, no UI imports.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Literal

import pyarrow as pa

from rivet_core.compiler import CompiledAssembly
from rivet_core.executor import CheckExecutionResult
from rivet_core.models import Joint
from rivet_core.stats import RunStats


@dataclass(frozen=True)
class QueryPlan:
    """The transient mini-pipeline built for a REPL query."""

    sources: list[Joint]
    query_joint: Joint
    sink: Joint
    resolved_references: dict[str, str]  # table_ref → "joint:X" or "catalog:Y.Z.W"
    assembly: CompiledAssembly


@dataclass(frozen=True)
class QueryResult:
    """Result of a single query execution."""

    table: pa.Table
    row_count: int
    column_names: list[str]
    column_types: list[str]  # Arrow type names
    elapsed_ms: float
    query_plan: QueryPlan | None
    quality_results: list[CheckExecutionResult] | None
    truncated: bool  # True if row limit was applied
    run_stats: RunStats | None = None


@dataclass(frozen=True)
class QueryProgress:
    """Progress update during execution."""

    joint_name: str
    status: Literal["compiling", "executing", "done", "failed"]
    current: int
    total: int
    rows: int | None
    elapsed_ms: float


@dataclass(frozen=True)
class ChangedRow:
    """A row that exists in both tables but has differing values."""

    key: dict[str, Any]
    changes: dict[str, tuple[Any, Any]]  # column → (old, new)


@dataclass(frozen=True)
class DiffResult:
    """Result of comparing two Arrow tables."""

    added: pa.Table
    removed: pa.Table
    changed: list[ChangedRow]
    unchanged_count: int
    key_columns: list[str]


@dataclass(frozen=True)
class ColumnProfile:
    """Per-column statistics."""

    name: str
    dtype: str
    null_count: int
    null_pct: float
    distinct_count: int
    min: Any | None
    max: Any | None
    mean: float | None  # numeric only
    median: float | None  # numeric only
    stddev: float | None  # numeric only
    histogram: list[int] | None  # 8-bin counts for numeric/temporal
    top_values: list[tuple[Any, int]] | None  # top-5 for string/bool


@dataclass(frozen=True)
class ProfileResult:
    """Column-level statistics for an Arrow table."""

    row_count: int
    column_count: int
    columns: list[ColumnProfile]


class Verbosity(Enum):
    """Inspection verbosity level."""

    SUMMARY = "summary"
    NORMAL = "normal"
    FULL = "full"


@dataclass(frozen=True)
class InspectFilter:
    """Filter criteria for assembly inspection. Combined with AND logic."""

    engine: str | None = None
    tag: str | None = None
    joint_type: str | None = None


# ---------------------------------------------------------------------------
# Assembly inspection types (tasks 1.2, 1.3)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SchemaField:
    """A field in an output schema."""

    name: str
    type: str


@dataclass(frozen=True)
class EngineInfo:
    name: str
    engine_type: str
    joint_count: int


@dataclass(frozen=True)
class CatalogInfo:
    name: str
    type: str


@dataclass(frozen=True)
class AdapterInfo:
    engine_type: str
    catalog_type: str
    source: str


@dataclass(frozen=True)
class OverviewSection:
    """Assembly summary."""

    profile_name: str
    joint_counts: dict[str, int]
    total_joints: int
    fused_group_count: int
    materialization_count: int
    engines: list[EngineInfo]
    catalogs: list[CatalogInfo]
    adapters: list[AdapterInfo]
    success: bool
    warnings: list[str]
    errors: list[str]


@dataclass(frozen=True)
class ExecutionStep:
    """A single step in the execution order."""

    step_number: int
    id: str
    engine: str
    joints: list[str]
    is_fused: bool
    has_materialization: bool
    wave_number: int


@dataclass(frozen=True)
class ExecutionOrderSection:
    """Ordered execution steps."""

    steps: list[ExecutionStep]


@dataclass(frozen=True)
class FusedGroupDetail:
    """Detail for a single fused group."""

    id: str
    engine: str
    engine_type: str
    fusion_strategy: str
    joints: list[str]
    entry_joints: list[str]
    exit_joints: list[str]
    adapters: dict[str, str | None]
    fused_sql: str | None
    resolved_sql: str | None
    pushdown_predicates: list[str] | None
    residual_operations: list[str] | None


@dataclass(frozen=True)
class FusedGroupsSection:
    """All fused groups in the assembly."""

    groups: list[FusedGroupDetail]


@dataclass(frozen=True)
class MaterializationDetail:
    """Detail for a single materialization boundary."""

    from_joint: str
    to_joint: str
    trigger: str
    detail: str
    strategy: str


@dataclass(frozen=True)
class MaterializationsSection:
    """All materializations grouped by trigger reason."""

    by_trigger: dict[str, list[MaterializationDetail]]


@dataclass(frozen=True)
class DagNode:
    """A node in the DAG visualization."""

    name: str
    joint_type: str
    engine: str
    fused_group_id: str | None
    icon: str


@dataclass(frozen=True)
class DagEdge:
    """An edge in the DAG visualization."""

    from_joint: str
    to_joint: str


@dataclass(frozen=True)
class DagSection:
    """Text-based DAG visualization."""

    nodes: list[DagNode]
    edges: list[DagEdge]
    rendered_text: str


@dataclass(frozen=True)
class SourceStatsInfo:
    """Source stats for display in joint inspection."""

    row_count: int | None = None
    size_bytes: int | None = None
    last_modified: datetime | None = None
    partition_count: int | None = None


@dataclass(frozen=True)
class JointInspection:
    """Detailed inspection of a single compiled joint."""

    name: str
    type: str
    source_file: str | None
    engine: str
    engine_resolution: str
    adapter: str | None
    catalog: str | None
    table: str | None
    fused_group_id: str | None
    upstream: list[str]
    output_schema: list[SchemaField] | None
    sql_original: str | None
    sql_translated: str | None
    sql_resolved: str | None
    write_strategy: str | None
    tags: list[str]
    description: str | None
    checks: list[str]
    optimizations: list[str]
    schema_confidence: str = "none"
    source_stats: SourceStatsInfo | None = None


@dataclass(frozen=True)
class AssemblyInspection:
    """Complete structured inspection of a CompiledAssembly."""

    overview: OverviewSection
    execution_order: ExecutionOrderSection | None
    fused_groups: FusedGroupsSection | None
    materializations: MaterializationsSection | None
    dag: DagSection | None
    joint_details: list[JointInspection] | None
    filter_applied: InspectFilter | None
    verbosity: Verbosity


class Activity_State(Enum):
    """Current activity of the InteractiveSession."""

    IDLE = "idle"
    COMPILING = "compiling"
    EXECUTING = "executing"


@dataclass(frozen=True)
class Execution_Log:
    """A single log entry captured during engine execution."""

    timestamp: datetime
    level: str  # "DEBUG", "INFO", "WARNING", "ERROR"
    source: str  # engine name or component name
    message: str


class CompletionKind(Enum):
    JOINT = "joint"
    CATALOG_TABLE = "catalog_table"
    CATALOG_SCHEMA = "catalog_schema"
    CATALOG_NAME = "catalog_name"
    COLUMN = "column"
    SQL_KEYWORD = "sql_keyword"
    ANNOTATION = "annotation"
    SNIPPET = "snippet"


@dataclass(frozen=True)
class Completion:
    """A single completion candidate."""

    label: str
    insert_text: str
    kind: CompletionKind
    detail: str | None
    sort_key: int
    source: str | None  # which catalog or joint this came from


@dataclass(frozen=True)
class CatalogSearchResult:
    """A single fuzzy search hit in the catalog."""

    kind: Literal["catalog", "schema", "table", "column", "joint"]
    qualified_name: str
    short_name: str
    parent: str | None
    match_positions: list[int]  # character indices for highlighting
    score: float  # lower = better


@dataclass(frozen=True)
class QueryHistoryEntry:
    """A single entry in the execution history."""

    timestamp: datetime
    action_type: str  # "query", "joint", "pipeline", "preview"
    name: str  # query text or joint name
    row_count: int | None
    duration_ms: float
    status: str  # "success", "failed", "canceled", "warning"


@dataclass(frozen=True)
class ResolvedReference:
    """Result of resolving a table reference."""

    kind: Literal["joint", "catalog_table"]
    joint_name: str | None  # if kind == "joint"
    catalog: str | None  # if kind == "catalog_table"
    schema: str | None
    table: str | None
    cached: bool  # True if joint output is in MaterialCache


@dataclass(frozen=True)
class PreprocessedSQL:
    """Result of SQL preprocessing — all table refs resolved and rewritten."""

    sql: str
    source_joints: list[Joint]
    resolved_refs: dict[str, ResolvedReference]


@dataclass(frozen=True)
class ExecutionResult:
    """Result of a full pipeline execution."""

    success: bool
    joints_executed: list[str]
    elapsed_ms: float
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ReplState:
    """Persistent REPL session state."""

    editor_sql: str = ""
    adhoc_engine: str | None = None
    dialect: str | None = None


@dataclass(frozen=True)
class JointPreviewData:
    """Metadata and optional cached rows for a joint preview."""

    joint_name: str
    engine: str
    fusion_group: str | None
    upstream: list[str]
    tags: list[str]
    schema: list[SchemaField] | None
    preview_rows: pa.Table | None
