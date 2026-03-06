"""Public API for the interactive service layer.

All types and service classes are exported from this module.
"""

from rivet_core.interactive.assembly_formatter import AssemblyFormatter
from rivet_core.interactive.catalog_search import CatalogSearch
from rivet_core.interactive.completions import CompletionEngine
from rivet_core.interactive.exporter import ExportFormat, export_table
from rivet_core.interactive.history import load_history, save_history
from rivet_core.interactive.log_buffer import Log_Buffer
from rivet_core.interactive.query_planner import QueryPlanner
from rivet_core.interactive.session import (
    ExecutionInProgressError,
    InteractiveSession,
    ReadOnlyError,
    SessionError,
)
from rivet_core.interactive.types import (
    Activity_State,
    AdapterInfo,
    AssemblyInspection,
    CatalogInfo,
    CatalogSearchResult,
    ChangedRow,
    ColumnProfile,
    Completion,
    CompletionKind,
    DagEdge,
    DagNode,
    DagSection,
    DiffResult,
    EngineInfo,
    Execution_Log,
    ExecutionOrderSection,
    ExecutionResult,
    ExecutionStep,
    FusedGroupDetail,
    FusedGroupsSection,
    InspectFilter,
    JointInspection,
    JointPreviewData,
    MaterializationDetail,
    MaterializationsSection,
    OverviewSection,
    ProfileResult,
    QueryHistoryEntry,
    QueryPlan,
    QueryProgress,
    QueryResult,
    ReplState,
    ResolvedReference,
    SchemaField,
    Verbosity,
)

__all__ = [
    # Service classes
    "AssemblyFormatter",
    "CatalogSearch",
    "CompletionEngine",
    "InteractiveSession",
    "Log_Buffer",
    "QueryPlanner",
    "ReadOnlyError",
    "SessionError",
    "ExecutionInProgressError",
    # Types
    "Activity_State",
    "AdapterInfo",
    "AssemblyInspection",
    "CatalogInfo",
    "CatalogSearchResult",
    "ChangedRow",
    "ColumnProfile",
    "Completion",
    "CompletionKind",
    "DagEdge",
    "DagNode",
    "DagSection",
    "DiffResult",
    "EngineInfo",
    "Execution_Log",
    "ExecutionOrderSection",
    "ExecutionResult",
    "ExecutionStep",
    "ExportFormat",
    "FusedGroupDetail",
    "FusedGroupsSection",
    "InspectFilter",
    "JointInspection",
    "JointPreviewData",
    "MaterializationDetail",
    "MaterializationsSection",
    "OverviewSection",
    "ProfileResult",
    "QueryHistoryEntry",
    "QueryPlan",
    "QueryProgress",
    "QueryResult",
    "ReplState",
    "ResolvedReference",
    "SchemaField",
    "Verbosity",
    # Functions
    "export_table",
    "load_history",
    "save_history",
]
