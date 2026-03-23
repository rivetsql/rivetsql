"""Execution data models, Protocol, TypeVar, and logger.

All execution-related dataclasses, the ``ProgressCallback`` Protocol,
the generic ``T`` TypeVar, and the shared module logger live here.
This is Level 1 of the executor package dependency hierarchy — it does
NOT import from ``helpers/``, ``phases/``, or ``pipeline.py``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Protocol, TypeVar, runtime_checkable

import pyarrow

from rivet_core.compiler import (
    CompiledCatalog,
    CompiledJoint,
    Materialization,
)
from rivet_core.errors import RivetError
from rivet_core.metrics import (
    MaterializationStats,
    PhasedTiming,
    PluginMetrics,
)
from rivet_core.optimizer import (
    FusedGroup,
    ResidualPlan,
)
from rivet_core.plugins import PluginRegistry
from rivet_core.stats import RunStats, StatsCollector
from rivet_core.strategies import MaterializedRef

# TypeVar for generic return type in _run_in_loop
T = TypeVar("T")

logger = logging.getLogger("rivet_core.executor")


@dataclass(frozen=True)
class CheckExecutionResult:
    """Result of executing a single quality check (assertion or audit).

    Attributes:
        type: The check type (e.g. ``"not_null"``, ``"unique"``).
        severity: ``"error"`` or ``"warning"``.
        passed: Whether the check passed.
        message: Human-readable result description.
        phase: ``"assertion"`` (pre-write) or ``"audit"`` (post-write).
        read_back_rows: Row count from read-back audits, if applicable.
        execution_method: How the check was executed — ``"arrow"`` for
            Python-side Arrow table checks, ``"engine_native"`` for
            SQL dispatched to the engine plugin via
            ``execute_assertion_sql``.
    """

    type: str
    severity: str
    passed: bool
    message: str
    phase: str  # "assertion" or "audit"
    read_back_rows: int | None = None
    execution_method: str = "arrow"


@dataclass(frozen=True)
class JointExecutionResult:
    name: str
    success: bool
    rows_in: int | None
    rows_out: int | None
    timing: PhasedTiming | None
    fused_group_id: str | None
    materialized: bool
    materialization_trigger: str | None
    materialization_stats: MaterializationStats | None
    check_results: list[CheckExecutionResult]
    plugin_metrics: PluginMetrics | None
    error: RivetError | None
    write_path: str | None = None  # "native_sql" | "arrow_fallback" | None


@dataclass(frozen=True)
class FusedGroupExecutionResult:
    group_id: str
    joints: list[str]
    success: bool
    rows_in: int
    rows_out: int
    timing: PhasedTiming
    materialization_stats: MaterializationStats | None
    plugin_metrics: PluginMetrics | None
    error: RivetError | None


@dataclass(frozen=True)
class ExecutionResult:
    success: bool
    status: str  # "success", "failure", "partial_failure"
    joint_results: list[JointExecutionResult]
    group_results: list[FusedGroupExecutionResult]
    total_time_ms: float
    total_materializations: int
    total_failures: int
    total_check_failures: int
    total_check_warnings: int
    run_stats: RunStats | None = None


@dataclass
class ExecutionState:
    materials: dict[str, MaterializedRef] = field(default_factory=dict)
    failed_joints: set[str] = field(default_factory=set)
    joint_results: list[JointExecutionResult] = field(default_factory=list)
    group_results: list[FusedGroupExecutionResult] = field(default_factory=list)
    total_materializations: int = 0
    total_failures: int = 0
    total_check_failures: int = 0
    total_check_warnings: int = 0


@dataclass(frozen=True)
class ExecutionContext:
    registry: PluginRegistry
    joint_map: dict[str, CompiledJoint]
    group_map: dict[str, FusedGroup]
    catalog_map: dict[str, CompiledCatalog]
    materialization_map: dict[str, list[Materialization]]
    stats_collector: StatsCollector | None = None


@dataclass(frozen=True)
class GroupEnginePhaseResult:
    result_ref: MaterializedRef
    adapter_residual: ResidualPlan | None
    arrow_materials: dict[str, pyarrow.Table]
    engine_ms: float
    write_path: str | None
    native_write_used: bool
    exit_joint_name: str


@dataclass(frozen=True)
class GroupPostprocessResult:
    result_ref: MaterializedRef
    residual_ms: float
    materialized: bool
    materialization_trigger: str | None
    materialization_stats: MaterializationStats | None
    total_materializations: int
    materialize_ms: float


@dataclass(frozen=True)
class GroupCheckPhaseResult:
    all_check_results: dict[str, list[CheckExecutionResult]]
    assertion_error: bool
    check_failures: int
    check_warnings: int
    check_ms: float


@runtime_checkable
class ProgressCallback(Protocol):
    """Protocol for receiving live progress events during pipeline execution."""

    def on_group_start(self, group_id: str, engine: str) -> None: ...

    def on_group_complete(
        self,
        group_id: str,
        success: bool,
        joint_results: list[JointExecutionResult],
        elapsed_ms: float,
    ) -> None: ...

    def on_materialization(
        self,
        source_joint: str,
        target_engine: str,
        strategy: str,
    ) -> None: ...

    def on_check_result(
        self,
        joint_name: str,
        check_type: str,
        passed: bool,
        phase: str,
    ) -> None: ...

    def on_error(self, group_id: str, error: RivetError) -> None: ...
