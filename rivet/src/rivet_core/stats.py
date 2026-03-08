"""Run statistics data models and collector for rivet-core.

Provides frozen dataclasses for per-group, per-joint, source-read, and check
statistics, plus a mutable StatsCollector that accumulates observations during
DAG execution and produces an immutable RunStats summary.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from rivet_core.errors import RivetError
from rivet_core.metrics import MaterializationStats, PhasedTiming, PluginMetrics

# ---------------------------------------------------------------------------
# Frozen data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SourceReadStats:
    """Stats for a single source joint read."""

    joint_name: str
    adapter_name: str
    catalog_type: str
    row_count: int | None
    read_ms: float
    error_code: str | None = None
    error_message: str | None = None
    has_residual: bool = False


@dataclass(frozen=True)
class JointCheckStats:
    """Aggregated check outcomes for a single joint."""

    joint_name: str
    phase: str  # "assertion" or "audit"
    passed: int
    failed: int
    warned: int
    read_back_rows: int | None = None


@dataclass(frozen=True)
class JointStats:
    """Per-joint execution statistics."""

    name: str
    rows_in: int | None
    rows_out: int | None
    timing: PhasedTiming | None
    materialization_stats: MaterializationStats | None
    skipped: bool = False
    skip_reason: str | None = None


@dataclass(frozen=True)
class GroupStats:
    """Per-fused-group execution statistics."""

    group_id: str
    joints: list[str]
    timing: PhasedTiming
    success: bool
    plugin_metrics: PluginMetrics | None = None
    error_code: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class RunStats:
    """Top-level aggregated pipeline execution statistics."""

    # Pipeline-level aggregates
    total_time_ms: float
    total_engine_ms: float
    total_rivet_ms: float
    total_rows_in: int
    total_rows_out: int
    total_bytes_materialized: int
    total_materializations: int
    total_groups_executed: int
    total_groups_failed: int
    total_checks_passed: int
    total_checks_failed: int
    total_check_warnings: int

    # Per-element breakdowns (execution order preserved for groups)
    group_stats: list[GroupStats]
    joint_stats: dict[str, JointStats]

    # Source and check detail
    source_read_stats: list[SourceReadStats]
    check_stats: list[JointCheckStats]

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-compatible dictionary."""
        return asdict(self)



# ---------------------------------------------------------------------------
# Mutable accumulator
# ---------------------------------------------------------------------------


class StatsCollector:
    """Mutable accumulator for execution statistics. Not thread-safe.

    Records per-group timing, per-joint stats, engine metrics, source reads,
    and check results during DAG execution.  Call ``build_run_stats`` at the
    end to produce an immutable ``RunStats`` snapshot.
    """

    def __init__(self) -> None:
        self._group_entries: list[dict[str, Any]] = []
        self._joint_entries: dict[str, dict[str, Any]] = {}
        self._engine_metrics: dict[str, PluginMetrics | None] = {}
        self._source_reads: list[SourceReadStats] = []
        self._check_entries: list[JointCheckStats] = []

    # -- recording methods ---------------------------------------------------

    def record_group_timing(
        self,
        group_id: str,
        joints: list[str],
        timing: PhasedTiming,
        success: bool,
        error: RivetError | None = None,
    ) -> None:
        entry: dict[str, Any] = {
            "group_id": group_id,
            "joints": list(joints),
            "timing": timing,
            "success": success,
            "error_code": error.code if error else None,
            "error_message": error.message if error else None,
        }
        self._group_entries.append(entry)

    def record_joint_stats(
        self,
        name: str,
        rows_in: int | None,
        rows_out: int | None,
        timing: PhasedTiming | None,
        materialization_stats: MaterializationStats | None,
        skipped: bool = False,
        skip_reason: str | None = None,
    ) -> None:
        self._joint_entries[name] = {
            "name": name,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "timing": timing,
            "materialization_stats": materialization_stats,
            "skipped": skipped,
            "skip_reason": skip_reason,
        }

    def record_engine_metrics(
        self, group_id: str, metrics: PluginMetrics | None
    ) -> None:
        self._engine_metrics[group_id] = metrics

    def record_source_read(
        self,
        joint_name: str,
        adapter_name: str,
        catalog_type: str,
        row_count: int | None,
        read_ms: float,
        error: RivetError | None = None,
        has_residual: bool = False,
    ) -> None:
        self._source_reads.append(
            SourceReadStats(
                joint_name=joint_name,
                adapter_name=adapter_name,
                catalog_type=catalog_type,
                row_count=row_count,
                read_ms=read_ms,
                error_code=error.code if error else None,
                error_message=error.message if error else None,
                has_residual=has_residual,
            )
        )

    def record_check_results(
        self,
        joint_name: str,
        phase: str,
        passed: int,
        failed: int,
        warned: int,
        read_back_rows: int | None = None,
    ) -> None:
        self._check_entries.append(
            JointCheckStats(
                joint_name=joint_name,
                phase=phase,
                passed=passed,
                failed=failed,
                warned=warned,
                read_back_rows=read_back_rows,
            )
        )

    # -- build final snapshot ------------------------------------------------

    def build_run_stats(self, total_time_ms: float) -> RunStats:
        """Compute aggregates and return a frozen ``RunStats``."""

        # Build group stats, attaching engine metrics where available
        group_stats: list[GroupStats] = []
        groups_failed = 0
        for entry in self._group_entries:
            gid = entry["group_id"]
            raw_metrics = self._engine_metrics.get(gid)
            pm = raw_metrics if raw_metrics is not None else None
            gs = GroupStats(
                group_id=gid,
                joints=entry["joints"],
                timing=entry["timing"],
                success=entry["success"],
                plugin_metrics=pm,
                error_code=entry["error_code"],
                error_message=entry["error_message"],
            )
            group_stats.append(gs)
            if not entry["success"]:
                groups_failed += 1

        # Build joint stats dict
        joint_stats: dict[str, JointStats] = {}
        total_rows_in = 0
        total_rows_out = 0
        total_bytes_materialized = 0
        total_materializations = 0
        for jdata in self._joint_entries.values():
            js = JointStats(
                name=jdata["name"],
                rows_in=jdata["rows_in"],
                rows_out=jdata["rows_out"],
                timing=jdata["timing"],
                materialization_stats=jdata["materialization_stats"],
                skipped=jdata["skipped"],
                skip_reason=jdata["skip_reason"],
            )
            joint_stats[js.name] = js
            if js.rows_in is not None:
                total_rows_in += js.rows_in
            if js.rows_out is not None:
                total_rows_out += js.rows_out
            if js.materialization_stats is not None:
                total_bytes_materialized += js.materialization_stats.byte_size
                total_materializations += 1

        # Aggregate check counts
        total_checks_passed = sum(c.passed for c in self._check_entries)
        total_checks_failed = sum(c.failed for c in self._check_entries)
        total_check_warnings = sum(c.warned for c in self._check_entries)

        # Aggregate engine vs rivet time across all groups
        total_engine_ms = sum(gs.timing.engine_ms for gs in group_stats)
        total_rivet_ms = total_time_ms - total_engine_ms

        return RunStats(
            total_time_ms=total_time_ms,
            total_engine_ms=total_engine_ms,
            total_rivet_ms=total_rivet_ms,
            total_rows_in=total_rows_in,
            total_rows_out=total_rows_out,
            total_bytes_materialized=total_bytes_materialized,
            total_materializations=total_materializations,
            total_groups_executed=len(group_stats),
            total_groups_failed=groups_failed,
            total_checks_passed=total_checks_passed,
            total_checks_failed=total_checks_failed,
            total_check_warnings=total_check_warnings,
            group_stats=group_stats,
            joint_stats=joint_stats,
            source_read_stats=list(self._source_reads),
            check_stats=list(self._check_entries),
        )
