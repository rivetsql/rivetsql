"""Metrics data models for rivet-core.

Provides phased timing, materialization stats, and extensible plugin metrics.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class PhasedTiming:
    """Timing breakdown per execution unit. Phases sum to ~total_ms."""

    total_ms: float
    engine_ms: float
    materialize_ms: float
    residual_ms: float
    check_ms: float


@dataclass(frozen=True)
class ColumnExecutionStats:
    """Per-column statistics computed at materialization."""

    column: str
    null_count: int
    distinct_count_estimate: int  # HyperLogLog estimate
    min_value: str | None
    max_value: str | None


@dataclass(frozen=True)
class MaterializationStats:
    """Statistics captured at every materialization point."""

    row_count: int
    byte_size: int
    column_stats: list[ColumnExecutionStats]
    sampled: bool = False


@dataclass(frozen=True)
class QueryPlanningMetrics:
    """Standard metric category: query planning."""

    planning_time_ms: float | None = None
    estimated_rows: int | None = None
    actual_rows: int | None = None
    plan_text: str | None = None


@dataclass(frozen=True)
class IOMetrics:
    """Standard metric category: I/O."""

    bytes_read: int | None = None
    bytes_written: int | None = None
    network_bytes: int | None = None
    requests: int | None = None
    files_read: int | None = None
    files_skipped: int | None = None


@dataclass(frozen=True)
class MemoryMetrics:
    """Standard metric category: memory usage."""

    peak_bytes: int | None = None
    spilled_bytes: int | None = None
    spilled: bool = False


@dataclass(frozen=True)
class ParallelismMetrics:
    """Standard metric category: parallelism."""

    threads_used: int | None = None
    thread_time_ms: float | None = None


@dataclass(frozen=True)
class CacheMetrics:
    """Standard metric category: caching."""

    hits: int | None = None
    misses: int | None = None
    hit_ratio: float | None = None
    evictions: int | None = None


@dataclass(frozen=True)
class ScanMetrics:
    """Standard metric category: scan statistics."""

    rows_scanned: int | None = None
    rows_filtered: int | None = None
    filter_selectivity: float | None = None
    partitions_scanned: int | None = None
    partitions_pruned: int | None = None


# Union of all well-known metric categories
MetricCategory = (
    QueryPlanningMetrics
    | IOMetrics
    | MemoryMetrics
    | ParallelismMetrics
    | CacheMetrics
    | ScanMetrics
)


@dataclass(frozen=True)
class PluginMetrics:
    """Extensible metrics reported by engine plugins.

    well_known: standard metric categories with fixed schemas.
    extensions: plugin-specific data, keys must be namespaced as
        "{plugin_name}.{metric_name}" (must contain a dot).
    """

    well_known: dict[str, MetricCategory] = field(default_factory=dict)
    extensions: dict[str, Any] = field(default_factory=dict)
    engine: str = ""
    adapter: str | None = None

    def __post_init__(self) -> None:
        for key in self.extensions:
            if "." not in key:
                raise ValueError(
                    f"Extension metric key '{key}' must be namespaced as "
                    f"'{{plugin_name}}.{{metric_name}}'"
                )
