"""CLI metrics collection."""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger("rivet.cli.metrics")


@dataclass
class CLIMetrics:
    """Collected metrics for a single CLI invocation."""

    command: str
    command_duration_ms: float
    compile_duration_ms: float | None = None
    execution_duration_ms: float | None = None
    test_duration_ms: float | None = None
    exit_code: int = 0


def record_metrics(metrics: CLIMetrics) -> None:
    """Record CLI metrics via structured logging."""
    logger.info(
        "rivet.cli.command command=%s exit_code=%d duration_ms=%.1f"
        " compile_ms=%s execution_ms=%s test_ms=%s",
        metrics.command,
        metrics.exit_code,
        metrics.command_duration_ms,
        f"{metrics.compile_duration_ms:.1f}" if metrics.compile_duration_ms is not None else "n/a",
        f"{metrics.execution_duration_ms:.1f}" if metrics.execution_duration_ms is not None else "n/a",
        f"{metrics.test_duration_ms:.1f}" if metrics.test_duration_ms is not None else "n/a",
    )
