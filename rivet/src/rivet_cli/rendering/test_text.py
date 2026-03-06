"""Test text format renderer for pass/fail summary."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from rivet_cli.rendering.colors import (
    BOLD,
    DIM,
    GREEN,
    RED,
    SYM_CHECK,
    SYM_ERROR,
    colorize,
)
from rivet_core.testing.models import TestResult


@dataclass(frozen=True)
class CaseResult:
    """Result of a single test case execution."""

    name: str
    passed: bool
    duration_ms: float
    diff: str | None = None
    # Verbose fields
    inputs: dict[str, Any] = field(default_factory=dict)
    target_joint: str = ""
    comparison: str = ""


def _render_diff(diff: list[dict[str, Any]]) -> list[str]:
    """Render structured diff rows into display lines."""
    lines: list[str] = []
    for row in diff[:5]:
        col = row.get("column", "?")
        expected = row.get("expected", "?")
        actual = row.get("actual", "?")
        row_idx = row.get("row")
        prefix = f"row {row_idx}: " if row_idx is not None else ""
        lines.append(f"  {prefix}{col}: expected {expected!r}, got {actual!r}")
    return lines


def render_test_results(
    results: list[TestResult],
    verbosity: int,
    color: bool,
) -> str:
    """Render TestResult list as pass/fail summary with structured diff."""
    lines: list[str] = []

    for r in results:
        status = colorize(f"{SYM_CHECK} PASS", GREEN, color) if r.passed else colorize(f"{SYM_ERROR} FAIL", RED, color)
        name = colorize(r.name, BOLD, color)
        timing = colorize(f"({r.duration_ms:.0f}ms)", DIM, color)
        lines.append(f"  {status}  {name}  {timing}")

        if not r.passed:
            if r.error:
                lines.append(f"    error: {r.error}")
            elif r.comparison_result:
                cr = r.comparison_result
                lines.append(f"    {cr.message}")
                if cr.diff:
                    lines.extend(f"    {dl}" for dl in _render_diff(cr.diff))

    # Summary
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = total - passed
    total_time = sum(r.duration_ms for r in results)

    lines.append("")
    summary_parts: list[str] = []
    if passed:
        summary_parts.append(colorize(f"{passed} passed", GREEN, color))
    if failed:
        summary_parts.append(colorize(f"{failed} failed", RED, color))
    summary_parts.append(f"{total} total")
    summary_parts.append(colorize(f"{total_time:.0f}ms", DIM, color))
    lines.append(f"Tests: {', '.join(summary_parts)}")

    return "\n".join(lines)


def render_test_results_json(results: list[TestResult]) -> str:
    """Render TestResult list as JSON."""
    def _serialize(r: TestResult) -> dict[str, Any]:
        out: dict[str, Any] = {
            "name": r.name,
            "passed": r.passed,
            "duration_ms": r.duration_ms,
        }
        if r.error is not None:
            out["error"] = r.error
        if r.comparison_result is not None:
            cr = r.comparison_result
            out["comparison_result"] = {
                "passed": cr.passed,
                "message": cr.message,
                "diff": cr.diff,
            }
        return out

    return json.dumps([_serialize(r) for r in results], indent=2)


def render_test_text(
    results: list[CaseResult],
    verbosity: int,
    color: bool,
) -> str:
    """Render test results as pass/fail summary."""
    lines: list[str] = []

    for r in results:
        status = colorize(f"{SYM_CHECK} PASS", GREEN, color) if r.passed else colorize(f"{SYM_ERROR} FAIL", RED, color)
        name = colorize(r.name, BOLD, color)
        timing = colorize(f"({r.duration_ms:.0f}ms)", DIM, color)
        lines.append(f"  {status}  {name}  {timing}")

        if not r.passed and r.diff:
            for diff_line in r.diff.splitlines():
                lines.append(f"         {diff_line}")

        if verbosity >= 1:
            if r.target_joint:
                lines.append(f"    joint: {r.target_joint}")
            if r.inputs:
                lines.append(f"    inputs: {', '.join(r.inputs.keys())}")
            if r.comparison:
                lines.append(f"    comparison: {r.comparison}")

    # Summary
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = total - passed
    total_time = sum(r.duration_ms for r in results)

    lines.append("")
    summary_parts: list[str] = []
    if passed:
        summary_parts.append(colorize(f"{passed} passed", GREEN, color))
    if failed:
        summary_parts.append(colorize(f"{failed} failed", RED, color))
    summary_parts.append(f"{total} total")
    summary_parts.append(colorize(f"{total_time:.0f}ms", DIM, color))
    lines.append(f"Tests: {', '.join(summary_parts)}")

    return "\n".join(lines)
