"""Doctor text format renderer for check results."""

from __future__ import annotations

from dataclasses import dataclass

from rivet_cli.rendering.colors import (
    BOLD,
    GREEN,
    RED,
    SYM_CHECK,
    SYM_ERROR,
    SYM_WARN,
    YELLOW,
    colorize,
)


@dataclass(frozen=True)
class DoctorCheckResult:
    """Result of a single doctor check."""

    level: int
    name: str
    status: str  # "pass", "warning", "error"
    message: str
    details: str | None = None


def render_doctor_text(checks: list[DoctorCheckResult], color: bool) -> str:
    """Render doctor check results with status symbols and summary."""
    lines: list[str] = []

    for check in checks:
        if check.status == "pass":
            sym = colorize(SYM_CHECK, GREEN, color)
        elif check.status == "warning":
            sym = colorize(SYM_WARN, YELLOW, color)
        else:
            sym = colorize(SYM_ERROR, RED, color)

        name = colorize(check.name, BOLD, color)
        lines.append(f"  {sym} {name}: {check.message}")
        if check.details:
            for detail_line in check.details.splitlines():
                lines.append(f"      {detail_line}")

    # Summary
    errors = sum(1 for c in checks if c.status == "error")
    warnings = sum(1 for c in checks if c.status == "warning")
    lines.append("")
    summary_parts: list[str] = []
    if errors:
        summary_parts.append(colorize(f"{errors} error(s)", RED, color))
    if warnings:
        summary_parts.append(colorize(f"{warnings} warning(s)", YELLOW, color))
    if not errors and not warnings:
        summary_parts.append(colorize("All checks passed", GREEN, color))
    lines.append("  " + ", ".join(summary_parts))

    return "\n".join(lines)
