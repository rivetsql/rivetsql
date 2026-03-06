"""Error taxonomy for rivet-core.

Error code ranges:
    1xx — engine errors
    2xx — plugin errors
    3xx — DAG errors
    4xx — materialization errors
    5xx — execution errors
    600-649 — assertion errors
    650-699 — audit errors
    7xx — SQL errors
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class RivetError:
    """Structured, actionable error with code, context, and remediation."""

    code: str  # "RVT-NNN"
    message: str
    context: dict[str, Any] = field(default_factory=dict)
    remediation: str | None = None

    # SQL-specific fields (populated for RVT-7xx)
    original_sql: str | None = None
    dialect: str | None = None
    error_position: tuple[int, int] | None = None  # (line, column)
    failing_construct: str | None = None

    def __str__(self) -> str:
        parts = [f"[{self.code}] {self.message}"]
        if self.remediation:
            parts.append(f"  Remediation: {self.remediation}")
        return "\n".join(parts)


class CompilationError(Exception):
    """Raised when compilation fails and execution is attempted."""

    def __init__(self, errors: list[RivetError]) -> None:
        self.errors = errors
        messages = "; ".join(str(e) for e in errors)
        super().__init__(f"Compilation failed with {len(errors)} error(s): {messages}")


class ExecutionError(Exception):
    """Raised when execution encounters a fatal error."""

    def __init__(self, error: RivetError) -> None:
        self.error = error
        super().__init__(str(error))


class PluginValidationError(Exception):
    """Raised when plugin validation fails (RVT-2xx)."""

    def __init__(self, error: RivetError) -> None:
        self.error = error
        super().__init__(str(error))


class SQLParseError(Exception):
    """Raised when SQL parsing fails (RVT-7xx)."""

    def __init__(self, error: RivetError) -> None:
        self.error = error
        super().__init__(str(error))


def plugin_error(
    code: str,
    message: str,
    *,
    plugin_name: str,
    plugin_type: str,
    remediation: str,
    adapter: str | None = None,
    **extra_context: Any,
) -> RivetError:
    """Create a structured RivetError with mandatory plugin context.

    Every plugin error must include plugin_name, plugin_type, and remediation.
    Adapter errors should also include the adapter identity.

    Args:
        code: Error code (e.g. "RVT-201").
        message: Human-readable error description.
        plugin_name: Name of the plugin package (e.g. "rivet_duckdb").
        plugin_type: Plugin component type ("catalog", "engine", "adapter", "source", "sink").
        remediation: Actionable suggestion for resolution.
        adapter: Adapter class name if applicable (e.g. "S3DuckDBAdapter").
        **extra_context: Additional context key-value pairs.

    Returns:
        A fully structured RivetError.
    """
    ctx: dict[str, Any] = {"plugin_name": plugin_name, "plugin_type": plugin_type}
    if adapter is not None:
        ctx["adapter"] = adapter
    ctx.update(extra_context)
    return RivetError(code=code, message=message, context=ctx, remediation=remediation)
