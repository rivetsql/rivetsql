"""Assertion and check data models for rivet-core.

Check types: not_null, unique, row_count, accepted_values, expression,
             custom, schema, freshness, relationship
Severity:    error (default), warning
Phase:       assertion (pre-write), audit (post-write)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

CHECK_TYPES = frozenset(
    {
        "not_null",
        "unique",
        "row_count",
        "accepted_values",
        "expression",
        "custom",
        "schema",
        "freshness",
        "relationship",
    }
)

SEVERITIES = frozenset({"error", "warning"})
PHASES = frozenset({"assertion", "audit"})


@dataclass(frozen=True)
class Assertion:
    """Inline data quality check attached to a joint.

    type must be one of the 9 supported check types.
    severity defaults to "error"; "warning" logs and continues.
    phase "assertion" runs pre-write; "audit" runs post-write on sink joints only.
    """

    type: str
    severity: str = "error"
    config: dict[str, Any] = field(default_factory=dict)
    phase: str = "assertion"

    def __post_init__(self) -> None:
        if self.type not in CHECK_TYPES:
            raise ValueError(f"Invalid check type: {self.type!r}. Must be one of {sorted(CHECK_TYPES)}")
        if self.severity not in SEVERITIES:
            raise ValueError(f"Invalid severity: {self.severity!r}. Must be 'error' or 'warning'")
        if self.phase not in PHASES:
            raise ValueError(f"Invalid phase: {self.phase!r}. Must be 'assertion' or 'audit'")


@dataclass(frozen=True)
class AssertionResult:
    """Result produced by executing an assertion or audit check."""

    passed: bool
    message: str
    details: dict[str, Any] | None = None
    failing_rows: int | None = None


@dataclass(frozen=True)
class CompiledCheck:
    """Compiled representation of an assertion, stored in CompiledJoint."""

    type: str
    severity: str
    config: dict[str, Any]
    phase: str  # "assertion" or "audit"
