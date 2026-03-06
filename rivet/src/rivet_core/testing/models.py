"""Frozen dataclasses for test definitions, results, and comparisons."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ComparisonResult:
    """Outcome of comparing actual vs expected Arrow tables."""

    passed: bool
    message: str
    diff: list[dict[str, Any]] | None = None


@dataclass(frozen=True)
class TestResult:
    """Outcome of a single test execution."""

    name: str
    passed: bool
    duration_ms: float
    comparison_result: ComparisonResult | None = None
    check_results: list[Any] = field(default_factory=list)
    error: str | None = None


@dataclass(frozen=True)
class TestDef:
    """Parsed test definition from YAML."""

    name: str
    target: str
    targets: dict[str, dict[str, Any]] | None = None
    scope: str = "joint"
    inputs: dict[str, Any] = field(default_factory=dict)
    expected: dict[str, Any] | None = None
    compare: str = "exact"
    compare_function: str | None = None
    tags: list[str] = field(default_factory=list)
    description: str | None = None
    options: dict[str, Any] = field(default_factory=dict)
    extends: str | None = None
    source_file: Path | None = None
    engine: str | None = None

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("TestDef.name must be non-empty")
        if not self.target:
            raise ValueError("TestDef.target must be non-empty")
