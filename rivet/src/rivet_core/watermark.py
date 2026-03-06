"""Watermark state and backend contract for incremental_append write strategy."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class WatermarkState:
    """Persisted watermark state for incremental_append.

    column: the watermark column name
    value: serialized watermark value (as string)
    value_type: type hint for deserialization (e.g. "timestamp", "integer", "date")
    last_run: ISO 8601 timestamp of the last successful run
    rows_loaded: number of rows loaded in the last run
    metadata: arbitrary extra metadata
    """

    column: str
    value: str
    value_type: str
    last_run: str  # ISO 8601
    rows_loaded: int
    metadata: dict[str, Any] = field(default_factory=dict)


class WatermarkBackend(ABC):
    """Abstract interface for watermark state persistence.

    Implementations are provided by core (local_file, etc.) or plugins.
    """

    @abstractmethod
    def read(self, sink_name: str, profile: str) -> WatermarkState | None:
        """Return the current watermark state, or None if not yet set."""

    @abstractmethod
    def write(self, sink_name: str, profile: str, state: WatermarkState) -> None:
        """Persist the watermark state."""

    @abstractmethod
    def delete(self, sink_name: str, profile: str) -> None:
        """Remove the watermark state."""
