"""Watermark state and backend contract for incremental_append write strategy.

Watermarks are *advisory* metadata for incremental loads. They record the
high-water-mark column value last observed for a sink so that operators can
inspect or reset it. They are **not** how sinks deduplicate data:

- Sink plugins implement ``write_strategy="incremental_append"`` via
  key-based deduplication (``ON CONFLICT DO NOTHING`` / ``WHERE NOT EXISTS``).
- The watermark store is consumed by the ``rivet watermark`` CLI commands
  and by user-defined SQL that reads ``state('watermark', sink_name)`` to
  build incremental queries.

The backend contract below is the single source of truth for both the CLI
and any future state-driven loaders.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from pathlib import Path
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

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)

    @classmethod
    def from_json(cls, raw: str) -> WatermarkState:
        data = json.loads(raw)
        return cls(
            column=data.get("column", ""),
            value=data.get("value", ""),
            value_type=data.get("value_type", "string"),
            last_run=data.get("last_run", ""),
            rows_loaded=int(data.get("rows_loaded", 0)),
            metadata=data.get("metadata", {}),
        )


class WatermarkBackend(ABC):
    """Abstract interface for watermark state persistence."""

    @abstractmethod
    def read(self, sink_name: str, profile: str) -> WatermarkState | None:
        """Return the current watermark state, or None if not yet set."""

    @abstractmethod
    def write(self, sink_name: str, profile: str, state: WatermarkState) -> None:
        """Persist the watermark state."""

    @abstractmethod
    def delete(self, sink_name: str, profile: str) -> None:
        """Remove the watermark state (no-op if absent)."""

    @abstractmethod
    def list(self, profile: str) -> list[str]:
        """Return all sink names that have a watermark for *profile*."""


class LocalFileWatermarkBackend(WatermarkBackend):
    """File-system-backed watermark store under ``<root>/.rivet/watermarks/<profile>/``.

    One JSON document per sink. Schema-tolerant on read so legacy files written
    by the pre-backend CLI (which only stored ``{"value": ...}``) are still
    readable.
    """

    def __init__(self, root: Path) -> None:
        self._root = Path(root)

    def _dir(self, profile: str) -> Path:
        return self._root / ".rivet" / "watermarks" / profile

    def _file(self, sink_name: str, profile: str) -> Path:
        return self._dir(profile) / f"{sink_name}.json"

    def read(self, sink_name: str, profile: str) -> WatermarkState | None:
        path = self._file(sink_name, profile)
        if not path.is_file():
            return None
        try:
            raw = path.read_text(encoding="utf-8")
            data = json.loads(raw)
        except (OSError, json.JSONDecodeError):
            return None
        # Tolerate the legacy {"value": ...}-only files.
        if "column" not in data:
            return WatermarkState(
                column=data.get("column", ""),
                value=str(data.get("value", "")),
                value_type=data.get("value_type", "string"),
                last_run=data.get("last_run", ""),
                rows_loaded=int(data.get("rows_loaded", 0)),
                metadata=data.get("metadata", {}),
            )
        return WatermarkState.from_json(raw)

    def write(self, sink_name: str, profile: str, state: WatermarkState) -> None:
        directory = self._dir(profile)
        directory.mkdir(parents=True, exist_ok=True)
        self._file(sink_name, profile).write_text(state.to_json(), encoding="utf-8")

    def delete(self, sink_name: str, profile: str) -> None:
        path = self._file(sink_name, profile)
        if path.exists():
            path.unlink()

    def list(self, profile: str) -> list[str]:
        directory = self._dir(profile)
        if not directory.is_dir():
            return []
        return sorted(p.stem for p in directory.glob("*.json"))
