"""Structured logging for rivet-core.

Outputs structured log events to stderr. Never interferes with stdout JSON output.
"""

from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import IntEnum
from typing import Any


class LogLevel(IntEnum):
    """Log levels ordered by severity."""

    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40


@dataclass(frozen=True)
class LogEvent:
    """A single structured log event."""

    timestamp: str
    level: str
    phase: str  # "compilation" or "execution"
    event_type: str
    message: str
    joint: str | None = None
    context: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        return {k: v for k, v in d.items() if v is not None and v != {}}


class RivetLogger:
    """Structured logger that writes JSON events to stderr."""

    def __init__(self, level: LogLevel = LogLevel.INFO) -> None:
        self._level = level

    @property
    def level(self) -> LogLevel:
        return self._level

    @level.setter
    def level(self, value: LogLevel) -> None:
        self._level = value

    def _emit(
        self,
        level: LogLevel,
        phase: str,
        event_type: str,
        message: str,
        joint: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> LogEvent | None:
        if level < self._level:
            return None
        event = LogEvent(
            timestamp=datetime.now(UTC).isoformat(),
            level=level.name,
            phase=phase,
            event_type=event_type,
            message=message,
            joint=joint,
            context=context or {},
        )
        print(json.dumps(event.to_dict(), default=str), file=sys.stderr)
        return event

    def debug(
        self,
        phase: str,
        event_type: str,
        message: str,
        joint: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> LogEvent | None:
        return self._emit(LogLevel.DEBUG, phase, event_type, message, joint, context)

    def info(
        self,
        phase: str,
        event_type: str,
        message: str,
        joint: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> LogEvent | None:
        return self._emit(LogLevel.INFO, phase, event_type, message, joint, context)

    def warning(
        self,
        phase: str,
        event_type: str,
        message: str,
        joint: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> LogEvent | None:
        return self._emit(LogLevel.WARNING, phase, event_type, message, joint, context)

    def error(
        self,
        phase: str,
        event_type: str,
        message: str,
        joint: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> LogEvent | None:
        return self._emit(LogLevel.ERROR, phase, event_type, message, joint, context)
