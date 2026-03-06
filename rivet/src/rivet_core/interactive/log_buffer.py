"""Ring buffer for execution log entries.

Thread-safe. Owned by InteractiveSession. No TUI or CLI imports — only stdlib.
"""

from __future__ import annotations

import threading
from collections import deque

from rivet_core.interactive.types import Execution_Log


class Log_Buffer:
    """Ring buffer for execution log entries.

    Uses ``collections.deque(maxlen=capacity)`` for O(1) append with
    automatic oldest-entry eviction.  All public operations are
    guarded by a ``threading.Lock``.
    """

    def __init__(self, capacity: int = 5_000) -> None:
        self._buf: deque[Execution_Log] = deque(maxlen=capacity)
        self._lock = threading.Lock()

    def append(self, entry: Execution_Log) -> None:
        """Add *entry*. Evicts oldest if at capacity."""
        with self._lock:
            self._buf.append(entry)

    def get_all(self) -> list[Execution_Log]:
        """Return all entries in chronological (insertion) order."""
        with self._lock:
            return list(self._buf)

    def clear(self) -> None:
        """Remove all entries."""
        with self._lock:
            self._buf.clear()

    def __len__(self) -> int:
        """Current entry count."""
        with self._lock:
            return len(self._buf)

    @property
    def capacity(self) -> int:
        """Maximum number of entries before eviction."""
        return self._buf.maxlen  # type: ignore[return-value]
