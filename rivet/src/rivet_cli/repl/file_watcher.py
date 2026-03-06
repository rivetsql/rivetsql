"""FileWatcher — background file monitoring for the Rivet REPL.

Monitors the project directory for changes to relevant files, debounces
at 500ms, triggers session.on_file_changed(), and posts ProjectCompiled
messages back to the main Textual thread.

Runs on a worker thread; never blocks the main asyncio/Textual thread.

Requirements: 23.1, 23.2, 23.3, 23.4, 32.2
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rivet_core.interactive.session import InteractiveSession

# Glob patterns for files that trigger recompilation (Requirement 23.1)
_WATCH_PATTERNS: tuple[str, ...] = (
    "rivet.yaml",
    "profiles.yaml",
    "sources/*.sql",
    "joints/*.sql",
    "sinks/*.sql",
    "quality/*.yaml",
    "*.sql",
)

_DEBOUNCE_SECONDS = 0.5
_POLL_INTERVAL = 0.1


def _collect_watched_files(project_path: Path) -> dict[Path, float]:
    """Return {path: mtime} for all files matching watch patterns."""
    seen: dict[Path, float] = {}
    for pattern in _WATCH_PATTERNS:
        for p in project_path.glob(pattern):
            if p.is_file():
                try:
                    seen[p] = p.stat().st_mtime
                except OSError:
                    pass
    return seen


class FileWatcher:
    """Background file watcher for the Rivet REPL.

    Polls the project directory on a daemon thread, debounces changes at
    500ms, calls session.on_file_changed(), and invokes the on_compiled
    callback with the result so the TUI can post a ProjectCompiled message.

    Args:
        project_path:   Root directory of the Rivet project.
        session:        InteractiveSession to notify on file changes.
        on_compiled:    Callback invoked on the worker thread after
                        recompilation.  Signature:
                        ``(success: bool, elapsed_ms: float, error: str | None) -> None``
        on_conflict:    Optional callback invoked when an external edit
                        conflicts with an unsaved editor buffer.  Signature:
                        ``(path: Path) -> None``
    """

    def __init__(
        self,
        project_path: Path,
        session: InteractiveSession,
        on_compiled: Callable[[bool, float, str | None], None],
        on_conflict: Callable[[Path], None] | None = None,
    ) -> None:
        self._project_path = project_path
        self._session = session
        self._on_compiled = on_compiled
        self._on_conflict = on_conflict

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._dirty_paths: set[Path] = set()
        self._last_change_time: float = 0.0
        self._lock = threading.Lock()

        # Snapshot of mtimes at start
        self._mtimes: dict[Path, float] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background watcher thread."""
        self._mtimes = _collect_watched_files(self._project_path)
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="rivet-file-watcher",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop the background watcher thread (blocks until it exits)."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def notify_unsaved_paths(self, unsaved: set[Path]) -> None:
        """Register paths with unsaved editor buffers for conflict detection.

        Called by the TUI whenever the set of dirty editor tabs changes.
        """
        with self._lock:
            self._unsaved_paths = unsaved

    # ------------------------------------------------------------------
    # Worker thread
    # ------------------------------------------------------------------

    def _run(self) -> None:
        """Main polling loop — runs on the worker thread."""
        self._unsaved_paths: set[Path] = set()  # type: ignore[no-redef]

        while not self._stop_event.is_set():
            self._poll()
            self._flush_if_debounced()
            self._stop_event.wait(timeout=_POLL_INTERVAL)

    def _poll(self) -> None:
        """Detect new/modified/deleted files and record dirty paths."""
        current = _collect_watched_files(self._project_path)

        changed: set[Path] = set()

        # Modified or new files
        for path, mtime in current.items():
            if self._mtimes.get(path) != mtime:
                changed.add(path)

        # Deleted files
        for path in self._mtimes:
            if path not in current:
                changed.add(path)

        if changed:
            with self._lock:
                self._dirty_paths.update(changed)
                self._last_change_time = time.monotonic()

        self._mtimes = current

    def _flush_if_debounced(self) -> None:
        """If dirty paths exist and debounce window has elapsed, recompile."""
        with self._lock:
            if not self._dirty_paths:
                return
            if time.monotonic() - self._last_change_time < _DEBOUNCE_SECONDS:
                return
            paths = list(self._dirty_paths)
            self._dirty_paths.clear()
            unsaved = set(self._unsaved_paths)

        # Check for conflicts (Requirement 23.3)
        if self._on_conflict is not None:
            for p in paths:
                if p in unsaved:
                    self._on_conflict(p)

        # Trigger session recompilation (Requirement 23.2)
        t0 = time.monotonic()
        try:
            result = self._session.on_file_changed([Path(p) for p in paths])
            elapsed_ms = (time.monotonic() - t0) * 1000.0
            if result is not None:
                self._on_compiled(True, elapsed_ms, None)
            # If result is None, the files weren't relevant — no notification needed
        except Exception as exc:  # noqa: BLE001
            elapsed_ms = (time.monotonic() - t0) * 1000.0
            self._on_compiled(False, elapsed_ms, str(exc))
