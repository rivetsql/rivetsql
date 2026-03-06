"""ProgressIndicator widget for the Rivet REPL TUI.

Displays execution progress: phase label, joint name, progress bar,
elapsed time, and cancel hint. Mounted inside ResultsPanel during execution.

Requirements: 2.1, 2.2, 2.3
"""

from __future__ import annotations

import time
from typing import Any

from rivet_core.interactive.types import QueryProgress

try:
    from textual.app import ComposeResult
    from textual.containers import Vertical
    from textual.timer import Timer
    from textual.widget import Widget
    from textual.widgets import ProgressBar, Static

    _TEXTUAL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _TEXTUAL_AVAILABLE = False


_PHASE_LABELS = {
    "compiling": "Compiling\u2026",
    "executing": "Executing\u2026",
    "done": "Done",
    "failed": "Failed",
}


class _ProgressIndicatorState:
    """Pure state for ProgressIndicator — no TUI dependencies."""

    phase: str
    joint_name: str
    current: int
    total: int

    def _init_state(self) -> None:
        self.phase = "Compiling\u2026"
        self.joint_name = ""
        self.current = 0
        self.total = 0

    def update_progress(self, progress: QueryProgress) -> None:
        """Update state from a QueryProgress dataclass."""
        self.phase = _PHASE_LABELS.get(progress.status, progress.status)
        self.joint_name = progress.joint_name
        self.current = progress.current
        self.total = progress.total
        self._on_state_changed()

    def _on_state_changed(self) -> None:
        """Hook for subclasses."""


if _TEXTUAL_AVAILABLE:

    class ProgressIndicator(Widget, _ProgressIndicatorState):
        """Progress indicator shown in ResultsPanel during query execution."""

        DEFAULT_CSS = """
        ProgressIndicator {
            height: auto;
            padding: 1 2;
        }
        ProgressIndicator .pi-phase {
            text-style: bold;
        }
        ProgressIndicator .pi-joint {
            color: $text-muted;
        }
        ProgressIndicator .pi-elapsed {
            color: $text-muted;
        }
        ProgressIndicator .pi-hint {
            color: $text-muted;
            text-style: italic;
        }
        """

        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self._init_state()
            self._t0 = time.monotonic()
            self._timer: Timer | None = None

        def compose(self) -> ComposeResult:
            with Vertical():
                yield Static("Compiling\u2026", id="pi-phase", classes="pi-phase")
                yield Static("", id="pi-joint", classes="pi-joint")
                yield ProgressBar(total=100, show_eta=False, id="pi-bar")
                yield Static("Elapsed: 0.0s", id="pi-elapsed", classes="pi-elapsed")
                yield Static("Ctrl+C to cancel", id="pi-hint", classes="pi-hint")

        def on_mount(self) -> None:
            self._timer = self.set_interval(0.5, self._tick_elapsed)

        def _tick_elapsed(self) -> None:
            elapsed = time.monotonic() - self._t0
            try:
                self.query_one("#pi-elapsed", Static).update(f"Elapsed: {elapsed:.1f}s")
            except Exception:
                pass

        def _on_state_changed(self) -> None:
            if not self.is_mounted:
                return
            try:
                self.query_one("#pi-phase", Static).update(self.phase)
                self.query_one("#pi-joint", Static).update(self.joint_name)
                bar = self.query_one("#pi-bar", ProgressBar)
                bar.update(total=max(self.total, 1), progress=self.current)
            except Exception:
                pass

else:

    class ProgressIndicator(_ProgressIndicatorState):  # type: ignore[no-redef]
        """Headless ProgressIndicator for testing without Textual."""

        def __init__(self, **kwargs: Any) -> None:
            self._init_state()
