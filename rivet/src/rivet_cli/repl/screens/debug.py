"""Debug mode overlay and controls for the Rivet REPL TUI.

Provides stepping execution through pipeline joints, breakpoint management,
and intermediate Material inspection.

Controls:
  F10       — Step (execute next joint)
  F5        — Continue (run remaining joints, pause at breakpoints)
  Shift+F5  — Stop (abort debug session)
  F9        — Toggle breakpoint on selected joint

Requirements: 24.1, 24.2, 24.3, 24.4, 24.5, 24.6
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

try:
    from textual.app import ComposeResult
    from textual.binding import Binding
    from textual.containers import Horizontal, Vertical
    from textual.screen import ModalScreen
    from textual.widgets import DataTable, Footer, Label, ListItem, ListView, Static

    _TEXTUAL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _TEXTUAL_AVAILABLE = False



# ---------------------------------------------------------------------------
# Pure state — no UI dependencies
# ---------------------------------------------------------------------------


@dataclass
class DebugState:
    """Pure state for debug mode. No UI dependencies.

    Tracks the execution order of joints, current position, breakpoints,
    and intermediate Material snapshots.
    """

    joint_names: list[str] = field(default_factory=list)
    current_index: int = -1
    breakpoints: set[str] = field(default_factory=set)
    materials: dict[str, Any] = field(default_factory=dict)
    stopped: bool = False

    @property
    def is_active(self) -> bool:
        """True if debug session is in progress and not stopped."""
        return not self.stopped and len(self.joint_names) > 0

    @property
    def current_joint(self) -> str | None:
        """Name of the joint at the current step, or None."""
        if 0 <= self.current_index < len(self.joint_names):
            return self.joint_names[self.current_index]
        return None

    @property
    def is_complete(self) -> bool:
        """True if all joints have been stepped through."""
        return self.current_index >= len(self.joint_names)

    def toggle_breakpoint(self, joint_name: str) -> bool:
        """Toggle breakpoint on *joint_name*. Returns True if now set."""
        if joint_name in self.breakpoints:
            self.breakpoints.discard(joint_name)
            return False
        self.breakpoints.add(joint_name)
        return True

    def step(self) -> str | None:
        """Advance to the next joint. Returns the joint name or None if done."""
        if self.stopped or self.is_complete:
            return None
        self.current_index += 1
        return self.current_joint

    def continue_to_breakpoint(self) -> str | None:
        """Advance past the current joint until a breakpoint or end.

        Returns the joint name where execution paused, or None if complete.
        """
        if self.stopped or self.is_complete:
            return None
        # Move past current position first
        self.current_index += 1
        while self.current_index < len(self.joint_names):
            name = self.joint_names[self.current_index]
            if name in self.breakpoints:
                return name
            self.current_index += 1
        return None

    def stop(self) -> None:
        """Abort the debug session."""
        self.stopped = True

    def store_material(self, joint_name: str, material: Any) -> None:
        """Store an intermediate Material for inspection."""
        self.materials[joint_name] = material

    def get_material(self, joint_name: str) -> Any | None:
        """Retrieve a stored Material by joint name."""
        return self.materials.get(joint_name)

    def status_for(self, joint_name: str) -> str:
        """Return a display status for a joint in the debug sequence."""
        try:
            idx = self.joint_names.index(joint_name)
        except ValueError:
            return ""
        if self.stopped:
            return "stopped" if idx > self.current_index else "done"
        if idx < self.current_index:
            return "done"
        if idx == self.current_index:
            return "current"
        return "pending"


def _format_material_preview(material: Any) -> list[tuple[str, str]]:
    """Return (column_name, type_string) pairs for a material preview.

    Accepts a pyarrow Table or anything with a .schema attribute.
    Falls back to an empty list.
    """
    try:
        schema = material.schema
        return [(f.name, str(f.type)) for f in schema]
    except (AttributeError, TypeError):
        return []


# ---------------------------------------------------------------------------
# Textual screen
# ---------------------------------------------------------------------------

if _TEXTUAL_AVAILABLE:

    class DebugScreen(ModalScreen[str | None]):
        """Debug mode overlay.

        Shows the joint execution sequence, current position, breakpoints,
        and material preview. Dismisses with an action string or None.

        Actions returned:
          "step"     — execute next joint (F10)
          "continue" — run to next breakpoint (F5)
          "stop"     — abort debug session (Shift+F5)
          None       — dismissed without action (Escape)
        """

        BINDINGS = [
            Binding("f10", "step", "Step", show=True),
            Binding("f5", "continue_run", "Continue", show=True),
            Binding("shift+f5", "stop_debug", "Stop", show=True),
            Binding("f9", "toggle_breakpoint", "Breakpoint", show=True),
            Binding("escape", "dismiss_screen", "Close", show=True),
        ]

        DEFAULT_CSS = """
        DebugScreen {
            align: center middle;
        }
        #debug-container {
            width: 90%;
            height: 80%;
            border: thick $accent;
            background: $surface;
        }
        #debug-title {
            height: 1;
            background: $accent;
            color: $text;
            content-align: center middle;
            text-style: bold;
        }
        #debug-body {
            height: 1fr;
        }
        #joint-list {
            width: 40%;
            min-width: 20;
            height: 1fr;
        }
        #material-preview {
            width: 1fr;
            height: 1fr;
        }
        #debug-status {
            height: 1;
            background: $accent-darken-1;
            color: $text;
            content-align: center middle;
        }
        """

        def __init__(self, state: DebugState) -> None:
            super().__init__()
            self._state = state

        @property
        def state(self) -> DebugState:
            return self._state

        def compose(self) -> ComposeResult:
            with Vertical(id="debug-container"):
                yield Label("Debug Mode", id="debug-title")
                with Horizontal(id="debug-body"):
                    yield ListView(id="joint-list")
                    yield DataTable(id="material-preview", cursor_type="row")
                yield Label(self._status_text(), id="debug-status")
            yield Footer()

        def on_mount(self) -> None:
            self._refresh_joint_list()
            self._refresh_material_preview()

        def _status_text(self) -> str:
            s = self._state
            if s.stopped:
                return "Debug stopped"
            if s.is_complete:
                return "Debug complete — all joints executed"
            current = s.current_joint
            bp_count = len(s.breakpoints)
            pos = f"Step {s.current_index + 1}/{len(s.joint_names)}"
            bp = f" | 🔴 {bp_count} breakpoint(s)" if bp_count else ""
            joint_label = f" | Current: {current}" if current else ""
            return f"{pos}{joint_label}{bp}"

        def _refresh_joint_list(self) -> None:
            lv = self.query_one("#joint-list", ListView)
            lv.clear()
            for name in self._state.joint_names:
                status = self._state.status_for(name)
                bp = "🔴 " if name in self._state.breakpoints else "   "
                icon = {"done": "✅", "current": "▶️", "pending": "⬜", "stopped": "⏹️"}.get(
                    status, "  "
                )
                item = ListItem(Static(f"{bp}{icon} {name}"))
                item.data = name  # type: ignore[attr-defined]
                lv.append(item)

        def _refresh_material_preview(self) -> None:
            table = self.query_one("#material-preview", DataTable)
            table.clear(columns=True)
            current = self._state.current_joint
            if current is None:
                return
            material = self._state.get_material(current)
            if material is None:
                table.add_columns("Info")
                table.add_row("No material available for this joint")
                return
            cols = _format_material_preview(material)
            if not cols:
                table.add_columns("Info")
                table.add_row("Material has no schema")
                return
            table.add_columns("Column", "Type")
            for col_name, col_type in cols:
                table.add_row(col_name, col_type)

        def _refresh_all(self) -> None:
            self._refresh_joint_list()
            self._refresh_material_preview()
            self.query_one("#debug-status", Label).update(self._status_text())

        def action_step(self) -> None:
            self._state.step()
            self._refresh_all()

        def action_continue_run(self) -> None:
            self._state.continue_to_breakpoint()
            self._refresh_all()

        def action_stop_debug(self) -> None:
            self._state.stop()
            self._refresh_all()
            self.dismiss("stop")

        def action_toggle_breakpoint(self) -> None:
            lv = self.query_one("#joint-list", ListView)
            highlighted = lv.highlighted_child
            if highlighted is not None:
                name = getattr(highlighted, "data", None)
                if name:
                    self._state.toggle_breakpoint(name)
                    self._refresh_joint_list()
                    self.query_one("#debug-status", Label).update(self._status_text())

        def action_dismiss_screen(self) -> None:
            self.dismiss(None)

else:  # pragma: no cover

    class DebugScreen:  # type: ignore[no-redef]
        """Stub when Textual is not installed."""

        def __init__(self, state: DebugState) -> None:
            self._state = state

        @property
        def state(self) -> DebugState:
            return self._state
