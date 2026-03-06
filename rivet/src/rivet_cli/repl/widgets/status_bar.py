"""StatusBar widget for the Rivet REPL TUI.

Displays: active profile name, default engine, dialect, compilation status
(compiling / compiled with timing / compile error), connection indicator,
and file watch state.

Reacts to:
  - ProjectCompiled   — update compilation status and timing
  - ExecutionStarted  — show executing joint name and progress
  - ExecutionProgress — update progress counter and row count
  - ExecutionComplete — clear execution state, show final status
  - ProfileChanged    — update profile name and engine

Requirements: 3.2, 3.4, 7.1, 7.2, 10.3, 18.3, 19.2, 23.4
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

try:
    from textual.app import ComposeResult
    from textual.message import Message
    from textual.reactive import reactive
    from textual.widget import Widget
    from textual.widgets import Label

    _TEXTUAL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _TEXTUAL_AVAILABLE = False

from rivet_core.interactive.types import Activity_State

# ---------------------------------------------------------------------------
# Messages (defined unconditionally so they can be imported and posted
# without a running Textual app — e.g. from worker threads or tests)
# ---------------------------------------------------------------------------

if _TEXTUAL_AVAILABLE:

    class ProjectCompiled(Message):
        """Posted when the project has been compiled (or failed to compile).

        Attributes:
            success:    True if compilation succeeded.
            elapsed_ms: Compilation time in milliseconds (0 on failure).
            error:      Error message string on failure, None on success.
            compilation_stats: Optional CompilationStats from the compiled assembly.
        """

        def __init__(
            self,
            *,
            success: bool,
            elapsed_ms: float = 0.0,
            error: str | None = None,
            compilation_stats: object | None = None,
        ) -> None:
            super().__init__()
            self.success = success
            self.elapsed_ms = elapsed_ms
            self.error = error
            self.compilation_stats = compilation_stats

    class ExecutionStarted(Message):
        """Posted when pipeline/joint execution begins.

        Attributes:
            joint_name: Name of the first joint being executed.
            total:      Total number of joints to execute.
        """

        def __init__(self, *, joint_name: str, total: int) -> None:
            super().__init__()
            self.joint_name = joint_name
            self.total = total

    class ExecutionProgress(Message):
        """Posted for each joint progress update during execution.

        Attributes:
            joint_name: Name of the joint currently executing.
            current:    1-based index of the current joint.
            total:      Total number of joints.
            rows:       Row count produced so far (None if unknown).
            elapsed_ms: Elapsed time in milliseconds.
        """

        def __init__(
            self,
            *,
            joint_name: str,
            current: int,
            total: int,
            rows: int | None,
            elapsed_ms: float,
        ) -> None:
            super().__init__()
            self.joint_name = joint_name
            self.current = current
            self.total = total
            self.rows = rows
            self.elapsed_ms = elapsed_ms

    class ExecutionComplete(Message):
        """Posted when execution finishes (success, failure, or cancellation).

        Attributes:
            success:    True if execution completed without errors.
            canceled:   True if the user canceled the execution.
            elapsed_ms: Total execution time in milliseconds.
            rows:       Total row count produced (None if unknown or not applicable).
            error:      Error message on failure, None otherwise.
        """

        def __init__(
            self,
            *,
            success: bool,
            canceled: bool = False,
            elapsed_ms: float = 0.0,
            rows: int | None = None,
            error: str | None = None,
        ) -> None:
            super().__init__()
            self.success = success
            self.canceled = canceled
            self.elapsed_ms = elapsed_ms
            self.rows = rows
            self.error = error

    class ProfileChanged(Message):
        """Posted when the active profile is switched.

        Attributes:
            profile_name: New profile name.
            engine_name:  Default engine name for the new profile (empty string if unknown).
            connected:    True if catalog connections succeeded.
        """

        def __init__(
            self,
            *,
            profile_name: str,
            engine_name: str = "",
            connected: bool = True,
        ) -> None:
            super().__init__()
            self.profile_name = profile_name
            self.engine_name = engine_name
            self.connected = connected

    class OpenEngineSelector(Message):
        """Posted when the user clicks the engine indicator to open the selector."""

    class OpenDialectSelector(Message):
        """Posted when the user clicks the dialect indicator to open the selector."""

    class ActivityChanged(Message):
        """Posted when the session activity state changes (IDLE/COMPILING/EXECUTING).

        Attributes:
            state: The new Activity_State value.
        """

        def __init__(self, state: Activity_State) -> None:
            super().__init__()
            self.state = state

    class ExecutionRejected(Message):
        """Posted when a submission is rejected by the execution guard.

        Attributes:
            reason: Human-readable rejection reason.
        """

        def __init__(self, reason: str) -> None:
            self.reason = reason
            super().__init__()

    # -----------------------------------------------------------------------
    # StatusBar widget
    # -----------------------------------------------------------------------

    class StatusBar(Widget):
        """Top status bar for the Rivet REPL.

        Displays profile, engine, compilation status, connection indicator,
        and file watch state.  Reacts to the five REPL messages defined above.
        """

        DEFAULT_CSS = """
        StatusBar {
            height: 1;
            background: $panel;
            color: $text;
            layout: horizontal;
        }
        StatusBar Label {
            height: 1;
            padding: 0 1;
        }
        StatusBar .status-profile {
            color: $accent;
        }
        StatusBar .status-engine {
            color: $text-muted;
        }
        StatusBar .status-engine:hover {
            color: $accent;
            text-style: underline;
        }
        StatusBar .status-dialect {
            color: $text-muted;
        }
        StatusBar .status-dialect:hover {
            color: $accent;
            text-style: underline;
        }
        StatusBar .status-compile-ok {
            color: $success;
        }
        StatusBar .status-compile-error {
            color: $error;
        }
        StatusBar .status-compiling {
            color: $warning;
        }
        StatusBar .status-executing {
            color: $warning;
        }
        StatusBar .status-canceled {
            color: $warning;
        }
        StatusBar .status-connection-ok {
            color: $success;
        }
        StatusBar .status-connection-fail {
            color: $error;
        }
        StatusBar .status-watch {
            color: $text-muted;
        }
        StatusBar .status-activity {
            color: $warning;
        }
        """

        # Reactive state — changes automatically re-render the widget.
        _profile: reactive[str] = reactive("default")
        _adhoc_engine: reactive[str | None] = reactive(None)
        _engine_names: reactive[tuple[str, ...]] = reactive(())
        _dialect: reactive[str | None] = reactive(None)
        _inferred_dialect: reactive[str | None] = reactive(None)
        _compile_label: reactive[str] = reactive("⚡ compiled")
        _compile_class: reactive[str] = reactive("status-compile-ok")
        _exec_label: reactive[str] = reactive("")
        _activity_label: reactive[str] = reactive("")
        _connected: reactive[bool] = reactive(True)
        _watching: reactive[bool] = reactive(True)

        def __init__(
            self,
            profile: str = "default",
            engine: str = "",
            watching: bool = True,
            adhoc_engine: str | None = None,
            engine_names: list[str] | None = None,
            engine_types: dict[str, str] | None = None,
            dialect: str | None = None,
            inferred_dialect: str | None = None,
            **kwargs: object,
        ) -> None:
            super().__init__(**kwargs)  # type: ignore[arg-type]
            self._profile = profile
            self._adhoc_engine = adhoc_engine or (engine if engine else None)
            self._engine_names = tuple(engine_names) if engine_names else ()
            self._engine_types: dict[str, str] = dict(engine_types) if engine_types else {}
            self._watching = watching
            self._dialect = dialect
            self._inferred_dialect = inferred_dialect

        # ------------------------------------------------------------------
        # Compose
        # ------------------------------------------------------------------

        def compose(self) -> ComposeResult:
            yield Label(self._profile_text(), classes="status-profile", id="sb-profile")
            yield Label(self._engine_text(), classes="status-engine", id="sb-engine")
            yield Label(self._dialect_text(), classes="status-dialect", id="sb-dialect")
            yield Label("", classes="status-activity", id="sb-activity")
            yield Label(self._compile_label, classes=self._compile_class, id="sb-compile")
            yield Label("", id="sb-exec")
            yield Label(self._connection_text(), id="sb-connection")
            yield Label(self._watch_text(), classes="status-watch", id="sb-watch")

        # ------------------------------------------------------------------
        # Helpers
        # ------------------------------------------------------------------

        def _profile_text(self) -> str:
            return f"● {self._profile}"

        def _engine_text(self) -> str:
            if self._adhoc_engine is not None:
                etype = self._engine_types.get(self._adhoc_engine, "")
                suffix = f" ({etype})" if etype else ""
                return f"⚙ {self._adhoc_engine}{suffix}"
            if self._engine_names:
                name = self._engine_names[0]
                return f"⚙ {name} (default)"
            return "⚙ no engine"

        def _dialect_text(self) -> str:
            if self._dialect is not None:
                return f"📝 {self._dialect}"
            if self._inferred_dialect is not None:
                return f"📝 {self._inferred_dialect} (auto)"
            return ""

        def _connection_text(self) -> str:
            return "⬤ connected" if self._connected else "⬤ disconnected"

        def _watch_text(self) -> str:
            return "👁 watching" if self._watching else ""

        # ------------------------------------------------------------------
        # Reactive watchers — keep labels in sync with reactive state
        # ------------------------------------------------------------------

        def watch__profile(self, value: str) -> None:
            if self.is_mounted:
                self._refresh_label("sb-profile", self._profile_text(), "status-profile")

        def watch__adhoc_engine(self, value: str | None) -> None:
            if self.is_mounted:
                self._refresh_label("sb-engine", self._engine_text(), "status-engine")

        def watch__engine_names(self, value: tuple[str, ...]) -> None:
            if self.is_mounted:
                self._refresh_label("sb-engine", self._engine_text(), "status-engine")

        def watch__dialect(self, value: str | None) -> None:
            if self.is_mounted:
                self._refresh_label("sb-dialect", self._dialect_text(), "status-dialect")

        def watch__inferred_dialect(self, value: str | None) -> None:
            if self.is_mounted:
                self._refresh_label("sb-dialect", self._dialect_text(), "status-dialect")

        def watch__compile_label(self, value: str) -> None:
            if self.is_mounted:
                self.query_one("#sb-compile", Label).update(value)

        def watch__compile_class(self, value: str) -> None:
            if self.is_mounted:
                label = self.query_one("#sb-compile", Label)
                for cls in ("status-compile-ok", "status-compile-error", "status-compiling"):
                    label.remove_class(cls)
                label.add_class(value)

        def watch__exec_label(self, value: str) -> None:
            if self.is_mounted:
                self.query_one("#sb-exec", Label).update(value)

        def watch__activity_label(self, value: str) -> None:
            if self.is_mounted:
                self.query_one("#sb-activity", Label).update(value)

        def watch__connected(self, value: bool) -> None:
            if self.is_mounted:
                label = self.query_one("#sb-connection", Label)
                label.update(self._connection_text())
                for cls in ("status-connection-ok", "status-connection-fail"):
                    label.remove_class(cls)
                label.add_class("status-connection-ok" if value else "status-connection-fail")

        def watch__watching(self, value: bool) -> None:
            if self.is_mounted:
                self._refresh_label("sb-watch", self._watch_text(), "status-watch")

        def _refresh_label(self, label_id: str, text: str, css_class: str) -> None:
            label = self.query_one(f"#{label_id}", Label)
            label.update(text)

        # ------------------------------------------------------------------
        # Public API — called by the app when state changes outside messages
        # ------------------------------------------------------------------

        def set_watching(self, watching: bool) -> None:
            """Update file watch indicator."""
            self._watching = watching

        def set_recompiling(self) -> None:
            """Show 'recompiling…' state (e.g. triggered by file watcher)."""
            self._compile_label = "⟳ recompiling…"
            self._compile_class = "status-compiling"

        def set_engine(
            self,
            adhoc_engine: str | None = None,
            engine_names: list[str] | None = None,
            engine_types: dict[str, str] | None = None,
        ) -> None:
            """Update engine indicator state."""
            self._adhoc_engine = adhoc_engine
            if engine_names is not None:
                self._engine_names = tuple(engine_names)
            if engine_types is not None:
                self._engine_types = dict(engine_types)

        def set_dialect(
            self,
            dialect: str | None = None,
            inferred_dialect: str | None = None,
        ) -> None:
            """Update dialect indicator state.

            Args:
                dialect: Explicit dialect set by user (None = no override).
                inferred_dialect: Engine-inferred dialect (shown with '(auto)' suffix).
            """
            self._dialect = dialect
            if inferred_dialect is not None:
                self._inferred_dialect = inferred_dialect

        def on_click(self, event: Any) -> None:
            """If click is on engine indicator, post OpenEngineSelector.
            If click is on dialect indicator, post OpenDialectSelector."""
            try:
                engine_label = self.query_one("#sb-engine", Label)
                dialect_label = self.query_one("#sb-dialect", Label)
            except Exception:  # noqa: BLE001
                return
            if engine_label.region.contains_point(event.screen_offset):
                self.post_message(OpenEngineSelector())
            elif dialect_label.region.contains_point(event.screen_offset):
                self.post_message(OpenDialectSelector())

        # ------------------------------------------------------------------
        # Message handlers
        # ------------------------------------------------------------------

        def on_project_compiled(self, message: ProjectCompiled) -> None:
            """React to ProjectCompiled — update compilation status."""
            if message.success:
                cs = message.compilation_stats
                if cs is not None:
                    self._compile_label = (
                        f"⚡ compiled ({cs.joints_with_schema}/{cs.joints_total} schemas)"  # type: ignore[attr-defined]
                        f" {cs.compile_duration_ms}ms"  # type: ignore[attr-defined]
                        f" [introspection: {cs.introspection_succeeded} ok,"  # type: ignore[attr-defined]
                        f" {cs.introspection_failed} failed,"  # type: ignore[attr-defined]
                        f" {cs.introspection_skipped} skipped]"  # type: ignore[attr-defined]
                    )
                else:
                    elapsed_s = message.elapsed_ms / 1000.0
                    self._compile_label = f"⚡ compiled ({elapsed_s:.1f}s)"
                self._compile_class = "status-compile-ok"
            else:
                self._compile_label = "✗ compile error"
                self._compile_class = "status-compile-error"
            # Clear any execution label once compilation finishes
            self._exec_label = ""

        def on_execution_started(self, message: ExecutionStarted) -> None:
            """React to ExecutionStarted — show executing state."""
            self._exec_label = f"⏳ executing {message.joint_name} (1/{message.total})"

        def on_execution_progress(self, message: ExecutionProgress) -> None:
            """React to ExecutionProgress — update progress counter."""
            rows_part = f"  │  {message.rows:,} rows" if message.rows is not None else ""
            elapsed_s = message.elapsed_ms / 1000.0
            self._exec_label = (
                f"⏳ executing {message.joint_name} "
                f"({message.current}/{message.total})"
                f"{rows_part}  │  {elapsed_s:.1f}s"
            )

        def on_execution_complete(self, message: ExecutionComplete) -> None:
            """React to ExecutionComplete — flash result summary for 3 seconds."""
            if message.canceled:
                self._exec_label = "⚠ execution canceled"
            elif message.success and message.rows is not None:
                import asyncio

                elapsed_ms = round(message.elapsed_ms)
                self._exec_label = f"✓ {message.rows:,} rows · {elapsed_ms}ms"
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None
                if loop is not None:
                    self.set_timer(3.0, self._clear_flash)
            else:
                self._exec_label = ""

        def _clear_flash(self) -> None:
            """Restore steady-state display after flash timer fires."""
            self._exec_label = ""

        def on_profile_changed(self, message: ProfileChanged) -> None:
            """React to ProfileChanged — update profile and engine display."""
            self._profile = message.profile_name
            if message.engine_name:
                self._adhoc_engine = message.engine_name
            self._connected = message.connected

        def on_activity_changed(self, message: ActivityChanged) -> None:
            """React to ActivityChanged — update activity indicator."""
            _LABELS = {
                Activity_State.IDLE: "",
                Activity_State.COMPILING: "⟳ Compiling…",
                Activity_State.EXECUTING: "⟳ Executing…",
            }
            self._activity_label = _LABELS.get(message.state, "")

        def on_execution_rejected(self, message: ExecutionRejected) -> None:
            """React to ExecutionRejected — show transient notification."""
            self.notify(
                "⚠ Execution in progress — cancel first (Ctrl+C)",
                severity="warning",
            )

else:  # pragma: no cover — Textual not installed

    @dataclass
    class ProjectCompiled:  # type: ignore[no-redef]
        success: bool
        elapsed_ms: float = 0.0
        error: str | None = None
        compilation_stats: object | None = None

    @dataclass
    class ExecutionStarted:  # type: ignore[no-redef]
        joint_name: str
        total: int

    @dataclass
    class ExecutionProgress:  # type: ignore[no-redef]
        joint_name: str
        current: int
        total: int
        rows: int | None
        elapsed_ms: float

    @dataclass
    class ExecutionComplete:  # type: ignore[no-redef]
        success: bool
        canceled: bool = False
        elapsed_ms: float = 0.0
        rows: int | None = None
        error: str | None = None

    @dataclass
    class ProfileChanged:  # type: ignore[no-redef]
        profile_name: str
        engine_name: str = ""
        connected: bool = True

    @dataclass
    class OpenEngineSelector:  # type: ignore[no-redef]
        pass

    @dataclass
    class OpenDialectSelector:  # type: ignore[no-redef]
        pass

    @dataclass
    class ActivityChanged:  # type: ignore[no-redef]
        state: Activity_State = Activity_State.IDLE

    @dataclass
    class ExecutionRejected:  # type: ignore[no-redef]
        reason: str = ""

    class StatusBar:  # type: ignore[no-redef]
        """Stub StatusBar for environments without Textual installed."""

        def __init__(
            self,
            profile: str = "default",
            engine: str = "",
            watching: bool = True,
            adhoc_engine: str | None = None,
            engine_names: list[str] | None = None,
            engine_types: dict[str, str] | None = None,
            dialect: str | None = None,
            inferred_dialect: str | None = None,
            **kwargs: object,
        ) -> None:
            self._profile = profile
            self._adhoc_engine = adhoc_engine or (engine if engine else None)
            self._engine_names = tuple(engine_names) if engine_names else ()
            self._engine_types: dict[str, str] = dict(engine_types) if engine_types else {}
            self._watching = watching
            self._compile_label = "⚡ compiled"
            self._compile_class = "status-compile-ok"
            self._exec_label = ""
            self._activity_label = ""
            self._connected = True
            self._dialect = dialect
            self._inferred_dialect = inferred_dialect

        def _engine_text(self) -> str:
            if self._adhoc_engine is not None:
                etype = self._engine_types.get(self._adhoc_engine, "")
                suffix = f" ({etype})" if etype else ""
                return f"⚙ {self._adhoc_engine}{suffix}"
            if self._engine_names:
                name = self._engine_names[0]
                return f"⚙ {name} (default)"
            return "⚙ no engine"

        def set_watching(self, watching: bool) -> None:
            self._watching = watching

        def set_recompiling(self) -> None:
            self._compile_label = "⟳ recompiling…"
            self._compile_class = "status-compiling"

        def set_engine(
            self,
            adhoc_engine: str | None = None,
            engine_names: list[str] | None = None,
            engine_types: dict[str, str] | None = None,
        ) -> None:
            self._adhoc_engine = adhoc_engine
            if engine_names is not None:
                self._engine_names = tuple(engine_names)
            if engine_types is not None:
                self._engine_types = dict(engine_types)

        def _dialect_text(self) -> str:
            if self._dialect is not None:
                return f"📝 {self._dialect}"
            if self._inferred_dialect is not None:
                return f"📝 {self._inferred_dialect} (auto)"
            return ""

        def set_dialect(
            self,
            dialect: str | None = None,
            inferred_dialect: str | None = None,
        ) -> None:
            self._dialect = dialect
            if inferred_dialect is not None:
                self._inferred_dialect = inferred_dialect

        def on_project_compiled(self, message: ProjectCompiled) -> None:
            if message.success:
                cs = message.compilation_stats
                if cs is not None:
                    self._compile_label = (
                        f"⚡ compiled ({cs.joints_with_schema}/{cs.joints_total} schemas)"  # type: ignore[attr-defined]
                        f" {cs.compile_duration_ms}ms"  # type: ignore[attr-defined]
                        f" [introspection: {cs.introspection_succeeded} ok,"  # type: ignore[attr-defined]
                        f" {cs.introspection_failed} failed,"  # type: ignore[attr-defined]
                        f" {cs.introspection_skipped} skipped]"  # type: ignore[attr-defined]
                    )
                else:
                    elapsed_s = message.elapsed_ms / 1000.0
                    self._compile_label = f"⚡ compiled ({elapsed_s:.1f}s)"
                self._compile_class = "status-compile-ok"
            else:
                self._compile_label = "✗ compile error"
                self._compile_class = "status-compile-error"
            self._exec_label = ""

        def on_execution_started(self, message: ExecutionStarted) -> None:
            self._exec_label = f"⏳ executing {message.joint_name} (1/{message.total})"

        def on_execution_progress(self, message: ExecutionProgress) -> None:
            rows_part = f"  │  {message.rows:,} rows" if message.rows is not None else ""
            elapsed_s = message.elapsed_ms / 1000.0
            self._exec_label = (
                f"⏳ executing {message.joint_name} "
                f"({message.current}/{message.total})"
                f"{rows_part}  │  {elapsed_s:.1f}s"
            )

        def on_execution_complete(self, message: ExecutionComplete) -> None:
            if message.canceled:
                self._exec_label = "⚠ execution canceled"
            elif message.success and message.rows is not None:
                elapsed_ms = round(message.elapsed_ms)
                self._exec_label = f"✓ {message.rows:,} rows · {elapsed_ms}ms"
            else:
                self._exec_label = ""

        def _clear_flash(self) -> None:
            self._exec_label = ""

        def on_profile_changed(self, message: ProfileChanged) -> None:
            self._profile = message.profile_name
            if message.engine_name:
                self._adhoc_engine = message.engine_name
            self._connected = message.connected

        def on_activity_changed(self, message: ActivityChanged) -> None:
            _LABELS = {
                Activity_State.IDLE: "",
                Activity_State.COMPILING: "⟳ Compiling…",
                Activity_State.EXECUTING: "⟳ Executing…",
            }
            self._activity_label = _LABELS.get(message.state, "")
