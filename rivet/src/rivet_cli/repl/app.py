"""RivetRepl — main Textual App for the Rivet REPL TUI.

Three-panel layout: CatalogPanel (left), EditorPanel (top-right),
ResultsPanel (bottom-right), with StatusBar (top) and Footer (bottom).

Global keybindings:
  Ctrl+Q   — quit (with unsaved-buffer prompt)
  Ctrl+B   — toggle catalog panel
  Ctrl+\\   — toggle results panel (full-screen editor)
  F11      — toggle fullscreen for the focused panel
  F6       — execute full pipeline
  Ctrl+C   — cancel running execution
  Tab      — cycle focus: CatalogPanel → EditorPanel → ResultsPanel
  Shift+Tab — reverse focus cycle
  Escape   — return focus to EditorPanel (known state)

Requirements: 1.2, 1.3, 1.5, 2.1, 2.2, 2.3, 3.1, 3.6, 3.7, 3.8, 18.1, 18.2, 18.3, 18.4, 18.5, 19.1, 19.2, 34.1, 34.3, 34.4, 35.1, 35.2, 35.3
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING

from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.screen import ModalScreen
from textual.widgets import Footer, Static

from .accessibility import (
    ARIA_CATALOG_PANEL,
    ARIA_EDITOR_PANEL,
    ARIA_RESULTS_PANEL,
    ARIA_STATUS_BAR,
    PANEL_TAB_ORDER,
)
from .errors import ReplError, make_repl_error
from .file_watcher import FileWatcher
from .keymap import load_keymap
from .screens.dialect_selector import DialectSelectorScreen
from .screens.engine_selector import EngineSelectorScreen
from .screens.error_modal import ErrorModal
from .screens.profile_selector import ProfileSelectorScreen
from .widgets.catalog import CatalogPanel
from .widgets.command_input import CommandSubmitted
from .widgets.compilation_view import CompilationView
from .widgets.editor import EditorPanel
from .widgets.logs_view import LogsView
from .widgets.results import PanelTab, ResultsPanel  # type: ignore[attr-defined]
from .widgets.results_tab_bar import ResultsTabBar
from .widgets.splitter import HorizontalSplitter, VerticalSplitter
from .widgets.status_bar import (
    ActivityChanged,
    ExecutionComplete,
    ExecutionProgress,
    ExecutionRejected,
    ExecutionStarted,
    OpenDialectSelector,
    OpenEngineSelector,
    ProfileChanged,
    ProjectCompiled,
    StatusBar,
)

if TYPE_CHECKING:
    from rivet_core.interactive.session import InteractiveSession
    from rivet_core.interactive.types import Execution_Log, QueryProgress

    from .config import ReplConfig

from rivet_core.interactive.session import ExecutionInProgressError
from rivet_core.interactive.types import Activity_State

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Confirmation screen for unsaved buffers
# ---------------------------------------------------------------------------


class QuitConfirmScreen(ModalScreen[bool]):
    """Modal asking the user to confirm quit with unsaved buffers."""

    BINDINGS = [
        Binding("y", "confirm", "Yes", show=True),
        Binding("n", "cancel", "No", show=True),
        Binding("escape", "cancel", "Cancel", show=True),
    ]

    DEFAULT_CSS = """
    QuitConfirmScreen {
        align: center middle;
    }
    QuitConfirmScreen #dialog {
        width: 50;
        height: 5;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
        content-align: center middle;
    }
    """

    def compose(self) -> ComposeResult:
        yield Static(
            "You have unsaved buffers. Quit anyway? [y/n]",
            id="dialog",
        )

    def action_confirm(self) -> None:
        self.dismiss(True)

    def action_cancel(self) -> None:
        self.dismiss(False)


# ---------------------------------------------------------------------------
# Log callback message
# ---------------------------------------------------------------------------


class LogReceived(Message):
    """Posted from the on_log callback (worker thread → main thread)."""

    def __init__(self, entry: Execution_Log) -> None:
        super().__init__()
        self.entry = entry


# ---------------------------------------------------------------------------
# Main App
# ---------------------------------------------------------------------------


class RivetRepl(App):  # type: ignore[type-arg]
    """Main Textual App for the Rivet REPL.

    Holds an InteractiveSession reference and delegates all logic to it.
    Widgets handle layout, styling, and user input only.
    """

    TITLE = "Rivet REPL"

    CSS = """
    #main-layout {
        height: 1fr;
    }
    #catalog-panel {
        width: 20%;
        min-width: 15;
        max-width: 40%;
    }
    #right-pane {
        width: 1fr;
    }
    #editor-panel {
        height: 1fr;
    }
    #results-panel {
        height: 1fr;
    }
    .hidden {
        display: none;
    }
    """

    BINDINGS = [
        Binding("ctrl+q", "request_quit", "Quit", show=True, priority=True),
        Binding("ctrl+b", "toggle_catalog", "Toggle Catalog", show=True),
        Binding("ctrl+backslash", "toggle_results", "Toggle Results", show=True),
        Binding("f11", "fullscreen_panel", "Fullscreen", show=True),
        Binding("ctrl+shift+p", "open_profile_selector", "Switch Profile", show=True),
        Binding("f6", "execute_pipeline", "Run Pipeline", show=True),
        Binding("ctrl+c", "cancel_execution", "Cancel", show=True, priority=True),
        Binding("tab", "focus_next_panel", "Next Panel", show=False, priority=False),
        Binding("shift+tab", "focus_prev_panel", "Prev Panel", show=False, priority=False),
        Binding("escape", "focus_editor", "Focus Editor", show=False, priority=False),
        Binding("ctrl+shift+e", "open_engine_selector", "Switch Engine", show=True),
        Binding("ctrl+shift+d", "open_dialect_selector", "Switch Dialect", show=True),
    ]

    def __init__(
        self,
        session: InteractiveSession,
        config: ReplConfig,
        editor_path: Path | None = None,
        initial_sql: str | None = None,
    ) -> None:
        super().__init__()
        self._session = session
        self._config = config
        self._editor_path = editor_path
        self._initial_sql = initial_sql
        self._file_watcher: FileWatcher | None = None

        # Load keymap from config; fall back to default with RVT-864 on failure
        keymap, error_code = load_keymap(config.keymap)
        self._keymap = keymap  # type: ignore[assignment]
        self._keymap_error: str | None = error_code

        # Wire log callback: worker thread → Textual message
        session.on_log = lambda entry: self.call_from_thread(  # type: ignore[assignment]
            self.post_message, LogReceived(entry)
        )

        # Wire activity state callback: worker thread → Textual message
        session.on_activity_change = lambda state: self.call_from_thread(  # type: ignore[assignment]
            self.post_message, ActivityChanged(state=state.value)  # type: ignore[arg-type]
        )

    # ------------------------------------------------------------------
    # Compose
    # ------------------------------------------------------------------

    def compose(self) -> ComposeResult:
        yield StatusBar(
            profile=self._session.active_profile,
            watching=self._config.file_watch,
            id="status-bar",
        )
        with Horizontal(id="main-layout"):
            yield CatalogPanel(
                self._session,
                id="catalog-panel",
            )
            yield VerticalSplitter(catalog_id="catalog-panel", id="catalog-splitter")
            with Vertical(id="right-pane"):
                yield EditorPanel(
                    session=self._session,
                    show_line_numbers=self._config.show_line_numbers,
                    tab_size=self._config.tab_size,
                    word_wrap=self._config.word_wrap,
                    id="editor-panel",
                )
                yield HorizontalSplitter(
                    top_id="editor-panel",
                    bottom_id="results-panel",
                    id="editor-results-splitter",
                )
                yield ResultsPanel(id="results-panel")
        yield Footer()

    def on_mount(self) -> None:
        if self._keymap_error:
            self.notify(
                f"{self._keymap_error}: keymap {self._config.keymap!r} not found; "
                f"using default keymap {self._keymap.name!r}",  # type: ignore[attr-defined]
                severity="warning",
            )

        # Apply ARIA-style labels (tooltips) to panels — Requirement 35.1
        self.query_one("#status-bar").tooltip = ARIA_STATUS_BAR
        self.query_one("#catalog-panel").tooltip = ARIA_CATALOG_PANEL
        self.query_one("#editor-panel").tooltip = ARIA_EDITOR_PANEL
        self.query_one("#results-panel").tooltip = ARIA_RESULTS_PANEL

        if self._editor_path is not None:
            editor = self.query_one("#editor-panel", EditorPanel)
            editor.open_joint_file(self._editor_path)

        # Restore editor SQL from ReplState if non-empty (Requirement 2.2)
        editor_sql = self._session.repl_state.editor_sql
        if editor_sql:
            editor = self.query_one("#editor-panel", EditorPanel)
            editor.restore_editor_sql(editor_sql)

        # Focus the SQL editor by default
        try:
            self.query_one("#editor-area").focus()
        except Exception:
            pass

        # --- Full startup flow (Requirement 1.2, 1.3, 1.5) ---
        # TUI is already rendered at this point (lazy panels, fast interactive state).
        # Now kick off background compilation and catalog introspection.
        self._run_startup()

        # If launched with initial SQL (e.g. from explore preview), load it
        if self._initial_sql is not None:
            editor = self.query_one("#editor-panel", EditorPanel)
            editor.set_query_and_execute(self._initial_sql)

    @work(thread=True)
    async def _run_startup(self) -> None:
        """Background startup: compile project, connect catalogs, start file watcher.

        Runs on a worker thread so the TUI remains interactive immediately.
        Catalog connection failures result in degraded mode (Requirement 1.3).
        """
        # Show compiling state in status bar
        self.call_from_thread(
            self.query_one("#status-bar", StatusBar).set_recompiling,
        )

        t0 = time.monotonic()
        success = True
        error_msg: str | None = None
        catalogs_connected = True
        catalog_infos: list = []  # type: ignore[type-arg]

        try:
            # Compile the project (session.start() was already called in __init__.py,
            # but we post the result as a ProjectCompiled message for the TUI)
            assembly = self._session.assembly

            # Check catalog connection status — degraded mode (Requirement 1.3)
            try:
                catalog_infos = self._session.get_catalogs()
                for cat in catalog_infos:
                    if not cat.connected:
                        catalogs_connected = False
                        logger.warning(
                            "Catalog %s: connection failed — degraded mode",
                            cat.name,
                        )
            except Exception:  # noqa: BLE001
                logger.debug("Catalog introspection failed during startup", exc_info=True)
                catalogs_connected = False

            # Update completion engine with catalog and assembly data
            try:
                if catalog_infos:
                    self._session.completion_engine.update_catalogs(catalog_infos)
            except Exception:  # noqa: BLE001
                logger.debug("Completion engine catalog update failed", exc_info=True)

            try:
                if assembly is not None:
                    self._session.completion_engine.update_assembly(assembly)  # type: ignore[arg-type]
            except Exception:  # noqa: BLE001
                logger.debug("Completion engine assembly update failed", exc_info=True)

            # Update catalog search index
            try:
                joints = self._session.get_joints()
                self._session._catalog_search.update(catalog_infos, joints)  # type: ignore[arg-type]
            except Exception:  # noqa: BLE001
                logger.debug("Catalog search index update failed", exc_info=True)

        except Exception as exc:  # noqa: BLE001
            success = False
            error_msg = str(exc)
            logger.debug("Startup compilation failed", exc_info=True)

        elapsed_ms = (time.monotonic() - t0) * 1000

        # Push engine state to status bar now that session is initialized
        self.call_from_thread(
            self.query_one("#status-bar", StatusBar).set_engine,
            adhoc_engine=self._session.adhoc_engine,
            engine_names=self._session.engine_names,
            engine_types=self._session.engine_types,
        )

        # Post ProjectCompiled message to update StatusBar and CatalogPanel
        cs = getattr(assembly, "compilation_stats", None) if assembly is not None else None
        self.call_from_thread(
            self.post_message,
            ProjectCompiled(success=success, elapsed_ms=elapsed_ms, error=error_msg, compilation_stats=cs),
        )

        # Update connection status in status bar if catalogs failed
        if not catalogs_connected:
            self.call_from_thread(
                self.post_message,
                ProfileChanged(
                    profile_name=self._session.active_profile,
                    engine_name="",
                    connected=False,
                ),
            )

        # Start file watcher if enabled (Requirement 1.2)
        if self._config.file_watch:
            self._start_file_watcher()

    @property
    def active_keymap(self):  # type: ignore[no-untyped-def]
        """The currently active Keymap instance."""
        return self._keymap

    # ------------------------------------------------------------------
    # File watcher lifecycle
    # ------------------------------------------------------------------

    def _start_file_watcher(self) -> None:
        """Start the background file watcher."""
        self._file_watcher = FileWatcher(
            project_path=self._session._project_path,
            session=self._session,
            on_compiled=self._on_file_watcher_compiled,
        )
        self._file_watcher.start()
        self.call_from_thread(
            self.query_one("#status-bar", StatusBar).set_watching,
            True,
        )

    def _on_file_watcher_compiled(
        self, success: bool, elapsed_ms: float, error: str | None
    ) -> None:
        """Callback from file watcher after recompilation (runs on worker thread)."""
        cs = None
        assembly = getattr(self, "_session", None) and getattr(self._session, "assembly", None)
        if assembly is not None:
            cs = getattr(assembly, "compilation_stats", None)
        self.call_from_thread(
            self.post_message,
            ProjectCompiled(success=success, elapsed_ms=elapsed_ms, error=error, compilation_stats=cs),
        )
        # Refresh catalog panel after recompilation
        if success:
            try:
                catalog_panel = self.query_one("#catalog-panel", CatalogPanel)
                self.call_from_thread(catalog_panel.refresh_tree)
            except Exception:  # noqa: BLE001
                pass

    def _stop_file_watcher(self) -> None:
        """Stop the background file watcher."""
        if self._file_watcher is not None:
            self._file_watcher.stop()
            self._file_watcher = None

    # ------------------------------------------------------------------
    # Keybinding actions
    # ------------------------------------------------------------------

    def action_request_quit(self) -> None:
        """Ctrl+Q — quit with unsaved-buffer check."""
        editor = self.query_one("#editor-panel", EditorPanel)
        has_unsaved = any(t.dirty and t.path is not None for t in editor.tabs)
        if has_unsaved:
            self.push_screen(QuitConfirmScreen(), self._on_quit_confirmed)  # type: ignore[arg-type]
        else:
            self._stop_file_watcher()
            self.exit()

    def _on_quit_confirmed(self, confirmed: bool) -> None:
        if confirmed:
            self._stop_file_watcher()
            self.exit()

    def action_toggle_catalog(self) -> None:
        """Ctrl+B — toggle catalog panel visibility."""
        panel = self.query_one("#catalog-panel", CatalogPanel)
        panel.toggle_class("hidden")

    def action_toggle_results(self) -> None:
        """Ctrl+\\ — toggle results panel (full-screen editor)."""
        panel = self.query_one("#results-panel", ResultsPanel)
        panel.toggle_class("hidden")

    def action_fullscreen_panel(self) -> None:
        """F11 — toggle fullscreen for the currently focused panel."""
        focused = self.focused
        if focused is None:
            return
        # Walk up to find the panel-level widget
        widget = focused
        while widget is not None:
            if widget.id in ("catalog-panel", "editor-panel", "results-panel"):
                break
            widget = widget.parent  # type: ignore[assignment]
        if widget is None:
            return

        panel_ids = ("catalog-panel", "editor-panel", "results-panel")
        is_fullscreen = all(
            self.query_one(f"#{pid}").has_class("hidden")
            for pid in panel_ids
            if pid != widget.id
        )
        for pid in panel_ids:
            target = self.query_one(f"#{pid}")
            if pid == widget.id or is_fullscreen:
                target.remove_class("hidden")
            else:
                target.add_class("hidden")

    def action_open_profile_selector(self) -> None:
        """Ctrl+Shift+P — open profile selector overlay."""
        self.push_screen(
            ProfileSelectorScreen(
                session=self._session,
                project_path=self._session._project_path,
            ),
            self._on_profile_selected,
        )

    def _on_profile_selected(self, profile_name: str | None) -> None:
        """Handle profile selection from the overlay."""
        if profile_name is None or profile_name == self._session.active_profile:
            return
        try:
            self._session.switch_profile(profile_name)
        except Exception as exc:  # noqa: BLE001
            # RVT-867: profile switch failed — remain on current profile
            self.show_repl_error("RVT-867", detail=str(exc))
            return
        self.post_message(
            ProfileChanged(
                profile_name=self._session.active_profile,
                engine_name="",
                connected=True,
            )
        )

    def on_open_engine_selector(self, message: OpenEngineSelector) -> None:
        """Handle click on engine indicator in status bar."""
        self.action_open_engine_selector()

    def on_open_dialect_selector(self, message: OpenDialectSelector) -> None:
        """Handle click on dialect indicator in status bar."""
        self.action_open_dialect_selector()

    def action_open_engine_selector(self) -> None:
        """Ctrl+Shift+E — open engine selector overlay."""
        self.push_screen(
            EngineSelectorScreen(
                engine_names=self._session.engine_names,
                current_engine=self._session.adhoc_engine,
            ),
            self._on_engine_selected,
        )

    def _on_engine_selected(self, engine_name: str | None) -> None:
        """Handle engine selection from the overlay."""
        if engine_name is None:
            return
        try:
            self._session.adhoc_engine = engine_name
        except Exception as exc:  # noqa: BLE001
            self.show_repl_error("RVT-868", detail=str(exc))
            return
        self.query_one("#status-bar", StatusBar).set_engine(
            adhoc_engine=engine_name,
            engine_names=self._session.engine_names,
            engine_types=self._session.engine_types,
        )
        self.post_message(
            ProfileChanged(
                profile_name=self._session.active_profile,
                engine_name=engine_name,
                connected=True,
            )
        )

    def action_open_dialect_selector(self) -> None:
        """Ctrl+Shift+D — open dialect selector overlay."""
        self.push_screen(
            DialectSelectorScreen(
                dialect_names=self._session.dialect_names,
                current_dialect=self._session.repl_state.dialect,
            ),
            self._on_dialect_selected,
        )

    def _on_dialect_selected(self, dialect: str | None) -> None:
        """Handle dialect selection from the overlay."""
        try:
            self._session.set_dialect(dialect)
        except Exception as exc:  # noqa: BLE001
            self.notify(str(exc), severity="error")
            return
        self.query_one("#status-bar", StatusBar).set_dialect(dialect)
        try:
            self.query_one("#editor-panel", EditorPanel).revalidate()
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------
    # Pipeline / joint execution
    # ------------------------------------------------------------------

    def _make_progress_callback(self, t0: float, total: int):  # type: ignore[no-untyped-def]
        """Return a progress callback that posts messages to the app."""

        def on_progress(progress: QueryProgress) -> None:
            self.call_from_thread(
                self.post_message,
                ExecutionProgress(
                    joint_name=progress.joint_name,
                    current=progress.current,
                    total=progress.total,
                    rows=progress.rows,
                    elapsed_ms=progress.elapsed_ms,
                ),
            )
            # Forward progress to ResultsPanel
            try:
                self.call_from_thread(
                    self.query_one("#results-panel", ResultsPanel).update_progress,
                    progress,
                )
            except Exception:  # noqa: BLE001
                pass
            # Update joint status in catalog panel
            status = "executing" if progress.status == "executing" else progress.status
            self.call_from_thread(
                self.query_one("#catalog-panel", CatalogPanel).update_joint_status,
                progress.joint_name,
                status,
            )

        return on_progress

    def action_execute_pipeline(self) -> None:
        """F6 — execute the full pipeline with streaming progress."""
        self._run_pipeline()

    @work(thread=True)
    async def _run_pipeline(self) -> None:
        """Execute the full pipeline on a worker thread."""
        joints = self._session.get_joints()
        total = len(joints) if joints else 1
        t0 = time.monotonic()

        # Clear previous execution state
        self.call_from_thread(self._clear_execution_state)

        self.call_from_thread(
            self.post_message,
            ExecutionStarted(
                joint_name=joints[0].name if joints else "pipeline",
                total=total,
            ),
        )

        # Show progress indicator in results panel
        try:
            self.call_from_thread(
                self.query_one("#results-panel", ResultsPanel).show_progress,
            )
        except Exception:  # noqa: BLE001
            pass

        try:
            result = self._session.execute_pipeline(
                on_progress=self._make_progress_callback(t0, total),
            )
            elapsed = (time.monotonic() - t0) * 1000

            # Update joint statuses to success
            for name in result.joints_executed:
                self.call_from_thread(
                    self.query_one("#catalog-panel", CatalogPanel).update_joint_status,
                    name,
                    "success",
                )

            self.call_from_thread(
                self.post_message,
                ExecutionComplete(
                    success=result.success,
                    elapsed_ms=elapsed,
                    error=result.errors[0] if result.errors else None,
                ),
            )
        except ExecutionInProgressError as exc:
            self.call_from_thread(
                self.post_message,
                ExecutionRejected(reason=str(exc)),
            )
        except Exception as exc:  # noqa: BLE001
            elapsed = (time.monotonic() - t0) * 1000
            canceled = "cancel" in str(exc).lower()
            self.call_from_thread(
                self.post_message,
                ExecutionComplete(
                    success=False,
                    canceled=canceled,
                    elapsed_ms=elapsed,
                    error=str(exc),
                ),
            )

    def action_cancel_execution(self) -> None:
        """Ctrl+C — cancel the currently running execution."""
        if self._session.activity_state == Activity_State.EXECUTING:
            self._session.cancel()

    # ------------------------------------------------------------------
    # Message handlers — CatalogPanel requests
    # ------------------------------------------------------------------

    def on_catalog_panel_execute_joint_requested(
        self, message: CatalogPanel.ExecuteJointRequested
    ) -> None:
        """Ctrl+Enter on a joint in the catalog panel."""
        self._run_joint(message.joint_name)

    def on_catalog_panel_quick_query(
        self, message: CatalogPanel.QuickQuery
    ) -> None:
        """F4 on a catalog node — put SQL in editor and execute from there."""
        self.query_one("#editor-panel", EditorPanel).set_query_and_execute(message.sql)

    def on_catalog_panel_joint_selected(
        self, message: CatalogPanel.JointSelected
    ) -> None:
        """Enter on a joint — open joint SQL in a read-only preview tab (Requirement 6.1)."""
        try:
            sql = self._session.get_joint_sql(message.joint_name)
        except Exception as exc:  # noqa: BLE001
            self.notify(str(exc), severity="error")
            return
        editor = self.query_one("#editor-panel", EditorPanel)
        editor.open_preview(title=message.joint_name, content=sql)

    def on_catalog_panel_joint_sql_extract_requested(
        self, message: CatalogPanel.JointSqlExtractRequested
    ) -> None:
        """Shift+Enter on a joint — extract SQL into a new editable ad-hoc tab."""
        try:
            sql = self._session.get_joint_sql(message.joint_name)
        except Exception as exc:  # noqa: BLE001
            self.notify(str(exc), severity="error")
            return
        editor = self.query_one("#editor-panel", EditorPanel)
        editor.new_ad_hoc_tab(title=f"[{message.joint_name}] (extracted)", content=sql)

    def on_catalog_panel_joint_preview_requested(
        self, message: CatalogPanel.JointPreviewRequested
    ) -> None:
        """Cursor rested on a joint node — show lightweight preview."""
        from rivet_core.interactive.types import JointPreviewData, SchemaField

        assembly = self._session.assembly
        if assembly is None:
            return

        # Find the compiled joint
        joint = None
        for j in assembly.joints:
            if j.name == message.joint_name:
                joint = j
                break
        if joint is None:
            return

        # Build schema fields from compiled joint output_schema
        schema = None
        if joint.output_schema is not None:
            schema = [
                SchemaField(name=c.name, type=c.type)
                for c in joint.output_schema.columns
            ]

        # Check MaterialCache for cached rows
        preview_rows = None
        cached = self._session._material_cache.get(message.joint_name)
        if cached is not None:
            try:
                table = cached.to_arrow()
                preview_rows = table.slice(0, 10)
            except Exception:  # noqa: BLE001
                pass

        preview = JointPreviewData(
            joint_name=joint.name,
            engine=joint.engine,
            fusion_group=joint.fused_group_id,
            upstream=list(joint.upstream),
            tags=list(joint.tags),
            schema=schema,
            preview_rows=preview_rows,
        )
        self.query_one("#results-panel", ResultsPanel).show_joint_preview(preview)

    def on_editor_panel_query_submitted(
        self, message: EditorPanel.QuerySubmitted
    ) -> None:
        """F5 in the editor — run ad-hoc SQL."""
        self._run_ad_hoc_query(message.sql)

    def _clear_execution_state(self) -> None:
        """Reset logs, compilation, and badge for a fresh execution cycle."""
        self._session.clear_logs()
        try:
            self.query_one(LogsView).clear()
        except Exception:  # noqa: BLE001
            pass
        try:
            self.query_one(CompilationView).show_placeholder()
        except Exception:  # noqa: BLE001
            pass
        try:
            panel = self.query_one("#results-panel", ResultsPanel)
            panel._log_badge_count = 0
            self.query_one(ResultsTabBar).log_badge_count = 0
        except Exception:  # noqa: BLE001
            pass

    @work(thread=True)
    async def _run_ad_hoc_query(self, sql: str) -> None:
        """Execute an ad-hoc SQL query on a worker thread."""
        t0 = time.monotonic()

        # Clear previous execution state
        self.call_from_thread(self._clear_execution_state)

        self.call_from_thread(
            self.post_message,
            ExecutionStarted(joint_name="ad-hoc query", total=1),
        )

        # Show progress indicator in results panel
        try:
            self.call_from_thread(
                self.query_one("#results-panel", ResultsPanel).show_progress,
            )
        except Exception:  # noqa: BLE001
            pass

        try:
            result = self._session.execute_query(
                sql,
                on_progress=self._make_progress_callback(t0, 1),
            )
            elapsed = (time.monotonic() - t0) * 1000

            # Show result in the results panel and switch to Result tab
            self.call_from_thread(
                self.query_one("#results-panel", ResultsPanel).show_query_result,
                result,
            )

            # Populate compilation tab with the transient compiled assembly
            try:
                assembly = result.query_plan.assembly if result.query_plan else None
                if assembly is not None:
                    self.call_from_thread(
                        self.query_one(CompilationView).show_assembly,
                        assembly,
                        1,
                    )
            except Exception:  # noqa: BLE001
                pass

            self.call_from_thread(
                self.post_message,
                ExecutionComplete(success=True, elapsed_ms=elapsed),
            )
        except ExecutionInProgressError as exc:
            self.call_from_thread(
                self.post_message,
                ExecutionRejected(reason=str(exc)),
            )
        except Exception as exc:  # noqa: BLE001
            elapsed = (time.monotonic() - t0) * 1000
            canceled = "cancel" in str(exc).lower()
            self.call_from_thread(
                self.post_message,
                ExecutionComplete(
                    success=False,
                    canceled=canceled,
                    elapsed_ms=elapsed,
                    error=str(exc),
                ),
            )

    @work(thread=True)
    async def _run_joint(self, joint_name: str) -> None:
        """Execute a single joint and its upstream on a worker thread."""
        t0 = time.monotonic()

        # Clear previous execution state
        self.call_from_thread(self._clear_execution_state)

        self.call_from_thread(
            self.post_message,
            ExecutionStarted(joint_name=joint_name, total=1),
        )
        # Show progress indicator in results panel
        try:
            self.call_from_thread(
                self.query_one("#results-panel", ResultsPanel).show_progress,
            )
        except Exception:  # noqa: BLE001
            pass
        self.call_from_thread(
            self.query_one("#catalog-panel", CatalogPanel).update_joint_status,
            joint_name,
            "executing",
        )

        try:
            result = self._session.execute_joint(
                joint_name,
                on_progress=self._make_progress_callback(t0, 1),
            )
            elapsed = (time.monotonic() - t0) * 1000

            self.call_from_thread(
                self.query_one("#catalog-panel", CatalogPanel).update_joint_status,
                joint_name,
                "success",
            )

            # Wire quality check results to catalog panel
            if result.quality_results:
                catalog = self.query_one("#catalog-panel", CatalogPanel)
                for qr in result.quality_results:
                    self.call_from_thread(
                        catalog.update_check_result,
                        joint_name,
                        qr.type,
                        qr.passed,
                        qr.message or "",
                    )

            self.call_from_thread(
                self.post_message,
                ExecutionComplete(success=True, elapsed_ms=elapsed),
            )
        except ExecutionInProgressError as exc:
            self.call_from_thread(
                self.post_message,
                ExecutionRejected(reason=str(exc)),
            )
        except Exception as exc:  # noqa: BLE001
            elapsed = (time.monotonic() - t0) * 1000
            canceled = "cancel" in str(exc).lower()
            self.call_from_thread(
                self.query_one("#catalog-panel", CatalogPanel).update_joint_status,
                joint_name,
                "failed",
            )
            self.call_from_thread(
                self.post_message,
                ExecutionComplete(
                    success=False,
                    canceled=canceled,
                    elapsed_ms=elapsed,
                    error=str(exc),
                ),
            )

    # ------------------------------------------------------------------
    # Accessibility — panel focus cycling (Requirements 35.2, 35.3)
    # ------------------------------------------------------------------

    def _visible_panel_ids(self) -> list[str]:
        """Return panel IDs that are currently visible, in tab order."""
        return [
            pid.lstrip("#")
            for pid in PANEL_TAB_ORDER
            if not self.query_one(pid).has_class("hidden")
        ]

    def _focused_panel_id(self) -> str | None:
        """Return the ID of the panel that contains the currently focused widget."""
        focused = self.focused
        if focused is None:
            return None
        widget = focused
        panel_ids = {pid.lstrip("#") for pid in PANEL_TAB_ORDER}
        while widget is not None:
            if widget.id in panel_ids:
                return widget.id
            widget = widget.parent  # type: ignore[assignment]
        return None

    def action_focus_next_panel(self) -> None:
        """Tab — cycle focus to the next visible panel."""
        visible = self._visible_panel_ids()
        if not visible:
            return
        current = self._focused_panel_id()
        if current is None or current not in visible:
            next_id = visible[0]
        else:
            idx = visible.index(current)
            next_id = visible[(idx + 1) % len(visible)]
        self.query_one(f"#{next_id}").focus()

    def action_focus_prev_panel(self) -> None:
        """Shift+Tab — cycle focus to the previous visible panel."""
        visible = self._visible_panel_ids()
        if not visible:
            return
        current = self._focused_panel_id()
        if current is None or current not in visible:
            prev_id = visible[-1]
        else:
            idx = visible.index(current)
            prev_id = visible[(idx - 1) % len(visible)]
        self.query_one(f"#{prev_id}").focus()

    def action_focus_editor(self) -> None:
        """Escape — return focus to the editor panel (known state)."""
        editor_panel = self.query_one("#editor-panel")
        if not editor_panel.has_class("hidden"):
            editor_panel.focus()

    # ------------------------------------------------------------------
    # Error display — Requirements 34.1, 34.3, 34.4
    # ------------------------------------------------------------------

    def _show_error_modal(self, error: ReplError, show_in_editor: bool = False) -> None:
        """Push an ErrorModal and handle its dismiss result (copy / show_editor)."""

        def _on_dismiss(result: str | None) -> None:
            if result == "copy":
                self.copy_to_clipboard(error.format_text())

        self.push_screen(ErrorModal(error=error, show_in_editor=show_in_editor), callback=_on_dismiss)

    def on_execution_complete(self, message: ExecutionComplete) -> None:
        """Show error modal on execution failure (Requirement 34.4)."""
        if not message.success and not message.canceled and message.error:
            error = ReplError(
                code="RVT-EXE",
                description=f"Execution error: {message.error}",
                remediation="Check the error details and fix the failing joint, then re-run.",
            )
            self._show_error_modal(error, show_in_editor=True)

    def on_project_compiled(self, message: ProjectCompiled) -> None:
        """Forward compilation result to CompilationView and show error modal on failure."""
        # Compilation tab is only populated during query execution.
        # Startup/file-change recompiles only show errors via modal.
        if not message.success and message.error:
            error = ReplError(
                code="RVT-CMP",
                description=f"Compilation error: {message.error}",
                remediation="Fix the SQL error in the editor, save the file, and it will auto-recompile.",
            )
            self._show_error_modal(error, show_in_editor=True)

    def show_repl_error(self, code: str, detail: str | None = None, show_in_editor: bool = False) -> None:
        """Display a registered REPL error (RVT-860 through RVT-867) as a modal."""
        error = make_repl_error(code, detail)
        self._show_error_modal(error, show_in_editor=show_in_editor)

    # ------------------------------------------------------------------
    # Log callback handler — Requirements 4.3, 5.6, 8.4
    # ------------------------------------------------------------------

    def on_log_received(self, message: LogReceived) -> None:
        """Forward log entry to LogsView and increment badge when Logs tab is not active."""
        try:
            self.query_one(LogsView).append_log(message.entry)
        except Exception:  # noqa: BLE001 — LogsView may not be composed yet
            pass
        # Increment badge count when Logs tab is not active
        panel = self.query_one("#results-panel", ResultsPanel)
        if panel._active_panel_tab != PanelTab.LOGS:
            panel._log_badge_count += 1
            try:
                self.query_one(ResultsTabBar).log_badge_count = panel._log_badge_count
            except Exception:  # noqa: BLE001 — tab bar may not be composed yet
                pass

    # ------------------------------------------------------------------
    # Colon command dispatch
    # ------------------------------------------------------------------

    def on_command_submitted(self, message: CommandSubmitted) -> None:
        """Handle a colon command submitted from the CommandInput widget."""
        if message.command == "engine":
            self._handle_engine_command(message.args)
        elif message.command == "inspect":
            self._handle_inspect_command(message.args)
        elif message.command == "clear-logs":
            self._handle_clear_logs()
        elif message.command == "generate":
            self._handle_generate_command(message.args)

    def _handle_engine_command(self, args: list[str]) -> None:
        """Handle :engine [name] — switch ad-hoc query engine."""
        if not args:
            self.action_open_engine_selector()
            return
        name = args[0]
        try:
            self._session.adhoc_engine = name
        except Exception as exc:  # noqa: BLE001
            self.notify(str(exc), severity="error")
            return
        self.post_message(
            ProfileChanged(
                profile_name=self._session.active_profile,
                engine_name=name,
                connected=True,
            )
        )
        self.notify(f"Ad-hoc engine: {name}")

    def _handle_clear_logs(self) -> None:
        """Handle :clear-logs — clear session log buffer and logs display."""
        self._session.clear_logs()
        try:
            self.query_one(LogsView).clear()
        except Exception:  # noqa: BLE001 — LogsView may not be composed yet
            pass

    def _handle_generate_command(self, args: list[str]) -> None:
        """Handle :generate <joint_name> [--description "<text>"] — create joint from last query."""
        if not args:
            self.notify(":generate requires a joint name", severity="error")
            return

        name = args[0]
        description: str | None = None

        # Parse optional --description "<text>"
        remaining = args[1:]
        if remaining and remaining[0] == "--description":
            if len(remaining) >= 2:
                description = " ".join(remaining[1:])
            else:
                self.notify("--description requires a value", severity="error")
                return

        try:
            file_path = self._session.generate_joint(name, description)
        except Exception as exc:  # noqa: BLE001
            self.notify(str(exc), severity="error")
            return

        self.notify(f"Joint '{name}' created: {file_path}")

    def _handle_inspect_command(self, args: list[str]) -> None:
        """Handle :inspect [target] [--full] [--engine <e>] [--tag <t>] [--type <tp>]."""
        from rivet_core.interactive.types import InspectFilter, Verbosity

        target: str | None = None
        verbosity = Verbosity.NORMAL
        engine: str | None = None
        tag: str | None = None
        joint_type: str | None = None

        i = 0
        while i < len(args):
            a = args[i]
            if a == "--full":
                verbosity = Verbosity.FULL
            elif a == "--engine" and i + 1 < len(args):
                i += 1
                engine = args[i]
            elif a == "--tag" and i + 1 < len(args):
                i += 1
                tag = args[i]
            elif a == "--type" and i + 1 < len(args):
                i += 1
                joint_type = args[i]
            elif not a.startswith("--"):
                target = a
            i += 1

        filt = InspectFilter(engine=engine, tag=tag, joint_type=joint_type) if (engine or tag or joint_type) else None

        try:
            result = self._session.inspect_assembly(target=target, verbosity=verbosity, filter=filt)
        except Exception as exc:  # noqa: BLE001
            self.notify(str(exc), severity="error")
            return

        self.query_one(ResultsPanel).show_inspect_result(result)
