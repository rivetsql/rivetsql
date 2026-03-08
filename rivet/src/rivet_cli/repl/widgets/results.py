"""ResultsPanel widget for the Rivet REPL TUI.

Displays query results as a data table with column headers (name + type),
footer (row count, column count, elapsed time), column sorting, cell
selection with copy, type-aware rendering, configurable row limit,
empty/zero-row/error states, multiple result sets, result pinning,
diff view, profiling panel, and query plan viewer.

Requirements: 12.1–12.7, 13.1–13.3, 13.5, 13.7, 14.1, 14.7, 15.1–15.5, 39.5
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar

from rivet_core.interactive.types import Verbosity

try:
    from textual.app import ComposeResult
    from textual.binding import Binding
    from textual.containers import Vertical
    from textual.message import Message
    from textual.widget import Widget
    from textual.widgets import DataTable, Static

    from rivet_cli.repl.accessibility import (
        ARIA_RESULTS_FOOTER,
        ARIA_RESULTS_TABLE,
        ARIA_RESULTS_TABS,
    )
    from rivet_cli.repl.widgets.compilation_view import CompilationView
    from rivet_cli.repl.widgets.logs_view import LogsView
    from rivet_cli.repl.widgets.progress_indicator import ProgressIndicator
    from rivet_cli.repl.widgets.results_tab_bar import (
        PanelTab,
        ResultsTabBar,
        TabChanged,
    )

    _TEXTUAL_AVAILABLE = True
except ImportError:  # pragma: no cover
    _TEXTUAL_AVAILABLE = False

if TYPE_CHECKING:

    from rivet_core.interactive.types import (
        QueryProgress,
        QueryResult,
    )


# ---------------------------------------------------------------------------
# View mode enum
# ---------------------------------------------------------------------------


class ViewMode(Enum):
    """Active sub-view in the results panel."""

    DATA = "data"
    DIFF = "diff"
    PROFILE = "profile"
    PLAN = "plan"
    ERROR = "error"
    INSPECT = "inspect"
    PREVIEW = "preview"


# ---------------------------------------------------------------------------
# Sort state
# ---------------------------------------------------------------------------


class SortDirection(Enum):
    NONE = "none"
    ASC = "asc"
    DESC = "desc"


@dataclass
class SortState:
    column: str | None = None
    direction: SortDirection = SortDirection.NONE


# ---------------------------------------------------------------------------
# Result set wrapper
# ---------------------------------------------------------------------------


@dataclass
class ResultSet:
    """One result set (one statement's output)."""

    table: Any  # pa.Table
    column_names: list[str]
    column_types: list[str]
    row_count: int
    elapsed_ms: float
    query_plan: Any | None = None  # QueryPlan
    truncated: bool = False
    error: str | None = None


# ---------------------------------------------------------------------------
# State management mixin — shared by Textual widget and stub
# ---------------------------------------------------------------------------


class _ResultsPanelState:
    """Pure state management for the results panel. No UI dependencies."""

    _session: Any
    _max_results: int
    _result_sets: list[ResultSet]
    _active_result_index: int
    _pinned_table: Any  # pa.Table | None
    _pinned_label: str
    _diff_result: Any  # DiffResult | None
    _profile_result: Any  # ProfileResult | None
    _sort_state: SortState
    _view_mode: ViewMode
    _query_counter: int
    _inspect_result: Any  # AssemblyInspection | JointInspection | None
    _active_panel_tab: PanelTab
    _log_badge_count: int
    _last_viewed_log_count: int

    def _init_state(
        self,
        session: Any | None = None,
        max_results: int = 10_000,
    ) -> None:
        self._session = session
        self._max_results = max_results
        self._result_sets = []
        self._active_result_index = 0
        self._pinned_table = None
        self._pinned_label = ""
        self._diff_result = None
        self._profile_result = None
        self._inspect_result = None
        self._sort_state = SortState()
        self._view_mode = ViewMode.DATA
        self._query_counter = 0
        self._active_panel_tab = PanelTab.RESULT
        self._log_badge_count = 0
        self._last_viewed_log_count = 0
        self._showing_progress = False
        self._preview_data: Any = None  # JointPreviewData | None

    def _on_state_changed(self) -> None:
        """Hook for subclasses to trigger rendering after state changes."""

    @property
    def pinned_table(self) -> Any | None:
        return self._pinned_table

    @property
    def pinned_label(self) -> str:
        return self._pinned_label

    @property
    def result_sets(self) -> list[ResultSet]:
        return list(self._result_sets)

    @property
    def active_result(self) -> ResultSet | None:
        if self._result_sets and 0 <= self._active_result_index < len(self._result_sets):
            return self._result_sets[self._active_result_index]
        return None

    @property
    def view_mode(self) -> ViewMode:
        return self._view_mode

    @property
    def sort_state(self) -> SortState:
        return self._sort_state

    def show_query_result(self, result: QueryResult) -> None:
        """Display a QueryResult in the panel, replacing any cursor-driven preview."""
        self._query_counter += 1
        rs = ResultSet(
            table=result.table,
            column_names=list(result.column_names),
            column_types=list(result.column_types),
            row_count=result.row_count,
            elapsed_ms=result.elapsed_ms,
            query_plan=result.query_plan,
            truncated=result.truncated,
        )
        self._result_sets = [rs]
        self._active_result_index = 0
        self._view_mode = ViewMode.DATA
        self._preview_data = None
        self._sort_state = SortState()
        self._diff_result = None
        self._profile_result = None
        self._on_state_changed()

    def show_multiple_results(self, results: list[QueryResult]) -> None:
        """Display multiple result sets (one per statement)."""
        self._result_sets = []
        for r in results:
            self._result_sets.append(ResultSet(
                table=r.table,
                column_names=list(r.column_names),
                column_types=list(r.column_types),
                row_count=r.row_count,
                elapsed_ms=r.elapsed_ms,
                query_plan=r.query_plan,
                truncated=r.truncated,
            ))
        self._active_result_index = 0
        self._view_mode = ViewMode.DATA
        self._sort_state = SortState()
        self._on_state_changed()

    def show_error(self, error: str) -> None:
        """Display an error message in the panel."""
        self._view_mode = ViewMode.ERROR
        self._result_sets = [ResultSet(
            table=None,
            column_names=[],
            column_types=[],
            row_count=0,
            elapsed_ms=0.0,
            error=error,
        )]
        self._on_state_changed()

    def show_inspect_result(self, inspection: Any) -> None:
        """Display an assembly or joint inspection in the panel."""
        self._inspect_result = inspection
        self._view_mode = ViewMode.INSPECT
        self._on_state_changed()

    def show_joint_preview(self, preview: Any) -> None:
        """Display a JointPreviewData in the panel with a 'Preview' label."""
        self._preview_data = preview
        self._view_mode = ViewMode.PREVIEW
        self._on_state_changed()

    def clear(self) -> None:
        """Reset to empty state."""
        self._result_sets = []
        self._active_result_index = 0
        self._view_mode = ViewMode.DATA
        self._sort_state = SortState()
        self._diff_result = None
        self._profile_result = None
        self._inspect_result = None
        self._on_state_changed()

    def pin(self) -> None:
        """Pin the current result. Replaces any previous pin."""
        rs = self.active_result
        if rs is None or rs.table is None:
            return
        self._pinned_table = rs.table
        self._pinned_label = f"Q{self._query_counter} ({rs.row_count} rows)"
        self._on_state_changed()

    def unpin(self) -> None:
        """Clear the pinned result."""
        self._pinned_table = None
        self._pinned_label = ""
        self._diff_result = None
        if self._view_mode == ViewMode.DIFF:
            self._view_mode = ViewMode.DATA
        self._on_state_changed()

    def switch_result_set(self, index: int) -> None:
        """Switch to a specific result set tab."""
        if 0 <= index < len(self._result_sets):
            self._active_result_index = index
            self._on_state_changed()


# ---------------------------------------------------------------------------
# Widget implementation
# ---------------------------------------------------------------------------

if _TEXTUAL_AVAILABLE:

    class ResultsPanel(_ResultsPanelState, Widget):
        """Bottom-right panel showing query results, diffs, profiles, and plans."""

        DEFAULT_CSS = """
        ResultsPanel {
            height: 1fr;
            width: 1fr;
        }
        ResultsPanel .results-footer {
            height: 1;
            dock: bottom;
            background: $panel;
            color: $text-muted;
            padding: 0 1;
        }
        ResultsPanel .results-error {
            color: $error;
            padding: 1 2;
        }
        ResultsPanel .results-empty {
            color: $text-muted;
            text-align: center;
            padding: 3 2;
        }
        ResultsPanel .results-tab-bar {
            height: 1;
            dock: top;
        }
        ResultsPanel #result-content {
            height: 1fr;
        }
        ResultsPanel #compilation-content {
            height: 1fr;
        }
        ResultsPanel #logs-content {
            height: 1fr;
        }
        ResultsPanel .diff-summary {
            height: 1;
            dock: top;
            color: $text-muted;
            padding: 0 1;
        }
        ResultsPanel .profile-panel {
            height: auto;
            max-height: 50%;
        }
        ResultsPanel .plan-panel {
            padding: 1 2;
        }
        """

        BINDINGS: ClassVar[list[Binding]] = [  # type: ignore[assignment]
            Binding("ctrl+k", "pin_result", "Pin Result", show=False),
            Binding("ctrl+d", "toggle_diff", "Diff View", show=False),
            Binding("ctrl+shift+d", "toggle_profile", "Profile", show=False),
            Binding("ctrl+e", "toggle_plan", "Query Plan", show=False),
            Binding("ctrl+c", "copy_selection", "Copy", show=False),
            Binding("alt+1", "tab_result", "Result", show=True),
            Binding("alt+2", "tab_compilation", "Compilation", show=True),
            Binding("alt+3", "tab_logs", "Logs", show=True),
        ]

        # --- Messages ---

        class ResultPinned(Message):
            """Posted when a result is pinned."""

            def __init__(self, row_count: int, label: str) -> None:
                super().__init__()
                self.row_count = row_count
                self.label = label

        class ResultUnpinned(Message):
            """Posted when a result is unpinned."""

        def __init__(
            self,
            session: Any | None = None,
            *,
            max_results: int = 10_000,
            name: str | None = None,
            id: str | None = None,
            classes: str | None = None,
        ) -> None:
            Widget.__init__(self, name=name, id=id, classes=classes)
            self._init_state(session=session, max_results=max_results)

        # --- Execution lifecycle (Requirements 2.1, 2.4, 2.5, 2.6) ---

        def show_progress(self) -> None:
            """Mount ProgressIndicator, replacing current result content."""
            if not self.is_mounted:
                return
            self._showing_progress = True
            try:
                content = self.query_one("#result-content", Vertical)
                content.display = True
                # Hide existing children
                for child in content.children:
                    child.display = False
                content.mount(ProgressIndicator(id="progress-indicator"))
            except Exception:
                pass

        def update_progress(self, progress: QueryProgress) -> None:
            """Forward a QueryProgress update to the mounted ProgressIndicator."""
            if not self._showing_progress:
                return
            try:
                self.query_one("#progress-indicator", ProgressIndicator).update_progress(progress)
            except Exception:
                pass

        def _remove_progress(self) -> None:
            """Remove the ProgressIndicator and restore result content."""
            if not self._showing_progress:
                return
            self._showing_progress = False
            try:
                self.query_one("#progress-indicator", ProgressIndicator).remove()
            except Exception:
                pass
            try:
                content = self.query_one("#result-content", Vertical)
                for child in content.children:
                    child.display = True
            except Exception:
                pass

        def show_query_result(self, result: QueryResult) -> None:
            """Display a QueryResult, removing progress indicator if present."""
            self._remove_progress()
            super().show_query_result(result)

        def show_error(self, error: str) -> None:
            """Display an error, removing progress indicator if present."""
            self._remove_progress()
            super().show_error(error)

        def show_cancellation(self, message: str = "Query cancelled.") -> None:
            """Display a cancellation message, removing progress indicator."""
            self._remove_progress()
            self._view_mode = ViewMode.ERROR
            self._result_sets = [ResultSet(
                table=None,
                column_names=[],
                column_types=[],
                row_count=0,
                elapsed_ms=0.0,
                error=message,
            )]
            self._on_state_changed()

        def _on_state_changed(self) -> None:
            """Re-render the appropriate view after state changes."""
            if not self.is_mounted:
                return
            if self._view_mode == ViewMode.ERROR:
                rs = self.active_result
                self._render_error(rs.error if rs else "Unknown error")  # type: ignore[arg-type]
            elif self._view_mode == ViewMode.DIFF:
                self._render_diff()
            elif self._view_mode == ViewMode.PROFILE:
                self._render_profile()
            elif self._view_mode == ViewMode.PLAN:
                self._render_plan()
            elif self._view_mode == ViewMode.INSPECT:
                self._render_inspect()
            elif self._view_mode == ViewMode.PREVIEW:
                self._render_preview()
            else:
                self._render_data()
            if len(self._result_sets) > 1:
                self._render_result_tabs()

        # --- Compose ---

        def compose(self) -> ComposeResult:
            yield ResultsTabBar(id="panel-tab-bar")
            with Vertical(id="result-content"):
                yield Static("", classes="results-tab-bar", id="results-tabs")
                yield DataTable(id="results-table")
                yield Static("", classes="results-footer", id="results-footer")
            yield CompilationView(session=self._session, id="compilation-content")
            yield LogsView(id="logs-content")

        def on_mount(self) -> None:
            self._render_empty_state()
            self._sync_tab_visibility()
            # ARIA-style labels — Requirement 35.1
            self.query_one("#results-table", DataTable).tooltip = ARIA_RESULTS_TABLE
            self.query_one("#results-tabs", Static).tooltip = ARIA_RESULTS_TABS
            self.query_one("#results-footer", Static).tooltip = ARIA_RESULTS_FOOTER

        def _sync_tab_visibility(self) -> None:
            """Show/hide content areas based on _active_panel_tab."""
            tab = self._active_panel_tab
            self.query_one("#result-content", Vertical).display = tab == PanelTab.RESULT
            self.query_one("#compilation-content", CompilationView).display = tab == PanelTab.COMPILATION
            self.query_one("#logs-content", LogsView).display = tab == PanelTab.LOGS
            # Defer re-render until after Textual completes its layout pass
            # so widgets have valid geometry after display toggle.
            if tab == PanelTab.RESULT:
                self.call_after_refresh(self._on_state_changed)
            elif tab == PanelTab.COMPILATION:
                cv = self.query_one("#compilation-content", CompilationView)
                self.call_after_refresh(cv._on_state_changed)
            elif tab == PanelTab.LOGS:
                logs = self.query_one("#logs-content", LogsView)
                self.call_after_refresh(logs._rebuild_display)

        def on_tab_changed(self, message: TabChanged) -> None:
            """Handle tab switch from ResultsTabBar click."""
            if self._active_panel_tab == message.tab:
                return  # Already synced (e.g. from _switch_panel_tab)
            self._active_panel_tab = message.tab
            self._sync_tab_visibility()

        # --- Pin/Unpin overrides to post messages ---

        def pin(self) -> None:
            rs = self.active_result
            if rs is None or rs.table is None:
                return
            super().pin()
            self.post_message(self.ResultPinned(row_count=rs.row_count, label=self._pinned_label))

        def unpin(self) -> None:
            super().unpin()
            self.post_message(self.ResultUnpinned())

        # --- Actions ---

        def action_pin_result(self) -> None:
            self.pin()

        def action_toggle_diff(self) -> None:
            if self._view_mode == ViewMode.DIFF:
                self._view_mode = ViewMode.DATA
                self._on_state_changed()
                return
            if self._pinned_table is None:
                return
            rs = self.active_result
            if rs is None or rs.table is None:
                return
            if self._session is not None:
                try:
                    self._diff_result = self._session.diff_results(self._pinned_table, rs.table)
                except Exception:
                    self._diff_result = None
                    return
            self._view_mode = ViewMode.DIFF
            self._on_state_changed()

        def action_toggle_profile(self) -> None:
            if self._view_mode == ViewMode.PROFILE:
                self._view_mode = ViewMode.DATA
                self._on_state_changed()
                return
            rs = self.active_result
            if rs is None or rs.table is None:
                return
            if self._session is not None:
                try:
                    self._profile_result = self._session.profile_result(rs.table)
                except Exception:
                    self._profile_result = None
                    return
            self._view_mode = ViewMode.PROFILE
            self._on_state_changed()

        def action_toggle_plan(self) -> None:
            if self._view_mode == ViewMode.PLAN:
                self._view_mode = ViewMode.DATA
                self._on_state_changed()
                return
            rs = self.active_result
            if rs is None or rs.query_plan is None:
                return
            self._view_mode = ViewMode.PLAN
            self._on_state_changed()

        def action_copy_selection(self) -> None:
            """Copy selected cell as TSV to clipboard."""
            try:
                dt = self.query_one("#results-table", DataTable)
                row_key, col_key = dt.coordinate_to_cell_key(dt.cursor_coordinate)
                value = dt.get_cell(row_key, col_key)
                self.app.copy_to_clipboard(str(value) if value is not None else "")
            except Exception:
                pass

        def _switch_panel_tab(self, tab: PanelTab) -> None:
            """Switch the active panel tab and sync visibility."""
            self._active_panel_tab = tab
            self._sync_tab_visibility()
            self.query_one("#panel-tab-bar", ResultsTabBar)._switch_tab(tab)

        def action_tab_result(self) -> None:
            self._switch_panel_tab(PanelTab.RESULT)

        def action_tab_compilation(self) -> None:
            self._switch_panel_tab(PanelTab.COMPILATION)

        def action_tab_logs(self) -> None:
            self._switch_panel_tab(PanelTab.LOGS)

        # --- Rendering ---

        def _render_empty_state(self) -> None:
            try:
                dt = self.query_one("#results-table", DataTable)
                dt.clear(columns=True)
            except Exception:
                pass
            self._update_footer("Run a query or preview a table")

        def _render_data(self) -> None:
            rs = self.active_result
            if rs is None or rs.table is None:
                self._render_empty_state()
                return
            try:
                dt = self.query_one("#results-table", DataTable)
                dt.clear(columns=True)
                for name, dtype in zip(rs.column_names, rs.column_types):
                    dt.add_column(f"{name} ({dtype})", key=name)
                table = rs.table
                if self._sort_state.column and self._sort_state.direction != SortDirection.NONE:
                    table = self._sort_table(table)
                for i in range(min(table.num_rows, self._max_results)):
                    row = []
                    for col_name in rs.column_names:
                        val = table.column(col_name)[i].as_py()
                        row.append(self._format_cell(val, col_name, rs.column_types[rs.column_names.index(col_name)]))
                    dt.add_row(*row)
            except Exception:
                pass
            self._update_footer()

        def _render_error(self, error: str) -> None:
            try:
                dt = self.query_one("#results-table", DataTable)
                dt.clear(columns=True)
                dt.add_column("Error")
                dt.add_row(error)
            except Exception:
                pass
            self._update_footer("Error")

        def _render_diff(self) -> None:
            if self._diff_result is None:
                return
            diff = self._diff_result
            try:
                dt = self.query_one("#results-table", DataTable)
                dt.clear(columns=True)
                dt.add_column("Status", key="_status")
                rs = self.active_result
                if rs:
                    for name, dtype in zip(rs.column_names, rs.column_types):
                        dt.add_column(f"{name} ({dtype})", key=name)
                for i in range(diff.added.num_rows):
                    row = ["+"]
                    for col_name in diff.added.schema.names:
                        row.append(str(diff.added.column(col_name)[i].as_py()))
                    dt.add_row(*row)
                for i in range(diff.removed.num_rows):
                    row = ["-"]
                    for col_name in diff.removed.schema.names:
                        row.append(str(diff.removed.column(col_name)[i].as_py()))
                    dt.add_row(*row)
                for changed_row in diff.changed:
                    row = ["~"]
                    if rs:
                        for col_name in rs.column_names:
                            if col_name in changed_row.changes:
                                old, new = changed_row.changes[col_name]
                                row.append(f"{old} → {new}")
                            elif col_name in changed_row.key:
                                row.append(str(changed_row.key[col_name]))
                            else:
                                row.append("")
                    dt.add_row(*row)
            except Exception:
                pass
            summary = (
                f"+{diff.added.num_rows} rows, "
                f"-{diff.removed.num_rows} rows, "
                f"~{len(diff.changed)} changed, "
                f"{diff.unchanged_count} unchanged"
            )
            self._update_footer(summary)

        def _render_profile(self) -> None:
            if self._profile_result is None:
                return
            prof = self._profile_result
            try:
                dt = self.query_one("#results-table", DataTable)
                dt.clear(columns=True)
                dt.add_column("Column", key="_col")
                dt.add_column("Type", key="_type")
                dt.add_column("Null%", key="_null_pct")
                dt.add_column("Distinct", key="_distinct")
                dt.add_column("Min", key="_min")
                dt.add_column("Max", key="_max")
                dt.add_column("Mean", key="_mean")
                dt.add_column("Median", key="_median")
                dt.add_column("Stddev", key="_stddev")
                for cp in prof.columns:
                    dt.add_row(
                        cp.name,
                        cp.dtype,
                        f"{cp.null_pct:.1f}%",
                        str(cp.distinct_count),
                        str(cp.min) if cp.min is not None else "",
                        str(cp.max) if cp.max is not None else "",
                        f"{cp.mean:.2f}" if cp.mean is not None else "",
                        f"{cp.median:.2f}" if cp.median is not None else "",
                        f"{cp.stddev:.2f}" if cp.stddev is not None else "",
                    )
            except Exception:
                pass
            self._update_footer(f"Profile: {prof.row_count} rows, {prof.column_count} columns")

        def _render_plan(self) -> None:
            rs = self.active_result
            if rs is None or rs.query_plan is None:
                return
            plan = rs.query_plan
            try:
                dt = self.query_one("#results-table", DataTable)
                dt.clear(columns=True)
                dt.add_column("Component", key="_component")
                dt.add_column("Details", key="_details")
                for src in plan.sources:
                    dt.add_row("Source", f"{src.name} ({src.joint_type})")
                dt.add_row("Query", f"{plan.query_joint.name} ({plan.query_joint.joint_type})")
                dt.add_row("Sink", f"{plan.sink.name} ({plan.sink.joint_type})")
                for ref, target in plan.resolved_references.items():
                    dt.add_row("Reference", f"{ref} → {target}")
            except Exception:
                pass
            self._update_footer("Query Plan")

        def _render_inspect_overview(self, dt: DataTable, ov: Any) -> None:  # type: ignore[type-arg]
            """Render the Overview section of an assembly inspection."""
            dt.add_row("[bold cyan]═══ Overview ═══[/]", "")
            dt.add_row("Profile", ov.profile_name)
            counts = ", ".join(f"{k}: {v}" for k, v in ov.joint_counts.items())
            dt.add_row("Joints", f"{ov.total_joints} total ({counts})")
            dt.add_row("Fused Groups", str(ov.fused_group_count))
            dt.add_row("Materializations", str(ov.materialization_count))
            for eng in ov.engines:
                dt.add_row("Engine", f"{eng.name} ({eng.engine_type}) — {eng.joint_count} joints")
            for cat in ov.catalogs:
                dt.add_row("Catalog", f"{cat.name} ({cat.type})")
            for adp in ov.adapters:
                dt.add_row("Adapter", f"{adp.engine_type} ↔ {adp.catalog_type} ({adp.source})")
            status = "[green]✓ Success[/]" if ov.success else "[red]✗ Failed[/]"
            if ov.warnings:
                status += f" [yellow]({len(ov.warnings)} warnings)[/]"
            dt.add_row("Status", status)
            for w in ov.warnings:
                dt.add_row("[yellow]Warning[/]", w)
            for e in ov.errors:
                dt.add_row("[red]Error[/]", e)

        def _render_inspect_execution_order(self, dt: DataTable, eo: Any) -> None:  # type: ignore[type-arg]
            """Render the Execution Order section with wave grouping."""
            dt.add_row("", "")
            dt.add_row("[bold cyan]═══ Execution Order ═══[/]", "")

            # Group steps by wave_number
            waves: dict[int, list[Any]] = {}
            for step in eo.steps:
                wn = getattr(step, "wave_number", 0)
                waves.setdefault(wn, []).append(step)

            has_waves = any(w != 0 for w in waves)

            if has_waves:
                for wave_num in sorted(waves):
                    if wave_num == 0:
                        for step in waves[wave_num]:
                            fused = " [magenta](fused)[/]" if step.is_fused else ""
                            mat = " ⚡" if step.has_materialization else ""
                            joints = ", ".join(step.joints)
                            dt.add_row(
                                f"Step {step.step_number}",
                                f"{step.id} [{step.engine}]{fused}{mat} — {joints}",
                            )
                    else:
                        dt.add_row(f"[bold]Wave {wave_num}[/]", "")
                        for step in waves[wave_num]:
                            fused = " [magenta](fused)[/]" if step.is_fused else ""
                            mat = " ⚡" if step.has_materialization else ""
                            joints = ", ".join(step.joints)
                            dt.add_row(
                                f"  Step {step.step_number}",
                                f"{step.id} [{step.engine}]{fused}{mat} — {joints}",
                            )
            else:
                for step in eo.steps:
                    fused = " [magenta](fused)[/]" if step.is_fused else ""
                    mat = " ⚡" if step.has_materialization else ""
                    joints = ", ".join(step.joints)
                    dt.add_row(
                        f"Step {step.step_number}",
                        f"{step.id} [{step.engine}]{fused}{mat} — {joints}",
                    )

        def _render_inspect_fused_groups(self, dt: DataTable, fg: Any) -> None:  # type: ignore[type-arg]
            """Render the Fused Groups section."""
            dt.add_row("", "")
            dt.add_row("[bold cyan]═══ Fused Groups ═══[/]", "")
            for g in fg.groups:
                dt.add_row(
                    f"[bold]{g.id}[/]",
                    f"{g.engine} ({g.engine_type}) — {g.fusion_strategy}",
                )
                dt.add_row("  Joints", ", ".join(g.joints))
                dt.add_row("  Entry", ", ".join(g.entry_joints))
                dt.add_row("  Exit", ", ".join(g.exit_joints))
                if g.fused_sql:
                    dt.add_row("  [green]Fused SQL[/]", g.fused_sql)
                if g.resolved_sql:
                    dt.add_row("  [green]Resolved SQL[/]", g.resolved_sql)
                if g.pushdown_predicates:
                    dt.add_row("  Pushdown", ", ".join(g.pushdown_predicates))
                if g.residual_operations:
                    dt.add_row("  Residual", ", ".join(g.residual_operations))

        def _render_inspect_materializations(self, dt: DataTable, mat_section: Any) -> None:  # type: ignore[type-arg]
            """Render the Materializations section."""
            dt.add_row("", "")
            dt.add_row("[bold cyan]═══ Materializations ═══[/]", "")
            for trigger, details in mat_section.by_trigger.items():
                dt.add_row(f"[bold]{trigger}[/]", "")
                for m in details:
                    dt.add_row(
                        f"  {m.from_joint} → {m.to_joint}",
                        f"{m.detail} ({m.strategy})",
                    )

        def _render_inspect_dag(self, dt: DataTable, dag: Any) -> None:  # type: ignore[type-arg]
            """Render the DAG section."""
            dt.add_row("", "")
            dt.add_row("[bold cyan]═══ DAG ═══[/]", "")
            for line in dag.rendered_text.splitlines():
                dt.add_row("", line)

        def _render_inspect_joint_details(self, dt: DataTable, jd: Any) -> None:  # type: ignore[type-arg]
            """Render the Joint Details section."""
            dt.add_row("", "")
            dt.add_row("[bold cyan]═══ Joint Details ═══[/]", "")
            for j in jd:
                dt.add_row(f"[bold]{j.name}[/]", f"{j.type} — {j.engine}")
                if j.source_file:
                    dt.add_row("  Source", j.source_file)
                dt.add_row("  Resolution", j.engine_resolution)
                if j.adapter:
                    dt.add_row("  Adapter", j.adapter)
                if j.catalog:
                    dt.add_row("  Catalog", j.catalog)
                if j.table:
                    dt.add_row("  Table", j.table)
                if j.fused_group_id:
                    dt.add_row("  Fused Group", j.fused_group_id)
                if j.upstream:
                    dt.add_row("  Upstream", ", ".join(j.upstream))
                if j.output_schema:
                    schema = ", ".join(f"{f.name}: {f.type}" for f in j.output_schema)
                    dt.add_row("  Schema", schema)
                if j.sql_original:
                    dt.add_row("  [green]SQL (original)[/]", j.sql_original)
                if j.sql_translated:
                    dt.add_row("  [green]SQL (translated)[/]", j.sql_translated)
                if j.sql_resolved:
                    dt.add_row("  [green]SQL (resolved)[/]", j.sql_resolved)
                if j.write_strategy:
                    dt.add_row("  Write Strategy", j.write_strategy)
                if j.tags:
                    dt.add_row("  Tags", ", ".join(j.tags))
                if j.description:
                    dt.add_row("  Description", j.description)
                if j.checks:
                    dt.add_row("  Checks", ", ".join(j.checks))
                if j.optimizations:
                    dt.add_row("  Optimizations", ", ".join(j.optimizations))
                if j.source_stats is not None:
                    ss = j.source_stats
                    parts = []
                    if ss.row_count is not None:
                        parts.append(f"Rows: {ss.row_count:,}")
                    if ss.size_bytes is not None:
                        parts.append(f"Size: {ss.size_bytes:,} bytes")
                    if ss.last_modified is not None:
                        parts.append(f"Modified: {ss.last_modified.isoformat()}")
                    if ss.partition_count is not None:
                        parts.append(f"Partitions: {ss.partition_count}")
                    if parts:
                        dt.add_row("  Source Stats", " │ ".join(parts))

        def _render_inspect(self) -> None:
            """Render AssemblyInspection with section headers and SQL highlighting."""
            inspection = self._inspect_result
            if inspection is None:
                self._render_empty_state()
                return
            try:
                dt = self.query_one("#results-table", DataTable)
                dt.clear(columns=True)
                dt.add_column("Section", key="_section")
                dt.add_column("Details", key="_details")

                if inspection.overview is not None:
                    self._render_inspect_overview(dt, inspection.overview)
                if inspection.execution_order is not None:
                    self._render_inspect_execution_order(dt, inspection.execution_order)
                if inspection.fused_groups is not None:
                    self._render_inspect_fused_groups(dt, inspection.fused_groups)
                if inspection.materializations is not None:
                    self._render_inspect_materializations(dt, inspection.materializations)
                if inspection.dag is not None:
                    self._render_inspect_dag(dt, inspection.dag)
                if inspection.joint_details is not None:
                    self._render_inspect_joint_details(dt, inspection.joint_details)

                # --- Verbosity hint ---
                if (
                    inspection.overview is not None
                    and inspection.overview.total_joints > 10
                    and inspection.verbosity != Verbosity.FULL
                ):
                    dt.add_row("", "")
                    dt.add_row(
                        "[yellow]Hint[/]",
                        "Showing partial output. Use :inspect --full for complete details.",
                    )

            except Exception:
                pass

            # --- Footer ---
            footer = f"Assembly Inspection ({inspection.verbosity.value})"
            if inspection.filter_applied is not None:
                parts = []
                if inspection.filter_applied.engine:
                    parts.append(f"engine={inspection.filter_applied.engine}")
                if inspection.filter_applied.tag:
                    parts.append(f"tag={inspection.filter_applied.tag}")
                if inspection.filter_applied.joint_type:
                    parts.append(f"type={inspection.filter_applied.joint_type}")
                if parts:
                    footer += f"  │  Filter: {', '.join(parts)}"
            self._update_footer(footer)

        def _render_preview(self) -> None:
            """Render a JointPreviewData with metadata and optional cached rows."""
            preview = self._preview_data
            if preview is None:
                self._render_empty_state()
                return
            try:
                dt = self.query_one("#results-table", DataTable)
                dt.clear(columns=True)

                if preview.preview_rows is not None and preview.preview_rows.num_rows > 0:
                    # Show cached data rows
                    table = preview.preview_rows
                    for col_field in table.schema:
                        dt.add_column(f"{col_field.name} ({col_field.type})", key=col_field.name)
                    for i in range(table.num_rows):
                        row = []
                        for col_field in table.schema:
                            val = table.column(col_field.name)[i].as_py()
                            row.append(self._format_cell(val, col_field.name, str(col_field.type)))
                        dt.add_row(*row)
                else:
                    # Metadata-only preview
                    dt.add_column("Property", key="_prop")
                    dt.add_column("Value", key="_val")
                    dt.add_row("Engine", preview.engine)
                    if preview.fusion_group:
                        dt.add_row("Fusion Group", preview.fusion_group)
                    if preview.upstream:
                        dt.add_row("Upstream", ", ".join(preview.upstream))
                    if preview.tags:
                        dt.add_row("Tags", ", ".join(preview.tags))
                    if preview.schema:
                        for sf in preview.schema:
                            dt.add_row(f"  {sf.name}", sf.type)
            except Exception:
                pass

            parts = [f"Preview: {preview.joint_name}"]
            parts.append(f"engine={preview.engine}")
            if preview.preview_rows is not None:
                parts.append(f"{preview.preview_rows.num_rows} rows (cached)")
            else:
                parts.append("no cached data")
            self._update_footer("  │  ".join(parts))

        def _render_result_tabs(self) -> None:
            if len(self._result_sets) <= 1:
                try:
                    self.query_one("#results-tabs", Static).update("")
                except Exception:
                    pass
                return
            parts = []
            for i, _rs in enumerate(self._result_sets):
                prefix = "[bold]" if i == self._active_result_index else ""
                suffix = "[/bold]" if i == self._active_result_index else ""
                parts.append(f"{prefix}[Result {i + 1}]{suffix}")
            try:
                self.query_one("#results-tabs", Static).update(" ".join(parts))
            except Exception:
                pass

        def _update_footer(self, text: str | None = None) -> None:
            if text is not None:
                footer_text = text
            else:
                rs = self.active_result
                if rs is None:
                    footer_text = "Run a query or preview a table"
                elif rs.error:
                    footer_text = "Error"
                else:
                    elapsed_s = rs.elapsed_ms / 1000.0
                    trunc = " (truncated)" if rs.truncated else ""
                    footer_text = (
                        f"{rs.row_count} rows  │  "
                        f"{len(rs.column_names)} columns  │  "
                        f"{elapsed_s:.2f}s{trunc}"
                    )
            if self._pinned_table is not None:
                footer_text += f"  │  📌 Pinned: {self._pinned_label}"
            try:
                self.query_one("#results-footer", Static).update(footer_text)
            except Exception:
                pass

        def _format_cell(self, value: Any, col_name: str, dtype: str) -> str:
            if value is None:
                return "NULL"
            s = str(value)
            if len(s) > 100:
                return s[:99] + "…"
            return s

        def _sort_table(self, table: Any) -> Any:
            """Sort an Arrow table by the current sort state."""
            import pyarrow.compute as pc

            col = self._sort_state.column
            if col is None or col not in table.schema.names:
                return table
            indices = pc.sort_indices(
                table,
                sort_keys=[(col, "ascending" if self._sort_state.direction == SortDirection.ASC else "descending")],
            )
            return table.take(indices)

        def on_data_table_header_selected(self, event: DataTable.HeaderSelected) -> None:
            """Cycle sort direction on header click."""
            col_key = str(event.column_key)
            if self._sort_state.column == col_key:
                if self._sort_state.direction == SortDirection.NONE:
                    self._sort_state.direction = SortDirection.ASC
                elif self._sort_state.direction == SortDirection.ASC:
                    self._sort_state.direction = SortDirection.DESC
                else:
                    self._sort_state = SortState()
            else:
                self._sort_state = SortState(column=col_key, direction=SortDirection.ASC)
            if self._view_mode == ViewMode.DATA:
                self._on_state_changed()

else:  # pragma: no cover — Textual not installed

    class ResultsPanel(_ResultsPanelState):  # type: ignore[no-redef]
        """Stub ResultsPanel for environments without Textual installed."""

        def __init__(
            self,
            session: Any | None = None,
            *,
            max_results: int = 10_000,
            **kwargs: object,
        ) -> None:
            self._init_state(session=session, max_results=max_results)
