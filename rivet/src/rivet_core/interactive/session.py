"""Headless interactive session for the Rivet REPL.

UI-agnostic — no TUI or web imports. Manages project state, compiled assembly,
catalog connections, material cache, query history, and all query
planning/execution logic.

Imports only from stdlib, PyArrow, sqlglot, and rivet_core — respecting
module boundary rules.

Requirements: 1.2, 1.6, 11.1–11.6, 19.1, 20.1–20.4, 22.2, 22.3, 30.1, 30.2, 30.5, 37.1
"""

from __future__ import annotations

import dataclasses
import json
import logging
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Protocol

import pyarrow as pa

from rivet_core.assembly import Assembly
from rivet_core.catalog_explorer import CatalogExplorer, CatalogInfo, ExplorerNode
from rivet_core.compiler import CompiledAssembly, CompiledJoint
from rivet_core.compiler import compile as core_compile
from rivet_core.interactive.assembly_formatter import AssemblyFormatter
from rivet_core.interactive.catalog_search import CatalogSearch
from rivet_core.interactive.completions import CompletionEngine
from rivet_core.interactive.differ import Differ
from rivet_core.interactive.formatter import format_sql as _format_sql
from rivet_core.interactive.history import load_history, save_history
from rivet_core.interactive.log_buffer import Log_Buffer
from rivet_core.interactive.material_cache import MaterialCache
from rivet_core.interactive.profiler import Profiler
from rivet_core.interactive.query_planner import QueryPlanner
from rivet_core.interactive.types import (
    Activity_State,
    AssemblyInspection,
    CatalogSearchResult,
    Completion,
    DiffResult,
    Execution_Log,
    ExecutionResult,
    InspectFilter,
    ProfileResult,
    QueryHistoryEntry,
    QueryPlan,
    QueryProgress,
    QueryResult,
    ReplState,
    Verbosity,
)
from rivet_core.models import Catalog, ComputeEngine, Joint
from rivet_core.plugins import PluginRegistry
from rivet_core.smart_cache import CacheMode, SmartCache

logger = logging.getLogger(__name__)


class ReadOnlyError(Exception):
    """Raised when an execution operation is attempted in read-only mode."""


class SessionError(Exception):
    """Raised for general session errors."""


class ExecutionInProgressError(SessionError):
    """Raised when an Execution_Action is attempted while another is running."""


class ProjectLoader(Protocol):
    """Protocol for loading/reloading a project. Implemented by the TUI layer."""

    def load(
        self, project_path: Path, profile_name: str
    ) -> tuple[Assembly, dict[str, Catalog], dict[str, ComputeEngine], PluginRegistry, str]:
        """Return (assembly, catalogs, engines, registry, default_engine)."""
        ...


class InteractiveSession:
    """Headless service layer for interactive Rivet sessions."""

    def __init__(
        self,
        project_path: Path,
        profile: str | None = None,
        read_only: bool = False,
        max_results: int = 10_000,
        loader: ProjectLoader | None = None,
        skip_catalog_probe: bool = False,
    ) -> None:
        self._project_path = project_path
        self._profile_name = profile or "default"
        self._read_only = read_only
        self._max_results = max_results
        self._loader = loader
        self._skip_catalog_probe = skip_catalog_probe

        # Core services
        self._material_cache = MaterialCache()
        self._query_planner = QueryPlanner()

        # TUI-only services — deferred when running headless (skip_catalog_probe)
        if not skip_catalog_probe:
            self._differ = Differ()
            self._profiler = Profiler()
            self._completion_engine = CompletionEngine()
            self._catalog_search = CatalogSearch()
            self._formatter = AssemblyFormatter()
        else:
            self._differ: Differ | None = None  # type: ignore[assignment, no-redef]
            self._profiler: Profiler | None = None  # type: ignore[assignment, no-redef]
            self._completion_engine: CompletionEngine | None = None  # type: ignore[assignment, no-redef]
            self._catalog_search: CatalogSearch | None = None  # type: ignore[assignment, no-redef]
            self._formatter: AssemblyFormatter | None = None  # type: ignore[assignment, no-redef]

        # State set by start() or init_from()
        self._assembly: CompiledAssembly | None = None
        self._raw_assembly: Assembly | None = None
        self._catalogs: dict[str, Catalog] = {}
        self._engines: dict[str, ComputeEngine] = {}
        self._registry: PluginRegistry | None = None
        self._explorer: CatalogExplorer | None = None
        self._smart_cache: SmartCache | None = None
        self._history: list[QueryHistoryEntry] = []
        self._cancel_event = threading.Event()
        # Engine override cascade state
        self._adhoc_engine: str | None = None
        self._default_engine: str | None = None

        # Log capture
        self._log_buffer = Log_Buffer()
        self.on_log: Callable[[Execution_Log], None] | None = None

        # Activity state
        self._activity_state = Activity_State.IDLE
        self._exec_lock = threading.Lock()
        self.on_activity_change: Callable[[Activity_State], None] | None = None

        # REPL state (persisted across sessions)
        self._repl_state = ReplState()

        # Last executed query tracking (for generate_joint)
        self._last_query_sql: str | None = None
        self._last_query_engine: str | None = None
        self._last_query_upstream: list[str] = []

        # Metrics
        self._metrics: dict[str, int | float] = {
            "compilations": 0,
            "executions": 0,
            "queries": 0,
            "file_reloads": 0,
            "startup_ms": 0.0,
            "session_duration_ms": 0.0,
        }
        self._start_time: float | None = None

    # --- Lifecycle ---

    def init_from(
        self,
        assembly: Assembly,
        catalogs: dict[str, Catalog],
        engines: dict[str, ComputeEngine],
        registry: PluginRegistry,
        default_engine: str | None = None,
    ) -> None:
        """Initialize session from pre-built objects (no config/bridge imports)."""
        self._raw_assembly = assembly
        self._catalogs = catalogs
        self._engines = engines
        self._registry = registry
        self._default_engine = (
            default_engine
            if default_engine is not None
            else (next(iter(engines)) if engines else None)
        )

    def start(self) -> None:
        """Load project, connect catalogs, compile assembly.

        If init_from() was called, uses those objects. Otherwise uses the
        ProjectLoader protocol (which the TUI layer provides).

        Creates a ``SmartCache`` in ``READ_WRITE`` mode and passes it to
        ``CatalogExplorer`` so that cached catalog trees are available
        immediately (warm-start).
        """
        t0 = time.monotonic()
        self._start_time = t0

        if self._raw_assembly is None and self._loader is not None:
            asm, cats, engs, reg, default_eng = self._loader.load(
                self._project_path, self._profile_name
            )
            self.init_from(asm, cats, engs, reg, default_eng)

        if self._raw_assembly is None or self._registry is None:
            raise SessionError("Session not initialized — call init_from() or provide a loader")

        self._assembly = core_compile(
            assembly=self._raw_assembly,
            catalogs=list(self._catalogs.values()),
            engines=list(self._engines.values()),
            registry=self._registry,
            profile_name=self._profile_name,
            default_engine=self._default_engine,
            project_root=self._project_path,
        )
        self._metrics["compilations"] += 1

        if not self._skip_catalog_probe:
            try:
                self._smart_cache = SmartCache(profile=self._profile_name)
                self._explorer = CatalogExplorer(
                    catalogs=self._catalogs,
                    engines=self._engines,
                    registry=self._registry,
                    smart_cache=self._smart_cache,
                    cache_mode=CacheMode.READ_WRITE,
                )
            except Exception:
                logger.debug("Catalog explorer init failed", exc_info=True)

            self._query_planner = QueryPlanner(catalog_explorer=self._explorer)
        else:
            self._query_planner = QueryPlanner()

        self._metrics["startup_ms"] = (time.monotonic() - t0) * 1000
        if not self._skip_catalog_probe:
            self._history = load_history(self._project_path)
            self._repl_state = self._load_repl_state()

    def stop(self) -> None:
        """Close connections, flush SmartCache to disk, save history."""
        self._material_cache.clear()
        if self._smart_cache is not None:
            self._smart_cache.flush()
        if self._explorer is not None:
            self._explorer.close()
        if not self._skip_catalog_probe:
            save_history(self._project_path, self._history)
        if self._start_time is not None:
            self._metrics["session_duration_ms"] = (time.monotonic() - self._start_time) * 1000

    # --- Query Execution ---

    def _require_writable(self) -> None:
        if self._read_only:
            raise ReadOnlyError("Operation not allowed in read-only mode")

    def _require_assembly(self) -> CompiledAssembly:
        if self._assembly is None:
            raise SessionError("Session not started — call start() first")
        return self._assembly

    @contextmanager
    def _execution_guard(self) -> Iterator[None]:
        """Acquire execution lock, set EXECUTING, guarantee release."""
        if not self._exec_lock.acquire(blocking=False):
            raise ExecutionInProgressError("Execution in progress — cancel first (Ctrl+C)")
        try:
            self._set_activity(Activity_State.EXECUTING)
            yield
        finally:
            self._set_activity(Activity_State.IDLE)
            self._exec_lock.release()

    def _invoke_progress(
        self,
        callback: Callable[[QueryProgress], None] | None,
        *,
        joint_name: str,
        status: str,
        current: int,
        total: int,
        rows: int | None,
        t0: float,
    ) -> None:
        if callback is None:
            return
        try:
            callback(
                QueryProgress(
                    joint_name=joint_name,
                    status=status,  # type: ignore[arg-type]
                    current=current,
                    total=total,
                    rows=rows,
                    elapsed_ms=(time.monotonic() - t0) * 1000,
                )
            )
        except Exception:
            logger.warning("on_progress callback failed", exc_info=True)

    def _build_and_compile_transient(
        self,
        sql: str,
        catalog_context: str | None = None,
        engine_override: str | None = None,
    ) -> CompiledAssembly:
        """Build and compile a transient assembly from ad-hoc SQL.

        Shared by execute_query and inspect_assembly. Encapsulates:
        1. Resolve effective engine via override cascade:
           explicit engine_override > session adhoc_engine > project default_engine.
        2. Build transient pipeline via QueryPlanner.
        3. Add __display sink joint downstream of __query.
        4. Compile the transient assembly.
        5. Raise SessionError on compilation failure.
        """
        assembly = self._require_assembly()
        if self._registry is None:
            raise SessionError("Session not initialized — no plugin registry")

        effective_engine = engine_override or self._adhoc_engine or self._default_engine

        try:
            transient, _needs_exec = self._query_planner.build_transient_pipeline(
                sql,
                catalog_context,
                assembly,
                self._material_cache,
                catalog_names=frozenset(self._catalogs.keys()),
                raw_assembly=self._raw_assembly,
                engine_override=effective_engine,
            )
        except ValueError as exc:
            raise SessionError(str(exc)) from exc

        # Add __display sink joint downstream of __query, using the same engine
        display_joint = Joint(
            name="__display",
            joint_type="sink",
            upstream=["__query"],
            engine=effective_engine,
        )
        all_joints = list(transient.joints.values()) + [display_joint]
        transient = Assembly(all_joints)

        compiled = core_compile(
            assembly=transient,
            catalogs=list(self._catalogs.values()),
            engines=list(self._engines.values()),
            registry=self._registry,
            profile_name=self._profile_name,
            project_root=self._project_path,
        )

        if not compiled.success:
            msgs = "; ".join(str(e) for e in compiled.errors)
            raise SessionError(f"Transient pipeline compilation failed: {msgs}")

        return compiled

    def execute_query(
        self,
        sql: str,
        catalog_context: str | None = None,
        engine: str | None = None,
        on_progress: Callable[[QueryProgress], None] | None = None,
    ) -> QueryResult:
        """Execute ad-hoc SQL via the standard compile → execute path."""
        self._require_writable()
        self._cancel_event.clear()

        with self._execution_guard():
            t0 = time.monotonic()

            self._emit_log(
                Execution_Log(
                    timestamp=datetime.now(tz=UTC),
                    level="INFO",
                    source="session",
                    message="Compiling ad-hoc query",
                )
            )

            self._invoke_progress(
                on_progress,
                joint_name="__query",
                status="compiling",
                current=0,
                total=1,
                rows=None,
                t0=t0,
            )

            compiled = self._build_and_compile_transient(sql, catalog_context, engine)

            # Log assembly details
            engine_names = [e.name for e in compiled.engines]
            joint_count = len(compiled.joints)
            group_count = len(compiled.fused_groups)
            total_steps = len(compiled.execution_order)
            self._emit_log(
                Execution_Log(
                    timestamp=datetime.now(tz=UTC),
                    level="INFO",
                    source="compiler",
                    message=f"Compiled: {joint_count} joints, {group_count} fused groups, engines: {', '.join(engine_names) or 'none'}",
                )
            )
            if compiled.warnings:
                for w in compiled.warnings:
                    self._emit_log(
                        Execution_Log(
                            timestamp=datetime.now(tz=UTC),
                            level="WARNING",
                            source="compiler",
                            message=w,
                        )
                    )

            # Log execution order
            for step_idx, step_id in enumerate(compiled.execution_order, 1):
                group = next((g for g in compiled.fused_groups if g.id == step_id), None)
                if group:
                    self._emit_log(
                        Execution_Log(
                            timestamp=datetime.now(tz=UTC),
                            level="DEBUG",
                            source="executor",
                            message=f"Step {step_idx}: group '{step_id}' — joints: {', '.join(group.joints)}, engine: {group.engine}",
                        )
                    )

            effective_engine = compiled.engines[0].name if compiled.engines else "unknown"
            self._emit_log(
                Execution_Log(
                    timestamp=datetime.now(tz=UTC),
                    level="INFO",
                    source="engine",
                    message=f"Executing on '{effective_engine}'",
                )
            )

            # Progress: executing per fused group step
            for step_idx, step_id in enumerate(compiled.execution_order):
                group = next((g for g in compiled.fused_groups if g.id == step_id), None)
                step_name = group.joints[0] if group else step_id
                self._invoke_progress(
                    on_progress,
                    joint_name=step_name,
                    status="executing",
                    current=step_idx,
                    total=total_steps,
                    rows=None,
                    t0=t0,
                )

            from rivet_core.executor import Executor  # noqa: PLC0415

            executor = Executor(registry=self._registry, project_root=self._project_path)
            table, run_stats = executor.run_query_with_stats_sync(
                compiled, target_joint="__display"
            )

            table, truncated = self._apply_truncation(table)
            elapsed = (time.monotonic() - t0) * 1000

            trunc_note = f" (truncated to {table.num_rows})" if truncated else ""
            self._emit_log(
                Execution_Log(
                    timestamp=datetime.now(tz=UTC),
                    level="INFO",
                    source="engine",
                    message=f"Query completed: {table.num_rows} rows, {table.num_columns} cols in {elapsed:.0f}ms{trunc_note}",
                )
            )

            self._invoke_progress(
                on_progress,
                joint_name="__query",
                status="done",
                current=max(total_steps, 1),
                total=max(total_steps, 1),
                rows=table.num_rows,
                t0=t0,
            )

            # Build QueryPlan so the TUI can show the transient assembly
            query_cj = next((j for j in compiled.joints if j.name == "__query"), None)
            source_joints = [
                Joint(name=j.name, joint_type=j.type) for j in compiled.joints if j.type == "source"
            ]
            query_plan = QueryPlan(
                sources=source_joints,
                query_joint=Joint(
                    name="__query", joint_type=query_cj.type if query_cj else "sql", sql=sql
                ),
                sink=Joint(name="__display", joint_type="sink", upstream=["__query"]),
                resolved_references={},
                assembly=compiled,
            )

            result = QueryResult(
                table=table,
                row_count=table.num_rows,
                column_names=table.column_names,
                column_types=[str(t) for t in table.schema.types],
                elapsed_ms=elapsed,
                query_plan=query_plan,
                quality_results=None,
                truncated=truncated,
                run_stats=run_stats,
            )

            self._metrics["queries"] += 1
            self._record_history("query", sql, table.num_rows, elapsed, "success")

            # Track last query for generate_joint
            self._last_query_sql = sql
            self._last_query_engine = effective_engine
            query_cj_compiled = next((j for j in compiled.joints if j.name == "__query"), None)
            self._last_query_upstream = (
                list(query_cj_compiled.upstream) if query_cj_compiled else []
            )

            return result

    def execute_joint(
        self,
        joint_name: str,
        on_progress: Callable[[QueryProgress], None] | None = None,
    ) -> QueryResult:
        """Execute a specific pipeline joint and its upstream."""
        self._require_writable()
        assembly = self._require_assembly()
        self._cancel_event.clear()

        with self._execution_guard():
            t0 = time.monotonic()
            joint = None
            for j in assembly.joints:
                if j.name == joint_name:
                    joint = j
                    break
            if joint is None:
                raise SessionError(f"Joint '{joint_name}' not found in assembly")

            total_steps = len(assembly.execution_order)
            self._invoke_progress(
                on_progress,
                joint_name=joint_name,
                status="compiling",
                current=0,
                total=max(total_steps, 1),
                rows=None,
                t0=t0,
            )

            for step_idx, step_id in enumerate(assembly.execution_order):
                group = next((g for g in assembly.fused_groups if g.id == step_id), None)
                step_name = group.joints[0] if group else step_id
                self._invoke_progress(
                    on_progress,
                    joint_name=step_name,
                    status="executing",
                    current=step_idx,
                    total=total_steps,
                    rows=None,
                    t0=t0,
                )

            elapsed = (time.monotonic() - t0) * 1000
            self._invoke_progress(
                on_progress,
                joint_name=joint_name,
                status="done",
                current=max(total_steps, 1),
                total=max(total_steps, 1),
                rows=0,
                t0=t0,
            )

            result = QueryResult(
                table=pa.table({}),
                row_count=0,
                column_names=[],
                column_types=[],
                elapsed_ms=elapsed,
                query_plan=None,
                quality_results=None,
                truncated=False,
            )

            self._metrics["executions"] += 1
            self._record_history("joint", joint_name, 0, elapsed, "success")
            return result

    def execute_pipeline(
        self,
        tags: list[str] | None = None,
        on_progress: Callable[[QueryProgress], None] | None = None,
    ) -> ExecutionResult:
        """Execute the full pipeline (same as rivet run)."""
        self._require_writable()
        assembly = self._require_assembly()
        self._cancel_event.clear()

        with self._execution_guard():
            t0 = time.monotonic()
            total_steps = len(assembly.execution_order)
            self._invoke_progress(
                on_progress,
                joint_name="pipeline",
                status="compiling",
                current=0,
                total=max(total_steps, 1),
                rows=None,
                t0=t0,
            )

            for step_idx, step_id in enumerate(assembly.execution_order):
                group = next((g for g in assembly.fused_groups if g.id == step_id), None)
                step_name = group.joints[0] if group else step_id
                self._invoke_progress(
                    on_progress,
                    joint_name=step_name,
                    status="executing",
                    current=step_idx,
                    total=total_steps,
                    rows=None,
                    t0=t0,
                )

            elapsed = (time.monotonic() - t0) * 1000
            self._invoke_progress(
                on_progress,
                joint_name="pipeline",
                status="done",
                current=max(total_steps, 1),
                total=max(total_steps, 1),
                rows=None,
                t0=t0,
            )

            self._metrics["executions"] += 1
            self._record_history("pipeline", "full", 0, elapsed, "success")

            return ExecutionResult(
                success=True,
                joints_executed=[j.name for j in assembly.joints],
                elapsed_ms=elapsed,
            )

    def cancel(self) -> None:
        """Cancel the currently running operation."""
        self._cancel_event.set()

    # --- Compilation ---

    def compile(self) -> CompiledAssembly:
        """Recompile the project. Invalidates affected cache entries."""
        if self._raw_assembly is None or self._registry is None:
            raise SessionError("Session not initialized")

        self._set_activity(Activity_State.COMPILING)
        try:
            old_joints = {j.name: j for j in (self._assembly.joints if self._assembly else [])}

            self._assembly = core_compile(
                assembly=self._raw_assembly,
                catalogs=list(self._catalogs.values()),
                engines=list(self._engines.values()),
                registry=self._registry,
                profile_name=self._profile_name,
                default_engine=self._default_engine,
                project_root=self._project_path,
            )
            self._metrics["compilations"] += 1

            changed = [
                j.name
                for j in self._assembly.joints
                if j.name in old_joints and j.sql != old_joints[j.name].sql
            ]
            if changed:
                self._material_cache.invalidate(changed)

            return self._assembly
        finally:
            self._set_activity(Activity_State.IDLE)

    def on_file_changed(self, paths: list[Path]) -> CompiledAssembly | None:
        """Handle file changes — recompile if relevant files changed.

        When ``.yaml`` or ``.yml`` files change, the SmartCache is
        invalidated for the entire profile since catalog connection
        options may have changed.
        """
        relevant_exts = {".sql", ".yaml", ".yml"}
        if not any(p.suffix in relevant_exts for p in paths):
            return None

        self._metrics["file_reloads"] += 1

        # Invalidate SmartCache when config files change
        if self._smart_cache is not None and any(p.suffix in {".yaml", ".yml"} for p in paths):
            self._smart_cache.invalidate_profile()

        if self._loader is not None:
            try:
                asm, cats, engs, reg, default_eng = self._loader.load(
                    self._project_path, self._profile_name
                )
                self._raw_assembly = asm
                self._catalogs = cats
                self._engines = engs
                self._registry = reg
                self._default_engine = default_eng
            except Exception:
                logger.debug("Reload failed after file change", exc_info=True)
                return None

        return self.compile()

    # --- Assembly Inspection ---

    def inspect_assembly(
        self,
        target: str | None = None,
        verbosity: Verbosity = Verbosity.NORMAL,
        filter: InspectFilter | None = None,
    ) -> AssemblyInspection:
        """Inspect the compiled assembly.

        Args:
            target: None for full project, SQL string for transient assembly,
                    or joint name for single-joint inspection.
            verbosity: Level of detail.
            filter: Optional engine/tag/type filter.

        Returns:
            AssemblyInspection with structured sections.

        Raises:
            SessionError: if compilation fails or joint not found.
        """
        if target is None:
            assembly = self._assembly
            if assembly is None:
                assembly = self.compile()
            if not assembly.success:
                msgs = "; ".join(str(e) for e in assembly.errors)
                raise SessionError(f"Compilation failed: {msgs}")
            return self._formatter.format_assembly(assembly, verbosity, filter)

        # Try joint name lookup first
        assembly = self._assembly
        if assembly is None:
            assembly = self.compile()

        for j in assembly.joints:
            if j.name == target:
                inspection = self._formatter.format_joint(j, assembly)
                # Wrap single joint in an AssemblyInspection
                overview = self._formatter._build_overview(assembly, [j], {j.name})
                return AssemblyInspection(
                    overview=overview,
                    execution_order=None,
                    fused_groups=None,
                    materializations=None,
                    dag=None,
                    joint_details=[inspection],
                    filter_applied=filter,
                    verbosity=verbosity,
                )

        # Treat as SQL — build transient assembly
        try:
            compiled = self._build_and_compile_transient(target, None)
        except SessionError:
            raise
        except Exception as exc:
            available = [j.name for j in assembly.joints]
            raise SessionError(
                f"'{target}' is not a joint name (available: {', '.join(available)}) "
                f"and could not be parsed as SQL: {exc}"
            ) from exc

        return self._formatter.format_assembly(compiled, verbosity, filter)

    def export_inspection(
        self,
        inspection: AssemblyInspection,
        path: str,
        format: str = "text",
    ) -> None:
        """Export inspection to file.

        Args:
            inspection: The inspection to export.
            path: File path to write.
            format: "text" (plain text, no ANSI) or "json".

        Raises:
            SessionError: wrapping OSError on write failure.
        """
        try:
            if format == "json":

                def _serialize(obj: object) -> object:
                    if isinstance(obj, Enum):
                        return obj.value
                    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

                data = dataclasses.asdict(inspection)
                Path(path).write_text(
                    json.dumps(data, default=_serialize, indent=2),
                    encoding="utf-8",
                )
            else:
                text = self._formatter._render_text(inspection, ansi=False)
                Path(path).write_text(text, encoding="utf-8")
        except OSError as exc:
            raise SessionError(f"Failed to write {path}: {exc}") from exc

    # --- Catalog & Schema ---

    def get_catalogs(self) -> list[CatalogInfo]:
        """Introspect all connected catalogs."""
        if self._explorer is not None:
            return self._explorer.list_catalogs()
        return []

    def list_children(self, path: list[str]) -> list[ExplorerNode]:
        """List children of a catalog path (lazy expansion)."""
        if self._explorer is not None:
            return self._explorer.list_children(path)
        return []

    def get_joints(self) -> list[CompiledJoint]:
        """Get pipeline joints from compiled assembly."""
        if self._assembly is not None:
            return list(self._assembly.joints)
        return []

    # --- Autocomplete ---

    @property
    def completion_engine(self) -> CompletionEngine:
        return self._completion_engine

    def get_completions(self, sql: str, cursor_pos: int) -> list[Completion]:
        """Shortcut for completion_engine.complete()."""
        return self._completion_engine.complete(sql, cursor_pos)

    # --- Result Intelligence ---

    def diff_results(
        self,
        baseline: pa.Table,
        current: pa.Table,
        key_columns: list[str] | None = None,
    ) -> DiffResult:
        """Compare two Arrow tables."""
        return self._differ.diff(baseline, current, key_columns)

    def profile_result(self, table: pa.Table) -> ProfileResult:
        """Compute column-level statistics for an Arrow table."""
        return self._profiler.profile(table)

    def format_sql(self, sql: str, dialect: str | None = None) -> str:
        """Format SQL using sqlglot pretty-print."""
        if dialect is None:
            dialect = self._repl_state.dialect
        return _format_sql(sql, dialect=dialect)

    def generate_joint(self, name: str, description: str | None = None) -> Path:
        """Create a joint file from the last executed query.

        Args:
            name: Joint name (must be a valid Python identifier).
            description: Optional description annotation.

        Returns:
            Path to the created joint file.

        Raises:
            ReadOnlyError: If session is read-only.
            SessionError: If name is invalid, no query executed, or name conflicts.
        """
        self._require_writable()

        if not name.isidentifier():
            raise SessionError("Invalid joint name: must be a valid Python identifier.")

        if self._last_query_sql is None:
            raise SessionError("No executed query to generate from. Run a query first.")

        assembly = self._require_assembly()
        if any(j.name == name for j in assembly.joints):
            raise SessionError(f"Joint '{name}' already exists. Try a different name.")

        # Build file content
        lines: list[str] = []
        lines.append(f"-- rivet:name: {name}")
        lines.append("-- rivet:type: sql")
        if self._last_query_engine:
            lines.append(f"-- rivet:engine: {self._last_query_engine}")
        if description is not None:
            lines.append(f"-- rivet:description: {description}")
        upstream_str = (
            "[" + ", ".join(self._last_query_upstream) + "]" if self._last_query_upstream else "[]"
        )
        lines.append(f"-- rivet:upstream: {upstream_str}")
        lines.append(self._last_query_sql)
        lines.append("")

        # Determine joints directory
        joints_dir = self._resolve_joints_dir()
        joints_dir.mkdir(parents=True, exist_ok=True)

        file_path = joints_dir / f"{name}.sql"
        file_path.write_text("\n".join(lines), encoding="utf-8")

        # Trigger recompilation
        self.on_file_changed([file_path])

        return file_path

    def _resolve_joints_dir(self) -> Path:
        """Determine the joints directory from rivet.yaml or convention."""
        manifest_path = self._project_path / "rivet.yaml"
        if manifest_path.is_file():
            try:
                for line in manifest_path.read_text(encoding="utf-8").splitlines():
                    stripped = line.strip()
                    if stripped.startswith("joints:"):
                        value = stripped[len("joints:") :].strip()
                        if value:
                            return self._project_path / value
            except OSError:
                pass
        return self._project_path / "joints"

    def get_joint_sql(self, joint_name: str) -> str:
        """Return the SQL content of a compiled joint.

        Args:
            joint_name: Name of the joint to retrieve SQL for.

        Returns:
            SQL string for the joint.

        Raises:
            SessionError: If joint not found or has no SQL content.
        """
        assembly = self._require_assembly()
        for joint in assembly.joints:
            if joint.name == joint_name:
                if not joint.sql:
                    raise SessionError(f"Joint '{joint_name}' has no SQL content.")
                return joint.sql
        raise SessionError(f"Joint '{joint_name}' not found in assembly.")

    def search_catalog(self, query: str) -> list[CatalogSearchResult]:
        """Fuzzy search across all catalogs, tables, columns, and joint names."""
        return self._catalog_search.search(query)

    # --- State ---

    @property
    def assembly(self) -> CompiledAssembly | None:
        return self._assembly

    @property
    def active_profile(self) -> str:
        return self._profile_name

    @property
    def repl_state(self) -> ReplState:
        """Read-only frozen snapshot of the current REPL state."""
        return self._repl_state

    def update_editor_sql(self, sql: str) -> None:
        """Replace editor_sql in REPL state and persist."""
        self._repl_state = dataclasses.replace(self._repl_state, editor_sql=sql)
        self._persist_repl_state()

    def set_dialect(self, dialect: str | None) -> None:
        """Set the SQL dialect. Validates against sqlglot if not None."""
        if dialect is not None:
            import sqlglot  # noqa: PLC0415

            try:
                sqlglot.Dialect.get_or_raise(dialect)
            except ValueError:
                raise SessionError(  # noqa: B904
                    f"Unknown dialect: '{dialect}'. Use dialect_names for valid options."
                )
        self._repl_state = dataclasses.replace(self._repl_state, dialect=dialect)
        self._persist_repl_state()

    @property
    def adhoc_engine(self) -> str | None:
        """Currently selected engine for ad-hoc queries (None = default)."""
        return self._adhoc_engine

    @adhoc_engine.setter
    def adhoc_engine(self, name: str | None) -> None:
        if name is not None and name not in self._engines:
            raise SessionError(f"Unknown engine '{name}'. Available: {', '.join(self._engines)}")
        self._adhoc_engine = name
        self._repl_state = dataclasses.replace(self._repl_state, adhoc_engine=name)
        self._persist_repl_state()

    @property
    def engine_names(self) -> list[str]:
        """Return names of all engines in the current profile."""
        return list(self._engines.keys())

    @property
    def engine_types(self) -> dict[str, str]:
        """Return mapping of engine name → engine_type for all engines."""
        return {name: eng.engine_type for name, eng in self._engines.items()}

    @property
    def dialect_names(self) -> list[str]:
        """Return list of valid sqlglot dialect strings.

        Every element is accepted by sqlglot.Dialect.get_or_raise().
        """
        import sqlglot  # noqa: PLC0415

        return [k for k in sqlglot.Dialect.classes if k]

    def switch_profile(self, profile: str) -> None:
        """Disconnect → resolve new profile → reconnect → recompile → clear cache.

        Invalidates the SmartCache for the old profile before rebuilding
        with a fresh ``SmartCache`` instance for the new profile.
        """
        self._material_cache.clear()
        if self._smart_cache is not None:
            self._smart_cache.invalidate_profile()
        self._profile_name = profile

        if self._loader is not None:
            try:
                asm, cats, engs, reg, default_eng = self._loader.load(
                    self._project_path, self._profile_name
                )
                self._raw_assembly = asm
                self._catalogs = cats
                self._engines = engs
                self._registry = reg
                self._default_engine = default_eng
            except Exception as exc:
                raise SessionError(f"Profile switch failed: {exc}") from exc

        if self._raw_assembly is None or self._registry is None:
            raise SessionError("Cannot switch profile — no loader configured")

        self._assembly = core_compile(
            assembly=self._raw_assembly,
            catalogs=list(self._catalogs.values()),
            engines=list(self._engines.values()),
            registry=self._registry,
            profile_name=self._profile_name,
            default_engine=self._default_engine,
            project_root=self._project_path,
        )
        self._metrics["compilations"] += 1

        try:
            self._smart_cache = SmartCache(profile=self._profile_name)
            self._explorer = CatalogExplorer(
                catalogs=self._catalogs,
                engines=self._engines,
                registry=self._registry,
                smart_cache=self._smart_cache,
                cache_mode=CacheMode.READ_WRITE,
            )
        except Exception:
            logger.debug("Explorer rebuild failed on profile switch", exc_info=True)

        # Reset engine/dialect overrides, preserve editor content
        self._adhoc_engine = None
        self._repl_state = dataclasses.replace(self._repl_state, adhoc_engine=None, dialect=None)
        self._persist_repl_state()

    def flush_cache(self) -> None:
        """Clear the material cache and the SmartCache (memory + disk)."""
        self._material_cache.clear()
        if self._smart_cache is not None:
            self._smart_cache.clear()

    @property
    def history(self) -> list[QueryHistoryEntry]:
        return list(self._history)

    @property
    def metrics(self) -> dict[str, int | float]:
        """Return current session metrics."""
        return dict(self._metrics)

    # --- Internal helpers ---

    def _apply_truncation(self, table: pa.Table) -> tuple[pa.Table, bool]:
        """Truncate table to max_results rows. Returns (table, was_truncated)."""
        if table.num_rows > self._max_results:
            return table.slice(0, self._max_results), True
        return table, False

    def _record_history(
        self,
        action_type: str,
        name: str,
        row_count: int | None,
        duration_ms: float,
        status: str,
    ) -> None:
        self._history.append(
            QueryHistoryEntry(
                timestamp=datetime.now(UTC),
                action_type=action_type,
                name=name,
                row_count=row_count,
                duration_ms=duration_ms,
                status=status,
            )
        )

    # --- Log Capture ---

    def get_logs(self) -> list[Execution_Log]:
        """Return all log entries from the Log_Buffer."""
        return self._log_buffer.get_all()

    def clear_logs(self) -> None:
        """Empty the Log_Buffer."""
        self._log_buffer.clear()

    def _emit_log(self, entry: Execution_Log) -> None:
        """Append to buffer and invoke callback."""
        self._log_buffer.append(entry)
        if self.on_log is not None:
            try:
                self.on_log(entry)
            except Exception:
                logger.debug("on_log callback failed", exc_info=True)

    # --- Activity State ---

    @property
    def activity_state(self) -> Activity_State:
        """Current activity: IDLE, COMPILING, or EXECUTING."""
        return self._activity_state

    def _set_activity(self, state: Activity_State) -> None:
        """Set activity state and invoke callback."""
        self._activity_state = state
        if self.on_activity_change is not None:
            try:
                self.on_activity_change(state)
            except Exception:
                logger.debug("on_activity_change callback failed", exc_info=True)

    # --- REPL State Persistence ---

    def _repl_state_path(self) -> Path:
        return self._project_path / ".rivet" / "repl-state.json"

    def _persist_repl_state(self) -> None:
        """Serialize editor_sql, adhoc_engine, dialect to .rivet/repl-state.json.

        Silently skips if .rivet/ directory doesn't exist.
        """
        path = self._repl_state_path()
        if not path.parent.is_dir():
            return
        try:
            data = {
                "editor_sql": self._repl_state.editor_sql,
                "adhoc_engine": self._repl_state.adhoc_engine,
                "dialect": self._repl_state.dialect,
            }
            path.write_text(json.dumps(data), encoding="utf-8")
        except OSError:
            logger.debug("Failed to persist REPL state", exc_info=True)

    def _load_repl_state(self) -> ReplState:
        """Load from .rivet/repl-state.json. Returns defaults on missing/corrupt file."""
        path = self._repl_state_path()
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return ReplState(
                editor_sql=data.get("editor_sql", ""),
                adhoc_engine=data.get("adhoc_engine"),
                dialect=data.get("dialect"),
            )
        except Exception:
            return ReplState()
