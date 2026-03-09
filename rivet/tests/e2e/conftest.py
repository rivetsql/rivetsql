"""E2E test harness: fixtures and helpers for end-to-end CLI tests."""

from __future__ import annotations

import dataclasses
import pathlib
from pathlib import Path
from unittest.mock import patch

import pyarrow.csv as pcsv
import pytest

from rivet_cli.app import _main

_E2E_DIR = pathlib.Path(__file__).resolve().parent


def pytest_collection_modifyitems(items):
    for item in items:
        if _E2E_DIR in pathlib.Path(item.fspath).resolve().parents:
            item.add_marker(pytest.mark.e2e)


# ---------------------------------------------------------------------------
# Ensure DuckDB plugin is available even without installed entry points.
# On CI the sub-packages (rivet_duckdb, etc.) are on sys.path via
# pytest's ``pythonpath`` but are NOT installed as packages, so
# ``importlib.metadata.entry_points(group="rivet.plugins")`` returns
# nothing.  We wrap ``register_optional_plugins`` to fall back to a
# direct import when entry-point discovery misses the DuckDB plugin.
# ---------------------------------------------------------------------------

_original_register = None


def _register_with_fallback(registry, only=None):
    """Call the real register_optional_plugins, then ensure DuckDB is present."""
    _original_register(registry, only=only)
    if "duckdb" not in registry._engine_plugins:
        try:
            from rivet_duckdb import DuckDBPlugin
            DuckDBPlugin(registry)
        except Exception:
            pass


@pytest.fixture(autouse=True, scope="session")
def _ensure_duckdb_plugin():
    """Patch register_optional_plugins so DuckDB is always available in e2e tests.

    The function is imported by name into many CLI command modules at
    import time, so patching only ``rivet_bridge.plugins`` is not enough.
    We must also patch every module that already holds a local reference.
    """
    import contextlib
    import rivet_bridge
    import rivet_bridge.plugins as _bp

    global _original_register
    _original_register = _bp.register_optional_plugins

    # All modules that bind ``register_optional_plugins`` as a local name.
    _targets = [
        (_bp, "register_optional_plugins"),
        (rivet_bridge, "register_optional_plugins"),
    ]

    # CLI command modules that do top-level imports.
    for mod_name in (
        "rivet_cli.commands.compile",
        "rivet_cli.commands.run",
        "rivet_cli.commands.test",
        "rivet_cli.commands.engine",
        "rivet_cli.commands.catalog",
        "rivet_cli.commands.catalog_create",
        "rivet_cli.commands.engine_create",
    ):
        import importlib
        try:
            mod = importlib.import_module(mod_name)
            if hasattr(mod, "register_optional_plugins"):
                _targets.append((mod, "register_optional_plugins"))
        except ImportError:
            pass

    with contextlib.ExitStack() as stack:
        for target_mod, attr in _targets:
            stack.enter_context(patch.object(target_mod, attr, _register_with_fallback))
        yield

# ---------------------------------------------------------------------------
# rivet.yaml and profiles.yaml templates
# ---------------------------------------------------------------------------

_RIVET_YAML = """\
profiles: profiles.yaml
sources: sources
joints: joints
sinks: sinks
tests: tests
quality: quality
"""

_PROFILES_YAML = """\
default:
  catalogs:
    local:
      type: filesystem
      path: ./data
      format: csv
  engines:
    - name: duckdb_primary
      type: duckdb
      catalogs: [local]
    - name: duckdb_secondary
      type: duckdb
      catalogs: [local]
  default_engine: duckdb_primary
"""

# ---------------------------------------------------------------------------
# CLIResult dataclass
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class CLIResult:
    """Captured result of a CLI invocation."""

    exit_code: int
    stdout: str
    stderr: str


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def rivet_project(tmp_path: Path) -> Path:
    """Create a bare Rivet scaffold and return the project root.

    The scaffold contains rivet.yaml, profiles.yaml (two DuckDB engine
    instances: duckdb_primary, duckdb_secondary; filesystem catalog: local),
    and the standard directory structure.

    Cleanup is automatic via pytest's tmp_path.
    """
    (tmp_path / "rivet.yaml").write_text(_RIVET_YAML)
    (tmp_path / "profiles.yaml").write_text(_PROFILES_YAML)

    for d in ("sources", "joints", "sinks", "tests", "quality", "data"):
        (tmp_path / d).mkdir()

    return tmp_path


# ---------------------------------------------------------------------------
# CLI helper
# ---------------------------------------------------------------------------


def run_cli(project_path: Path, argv: list[str], capsys: pytest.CaptureFixture[str]) -> CLIResult:
    """Invoke ``_main`` with ``--project`` pointing at *project_path*.

    Captures stdout/stderr via *capsys* and returns a :class:`CLIResult`.
    The helper never raises on CLI failures — it always returns the exit code.

    The ``--project`` flag is inserted *after* the subcommand name so that
    argparse's subparser picks it up correctly (placing it before the
    subcommand causes the subparser default to overwrite the value).
    """
    if argv:
        # Insert --project after the first positional arg (the subcommand)
        full_argv = [argv[0], "--project", str(project_path)] + argv[1:]
    else:
        full_argv = ["--project", str(project_path)]
    exit_code = _main(full_argv)
    captured = capsys.readouterr()
    return CLIResult(exit_code=exit_code, stdout=captured.out, stderr=captured.err)


# ---------------------------------------------------------------------------
# File helpers
# ---------------------------------------------------------------------------


def write_source(
    project: Path,
    name: str,
    *,
    catalog: str,
    table: str,
) -> None:
    """Write a source SQL declaration file into ``sources/``.

    The declaration omits a SQL body so that the filesystem catalog adapter
    reads the data file directly and registers it as an input table.
    Including ``SELECT * FROM {table}`` would fail for filesystem catalogs
    because DuckDB cannot resolve the table name natively.
    """
    content = (
        f"-- rivet:name: {name}\n"
        f"-- rivet:type: source\n"
        f"-- rivet:catalog: {catalog}\n"
        f"-- rivet:table: {table}\n"
    )
    (project / "sources" / f"{name}.sql").write_text(content)


def write_joint(
    project: Path,
    name: str,
    sql: str,
    *,
    engine: str | None = None,
) -> None:
    """Write a SQL joint declaration file into ``joints/``."""
    lines = [
        f"-- rivet:name: {name}",
        "-- rivet:type: sql",
    ]
    if engine is not None:
        lines.append(f"-- rivet:engine: {engine}")
    lines.append(sql)
    (project / "joints" / f"{name}.sql").write_text("\n".join(lines) + "\n")


def write_sink(
    project: Path,
    name: str,
    *,
    catalog: str,
    table: str,
    upstream: list[str],
) -> None:
    """Write a sink SQL declaration file into ``sinks/``."""
    upstream_str = ", ".join(upstream)
    content = (
        f"-- rivet:name: {name}\n"
        f"-- rivet:type: sink\n"
        f"-- rivet:catalog: {catalog}\n"
        f"-- rivet:table: {table}\n"
        f"-- rivet:upstream: [{upstream_str}]\n"
    )
    (project / "sinks" / f"{name}.sql").write_text(content)


def read_sink_csv(project: Path, table: str) -> pyarrow.Table:  # noqa: F821
    """Read a filesystem sink output CSV file via ``pyarrow.csv.read_csv``."""

    path = project / "data" / f"{table}.csv"
    return pcsv.read_csv(str(path))


# ---------------------------------------------------------------------------
# QueryRecorder — runtime SQL interception for optimizer assertions
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class RecordedQuery:
    """A single SQL query captured by :class:`QueryRecorder`."""

    engine_name: str
    sql: str
    input_table_names: list[str]


class QueryRecorder:
    """Records every SQL query dispatched to DuckDB during ``rivet run``.

    Monkeypatches :meth:`DuckDBComputeEnginePlugin.execute_sql` to capture
    ``(engine_name, sql, input_table_names)`` for each call, while still
    executing the original method so the pipeline produces real output.

    Usage::

        recorder = QueryRecorder()
        with recorder:
            run_cli(project, ["run"], capsys)
        assert recorder.query_count("duckdb_primary") == 1
    """

    def __init__(self) -> None:
        self.queries: list[RecordedQuery] = []
        self._original_execute_sql = None

    def __enter__(self) -> QueryRecorder:
        from rivet_duckdb.engine import DuckDBComputeEnginePlugin

        self._original_execute_sql = DuckDBComputeEnginePlugin.execute_sql

        recorder = self

        def _recording_execute_sql(plugin_self, engine, sql, input_tables):
            recorder.queries.append(
                RecordedQuery(
                    engine_name=engine.name,
                    sql=sql,
                    input_table_names=list(input_tables.keys()),
                )
            )
            return recorder._original_execute_sql(plugin_self, engine, sql, input_tables)

        DuckDBComputeEnginePlugin.execute_sql = _recording_execute_sql  # type: ignore[assignment]
        return self

    def __exit__(self, *exc: object) -> None:
        from rivet_duckdb.engine import DuckDBComputeEnginePlugin

        if self._original_execute_sql is not None:
            DuckDBComputeEnginePlugin.execute_sql = self._original_execute_sql  # type: ignore[assignment]
            self._original_execute_sql = None

    # -- Convenience helpers -------------------------------------------------

    def queries_for_engine(self, engine_name: str) -> list[RecordedQuery]:
        """Return only queries dispatched to a specific engine instance."""
        return [q for q in self.queries if q.engine_name == engine_name]

    def query_count(self, engine_name: str | None = None) -> int:
        """Total query count, optionally filtered by engine."""
        if engine_name is None:
            return len(self.queries)
        return len(self.queries_for_engine(engine_name))

    def assert_single_query(self, engine_name: str) -> RecordedQuery:
        """Assert exactly one query was sent to *engine_name* and return it."""
        matched = self.queries_for_engine(engine_name)
        assert len(matched) == 1, (
            f"Expected exactly 1 query for engine {engine_name!r}, "
            f"got {len(matched)}: {[q.sql[:80] for q in matched]}"
        )
        return matched[0]


@pytest.fixture()
def query_recorder() -> QueryRecorder:
    """Provide a :class:`QueryRecorder` that patches DuckDB ``execute_sql``."""
    return QueryRecorder()


@pytest.fixture(autouse=True)
def _reset_default_thread_pool() -> None:  # type: ignore[return]
    """Force garbage collection after each test to clean up DuckDB connections.

    DuckDB in-process connections created inside ``asyncio.to_thread()``
    worker threads can survive across test boundaries.  A GC pass ensures
    they are finalized promptly.
    """
    yield
    import gc

    gc.collect()
