"""E2E tests for ``rivet catalog list`` path-based navigation.

Each test creates a temporary Rivet project with a DuckDB catalog containing
schemas, tables, and columns, then invokes ``_main(argv)`` in-process and
asserts on exit codes and stdout/stderr output.

Validates Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 2.1, 2.2, 3.1, 3.2, 3.3,
4.1, 4.2, 4.3
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from rivet_cli.app import _main
from tests.e2e.conftest import CLIResult

# ---------------------------------------------------------------------------
# Profiles template: single DuckDB catalog named "mycat"
# ---------------------------------------------------------------------------

_RIVET_YAML = """\
profiles: profiles.yaml
sources: sources
joints: joints
sinks: sinks
tests: tests
quality: quality
"""

_PROFILES_TEMPLATE = """\
default:
  catalogs:
    mycat:
      type: duckdb
      path: {db_path}
  engines:
    - name: duckdb_primary
      type: duckdb
      catalogs: [mycat]
  default_engine: duckdb_primary
"""


@pytest.fixture()
def catalog_project(tmp_path: Path) -> Path:
    """Scaffold a Rivet project with a DuckDB catalog containing test data.

    Creates:
      - mycat (DuckDB catalog)
        └─ main (schema — DuckDB default)
           ├─ users (table: id INTEGER, name VARCHAR, active BOOLEAN)
           └─ orders (table: id INTEGER, user_id INTEGER, amount DOUBLE)
    """
    import duckdb

    db_path = tmp_path / "warehouse.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR, active BOOLEAN)")
    conn.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount DOUBLE)")
    conn.close()

    (tmp_path / "rivet.yaml").write_text(_RIVET_YAML)
    (tmp_path / "profiles.yaml").write_text(_PROFILES_TEMPLATE.format(db_path=db_path))
    for d in ("sources", "joints", "sinks", "tests", "quality"):
        (tmp_path / d).mkdir(exist_ok=True)

    return tmp_path


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _run(project: Path, argv: list[str], capsys: pytest.CaptureFixture[str]) -> CLIResult:
    """Invoke ``catalog list`` with ``--project`` placed after the sub-subcommand.

    ``run_cli`` inserts ``--project`` after ``argv[0]``, which breaks nested
    subparsers (argparse defaults overwrite the flag).  For ``catalog list``
    we need ``--project`` after ``list``.
    """
    full_argv = ["catalog"] + argv[:1] + ["--project", str(project)] + argv[1:]
    exit_code = _main(full_argv)
    captured = capsys.readouterr()
    return CLIResult(exit_code=exit_code, stdout=captured.out, stderr=captured.err)


# ---------------------------------------------------------------------------
# Tests — Path-based navigation (Requirements 1.1–1.5)
# ---------------------------------------------------------------------------


def test_list_with_single_segment_path(
    catalog_project: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Single segment lists schemas/databases of the catalog (Req 1.3)."""
    result = _run(catalog_project, ["list", "mycat"], capsys)
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    # DuckDB default schema is "main" — should appear as a child
    assert "main" in result.stdout


def test_list_with_two_segment_path(
    catalog_project: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Two segments lists tables in a schema (Req 1.4)."""
    result = _run(catalog_project, ["list", "mycat.main"], capsys)
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "users" in result.stdout
    assert "orders" in result.stdout


def test_list_with_three_segment_path(
    catalog_project: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Three segments lists columns of a table (Req 1.5)."""
    result = _run(catalog_project, ["list", "mycat.main.users"], capsys)
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "id" in result.stdout
    assert "name" in result.stdout
    assert "active" in result.stdout


def test_list_with_path_and_depth(
    catalog_project: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Path with --depth 1 expands one level below the target (Req 1.2)."""
    result = _run(catalog_project, ["list", "mycat.main", "--depth", "1"], capsys)
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    # Should show tables AND their columns (one level deeper)
    assert "users" in result.stdout
    assert "orders" in result.stdout
    # Column names from the users table should appear
    assert "id" in result.stdout
    assert "name" in result.stdout


# ---------------------------------------------------------------------------
# Tests — Output format compatibility (Requirements 3.1–3.4)
# ---------------------------------------------------------------------------


def test_list_with_path_json_format(
    catalog_project: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """JSON format outputs valid JSON with required fields (Req 3.3)."""
    result = _run(catalog_project, ["list", "mycat.main", "--format", "json"], capsys)
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    data = json.loads(result.stdout)
    assert isinstance(data, list)
    assert len(data) >= 2  # at least users and orders
    for node in data:
        assert "name" in node
        assert "node_type" in node
        assert "path" in node
        assert "is_expandable" in node


def test_list_with_path_tree_format(
    catalog_project: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Tree format outputs tree indicators (Req 3.2)."""
    result = _run(catalog_project, ["list", "mycat.main", "--format", "tree"], capsys)
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "users" in result.stdout
    assert "orders" in result.stdout
    # Tree renderer uses expand/collapse/leaf symbols
    assert "▶" in result.stdout or "▼" in result.stdout or "·" in result.stdout


# ---------------------------------------------------------------------------
# Tests — Error handling (Requirements 2.1–2.2)
# ---------------------------------------------------------------------------


def test_list_unknown_catalog_error(
    catalog_project: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Unknown catalog returns exit code 10 with error message (Req 2.1)."""
    result = _run(catalog_project, ["list", "nosuchcatalog"], capsys)
    assert result.exit_code == 10
    assert "nosuchcatalog" in result.stderr.lower() or "not found" in result.stderr.lower()


def test_list_unresolved_segment_error(
    catalog_project: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Unresolved intermediate segment returns exit code 10 (Req 2.2)."""
    result = _run(catalog_project, ["list", "mycat.nosuchschema"], capsys)
    assert result.exit_code == 10
    assert "nosuchschema" in result.stderr.lower() or "not found" in result.stderr.lower()


# ---------------------------------------------------------------------------
# Tests — Backward compatibility (Requirements 4.1–4.3)
# ---------------------------------------------------------------------------


def test_list_no_path_backward_compat(
    catalog_project: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """No arguments produces catalog list with connection status (Req 4.1)."""
    result = _run(catalog_project, ["list"], capsys)
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    # Should show catalog name and connection status
    assert "mycat" in result.stdout
    assert "connected" in result.stdout.lower() or "●" in result.stdout


def test_list_single_segment_shows_children(
    catalog_project: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Single segment lists the catalog's children (schemas) directly.

    ``rivet catalog list mycat`` now drills into the catalog instead of
    showing the catalog info summary table.
    """
    result = _run(catalog_project, ["list", "mycat"], capsys)
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    # DuckDB default schema is "main" — should appear as a child
    assert "main" in result.stdout
