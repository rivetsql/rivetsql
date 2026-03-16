"""E2E tests for native SQL write optimization.

Exercises the full pipeline lifecycle with DuckDB engine + DuckDB catalog,
verifying that native SQL write eliminates the Arrow round-trip for
sink and checkpoint writes with supported strategies.

The tests use a filesystem source catalog (CSV) and a DuckDB sink catalog.
The DuckDB engine reads from filesystem via adapter, then the native SQL
write path writes directly to the DuckDB sink catalog.

Requirements: 2.1, 2.2, 3.1, 3.2, 3.3, 4.1, 4.2, 7.1, 7.2, 8.1, 9.1, 9.2
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pytest

from tests.e2e.conftest import run_cli, write_source

# ---------------------------------------------------------------------------
# Project templates
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
    local:
      type: filesystem
      path: {data_dir}
      format: csv
    sink_db:
      type: duckdb
      path: {sink_path}
  engines:
    - name: duckdb_primary
      type: duckdb
      catalogs: [local, sink_db]
  default_engine: duckdb_primary
"""


@pytest.fixture()
def duckdb_sink_project(tmp_path: Path) -> Path:
    """Scaffold a Rivet project with filesystem source and DuckDB sink catalogs."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    sink_path = tmp_path / "sink.duckdb"

    # Pre-create sink DB so catalog validation passes
    conn = duckdb.connect(str(sink_path))
    conn.close()

    # Seed source CSV
    (data_dir / "products.csv").write_text("id,name,price\n1,Widget,10\n2,Gadget,25\n3,Gizmo,50\n")

    (tmp_path / "rivet.yaml").write_text(_RIVET_YAML)
    (tmp_path / "profiles.yaml").write_text(
        _PROFILES_TEMPLATE.format(data_dir=data_dir, sink_path=sink_path)
    )
    for d in ("sources", "joints", "sinks", "tests", "quality"):
        (tmp_path / d).mkdir(exist_ok=True)

    return tmp_path


def _read_sink_duckdb(project: Path, table: str) -> pa.Table:
    """Read a table from the sink DuckDB database."""
    sink_db = project / "sink.duckdb"
    conn = duckdb.connect(str(sink_db), read_only=True)
    try:
        result = conn.execute(f"SELECT * FROM {table}").arrow()
        if hasattr(result, "read_all"):
            result = result.read_all()
        return result
    finally:
        conn.close()


def _write_sink_to_duckdb(
    project: Path,
    name: str,
    table: str,
    upstream: list[str],
    strategy: str = "replace",
) -> None:
    upstream_str = ", ".join(upstream)
    content = (
        f"-- rivet:name: {name}\n"
        f"-- rivet:type: sink\n"
        f"-- rivet:catalog: sink_db\n"
        f"-- rivet:table: {table}\n"
        f"-- rivet:upstream: [{upstream_str}]\n"
        f"-- rivet:write_strategy: {{mode: {strategy}}}\n"
    )
    (project / "sinks" / f"{name}.sql").write_text(content)


def _write_sql_joint(project: Path, name: str, sql: str) -> None:
    content = f"-- rivet:name: {name}\n-- rivet:type: sql\n{sql}\n"
    (project / "joints" / f"{name}.sql").write_text(content)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_sink_native_sql_write_replace(duckdb_sink_project: Path, capsys: Any) -> None:
    """Full pipeline source→sql→sink with DuckDB sink, strategy replace."""
    project = duckdb_sink_project
    write_source(project, "src_products", catalog="local", table="products")
    _write_sql_joint(
        project,
        "transform",
        "SELECT id, name, price * 2 AS doubled FROM src_products",
    )
    _write_sink_to_duckdb(project, "sink_out", "output", ["transform"], "replace")

    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    table = _read_sink_duckdb(project, "output")
    assert table.num_rows == 3
    assert sorted(table.column("id").to_pylist()) == [1, 2, 3]
    assert sorted(table.column("doubled").to_pylist()) == [20.0, 50.0, 100.0]


def test_sink_native_sql_write_append(duckdb_sink_project: Path, capsys: Any) -> None:
    """Strategy append accumulates rows across runs."""
    project = duckdb_sink_project
    write_source(project, "src_products", catalog="local", table="products")
    _write_sink_to_duckdb(project, "sink_out", "output", ["src_products"], "append")

    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"
    assert _read_sink_duckdb(project, "output").num_rows == 3

    result2 = run_cli(project, ["run"], capsys)
    assert result2.exit_code == 0, f"second run failed:\n{result2.stderr}"
    assert _read_sink_duckdb(project, "output").num_rows == 6


def test_sink_native_sql_write_truncate_insert(duckdb_sink_project: Path, capsys: Any) -> None:
    """Strategy truncate_insert replaces data on each run."""
    project = duckdb_sink_project
    write_source(project, "src_products", catalog="local", table="products")
    _write_sink_to_duckdb(
        project,
        "sink_out",
        "output",
        ["src_products"],
        "truncate_insert",
    )

    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"
    assert _read_sink_duckdb(project, "output").num_rows == 3

    result2 = run_cli(project, ["run"], capsys)
    assert result2.exit_code == 0, f"second run failed:\n{result2.stderr}"
    assert _read_sink_duckdb(project, "output").num_rows == 3


def test_checkpoint_native_sql_write(duckdb_sink_project: Path, capsys: Any) -> None:
    """Pipeline source→sql→checkpoint→sql→sink uses native write for checkpoint."""
    project = duckdb_sink_project
    write_source(project, "src_products", catalog="local", table="products")
    _write_sql_joint(project, "transform", "SELECT id, name FROM src_products")

    # Checkpoint joint writing to DuckDB catalog
    checkpoint = (
        "name: cp\n"
        "type: checkpoint\n"
        "catalog: sink_db\n"
        "table: checkpoint_tbl\n"
        "upstream: [transform]\n"
    )
    (project / "joints" / "cp.yaml").write_text(checkpoint)

    _write_sql_joint(project, "post_cp", "SELECT id, name FROM cp")
    _write_sink_to_duckdb(project, "sink_out", "final_output", ["post_cp"])

    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    final = _read_sink_duckdb(project, "final_output")
    assert final.num_rows == 3
    assert sorted(final.column("id").to_pylist()) == [1, 2, 3]


def test_fallback_to_filesystem_sink(duckdb_sink_project: Path, capsys: Any) -> None:
    """DuckDB engine + filesystem sink uses Arrow fallback (no native write)."""
    project = duckdb_sink_project
    write_source(project, "src_products", catalog="local", table="products")

    # Sink to filesystem catalog (not DuckDB) — should use Arrow fallback
    sink_content = (
        "-- rivet:name: sink_fs\n"
        "-- rivet:type: sink\n"
        "-- rivet:catalog: local\n"
        "-- rivet:table: products_out\n"
        "-- rivet:upstream: [src_products]\n"
    )
    (project / "sinks" / "sink_fs.sql").write_text(sink_content)

    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    import pyarrow.csv as pcsv

    data_dir = project / "data"
    out = pcsv.read_csv(str(data_dir / "products_out.csv"))
    assert out.num_rows == 3
