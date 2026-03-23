"""E2E tests for checkpoint source resolution.

Exercises the deferred resolution model: checkpoint joints return a DeferredRef
that downstream groups resolve through the adapter/source-plugin/fallback path,
and the compiler pre-resolves checkpoint_sources metadata on FusedGroups.

Validates: Property 9, Requirements 3.5, 4.3, 6.1, 6.2, 6.3, 7.1, 7.2, 7.3
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e.conftest import (
    read_sink_csv,
    run_cli,
    write_joint,
    write_sink,
    write_source,
)


def _write_checkpoint(
    project: Path,
    name: str,
    *,
    catalog: str,
    table: str,
    upstream: list[str],
) -> None:
    """Write a checkpoint joint YAML declaration into ``joints/``."""
    upstream_str = ", ".join(upstream)
    lines = [
        f"name: {name}",
        "type: checkpoint",
        f"catalog: {catalog}",
        f"table: {table}",
        f"upstream: [{upstream_str}]",
    ]
    (project / "joints" / f"{name}.yaml").write_text("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# 8.1 — Single-engine checkpoint pipeline: source → sql → checkpoint → sql → sink
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_single_engine_checkpoint_pipeline(rivet_project: Path, capsys) -> None:
    """Source → SQL → Checkpoint → SQL → Sink on DuckDB produces correct output.

    The checkpoint writes intermediate data, and the downstream SQL joint
    reads it back through the deferred resolution path. The sink output
    must match the expected transformed data (backward compatibility).
    """
    project = rivet_project

    (project / "data" / "sales.csv").write_text(
        "id,region,revenue\n1,east,100\n2,west,200\n3,east,150\n4,west,50\n"
    )

    write_source(project, "src_sales", catalog="local", table="sales")
    write_joint(
        project,
        "filter_east",
        "SELECT id, revenue FROM src_sales WHERE region = 'east'",
    )
    _write_checkpoint(
        project,
        "cp_east_sales",
        catalog="local",
        table="cp_east_sales",
        upstream=["filter_east"],
    )
    write_joint(
        project,
        "add_tax",
        "SELECT id, revenue, CAST(revenue * 0.1 AS INTEGER) AS tax FROM cp_east_sales",
    )
    write_sink(
        project,
        "sink_taxed",
        catalog="local",
        table="sink_taxed",
        upstream=["add_tax"],
    )

    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    # Checkpoint table should exist with filtered rows
    cp_table = read_sink_csv(project, "cp_east_sales")
    assert cp_table.num_rows == 2
    assert set(cp_table.column_names) == {"id", "revenue"}
    assert sorted(cp_table.column("id").to_pylist()) == [1, 3]

    # Sink should have the tax-augmented rows
    sink_table = read_sink_csv(project, "sink_taxed")
    assert sink_table.num_rows == 2
    assert set(sink_table.column_names) == {"id", "revenue", "tax"}
    revenues = sorted(sink_table.column("revenue").to_pylist())
    assert revenues == [100, 150]
    taxes = sorted(sink_table.column("tax").to_pylist())
    assert taxes == [10, 15]


# ---------------------------------------------------------------------------
# 8.2 — Checkpoint fan-out: source → checkpoint → [sql_a, sql_b] → [sink_a, sink_b]
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_checkpoint_fanout_both_sinks_correct(rivet_project: Path, capsys) -> None:
    """Checkpoint fans out to two downstream branches; both sinks receive correct data.

    This validates that multiple downstream groups can independently resolve
    the same checkpoint DeferredRef and produce correct, independent results.
    """
    project = rivet_project

    (project / "data" / "employees.csv").write_text(
        "id,name,dept,salary\n1,Alice,eng,120\n2,Bob,sales,90\n3,Carol,eng,110\n"
    )

    write_source(project, "src_employees", catalog="local", table="employees")
    _write_checkpoint(
        project,
        "cp_employees",
        catalog="local",
        table="cp_employees",
        upstream=["src_employees"],
    )
    write_joint(
        project,
        "names_only",
        "SELECT id, name FROM cp_employees",
    )
    write_joint(
        project,
        "salary_summary",
        "SELECT dept, SUM(salary) AS total_salary FROM cp_employees GROUP BY dept",
    )
    write_sink(
        project,
        "sink_names",
        catalog="local",
        table="sink_names",
        upstream=["names_only"],
    )
    write_sink(
        project,
        "sink_salaries",
        catalog="local",
        table="sink_salaries",
        upstream=["salary_summary"],
    )

    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    # Branch A: names only
    names_table = read_sink_csv(project, "sink_names")
    assert names_table.num_rows == 3
    assert set(names_table.column_names) == {"id", "name"}
    assert sorted(names_table.column("name").to_pylist()) == ["Alice", "Bob", "Carol"]

    # Branch B: salary summary by dept
    salary_table = read_sink_csv(project, "sink_salaries")
    assert salary_table.num_rows == 2
    assert set(salary_table.column_names) == {"dept", "total_salary"}
    dept_col = salary_table.column("dept").to_pylist()
    total_col = salary_table.column("total_salary").to_pylist()
    salary_by_dept = dict(zip(dept_col, total_col))
    assert salary_by_dept["eng"] == 230
    assert salary_by_dept["sales"] == 90


# ---------------------------------------------------------------------------
# 8.3 — Compile output shows checkpoint_sources metadata
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_compile_output_includes_checkpoint_sources(rivet_project: Path, capsys) -> None:
    """Compiling a checkpoint pipeline shows checkpoint source resolution metadata.

    The compiler pre-resolves adapter info for (downstream_engine_type,
    checkpoint_catalog_type) via _build_checkpoint_sources. For a filesystem
    catalog with a DuckDB engine, no duckdb:filesystem adapter exists, so
    the compiler emits a fallback warning. This warning proves the
    checkpoint_sources resolution ran and is visible at compile time.

    Additionally, the pipeline must run successfully using the fallback path.
    """
    project = rivet_project

    (project / "data" / "items.csv").write_text("id,value\n1,10\n2,20\n")

    write_source(project, "src_items", catalog="local", table="items")
    _write_checkpoint(
        project,
        "cp_items",
        catalog="local",
        table="cp_items",
        upstream=["src_items"],
    )
    write_joint(project, "read_cp", "SELECT id, value FROM cp_items")
    write_sink(
        project,
        "sink_items",
        catalog="local",
        table="sink_items",
        upstream=["read_cp"],
    )

    result = run_cli(project, ["compile", "--verbose", "--no-color"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    # The compiler should emit a warning about checkpoint source resolution
    # for the (duckdb, filesystem) pair — no adapter exists, so it falls back
    # to SourcePlugin or Arrow passthrough.
    combined = result.stdout + result.stderr
    assert "checkpoint" in combined.lower(), (
        f"Expected checkpoint-related output in compile:\n{combined}"
    )
    assert "cp_items" in combined, f"Expected 'cp_items' in compile output:\n{combined}"

    # The pipeline should still run successfully via the fallback path
    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    sink_table = read_sink_csv(project, "sink_items")
    assert sink_table.num_rows == 2
    assert set(sink_table.column_names) == {"id", "value"}
    assert sorted(sink_table.column("id").to_pylist()) == [1, 2]


# ---------------------------------------------------------------------------
# 4.1 — Checkpoint CTE appears in compiled fused SQL (single-engine)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_checkpoint_cte_in_compiled_fused_sql(rivet_project: Path, capsys) -> None:
    """Compiled fused SQL contains checkpoint CTE for cross-group checkpoint deps.

    Validates: Requirements 1.1, 5.1
    """
    project = rivet_project

    (project / "data" / "sales.csv").write_text("id,region,revenue\n1,east,100\n2,west,200\n")

    write_source(project, "src_sales", catalog="local", table="sales")
    write_joint(
        project,
        "filter_east",
        "SELECT id, revenue FROM src_sales WHERE region = 'east'",
    )
    _write_checkpoint(
        project,
        "cp_east_sales",
        catalog="local",
        table="cp_east_sales",
        upstream=["filter_east"],
    )
    write_joint(
        project,
        "add_tax",
        "SELECT id, revenue, CAST(revenue * 0.1 AS INTEGER) AS tax FROM cp_east_sales",
    )
    write_sink(
        project,
        "sink_taxed",
        catalog="local",
        table="sink_taxed",
        upstream=["add_tax"],
    )

    result = run_cli(project, ["compile", "--verbose", "--no-color"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    # The downstream fused group (containing add_tax) should have a checkpoint
    # CTE injected for cp_east_sales in its fused SQL.
    combined = result.stdout + result.stderr
    assert "cp_east_sales AS (" in combined, (
        f"Expected checkpoint CTE 'cp_east_sales AS (' in compile output:\n{combined}"
    )
    # The CTE body should reference the checkpoint table via the engine-native
    # reader (e.g. read_csv_auto for filesystem catalogs) or a SELECT *.
    assert "SELECT * FROM" in combined, f"Expected 'SELECT * FROM' in compile output:\n{combined}"

    # Also verify the pipeline runs and produces correct data
    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    sink_table = read_sink_csv(project, "sink_taxed")
    assert sink_table.num_rows == 1
    assert set(sink_table.column_names) == {"id", "revenue", "tax"}
    assert sink_table.column("id").to_pylist() == [1]
    assert sink_table.column("revenue").to_pylist() == [100]
    assert sink_table.column("tax").to_pylist() == [10]


# ---------------------------------------------------------------------------
# 4.2 — Multiple checkpoint CTEs in compiled fused SQL
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_multi_checkpoint_ctes_in_compiled_fused_sql(rivet_project: Path, capsys) -> None:
    """Multiple checkpoint CTEs from different groups appear in downstream fused SQL.

    Validates: Requirements 1.1, 1.3, 5.1
    """
    project = rivet_project

    (project / "data" / "products.csv").write_text("id,name,price\n1,Widget,10\n2,Gadget,20\n")
    (project / "data" / "regions.csv").write_text("id,region\n1,east\n2,west\n")

    write_source(project, "src_products", catalog="local", table="products")
    write_source(project, "src_regions", catalog="local", table="regions")
    _write_checkpoint(
        project,
        "cp_products",
        catalog="local",
        table="cp_products",
        upstream=["src_products"],
    )
    _write_checkpoint(
        project,
        "cp_regions",
        catalog="local",
        table="cp_regions",
        upstream=["src_regions"],
    )
    write_joint(
        project,
        "combined",
        "SELECT p.id, p.name, p.price, r.region "
        "FROM cp_products p INNER JOIN cp_regions r ON p.id = r.id",
    )
    write_sink(
        project,
        "sink_combined",
        catalog="local",
        table="sink_combined",
        upstream=["combined"],
    )

    result = run_cli(project, ["compile", "--verbose", "--no-color"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    # Both checkpoint CTEs should appear in the downstream group's fused SQL
    combined = result.stdout + result.stderr
    assert "cp_products AS (" in combined, (
        f"Expected 'cp_products AS (' CTE in compile output:\n{combined}"
    )
    assert "cp_regions AS (" in combined, (
        f"Expected 'cp_regions AS (' CTE in compile output:\n{combined}"
    )

    # Also verify the pipeline runs and produces correct data
    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    sink_table = read_sink_csv(project, "sink_combined")
    assert sink_table.num_rows == 2
    assert set(sink_table.column_names) == {"id", "name", "price", "region"}
