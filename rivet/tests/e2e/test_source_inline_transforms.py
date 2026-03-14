"""E2E tests for source inline transforms.

Exercises the full pipeline lifecycle for source joints with YAML columns,
filter, limit, SQL equivalents, aliased expressions, CAST expressions,
single-table constraint violations, and cross-group interaction.

Source joints with inline transforms (columns/filter/limit) generate SQL
that gets fused with downstream joints on the same engine. To test the
adapter-level pushdown path, tests use a secondary engine for the downstream
joint, forcing a materialization boundary that reads the source via the
adapter and applies inline transforms.
"""

from __future__ import annotations

from pathlib import Path

from tests.e2e.conftest import read_sink_csv, run_cli, write_joint, write_sink

# ---------------------------------------------------------------------------
# 11.1 — YAML columns + filter → filtered/projected output
# ---------------------------------------------------------------------------


def test_yaml_columns_and_filter(rivet_project: Path, capsys) -> None:
    """Source with YAML columns + filter produces only declared columns and filtered rows."""
    project = rivet_project

    (project / "data" / "orders.csv").write_text(
        "id,customer,amount,status\n"
        "1,Alice,100,active\n"
        "2,Bob,30,inactive\n"
        "3,Charlie,75,active\n"
        "4,Diana,200,inactive\n"
        "5,Eve,50,active\n"
    )

    (project / "sources" / "src_orders.yaml").write_text(
        "name: src_orders\n"
        "type: source\n"
        "catalog: local\n"
        "table: orders\n"
        "columns:\n"
        "  - id\n"
        "  - customer\n"
        "  - amount\n"
        "filter: status = 'active'\n"
    )

    # Use secondary engine to force materialization at the source boundary
    write_joint(
        project,
        "pass_orders",
        "SELECT * FROM src_orders",
        engine="duckdb_secondary",
    )
    write_sink(
        project,
        "sink_orders",
        catalog="local",
        table="orders_out",
        upstream=["pass_orders"],
    )

    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    table = read_sink_csv(project, "orders_out")
    assert set(table.column_names) == {"id", "customer", "amount"}
    assert sorted(table.column("id").to_pylist()) == [1, 3, 5]
    assert table.num_rows == 3


# ---------------------------------------------------------------------------
# 11.2 — SQL equivalent → same output as YAML form
# ---------------------------------------------------------------------------


def test_sql_equivalent_matches_yaml(rivet_project: Path, capsys) -> None:
    """Source declared in SQL produces identical output to the YAML form."""
    project = rivet_project

    (project / "data" / "orders.csv").write_text(
        "id,customer,amount,status\n"
        "1,Alice,100,active\n"
        "2,Bob,30,inactive\n"
        "3,Charlie,75,active\n"
        "4,Diana,200,inactive\n"
        "5,Eve,50,active\n"
    )

    source_sql = (
        "-- rivet:name: src_orders\n"
        "-- rivet:type: source\n"
        "-- rivet:catalog: local\n"
        "-- rivet:table: orders\n"
        "SELECT id, customer, amount FROM orders WHERE status = 'active'\n"
    )
    (project / "sources" / "src_orders.sql").write_text(source_sql)

    write_joint(
        project,
        "pass_orders",
        "SELECT * FROM src_orders",
        engine="duckdb_secondary",
    )
    write_sink(
        project,
        "sink_orders",
        catalog="local",
        table="orders_out",
        upstream=["pass_orders"],
    )

    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    table = read_sink_csv(project, "orders_out")
    assert set(table.column_names) == {"id", "customer", "amount"}
    assert sorted(table.column("id").to_pylist()) == [1, 3, 5]
    assert table.num_rows == 3


# ---------------------------------------------------------------------------
# 11.3 — Aliased expression → computed column in output
# ---------------------------------------------------------------------------


def test_aliased_expression_computed_column(rivet_project: Path, capsys) -> None:
    """Source with aliased expression produces computed column with correct values."""
    project = rivet_project

    (project / "data" / "items.csv").write_text("id,price,quantity\n1,10,5\n2,20,3\n3,15,4\n")

    (project / "sources" / "src_items.yaml").write_text(
        "name: src_items\n"
        "type: source\n"
        "catalog: local\n"
        "table: items\n"
        "columns:\n"
        "  - id\n"
        "  - revenue: price * quantity\n"
    )

    write_joint(
        project,
        "pass_items",
        "SELECT * FROM src_items",
        engine="duckdb_secondary",
    )
    write_sink(
        project,
        "sink_items",
        catalog="local",
        table="items_out",
        upstream=["pass_items"],
    )

    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    table = read_sink_csv(project, "items_out")
    assert set(table.column_names) == {"id", "revenue"}
    assert table.num_rows == 3

    ids = table.column("id").to_pylist()
    revenues = table.column("revenue").to_pylist()
    row_map = dict(zip(ids, revenues))
    assert row_map[1] == 50  # 10 * 5
    assert row_map[2] == 60  # 20 * 3
    assert row_map[3] == 60  # 15 * 4


# ---------------------------------------------------------------------------
# 11.4 — CAST expression → verify type in output
# ---------------------------------------------------------------------------


def test_cast_expression_type(rivet_project: Path, capsys) -> None:
    """Source with CAST expression produces column with target type."""
    project = rivet_project

    (project / "data" / "raw.csv").write_text("id,raw_amount\n1,100\n2,200\n3,300\n")

    (project / "sources" / "src_raw.yaml").write_text(
        "name: src_raw\n"
        "type: source\n"
        "catalog: local\n"
        "table: raw\n"
        "columns:\n"
        "  - id\n"
        "  - amount: CAST(raw_amount AS DOUBLE)\n"
    )

    write_joint(
        project,
        "pass_raw",
        "SELECT * FROM src_raw",
        engine="duckdb_secondary",
    )
    write_sink(
        project,
        "sink_raw",
        catalog="local",
        table="raw_out",
        upstream=["pass_raw"],
    )

    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    table = read_sink_csv(project, "raw_out")
    assert set(table.column_names) == {"id", "amount"}
    assert table.num_rows == 3

    amounts = table.column("amount").to_pylist()
    assert sorted(amounts) == [100.0, 200.0, 300.0]
    # Note: CSV round-trip does not preserve float vs int distinction.
    # The CAST is verified at the Arrow level in unit tests; here we
    # verify the values are correct through the full pipeline.


# ---------------------------------------------------------------------------
# 11.5 — Single-table constraint violation → compilation error
# ---------------------------------------------------------------------------


def test_single_table_constraint_violation(rivet_project: Path, capsys) -> None:
    """Source with JOIN in SQL triggers RVT-760 compilation error."""
    project = rivet_project

    (project / "data" / "orders.csv").write_text("id,amount\n1,100\n")
    (project / "data" / "customers.csv").write_text("id,name\n1,Alice\n")

    source_sql = (
        "-- rivet:name: src_bad\n"
        "-- rivet:type: source\n"
        "-- rivet:catalog: local\n"
        "-- rivet:table: orders\n"
        "SELECT o.id, c.name FROM orders o JOIN customers c ON o.id = c.id\n"
    )
    (project / "sources" / "src_bad.sql").write_text(source_sql)

    write_joint(project, "pass_bad", "SELECT * FROM src_bad")
    write_sink(
        project,
        "sink_bad",
        catalog="local",
        table="bad_out",
        upstream=["pass_bad"],
    )

    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code != 0, (
        f"Expected compile to fail for single-table constraint violation.\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    combined = result.stdout + result.stderr
    assert "RVT-760" in combined, (
        f"Expected RVT-760 error code in output.\nstdout: {result.stdout}\nstderr: {result.stderr}"
    )


# ---------------------------------------------------------------------------
# 11.6 — Inline filter + cross-group predicate pushdown → both applied
# ---------------------------------------------------------------------------


def test_inline_filter_plus_cross_group_predicate(rivet_project: Path, capsys) -> None:
    """Source inline filter AND cross-group predicate both apply (AND semantics)."""
    project = rivet_project

    (project / "data" / "orders.csv").write_text(
        "id,amount,status\n1,100,active\n2,30,active\n3,75,inactive\n4,200,active\n5,50,active\n"
    )

    # Source with inline filter: status = 'active'
    (project / "sources" / "src_orders.yaml").write_text(
        "name: src_orders\ntype: source\ncatalog: local\ntable: orders\nfilter: status = 'active'\n"
    )

    # Downstream joint on different engine adds another predicate: amount > 50
    write_joint(
        project,
        "big_orders",
        "SELECT id, amount FROM src_orders WHERE amount > 50",
        engine="duckdb_secondary",
    )

    write_sink(
        project,
        "sink_big",
        catalog="local",
        table="big_out",
        upstream=["big_orders"],
    )

    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    table = read_sink_csv(project, "big_out")

    # Only rows where status='active' AND amount > 50: ids 1 (100), 4 (200)
    # id=3 (75, inactive) excluded by source filter
    # id=2 (30, active) and id=5 (50, active) excluded by amount > 50
    ids = sorted(table.column("id").to_pylist())
    assert ids == [1, 4]
    assert table.num_rows == 2


# ---------------------------------------------------------------------------
# 11.7 — YAML limit → limited output
# ---------------------------------------------------------------------------


def test_yaml_limit(rivet_project: Path, capsys) -> None:
    """Source with YAML limit produces at most N rows."""
    project = rivet_project

    # Write 20 rows
    lines = ["id,value"]
    for i in range(1, 21):
        lines.append(f"{i},{i * 10}")
    (project / "data" / "big.csv").write_text("\n".join(lines) + "\n")

    (project / "sources" / "src_big.yaml").write_text(
        "name: src_big\ntype: source\ncatalog: local\ntable: big\nlimit: 10\n"
    )

    write_joint(
        project,
        "pass_big",
        "SELECT * FROM src_big",
        engine="duckdb_secondary",
    )
    write_sink(
        project,
        "sink_big",
        catalog="local",
        table="big_out",
        upstream=["pass_big"],
    )

    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    table = read_sink_csv(project, "big_out")
    assert table.num_rows <= 10, f"Expected at most 10 rows with limit: 10, got {table.num_rows}"
