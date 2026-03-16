"""E2E tests for checkpoint SQL parity with sink joints.

Exercises the full pipeline lifecycle for checkpoint joints with SQL:
YAML columns/filter/limit generation, .sql file declarations, backward
compatibility for no-SQL checkpoints, and cross-group predicate pushdown.

Requirements: 1.1–1.8, 2.1–2.4, 3.1–3.5, 4.1–4.5, 5.1–5.4
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from tests.e2e.conftest import CLIResult, read_sink_csv, run_cli, write_sink, write_source


def _run(project: Path, argv: list[str], capsys: Any) -> CLIResult:
    result = run_cli(project, argv, capsys)
    return result


# ---------------------------------------------------------------------------
# Helper: write a checkpoint YAML declaration
# ---------------------------------------------------------------------------


def _write_checkpoint_yaml(
    project: Path,
    name: str,
    *,
    catalog: str,
    table: str,
    upstream: list[str],
    columns: list[str] | None = None,
    filter_expr: str | None = None,
    limit: int | None = None,
    sql: str | None = None,
) -> None:
    lines = [
        f"name: {name}",
        "type: checkpoint",
        f"catalog: {catalog}",
        f"table: {table}",
        "upstream:",
    ]
    for up in upstream:
        lines.append(f"  - {up}")
    if columns is not None:
        lines.append("columns:")
        for col in columns:
            lines.append(f"  - {col}")
    if filter_expr is not None:
        lines.append(f"filter: {filter_expr}")
    if limit is not None:
        lines.append(f"limit: {limit}")
    if sql is not None:
        lines.append(f"sql: |\n  {sql}")
    (project / "sinks" / f"{name}.yaml").write_text("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# 1. Checkpoint with YAML columns + filter + limit
# ---------------------------------------------------------------------------


def test_checkpoint_with_yaml_columns_filter_limit(rivet_project: Path, capsys: Any) -> None:
    """Checkpoint with YAML columns/filter/limit compiles and runs; downstream sees filtered rows."""
    project = rivet_project

    (project / "data" / "products.csv").write_text(
        "id,name,price\n1,Widget,10\n2,Gadget,25\n3,Gizmo,50\n4,Doohickey,5\n"
    )

    write_source(project, "src_products", catalog="local", table="products")

    # Checkpoint with YAML transforms — bridge generates SQL from these
    _write_checkpoint_yaml(
        project,
        "cp_products",
        catalog="local",
        table="cp_products_out",
        upstream=["src_products"],
        columns=["id", "name", "price"],
        filter_expr="price > 10",
        limit=2,
    )

    # Downstream sink reads from the checkpoint
    write_sink(project, "sink_final", catalog="local", table="final_out", upstream=["cp_products"])

    result = _run(project, ["run", "--no-color"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    # Downstream sink should only see rows where price > 10, capped at 2
    out = read_sink_csv(project, "final_out")
    assert out.num_rows <= 2
    prices = out.column("price").to_pylist()
    assert all(p > 10 for p in prices)


# ---------------------------------------------------------------------------
# 2. Checkpoint declared via .sql file
# ---------------------------------------------------------------------------


def test_checkpoint_with_sql_file(rivet_project: Path, capsys: Any) -> None:
    """Checkpoint declared via .sql file with type: checkpoint annotation compiles and runs."""
    project = rivet_project

    (project / "data" / "orders.csv").write_text(
        "id,customer,amount\n1,Alice,100\n2,Bob,30\n3,Charlie,75\n"
    )

    write_source(project, "src_orders", catalog="local", table="orders")

    # Checkpoint as .sql file
    (project / "sinks" / "cp_orders.sql").write_text(
        "-- rivet:name: cp_orders\n"
        "-- rivet:type: checkpoint\n"
        "-- rivet:catalog: local\n"
        "-- rivet:table: cp_orders_out\n"
        "-- rivet:upstream: [src_orders]\n"
        "SELECT id, customer, amount FROM src_orders WHERE amount >= 75\n"
    )

    write_sink(project, "sink_orders", catalog="local", table="orders_out", upstream=["cp_orders"])

    result = _run(project, ["run", "--no-color"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    out = read_sink_csv(project, "orders_out")
    assert out.num_rows >= 1
    amounts = out.column("amount").to_pylist()
    assert all(a >= 75 for a in amounts)


# ---------------------------------------------------------------------------
# 3. Backward compatibility: no-SQL checkpoint still works
# ---------------------------------------------------------------------------


def test_checkpoint_no_sql_backward_compat(rivet_project: Path, capsys: Any) -> None:
    """Existing no-SQL checkpoint passes upstream data through unchanged."""
    project = rivet_project

    (project / "data" / "items.csv").write_text("id,value\n1,10\n2,20\n3,30\n")

    write_source(project, "src_items", catalog="local", table="items")

    # Plain checkpoint with no SQL, no columns, no filter
    _write_checkpoint_yaml(
        project,
        "cp_items",
        catalog="local",
        table="cp_items_out",
        upstream=["src_items"],
    )

    write_sink(project, "sink_items", catalog="local", table="items_out", upstream=["cp_items"])

    result = _run(project, ["run", "--no-color"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    out = read_sink_csv(project, "items_out")
    # All 3 rows should pass through unchanged
    assert out.num_rows == 3
    assert set(out.column("id").to_pylist()) == {1, 2, 3}


# ---------------------------------------------------------------------------
# 4. Checkpoint SQL triggers cross-group predicate pushdown
# ---------------------------------------------------------------------------


def test_checkpoint_sql_pushdown_applied(rivet_project: Path, capsys: Any) -> None:
    """Checkpoint with WHERE predicate triggers cross-group predicate pushdown."""
    project = rivet_project

    (project / "data" / "events.csv").write_text("id,type,score\n1,click,5\n2,view,2\n3,click,8\n")

    write_source(project, "src_events", catalog="local", table="events")

    (project / "sinks" / "cp_events.sql").write_text(
        "-- rivet:name: cp_events\n"
        "-- rivet:type: checkpoint\n"
        "-- rivet:catalog: local\n"
        "-- rivet:table: cp_events_out\n"
        "-- rivet:upstream: [src_events]\n"
        "SELECT id, type, score FROM src_events WHERE score > 3\n"
    )

    write_sink(
        project,
        "sink_events",
        catalog="local",
        table="events_out",
        upstream=["cp_events"],
    )

    result = _run(project, ["compile", "--verbose", "--no-color"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    # The compile output should show the predicate was pushed
    output = result.stdout
    assert "score > 3" in output or "applied" in output.lower(), (
        f"Expected pushdown evidence in compile output:\n{output}"
    )
