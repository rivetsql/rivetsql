"""E2E tests for sink SQL parsing.

Exercises the full compile pipeline for sink joints with SQL (both YAML-generated
and explicit), verifying that the optimizer propagates predicates, projections,
and limits from sink SQL to upstream source groups.

Requirements: 1.1, 1.3, 2.4, 4.1, 5.1, 5.2, 6.1, 9.1, 9.2, 9.3
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from tests.e2e.conftest import CLIResult, run_cli, write_sink, write_source


def _compile_verbose(project: Path, capsys: Any) -> CLIResult:
    """Run ``rivet compile --verbose --no-color`` and return the result."""
    result = run_cli(project, ["compile", "--verbose", "--no-color"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"
    return result


# ---------------------------------------------------------------------------
# 8.1 — Sink with YAML columns + filter + limit → pushdown applied
# ---------------------------------------------------------------------------


def test_sink_yaml_columns_filter_limit_pushdown(rivet_project: Path, capsys: Any) -> None:
    """Sink with YAML columns, filter, and limit triggers predicate, projection, and limit pushdown."""
    project = rivet_project

    (project / "data" / "products.csv").write_text(
        "id,name,price,category\n"
        "1,Widget,10,A\n"
        "2,Gadget,25,B\n"
        "3,Gizmo,50,A\n"
        "4,Doohickey,5,C\n"
        "5,Thingamajig,100,A\n"
    )

    write_source(project, "src_products", catalog="local", table="products")

    # Sink with YAML columns + filter + limit (bridge generates SQL from these)
    (project / "sinks" / "sink_products.yaml").write_text(
        "name: sink_products\n"
        "type: sink\n"
        "catalog: local\n"
        "table: products_out\n"
        "upstream:\n"
        "  - src_products\n"
        "columns:\n"
        "  - id\n"
        "  - name\n"
        "  - price\n"
        "filter: price > 10\n"
        "limit: 3\n"
    )

    result = _compile_verbose(project, capsys)
    output = result.stdout

    # Pushdown details should show predicates pushed to the source
    assert "Pushed predicates:" in output, f"Expected pushed predicates in output:\n{output}"
    assert "price > 10" in output, f"Expected 'price > 10' predicate in output:\n{output}"

    # Pushed projections should include the selected columns
    assert "Pushed projections:" in output, f"Expected pushed projections in output:\n{output}"
    for col in ("id", "name", "price"):
        assert col in output, f"Expected column '{col}' in pushed projections:\n{output}"

    # The sink's generated SQL should appear in the output
    assert "SELECT id, name, price FROM src_products WHERE price > 10 LIMIT 3" in output, (
        f"Expected sink SQL in output:\n{output}"
    )

    # Summary should show applied optimizations
    assert "applied" in output, f"Expected 'applied' optimizations in summary:\n{output}"


# ---------------------------------------------------------------------------
# 8.2 — Sink with explicit SQL with WHERE + projections → pushdown
# ---------------------------------------------------------------------------


def test_sink_explicit_sql_where_projections_pushdown(rivet_project: Path, capsys: Any) -> None:
    """Sink with explicit SQL containing WHERE and projections triggers predicate and projection pushdown."""
    project = rivet_project

    (project / "data" / "orders.csv").write_text(
        "id,customer,amount,status\n"
        "1,Alice,100,active\n"
        "2,Bob,30,inactive\n"
        "3,Charlie,75,active\n"
        "4,Diana,200,inactive\n"
        "5,Eve,50,active\n"
    )

    write_source(project, "src_orders", catalog="local", table="orders")

    # Sink with explicit SQL
    sink_sql = (
        "-- rivet:name: sink_orders\n"
        "-- rivet:type: sink\n"
        "-- rivet:catalog: local\n"
        "-- rivet:table: orders_out\n"
        "-- rivet:upstream: [src_orders]\n"
        "SELECT id, customer, amount FROM src_orders WHERE amount > 50\n"
    )
    (project / "sinks" / "sink_orders.sql").write_text(sink_sql)

    result = _compile_verbose(project, capsys)
    output = result.stdout

    # Predicate pushdown: amount > 50
    assert "Pushed predicates:" in output, f"Expected pushed predicates in output:\n{output}"
    assert "amount > 50" in output, f"Expected 'amount > 50' predicate in output:\n{output}"

    # Projection pushdown: id, customer, amount
    assert "Pushed projections:" in output, f"Expected pushed projections in output:\n{output}"

    # The sink's SQL should appear in the output
    assert "SELECT id, customer, amount FROM src_orders WHERE amount > 50" in output, (
        f"Expected sink SQL in output:\n{output}"
    )

    # Summary should show applied optimizations
    assert "applied" in output, f"Expected 'applied' optimizations in summary:\n{output}"


# ---------------------------------------------------------------------------
# 8.3 — Sink with no SQL → no pushdown from that sink
# ---------------------------------------------------------------------------


def test_sink_no_sql_no_pushdown(rivet_project: Path, capsys: Any) -> None:
    """Sink with no SQL produces no pushdown from the sink group."""
    project = rivet_project

    (project / "data" / "items.csv").write_text("id,value\n1,10\n2,20\n3,30\n")

    write_source(project, "src_items", catalog="local", table="items")

    # Plain sink with no SQL, no columns, no filter, no limit
    write_sink(
        project,
        "sink_items",
        catalog="local",
        table="items_out",
        upstream=["src_items"],
    )

    result = _compile_verbose(project, capsys)
    output = result.stdout

    # No pushdown details should appear (no SQL on the sink)
    assert "Pushed predicates:" not in output, (
        f"Expected no pushed predicates for sink with no SQL:\n{output}"
    )
    assert "Pushed projections:" not in output, (
        f"Expected no pushed projections for sink with no SQL:\n{output}"
    )
    assert "Pushed limit:" not in output, (
        f"Expected no pushed limit for sink with no SQL:\n{output}"
    )

    # No cross-group optimizations section
    assert "Cross-Group Optimizations" not in output, (
        f"Expected no Cross-Group Optimizations for sink with no SQL:\n{output}"
    )
