"""E2E tests for checkpoint joints.

Exercises the full pipeline lifecycle with checkpoint joints: compile → run →
verify output. Checkpoint joints write intermediate data to a catalog table
and re-expose it for downstream consumers.

Requirements: 2.3, 3.3, 6.1, 6.2, 6.3, 7.1, 7.2, 7.3, 9.1, 9.2, 10.1, 10.2
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e.conftest import read_sink_csv, run_cli, write_joint, write_sink, write_source


def _write_checkpoint(
    project: Path,
    name: str,
    *,
    catalog: str,
    table: str,
    upstream: list[str],
    write_strategy: str | None = None,
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
    if write_strategy is not None:
        lines.append(f"write_strategy: {{mode: {write_strategy}}}")
    (project / "joints" / f"{name}.yaml").write_text("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# 12.1 — Full pipeline: source → sql → checkpoint → sql → sink
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_checkpoint_write_and_downstream_sink(rivet_project: Path, capsys) -> None:
    """Pipeline source → sql → checkpoint → sql → sink writes checkpoint table and feeds downstream."""
    project = rivet_project

    (project / "data" / "orders.csv").write_text(
        "id,amount,status\n1,100,active\n2,200,active\n3,50,cancelled\n"
    )

    write_source(project, "src_orders", catalog="local", table="orders")
    write_joint(
        project, "filter_active", "SELECT id, amount FROM src_orders WHERE status = 'active'"
    )
    _write_checkpoint(
        project,
        "cp_active",
        catalog="local",
        table="cp_active",
        upstream=["filter_active"],
    )
    write_joint(project, "double_amount", "SELECT id, amount * 2 AS amount FROM cp_active")
    write_sink(project, "final_out", catalog="local", table="final_out", upstream=["double_amount"])

    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    # Checkpoint table should exist in the catalog
    cp_table = read_sink_csv(project, "cp_active")
    assert cp_table.num_rows == 2
    assert set(cp_table.column_names) == {"id", "amount"}

    # Downstream sink should contain the doubled amounts
    sink_table = read_sink_csv(project, "final_out")
    assert sink_table.num_rows == 2
    amounts = sorted(sink_table.column("amount").to_pylist())
    assert amounts == [200, 400]


# ---------------------------------------------------------------------------
# 12.2 — Fan-out: source → checkpoint → [sql_a → sink_a, sql_b → sink_b]
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_checkpoint_fanout(rivet_project: Path, capsys) -> None:
    """Checkpoint fans out to two downstream branches; both sinks receive the same data."""
    project = rivet_project

    (project / "data" / "products.csv").write_text(
        "id,name,price\n1,Widget,10\n2,Gadget,25\n3,Gizmo,5\n"
    )

    write_source(project, "src_products", catalog="local", table="products")
    _write_checkpoint(
        project,
        "cp_products",
        catalog="local",
        table="cp_products",
        upstream=["src_products"],
    )
    write_joint(project, "branch_a", "SELECT id, name FROM cp_products")
    write_joint(project, "branch_b", "SELECT id, price FROM cp_products")
    write_sink(project, "sink_a", catalog="local", table="sink_a", upstream=["branch_a"])
    write_sink(project, "sink_b", catalog="local", table="sink_b", upstream=["branch_b"])

    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    # Both sinks should have 3 rows (same as checkpoint)
    table_a = read_sink_csv(project, "sink_a")
    table_b = read_sink_csv(project, "sink_b")
    assert table_a.num_rows == 3
    assert table_b.num_rows == 3

    # Verify branch_a has id + name columns
    assert set(table_a.column_names) == {"id", "name"}
    names = sorted(table_a.column("name").to_pylist())
    assert names == ["Gadget", "Gizmo", "Widget"]

    # Verify branch_b has id + price columns
    assert set(table_b.column_names) == {"id", "price"}
    prices = sorted(table_b.column("price").to_pylist())
    assert prices == [5, 10, 25]


# ---------------------------------------------------------------------------
# 12.3 — Terminal checkpoint (no downstream): compile warns, run succeeds
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_checkpoint_terminal(rivet_project: Path, capsys) -> None:
    """Checkpoint with no downstream compiles with a warning and runs successfully."""
    project = rivet_project

    (project / "data" / "items.csv").write_text("id,value\n1,100\n2,200\n")

    write_source(project, "src_items", catalog="local", table="items")
    _write_checkpoint(
        project,
        "cp_items",
        catalog="local",
        table="cp_items",
        upstream=["src_items"],
    )

    # Compile should succeed with a warning about no downstream consumers
    result = run_cli(project, ["compile", "--verbose", "--no-color"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"
    combined = result.stdout + result.stderr
    assert "no downstream" in combined.lower(), (
        f"Expected 'no downstream' warning in output:\n{combined}"
    )

    # Run should succeed and write the checkpoint table
    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    cp_table = read_sink_csv(project, "cp_items")
    assert cp_table.num_rows == 2
    assert set(cp_table.column_names) == {"id", "value"}


# ---------------------------------------------------------------------------
# 12.4 — Checkpoint with append write strategy: rows accumulate
# ---------------------------------------------------------------------------


@pytest.mark.e2e
def test_checkpoint_append_accumulates(rivet_project: Path, capsys) -> None:
    """Running a pipeline twice with checkpoint write_strategy=append doubles the rows."""
    project = rivet_project

    (project / "data" / "events.csv").write_text("id,event\n1,click\n2,view\n")

    write_source(project, "src_events", catalog="local", table="events")
    _write_checkpoint(
        project,
        "cp_events",
        catalog="local",
        table="cp_events",
        upstream=["src_events"],
        write_strategy="append",
    )
    write_joint(project, "pass_through", "SELECT * FROM cp_events")
    write_sink(
        project, "events_out", catalog="local", table="events_out", upstream=["pass_through"]
    )

    # First run
    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"
    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"first run failed:\n{result.stderr}"

    cp_table = read_sink_csv(project, "cp_events")
    assert cp_table.num_rows == 2

    # Second run — append should double the checkpoint rows
    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"second run failed:\n{result.stderr}"

    cp_table = read_sink_csv(project, "cp_events")
    assert cp_table.num_rows == 4

    ids = sorted(cp_table.column("id").to_pylist())
    assert ids == [1, 1, 2, 2]
