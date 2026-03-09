"""Write strategies E2E tests: append and replace filesystem sink behavior.

Validates Requirements 4.3, 4.6, 9.1
"""

from __future__ import annotations

from pathlib import Path

from tests.e2e.conftest import read_sink_csv, run_cli, write_joint, write_source


def _write_sink_with_strategy(
    project: Path,
    name: str,
    *,
    catalog: str,
    table: str,
    upstream: list[str],
    mode: str,
) -> None:
    """Write a sink SQL file with an explicit write_strategy annotation."""
    upstream_str = ", ".join(upstream)
    content = (
        f"-- rivet:name: {name}\n"
        f"-- rivet:type: sink\n"
        f"-- rivet:catalog: {catalog}\n"
        f"-- rivet:table: {table}\n"
        f"-- rivet:upstream: [{upstream_str}]\n"
        f"-- rivet:write_strategy: {{mode: {mode}}}\n"
    )
    (project / "sinks" / f"{name}.sql").write_text(content)


def test_append_strategy_accumulates_rows(rivet_project: Path, capsys) -> None:
    """Running a pipeline twice with append strategy doubles the output rows."""
    project = rivet_project

    (project / "data" / "orders.csv").write_text(
        "id,amount\n1,100\n2,200\n"
    )

    write_source(project, "src_orders", catalog="local", table="orders")
    write_joint(project, "pass_through", "SELECT * FROM src_orders")
    _write_sink_with_strategy(
        project,
        "orders_out",
        catalog="local",
        table="orders_out",
        upstream=["pass_through"],
        mode="append",
    )

    # First run
    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"
    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    table = read_sink_csv(project, "orders_out")
    assert table.num_rows == 2

    # Second run — append should double the rows
    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"second run failed:\n{result.stderr}"

    table = read_sink_csv(project, "orders_out")
    assert table.num_rows == 4
    ids = sorted(table.column("id").to_pylist())
    assert ids == [1, 1, 2, 2]


def test_replace_strategy_overwrites(rivet_project: Path, capsys) -> None:
    """Running a pipeline twice with replace strategy keeps only the latest rows."""
    project = rivet_project

    (project / "data" / "items.csv").write_text(
        "id,name\n1,Widget\n2,Gadget\n"
    )

    write_source(project, "src_items", catalog="local", table="items")
    write_joint(project, "pass_items", "SELECT * FROM src_items")
    _write_sink_with_strategy(
        project,
        "items_out",
        catalog="local",
        table="items_out",
        upstream=["pass_items"],
        mode="replace",
    )

    # First run
    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"
    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    table = read_sink_csv(project, "items_out")
    assert table.num_rows == 2

    # Second run — replace should keep exactly 2 rows, not 4
    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"second run failed:\n{result.stderr}"

    table = read_sink_csv(project, "items_out")
    assert table.num_rows == 2
    names = sorted(table.column("name").to_pylist())
    assert names == ["Gadget", "Widget"]


def test_replace_then_append_mixed(rivet_project: Path, capsys) -> None:
    """Two sinks from the same source: one append, one replace."""
    project = rivet_project

    (project / "data" / "sales.csv").write_text(
        "id,total\n1,50\n2,75\n"
    )

    write_source(project, "src_sales", catalog="local", table="sales")
    write_joint(project, "transform_sales", "SELECT * FROM src_sales")

    _write_sink_with_strategy(
        project,
        "sales_append",
        catalog="local",
        table="sales_append",
        upstream=["transform_sales"],
        mode="append",
    )
    _write_sink_with_strategy(
        project,
        "sales_replace",
        catalog="local",
        table="sales_replace",
        upstream=["transform_sales"],
        mode="replace",
    )

    # First run
    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"
    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    # Second run
    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"second run failed:\n{result.stderr}"

    append_table = read_sink_csv(project, "sales_append")
    replace_table = read_sink_csv(project, "sales_replace")

    # Append: 2 + 2 = 4 rows
    assert append_table.num_rows == 4
    # Replace: still 2 rows
    assert replace_table.num_rows == 2
