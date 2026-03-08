"""Core lifecycle E2E test: init → compile → run → verify output.

Validates Requirements 2.1, 2.2, 2.3, 2.4
"""

from __future__ import annotations

from pathlib import Path

from tests.e2e.conftest import read_sink_csv, run_cli, write_joint, write_sink, write_source


def test_compile_run_verify_output(rivet_project: Path, capsys) -> None:
    """Full lifecycle: scaffold → write pipeline → compile → run → verify sink data."""
    project = rivet_project

    # -- 1. Write CSV data file with known rows (positive and non-positive amounts) --
    csv_content = "id,amount\n1,100\n2,-50\n3,75\n4,0\n5,200\n"
    (project / "data" / "orders.csv").write_text(csv_content)

    # -- 2. Write a source pointing at local catalog / orders.csv --
    write_source(project, "raw_orders", catalog="local", table="orders")

    # -- 3. Write a SQL joint that filters WHERE amount > 0 --
    write_joint(
        project,
        "transform_orders",
        "SELECT * FROM raw_orders WHERE amount > 0",
    )

    # -- 4. Write a sink to local catalog / orders_clean.csv --
    write_sink(
        project,
        "orders_clean",
        catalog="local",
        table="orders_clean",
        upstream=["transform_orders"],
    )

    # -- 5. Compile and assert exit code 0 --
    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    # -- 6. Run and assert exit code 0 --
    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    # -- 7. Read sink output and verify only positive-amount rows --
    table = read_sink_csv(project, "orders_clean")
    ids = table.column("id").to_pylist()
    amounts = table.column("amount").to_pylist()

    # Should contain only rows where amount > 0: (1,100), (3,75), (5,200)
    assert sorted(ids) == [1, 3, 5]
    assert sorted(amounts) == [75, 100, 200]
    assert len(table) == 3
