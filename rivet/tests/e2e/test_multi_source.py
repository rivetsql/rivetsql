"""Multi-source pipeline E2E tests: 3+ sources feeding into a join chain → sink.

Validates Requirements 4.1, 4.6, 9.1
"""

from __future__ import annotations

from pathlib import Path

from tests.e2e.conftest import read_sink_csv, run_cli, write_joint, write_sink, write_source


def test_three_source_join_chain(rivet_project: Path, capsys) -> None:
    """Three CSV sources join through a chain into a single sink."""
    project = rivet_project

    # -- Write CSV data files --
    (project / "data" / "customers.csv").write_text(
        "id,name\n1,Alice\n2,Bob\n3,Charlie\n"
    )
    (project / "data" / "orders.csv").write_text(
        "id,customer_id,product_id,quantity\n"
        "101,1,10,2\n"
        "102,2,11,1\n"
        "103,1,12,3\n"
        "104,3,10,1\n"
    )
    (project / "data" / "products.csv").write_text(
        "id,product_name,price\n10,Widget,25\n11,Gadget,50\n12,Gizmo,15\n"
    )

    # -- Sources --
    write_source(project, "src_customers", catalog="local", table="customers")
    write_source(project, "src_orders", catalog="local", table="orders")
    write_source(project, "src_products", catalog="local", table="products")

    # -- Join chain: orders → customers, then → products --
    write_joint(
        project,
        "join_customers",
        (
            "SELECT o.id AS order_id, c.name AS customer_name, "
            "o.product_id, o.quantity\n"
            "FROM src_orders o\n"
            "JOIN src_customers c ON o.customer_id = c.id"
        ),
    )
    write_joint(
        project,
        "join_products",
        (
            "SELECT jc.order_id, jc.customer_name, "
            "p.product_name, jc.quantity, p.price, "
            "jc.quantity * p.price AS total\n"
            "FROM join_customers jc\n"
            "JOIN src_products p ON jc.product_id = p.id"
        ),
    )

    # -- Sink --
    write_sink(
        project,
        "order_details",
        catalog="local",
        table="order_details",
        upstream=["join_products"],
    )

    # -- Compile + Run --
    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    # -- Verify output --
    table = read_sink_csv(project, "order_details")
    assert table.num_rows == 4

    rows = table.to_pydict()
    order_ids = sorted(rows["order_id"])
    assert order_ids == [101, 102, 103, 104]

    # Verify join correctness: order 101 → Alice, Widget, qty 2, total 50
    for i, oid in enumerate(rows["order_id"]):
        if oid == 101:
            assert rows["customer_name"][i] == "Alice"
            assert rows["product_name"][i] == "Widget"
            assert rows["quantity"][i] == 2
            assert rows["total"][i] == 50
        elif oid == 102:
            assert rows["customer_name"][i] == "Bob"
            assert rows["product_name"][i] == "Gadget"
            assert rows["quantity"][i] == 1
            assert rows["total"][i] == 50


def test_four_sources_with_filter(rivet_project: Path, capsys) -> None:
    """Four sources join and filter into a sink, verifying complex DAG execution."""
    project = rivet_project

    # -- CSV data --
    (project / "data" / "employees.csv").write_text(
        "id,name,dept_id\n1,Alice,10\n2,Bob,20\n3,Charlie,10\n4,Diana,30\n"
    )
    (project / "data" / "departments.csv").write_text(
        "id,dept_name\n10,Engineering\n20,Sales\n30,Marketing\n"
    )
    (project / "data" / "salaries.csv").write_text(
        "employee_id,amount\n1,120000\n2,90000\n3,110000\n4,95000\n"
    )
    (project / "data" / "locations.csv").write_text(
        "dept_id,city\n10,Seattle\n20,New York\n30,Austin\n"
    )

    # -- Sources --
    write_source(project, "src_employees", catalog="local", table="employees")
    write_source(project, "src_departments", catalog="local", table="departments")
    write_source(project, "src_salaries", catalog="local", table="salaries")
    write_source(project, "src_locations", catalog="local", table="locations")

    # -- Join chain --
    write_joint(
        project,
        "join_emp_dept",
        (
            "SELECT e.id, e.name, d.dept_name, e.dept_id\n"
            "FROM src_employees e\n"
            "JOIN src_departments d ON e.dept_id = d.id"
        ),
    )
    write_joint(
        project,
        "join_salary",
        (
            "SELECT jed.id, jed.name, jed.dept_name, jed.dept_id, s.amount\n"
            "FROM join_emp_dept jed\n"
            "JOIN src_salaries s ON jed.id = s.employee_id"
        ),
    )
    write_joint(
        project,
        "join_location",
        (
            "SELECT js.id, js.name, js.dept_name, js.amount, l.city\n"
            "FROM join_salary js\n"
            "JOIN src_locations l ON js.dept_id = l.dept_id\n"
            "WHERE js.amount > 100000"
        ),
    )

    # -- Sink --
    write_sink(
        project,
        "high_earners",
        catalog="local",
        table="high_earners",
        upstream=["join_location"],
    )

    # -- Compile + Run --
    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    # -- Verify: only employees with salary > 100000 --
    table = read_sink_csv(project, "high_earners")
    assert table.num_rows == 2

    names = sorted(table.column("name").to_pylist())
    assert names == ["Alice", "Charlie"]

    cities = sorted(table.column("city").to_pylist())
    assert cities == ["Seattle", "Seattle"]
