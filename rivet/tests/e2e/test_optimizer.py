"""Rich Pipeline optimizer E2E test.

Exercises all optimizer features simultaneously through a single DAG:
- Multi-upstream (join_orders joins src_customers + src_orders)
- Group fusion (join_orders → filter_big → select_cols fuse on duckdb_primary)
- Predicate pushdown (WHERE amount > 50)
- Projection pushdown (SELECT id, customer_name, amount)
- Limit pushdown (LIMIT 10)
- Cross-engine materialization (cross_engine on duckdb_secondary)
- Multi-downstream fan-out (select_cols → sink_main, limit_rows, cross_engine)

Validates Requirements 4.1, 4.2, 4.3
"""

from __future__ import annotations

from pathlib import Path

from tests.e2e.conftest import (
    read_sink_csv,
    run_cli,
    write_joint,
    write_sink,
    write_source,
)


def test_optimizer_rich_pipeline(rivet_project: Path, query_recorder, capsys) -> None:
    """Set up the Rich Pipeline DAG with all optimizer features."""
    project = rivet_project

    # -- 1. Write test data CSV files --
    customers_csv = "id,name\n1,Alice\n2,Bob\n3,Charlie\n"
    (project / "data" / "customers.csv").write_text(customers_csv)

    orders_csv = (
        "id,customer_id,amount\n"
        "101,1,100\n"
        "102,2,30\n"
        "103,1,75\n"
        "104,3,200\n"
        "105,2,10\n"
    )
    (project / "data" / "orders.csv").write_text(orders_csv)

    # -- 2. Write two sources on duckdb_primary --
    write_source(project, "src_customers", catalog="local", table="customers")
    write_source(project, "src_orders", catalog="local", table="orders")

    # -- 3. join_orders: JOIN src_customers + src_orders (multi-upstream) --
    write_joint(
        project,
        "join_orders",
        (
            "SELECT o.id, c.name AS customer_name, o.amount\n"
            "FROM src_orders o\n"
            "JOIN src_customers c ON o.customer_id = c.id"
        ),
        engine="duckdb_primary",
    )

    # -- 4. filter_big: WHERE amount > 50 (predicate pushdown, group fusion) --
    write_joint(
        project,
        "filter_big",
        "SELECT * FROM join_orders WHERE amount > 50",
        engine="duckdb_primary",
    )

    # -- 5. select_cols: projection pushdown, group fusion --
    write_joint(
        project,
        "select_cols",
        "SELECT id, customer_name, amount FROM filter_big",
        engine="duckdb_primary",
    )

    # -- 6. limit_rows: LIMIT 10 (limit pushdown) --
    write_joint(
        project,
        "limit_rows",
        "SELECT * FROM select_cols LIMIT 10",
        engine="duckdb_primary",
    )

    # -- 7. cross_engine: on duckdb_secondary (cross-engine materialization) --
    write_joint(
        project,
        "cross_engine",
        "SELECT * FROM select_cols",
        engine="duckdb_secondary",
    )

    # -- 8. Three sinks (multi-downstream fan-out from select_cols) --
    write_sink(
        project,
        "sink_main",
        catalog="local",
        table="orders_main",
        upstream=["select_cols"],
    )

    write_sink(
        project,
        "sink_limited",
        catalog="local",
        table="orders_limited",
        upstream=["limit_rows"],
    )

    write_sink(
        project,
        "sink_cross",
        catalog="local",
        table="orders_cross",
        upstream=["cross_engine"],
    )

    # -- Compile --
    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed: {result.stderr}"

    # -- Run (with query recording for optimizer assertions) --
    with query_recorder:
        result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed: {result.stderr}"

    # -- Data output assertions (task 6.3) --
    expected_ids = {101, 103, 104}
    expected_amounts = {100, 75, 200}
    expected_names = ["Alice", "Alice", "Charlie"]

    # sink_main: 3 rows with amount > 50, columns id/customer_name/amount
    main_tbl = read_sink_csv(project, "orders_main")
    assert main_tbl.num_rows == 3
    assert set(main_tbl.column_names) == {"id", "customer_name", "amount"}
    assert set(main_tbl.column("id").to_pylist()) == expected_ids
    assert set(main_tbl.column("amount").to_pylist()) == expected_amounts
    assert sorted(main_tbl.column("customer_name").to_pylist()) == sorted(expected_names)

    # sink_limited: same 3 rows (all fit within LIMIT 10)
    limited_tbl = read_sink_csv(project, "orders_limited")
    assert limited_tbl.num_rows == 3
    assert set(limited_tbl.column_names) == {"id", "customer_name", "amount"}
    assert set(limited_tbl.column("id").to_pylist()) == expected_ids
    assert set(limited_tbl.column("amount").to_pylist()) == expected_amounts
    assert sorted(limited_tbl.column("customer_name").to_pylist()) == sorted(expected_names)

    # sink_cross: same data as sink_main (executed on duckdb_secondary)
    cross_tbl = read_sink_csv(project, "orders_cross")
    assert cross_tbl.num_rows == 3
    assert set(cross_tbl.column_names) == {"id", "customer_name", "amount"}
    assert set(cross_tbl.column("id").to_pylist()) == expected_ids
    assert set(cross_tbl.column("amount").to_pylist()) == expected_amounts
    assert sorted(cross_tbl.column("customer_name").to_pylist()) == sorted(expected_names)

    # -- Query assertions (task 6.4) --

    # 1. Group fusion: duckdb_primary should have a fused CTE query
    primary_queries = query_recorder.queries_for_engine("duckdb_primary")
    assert len(primary_queries) >= 1, "Expected at least 1 query on duckdb_primary"

    # Find the fused CTE query (contains WITH clause)
    fused_queries = [q for q in primary_queries if "WITH" in q.sql.upper()]
    assert len(fused_queries) >= 1, (
        "Expected a fused CTE query (WITH clause) on duckdb_primary, "
        f"got queries: {[q.sql[:120] for q in primary_queries]}"
    )
    fused_query = fused_queries[0]

    # The fused query should reference the CTE names from the fused joints
    fused_sql_lower = fused_query.sql.lower()
    assert "join_orders" in fused_sql_lower, (
        f"Fused CTE query should contain 'join_orders' CTE: {fused_query.sql[:200]}"
    )
    assert "filter_big" in fused_sql_lower, (
        f"Fused CTE query should contain 'filter_big' CTE: {fused_query.sql[:200]}"
    )

    # 2. Predicate pushdown: the fused query contains the WHERE predicate
    assert "amount > 50" in fused_query.sql or "amount>50" in fused_query.sql.replace(" ", ""), (
        f"Fused query should contain predicate 'amount > 50': {fused_query.sql[:300]}"
    )

    # 3. Projection pushdown: the fused query's final SELECT is NOT SELECT *
    # Split on the last AS to isolate the outermost SELECT
    final_select_portion = fused_query.sql.split(")")[-1] if ")" in fused_query.sql else fused_query.sql
    assert "select *" not in final_select_portion.lower(), (
        f"Projection pushdown: final SELECT should not be SELECT *, "
        f"got: {final_select_portion[:200]}"
    )

    # 4. Limit pushdown: some query across all engines contains LIMIT
    all_sql_upper = " ".join(q.sql.upper() for q in query_recorder.queries)
    assert "LIMIT" in all_sql_upper, (
        "Expected at least one query to contain LIMIT (limit pushdown)"
    )

    # 5. Cross-engine materialization: duckdb_secondary received exactly 1 query
    secondary_query = query_recorder.assert_single_query("duckdb_secondary")
    assert secondary_query.sql, "Cross-engine query should have non-empty SQL"

    # 6. Total query count is bounded (no unexpected extra queries)
    total = query_recorder.query_count()
    assert total <= 5, (
        f"Total query count should be bounded (<= 5), got {total}: "
        f"{[(q.engine_name, q.sql[:80]) for q in query_recorder.queries]}"
    )
