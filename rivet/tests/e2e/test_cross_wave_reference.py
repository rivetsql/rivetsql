"""E2E test for cross-wave table references with assertion boundaries.

Tests that joints in Wave 2 can reference materialized tables from Wave 1
when the Wave 1 joint has assertions (assertion_boundary materialization).
"""

from __future__ import annotations

from pathlib import Path

from tests.e2e.conftest import read_sink_csv, run_cli, write_joint, write_sink, write_source


def test_cross_wave_reference_with_assertion_boundary(rivet_project: Path, capsys) -> None:
    """Wave 2 joint references Wave 1 materialized table with assertion boundary."""
    project = rivet_project

    # -- 1. Write CSV data files --
    transactions_csv = "transaction_id,customer_id,amount,transaction_date\n1,101,100,2024-01-01\n2,102,200,2024-01-02\n3,101,150,2024-01-03\n"
    (project / "data" / "transactions.csv").write_text(transactions_csv)

    web_analytics_csv = "session_id,user_id,page_views,session_start\n1,101,5,2024-01-01 10:00:00\n2,102,3,2024-01-02 11:00:00\n3,101,7,2024-01-03 12:00:00\n"
    (project / "data" / "web_analytics.csv").write_text(web_analytics_csv)

    # -- 2. Write sources --
    write_source(project, "transactions", catalog="local", table="transactions")
    write_source(project, "web_analytics", catalog="local", table="web_analytics")

    # -- 3. Write Wave 1 joint with assertion (triggers assertion_boundary materialization) --
    enrich_sql = """
    -- rivet:check: row_count > 0
    SELECT
        transaction_id,
        customer_id,
        amount,
        transaction_date
    FROM transactions
    WHERE amount > 0
    """
    write_joint(project, "enrich_transactions", enrich_sql)

    # -- 4. Write Wave 2 joint that references Wave 1 materialized table --
    unified_sql = """
    WITH daily_sales AS (
        SELECT
            DATE_TRUNC('day', transaction_date) AS date,
            SUM(amount) AS daily_revenue,
            COUNT(DISTINCT customer_id) AS daily_customers
        FROM enrich_transactions
        GROUP BY DATE_TRUNC('day', transaction_date)
    ),
    web_metrics AS (
        SELECT
            DATE_TRUNC('day', session_start) AS date,
            COUNT(session_id) AS daily_sessions,
            SUM(page_views) AS daily_page_views
        FROM web_analytics
        GROUP BY DATE_TRUNC('day', session_start)
    )
    SELECT
        ds.date,
        ds.daily_revenue,
        ds.daily_customers,
        wm.daily_sessions,
        wm.daily_page_views
    FROM daily_sales ds
    LEFT JOIN web_metrics wm ON ds.date = wm.date
    """
    write_joint(project, "unified_business_metrics", unified_sql)

    # -- 5. Write sink --
    write_sink(
        project,
        "business_metrics",
        catalog="local",
        table="business_metrics",
        upstream=["unified_business_metrics"],
    )

    # -- 6. Compile and assert exit code 0 --
    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    # -- 7. Run and assert exit code 0 --
    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    # -- 8. Verify sink output --
    table = read_sink_csv(project, "business_metrics")
    assert len(table) == 3, f"Expected 3 rows, got {len(table)}"

    # Verify columns exist
    assert "daily_revenue" in table.column_names
    assert "daily_customers" in table.column_names
    assert "daily_sessions" in table.column_names
    assert "daily_page_views" in table.column_names

    # Verify data integrity
    revenues = table.column("daily_revenue").to_pylist()
    assert sum(revenues) == 450, f"Expected total revenue 450, got {sum(revenues)}"
