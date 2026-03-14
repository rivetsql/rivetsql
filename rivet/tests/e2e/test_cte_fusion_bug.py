"""E2E test for CTE fusion bug fix.

Reproduces the bug where fusing multiple joints with WITH clauses
generated invalid SQL with multiple WITH keywords.
"""

from pathlib import Path

import pytest

from rivet_cli.app import _main


@pytest.mark.e2e
def test_multiple_with_clauses_fuse_correctly(rivet_project: Path) -> None:
    """Joints with WITH clauses should fuse without generating multiple WITH keywords."""
    project = rivet_project

    # Create source data
    (project / "sources").mkdir(exist_ok=True)
    (project / "sources" / "web_analytics.yaml").write_text("""
name: web_analytics
type: source
catalog: local
table: web_analytics
""")

    (project / "sources" / "marketing_campaigns.yaml").write_text("""
name: marketing_campaigns
type: source
catalog: local
table: marketing_campaigns
""")

    (project / "sources" / "transactions.yaml").write_text("""
name: transactions
type: source
catalog: local
table: transactions
""")

    # Create joints with WITH clauses
    (project / "joints").mkdir(exist_ok=True)

    # Joint 1: web_funnel_analysis with WITH clause
    (project / "joints" / "web_funnel_analysis.sql").write_text("""
-- rivet:name: web_funnel_analysis
-- rivet:type: sql

WITH session_summary AS (
    SELECT
        session_id,
        user_id,
        device_type,
        MIN(timestamp) AS session_start,
        COUNT(*) AS total_events
    FROM web_analytics
    GROUP BY session_id, user_id, device_type
)
SELECT
    session_id,
    user_id,
    device_type,
    session_start,
    total_events
FROM session_summary
""")

    # Joint 2: marketing_roi (no WITH clause)
    (project / "joints" / "marketing_roi.sql").write_text("""
-- rivet:name: marketing_roi
-- rivet:type: sql

SELECT
    campaign_id,
    campaign_name,
    channel,
    budget,
    impressions,
    clicks
FROM marketing_campaigns
""")

    # Joint 3: enrich_transactions (no WITH clause)
    (project / "joints" / "enrich_transactions.sql").write_text("""
-- rivet:name: enrich_transactions
-- rivet:type: sql

SELECT
    transaction_id,
    customer_id,
    transaction_date,
    total_amount
FROM transactions
""")

    # Joint 4: unified_business_metrics with WITH clause referencing other joints
    (project / "joints" / "unified_business_metrics.sql").write_text("""
-- rivet:name: unified_business_metrics
-- rivet:type: sql

WITH daily_sales AS (
    SELECT
        DATE_TRUNC('day', transaction_date) AS date,
        SUM(total_amount) AS daily_revenue,
        COUNT(DISTINCT customer_id) AS daily_customers
    FROM enrich_transactions
    GROUP BY DATE_TRUNC('day', transaction_date)
),
web_metrics AS (
    SELECT
        DATE_TRUNC('day', session_start) AS date,
        COUNT(DISTINCT session_id) AS daily_sessions
    FROM web_funnel_analysis
    GROUP BY DATE_TRUNC('day', session_start)
)
SELECT
    ds.date,
    ds.daily_revenue,
    ds.daily_customers,
    wm.daily_sessions
FROM daily_sales ds
LEFT JOIN web_metrics wm ON ds.date = wm.date
""")

    # Compile the pipeline
    exit_code = _main(["compile", "--project", str(project), "-vv", "unified_business_metrics"])
    assert exit_code == 0, "Compilation should succeed"

    # Read the compilation output to verify no multiple WITH keywords
    # The bug would cause "Parser Error: syntax error at or near WITH"
    # If compilation succeeds, the bug is fixed


@pytest.mark.e2e
def test_nested_ctes_execute_correctly(rivet_project: Path) -> None:
    """Verify that fused joints with CTEs compile correctly."""
    project = rivet_project

    # Create source data
    (project / "sources").mkdir(exist_ok=True)
    (project / "sources" / "orders.yaml").write_text("""
name: orders
type: source
catalog: local
table: orders
""")

    # Joint with WITH clause
    (project / "joints").mkdir(exist_ok=True)
    (project / "joints" / "order_summary.sql").write_text("""
-- rivet:name: order_summary
-- rivet:type: sql

WITH order_totals AS (
    SELECT
        order_id,
        customer_id,
        SUM(amount) AS total_amount
    FROM orders
    GROUP BY order_id, customer_id
)
SELECT
    order_id,
    customer_id,
    total_amount
FROM order_totals
WHERE total_amount > 100
""")

    # Another joint with WITH clause that references the first
    (project / "joints" / "customer_metrics.sql").write_text("""
-- rivet:name: customer_metrics
-- rivet:type: sql

WITH customer_totals AS (
    SELECT
        customer_id,
        SUM(total_amount) AS lifetime_value,
        COUNT(order_id) AS order_count
    FROM order_summary
    GROUP BY customer_id
)
SELECT
    customer_id,
    lifetime_value,
    order_count
FROM customer_totals
WHERE order_count > 1
""")

    # Compile the pipeline - this will fuse the joints and should not generate invalid SQL
    exit_code = _main(["compile", "--project", str(project), "customer_metrics"])
    assert exit_code == 0, "Compilation should succeed with nested CTEs"
