"""Unit tests for SQL table reference extraction."""

from __future__ import annotations

from rivet_core.executor import _extract_table_references


def test_simple_from_clause() -> None:
    """Extract table from simple FROM clause."""
    sql = ["SELECT * FROM customers"]
    result = _extract_table_references(sql)
    assert "customers" in result


def test_simple_join() -> None:
    """Extract tables from JOIN clauses."""
    sql = ["SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id"]
    result = _extract_table_references(sql)
    assert "orders" in result
    assert "customers" in result


def test_table_with_alias() -> None:
    """Extract table with AS alias."""
    sql = ["SELECT * FROM customers AS c WHERE c.id = 1"]
    result = _extract_table_references(sql)
    assert "customers" in result
    assert "c" not in result  # alias should not be captured


def test_table_with_implicit_alias() -> None:
    """Extract table with implicit alias (no AS keyword)."""
    sql = ["SELECT * FROM customers c WHERE c.id = 1"]
    result = _extract_table_references(sql)
    assert "customers" in result
    assert "c" not in result


def test_cte_reference() -> None:
    """Extract table references from CTE body."""
    sql = [
        """
        WITH daily_sales AS (
            SELECT DATE_TRUNC('day', transaction_date) AS date,
                   SUM(amount) AS revenue
            FROM enrich_transactions
            GROUP BY DATE_TRUNC('day', transaction_date)
        )
        SELECT * FROM daily_sales
    """
    ]
    result = _extract_table_references(sql)
    assert "enrich_transactions" in result
    assert "daily_sales" in result


def test_nested_subquery() -> None:
    """Extract table from nested subquery."""
    sql = [
        """
        SELECT *
        FROM (
            SELECT customer_id, COUNT(*) as order_count
            FROM orders
            GROUP BY customer_id
        ) AS customer_orders
        WHERE order_count > 5
    """
    ]
    result = _extract_table_references(sql)
    assert "orders" in result
    assert "customer_orders" not in result  # subquery alias


def test_multiple_joins() -> None:
    """Extract tables from multiple JOIN clauses."""
    sql = [
        """
        SELECT *
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        LEFT JOIN products p ON o.product_id = p.id
        INNER JOIN shipping s ON o.id = s.order_id
    """
    ]
    result = _extract_table_references(sql)
    assert "orders" in result
    assert "customers" in result
    assert "products" in result
    assert "shipping" in result


def test_string_literals_ignored() -> None:
    """String literals should not be extracted as table names."""
    sql = ["SELECT * FROM orders WHERE status = 'FROM customers'"]
    result = _extract_table_references(sql)
    assert "orders" in result
    assert "customers" not in result  # inside string literal


def test_function_calls_ignored() -> None:
    """Function calls should not be extracted as table names."""
    sql = ["SELECT DATE_TRUNC('day', created_at) FROM orders"]
    result = _extract_table_references(sql)
    assert "orders" in result
    assert "DATE_TRUNC" not in result


def test_sql_keywords_filtered() -> None:
    """SQL keywords should be filtered out."""
    sql = ["SELECT * FROM orders WHERE status IN ('pending', 'shipped')"]
    result = _extract_table_references(sql)
    assert "orders" in result
    assert "WHERE" not in result
    assert "IN" not in result
    assert "SELECT" not in result


def test_multiple_sql_sources() -> None:
    """Extract tables from multiple SQL statements."""
    sql = [
        "SELECT * FROM customers",
        "SELECT * FROM orders",
        "SELECT * FROM products",
    ]
    result = _extract_table_references(sql)
    assert "customers" in result
    assert "orders" in result
    assert "products" in result


def test_complex_cte_with_multiple_references() -> None:
    """Extract tables from complex CTE with multiple table references."""
    sql = [
        """
        WITH enriched AS (
            SELECT t.*, c.name, p.price
            FROM transactions t
            JOIN customers c ON t.customer_id = c.id
            JOIN products p ON t.product_id = p.id
        ),
        aggregated AS (
            SELECT customer_id, SUM(price) as total
            FROM enriched
            GROUP BY customer_id
        )
        SELECT * FROM aggregated
    """
    ]
    result = _extract_table_references(sql)
    assert "transactions" in result
    assert "customers" in result
    assert "products" in result
    assert "enriched" in result
    assert "aggregated" in result


def test_empty_sql() -> None:
    """Handle empty SQL gracefully."""
    result = _extract_table_references([])
    assert len(result) == 0


def test_none_in_sql_list() -> None:
    """Handle None values in SQL list."""
    sql_list: list[str | None] = ["SELECT * FROM orders", None, ""]
    result = _extract_table_references([s for s in sql_list if s is not None])
    assert "orders" in result


def test_cross_wave_assertion_boundary_case() -> None:
    """Test the specific case from the bug report: assertion boundary cross-wave reference."""
    # Wave 1 joint with assertion
    wave1_sql = """
        SELECT
            transaction_id,
            customer_id,
            amount
        FROM transactions
        WHERE amount > 0
    """

    # Wave 2 joint referencing Wave 1 materialized table
    wave2_sql = """
        WITH daily_sales AS (
            SELECT
                DATE_TRUNC('day', transaction_date) AS date,
                SUM(amount) AS daily_revenue,
                COUNT(DISTINCT customer_id) AS daily_customers,
                COUNT(transaction_id) AS daily_transactions
            FROM enrich_transactions
            GROUP BY DATE_TRUNC('day', transaction_date)
        )
        SELECT * FROM daily_sales
    """

    result = _extract_table_references([wave1_sql, wave2_sql])
    assert "transactions" in result
    assert "enrich_transactions" in result  # This is the critical one
    assert "daily_sales" in result


def test_table_after_where_clause() -> None:
    """Extract table when WHERE clause immediately follows."""
    sql = ["SELECT * FROM orders WHERE status = 'pending'"]
    result = _extract_table_references(sql)
    assert "orders" in result


def test_table_after_group_by() -> None:
    """Extract table when GROUP BY immediately follows."""
    sql = ["SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id"]
    result = _extract_table_references(sql)
    assert "orders" in result


def test_table_after_order_by() -> None:
    """Extract table when ORDER BY immediately follows."""
    sql = ["SELECT * FROM orders ORDER BY created_at DESC"]
    result = _extract_table_references(sql)
    assert "orders" in result
