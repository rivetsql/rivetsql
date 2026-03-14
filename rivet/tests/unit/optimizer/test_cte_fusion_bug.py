"""Test for CTE fusion bug with multiple WITH clauses.

Reproduces the bug where fusing joints that contain WITH clauses
generates invalid SQL with multiple WITH keywords.
"""

from rivet_core.optimizer import _compose_cte


def test_compose_cte_with_nested_with_clause() -> None:
    """Fusing joints with WITH clauses should merge CTEs, not nest them."""
    # Joint 1 has a WITH clause
    joint1_sql = """WITH session_summary AS (
    SELECT session_id, user_id FROM web_analytics
)
SELECT * FROM session_summary"""

    # Joint 2 references joint1
    joint2_sql = "SELECT user_id FROM joint1"

    result = _compose_cte(
        ["joint1", "joint2"],
        {"joint1": joint1_sql, "joint2": joint2_sql},
    )

    assert result is not None
    fused = result.fused_sql

    # Should have only ONE WITH keyword at the top level
    with_count = fused.count("WITH ")
    assert with_count == 1, f"Expected 1 WITH clause, found {with_count}:\n{fused}"

    # Should contain both CTEs at the same level
    assert "session_summary AS (" in fused
    assert "joint1 AS (" in fused

    # Should not have nested WITH clauses
    assert "WITH session_summary" not in fused or fused.index("WITH") == fused.index(
        "WITH session_summary"
    )


def test_compose_cte_multiple_joints_with_with_clauses() -> None:
    """Multiple joints with WITH clauses should all merge into one top-level WITH."""
    joint1_sql = """WITH cte1 AS (
    SELECT id FROM source1
)
SELECT * FROM cte1"""

    joint2_sql = """WITH cte2 AS (
    SELECT id FROM source2
)
SELECT * FROM cte2"""

    joint3_sql = """WITH cte3 AS (
    SELECT id FROM joint1
    UNION ALL
    SELECT id FROM joint2
)
SELECT * FROM cte3"""

    result = _compose_cte(
        ["joint1", "joint2", "joint3"],
        {"joint1": joint1_sql, "joint2": joint2_sql, "joint3": joint3_sql},
    )

    assert result is not None
    fused = result.fused_sql

    # Should have only ONE WITH keyword
    with_count = fused.count("WITH ")
    assert with_count == 1, f"Expected 1 WITH clause, found {with_count}:\n{fused}"

    # Should contain all CTEs
    assert "cte1 AS (" in fused
    assert "cte2 AS (" in fused
    assert "cte3 AS (" in fused
    assert "joint1 AS (" in fused
    assert "joint2 AS (" in fused
