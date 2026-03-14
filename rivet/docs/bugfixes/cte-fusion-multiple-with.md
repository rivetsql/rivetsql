# CTE Fusion Bug Fix: Multiple WITH Clauses

## Problem

When Rivet fused multiple joints that contained `WITH` clauses into a single execution group, it generated invalid SQL with multiple `WITH` keywords.

### Example

Given these joints:

```sql
-- Joint 1: web_funnel_analysis
WITH session_summary AS (
    SELECT session_id, user_id FROM web_analytics
    GROUP BY session_id, user_id
)
SELECT * FROM session_summary
```

```sql
-- Joint 2: unified_business_metrics
WITH daily_sales AS (
    SELECT date, SUM(amount) AS revenue
    FROM transactions
    GROUP BY date
)
SELECT * FROM daily_sales
JOIN web_funnel_analysis ON ...
```

The fusion process would generate:

```sql
WITH web_funnel_analysis AS (
    WITH session_summary AS (...)  -- ❌ Nested WITH clause
    SELECT * FROM session_summary
)
WITH daily_sales AS (...)  -- ❌ Second WITH keyword
SELECT * FROM daily_sales
JOIN web_funnel_analysis ON ...
```

This resulted in: `Parser Error: syntax error at or near "WITH"`

## Solution

The `_compose_cte` function in `rivet_core/optimizer.py` now:

1. Extracts CTEs from each joint's SQL using sqlglot AST parsing
2. Merges all CTEs into a single top-level `WITH` clause
3. Uses the main query body (without the `WITH` clause) for each joint

### Corrected Output

```sql
WITH session_summary AS (
    SELECT session_id, user_id FROM web_analytics
    GROUP BY session_id, user_id
),
web_funnel_analysis AS (
    SELECT * FROM session_summary
),
daily_sales AS (
    SELECT date, SUM(amount) AS revenue
    FROM transactions
    GROUP BY date
)
SELECT * FROM daily_sales
JOIN web_funnel_analysis ON ...
```

## Implementation Details

Added `_extract_ctes_from_sql()` helper function that:
- Parses SQL using sqlglot
- Extracts CTE definitions (name and body)
- Returns the main query without the `WITH` clause
- Handles parse failures gracefully by returning the original SQL

The `_compose_cte()` function now:
- Processes each joint to extract inner CTEs
- Accumulates all CTEs in order (preserving dependencies)
- Builds a single `WITH` clause with all CTEs
- Appends the final query body

## Testing

- Unit tests: `tests/unit/optimizer/test_cte_fusion_bug.py`
  - Single joint with nested WITH clause
  - Multiple joints with WITH clauses

- E2E tests: `tests/e2e/test_cte_fusion_bug.py`
  - Real pipeline with multiple joints containing WITH clauses
  - Verifies compilation succeeds without SQL syntax errors

## Impact

This fix enables:
- Complex analytical queries with CTEs to fuse correctly
- Better query organization using CTEs within joints
- No workarounds needed for CTE-heavy pipelines
