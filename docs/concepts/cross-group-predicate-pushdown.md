# Cross-Group Predicate Pushdown

When a pipeline joins data from multiple engines — say a Polars SQL joint consuming two Databricks source tables — the optimizer can push WHERE filters across the materialization boundary so that each adapter reads only the rows it needs, instead of fetching full tables and filtering locally.

This is called **cross-group predicate pushdown**. It runs automatically after the existing intra-group pushdown pass and requires no changes to your pipeline definitions.

---

## Why It Matters

Consider a Polars SQL joint that joins two Databricks sources:

```sql
-- consumer joint (Polars engine)
SELECT o.order_id, c.name
FROM   source_orders  o
JOIN   source_customers c ON o.customer_id = c.id
WHERE  o.correlation_id = 'abc-123'
```

Without cross-group pushdown, the executor issues three operations:

1. `SELECT * FROM orders` — full table scan on Databricks
2. `SELECT * FROM customers` — full table scan on Databricks
3. Filter and join locally in Polars

With cross-group pushdown, the optimizer traces `correlation_id` through column lineage back to the `source_orders` Databricks adapter and pushes the filter down:

1. `SELECT * FROM orders WHERE correlation_id = 'abc-123'` — filtered read on Databricks
2. `SELECT * FROM customers` — unchanged (predicate doesn't target this source)
3. Join locally in Polars (consumer still applies the filter as a safety net)

The result: less data transferred, faster reads, lower cost.

---

## Join-Equality Propagation

The optimizer goes one step further. When a predicate column participates in an INNER JOIN equality, the filter is inferred for the other side of the join too.

```sql
SELECT o.order_id, c.name
FROM   source_orders  o
INNER JOIN source_customers c ON o.customer_id = c.id
WHERE  o.customer_id = 42
```

Here `customer_id = 42` is pushed to `source_orders`, and because `o.customer_id = c.id` is an INNER JOIN equality, the optimizer derives `id = 42` and pushes it to `source_customers` as well. Both Databricks reads are now filtered.

This only applies to **INNER JOINs** with simple column-reference equalities. LEFT, RIGHT, FULL OUTER, and CROSS joins are excluded because filtering the non-preserved side changes the result set.

---

## Which Predicates Are Pushable

A WHERE conjunct is eligible for cross-group pushdown when **all** of the following hold:

| Condition | Reason |
|-----------|--------|
| Location is `WHERE` (not `HAVING`) | HAVING applies after aggregation — pushing it before aggregation changes semantics |
| No subqueries | Correlated/uncorrelated subqueries can't be evaluated at the adapter level |
| All referenced columns have column lineage | Without lineage, the optimizer can't map the column to a source |
| Lineage transform is `direct` or `renamed` | Aggregation, window, expression, multi-column, and opaque transforms mean the column doesn't exist as-is at the source |
| All columns trace to a single source joint | Multi-source predicates (e.g., `a.col = b.col`) can't be pushed to one side |
| Target adapter has `predicate_pushdown` capability | Some adapters don't support server-side filtering |

Predicates that don't meet these criteria stay on the consumer side — the query is still correct, just not optimized at the adapter level.

!!! note
    DISTINCT and ORDER BY on the consumer joint do **not** block pushdown. These clauses affect output ordering and deduplication, not input row filtering.

---

## Verifying Pushdown

Run your pipeline with verbose output (`-vv`) to see optimization results. Each pushed predicate produces an `OptimizationResult` entry:

```
[optimizer] cross_group_predicate_pushdown: applied
  Pushed predicate 'correlation_id = 'abc-123'' to source joint 'source_orders' in group 'group_0'
```

Non-pushable predicates are logged with a reason:

```
[optimizer] cross_group_predicate_pushdown: skipped
  Predicate 'count_col > 5' on exit joint 'consumer' is non-pushable
  (HAVING, subquery, or non-direct lineage transform)
```

Incapable adapters are logged as `not_applicable`:

```
[optimizer] cross_group_predicate_pushdown: not_applicable
  Predicate 'status = 'active'' targets source joint 'csv_source' in group 'group_1'
  whose adapter lacks predicate_pushdown capability
```

---

## Limitations

- **Expression-based join conditions** — `ON UPPER(a.col) = b.col` does not trigger join-equality propagation. Only simple column-reference equalities are eligible.
- **Multi-source predicates** — `WHERE a.col = b.col` references columns from two sources and cannot be pushed to either side alone.
- **Adapters without `predicate_pushdown`** — file-based adapters (CSV, Parquet without partition pruning) may not support server-side filtering. The predicate stays on the consumer side.
- **Non-INNER joins** — LEFT, RIGHT, FULL OUTER, and CROSS joins block join-equality derivation. The direct predicate is still pushed to its own source, but no derived predicate is inferred for the other side.
- **Aggregation/window-derived columns** — columns produced by `COUNT(*)`, `SUM()`, `ROW_NUMBER()`, etc. cannot be pushed because the value doesn't exist at the source level.
- **Consumer predicates are always retained** — even when a predicate is pushed, the consumer engine re-applies it after materialization as a correctness safety net. This means pushdown can only make things faster, never incorrect.
