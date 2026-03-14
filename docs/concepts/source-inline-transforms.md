# Source Inline Transforms

Source joints normally read an entire table from a catalog. Source inline transforms let you declare filtering, column selection, renaming, type casting, and simple expressions directly on the source joint — so the adapter reads only the data you need.

Inline transforms are declared using YAML fields (`columns`, `filter`, `limit`) or an equivalent SQL annotation. Both forms produce identical results and are fully interchangeable.

---

## When to Use

Use source inline transforms when you want to:

- **Filter rows at the source** — push WHERE conditions to the adapter so less data is transferred.
- **Select a column subset** — read only the columns your pipeline needs.
- **Rename or cast columns** — reshape the schema before downstream joints see it.
- **Compute simple expressions** — derive new columns (e.g., `revenue: price * quantity`) without a separate SQL joint.
- **Limit rows** — cap the number of rows read from the source.

If your transform requires joins, CTEs, subqueries, aggregations, or window functions, use a SQL joint instead.

---

## YAML Fields

### `columns`

A list of column declarations. Each entry is either a plain column name (pass-through) or a `name: expression` mapping (computed/aliased column).

```yaml
name: orders
type: source
catalog: warehouse
table: raw_orders
columns:
  - order_id
  - customer_name
  - revenue: price * quantity
  - amount: CAST(raw_amount AS DOUBLE)
  - full_name: first_name || ' ' || last_name
```

- Plain string (`order_id`) — selects the column as-is.
- `alias: expression` (`revenue: price * quantity`) — evaluates the expression and names the output column `revenue`.
- `alias: CAST(col AS TYPE)` — applies a type cast.

When `columns` is omitted, the source reads all columns (`SELECT *`).

### `filter`

A raw SQL expression used as the WHERE clause.

```yaml
name: active_users
type: source
catalog: analytics
table: users
filter: is_active = true AND last_login > '2025-06-01'
```

### `limit`

A positive integer that caps the number of rows read.

```yaml
name: sample_events
type: source
catalog: analytics
table: events
limit: 100
```

### Combined Example

```yaml
name: recent_orders
type: source
catalog: warehouse
table: raw_orders
columns:
  - order_id
  - customer_name
  - revenue: price * quantity
filter: status = 'active' AND created_at > '2025-01-01'
limit: 1000
```

---

## SQL Equivalent

Every YAML source declaration has an equivalent SQL form. The two are interchangeable — the bridge converts YAML fields into SQL internally, and the compiler works from the same `LogicalPlan` regardless of which form you use.

=== "YAML"

    ```yaml
    name: orders
    type: source
    catalog: warehouse
    table: raw_orders
    columns:
      - order_id
      - customer_name
      - revenue: price * quantity
    filter: status = 'active'
    limit: 1000
    ```

=== "SQL"

    ```sql
    -- rivet:name: orders
    -- rivet:type: source
    -- rivet:catalog: warehouse
    -- rivet:table: raw_orders

    SELECT
        order_id,
        customer_name,
        price * quantity AS revenue
    FROM raw_orders
    WHERE status = 'active'
    LIMIT 1000
    ```

!!! note
    You cannot set both `sql` and `columns`/`filter`/`limit` on the same joint. The compiler rejects this with error RVT-763.

---

## Expressions

Source inline transforms support three kinds of column expressions, all evaluated after the adapter read completes:

| Kind | Example | Behavior |
|------|---------|----------|
| Rename | `full_name: first_name` | Renames the column in the output schema |
| Type cast | `amount: CAST(raw_amount AS DOUBLE)` | Casts the column to the target type |
| Computed | `revenue: price * quantity` | Evaluates the expression and produces a new column |

Expressions are applied in declaration order. A later expression can reference a column produced by an earlier one:

```yaml
columns:
  - total: price * quantity
  - tax: total * 0.1
```

The base columns referenced by expressions (e.g., `price`, `quantity`) are pushed to the adapter as projections. The expression itself is always evaluated locally after materialization.

---

## Single-Table Constraint

Source inline transforms may only reference a single source table. The compiler rejects SQL that contains:

| Construct | Error Code |
|-----------|------------|
| JOIN (explicit or comma-separated FROM) | RVT-760 |
| CTE (WITH clause) | RVT-761 |
| Subquery in WHERE | RVT-762 |

Valid single-table constructs are accepted: WHERE, projections, ORDER BY, LIMIT, DISTINCT.

If you need joins or CTEs, use a SQL joint downstream of the source instead.

---

## Interaction with Optimizer Passes

Source inline transforms compose with the existing cross-group optimizer passes. The merge order during execution is:

1. **Source limit** — from the source's `limit` field or SQL `LIMIT`.
2. **Source predicates** — from the source's `filter` field or SQL `WHERE`.
3. **Source projections** — from the source's `columns` field or SQL `SELECT`.
4. **Cross-group predicates** — pushed from downstream consumer joints.
5. **Cross-group projections** — pruned by downstream column usage.
6. **Cross-group limits** — pushed from downstream `LIMIT`.

All predicates (source + cross-group) are combined with AND semantics. Projections are intersected — only columns needed by both the source declaration and downstream consumers are read. Limits take the minimum of the source and cross-group values.

When the adapter supports the corresponding pushdown capability, transforms are pushed to storage. Otherwise, they are applied as residuals on the materialized Arrow table after the read completes.

---

## Schema Propagation

When a source joint has inline transforms, the compiler computes a transformed output schema and propagates it to downstream joints. This means downstream SQL joints see the correct column names and types.

| Transform | Schema Effect |
|-----------|---------------|
| Column subset | Output schema contains only the selected columns |
| Rename (`alias: col`) | Output schema uses the alias name |
| CAST (`alias: CAST(col AS TYPE)`) | Output schema uses the target type |
| Computed expression | Output schema includes the new column (type inferred when possible) |
| `SELECT *` (no columns field) | Output schema is the full catalog schema |

If the compiler cannot determine the output type of a complex expression, it emits a warning and omits the type rather than guessing.

---

## Compilation Diagnostics

The compiler provides clear feedback for source inline transform issues:

| Code | Condition |
|------|-----------|
| RVT-760 | SQL references multiple tables (JOIN, comma FROM) |
| RVT-761 | SQL contains a CTE (WITH clause) |
| RVT-762 | SQL contains a subquery in WHERE |
| RVT-763 | Both `sql` and `columns`/`filter`/`limit` are set |
| RVT-764 | Invalid filter SQL syntax |
| RVT-765 | Invalid column expression SQL syntax |

The compiler also emits warnings (non-fatal) when a filter or column expression references a column not found in the introspected catalog schema.
