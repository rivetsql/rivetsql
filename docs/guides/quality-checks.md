# Quality Checks

Quality checks validate data before it is written to a sink. Rivet runs assertions on the computed result set and halts execution if any check fails, preventing bad data from reaching your target catalog.

!!! info "Assertions vs Audits vs Tests"
    **Assertions** run pre-write on computed data in memory.
    **Audits** run post-write by reading back from the target catalog.
    **Tests** run offline against fixture data.
    This guide covers assertions. See [Assertions, Audits & Tests](../concepts/assertions-audits-tests.md) for the full picture.

---

## How Quality Checks Work

Checks are attached to a joint (typically a sink). When the executor reaches that joint, it evaluates every check against the materialized result before writing. If any check fails, execution stops with a `RVT-6xx` error.

Checks can be declared in:

- Inline in SQL via `-- rivet:assert:` annotations
- Inline in YAML under the `quality:` block
- Co-located YAML file with the same stem as the joint
- Dedicated YAML in the `quality/` directory

---

## Assertion Types

### `not_null`

Fails if any row has a `NULL` in the specified column.

=== "SQL"

    ```sql
    -- rivet:name: orders_clean
    -- rivet:type: sql
    -- rivet:assert: not_null(order_id)
    -- rivet:assert: not_null(customer_id)

    SELECT order_id, customer_id, amount
    FROM raw_orders
    WHERE status = 'completed'
    ```

=== "YAML"

    ```yaml
    name: orders_clean
    type: sql
    sql: |
      SELECT order_id, customer_id, amount
      FROM raw_orders WHERE status = 'completed'
    quality:
      assertions:
        - type: not_null
          columns: [order_id]
        - type: not_null
          columns: [customer_id]
    ```

=== "Rivet API"

    ```python
    from rivet_core.checks import Assertion
    from rivet_core.models import Joint

    orders_clean = Joint(
        name="orders_clean",
        joint_type="sql",
        sql="SELECT order_id, customer_id, amount FROM raw_orders WHERE status = 'completed'",
        assertions=[
            Assertion(type="not_null", config={"column": "order_id"}),
            Assertion(type="not_null", config={"column": "customer_id"}),
        ],
    )
    ```

---

### `unique`

Fails if any value (or combination) appears more than once.

=== "SQL"

    ```sql
    -- rivet:name: dim_customers
    -- rivet:type: sql
    -- rivet:assert: unique(customer_id)

    SELECT DISTINCT customer_id, name, email
    FROM raw_customers
    ```

=== "YAML"

    ```yaml
    name: dim_customers
    type: sql
    sql: |
      SELECT DISTINCT customer_id, name, email FROM raw_customers
    quality:
      assertions:
        - type: unique
          columns: [customer_id]
    ```

=== "Rivet API"

    ```python
    from rivet_core.checks import Assertion
    from rivet_core.models import Joint

    dim_customers = Joint(
        name="dim_customers",
        joint_type="sql",
        sql="SELECT DISTINCT customer_id, name, email FROM raw_customers",
        assertions=[Assertion(type="unique", config={"column": "customer_id"})],
    )
    ```

---

### `row_count`

Fails if the number of rows falls outside the specified bounds.

=== "SQL"

    ```sql
    -- rivet:name: daily_summary
    -- rivet:type: sql
    -- rivet:assert: row_count(min=1)

    SELECT order_date, COUNT(*) AS orders
    FROM raw_orders
    GROUP BY order_date
    ```

=== "YAML"

    ```yaml
    name: daily_summary
    type: sql
    sql: |
      SELECT order_date, COUNT(*) AS orders FROM raw_orders GROUP BY order_date
    quality:
      assertions:
        - type: row_count
          min: 1
          max: 10000
    ```

=== "Rivet API"

    ```python
    from rivet_core.checks import Assertion
    from rivet_core.models import Joint

    daily_summary = Joint(
        name="daily_summary",
        joint_type="sql",
        sql="SELECT order_date, COUNT(*) AS orders FROM raw_orders GROUP BY order_date",
        assertions=[Assertion(type="row_count", config={"min": 1})],
    )
    ```

---

### `accepted_values`

Fails if any row contains a value not in the allowed set.

=== "SQL"

    ```sql
    -- rivet:assert: accepted_values(column=status, values=[pending, completed, cancelled])
    ```

=== "YAML"

    ```yaml
    quality:
      assertions:
        - type: accepted_values
          column: status
          values: [pending, completed, cancelled]
    ```

=== "Rivet API"

    ```python
    Assertion(
        type="accepted_values",
        config={"column": "status", "values": ["pending", "completed", "cancelled"]},
    )
    ```

---

### `expression`

Fails if the SQL expression evaluates to `FALSE` for any row.

=== "SQL"

    ```sql
    -- rivet:assert: expression(sql=amount > 0)
    ```

=== "YAML"

    ```yaml
    quality:
      assertions:
        - type: expression
          sql: "amount > 0"
        - type: expression
          sql: "discount >= 0 AND discount < amount"
    ```

=== "Rivet API"

    ```python
    Assertion(type="expression", config={"expression": "amount > 0"})
    ```

---

### `custom`

Runs a SQL query that must return zero rows to pass.

=== "SQL"

    ```sql
    -- rivet:assert: custom(sql=SELECT order_date FROM revenue_report GROUP BY order_date HAVING COUNT(*) > 1)
    ```

=== "YAML"

    ```yaml
    quality:
      assertions:
        - type: custom
          sql: |
            SELECT order_date FROM revenue_report
            GROUP BY order_date HAVING COUNT(*) > 1
    ```

=== "Rivet API"

    ```python
    Assertion(
        type="custom",
        config={"function": "my_project.checks.no_duplicate_dates"},
    )
    ```

---

### `schema`

Fails if the result set doesn't match declared column names and types.

=== "SQL"

    ```sql
    -- rivet:assert: schema(columns={order_id: int64, amount: float64, order_date: date})
    ```

=== "YAML"

    ```yaml
    quality:
      assertions:
        - type: schema
          columns:
            order_id: int64
            amount: float64
            order_date: date
    ```

=== "Rivet API"

    ```python
    Assertion(
        type="schema",
        config={"columns": {"order_id": "int64", "amount": "float64", "order_date": "date"}},
    )
    ```

---

### `freshness`

Fails if the most recent value in a timestamp column is older than the threshold.

=== "SQL"

    ```sql
    -- rivet:assert: freshness(column=event_time, max_age=24h)
    ```

=== "YAML"

    ```yaml
    quality:
      assertions:
        - type: freshness
          column: event_time
          max_age: 24h
    ```

=== "Rivet API"

    ```python
    Assertion(type="freshness", config={"column": "event_time", "max_age": "24h"})
    ```

Supported units: `m` (minutes), `h` (hours), `d` (days).

---

### `relationship`

Fails if any value in a column doesn't exist in a referenced joint's column.

!!! note
    The `relationship` check is recognized and can be declared, but is currently skipped at execution time. It will be fully implemented in a future release.

=== "SQL"

    ```sql
    -- rivet:assert: relationship(column=order_id, to=orders_clean.order_id)
    ```

=== "YAML"

    ```yaml
    quality:
      assertions:
        - type: relationship
          column: order_id
          to: orders_clean.order_id
    ```

=== "Rivet API"

    ```python
    Assertion(
        type="relationship",
        config={"column": "order_id", "to": "orders_clean.order_id"},
    )
    ```

---

## YAML Configuration

### Inline quality block

```yaml
# joints/orders_clean.yaml
name: orders_clean
type: sql
sql: |
  SELECT order_id, customer_id, amount, status
  FROM raw_orders WHERE status = 'completed'
quality:
  assertions:
    - type: not_null
      columns: [order_id]
    - type: unique
      columns: [order_id]
    - type: row_count
      min: 1
```

### Co-located quality file

Place a YAML file in the same directory with the same stem. It must not contain `name` and `type` keys:

```yaml
# joints/orders_clean.yaml (co-located quality file)
assertions:
  - type: not_null
    columns: [order_id]
  - type: unique
    columns: [order_id]
```

### Dedicated quality directory

```yaml
# quality/orders_clean.yaml
joint: orders_clean
assertions:
  - type: not_null
    columns: [order_id]
  - type: expression
    sql: "amount >= 0"
```

---

## Sink Integration

Quality checks are most commonly attached to sink joints:

=== "SQL"

    ```sql
    -- rivet:name: write_orders
    -- rivet:type: sink
    -- rivet:upstream: orders_clean
    -- rivet:catalog: warehouse
    -- rivet:table: orders
    -- rivet:assert: not_null(order_id)
    -- rivet:assert: unique(order_id)
    -- rivet:assert: row_count(min=1)
    ```

=== "YAML"

    ```yaml
    name: write_orders
    type: sink
    upstream: orders_clean
    catalog: warehouse
    table: orders
    quality:
      assertions:
        - type: not_null
          columns: [order_id]
        - type: unique
          columns: [order_id]
        - type: row_count
          min: 1
    ```

=== "Rivet API"

    ```python
    from rivet_core.checks import Assertion
    from rivet_core.models import Joint

    write_orders = Joint(
        name="write_orders",
        joint_type="sink",
        upstream=["orders_clean"],
        catalog="warehouse",
        table="orders",
        assertions=[
            Assertion(type="not_null", config={"column": "order_id"}),
            Assertion(type="unique", config={"column": "order_id"}),
            Assertion(type="row_count", config={"min": 1}),
        ],
    )
    ```

### Failure behavior

When a check fails:

1. Raises a `RVT-6xx` error with check type, joint name, and sample of failing rows
2. Aborts the write — no data reaches the target
3. Exits with a non-zero status code

```
$ rivet run --joint write_orders

✗ Quality check failed: not_null on column 'order_id' in joint 'write_orders'
  3 rows with NULL order_id (showing first 5):
  ┌──────────┬─────────────┬────────┐
  │ order_id │ customer_id │ amount │
  ├──────────┼─────────────┼────────┤
  │ NULL     │ 42          │ 19.99  │
  │ NULL     │ 17          │  5.00  │
  │ NULL     │ 88          │ 99.00  │
  └──────────┴─────────────┴────────┘

Error: RVT-601 assertion failed — write aborted
```

---

## Quick Reference

| Type | Key config | Fails when |
|------|-----------|------------|
| `not_null` | `column` / `columns` | Any NULL in column |
| `unique` | `column` / `columns` | Duplicate values |
| `row_count` | `min`, `max` | Count outside bounds |
| `accepted_values` | `column`, `values` | Value not in set |
| `expression` | `sql` | Expression is FALSE |
| `custom` | `sql` | Query returns rows |
| `schema` | `columns` | Schema mismatch |
| `freshness` | `column`, `max_age` | Timestamp too old |
| `relationship` | `column`, `to` | Missing FK (currently skipped) |
