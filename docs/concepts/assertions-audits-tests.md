# Assertions, Audits & Tests

Rivet provides three distinct mechanisms for validating data quality and pipeline correctness:

| Mechanism | When | What | Live data? |
|-----------|------|------|:----------:|
| **Assertion** | Pre-write | Inline validation before a sink writes | Yes |
| **Audit** | Post-write | Verification by reading back from target | Yes |
| **Test** | Offline | Logic validation using static fixtures | No |

---

## Assertions

An assertion is an inline quality check attached to a sink joint. It runs after upstream data is computed but before it is written. If any assertion fails, the write is aborted.

Supported types:

| Type | Description |
|------|-------------|
| `not_null` | No nulls in specified column(s) |
| `unique` | All values are distinct |
| `row_count` | Row count satisfies min/max constraint |
| `accepted_values` | Column contains only allowed values |
| `expression` | SQL expression evaluates to true for every row |
| `custom` | Python function returns pass/fail |
| `schema` | Output schema matches declared schema |
| `freshness` | Timestamp column contains recent-enough values |
| `relationship` | Foreign key values exist in a reference table |

=== "SQL"

    ```sql
    -- rivet:name: revenue_sink
    -- rivet:type: sink
    -- rivet:upstream: daily_revenue
    -- rivet:catalog: warehouse
    -- rivet:table: daily_revenue
    -- rivet:write_strategy: replace
    -- rivet:assert:not_null: revenue
    -- rivet:assert:row_count: min=1
    ```

=== "YAML"

    ```yaml
    name: revenue_sink
    type: sink
    upstream: daily_revenue
    catalog: warehouse
    table: daily_revenue
    write_strategy: replace
    quality:
      assertions:
        - type: not_null
          columns: [revenue]
        - type: row_count
          min: 1
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint
    from rivet_core.checks import Assertion

    revenue_sink = Joint(
        name="revenue_sink",
        joint_type="sink",
        upstream=["daily_revenue"],
        catalog="warehouse",
        table="daily_revenue",
        write_strategy="replace",
        assertions=[
            Assertion(type="not_null", config={"columns": ["revenue"]}),
            Assertion(type="row_count", config={"min": 1}),
        ],
    )
    ```

!!! warning "Assertion failure aborts the write"
    When an assertion fails, data is not written to the target. The error includes the assertion type, the failing column or expression, and the number of violating rows.

---

## Audits

An audit is a post-write verification that reads data back from the target catalog. Audits confirm data was persisted correctly and the target state matches expectations.

=== "SQL"

    ```sql
    -- rivet:name: revenue_sink
    -- rivet:type: sink
    -- rivet:upstream: daily_revenue
    -- rivet:catalog: warehouse
    -- rivet:table: daily_revenue
    -- rivet:write_strategy: replace
    -- rivet:audit:row_count: min=1
    -- rivet:audit:freshness: column=order_date, max_age_hours=25
    ```

=== "YAML"

    ```yaml
    name: revenue_sink
    type: sink
    upstream: daily_revenue
    catalog: warehouse
    table: daily_revenue
    write_strategy: replace
    quality:
      audits:
        - type: row_count
          min: 1
        - type: freshness
          column: order_date
          max_age_hours: 25
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint
    from rivet_core.checks import Assertion

    revenue_sink = Joint(
        name="revenue_sink",
        joint_type="sink",
        upstream=["daily_revenue"],
        catalog="warehouse",
        table="daily_revenue",
        write_strategy="replace",
        assertions=[
            Assertion(type="row_count", phase="audit", config={"min": 1}),
            Assertion(type="freshness", phase="audit", config={
                "column": "order_date", "max_age_hours": 25
            }),
        ],
    )
    ```

!!! tip "Assertions vs Audits"
    Use assertions to catch bad data before it reaches the target. Use audits to verify the target state after a write — for example, to detect silent write failures or catalog-level truncation.

---

## Tests

A test validates pipeline logic using static input/output fixtures. Tests run entirely offline — no catalog, no engine, no live data.

=== "YAML"

    ```yaml
    # tests/daily_revenue.test.yaml
    name: test_daily_revenue
    target: daily_revenue
    inputs:
      raw_orders:
        columns: [order_date, amount, status]
        rows:
          - ["2024-01-01", 100, "completed"]
          - ["2024-01-01", 200, "completed"]
          - ["2024-01-01",  50, "cancelled"]
          - ["2024-01-02", 300, "completed"]
    expected:
      columns: [order_date, revenue]
      rows:
        - ["2024-01-01", 300]
        - ["2024-01-02", 300]
    ```

=== "Rivet API"

    ```python
    from rivet_core.testing.models import TestDef

    test_daily_revenue = TestDef(
        name="test_daily_revenue",
        target="daily_revenue",
        inputs={
            "raw_orders": {
                "columns": ["order_date", "amount", "status"],
                "rows": [
                    ["2024-01-01", 100, "completed"],
                    ["2024-01-01", 200, "completed"],
                    ["2024-01-01",  50, "cancelled"],
                    ["2024-01-02", 300, "completed"],
                ],
            }
        },
        expected={
            "columns": ["order_date", "revenue"],
            "rows": [["2024-01-01", 300], ["2024-01-02", 300]],
        },
    )
    ```

```bash
rivet test
```

!!! tip "Tests run without a catalog"
    Tests use an in-process DuckDB instance to execute SQL fixtures. Fast, deterministic, and safe to run in CI without any infrastructure.

---

## Execution Order

During `rivet run`, the three mechanisms execute in this order:

```mermaid
graph LR
    A[Compute data] --> B[Run assertions]
    B --> C[Write to catalog]
    C --> D[Run audits]

    style A fill:#6c63ff,color:#fff,stroke:none
    style B fill:#818cf8,color:#fff,stroke:none
    style C fill:#3b82f6,color:#fff,stroke:none
    style D fill:#2563eb,color:#fff,stroke:none
```

Tests are not part of `rivet run`. They run separately via `rivet test` and never touch a live catalog.

---

## Choosing the Right Tool

| If you need to... | Use |
|---|---|
| Prevent bad data from reaching the target | Assertions |
| Verify target state after a write | Audits |
| Validate joint logic in isolation | Tests |

All three can coexist on the same sink. A typical production sink might have assertions for nullability, audits for freshness, and tests for the SQL logic.
