# Testing Guide

Rivet's testing framework validates joint logic against fixture data without a live database. Tests are offline — they run entirely in-memory using the Arrow catalog, so they are fast, deterministic, and require no external infrastructure.

---

## How Tests Work

A test defines:

- **target** — the joint under test
- **inputs** — fixture rows for each upstream joint
- **expected** — the rows you expect the joint to produce

When you run `rivet test`, Rivet builds an isolated assembly containing only the tested joint and its declared inputs, compiles it, executes it against the fixture data, and compares the output to the expected rows.

!!! note
    Tests are distinct from assertions and audits. Assertions run pre-write during `rivet run`. Audits run post-write. Tests run offline and never touch a live catalog.

---

## Test Fixture Format

Test fixtures live in the `tests/` directory using the `*.test.yaml` extension:

=== "YAML"

    ```yaml
    # tests/transform_orders.test.yaml
    name: test_transform_orders
    target: transform_orders
    inputs:
      raw_orders:
        rows:
          - {id: 1, customer_name: Alice, amount: 100.00, created_at: "2024-01-01"}
          - {id: 2, customer_name: Bob,   amount: -5.00,  created_at: "2024-01-02"}
          - {id: 3, customer_name: Carol, amount: 250.00, created_at: "2024-01-03"}
    expected:
      rows:
        - {id: 1, customer_name: Alice, amount: 100.00, created_at: "2024-01-01"}
        - {id: 3, customer_name: Carol, amount: 250.00, created_at: "2024-01-03"}
    ```

=== "Rivet API"

    ```python
    from rivet_core.testing.models import TestDef

    test_transform_orders = TestDef(
        name="test_transform_orders",
        target="transform_orders",
        inputs={
            "raw_orders": {
                "rows": [
                    {"id": 1, "customer_name": "Alice", "amount": 100.00, "created_at": "2024-01-01"},
                    {"id": 2, "customer_name": "Bob",   "amount": -5.00,  "created_at": "2024-01-02"},
                    {"id": 3, "customer_name": "Carol", "amount": 250.00, "created_at": "2024-01-03"},
                ]
            }
        },
        expected={
            "rows": [
                {"id": 1, "customer_name": "Alice", "amount": 100.00, "created_at": "2024-01-01"},
                {"id": 3, "customer_name": "Carol", "amount": 250.00, "created_at": "2024-01-03"},
            ]
        },
    )
    ```

### Fixture Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | Unique test name |
| `target` | yes | Joint to test |
| `inputs` | yes | Map of upstream joint name to fixture data |
| `inputs.<name>.rows` | yes | List of row objects |
| `expected` | no | Expected output rows |
| `extends` | no | Inherit inputs from another test |
| `compare` | no | Comparison mode (default: `exact`) |
| `tags` | no | Tags for filtering |
| `engine` | no | Override compute engine |

### Inline Data vs File References

Rows can be inline or loaded from CSV:

```yaml
inputs:
  raw_orders:
    file: tests/fixtures/raw_orders.csv
```

### Test Inheritance

Use `extends` to share fixture inputs:

```yaml
name: test_high_value_orders
target: high_value_orders
extends: test_transform_orders
expected:
  rows:
    - {id: 3, customer_name: Carol, amount: 250.00, created_at: "2024-01-03"}
```

---

## Running Tests

```bash
rivet test
```

```
✔ test_transform_orders   PASSED (2 rows → 1 row)
✔ test_high_value_orders  PASSED (3 rows → 1 row)

Tests: 2 passed, 0 failed
```

### Filtering

```bash
# By file
rivet test tests/transform_orders.test.yaml

# By target joint
rivet test --target transform_orders

# By tag
rivet test --tag smoke
rivet test --tag smoke --tag regression --tag-all  # AND semantics
```

### Options

| Flag | Description |
|------|-------------|
| `--target <joint>` | Filter by target joint |
| `--tag <tag>` / `-t` | Filter by tag (repeatable) |
| `--tag-all` | Require all tags match (AND) |
| `--update-snapshots` | Update snapshot files |
| `--fail-fast` | Stop on first failure |
| `--format <text\|json>` | Output format |

### Verbose Output

Use `-v` for failure diffs:

```
✘ test_transform_orders   FAILED

  Expected 1 row, got 2 rows.

  Unexpected rows:
    {id: 2, customer_name: Bob, amount: -5.00, created_at: "2024-01-02"}

Tests: 0 passed, 1 failed
```

---

## Multi-Joint Pipelines

Provide fixture data for each upstream dependency:

=== "YAML"

    ```yaml
    name: test_enriched_orders
    target: enriched_orders
    inputs:
      raw_orders:
        rows:
          - {id: 1, customer_id: 42, amount: 100.00}
      customers:
        rows:
          - {id: 42, name: Alice, tier: gold}
    expected:
      rows:
        - {id: 1, customer_name: Alice, tier: gold, amount: 100.00}
    ```

=== "Rivet API"

    ```python
    from rivet_core.testing.models import TestDef

    test_enriched_orders = TestDef(
        name="test_enriched_orders",
        target="enriched_orders",
        inputs={
            "raw_orders": {"rows": [{"id": 1, "customer_id": 42, "amount": 100.00}]},
            "customers": {"rows": [{"id": 42, "name": "Alice", "tier": "gold"}]},
        },
        expected={
            "rows": [{"id": 1, "customer_name": "Alice", "tier": "gold", "amount": 100.00}]
        },
    )
    ```

Rivet builds an isolated assembly — only the joints listed in `inputs` and the target are compiled.

---

## Testing Python Joints

Python joints are tested the same way. The handler receives `MaterializedRef` objects built from fixture rows:

```yaml
name: test_scored_orders
target: scored_orders
inputs:
  raw_orders:
    rows:
      - {id: 1, amount: 500.00, region: "US"}
      - {id: 2, amount: 10.00,  region: "EU"}
expected:
  rows:
    - {id: 1, amount: 500.00, region: "US", score: "high"}
    - {id: 2, amount: 10.00,  region: "EU", score: "low"}
```

---

## Exit Codes

| Code | Meaning |
|------|---------|
| `0` | All tests passed |
| `1` | One or more tests failed |
| `2` | Test discovery error |
