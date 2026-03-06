# Rivet Pipeline Development Guide

This document provides all the context an AI agent needs to write correct Rivet pipelines.

## What is Rivet

Rivet is a declarative SQL pipeline framework built on three pillars:

- **Joints** (what to compute): named, immutable units of computation — source, sql, python, sink
- **Engines** (how to compute): pluggable backends — DuckDB, Polars, PySpark, Postgres, Databricks
- **Catalogs** (where data lives): named data locations — filesystem, databases, object stores

Pipelines are defined once and can run on any engine without changing logic. Adjacent SQL joints on the same engine are automatically fused into a single query (CTE chain).

## Project Structure

A Rivet project scaffolded by `rivet init` looks like:

```
my_pipeline/
├── rivet.yaml              # project manifest
├── profiles.yaml           # engine + catalog config
├── sources/                # source joint declarations
├── joints/                 # transform joint declarations (SQL, Python)
├── sinks/                  # sink joint declarations
├── tests/                  # offline fixture-based tests
├── quality/                # assertion and audit definitions
└── data/                   # local data files
```

## Configuration Files

### rivet.yaml — Project Manifest

```yaml
profiles: profiles.yaml
sources: sources
joints: joints
sinks: sinks
tests: tests
quality: quality
```

### profiles.yaml — Environment Config

```yaml
default:
  catalogs:
    local:
      type: filesystem
      path: ./data
    warehouse:
      type: duckdb
      options:
        path: warehouse.duckdb
  engines:
    - name: default
      type: duckdb
      catalogs: [local, warehouse]
  default_engine: default
```

Catalog types: `filesystem`, `duckdb`, `postgres`, `s3`, `glue`, `unity`.
Engine types: `duckdb`, `polars`, `pyspark`, `postgres`, `databricks`, `arrow` (built-in).

## Joint Types

There are exactly 4 joint types: `source`, `sql`, `python`, `sink`.

### Declaring Joints

Joints can be declared in 3 formats: SQL annotations, YAML, or Python API.

#### SQL Annotations Format

Annotations go at the top of `.sql` files using `-- rivet:key: value` syntax. The parser stops at the first non-annotation, non-blank line.

```sql
-- rivet:name: my_joint
-- rivet:type: sql
-- rivet:upstream: [dep_a, dep_b]
-- rivet:engine: spark
-- rivet:eager: true
-- rivet:tags: [finance, daily]
-- rivet:description: Aggregates daily revenue
SELECT ...
```

#### YAML Format

```yaml
name: my_joint
type: sql
upstream: [dep_a, dep_b]
engine: spark
sql: |
  SELECT ...
```

#### Python File Format

`.py` files in `joints/` use `# rivet:key: value` annotations:

```python
# joints/enrich.py
# rivet:upstream: [raw_orders]
import pyarrow as pa
from rivet_core.models import Material

def transform(material: Material) -> pa.Table:
    table = material.to_arrow()
    return table
```

Auto-derived defaults: `name` = file stem, `type` = `python`, `function` = `<module_path>:transform`.

## Joint Field Reference

All fields for the `Joint` model:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | str | yes | Globally unique joint name |
| `joint_type` / `type` | str | yes | One of: `source`, `sql`, `python`, `sink` |
| `catalog` | str | source/sink | Catalog name for reading/writing |
| `table` | str | source/sink | Table identifier within the catalog |
| `upstream` | list[str] | sql/python/sink | Upstream joint names |
| `sql` | str | sql joints | SQL query text |
| `function` | str | python joints | Dotted path `module.path:callable` |
| `engine` | str | no | Override default engine |
| `write_strategy` | str | sink joints | Write mode (default: `append`) |
| `eager` | bool | no | Force materialization, breaks fusion |
| `tags` | list[str] | no | Metadata tags |
| `description` | str | no | Human-readable description |
| `assertions` | list | no | Inline quality checks |
| `dialect` | str | no | SQL dialect hint |

### Upstream Constraints

- `source`: **no** upstream allowed
- `sql`: zero or more upstream
- `python`: explicit upstream required
- `sink`: at least one upstream required

## Source Joints

Sources read from a catalog. Always DAG root nodes.

```sql
-- sources/raw_orders.sql
-- rivet:name: raw_orders
-- rivet:type: source
-- rivet:catalog: local
-- rivet:table: raw_orders.csv
```

```yaml
# sources/raw_orders.yaml
name: raw_orders
type: source
catalog: local
table: raw_orders.csv
```

## SQL Joints

SQL joints transform data. They reference upstream joints by name in the FROM clause.

```sql
-- joints/daily_revenue.sql
-- rivet:name: daily_revenue
-- rivet:type: sql
SELECT order_date, SUM(amount) AS revenue
FROM raw_orders
WHERE status = 'completed'
GROUP BY order_date
```

In YAML, the `upstream` field must be explicit:

```yaml
name: daily_revenue
type: sql
upstream: [raw_orders]
sql: |
  SELECT order_date, SUM(amount) AS revenue
  FROM raw_orders WHERE status = 'completed'
  GROUP BY order_date
```

Note: in SQL files, upstream is auto-inferred from FROM clause references. In YAML, it must be declared explicitly.

## Sink Joints

Sinks write to a catalog. Always leaf nodes.

```sql
-- sinks/revenue_out.sql
-- rivet:name: revenue_out
-- rivet:type: sink
-- rivet:upstream: [daily_revenue]
-- rivet:catalog: warehouse
-- rivet:table: daily_revenue
-- rivet:write_strategy: replace
```

```yaml
name: revenue_out
type: sink
upstream: [daily_revenue]
catalog: warehouse
table: daily_revenue
write_strategy: replace
```

## Python Joints

Python joints run arbitrary Python. They receive `Material` objects and return data.

### Function Signatures

Single upstream → receives `Material` directly:
```python
def transform(material: Material) -> pa.Table:
    table = material.to_arrow()
    return table
```

Multiple upstreams → receives `dict[str, Material]`:
```python
def transform(inputs: dict[str, Material]) -> pa.Table:
    orders = inputs["raw_orders"].to_arrow()
    customers = inputs["raw_customers"].to_arrow()
    return result
```

Optional `RivetContext` parameter:
```python
from rivet_core.context import RivetContext

def transform(material: Material, context: RivetContext | None = None) -> pa.Table:
    context.logger.info(f"Running {context.joint_name}")
    return material.to_arrow()
```

### Material Conversion Methods

```python
material.to_arrow()    # → pyarrow.Table
material.to_pandas()   # → pandas.DataFrame
material.to_polars()   # → polars.DataFrame
material.to_duckdb()   # → duckdb.DuckDBPyRelation
material.to_spark()    # → pyspark.sql.DataFrame
material.columns       # → list[str]
material.num_rows      # → int
```

### Accepted Return Types

| Return type | Conversion overhead |
|-------------|-------------------|
| `pyarrow.Table` | None (preferred) |
| `pandas.DataFrame` | `from_pandas()` |
| `polars.DataFrame` | `.to_arrow()` |
| `pyspark.DataFrame` | `toPandas()` → Arrow (small results only) |
| `Material` | None (passthrough) |
| `MaterializedRef` | Wrapped in Material |

### Python Joint Fusion Behavior

Python joints always break SQL fusion. Keep them at pipeline edges when possible.

## Write Strategies

Sink joints support these write modes via `write_strategy`:

| Mode | Description | Key config |
|------|-------------|------------|
| `append` | Add rows (default) | — |
| `replace` | Drop and recreate table | — |
| `truncate_insert` | Truncate then insert (preserves table object) | — |
| `merge` | Upsert by key | `key_columns: [col]` |
| `delete_insert` | Delete matching partition, then insert | `partition_by: [col]` |
| `incremental_append` | Append rows newer than watermark | `watermark_column: col` |
| `scd2` | Slowly Changing Dimension Type 2 | `key_columns: [col]` |

Example merge sink:
```yaml
name: customers_sink
type: sink
upstream: clean_customers
catalog: warehouse
table: customers
write_strategy:
  mode: merge
  key_columns: [customer_id]
```

## Quality Checks (Assertions)

Assertions validate data pre-write. If a check fails with `severity: error`, the write is aborted.

### Inline in SQL

```sql
-- rivet:assert: not_null(order_id)
-- rivet:assert: unique(order_id)
-- rivet:assert: row_count(min=1)
-- rivet:assert: accepted_values(column=status, values=[pending, completed, cancelled])
-- rivet:assert: expression(sql=amount > 0)
-- rivet:assert: freshness(column=event_time, max_age=24h)
-- rivet:assert: schema(columns={order_id: int64, amount: float64})
```

### In YAML

```yaml
quality:
  assertions:
    - type: not_null
      columns: [order_id]
    - type: unique
      columns: [order_id]
    - type: row_count
      min: 1
      max: 10000
    - type: accepted_values
      column: status
      values: [pending, completed, cancelled]
    - type: expression
      sql: "amount > 0"
    - type: freshness
      column: event_time
      max_age: 24h
```

### Rivet API

```python
from rivet_core.checks import Assertion

Assertion(type="not_null", config={"column": "order_id"})
Assertion(type="unique", config={"column": "order_id"})
Assertion(type="row_count", config={"min": 1})
Assertion(type="accepted_values", config={"column": "status", "values": ["a", "b"]})
Assertion(type="expression", config={"expression": "amount > 0"})
Assertion(type="freshness", config={"column": "event_time", "max_age": "24h"})
Assertion(type="schema", config={"columns": {"order_id": "int64"}})
```

### Dedicated Quality Files

Place in `quality/` directory:
```yaml
# quality/orders_clean.yaml
assertions:
  - type: not_null
    columns: [id, customer_name]
    severity: error
  - type: unique
    columns: [id]
    severity: error
```

## Testing

Tests validate joint logic offline using fixture data. No live database needed.

```yaml
# tests/test_transform_orders.test.yaml
name: test_transform_orders
target: transform_orders
inputs:
  raw_orders:
    rows:
      - {id: 1, customer_name: Alice, amount: 100, created_at: "2024-01-01"}
      - {id: 2, customer_name: Bob, amount: -5, created_at: "2024-01-02"}
expected:
  rows:
    - {id: 1, customer_name: Alice, amount: 100, created_at: "2024-01-01"}
```

Run with `rivet test`. Supports `--target`, `--tag`, `--fail-fast`, `--update-snapshots`.

Inputs can also reference CSV files:
```yaml
inputs:
  raw_orders:
    file: tests/fixtures/raw_orders.csv
```

## CLI Commands

| Command | Purpose |
|---------|---------|
| `rivet init [dir]` | Scaffold project (`--style sql\|yaml\|mixed`, `--bare`) |
| `rivet run` | Compile and execute pipeline |
| `rivet test` | Run offline tests |
| `rivet compile` | Validate and inspect without executing |
| `rivet repl` | Interactive terminal UI |
| `rivet engine list` | List configured engines |
| `rivet engine create` | Interactive engine wizard |
| `rivet catalog create` | Interactive catalog wizard |

## Compilation Pipeline

Config Parsing → Bridge Forward → Assembly Building → Compilation → Execution

Key invariants:
- Compilation is pure (no I/O, deterministic)
- Execution follows `execution_order` exactly
- Fusion by default for adjacent SQL joints on same engine
- Universal materialization contract: every output supports `.to_arrow()`

## Common Error Codes

| Code | Cause |
|------|-------|
| RVT-301 | Duplicate joint name |
| RVT-302 | Unknown upstream reference |
| RVT-303 | Source joint has upstream |
| RVT-304 | Sink joint has no upstream |
| RVT-305 | Cyclic dependency |
| RVT-401 | Evicted MaterializedRef accessed |
| RVT-6xx | Quality check failure |
| RVT-751 | Python function import failed |
| RVT-752 | Python function returned None or unsupported type |
| RVT-753 | Function path not importable |

## Complete Pipeline Example

### profiles.yaml
```yaml
default:
  catalogs:
    local:
      type: filesystem
      path: ./data
    warehouse:
      type: duckdb
      options:
        path: warehouse.duckdb
  engines:
    - name: default
      type: duckdb
      catalogs: [local, warehouse]
  default_engine: default
```

### Source
```sql
-- sources/raw_orders.sql
-- rivet:name: raw_orders
-- rivet:type: source
-- rivet:catalog: local
-- rivet:table: raw_orders.csv
```

### Transform
```sql
-- joints/daily_revenue.sql
-- rivet:name: daily_revenue
-- rivet:type: sql
SELECT order_date, SUM(amount) AS revenue
FROM raw_orders
WHERE status = 'completed'
GROUP BY order_date
```

### Sink with quality checks
```sql
-- sinks/daily_revenue_out.sql
-- rivet:name: daily_revenue_out
-- rivet:type: sink
-- rivet:upstream: [daily_revenue]
-- rivet:catalog: warehouse
-- rivet:table: daily_revenue
-- rivet:write_strategy: replace
-- rivet:assert: not_null(revenue)
-- rivet:assert: row_count(min=1)
```

### Test
```yaml
# tests/test_daily_revenue.test.yaml
name: test_daily_revenue
target: daily_revenue
inputs:
  raw_orders:
    rows:
      - {order_date: "2024-01-01", amount: 100, status: completed}
      - {order_date: "2024-01-01", amount: 50, status: completed}
      - {order_date: "2024-01-02", amount: 200, status: cancelled}
expected:
  rows:
    - {order_date: "2024-01-01", revenue: 150}
```

## Best Practices

- Use SQL joints for transformations whenever possible (they fuse for performance)
- Reserve Python joints for logic that SQL can't express (ML, APIs, complex row logic)
- Place Python joints at pipeline edges to minimize fusion breaks
- Always add `not_null` and `unique` assertions on key columns in sinks
- Write tests for every transform joint
- Use `replace` strategy for dimension tables, `append` for event logs, `merge` for idempotent upserts
- Keep joint names descriptive and globally unique
- Use `pyarrow.Table` as the return type in Python joints for zero conversion overhead
