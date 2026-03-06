# Getting Started

Install Rivet, scaffold a project, and run your first pipeline in under 5 minutes.

---

## Installation

```bash
pip install rivetsql
```

Verify it works:

```bash
rivet --version
```

### Engine Plugins

Rivet ships with a built-in Arrow engine for testing. For real workloads, install one or more engine plugins:

| Extra | Package | Description |
|-------|---------|-------------|
| `duckdb` | `rivetsql-duckdb` | Fast local analytics engine (recommended for local dev) |
| `polars` | `rivetsql-polars` | In-process DataFrames with Polars |
| `pyspark` | `rivetsql-pyspark` | Distributed processing with Apache Spark |
| `postgres` | `rivetsql-postgres` | PostgreSQL engine and catalog |
| `aws` | `rivetsql-aws` | S3 filesystem and Glue catalog |
| `databricks` | `rivetsql-databricks` | Databricks and Unity Catalog |

Install individual plugins:

```bash
pip install 'rivetsql[duckdb]'       # recommended for local dev
pip install 'rivetsql[polars]'       # in-process DataFrames
pip install 'rivetsql[pyspark]'      # distributed Spark
pip install 'rivetsql[postgres]'     # PostgreSQL
pip install 'rivetsql[aws]'          # S3 + Glue
pip install 'rivetsql[databricks]'   # Databricks + Unity
```

Combine extras:

```bash
pip install 'rivetsql[duckdb,postgres]'
```

Or install everything:

```bash
pip install 'rivetsql[all]'
```

---

## Create a Project

```bash
mkdir my_pipeline && cd my_pipeline
rivet init
```

This scaffolds:

```
my_pipeline/
├── rivet.yaml              # project manifest
├── profiles.yaml           # engine + catalog config
├── sources/
│   └── raw_orders.yaml
├── joints/
│   └── transform_orders.sql
├── sinks/
│   └── orders_clean.yaml
├── tests/
│   └── test_transform_orders.yaml
├── quality/
│   └── orders_clean.yaml
└── data/
    └── raw_orders.csv
```

| Directory | Purpose |
|-----------|---------|
| `sources/` | Declare where input data comes from |
| `joints/` | Declare transformations (SQL, Python) |
| `sinks/` | Declare where results are written |
| `tests/` | Offline test fixtures |
| `quality/` | Assertion and audit definitions |

---

## Configure a Profile

Profiles define your catalogs and engines. The scaffolded `profiles.yaml` uses DuckDB by default:

```yaml
# profiles.yaml
default:
  catalogs:
    local:
      type: filesystem
      path: ./data
  engines:
    - name: default
      type: duckdb
      catalogs: [local]
  default_engine: default
```

!!! tip "CLI shortcuts"
    You can also manage catalogs and engines from the command line:

    ```bash
    rivet catalog create   # interactive catalog wizard
    rivet engine create    # interactive engine wizard
    rivet engine list      # show configured engines
    ```

---

## Define a Source

A source reads data from a catalog. No upstream dependencies — it's always a DAG root.

=== "SQL"

    ```sql
    -- sources/raw_orders.sql
    -- rivet:name: raw_orders
    -- rivet:type: source
    -- rivet:catalog: local
    -- rivet:table: raw_orders.csv
    ```

=== "YAML"

    ```yaml
    # sources/raw_orders.yaml
    name: raw_orders
    type: source
    catalog: local
    table: raw_orders.csv
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    raw_orders = Joint(
        name="raw_orders",
        joint_type="source",
        catalog="local",
        table="raw_orders.csv",
    )
    ```

---

## Define a Transform

A transform joint applies SQL or Python logic to upstream data:

=== "SQL"

    ```sql
    -- joints/transform_orders.sql
    -- rivet:name: transform_orders
    -- rivet:type: sql
    SELECT
        id,
        customer_name,
        amount,
        created_at
    FROM raw_orders
    WHERE amount > 0
    ```

=== "YAML"

    ```yaml
    # joints/transform_orders.yaml
    name: transform_orders
    type: sql
    upstream: [raw_orders]
    sql: |
      SELECT id, customer_name, amount, created_at
      FROM raw_orders
      WHERE amount > 0
    ```

=== "Python"

    ```python
    # joints/transform_orders.py
    # rivet:name: transform_orders
    # rivet:type: python
    # rivet:upstream: [raw_orders]
    import polars as pl
    from rivet_core.models import Material

    def transform(material: Material) -> pl.DataFrame:
        df = material.to_polars()
        return df.filter(pl.col("amount") > 0).select(
            "id", "customer_name", "amount", "created_at"
        )
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    transform_orders = Joint(
        name="transform_orders",
        joint_type="sql",
        upstream=["raw_orders"],
        sql="SELECT id, customer_name, amount, created_at FROM raw_orders WHERE amount > 0",
    )
    ```

---

## Define a Sink

A sink writes the output of a joint to a catalog:

=== "SQL"

    ```sql
    -- sinks/orders_clean.sql
    -- rivet:name: orders_clean
    -- rivet:type: sink
    -- rivet:catalog: local
    -- rivet:table: orders_clean
    -- rivet:upstream: [transform_orders]
    ```

=== "YAML"

    ```yaml
    # sinks/orders_clean.yaml
    name: orders_clean
    type: sink
    catalog: local
    table: orders_clean
    upstream: [transform_orders]
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    orders_clean = Joint(
        name="orders_clean",
        joint_type="sink",
        catalog="local",
        table="orders_clean",
        upstream=["transform_orders"],
    )
    ```

---

## Run the Pipeline

```bash
rivet run
```

```
✓ compiled 3 joints (3/3 schemas) in 45ms
  raw_orders         ✓ OK (5 rows)
  transform_orders   ✓ OK (4 rows)
  orders_clean       ✓ OK (4 rows)

  45ms | 3 joints | 1 groups | 0 materializations | 0 failures
```

Rivet compiles the DAG, resolves execution order, and runs each joint in sequence: source → transform → sink.

---

## Add Quality Checks

Assertions validate data before it's written. Define them in the `quality/` directory:

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

Assertions run automatically during `rivet run`. If a check fails with `severity: error`, the pipeline stops before writing bad data.

---

## Add Tests

Tests validate joint logic offline using fixture data — no database needed:

```yaml
# tests/test_transform_orders.yaml
name: test_transform_orders
joint: transform_orders
inputs:
  raw_orders:
    rows:
      - {id: 1, customer_name: Alice, amount: 100, created_at: "2024-01-01"}
      - {id: 2, customer_name: Bob,   amount: -5,  created_at: "2024-01-02"}
expected:
  rows:
    - {id: 1, customer_name: Alice, amount: 100, created_at: "2024-01-01"}
```

```bash
rivet test
```

```
  ✓ PASS  test_transform_orders  (12ms)

Tests: 1 passed, 1 total, 12ms
```

---

## Explore Your Data

Before diving into the docs, try the interactive REPL. It gives you a full-screen terminal UI to browse your pipeline, run ad-hoc queries against any joint, and inspect data — all without leaving the terminal.

```bash
rivet repl
```

From there you can query any joint directly, browse catalogs, and iterate on transforms in real time. It's the fastest way to understand what your pipeline is doing.

!!! tip "Try it now"
    Run `rivet repl` in your project directory. Type a joint name to preview its data, or write SQL against any joint in your pipeline.

Learn more in the [REPL guide](guides/repl.md).

---

## Next Steps

<div class="link-grid" markdown>

<a class="link-card" href="concepts/">
<strong>Concepts</strong>
<span>Deep dive into joints, engines, catalogs, and compilation</span>
</a>

<a class="link-card" href="guides/quality-checks/">
<strong>Quality Checks</strong>
<span>All assertion types and configuration options</span>
</a>

<a class="link-card" href="guides/testing/">
<strong>Testing Guide</strong>
<span>Advanced fixtures, snapshots, and CI integration</span>
</a>

<a class="link-card" href="guides/write-strategies/">
<strong>Write Strategies</strong>
<span>Append, merge, SCD2, and more</span>
</a>

<a class="link-card" href="plugins/">
<strong>Plugins</strong>
<span>DuckDB, Polars, PySpark, Postgres, AWS, Databricks</span>
</a>

<a class="link-card" href="guides/repl/">
<strong>Interactive REPL</strong>
<span>Explore data and debug pipelines in the terminal</span>
</a>

</div>
