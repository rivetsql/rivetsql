<div align="center">
  <img src="docs/assets/logo.png" alt="Rivet Logo" width="400"/>

  <h1>Rivet</h1>
  <p><b>Declarative SQL pipelines with multi-engine execution, quality checks, and built-in testing.</b></p>

  [![PyPI version](https://img.shields.io/pypi/v/rivetsql)](https://pypi.org/project/rivetsql/)
  [![Python versions](https://img.shields.io/pypi/pyversions/rivetsql)](https://pypi.org/project/rivetsql/)
  [![License](https://img.shields.io/github/license/rivetsql/rivetsql)](https://github.com/rivetsql/rivetsql/blob/main/LICENSE)
  [![Docs](https://img.shields.io/badge/docs-rivetsql.github.io-blue)](https://rivetsql.github.io/rivetsql)
</div>

---

Rivet is a framework that revolutionizes data pipelines by strictly separating concerns. It allows you to define your pipeline once and run it on DuckDB, Polars, PySpark, Postgres or any other engine without changing your logic.

## 🧠 The Mental Model

Rivet pipelines are built on three foundational pillars:

| Concept | Rivet Abstraction | Description |
|---|---|---|
| **What** to compute | **Joints** | Named, declarative units of computation (SQL, Python, Source, Sink). |
| **How** to compute | **Engines** | Deterministic compute engines that execute the logic. |
| **Where** data lives | **Catalogs** | Named references to data locations like filesystems, databases, or object stores. |

This architecture lets you build **portable pipelines**. Adjacent SQL joints assigned to the same engine are automatically fused into a single query to reduce memory pressure and avoid unnecessary data movement.

---

## ✨ Key Features

* **🔄 Multi-Engine Execution:** Swap compute engines without rewriting pipelines.
* **🛠️ Declarative Flexibility:** Define joints using SQL, YAML, or Python.
* **🛡️ Ironclad Data Quality:** * **Assertions** run pre-write on computed data to catch errors before they hit your target. 
    * **Audits** run post-write by reading back from the target catalog to verify state.
* **🧪 Built-in Offline Testing:** Validate your transformation logic using offline fixture data without needing a live database.
* **💻 Interactive REPL:** Use `rivet repl` for a full-screen terminal UI to explore data, run ad-hoc queries, and iterate on pipeline logic.
* **🔀 Advanced Write Strategies:** Supports 7 write modes including `append`, `replace`, `merge`, and `scd2` (Slowly Changing Dimensions).

---

## ⚡ Quick Start

### 1. Install
Install Rivet with all plugins:
```sh
pip install 'rivetsql[all]'
```

Or install only what you need:
```sh
pip install 'rivetsql[duckdb]'    # recommended for local dev
```

### 2. Initialize a Project
Scaffold a new project with the required directory structure:
```sh
rivet init my_pipeline
cd my_pipeline
```

### 3. Run the Pipeline
Compile and execute your DAG:
```sh
rivet run
```

---

## 💡 Example: A Complete Pipeline

Three files. Source → Transform → Sink. That's it.

**1. Read raw data from a catalog:**
```sql
-- sources/raw_orders.sql
-- rivet:name: raw_orders
-- rivet:type: source
-- rivet:catalog: local
-- rivet:table: raw_orders
select * from raw_orders
```

**2. Transform with plain SQL:**
```sql
-- joints/daily_revenue.sql
-- rivet:name: daily_revenue
-- rivet:type: sql
SELECT
    order_date,
    SUM(amount) AS revenue
FROM raw_orders
WHERE status = 'completed'
GROUP BY order_date
```

**3. Write results with quality checks:**
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

```sh
$ rivet run
✓ compiled 3 joints in 38ms
  raw_orders          ✓ OK (1200 rows)
  daily_revenue       ✓ OK (90 rows)
  daily_revenue_out   ✓ OK (90 rows)

  38ms | 3 joints | 1 groups | 0 failures
```

If an assertion like `not_null` fails, the write is completely aborted, keeping your target clean.

---

## 🧩 Rich Plugin Ecosystem

Rivet is fully extensible through plugins.

| Package | Engine Type | Catalog Type | Best For |
|---|---|---|---|
| **`rivet-duckdb`** | `duckdb` | `duckdb` | Local analytics and fast SQL on files. |
| **`rivet-polars`** | `polars` | — | In-process DataFrame transforms. |
| **`rivet-pyspark`** | `pyspark` | — | Large-scale distributed processing. |
| **`rivet-postgres`** | `postgres` | `postgres` | PostgreSQL databases as sources and sinks. |
| **`rivet-aws`** | — | `s3`, `glue` | AWS S3 object storage and Glue Data Catalog. |
| **`rivet-databricks`** | `databricks` | `unity`, `databricks` | Databricks SQL warehouses and Unity Catalog. |

---

## 📚 Documentation

Start here:
* **[Getting Started guide](docs/getting-started.md)**
* **[Concepts Overview](docs/concepts/index.md)**
* **[Testing Guide](docs/guides/testing.md)**
* **[Quality Checks Guide](docs/guides/quality-checks.md)**

---

## 🤝 Contributing

Pull requests are welcome! 
Check out our [Contribution Guidelines](https://github.com/rivetsql/rivetsql/blob/main/CONTRIBUTING.md).

```sh
git clone https://github.com/rivetsql/rivetsql
```

<div align="center">
  <br/>
  <i>Built for data engineers who love SQL, demand quality, and value flexibility.</i>
</div>