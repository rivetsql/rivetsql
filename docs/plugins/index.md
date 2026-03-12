# Plugins

Rivet's plugin system extends the core framework with concrete implementations of engines, catalogs, sources, and sinks. Every plugin is a separate Python package registered via entry points — the core never imports plugins directly.

---

## Architecture

Plugins implement abstract base classes from `rivet_core`:

| ABC | Entry Point | Purpose |
|-----|-------------|---------|
| `CatalogPlugin` | `rivet.catalogs` | Connect to a data store (filesystem, database, object store) |
| `ComputeEnginePlugin` | `rivet.compute_engines` | Create and run a compute engine |
| `ComputeEngineAdapter` | `rivet.compute_engine_adapters` | Bridge reads/writes between an engine and a catalog |
| `SourcePlugin` | `rivet.sources` | Read data from a catalog into a `MaterializedRef` |
| `SinkPlugin` | `rivet.sinks` | Write a `MaterializedRef` to a catalog |
| `CrossJointAdapter` | `rivet.cross_joint_adapters` | Resolve data flow at engine boundaries |

!!! info "Import boundary"
    Plugins import only `rivet_core` public API. No plugin imports another plugin, `rivet_config`, or `rivet_bridge`.

---

## Built-in Plugins

Two plugins ship with `rivet_core` and require no extra installation:

| Plugin | Type | Description |
|--------|------|-------------|
| `arrow` | Catalog + Engine | In-memory PyArrow catalog for testing and intermediate materialization |
| `filesystem` | Catalog | Local filesystem supporting CSV and Parquet |

---

## Available Plugins

| Package | Engine | Catalog | Best for |
|---------|--------|---------|----------|
| `rivet-duckdb` | `duckdb` | `duckdb` | Local analytics, fast SQL on files |
| `rivet-polars` | `polars` | — | In-process DataFrame transforms |
| `rivet-pyspark` | `pyspark` | — | Large-scale distributed processing |
| `rivet-postgres` | `postgres` | `postgres` | PostgreSQL databases |
| `rivet-aws` | — | `s3`, `glue` | AWS S3 and Glue Data Catalog |
| `rivet-databricks` | `databricks` | `unity`, `databricks` | Databricks SQL warehouses and Unity Catalog |
| `rivet-rest` | — | `rest_api` | REST API endpoints with auth, pagination, and pushdown |

---

## Capability Matrix

| Plugin | Engine | Catalog | Read | Write | List Tables | Schema | Test Connection |
|--------|:------:|:-------:|:----:|:-----:|:-----------:|:------:|:---------------:|
| `rivet-duckdb` | :material-check: | :material-check: | :material-check: | :material-check: | :material-check: | :material-check: | :material-check: |
| `rivet-polars` | :material-check: | — | :material-check: | — | — | — | — |
| `rivet-pyspark` | :material-check: | — | :material-check: | — | — | — | — |
| `rivet-postgres` | :material-check: | :material-check: | :material-check: | :material-check: | :material-check: | :material-check: | :material-check: |
| `rivet-aws` | — | :material-check: | :material-check: | :material-check: | :material-check: | :material-check: | :material-check: |
| `rivet-databricks` | :material-check: | :material-check: | :material-check: | :material-check: | :material-check: | :material-check: | :material-check: |
| `rivet-rest` | — | :material-check: | :material-check: | :material-check: | :material-check: | :material-check: | :material-check: |

---

## Installation

Install plugins as extras of the `rivetsql` package:

```bash
pip install 'rivetsql[duckdb,postgres]'
```

Or install everything:

```bash
pip install 'rivetsql[all]'
```

Rivet discovers plugins automatically at startup via Python entry points — no manual registration required.

---

## Configuration

Plugins are configured in `profiles.yaml`. Each catalog has a `type` that maps to a `CatalogPlugin`, and each engine has a `type` that maps to a `ComputeEnginePlugin`:

```yaml
default:
  engines:
    - name: default
      type: duckdb
      catalogs: [local, warehouse]
  catalogs:
    - name: local
      type: filesystem
      options:
        path: data/
        format: parquet
    - name: warehouse
      type: duckdb
      options:
        path: warehouse.duckdb
```

---

## Plugin Pages

<div class="link-grid" markdown>

<a class="link-card" href="duckdb/">
<strong>DuckDB</strong>
<span>Fast in-process SQL engine with filesystem and object store catalogs</span>
</a>

<a class="link-card" href="polars/">
<strong>Polars</strong>
<span>In-process DataFrame engine for Python-native transforms</span>
</a>

<a class="link-card" href="pyspark/">
<strong>PySpark</strong>
<span>Distributed processing on Apache Spark clusters</span>
</a>

<a class="link-card" href="postgres/">
<strong>Postgres</strong>
<span>PostgreSQL compute engine and catalog</span>
</a>

<a class="link-card" href="aws/">
<strong>AWS (S3 + Glue)</strong>
<span>S3 object storage and Glue Data Catalog</span>
</a>

<a class="link-card" href="unity/">
<strong>Unity / Databricks</strong>
<span>Databricks SQL warehouses and Unity Catalog</span>
</a>

<a class="link-card" href="rest/">
<strong>REST API</strong>
<span>REST API endpoints with authentication, pagination, and predicate pushdown</span>
</a>

<a class="link-card" href="development/">
<strong>Plugin Development</strong>
<span>Build your own engine, catalog, or adapter plugin</span>
</a>

</div>
