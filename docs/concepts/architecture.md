# Architecture

Rivet separates **what** to compute from **how** and **where**. This page explains how the core abstractions fit together.

## High-Level Overview

```mermaid
graph TB
    subgraph User["User Layer"]
        SQL["SQL Joint Files<br/>.sql with rivet: headers"]
        Config["rivet.yaml<br/>Catalogs & Engines"]
    end

    subgraph Core["rivet-core"]
        Compiler["Compiler<br/>Parse headers, resolve DAG"]
        Optimizer["Optimizer<br/>Pushdown & cross-joint planning"]
        Executor["Executor<br/>Orchestrate execution"]
        Assembly["Assembly<br/>Resolved pipeline graph"]
    end

    subgraph Plugins["Engine Plugins"]
        DuckDB["rivetsql-duckdb"]
        Polars["rivetsql-polars"]
        PySpark["rivetsql-pyspark"]
        Postgres["rivetsql-postgres"]
        AWS["rivetsql-aws"]
        Databricks["rivetsql-databricks"]
    end

    subgraph Catalogs["Catalog Layer"]
        Local["Local Files<br/>Parquet, CSV, Delta"]
        Cloud["Cloud Storage<br/>S3, GCS"]
        DB["Databases<br/>Postgres, DuckDB"]
        Unity["Unity Catalog<br/>Databricks"]
    end

    SQL --> Compiler
    Config --> Compiler
    Compiler --> Assembly
    Assembly --> Optimizer
    Optimizer --> Executor
    Executor --> DuckDB
    Executor --> Polars
    Executor --> PySpark
    Executor --> Postgres
    Executor --> AWS
    Executor --> Databricks
    DuckDB --> Catalogs
    Polars --> Catalogs
    PySpark --> Catalogs
    Postgres --> DB
    AWS --> Cloud
    Databricks --> Unity
```

## Compilation Pipeline

```mermaid
flowchart LR
    A["SQL Files"] --> B["Header Parser<br/>rivet: directives"]
    B --> C["DAG Builder<br/>upstream resolution"]
    C --> D["Type Checker<br/>schema inference"]
    D --> E["Assembly<br/>immutable graph"]
    E --> F["Optimizer<br/>pushdown hints"]
    F --> G["Executor"]
```

## Execution Model

Each joint goes through three phases at runtime:

```mermaid
sequenceDiagram
    participant Executor
    participant Engine as Engine Adapter
    participant Source as Source Catalog
    participant Sink as Sink Catalog

    Executor->>Engine: resolve_upstream(joint)
    Engine->>Source: read()
    Source-->>Engine: DataFrame / query ref
    Engine->>Engine: execute SQL transform
    Engine->>Engine: run assertions
    alt assertions pass
        Engine->>Sink: write(strategy)
        Sink-->>Executor: MaterializationResult
    else assertions fail
        Engine-->>Executor: AssertionError (write aborted)
    end
```

## Package Structure

```mermaid
graph LR
    rivetsql["rivetsql<br/>(meta-package)"]

    rivetsql --> core["rivetsql-core<br/>Assembly, Executor,<br/>Compiler, Plugins API"]
    rivetsql --> bridge["rivet-bridge<br/>Cross-engine routing"]
    rivetsql --> cli["rivet-cli<br/>CLI + REPL"]
    rivetsql --> duckdb["rivetsql-duckdb"]
    rivetsql --> polars["rivetsql-polars"]
    rivetsql --> pyspark["rivetsql-pyspark"]
    rivetsql --> postgres["rivetsql-postgres"]
    rivetsql --> aws["rivetsql-aws"]
    rivetsql --> databricks["rivetsql-databricks"]

    core --> config["rivet-config<br/>Schema & validation"]
```

## Key Abstractions

| Abstraction | Role | Defined In |
|---|---|---|
| `Joint` | A single SQL transform node in the DAG | `rivetsql-core` |
| `Assembly` | Immutable compiled pipeline graph | `rivetsql-core` |
| `ComputeEngine` | Named engine (e.g. `duckdb`, `spark`) | `rivetsql-core` |
| `Catalog` | Named data location (e.g. `warehouse`) | `rivetsql-core` |
| `ComputeEnginePlugin` | Plugin interface for engine adapters | `rivetsql-core` |
| `CatalogPlugin` | Plugin interface for catalog adapters | `rivetsql-core` |
| `Executor` | Walks the Assembly graph and drives execution | `rivetsql-core` |
| `CrossJointAdapter` | Handles data handoff between different engines | `rivet-bridge` |
