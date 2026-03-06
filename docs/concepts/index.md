# Concepts Overview

Rivet is built around a strict separation of three concerns:

- **What** to compute — declared by *Joints*
- **How** to compute it — decided by *ComputeEngines*
- **Where** data lives — managed by *Catalogs*

This separation keeps pipeline logic portable across engines and storage backends without
changing any joint declarations.

## The Three Pillars

### Joints — What

A Joint is a named, declarative unit of computation. Joints do not execute logic; they
describe what should happen. The four joint types are:

| Type | Role |
|------|------|
| **source** | Reads data from a catalog — no upstream dependencies |
| **sql** | Transforms data using a SQL query |
| **sink** | Writes data to a catalog — has upstream dependencies |
| **python** | Transforms data using a Python function |

Joints are uniquely named within an assembly. They may declare schema, tags, and
descriptions without affecting execution semantics.

### ComputeEngines — How

A ComputeEngine decides how to execute the SQL or Python logic declared by joints.
Engines are deterministic and do not perform introspection. Adjacent joints assigned to
the same engine instance are **fused by default** — they execute as a single query rather
than materializing intermediate results.

### Catalogs — Where

A Catalog represents a data location: a filesystem, a database, an object store. Catalog
names are globally unique, the `type` field is required, and configuration is opaque to
core (validated by the plugin or built-in implementation).

## The Compilation Pipeline

Before any data moves, Rivet compiles the project into an immutable `CompiledAssembly`.
Compilation is **pure** — it performs no reads, no writes, and no runtime introspection.

```mermaid
graph LR
    A[Config Parsing] --> B[Bridge Forward]
    B --> C[Assembly Building]
    C --> D[Compilation]
    D --> E[Execution]

    style A fill:#e8f4f8
    style B fill:#e8f4f8
    style C fill:#e8f4f8
    style D fill:#d4edda
    style E fill:#fff3cd
```

| Stage | What happens |
|-------|-------------|
| **Config Parsing** | Read `rivet.yaml` and `profiles.yaml`, resolve profile selection, validate schemas |
| **Bridge Forward** | Instantiate catalog and engine objects, resolve plugin entry points |
| **Assembly Building** | Collect joint declarations, resolve upstream references, build the DAG |
| **Compilation** | Validate the DAG, assign execution order, fuse adjacent joints, produce `CompiledAssembly` |
| **Execution** | Executor takes only `CompiledAssembly`, follows `execution_order` exactly |

The `CompiledAssembly` is the single source of truth. Every downstream consumer — CLI
display, execution, testing, inspection — reads from this one immutable object.

## Key Invariants

- **Compilation is pure** — `compile()` never touches data.
- **Execution is deterministic** — the executor follows `execution_order` exactly and
  never re-resolves engines, adapters, or targets at runtime.
- **Fusion by default** — adjacent joints on the same engine are fused unless an explicit
  boundary requires otherwise.
- **Universal materialization contract** — every materialization produces a `MaterializedRef`
  that supports `.to_arrow()`.

## What's Next

| Topic | Description |
|-------|-------------|
| [Joints](joints.md) | The four joint types with code examples |
| [Engines](engines.md) | ComputeEngine configuration and capabilities |
| [Catalogs](catalogs.md) | Catalog types and configuration |
| [Compilation](compilation.md) | Deep dive into the compilation pipeline |
| [Materialization](materialization.md) | `MaterializedRef`, `.to_arrow()`, and eviction |
| [Assertions, Audits & Tests](assertions-audits-tests.md) | Quality and correctness guarantees |
