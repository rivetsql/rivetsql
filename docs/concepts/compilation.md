# Compilation

Compilation transforms a set of joint declarations into an immutable `CompiledAssembly` — the single source of truth that drives execution, CLI display, testing, and inspection.

!!! abstract "Compilation is pure"
    `compile()` performs no data reads, no data writes, and no runtime introspection. Given the same inputs, it always produces the same output.

---

## The Pipeline

```mermaid
graph LR
    A[Config Parsing] --> B[Bridge Forward]
    B --> C[Assembly Building]
    C --> D[Compilation]
    D --> E[Execution]

    style A fill:#6c63ff,color:#fff,stroke:none
    style B fill:#7c74ff,color:#fff,stroke:none
    style C fill:#818cf8,color:#fff,stroke:none
    style D fill:#3b82f6,color:#fff,stroke:none
    style E fill:#2563eb,color:#fff,stroke:none
```

| Stage | What happens |
|-------|-------------|
| Config Parsing | Read `rivet.yaml` and `profiles.yaml`, resolve profile, validate schemas |
| Bridge Forward | Instantiate catalog and engine objects, resolve plugin entry points |
| Assembly Building | Collect joints, resolve upstream references, build the DAG |
| Compilation | Validate DAG, assign execution order, fuse adjacent joints, produce `CompiledAssembly` |
| Execution | Follow `execution_order` exactly — no re-resolution at runtime |

---

## Stage 1: Config Parsing

Rivet reads two configuration files:

- `rivet.yaml` — project manifest: directory paths for sources, joints, sinks, tests, and profiles
- `profiles.yaml` — environment-specific: catalogs, engines, default engine, credentials

The active profile is selected by `--profile` flag or `RIVET_PROFILE` environment variable.

---

## Stage 2: Bridge Forward

`rivet_bridge` converts raw config into live objects:

- Catalog configs become `Catalog` model instances
- Engine configs become `ComputeEngine` instances with resolved plugin types
- Plugin entry points are resolved via the plugin registry

This is the only stage where plugin code is loaded. After bridge forward, the rest of the pipeline works with pure data models.

---

## Stage 3: Assembly Building

The `Assembly` validates structural integrity of the joint DAG:

- Joint names must be globally unique
- All upstream references must resolve to existing joints
- Source joints must have no upstream
- Sink joints must have at least one upstream
- The graph must be acyclic

Violations raise an `AssemblyError` with a structured `RivetError` containing the error code, message, and remediation hint.

=== "SQL"

    ```sql
    -- rivet:name: raw_orders
    -- rivet:type: source
    -- rivet:catalog: local
    -- rivet:table: orders

    -- rivet:name: daily_revenue
    -- rivet:type: sql
    -- rivet:upstream: raw_orders
    SELECT order_date, SUM(amount) AS revenue
    FROM raw_orders
    GROUP BY order_date

    -- rivet:name: revenue_sink
    -- rivet:type: sink
    -- rivet:upstream: daily_revenue
    -- rivet:catalog: warehouse
    -- rivet:table: daily_revenue
    -- rivet:write_strategy: replace
    ```

=== "YAML"

    ```yaml
    # sources/raw_orders.yaml
    name: raw_orders
    type: source
    catalog: local
    table: orders

    # joints/daily_revenue.yaml
    name: daily_revenue
    type: sql
    upstream: [raw_orders]
    sql: |
      SELECT order_date, SUM(amount) AS revenue
      FROM raw_orders
      GROUP BY order_date

    # sinks/revenue_sink.yaml
    name: revenue_sink
    type: sink
    upstream: daily_revenue
    catalog: warehouse
    table: daily_revenue
    write_strategy: replace
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    raw_orders = Joint(
        name="raw_orders",
        joint_type="source",
        catalog="local",
        table="orders",
    )

    daily_revenue = Joint(
        name="daily_revenue",
        joint_type="sql",
        upstream=["raw_orders"],
        sql="SELECT order_date, SUM(amount) AS revenue FROM raw_orders GROUP BY order_date",
    )

    revenue_sink = Joint(
        name="revenue_sink",
        joint_type="sink",
        upstream=["daily_revenue"],
        catalog="warehouse",
        table="daily_revenue",
        write_strategy="replace",
    )
    ```

---

## Stage 4: Compilation

`compile()` takes an `Assembly` and produces a `CompiledAssembly`.

### Engine Resolution

Each joint is assigned an engine. Resolution order:

1. Joint-level `engine` override (highest priority)
2. Profile-level `default_engine`

### SQL Fusion

Adjacent SQL joints on the same engine instance are fused into a single query using CTEs:

```mermaid
graph LR
    A[raw_orders<br/><small>source</small>] --> B[filter_orders<br/><small>sql</small>]
    B --> C[daily_revenue<br/><small>sql</small>]
    C --> D[revenue_sink<br/><small>sink</small>]

    style B fill:#6c63ff,color:#fff,stroke:none
    style C fill:#6c63ff,color:#fff,stroke:none
```

Fusion is broken by:

- A Python joint between two SQL joints
- An engine instance change
- An explicit `eager: true` flag
- An assertion (quality check) on the upstream joint

#### Multi-Upstream Fusion

When a multi-input joint such as a SQL JOIN references several upstream joints, the optimizer merges all eligible upstream groups into a single fused group. This avoids wasteful standalone executions — for example, a bare `SELECT * FROM ...` adapter read for a source whose data is already available server-side inside the fused query.

```mermaid
graph LR
    S1[customers<br/><small>source</small>] --> J[customer_orders<br/><small>sql JOIN</small>]
    S2[orders<br/><small>source</small>] --> J
    J --> K[sink<br/><small>sink</small>]

    style S1 fill:#6c63ff,color:#fff,stroke:none
    style S2 fill:#6c63ff,color:#fff,stroke:none
    style J fill:#6c63ff,color:#fff,stroke:none
```

In the diagram above, both source joints and the JOIN are fused into a single CTE chain executed as one query. Without multi-upstream fusion, each source would run as a separate standalone group.

The same fusion-breaking conditions apply to each upstream independently — if one upstream is on a different engine or has `eager: true`, only that upstream stays in its own group while the remaining eligible upstreams are still merged.

### Execution Order

`compile()` produces a topologically sorted `execution_order` list. The executor follows this list exactly — it never re-resolves or re-orders at runtime.

### Introspection (Best-Effort)

During compilation, Rivet optionally introspects source schemas from catalogs. This improves SQL validation and column lineage tracking, but introspection failures never block compilation.

### Sink Schema Inference

After all upstream joints are compiled, Rivet automatically infers output schemas for sink joints based on their upstream data flow. This provides visibility into what data structure is being written and enables schema validation at write time.

**Single upstream:** When a sink has exactly one upstream joint, the sink inherits that upstream's output schema directly.

**Multiple upstreams:** When a sink has multiple upstream joints:

- If all upstream schemas are identical (same columns, types, nullability, and order), the sink uses that shared schema
- If upstream schemas differ or any upstream has no schema, the sink's output schema is set to None and a warning is emitted

**Schema confidence:** Sinks inherit schema confidence from their upstream joints:

- `introspected` — upstream data came from catalog introspection
- `inferred` — upstream schema was inferred from SQL analysis
- `partial` — schema merging failed due to conflicts
- `none` — no schema information available

Schema confidence follows the ranking: introspected > inferred > partial > none. When multiple upstreams have different confidence levels, the sink inherits the highest confidence present.

**Example output:**

```
✓ Compiled 3 joints in 0.12s

Execution order:
  1. raw_orders (source, duckdb, introspected schema: id: int64, amount: float64)
  2. revenue_sink (sink, duckdb, introspected schema: id: int64, amount: float64)

Schema confidence: introspected
```

When schemas conflict:

```
⚠ Warning: Sink 'combined_sink' has conflicting upstream schemas from joints: 'source1', 'source2'.
Schema inference failed. Sink output_schema set to None.
```

---

## The CompiledAssembly

`CompiledAssembly` is an immutable frozen dataclass:

| Field | Description |
|-------|-------------|
| `success` | Whether compilation succeeded |
| `joints` | `CompiledJoint` objects with resolved engine, adapter, SQL, and schema |
| `fused_groups` | Groups of SQL joints fused into single queries |
| `execution_order` | Topologically sorted list of group IDs and standalone joint names |
| `materializations` | Where intermediate results are materialized and why |
| `engine_boundaries` | Engine type changes between adjacent groups |
| `errors` | Structured `RivetError` list (non-empty when `success=False`) |
| `warnings` | Non-fatal issues (missing schemas, skipped introspection) |

### Compilation Errors

If compilation fails, `success` is `False` and the executor refuses to run. Common errors:

| Code | Cause |
|------|-------|
| `RVT-301` | Duplicate joint name |
| `RVT-302` | Unknown upstream reference |
| `RVT-303` | Source joint has upstream |
| `RVT-304` | Sink joint has no upstream |
| `RVT-305` | Cyclic dependency in DAG |
