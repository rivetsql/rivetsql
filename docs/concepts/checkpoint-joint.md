# Checkpoint Joint

A checkpoint joint writes intermediate pipeline results to a persistent catalog table and re-exposes them as a `MaterializedRef` for downstream joints. It combines the write behavior of a sink with the read behavior of a source, but sits in the middle of the DAG rather than at a leaf.

---

## Use Cases

- **Long pipelines** — materialize an expensive intermediate result so it doesn't need to be recomputed on every run
- **Fan-out** — multiple downstream joints consume the same materialized result without recomputation
- **Observability** — inspect intermediate data via `rivet catalog list` and `rivet explore`

---

## Configuration

A checkpoint joint requires a `catalog` and `table`. The `write_strategy` defaults to `replace` if omitted.

```yaml
joints:
  - name: staged_orders
    type: checkpoint
    catalog: my_warehouse
    table: staged_orders
    write_strategy: replace
    upstream:
      - transform_orders
```

All values from `VALID_WRITE_STRATEGY_TYPES` are supported: `replace`, `append`, `truncate_insert`, `merge`, `delete_insert`, `incremental_append`, `scd2`.

---

## Execution Model

When the executor reaches a checkpoint, it performs a write-then-defer sequence:

1. Execute the fused SQL group (the checkpoint is the exit joint of its group)
2. Write the result to the catalog table via `SinkPlugin.write()` (Arrow fallback) or native SQL write
3. Expose a `DeferredRef` — a catalog-backed reference holding the checkpoint's catalog metadata — under the checkpoint's name

The `DeferredRef` does not eagerly read data back from the catalog. Instead, downstream groups resolve the reference lazily when they need the data, using the same adapter and source plugin mechanism used for source joints.

When the compute engine and catalog share the same backend (e.g., DuckDB engine + DuckDB catalog), the executor uses the native SQL write optimization: the fused SQL is embedded directly into the write DDL (`CREATE TABLE ... AS <fused_sql>`), skipping the Arrow round-trip. See [Native SQL Write Optimization](../guides/write-strategies.md#native-sql-write-optimization) for details.

---

## Deferred Resolution

Downstream groups resolve checkpoint references through a three-tier fallback:

1. **Adapter-based read** — if the downstream engine has a registered adapter for the checkpoint's catalog type, the adapter reads the table natively with pushdown support (predicates, projections, limits). This is the preferred path for cross-engine pipelines.
2. **Source plugin fallback** — if no adapter is available, the executor reads the table via the `SourcePlugin` for the checkpoint's catalog type.
3. **Arrow fallback** — if neither adapter nor source plugin resolves the reference, `DeferredRef.to_arrow()` reads the table from the catalog and returns an Arrow table.

This means a checkpoint acts as a materialization boundary that different engines can read from natively. For example, a Spark joint can read directly from a DuckDB-written checkpoint table via the Spark adapter, avoiding an unnecessary Arrow round-trip.

The compiler pre-resolves adapter metadata for each (downstream engine, checkpoint catalog) pair at compile time. Missing adapters produce compile-time warnings, and the resolved adapters appear in `rivet compile` output.

### Write-Path-Aware Caching

`DeferredRef` avoids redundant re-reads when the Arrow table is already in memory:

- **Arrow fallback write path** — the Arrow table that was just written to the catalog is passed to `DeferredRef` as a cached table. If a downstream group falls back to `.to_arrow()`, it returns the cached table immediately.
- **Native SQL write path** — no Arrow table exists (the engine wrote directly to the catalog), so `.to_arrow()` reads from the catalog on first access and caches the result.

### Parallel Group Isolation

When multiple fused groups execute in parallel (or sequentially without dependencies), the executor avoids eagerly materializing `DeferredRef` entries from unrelated groups. A `DeferredRef` with no cached table (native SQL write path) is skipped when building Arrow input tables for other groups — only the group that actually depends on the checkpoint will resolve it.

This prevents `RVT-501` errors on deferred-execution backends like Databricks, where calling `.to_arrow()` on a `DeferredRef` would trigger a SourcePlugin read that requires engine execution.

### CTE Injection for Same-Engine Groups

When a downstream fused group references a checkpoint from another group on the same engine, the compiler injects a CTE entry for the checkpoint into the downstream group's fused SQL at compile time. The injected CTE has the form:

```sql
WITH checkpoint_name AS (
    SELECT * FROM catalog.schema.checkpoint_table
),
-- existing CTEs follow ...
```

This allows the engine's reference resolver to treat the checkpoint identically to a source — no special-casing needed. The fully-qualified table name is derived from the checkpoint's catalog options at compile time, consistent with how source references are resolved.

The compiler scans all joints in each fused group (not just entry joints) to discover checkpoint dependencies. A joint can have upstream both inside and outside its group — for example, a SQL joint that references a local source AND a checkpoint from another group. Since such joints have at least one intra-group upstream, they are not entry joints, but they still need checkpoint CTE injection.

CTE injection runs after reference resolution, so it does not interfere with the resolver's processing of source CTEs. The deferred resolution fallback (adapter, source plugin, Arrow) still applies when CTE injection is not available (e.g., cross-engine scenarios or engines without a reference resolver).

### Cross-Engine Pipelines

Checkpoints enable cross-engine data handoff without manual staging:

```yaml
joints:
  - name: staged
    type: checkpoint
    catalog: my_filesystem
    table: staged_data
    upstream: [duckdb_transform]

  - name: spark_report
    type: sql
    engine: spark
    upstream: [staged]
```

In this example, `duckdb_transform` writes to a filesystem catalog via DuckDB. The `spark_report` joint reads the checkpoint table natively through the Spark filesystem adapter — no Arrow round-trip required.

---

## Fusion Behavior

The checkpoint is a **downstream fusion boundary**. Upstream SQL joints fuse with the checkpoint normally (CTE chain), but nothing downstream can fuse into the checkpoint's group.

```
source → sql_a → sql_b → checkpoint → sql_c → sink
|___________ FusedGroup 1 ___________| |_ FG 2 _| |_ FG 3 _|
```

The checkpoint becomes the **exit joint** of its fused group. After the fused SQL executes, the executor dispatches the write-then-read sequence before handing data to the next group.

---

## Fan-Out

When multiple downstream joints reference the same checkpoint, the write-then-read executes exactly once. All downstream joints receive the same `MaterializedRef`:

```yaml
joints:
  - name: staged
    type: checkpoint
    catalog: warehouse
    table: staged_data
    upstream: [transform]

  - name: report_a
    type: sql
    upstream: [staged]

  - name: report_b
    type: sql
    upstream: [staged]
```

---

## Watermark Support

Checkpoints with `write_strategy: incremental_append` participate in the watermark lifecycle. The executor reads the current `WatermarkState` before writing and advances it after a successful write, following the same convention as sink joints. Watermark state is stored at `.rivet/watermarks/{profile}/{joint_name}.json`.

When no prior watermark exists, the checkpoint performs a full load.

---

## Validation Rules

| Rule | Error |
|---|---|
| Checkpoint with no upstream | `RVT-304` — at least one upstream is required |
| Missing `catalog` | Compilation error — `catalog` is required |
| Missing `table` | Compilation error — `table` is required |
| No downstream consumers | Warning (valid, but likely unintentional) |

---

## Key Invariants

!!! abstract "Guarantees"
    - Write-then-defer is atomic per checkpoint — downstream never sees partial writes
    - The checkpoint table persists between runs until overwritten by a subsequent run
    - Checkpoint tables are visible in `rivet catalog list` and queryable via `rivet explore`
    - Fan-out executes the checkpoint exactly once regardless of downstream consumer count
    - Deferred resolution produces data equivalent to what was written, regardless of resolution path (adapter, source plugin, or Arrow fallback)
    - Cross-engine downstream groups resolve checkpoint references using their own engine's adapter, enabling native reads without Arrow round-trips
