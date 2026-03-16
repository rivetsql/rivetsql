# Materialization

When the executor runs a joint, the result is wrapped in a `MaterializedRef`. This indirection is the universal materialization contract: every joint output, regardless of engine or storage backend, exposes the same interface.

---

## MaterializedRef

A `MaterializedRef` is an opaque handle to a computed dataset. The only guaranteed consumer interface is:

```python
ref.to_arrow() -> pa.Table
```

Downstream joints — including Python joints and assertions — always access data through this method. They never receive raw engine-specific objects.

```python
from rivet_core.models import Material

def enrich_orders(raw_orders: Material) -> Material:
    table = raw_orders.to_arrow()  # always works, regardless of engine
    # ... transform ...
    return enriched
```

The engine that produced the ref may hold data in memory, in a temporary table, or in a file — `MaterializedRef` hides that detail entirely.

---

## `.to_arrow()`

`.to_arrow()` converts the materialized result into a PyArrow `Table`:

- **Lazy** — conversion happens on first call, not when the ref is created
- **Idempotent** — calling it multiple times returns the same data
- **Engine-agnostic** — DuckDB, Polars, PySpark, and Postgres all produce refs whose `.to_arrow()` returns a standard `pa.Table`

This uniformity means Python joints and assertions can be written once and run on any engine.

---

## Eviction

`MaterializedRef` objects are held in memory for the duration of the execution run. Once a joint's downstream consumers have all completed, the ref is evicted to free resources.

Eviction is recorded in the `CompiledAssembly` — the compiler determines the eviction point for each joint based on the DAG structure. The executor follows this plan exactly.

If a ref is accessed after eviction, Rivet raises an actionable error:

```
RVT-401: MaterializedRef for joint '<name>' has been evicted.
Access it only within the joint's downstream execution window.
```

!!! warning
    Never store a `MaterializedRef` beyond the scope of the function that receives it. The ref is only valid during the execution of the joint that consumes it.

---

## Materialization in Python Joints

Python joints are the primary consumers of `MaterializedRef`. A handler receives a `Material` per upstream joint and returns a `Material`:

```python
import pyarrow as pa
import pyarrow.compute as pc
from rivet_core.models import Material

def compute_metrics(
    orders: Material,
    customers: Material,
) -> Material:
    orders_tbl = orders.to_arrow()
    customers_tbl = customers.to_arrow()
    joined = orders_tbl.join(customers_tbl, keys="customer_id")
    return joined
```

The handler signature must match the upstream joint names declared in the joint definition. Rivet resolves the mapping at compile time.

---

## Native SQL Write Optimization

When the compute engine and the catalog share the same backend (e.g., DuckDB engine writing to a DuckDB catalog), Rivet can bypass the Arrow materialization entirely. Instead of executing the fused SQL, converting to an Arrow table, and re-registering it for the write, the executor embeds the fused SQL directly into the write DDL:

```
CREATE TABLE target AS <fused_sql>
```

This eliminates the Arrow round-trip and executes the entire read-transform-write in a single statement on the shared backend. The optimization is transparent — pipeline definitions don't change, and the executor falls back to the Arrow path automatically when the engine and catalog don't share a backend or the write strategy isn't supported natively.

See [Write Strategies](../guides/write-strategies.md#native-sql-write-optimization) for details on which adapters and strategies support this optimization.

---

## Key Invariants

!!! abstract "Guarantees"
    - `.to_arrow()` must always exist — every engine adapter is required to implement it
    - Eviction must fail with an actionable error — stale ref access is never silently ignored
    - Eviction strategy is recorded in `CompiledAssembly` — determined at compile time, not runtime
    - Refs are engine-agnostic — Python joints and assertions are portable across engines
