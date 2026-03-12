# Wildcard Adapters

Wildcard adapters allow catalog plugins to work with any Arrow-compatible engine without writing per-engine adapter implementations. This document explains how wildcard adapters work and when to use them.

---

## The Problem

Rivet's compiler needs an adapter to bridge between an engine and a catalog. Without an adapter:

- The compiler rejects the `(engine_type, catalog_type)` combination
- No predicate/projection/limit pushdown is possible
- The source fallback path doesn't receive pushdown plans

For a new catalog type like `rest_api`, you'd normally need to write:
- `DuckDBRestApiAdapter`
- `PolarsRestApiAdapter`
- `PySparkRestApiAdapter`
- `PostgresRestApiAdapter`
- ... one for each engine

This is repetitive when the adapter logic is identical — fetch data, convert to Arrow, hand to engine.

---

## The Solution: Wildcard Adapters

A wildcard adapter registers with `target_engine_type = "*"` (asterisk), meaning it's not bound to a specific engine. The `PluginRegistry` resolves it as a fallback when no exact match exists.

### Registration

```python
class RestApiAdapter(ComputeEngineAdapter):
    target_engine_type = "*"  # Wildcard
    catalog_type = "rest_api"
    capabilities = ["predicate_pushdown", "projection_pushdown", "limit_pushdown"]

    def read_dispatch(self, engine, catalog, joint, pushdown):
        # Fetch from API, return Arrow table
        ...
```

### Resolution Logic

When the compiler needs an adapter for `(duckdb, rest_api)`:

1. **Exact match**: Check for `(duckdb, rest_api)` adapter → Not found
2. **Wildcard fallback**: Check for `(*, rest_api)` adapter → Found `RestApiAdapter`
3. **Arrow compatibility gate**: Does DuckDB support Arrow input?
   - Check if `"arrow"` is in DuckDB's `supported_catalog_types`
   - DuckDB declares `["duckdb", "arrow", "filesystem"]` → Yes
4. **Return**: `RestApiAdapter`

The same adapter works for Polars, PySpark, and any other Arrow-compatible engine.

---

## Arrow Compatibility Gate

The wildcard adapter is only returned if the engine can consume Arrow tables. This is checked by looking for `"arrow"` in the engine's `supported_catalog_types`.

### Compatibility Matrix

| Engine | Declares `"arrow"` | Wildcard Adapter Works? |
|--------|-------------------|------------------------|
| DuckDB | ✅ Yes | ✅ Yes |
| Polars | ✅ Yes | ✅ Yes |
| PySpark | ✅ Yes | ✅ Yes |
| Databricks | ❌ No | ❌ No (RVT-402) |
| Postgres | ❌ No | ❌ No (RVT-402) |

Databricks and Postgres don't support Arrow input — they require native catalog types (Unity, postgres).

---

## Precedence Rules

Exact matches always take precedence over wildcard adapters:

1. **Exact adapter exists**: Use it (even if wildcard also exists)
2. **No exact adapter**: Try wildcard with Arrow gate
3. **No wildcard or gate fails**: Return `None` (compiler emits RVT-402)

Example:
```python
# Both registered
registry.register_adapter(DuckDBRestApiAdapter())  # Exact
registry.register_adapter(RestApiAdapter())        # Wildcard

# Lookup (duckdb, rest_api)
adapter = registry.get_adapter("duckdb", "rest_api")
# Returns: DuckDBRestApiAdapter (exact match wins)

# Lookup (polars, rest_api)
adapter = registry.get_adapter("polars", "rest_api")
# Returns: RestApiAdapter (wildcard fallback)
```

---


## When to Use Wildcard Adapters

Use wildcard adapters when:

- The adapter logic is identical across engines (fetch data, convert to Arrow, hand off)
- The catalog produces Arrow tables naturally
- You want to support all Arrow-compatible engines without per-engine code

Don't use wildcard adapters when:

- The adapter needs engine-specific optimizations
- The catalog requires engine-specific features (e.g. Databricks Unity Catalog needs Databricks SQL)
- The data format is not Arrow-compatible

---

## Implementation Guide

### Step 1: Implement the Adapter

```python
from rivet_core.plugins import ComputeEngineAdapter
from rivet_core.optimizer import AdapterPushdownResult, PushdownPlan
from rivet_core.models import Material

class MyWildcardAdapter(ComputeEngineAdapter):
    target_engine_type = "*"  # Wildcard
    catalog_type = "my_catalog"
    capabilities = ["predicate_pushdown", "limit_pushdown"]
    source = "catalog_plugin"
    source_plugin = "rivet_my_catalog"

    def read_dispatch(self, engine, catalog, joint, pushdown):
        # 1. Fetch data from catalog
        # 2. Convert to Arrow table
        # 3. Return AdapterPushdownResult with deferred Material
        ...

    def write_dispatch(self, engine, catalog, joint, material):
        # 1. Convert Arrow table to catalog format
        # 2. Write to catalog
        ...
```

### Step 2: Register via Entry Point

```toml
# pyproject.toml
[project.entry-points."rivet.compute_engine_adapters"]
my_catalog_wildcard = "rivet_my_catalog.adapter:MyWildcardAdapter"
```

### Step 3: Ensure Engines Declare Arrow Support

For the wildcard adapter to work, engines must declare `"arrow"` in their `supported_catalog_types`:

```python
class DuckDBEnginePlugin(ComputeEnginePlugin):
    type = "duckdb"
    supported_catalog_types = {
        "duckdb": [...],
        "arrow": [...],  # Required for wildcard adapters
        "filesystem": [...],
    }
```

---

## How It Works Internally

### PluginRegistry.get_adapter()

```python
def get_adapter(self, engine_type: str, catalog_type: str) -> ComputeEngineAdapter | None:
    # 1. Exact match takes precedence
    exact = self._adapters.get((engine_type, catalog_type))
    if exact is not None:
        return exact

    # 2. Wildcard fallback
    wildcard = self._adapters.get(("*", catalog_type))
    if wildcard is None:
        return None

    # 3. Arrow compatibility gate
    engine_plugin = self._engine_plugins.get(engine_type)
    if engine_plugin is None:
        return None
    if "arrow" not in engine_plugin.supported_catalog_types:
        return None

    return wildcard
```

### PluginRegistry.resolve_capabilities()

The same logic applies to capability resolution:

```python
def resolve_capabilities(self, engine_type: str, catalog_type: str) -> list[str] | None:
    # 1. Exact adapter
    adapter = self._adapters.get((engine_type, catalog_type))
    if adapter is not None:
        return adapter.capabilities

    # 2. Engine native support
    plugin = self._engine_plugins.get(engine_type)
    if plugin is not None:
        caps = plugin.supported_catalog_types.get(catalog_type)
        if caps is not None:
            return caps

    # 3. Wildcard adapter with Arrow gate
    wildcard = self._adapters.get(("*", catalog_type))
    if wildcard is not None and plugin is not None:
        if "arrow" in plugin.supported_catalog_types:
            return wildcard.capabilities

    return None
```

---

## Benefits

### For Plugin Authors

- Write one adapter instead of N adapters (one per engine)
- Automatically support new Arrow-compatible engines
- Simpler maintenance and testing

### For Engine Authors

- New engines automatically gain support for wildcard catalog types
- Just declare `"arrow"` in `supported_catalog_types`
- No coordination needed with catalog plugin authors

### For Users

- More catalog types work with more engines
- Consistent behavior across engines
- Simpler mental model (if it's Arrow-compatible, it works)

---

## Limitations

- Wildcard adapters cannot use engine-specific optimizations
- All engines get the same adapter logic (no per-engine tuning)
- Requires engines to support Arrow input (not all do)

---

## See Also

- [REST API Plugin](../plugins/rest.md) — First wildcard adapter implementation
- [Plugin Development](../plugins/development.md) — Building custom plugins
- [Adapter Architecture](adapters.md) — How adapters work
