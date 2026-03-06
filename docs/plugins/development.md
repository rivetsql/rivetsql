# Plugin Development

This guide explains how to build a custom Rivet plugin. Plugins extend Rivet with new engines, catalogs, sources, and sinks by implementing abstract base classes from `rivet_core` and registering via Python entry points.

---

## Canonical Plugin Structure

All first-party plugins follow this directory layout. Third-party plugins should match it for consistency.

```
rivet_{name}/
├── __init__.py          # Registration function: core-first, adapters best-effort
├── engine.py            # ComputeEnginePlugin (if applicable)
├── catalog.py           # CatalogPlugin (if applicable)
├── source.py            # SourcePlugin (if applicable)
├── sink.py              # SinkPlugin (if applicable)
├── errors.py            # Error mapping utilities (if applicable, NOT _errors.py)
├── adapters/            # Only if plugin has ComputeEngineAdapters
│   ├── __init__.py
│   ├── s3.py            # Named by the catalog/engine type they bridge
│   ├── glue.py
│   └── unity.py
└── pyproject.toml
```

Rules:

- Adapter modules live in an `adapters/` subdirectory, never as flat files in the plugin root.
- Adapter files are named by the catalog or engine type they bridge (e.g., `s3.py`, `glue.py`, `unity.py`, `duckdb.py`, `pyspark.py`).
- If a plugin has no adapters, omit the `adapters/` directory entirely.
- Error modules are named `errors.py` (not `_errors.py`).

### Canonical `__init__.py` Registration Pattern

The registration function in `__init__.py` follows a strict two-phase pattern:

1. **Core components first** — catalogs, engines, sources, sinks, and cross-joint adapters are imported and registered without `try/except` guards. These are always available.
2. **Optional adapters second** — each `ComputeEngineAdapter` is registered in its own `try/except ImportError: pass` block, since adapters depend on optional third-party packages that may not be installed.

```python
"""rivet_{name} — {Description} plugin for Rivet."""

from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rivet_core.plugins import PluginRegistry


def {Name}Plugin(registry: PluginRegistry) -> None:
    """Register all rivet_{name} components into the plugin registry.

    Core components (catalog, engine, source, sink) are always registered.
    Cross-catalog adapters are registered best-effort since they depend
    on optional packages.
    """
    # 1. Core components — always registered, no try/except
    from rivet_{name}.engine import {Name}ComputeEnginePlugin

    registry.register_engine_plugin({Name}ComputeEnginePlugin())

    # 2. Optional adapters — each in its own try/except ImportError: pass
    try:
        from rivet_{name}.adapters.s3 import S3{Name}Adapter

        registry.register_adapter(S3{Name}Adapter())
    except ImportError:
        pass
```

If a plugin has no optional adapters (e.g., `rivet_aws`), the best-effort section and its docstring explanation are omitted entirely.

---

## Import Boundary Rule

Plugins import **only** `rivet_core` public API. A plugin must never import:

- Another plugin
- `rivet_config`
- `rivet_bridge`
- `rivet_cli`

This boundary is enforced as a build failure, not a warning. It ensures plugins remain decoupled and independently installable.

```python
# ✅ Correct
from rivet_core.plugins import CatalogPlugin, PluginRegistry
from rivet_core.models import Catalog, ComputeEngine, Joint, Material

# ❌ Wrong — never import other plugins or internal packages
from rivet_duckdb.engine import DuckDBComputeEnginePlugin  # no
from rivet_config.manifest import ProjectManifest           # no
```

---

## Plugin ABCs

All plugin ABCs live in `rivet_core.plugins`. Each ABC defines the contract a plugin must satisfy.

### CatalogPlugin

Connects Rivet to a data store (database, filesystem, object store). Provides validation, instantiation, table reference resolution, and optional introspection.

```python
from abc import ABC, abstractmethod
from typing import Any

from rivet_core.models import Catalog, CatalogNode, ObjectSchema, ObjectMetadata


class CatalogPlugin(ABC):
    type: str                              # unique catalog type identifier
    required_options: list[str]            # options that must be present
    optional_options: dict[str, Any]       # options with default values
    credential_options: list[str]          # sensitive options (masked in logs)
    credential_groups: dict[str, list[str]] = {}  # auth_type → relevant credential options
    env_var_hints: dict[str, str] = {}     # option → environment variable name

    @abstractmethod
    def validate(self, options: dict[str, Any]) -> None:
        """Raise on invalid options."""

    @abstractmethod
    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog:
        """Create a Catalog instance from config."""

    @abstractmethod
    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        """Map a logical joint name to a physical table reference."""

    def resolve_table_reference(self, logical_name: str, catalog: Catalog) -> str:
        """Resolve logical table name to physical reference.

        Priority: table_map entry > default_table_reference > logical name passthrough.
        """

    # Optional introspection — override to enable catalog browsing
    def list_tables(self, catalog: Catalog) -> list[CatalogNode]: ...
    def get_schema(self, catalog: Catalog, table: str) -> ObjectSchema: ...
    def test_connection(self, catalog: Catalog) -> None: ...
    def get_metadata(self, catalog: Catalog, table: str) -> ObjectMetadata | None: ...
    def list_children(self, catalog: Catalog, path: list[str]) -> list[CatalogNode]:
        """List immediate children of a path. Default filters list_tables()."""
```

### ComputeEnginePlugin

Creates a compute engine instance and executes SQL against it.

```python
import pyarrow

from rivet_core.models import ComputeEngine


class ComputeEnginePlugin(ABC):
    engine_type: str                                    # unique engine type identifier
    supported_catalog_types: dict[str, list[str]]       # catalog_type → capabilities

    @abstractmethod
    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        """Create a ComputeEngine instance from config."""

    @abstractmethod
    def validate(self, options: dict[str, Any]) -> None:
        """Raise on invalid engine options."""

    @abstractmethod
    def execute_sql(
        self,
        engine: ComputeEngine,
        sql: str,
        input_tables: dict[str, pyarrow.Table],
    ) -> pyarrow.Table:
        """Execute SQL and return results as an Arrow table."""

    @property
    def materialization_strategy_name(self) -> str | None:
        """Preferred materialization strategy name, or None for default (arrow)."""
        return None

    def get_reference_resolver(self) -> ReferenceResolver | None:
        """Return a ReferenceResolver for rewriting SQL references, or None."""
        return None

    def collect_metrics(self, execution_context: Any) -> PluginMetrics | None:
        """Collect engine-specific metrics after execution, or None."""
        return None
```

### ComputeEngineAdapter

Bridges an engine and a catalog by dispatching read and write operations. Adapters are registered for a specific `(engine_type, catalog_type)` pair.

`read_dispatch` accepts an optional `pushdown` parameter (`PushdownPlan | None`) and returns an `AdapterPushdownResult` containing the materialized data and a `ResidualPlan` listing any pushdown operations the adapter could not apply. When `pushdown` is `None`, the adapter performs a full scan and returns an empty residual.

```python
from rivet_core.plugins import ComputeEngineAdapter
from rivet_core.optimizer import PushdownPlan, AdapterPushdownResult, ResidualPlan, EMPTY_RESIDUAL


class ComputeEngineAdapter(ABC):
    target_engine_type: str        # engine type this adapter serves
    catalog_type: str              # catalog type this adapter handles
    capabilities: list[str]        # e.g. ["read", "write"]
    source: str                    # "engine_plugin" or "catalog_plugin"
    source_plugin: str | None      # package name, e.g. "rivet_postgres"

    @abstractmethod
    def read_dispatch(
        self,
        engine: Any,
        catalog: Any,
        joint: Any,
        pushdown: PushdownPlan | None = None,
    ) -> AdapterPushdownResult:
        """Read data from catalog through engine.

        When pushdown is provided, the adapter should apply as many operations
        as possible (predicates, projections, limit, casts) natively. Any
        operations that cannot be applied are returned in the residual, and the
        executor applies them post-materialization via PyArrow.

        When pushdown is None, perform a full scan and return EMPTY_RESIDUAL.
        """

    @abstractmethod
    def write_dispatch(self, engine: Any, catalog: Any, joint: Any, material: Any) -> Any:
        """Write materialized data to catalog through engine."""
```

#### Implementing Pushdown in a Custom Adapter

Each pushdown operation should be wrapped in `try/except` so that failures degrade gracefully — the failed operation moves to the residual instead of raising an error.

```python
from rivet_core.plugins import ComputeEngineAdapter
from rivet_core.optimizer import (
    PushdownPlan,
    AdapterPushdownResult,
    ResidualPlan,
    EMPTY_RESIDUAL,
)
from rivet_core.models import Material


class MyAdapter(ComputeEngineAdapter):
    target_engine_type = "my_engine"
    catalog_type = "my_catalog"
    capabilities = ["read", "write"]
    source = "engine_plugin"
    source_plugin = "rivet_my_plugin"

    def read_dispatch(self, engine, catalog, joint, pushdown=None):
        # 1. Read data from source (full scan)
        df = self._read_full(engine, catalog, joint)

        if pushdown is None:
            return AdapterPushdownResult(
                material=Material(materialized_ref=df),
                residual=EMPTY_RESIDUAL,
            )

        residual_predicates = list(pushdown.predicates.residual)
        residual_casts = list(pushdown.casts.residual)
        residual_limit = pushdown.limit.residual_limit

        # 2. Apply predicates
        for pred in pushdown.predicates.pushed:
            try:
                df = df.filter(pred.expression)
            except Exception:
                residual_predicates.append(pred)

        # 3. Apply projection
        try:
            if pushdown.projections.pushed_columns:
                df = df.select(pushdown.projections.pushed_columns)
        except Exception:
            pass  # full columns already present

        # 4. Apply limit
        try:
            if pushdown.limit.pushed_limit is not None:
                df = df.head(pushdown.limit.pushed_limit)
        except Exception:
            residual_limit = pushdown.limit.pushed_limit

        # 5. Apply casts
        for cast in pushdown.casts.pushed:
            try:
                df = df.cast_column(cast.column, cast.to_type)
            except Exception:
                residual_casts.append(cast)

        return AdapterPushdownResult(
            material=Material(materialized_ref=df),
            residual=ResidualPlan(
                predicates=residual_predicates,
                limit=residual_limit,
                casts=residual_casts,
            ),
        )

    def write_dispatch(self, engine, catalog, joint, material):
        ...
```

### SourcePlugin

Reads data from a catalog into a materialization.

```python
from rivet_core.plugins import SourcePlugin
from rivet_core.models import Catalog, Joint, Material


class SourcePlugin(ABC):
    @abstractmethod
    def read(self, catalog: Catalog, joint: Joint, pushdown: Any | None) -> Material:
        """Read data from the catalog for the given joint."""
```

### SinkPlugin

Writes materialized data to a catalog.

```python
from rivet_core.plugins import SinkPlugin
from rivet_core.models import Catalog, Joint, Material


class SinkPlugin(ABC):
    @abstractmethod
    def write(self, catalog: Catalog, joint: Joint, material: Material, strategy: str) -> None:
        """Write data to the catalog using the specified write strategy."""
```

### CrossJointAdapter

Declares how a consumer engine accesses upstream data at an engine boundary. Registered for a specific `(consumer_engine_type, producer_engine_type)` pair.

```python
from rivet_core.plugins import CrossJointAdapter, CrossJointContext, UpstreamResolution
from rivet_core.strategies import MaterializedRef


class CrossJointAdapter(ABC):
    consumer_engine_type: str
    producer_engine_type: str

    @abstractmethod
    def resolve_upstream(
        self,
        producer_ref: MaterializedRef,
        consumer_engine: Any,
        joint_context: CrossJointContext,
    ) -> UpstreamResolution:
        """Resolve how the consumer engine accesses upstream data.

        Returns an UpstreamResolution with strategy:
        - "arrow_passthrough": pass Arrow data directly
        - "native_reference": use a catalog table reference (zero-copy)
        - "unsupported": cannot transfer data between these engines
        """
```

### ReferenceResolver

Rewrites abstract table references into engine-native expressions at compile time. Returned by `ComputeEnginePlugin.get_reference_resolver()`.

```python
from rivet_core.plugins import ReferenceResolver


class ReferenceResolver(ABC):
    @abstractmethod
    def resolve_references(
        self,
        sql: str,
        joint: Any,
        catalog: Any,
        compiled_joints: dict[str, Any] | None = None,
        catalog_map: dict[str, Any] | None = None,
        fused_group_joints: list[str] | None = None,
    ) -> str | None:
        """Return rewritten SQL, or None if unchanged.

        fused_group_joints contains joint names in the same fused group
        (CTE aliases that must NOT be replaced with fully-qualified names).
        """
```

---

## Entry Points and Registration

Rivet discovers plugins at startup via Python entry points. There are two registration patterns:

### Monolithic Registration (Recommended)

Register all components through a single function in the `rivet.plugins` entry point group. This is the pattern all built-in plugins use.

The entry point resolves to a callable `plugin_fn(registry: PluginRegistry) -> None` that registers all components at once:

```toml
# pyproject.toml
[project.entry-points."rivet.plugins"]
my_plugin = "rivet_my_plugin:MyPlugin"
```

```python
# rivet_my_plugin/__init__.py
from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rivet_core.plugins import PluginRegistry


def MyPlugin(registry: PluginRegistry) -> None:
    from rivet_my_plugin.catalog import MyCatalogPlugin
    from rivet_my_plugin.engine import MyEnginePlugin

    registry.register_catalog_plugin(MyCatalogPlugin())
    registry.register_engine_plugin(MyEnginePlugin())
```

### Granular Registration

Alternatively, register individual components in separate entry point groups. Each entry point resolves to a plugin class that is instantiated and registered automatically:

| Entry point group | Registration method | ABC |
|---|---|---|
| `rivet.catalogs` | `register_catalog_plugin` | `CatalogPlugin` |
| `rivet.compute_engines` | `register_engine_plugin` | `ComputeEnginePlugin` |
| `rivet.compute_engine_adapters` | `register_adapter` | `ComputeEngineAdapter` |
| `rivet.sources` | `register_source` | `SourcePlugin` |
| `rivet.sinks` | `register_sink` | `SinkPlugin` |
| `rivet.cross_joint_adapters` | `register_cross_joint_adapter` | `CrossJointAdapter` |

```toml
# pyproject.toml — granular registration
[project.entry-points."rivet.catalogs"]
my_catalog = "rivet_my_plugin.catalog:MyCatalogPlugin"

[project.entry-points."rivet.compute_engines"]
my_engine = "rivet_my_plugin.engine:MyEnginePlugin"
```

---

## Complete Example: Minimal CatalogPlugin

A SQLite-backed catalog plugin that stores tables in a single database file.

```python
# rivet_sqlite/catalog.py
from __future__ import annotations

import sqlite3
from typing import Any

from rivet_core.models import Catalog, CatalogNode, ObjectSchema
from rivet_core.plugins import CatalogPlugin


class SQLiteCatalogPlugin(CatalogPlugin):
    type = "sqlite"
    required_options = ["path"]
    optional_options: dict[str, Any] = {}
    credential_options: list[str] = []

    def validate(self, options: dict[str, Any]) -> None:
        if "path" not in options:
            raise ValueError("SQLite catalog requires 'path' option")

    def instantiate(self, name: str, options: dict[str, Any]) -> Catalog:
        return Catalog(name=name, catalog_type=self.type, options=options)

    def default_table_reference(self, logical_name: str, options: dict[str, Any]) -> str:
        return logical_name

    def list_tables(self, catalog: Catalog) -> list[CatalogNode]:
        conn = sqlite3.connect(catalog.options["path"])
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = [CatalogNode(name=row[0], path=[row[0]], node_type="table") for row in cursor]
        conn.close()
        return tables

    def get_schema(self, catalog: Catalog, table: str) -> ObjectSchema:
        conn = sqlite3.connect(catalog.options["path"])
        cursor = conn.execute(f"PRAGMA table_info('{table}')")
        columns = {row[1]: row[2] for row in cursor}
        conn.close()
        return ObjectSchema(columns=columns)

    def test_connection(self, catalog: Catalog) -> None:
        conn = sqlite3.connect(catalog.options["path"])
        conn.execute("SELECT 1")
        conn.close()
```

## Complete Example: Minimal ComputeEnginePlugin

A compute engine plugin that uses SQLite for SQL execution.

```python
# rivet_sqlite/engine.py
from __future__ import annotations

import sqlite3
from typing import Any

import pyarrow

from rivet_core.models import ComputeEngine
from rivet_core.plugins import ComputeEnginePlugin


class SQLiteComputeEnginePlugin(ComputeEnginePlugin):
    engine_type = "sqlite"
    supported_catalog_types: dict[str, list[str]] = {
        "sqlite": ["read", "write"],
    }

    def create_engine(self, name: str, config: dict[str, Any]) -> ComputeEngine:
        return ComputeEngine(name=name, engine_type=self.engine_type, config=config)

    def validate(self, options: dict[str, Any]) -> None:
        pass  # no required engine options

    def execute_sql(
        self,
        engine: ComputeEngine,
        sql: str,
        input_tables: dict[str, pyarrow.Table],
    ) -> pyarrow.Table:
        conn = sqlite3.connect(":memory:")
        # Register input tables
        for table_name, arrow_table in input_tables.items():
            rows = arrow_table.to_pydict()
            if rows:
                cols = list(rows.keys())
                n_rows = len(next(iter(rows.values())))
                conn.execute(
                    f"CREATE TABLE {table_name} ({', '.join(cols)})"
                )
                for i in range(n_rows):
                    values = [rows[c][i] for c in cols]
                    placeholders = ", ".join("?" for _ in cols)
                    conn.execute(
                        f"INSERT INTO {table_name} VALUES ({placeholders})",
                        values,
                    )
        cursor = conn.execute(sql)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        data = cursor.fetchall()
        conn.close()
        arrays = {
            col: [row[i] for row in data] for i, col in enumerate(columns)
        }
        return pyarrow.table(arrays)
```

## Registering the Plugin

```toml
# pyproject.toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "rivet-sqlite"
version = "0.1.0"
description = "SQLite plugin for Rivet"
requires-python = ">=3.11"
dependencies = ["rivet-core>=0.1.0"]

[project.entry-points."rivet.plugins"]
sqlite = "rivet_sqlite:SQLitePlugin"

[tool.hatch.build.targets.wheel]
packages = ["rivet_sqlite"]
```

```python
# rivet_sqlite/__init__.py
from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rivet_core.plugins import PluginRegistry


def SQLitePlugin(registry: PluginRegistry) -> None:
    from rivet_sqlite.catalog import SQLiteCatalogPlugin
    from rivet_sqlite.engine import SQLiteComputeEnginePlugin

    registry.register_catalog_plugin(SQLiteCatalogPlugin())
    registry.register_engine_plugin(SQLiteComputeEnginePlugin())
```

Once installed (`pip install -e .`), Rivet discovers the plugin automatically — no manual registration required.

## Using the Plugin

Configure the plugin in `profiles.yaml`:

```yaml
default:
  engines:
    - name: sqlite_engine
      type: sqlite
      catalogs:
        - my_db
  catalogs:
    - name: my_db
      type: sqlite
      options:
        path: data/warehouse.db
```

Then use it in your pipeline like any other engine and catalog.
