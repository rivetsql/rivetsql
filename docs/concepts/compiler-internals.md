# Compiler Internals

This document provides an exhaustive reference for how the Rivet compiler transforms a declarative `Assembly` (a DAG of Joints) into an executable `CompiledAssembly` through a 10-phase pipeline. Every phase, data model, optimization pass, and decision point is covered.

---

## Overview

The compiler lives in `rivet_core/compiler.py` (~4000 lines). Its single entry point is `compile()`, which internally delegates to a `CompilationPipeline` — a sequential chain of 10 pure-function phases. Each phase reads from and writes to an immutable `PhaseState` accumulator (a frozen dataclass). The pipeline never mutates state; every phase returns a new `PhaseState` via `dataclasses.replace()`.

```
Assembly + Catalogs + Engines + PluginRegistry
    │
    ▼
┌─────────────────────────────────────────────┐
│           CompilationPipeline               │
│                                             │
│  Phase 1:  DAG Pruning                      │
│  Phase 2:  Metadata Resolution              │
│  Phase 3:  Source Introspection             │
│  Phase 4:  SQL Compilation                  │
│  Phase 5:  Fusion                           │
│  Phase 6:  Optimization (Pushdown)          │
│  Phase 7:  Strategy & Reference Resolution  │
│  Phase 8:  Engine Boundary Detection        │
│  Phase 9:  Materialization Determination    │
│  Phase 10: Finalization                     │
│                                             │
└─────────────────────────────────────────────┘
    │
    ▼
CompiledAssembly (immutable, frozen)
```

Key properties of the pipeline:

- **Purity**: Same inputs always produce the same output. No side effects.
- **Error accumulation**: Errors are collected, not raised. Compilation continues through as many phases as possible, producing a `CompiledAssembly` with `success=False` when errors exist.
- **Immutability**: `PhaseState` and all output dataclasses are frozen. Phases extend error/warning tuples via concatenation.
- **Observability**: Per-phase wall-clock timing is recorded in `CompilationStats.phase_durations_ms`. Plugin invocations are annotated via `PluginAnnotation` records.

---

## Public API

### `compile()`

```python
def compile(
    assembly: Assembly,
    catalogs: list[Catalog],
    engines: list[ComputeEngine],
    registry: PluginRegistry,
    profile_name: str | None = None,
    target_sink: str | None = None,
    tags: list[str] | None = None,
    tag_mode: TagMode = "or",
    default_fusion_strategy: FusionStrategyName = "cte",
    default_materialization_strategy: MaterializationStrategyName = "arrow",
    resolve_references: ReferenceResolver | None = None,
    default_engine: str | None = None,
    introspect: bool | None = None,
    introspect_timeout: float | None = None,
    project_root: Path | None = None,
    options: CompileOptions | None = None,
) -> CompiledAssembly
```

This is the main entry point. It builds a `CompileOptions`, constructs the initial `PhaseState`, runs all 10 phases, and returns the final `CompiledAssembly`. The signature is stable and backward-compatible.

### `compile_until()`

```python
def compile_until(
    assembly: Assembly,
    catalogs: list[Catalog],
    engines: list[ComputeEngine],
    registry: PluginRegistry,
    stop_after: str,
    options: CompileOptions | None = None,
) -> PhaseState
```

Runs the pipeline up to and including the named phase, then returns the intermediate `PhaseState`. Useful for testing and debugging individual phases. The `stop_after` parameter accepts any `PHASE_*` constant (e.g. `PHASE_FUSION`, `PHASE_OPTIMIZATION`).

### Phase Constants

```python
PHASE_PRUNE_DAG          = "prune_dag"
PHASE_RESOLVE_METADATA   = "resolve_metadata"
PHASE_INTROSPECT_SOURCES = "introspect_sources"
PHASE_COMPILE_SQL        = "compile_sql"
PHASE_FUSION             = "fusion"
PHASE_OPTIMIZATION       = "optimization"
PHASE_STRATEGY_RESOLUTION = "strategy_resolution"
PHASE_ENGINE_BOUNDARIES  = "engine_boundaries"
PHASE_MATERIALIZATION    = "materialization"
PHASE_FINALIZATION       = "finalization"
```

---

## Inputs to the Compiler

### Assembly

The `Assembly` (`rivet_core/assembly.py`) is a validated DAG of `Joint` nodes. It enforces at construction time:

- Unique joint names (`RVT-301`)
- All upstream references resolve to existing joints (`RVT-302`)
- Source joints have no upstream (`RVT-303`)
- Sink and checkpoint joints have at least one upstream (`RVT-304`)
- The graph is acyclic — detected via DFS coloring (`RVT-305`)

The Assembly provides `topological_order()` which returns a deterministic topological sort (alphabetical tie-breaking) and `subgraph()` which prunes the DAG to a target sink or tag set.

### Joint

A `Joint` (`rivet_core/models.py`) is a declarative node in the DAG. It carries no execution logic — only metadata:

| Field | Description |
|-------|-------------|
| `name` | Globally unique identifier |
| `joint_type` | One of: `source`, `sql`, `sink`, `python`, `checkpoint` |
| `catalog` | Named data domain (e.g. `"local"`, `"warehouse"`) |
| `upstream` | List of upstream joint names |
| `sql` | SQL text (for sql/sink/checkpoint joints, or inline transforms on sources) |
| `engine` | Optional engine override |
| `eager` | Force materialization after this joint |
| `table` | Physical table name for sources/sinks/checkpoints |
| `write_strategy` | `"replace"`, `"append"`, etc. for sinks/checkpoints |
| `function` | `module:callable` path for python joints |
| `dialect` | SQL dialect hint (e.g. `"duckdb"`, `"postgres"`) |
| `fusion_strategy_override` | `"cte"` or `"temp_view"` |
| `materialization_strategy_override` | `"arrow"` or `"temp_table"` |
| `assertions` | Quality checks (assertion/audit phase) |
| `tags` | User-defined tags for filtering |

### Catalog, ComputeEngine, PluginRegistry

- `Catalog`: Immutable named data domain with opaque `options` dict validated by the corresponding `CatalogPlugin`.
- `ComputeEngine`: Named execution backend instance with `engine_type` (e.g. `"duckdb"`, `"pyspark"`) and `config` dict.
- `PluginRegistry`: Central registry of all plugins — catalog plugins, engine plugins, adapters, source/sink plugins, cross-joint adapters. Plugins are discovered via entry points at startup.

---

## The PhaseState Accumulator

`PhaseState` is a frozen dataclass threaded through all 10 phases. Each phase reads fields set by prior phases and returns a new `PhaseState` with its own fields populated. Unpopulated fields remain `None`.

```python
@dataclass(frozen=True)
class PhaseState:
    # ── Inputs (set once at pipeline start) ──
    assembly: Assembly
    catalogs: list[Catalog]
    engines: list[ComputeEngine]
    registry: PluginRegistry
    options: CompileOptions
    catalog_map: dict[str, Catalog]
    engine_map: dict[str, ComputeEngine]

    # ── Phase 1 ──
    pruned: Assembly | None = None

    # ── Phase 2 ──
    topo_order: list[str] | None = None
    joint_metadata: dict[str, ResolvedJointMetadata] | None = None
    adapter_decisions: list[AdapterDecision] | None = None

    # ── Phase 3 ──
    introspection_results: dict[str, IntrospectionRecord] | None = None

    # ── Phase 4 ──
    compiled_joints: list[CompiledJoint] | None = None
    cj_map: dict[str, CompiledJoint] | None = None
    upstream_schemas: dict[str, Schema] | None = None

    # ── Phase 5 ──
    fused_groups: list[FusedGroup] | None = None
    joint_to_group: dict[str, str] | None = None

    # ── Phase 8 ──
    engine_boundaries: list[EngineBoundary] | None = None

    # ── Phase 9 ──
    materializations: list[Materialization] | None = None

    # ── Phase 10 ──
    compiled_assembly: CompiledAssembly | None = None

    # ── Subgraph poisoning ──
    poisoned_joints: frozenset[str] = frozenset()

    # ── Accumulated diagnostics ──
    errors: tuple[RivetError, ...] = ()
    warnings: tuple[str, ...] = ()
    phase_timings: dict[str, float] = field(default_factory=dict)
    plugin_annotations: list[PluginAnnotation] = field(default_factory=list)
    completed_phases: tuple[str, ...] = ()

    # ── Introspection counters ──
    introspection_attempted: int = 0
    introspection_succeeded: int = 0
    introspection_failed: int = 0
    introspection_skipped: int = 0
```

Errors and warnings are immutable tuples. Each phase extends them via concatenation (`(*state.errors, *new_errors)`), guaranteeing monotonic growth — no phase can accidentally discard diagnostics from an earlier phase.

---

## The CompilationPipeline

The `CompilationPipeline` orchestrates phases sequentially. Each phase implements the `CompilerPhase` protocol:

```python
class CompilerPhase(Protocol):
    @property
    def name(self) -> str: ...
    def __call__(self, state: PhaseState) -> PhaseState: ...
```

The pipeline records per-phase wall-clock timing and emits a `DEBUG` log after each phase transition:

```python
class CompilationPipeline:
    def run(self, initial_state: PhaseState, stop_after: str | None = None) -> PhaseState:
        state = initial_state
        for phase in self._phases:
            t0 = time.monotonic()
            state = phase(state)
            duration_ms = (time.monotonic() - t0) * 1000
            state = replace(state,
                phase_timings={**state.phase_timings, phase.name: duration_ms},
                completed_phases=(*state.completed_phases, phase.name),
            )
            if stop_after and phase.name == stop_after:
                break
        return state
```

The default pipeline instance `_DEFAULT_PIPELINE` chains all 10 phases in order.

---

## Phase 1: DAG Pruning (`prune_dag`)

**Class**: `_PruneDagPhase`
**Plugin calls**: None (core-only)
**Input**: `PhaseState.assembly`, `CompileOptions.target_sink`, `CompileOptions.tags`, `CompileOptions.tag_mode`
**Output**: `PhaseState.pruned` (a sub-Assembly)

This phase reduces the full Assembly DAG to only the joints needed for the requested target. It calls `assembly.subgraph(target_sink, tags, tag_mode)`.

**Behavior**:

- If `target_sink` is specified, the subgraph includes only the target sink and all its transitive upstream dependencies.
- If `tags` are specified, joints are filtered by tag membership. `tag_mode="or"` includes joints matching any tag; `tag_mode="and"` requires all tags.
- If neither is specified, the full assembly is used.

**Error handling**: If the target sink doesn't exist or the tag filter produces an empty graph, a `RVT-306` error is recorded and the phase returns early. This is the only phase that can cause early pipeline termination — all subsequent phases check `if state.pruned is None` and skip.

---

## Phase 2: Metadata Resolution (`resolve_metadata`)

**Class**: `_ResolveMetadataPhase`
**Plugin calls**: `registry.get_catalog_plugin()`, `registry.get_engine_plugin()`, `registry.get_adapter()`
**Input**: `PhaseState.pruned`
**Output**: `PhaseState.topo_order`, `PhaseState.joint_metadata`, `PhaseState.adapter_decisions`

This phase computes the topological order of the pruned DAG and resolves per-joint metadata: which catalog, engine, and adapter each joint will use.

### Topological Order

Calls `pruned.topological_order()` which uses Kahn's algorithm with alphabetical tie-breaking for determinism. The result is a list of joint names where every joint appears after all its upstreams.

### Engine Resolution

For each joint, `_resolve_engine()` determines the engine using this priority:

1. `joint.engine` — explicit override → resolution source: `"joint_override"`
2. `CompileOptions.default_engine` — profile-level default → `"project_default"`
3. No engine found → `RVT-401` error

The function returns `(engine_name, engine_type, resolution_source)`.

### Catalog & Adapter Resolution

For each joint with a catalog:

1. Look up the `Catalog` object from `catalog_map`
2. Get the `CatalogPlugin` from the registry for the catalog's type
3. Resolve the adapter for the `(engine_type, catalog_type)` pair via `_resolve_adapter()`

Adapter resolution uses a cache to avoid redundant registry lookups:

- **Exact match**: `registry.get_adapter(engine_type, catalog_type)` returns a `ComputeEngineAdapter` → adapter key is `"engine_type:catalog_type"`
- **Wildcard fallback**: If no exact adapter exists, `registry.resolve_capabilities()` checks if a wildcard adapter provides Arrow-compatible capabilities
- **No adapter**: `RVT-402` error — the engine doesn't support this catalog type

### AdapterDecision Traceability

Every adapter lookup produces an `AdapterDecision` record:

```python
@dataclass(frozen=True)
class AdapterDecision:
    joint_name: str
    engine_type: str
    catalog_type: str | None
    adapter_found: str | None          # e.g. "duckdb:filesystem"
    resolution_method: str             # "exact_match", "wildcard_fallback", "none"
    available_for_engine: list[str]    # all adapters for this engine_type
    available_for_catalog: list[str]   # all adapters for this catalog_type
    is_cross_joint: bool = False
    producer_engine_type: str | None = None
    consumer_engine_type: str | None = None
```

### PluginAnnotation

Each catalog plugin and engine plugin access is recorded as a `PluginAnnotation` with the phase name, joint name, plugin type, class name, operation, and result.

### ResolvedJointMetadata

The per-joint result is stored as:

```python
@dataclass(frozen=True)
class ResolvedJointMetadata:
    catalog: Catalog | None
    catalog_type: str | None
    catalog_plugin: CatalogPlugin | None
    engine_name: str
    engine_type: str
    resolution: EngineResolutionSource | None  # "joint_override" | "project_default" | ""
    adapter_name: str | None
```

### Subgraph Poisoning

When a joint fails engine resolution (`RVT-401`) or adapter resolution (`RVT-402`), it is added to the `poisoned_joints` set. Any downstream joint whose upstream includes a poisoned joint is also poisoned — without attempting resolution — preventing cascading errors (e.g. `RVT-401` on `raw_orders` would otherwise trigger secondary `RVT-401` and `RVT-762` errors on every downstream joint like `daily_revenue`).

Poisoned joints receive a placeholder `ResolvedJointMetadata` (all fields empty/`None`) so that later phases can safely look them up without `KeyError`. Phase 4 (`compile_sql`) and Phase 10 (`finalization`) skip poisoned joints entirely, and the `poisoned_joints` frozenset is propagated through the pipeline via `PhaseState`.

---

## Phase 3: Source Introspection (`introspect_sources`)

**Class**: `_IntrospectSourcesPhase`
**Plugin calls**: `catalog_plugin.get_schema()`, `catalog_plugin.get_metadata()`
**Input**: `PhaseState.pruned`, `PhaseState.topo_order`, `PhaseState.joint_metadata`
**Output**: `PhaseState.introspection_results`, introspection counters

This phase retrieves schema and table-level statistics from catalog plugins for all source joints. Introspection is best-effort — failures never block compilation.

### Parallel Execution

Source introspections are submitted concurrently to a `ThreadPoolExecutor` (up to 8 workers). Each source joint's introspection runs in its own thread with a configurable timeout (`CompileOptions.introspect_timeout`, default 5 seconds).

### Per-Source Flow

For each source joint:

1. Check if the joint has a catalog and catalog plugin. If not → `"skipped"`.
2. Submit `_do_introspect()` to the thread pool.
3. `_do_introspect()` calls:
   - `catalog_plugin.get_schema(catalog, table_name)` → returns `ObjectSchema` with columns
   - `catalog_plugin.get_metadata(catalog, table_name)` → returns `ObjectMetadata` with row count, size, last modified, partition count
4. Collect the result with timeout handling.

### Table Map Resolution

Before introspection, `_resolve_table_map_name()` checks the catalog's `table_map` option for alias resolution. This ensures introspection, compilation, and execution all see the same physical table name.

### IntrospectionRecord

Each source gets exactly one record:

```python
@dataclass(frozen=True)
class IntrospectionRecord:
    joint_name: str
    catalog_type: str | None
    catalog_plugin_class: str | None   # e.g. "FilesystemCatalogPlugin"
    result: str                        # "success", "failed", "timeout", "skipped"
    duration_ms: float
    schema_obtained: bool
    stats_obtained: bool
    error_message: str | None = None
```

### SourceStats

When metadata is available:

```python
@dataclass(frozen=True)
class SourceStats:
    row_count: int | None = None
    size_bytes: int | None = None
    last_modified: datetime | None = None
    partition_count: int | None = None
```

### Skip Conditions

If `CompileOptions.introspect` is `False`, the entire phase is skipped and all sources are counted as `introspection_skipped`.

---

## Phase 4: SQL Compilation (`compile_sql`)

**Class**: `_CompileSQLPhase`
**Plugin calls**: `registry.get_engine_plugin()` (for dialect)
**Input**: `PhaseState.pruned`, `PhaseState.topo_order`, `PhaseState.joint_metadata`, `PhaseState.introspection_results`
**Output**: `PhaseState.compiled_joints`, `PhaseState.cj_map`, `PhaseState.upstream_schemas`

This is the most complex phase. It iterates joints in topological order and compiles each one into a `CompiledJoint` — the enriched, immutable representation that carries all resolved metadata.

### Per-Joint Compilation

The dispatcher `_compile_joint_with_context()` handles each joint type differently:

#### Source Joints

1. **Table map resolution**: `_resolve_table_map_name()` applies catalog `table_map` aliases.
2. **Source SQL analysis** (`_analyze_source_sql()`): If the source has inline SQL (e.g. `SELECT id, name FROM __self WHERE active = true`):
   - Parses the SQL via `SQLParser.parse()`
   - Rewrites `__self` and `joint.name` references to the physical table name
   - Extracts a `LogicalPlan` for pushdown optimization
3. **Introspection attachment**: Attaches schema and stats from Phase 3 results.
4. **Inline transform validation** (`_validate_source_inline_transforms()`):
   - Enforces single-table constraint: no JOINs (`RVT-760`), no CTEs (`RVT-761`), no subqueries (`RVT-762`)
   - Warns about column references not found in the introspected schema
   - Computes the transformed output schema from projections

#### SQL / Sink / Checkpoint Joints

`_compile_sql_joint()` performs:

1. **SQL parsing**: `SQLParser.parse(sql, dialect)` → sqlglot AST
2. **Normalization**: `SQLParser.normalize(ast)` — flattens nested ANDs, standardizes expressions
3. **Table reference extraction**: `SQLParser.extract_table_references(ast)`
4. **Logical plan extraction**: `SQLParser.extract_logical_plan(ast)` → `LogicalPlan` containing:
   - `source_tables`: `TableReference` list (name, alias, type)
   - `projections`: `Projection` list (expression, alias, source_columns)
   - `predicates`: `Predicate` list (expression, columns, location: "where"/"having")
   - `joins`: `Join` list (type, table, condition, columns)
   - `aggregations`: `Aggregation` (group_by columns)
   - `limit`: `Limit` (count, offset)
   - `ordering`: `Ordering` (columns with direction)
   - `distinct`: bool
5. **Schema inference**: `SQLParser.infer_schema(ast, upstream_schemas)` — uses sqlglot's optimizer to infer output column types from upstream schemas
6. **Column lineage extraction**: `SQLParser.extract_lineage(ast, upstream_schemas)` → `ColumnLineage` list mapping each output column to its source origins with transform classification (`"source"`, `"direct"`, `"renamed"`, `"expression"`, `"aggregation"`, `"window"`, `"literal"`, `"multi_column"`, `"opaque"`)
7. **Dialect translation**: If the joint's SQL dialect differs from the engine's dialect, `SQLParser.translate(ast, source_dialect, target_dialect)` transpiles the SQL (e.g. Postgres → DuckDB)

#### Python Joints

`_compile_python_joint()`:

1. Validates the `function` path is importable via `_verify_callable()` (temporarily adds `project_root` to `sys.path`)
2. Produces opaque lineage: `ColumnLineage(output_column="*", transform="opaque", origins=[...all upstreams...])`

#### Quality Checks

`_compile_checks()` converts `Assertion` objects to `CompiledCheck` objects, validating that audit-phase checks only appear on sink joints (`RVT-651`).

### Post-Loop Processing

After all joints are compiled:

1. **Sink schema inference** (`_infer_sink_schemas()`): Sinks inherit upstream schemas. Single upstream → copy. Multiple identical → use shared. Conflicting → `None` + warning.
2. **Schema confidence assignment** (`_assign_schema_confidence()`): Each joint gets a confidence level:
   - `"introspected"` — source with successful catalog introspection
   - `"inferred"` — SQL joint where all upstreams have schemas
   - `"partial"` — some upstream schemas missing
   - `"none"` — no schema information (python joints, failed introspection)
3. **Checkpoint warning**: Checkpoint joints with no downstream consumers get a warning.

### The CompiledJoint

The output per joint:

```python
@dataclass(frozen=True)
class CompiledJoint:
    name: str
    type: JointType
    catalog: str | None
    catalog_type: str | None
    engine: str
    engine_resolution: EngineResolutionSource | None
    adapter: str | None
    sql: str | None                    # original user SQL
    sql_translated: str | None         # after dialect translation
    sql_resolved: str | None           # after reference resolution (Phase 7)
    sql_dialect: str | None
    engine_dialect: str | None
    upstream: list[str]
    eager: bool
    table: str | None
    write_strategy: str | None
    function: str | None
    source_file: str | None
    logical_plan: LogicalPlan | None
    output_schema: Schema | None
    column_lineage: list[ColumnLineage]
    optimizations: list[OptimizationResult]
    checks: list[CompiledCheck]
    fused_group_id: str | None         # set in Phase 10
    tags: list[str]
    description: str | None
    fusion_strategy_override: str | None
    materialization_strategy_override: str | None
    source_stats: SourceStats | None
    schema_confidence: SchemaConfidence
    execution_sql: str | None          # final SQL for engine (set in Phase 10)
```

---

## Phase 5: Fusion (`fusion`)

**Class**: `_FusionPhase`
**Plugin calls**: `registry.get_engine_plugin()` (for `supports_native_assertions`)
**Input**: `PhaseState.compiled_joints`, `PhaseState.cj_map`
**Output**: `PhaseState.fused_groups`, `PhaseState.joint_to_group`

Fusion merges adjacent compatible joints into `FusedGroup` instances so they execute as a single SQL statement. This is a pure algorithmic pass implemented in `rivet_core/optimizer.py`.

### FusionJoint

Before calling the optimizer, the compiler builds lightweight `FusionJoint` views:

```python
@dataclass(frozen=True)
class FusionJoint:
    name: str
    joint_type: str
    upstream: list[str]
    engine: str
    engine_type: str
    adapter: str | None
    eager: bool
    has_assertions: bool
    sql: str | None
```

The `has_assertions` flag is set to `False` when the engine supports native assertions and all assertion checks are SQL-translatable — this prevents unnecessary fusion breaks.

### Fusion Eligibility (`_can_fuse`)

A downstream joint can fuse with an upstream joint when ALL of these hold:

1. Same engine instance
2. Upstream is not `eager`
3. Upstream has no assertions (or assertions are engine-native)
4. Upstream has exactly one downstream consumer (single-consumer rule) — relaxed when all consumers of the upstream are the same multi-input joint
5. Neither joint is a Python joint
6. Upstream is not a checkpoint joint

### Group Assignment Algorithm (`_assign_fusion_groups`)

Joints are processed in topological order. For each joint:

- **Single upstream**: If fusible, join the upstream's group.
- **Multiple upstreams**: Collect all fusible upstream groups. If multiple eligible groups exist, merge them into one (the first eligible group absorbs the others). The joint joins the merged group.
- **No fusible upstream**: Create a new standalone group with a deterministic UUID (MD5 of joint name).

### SQL Composition

Once groups are assigned, SQL is composed using one of two strategies:

#### CTE Strategy (default)

Chains upstream joints as `WITH` clauses; the last joint's SQL becomes the final `SELECT`:

```sql
WITH filter_orders AS (
    SELECT * FROM raw_orders WHERE status = 'completed'
),
daily_revenue AS (
    SELECT order_date, SUM(amount) AS revenue
    FROM filter_orders
    GROUP BY order_date
)
SELECT * FROM daily_revenue
```

If a joint's SQL already contains `WITH` clauses, the inner CTEs are extracted and merged into the outer CTE chain to avoid nested `WITH` blocks.

#### Temp View Strategy

Creates `CREATE TEMPORARY VIEW` statements for intermediates:

```sql
CREATE TEMPORARY VIEW filter_orders AS (SELECT * FROM raw_orders WHERE status = 'completed');
CREATE TEMPORARY VIEW daily_revenue AS (SELECT order_date, SUM(amount) AS revenue FROM filter_orders GROUP BY order_date);
SELECT * FROM daily_revenue
```

### Entry and Exit Joints

Each group tracks:

- **Entry joints**: Joints whose upstreams are all outside the group (or have no upstream). These are the data ingestion points.
- **Exit joints**: Joints with no downstream within the group. These produce the group's output.

### FusedGroup

```python
@dataclass(frozen=True)
class FusedGroup:
    id: str
    joints: list[str]              # joint names in execution order
    engine: str                    # engine instance name
    engine_type: str
    adapters: dict[str, str | None]  # joint name → adapter name
    fused_sql: str | None          # composed CTE/temp_view SQL
    fusion_strategy: str           # "cte" or "temp_view"
    fusion_result: FusionResult | None
    resolved_sql: str | None       # after reference resolution
    entry_joints: list[str]
    exit_joints: list[str]
    pushdown: PushdownPlan | None
    residual: ResidualPlan | None
    materialization_strategy_name: str
    per_joint_predicates: dict[str, list[Predicate]]
    per_joint_projections: dict[str, list[str]]
    per_joint_limits: dict[str, int]
    checkpoint_sources: dict[str, CheckpointSourceInfo]
```

---

## Phase 6: Optimization (`optimization`)

**Class**: `_OptimizationPhase`
**Plugin calls**: `registry.resolve_capabilities()` (for adapter capabilities)
**Input**: `PhaseState.compiled_joints`, `PhaseState.cj_map`, `PhaseState.fused_groups`
**Output**: Updated `PhaseState.fused_groups` and `PhaseState.cj_map` with pushdown plans

This phase applies two optimization passes from `rivet_core/optimizer.py`:

### Pass 1: Intra-Group Pushdown (`pushdown_pass`)

For each fused group, analyzes the exit joint's `LogicalPlan` and determines what can be pushed down to the entry joints' adapters.

#### Predicate Pushdown

Examines `WHERE` predicates from the logical plan:

1. Split compound `AND` predicates into independent conjuncts
2. Skip `HAVING` predicates (never pushed)
3. Skip predicates containing subqueries (`SELECT`, `EXISTS`, `IN (SELECT ...)`)
4. Check adapter capabilities: requires `"predicate_pushdown"` in the adapter's capability list
5. Push eligible predicates to the entry joint's adapter

Result: `PredicatePushdownResult(pushed=[...], residual=[...])`

#### Projection Pushdown

Collects all columns referenced anywhere in the logical plan (projections, predicates, joins, aggregations, ordering):

1. Skip if `SELECT *` is present
2. Check adapter capabilities: requires `"projection_pushdown"`
3. Push the column list to the entry joint's adapter

Result: `ProjectionPushdownResult(pushed_columns=[...] | None, reason=...)`

#### Limit Pushdown

Examines the `LIMIT` clause:

1. Blocked if aggregations, joins, or `DISTINCT` are present (these change row counts)
2. Check adapter capabilities: requires `"limit_pushdown"`
3. Push the limit value to the entry joint's adapter

Result: `LimitPushdownResult(pushed_limit=N | None, residual_limit=N | None, reason=...)`

#### Cast Pushdown

Extracts `CAST(column AS type)` expressions from projections:

1. Only widening numeric casts and any-to-string casts are safe to push
2. Check adapter capabilities: requires `"cast_pushdown"`

Result: `CastPushdownResult(pushed=[...], residual=[...])`

#### PushdownPlan and ResidualPlan

Each group gets:

```python
@dataclass(frozen=True)
class PushdownPlan:
    predicates: PredicatePushdownResult
    projections: ProjectionPushdownResult
    limit: LimitPushdownResult
    casts: CastPushdownResult

@dataclass(frozen=True)
class ResidualPlan:
    predicates: list[Predicate]   # applied post-materialization
    limit: int | None
    casts: list[Cast]
```

### Pass 2: Cross-Group Pushdown (`cross_group_pushdown_pass`)

This pass propagates optimizations across fused-group boundaries — from consumer groups to upstream source groups. It operates on three dimensions:

#### Cross-Group Predicate Pushdown

For each consumer group's exit joint with `WHERE` predicates:

1. **Split**: Break compound `AND` predicates into conjuncts
2. **Classify**: Check if the conjunct is pushable (not `HAVING`, no subqueries, lineage transform is `"direct"` or `"renamed"`)
3. **Trace origins**: Walk column lineage backward through the DAG to find the ultimate source joint. Uses `_resolve_conjunct_origins()` which handles table-qualified columns via the logical plan's alias map.
4. **Single-origin check**: The predicate must trace to exactly one source joint (multi-origin predicates are skipped)
5. **Capability check**: The target source group's adapter must support `"predicate_pushdown"`
6. **Rewrite**: `_rewrite_predicate_for_source()` strips table aliases and applies column renames from lineage
7. **Push**: Store the rewritten predicate in `per_joint_predicates` on the target group

**Join-equality propagation**: When a predicate column participates in an `INNER JOIN` equality (`A.col = B.col`), the pass derives an equivalent predicate for the other side of the join and pushes it to that source too. This is implemented in `_derive_join_equality_predicates()`.

#### Cross-Group Projection Pushdown

1. Collect all columns referenced in the consumer's `LogicalPlan`
2. Map each column through lineage to its source joint
3. Store the mapped column lists in `per_joint_projections` on upstream source groups
4. Ensure columns from pushed predicates (including join-equality derived ones) are included in projections

#### Cross-Group Limit Pushdown

1. Extract the consumer's `LIMIT` clause
2. Safety guards: blocked by aggregations, joins, `DISTINCT`, multiple upstream source groups, or residual predicates
3. Store the limit in `per_joint_limits` on the upstream source group

### OptimizationResult

Every optimization decision (applied, skipped, not_applicable, capability_gap) is recorded:

```python
@dataclass(frozen=True)
class OptimizationResult:
    rule: str                  # e.g. "cross_group_predicate_pushdown"
    status: OptimizationStatus # "applied", "not_applicable", "capability_gap", "skipped"
    detail: str
    pushed: str | None
    residual: str | None
    target_joint: str | None
    target_group: str | None
```

These are attached to `CompiledJoint.optimizations` for observability.

---

## Phase 7: Strategy & Reference Resolution (`strategy_resolution`)

**Class**: `_StrategyResolutionPhase`
**Plugin calls**: `engine_plugin.get_reference_resolver()`, `resolver.resolve_references()`
**Input**: `PhaseState.fused_groups`, `PhaseState.cj_map`, `PhaseState.compiled_joints`, `PhaseState.joint_to_group`
**Output**: Updated `PhaseState.fused_groups` and `PhaseState.cj_map`

This phase has four sub-steps: strategy resolution, reference resolution, checkpoint source building, and checkpoint CTE injection.

### Sub-step 1: Strategy Resolution (`_resolve_strategy`)

Validates and resolves fusion and materialization strategy overrides per group:

1. Collect `fusion_strategy_override` values from all joints in the group
2. If multiple conflicting overrides exist → `RVT-603` error
3. If a single override exists, use it; otherwise use `default_fusion_strategy`
4. Validate against `{"cte", "temp_view"}` → `RVT-601` on invalid
5. If the resolved strategy differs from the group's current strategy, recompose the SQL using the appropriate composer (`_compose_cte` or `_compose_temp_view`)
6. Validate `materialization_strategy_override` per joint against `{"arrow", "temp_table"}` → `RVT-602` on invalid

### Sub-step 2: Reference Resolution (`_resolve_references`)

Transforms SQL table references into engine-native, catalog-qualified expressions. For example, a DuckDB filesystem catalog might rewrite `FROM orders` to `FROM read_parquet('/data/orders/*.parquet')`.

The process:

1. For each fused group, obtain the `ReferenceResolver` for the group's engine type:
   - If an explicit `resolve_references` was passed to `compile()`, use it for all groups (backward compat)
   - Otherwise, call `engine_plugin.get_reference_resolver()` per engine type (cached)
2. For each joint in the group with SQL:
   - Skip source joints in single-joint groups (no self-referencing CTEs needed)
   - Skip non-SQL joint types (python)
   - Call `resolver.resolve_references(input_sql, compiled_joint, compiled_catalog, ...)`
   - If the resolved SQL differs from the input, store it in `CompiledJoint.sql_resolved`
3. If any joint in the group was resolved, recompose the group's fused SQL using the resolved per-joint SQL. The result is stored in `FusedGroup.resolved_sql` and `FusionResult.resolved_fused_sql`.

The `ReferenceResolver` protocol:

```python
class ReferenceResolver(Protocol):
    def resolve_references(
        self,
        sql: str,
        joint: CompiledJoint,
        catalog: CompiledCatalog | None,
        compiled_joints: dict[str, CompiledJoint],
        catalog_map: dict[str, Catalog],
        fused_group_joints: list[str],
    ) -> str | None: ...
```

Each engine plugin provides its own resolver. For example, the DuckDB engine plugin rewrites filesystem catalog references to `read_parquet()`/`read_csv()` calls, while the Databricks plugin rewrites to Unity Catalog three-part names (`catalog.schema.table`).

### Sub-step 3: Checkpoint Source Building (`_build_checkpoint_sources`)

For each fused group, finds upstream joints that are checkpoints (in other groups) and pre-resolves the adapter metadata needed to read them back:

1. Iterate all joints in the group (not just entry joints — a joint can have upstream both inside and outside the group)
2. For each upstream that is a checkpoint in a different group:
   - Resolve the adapter for `(group.engine_type, checkpoint.catalog_type)`
   - If no adapter found → warning (will fall back to SourcePlugin or Arrow passthrough at runtime)
3. Store as `CheckpointSourceInfo` on the group:

```python
@dataclass(frozen=True)
class CheckpointSourceInfo:
    checkpoint_joint: str
    catalog: str
    catalog_type: str
    table: str
    adapter: str | None   # e.g. "duckdb:filesystem"
```

### Sub-step 4: Checkpoint CTE Injection (`_inject_checkpoint_ctes`)

For groups that consume checkpoints from other groups, prepends CTE definitions so the downstream SQL can reference the checkpoint by name:

1. For each checkpoint source, try engine-native resolution first:
   - Build `SELECT * FROM <checkpoint_name>` and pass it through the engine's `ReferenceResolver`
   - The resolver rewrites it to the engine-native expression (e.g. `read_parquet('/data/checkpoint_table/*.parquet')`)
2. If no resolver or resolution fails, fall back to a database-style fully-qualified name built by `_build_checkpoint_fq_name()` (e.g. `catalog.schema.table`)
3. Prepend the CTE definitions to the group's `fused_sql`, `resolved_sql`, and `fusion_result` fields using `_prepend_ctes()`

Example injected CTE:

```sql
WITH my_checkpoint AS (
    SELECT * FROM read_parquet('/data/warehouse/my_checkpoint/*.parquet')
),
-- ... existing CTEs ...
```

---

## Phase 9: Materialization Determination (`materialization`)

**Class**: `_MaterializationPhase`
**Plugin calls**: `registry.get_engine_plugin()` (for `supports_native_assertions`)
**Input**: `PhaseState.cj_map`, `PhaseState.joint_to_group`, `PhaseState.engine_boundaries`
**Output**: `PhaseState.materializations`, updated `PhaseState.cj_map`

This phase determines where intermediate results must be materialized (converted from a deferred SQL plan to a physical Arrow table or temp table). Materialization is required at certain boundaries to ensure correctness.

It runs after the Engine Boundary phase so it can use the validated `engine_boundaries` to detect engine-instance changes instead of performing its own ad-hoc engine-name comparison.

### Materialization Triggers

For each joint, the compiler examines its downstream consumers and checks for materialization triggers in this priority order:

| Trigger | Condition | Detail |
|---------|-----------|--------|
| `checkpoint_boundary` | Joint is a checkpoint | Checkpoints always materialize to persist data |
| `eager` | `joint.eager = True` | User explicitly requested materialization |
| `python_boundary` | Downstream is a Python joint | Python joints need Arrow tables as input |
| `assertion_boundary` | Joint has assertion checks AND engine doesn't support native assertions | Must materialize to run assertion SQL |
| `multi_consumer` | Joint has >1 downstream consumer | Avoid re-executing the same query multiple times |
| `engine_instance_change` | Joint is in the `boundary_joints` set from Phase 8 | Data must cross engine boundaries |
| `capability_gap` | Joint and downstream are in different fused groups | Groups execute independently |

### Native Assertion Optimization

Before checking the `assertion_boundary` trigger, the compiler checks if the engine can handle assertions natively:

1. Get the engine plugin for the joint's engine type
2. Check `plugin.supports_native_assertions`
3. Check if all assertion-phase checks are SQL-translatable (via `_is_sql_translatable()`)
4. If both conditions hold → suppress the `assertion_boundary` trigger and record an `OptimizationResult` with rule `"assertion_boundary_suppressed"` and status `"applied"`
5. If not → record with status `"not_applicable"` and the reason

### Materialization Strategy

Each materialization point gets a strategy:

- `"arrow"` — default. In-memory Arrow table via `ArrowMaterialization`. Zero-copy, fast, but memory-bound.
- `"temp_table"` — engine-native temporary table. Useful for large datasets that shouldn't live in memory.

Checkpoint boundaries always use `"arrow"`. Other triggers respect `joint.materialization_strategy_override` or fall back to `default_materialization_strategy`.

### Materialization Record

```python
@dataclass(frozen=True)
class Materialization:
    from_joint: str                    # producer joint
    to_joint: str                      # consumer joint
    trigger: MaterializationTrigger    # why materialization is needed
    detail: str                        # human-readable explanation
    strategy: MaterializationStrategyName  # "arrow" or "temp_table"
```

---

## Phase 8: Engine Boundary Detection (`engine_boundaries`)

**Class**: `_EngineBoundaryPhase`
**Plugin calls**: `registry.get_cross_joint_adapter()`, `registry.get_engine_plugin()`, `registry.get_adapter()`
**Input**: `PhaseState.fused_groups`, `PhaseState.cj_map`, `PhaseState.joint_to_group`
**Output**: `PhaseState.engine_boundaries`, updated `PhaseState.fused_groups`, `PhaseState.adapter_decisions`

This phase detects where the engine type changes between adjacent fused groups and resolves the cross-engine transfer strategy.

### Engine Boundary Detection (`_detect_engine_boundaries`)

1. For each group, examine entry joints' upstream references
2. If an upstream joint belongs to a different group with a different `engine_type` → engine boundary
3. Look up a `CrossJointAdapter` for the `(consumer_engine_type, producer_engine_type)` pair
4. If no adapter found → warning `RVT-504`, use default arrow passthrough

### CrossJointAdapter

The `CrossJointAdapter` protocol handles data transfer between engines:

```python
class CrossJointAdapter(Protocol):
    consumer_engine_type: str
    producer_engine_type: str

    def resolve_upstream(
        self,
        context: CrossJointContext,
        upstream: UpstreamResolution,
    ) -> UpstreamResolution: ...
```

### EngineBoundary Record

```python
@dataclass(frozen=True)
class EngineBoundary:
    producer_group_id: str
    consumer_group_id: str
    producer_engine_type: str
    consumer_engine_type: str
    boundary_joints: list[str]
    adapter_strategy: str | None   # class name or "default: arrow_passthrough"
```

### Materialization Strategy Resolution per Group

This phase also resolves the `materialization_strategy_name` for each fused group:

1. Check if any joint in the group has a `materialization_strategy_override`
2. If not, check the engine plugin's `materialization_strategy_name` property
3. Fall back to `"arrow"`

### AdapterDecision for Cross-Joint Adapters

Each cross-engine boundary produces an `AdapterDecision` with `is_cross_joint=True`, recording the producer and consumer engine types and the resolution method.

---

## Phase 10: Finalization (`finalization`)

**Class**: `_FinalizationPhase`
**Plugin calls**: None (core-only)
**Input**: All prior phase outputs
**Output**: `PhaseState.compiled_assembly`

This phase assembles the final `CompiledAssembly` from all accumulated state.

### Execution SQL Resolution

For each fused group, `resolve_execution_sql()` (`rivet_core/sql_resolver.py`) determines the final SQL string that will be sent to the engine:

1. **Prefer resolved SQL** (from reference resolution) when available AND the group has no materialized inputs from an engine boundary. Resolved SQL contains fully-qualified catalog references.
2. **Rewrite adapter-read sources**: If the group has source joints that read via adapters, rewrite their CTE bodies to `SELECT * FROM <name>` so the engine reads from registered Arrow input tables.
3. **Fall back** through `fusion_result.fused_sql` → `group.fused_sql`.

When a group receives materialized inputs from an engine boundary (`has_materialized_inputs=True`), resolved SQL is skipped because the receiving engine works with in-memory tables registered by joint name, not catalog-qualified references.

### Final Joint Assembly

Each `CompiledJoint` is updated with:

- `fused_group_id`: The ID of the fused group it belongs to
- `execution_sql`: The final SQL from `resolve_execution_sql()`

### Execution Order

A topologically sorted list of fused group IDs. Built by iterating joints in `topo_order` and collecting the first occurrence of each group ID:

```python
execution_order: list[str] = []
seen_groups: set[str] = set()
for jn in topo_order:
    gid = joint_to_group.get(jn)
    if gid and gid not in seen_groups:
        seen_groups.add(gid)
        execution_order.append(gid)
```

The executor follows this list exactly — no re-resolution at runtime.

### Parallel Execution Plan

`_compute_parallel_execution_plan()` uses wavefront analysis to determine which groups can execute concurrently:

1. Build upstream/downstream edges between fused groups
2. Compute in-degree for each group
3. Groups with in-degree 0 → wave 1
4. Remove wave 1 groups, find new in-degree 0 groups → wave 2
5. Repeat until all groups are assigned

Each wave is an `ExecutionWave`:

```python
@dataclass(frozen=True)
class ExecutionWave:
    wave_number: int
    groups: list[str]                    # fused group IDs
    engines: dict[str, list[str]]        # engine_name → group_ids on that engine
```

### Compiled Catalogs, Engines, Adapters

Built from the compiled joints:

- `CompiledCatalog`: Name, type, options for each catalog used by at least one joint
- `CompiledEngine`: Name, engine_type, native_catalog_types (from engine plugin)
- `CompiledAdapter`: Engine type, catalog type, source for each adapter used

Checkpoint-downstream adapter pairs are also included.

### CompilationStats

```python
@dataclass(frozen=True)
class CompilationStats:
    compile_duration_ms: int
    joints_with_schema: int
    joints_total: int
    introspection_attempted: int
    introspection_succeeded: int
    introspection_failed: int
    introspection_skipped: int
    phase_durations_ms: dict[str, float]  # per-phase wall-clock timing
```

### The Final CompiledAssembly

```python
@dataclass(frozen=True)
class CompiledAssembly:
    success: bool
    profile_name: str
    catalogs: list[CompiledCatalog]
    engines: list[CompiledEngine]
    adapters: list[CompiledAdapter]
    joints: list[CompiledJoint]
    fused_groups: list[FusedGroup]
    materializations: list[Materialization]
    execution_order: list[str]
    diagnostics: CompilationDiagnostics
    engine_boundaries: list[EngineBoundary]
    parallel_execution_plan: list[ExecutionWave]
    adapter_decisions: list[AdapterDecision]
    introspection_records: list[IntrospectionRecord]
    plugin_annotations: list[PluginAnnotation]
```

---

## The SQL Parser (`rivet_core/sql_parser.py`)

The `SQLParser` class wraps [sqlglot](https://github.com/tobymao/sqlglot) to provide SQL parsing, normalization, logical plan extraction, schema inference, column lineage, and dialect translation.

### Parsing

`parse(sql, dialect)` parses SQL text into a sqlglot AST. Applies pre-processing rewrites:

- `_normalize_iff()`: Rewrites DuckDB's `IFF(cond, a, b)` to standard `CASE WHEN cond THEN a ELSE b END`
- `_rewrite_map_unnest_for_duckdb()`: Rewrites `MAP` and `UNNEST` expressions for DuckDB compatibility

After parsing, `_validate_select()` ensures the top-level statement is a `SELECT` (not `INSERT`, `UPDATE`, etc.).

### Normalization

`normalize(ast)` standardizes the AST:

- Flattens nested `AND` expressions into a flat conjunction list
- Standardizes expression formatting

### Logical Plan Extraction

`extract_logical_plan(ast)` produces a `LogicalPlan`:

```python
@dataclass(frozen=True)
class LogicalPlan:
    source_tables: list[TableReference]
    projections: list[Projection]
    predicates: list[Predicate]
    joins: list[Join]
    aggregations: Aggregation | None
    limit: Limit | None
    ordering: Ordering | None
    distinct: bool
```

Component extraction:

| Component | Method | Details |
|-----------|--------|---------|
| `source_tables` | `extract_table_references()` | Name, alias, type (table/subquery/cte/lateral) |
| `projections` | `_extract_projections()` | Expression, alias, source_columns |
| `predicates` | `_extract_predicates()` | Expression, columns, location (where/having) |
| `joins` | `_extract_joins()` | Type (inner/left/right/full/cross), table, condition, columns |
| `aggregations` | `_extract_aggregations()` | group_by column list |
| `limit` | `_extract_limit()` | count, offset |
| `ordering` | `_extract_ordering()` | columns with asc/desc direction |
| `distinct` | Direct check | Whether SELECT DISTINCT is used |

### Schema Inference

`infer_schema(ast, upstream_schemas, dialect)` uses sqlglot's optimizer to infer output column types:

1. Build a sqlglot schema from upstream schemas
2. Run sqlglot's `optimize()` with the schema
3. Extract column names and types from the optimized AST
4. Convert sqlglot types to Arrow type strings via `_sqlglot_type_to_arrow()`

### Column Lineage Extraction

`extract_lineage(ast, upstream_schemas, joint_name)` traces each output column to its source origins:

1. For `SELECT *`, expand to all columns from upstream schemas via `_resolve_star()`
2. For each projection, classify the transform type via `_classify_transform()`:
   - `"source"` — direct column from a source table
   - `"direct"` — simple column reference (possibly renamed)
   - `"renamed"` — column with an alias
   - `"expression"` — computed expression
   - `"aggregation"` — aggregate function
   - `"window"` — window function
   - `"literal"` — constant value
   - `"multi_column"` — expression referencing multiple columns
   - `"opaque"` — cannot determine
3. Map source columns to upstream joint origins via `_cols_to_origins()`

### Dialect Translation

`translate(ast, source_dialect, target_dialect)` transpiles SQL between dialects using sqlglot's `transpile()`. For example, Postgres `ILIKE` → DuckDB `ILIKE`, or Spark `LATERAL VIEW EXPLODE` → DuckDB `UNNEST`.

---

## Column Lineage (`rivet_core/lineage.py`)

Column lineage tracks how each output column in a joint relates to columns in upstream joints. It is computed during Phase 4 (SQL Compilation) and used during Phase 6 (Cross-Group Pushdown) to trace predicates back to source joints.

### Data Models

```python
@dataclass(frozen=True)
class ColumnOrigin:
    joint: str     # upstream joint name
    column: str    # column name in that joint

@dataclass(frozen=True)
class ColumnLineage:
    output_column: str
    transform: str       # "source", "direct", "renamed", "expression", etc.
    origins: list[ColumnOrigin]
    expression: str | None
```

### Traversal Functions

- `trace_column_backward(compiled, joint_name, column_name)` — walks the lineage chain backward from a given joint/column to its ultimate source origins (source joints or literals). Uses a stack-based DFS with cycle detection.

- `trace_column_forward(compiled, joint_name, column_name)` — walks forward from a source column through downstream joints, collecting every output column that depends on the specified origin.

Both functions preserve per-joint lineage within fused groups for full chain navigation.

---

## The Bridge (`rivet_bridge/converter.py`)

The `JointConverter` translates `JointDeclaration` objects (from config parsing) into core `Joint` objects. It validates:

- Engine references exist in the engines map (`BRG-207`)
- Audit quality checks only appear on sink joints (`BRG-206`)

It converts quality check declarations into `Assertion` objects and maps all declaration fields to `Joint` fields.

---

## Config SQL Parser (`rivet_config/sql_parser.py`)

The `SQLParser` in `rivet_config` (distinct from `rivet_core.sql_parser`) parses SQL joint declaration files — the `.sql` files in the `joints/` directory that use annotation comments:

```sql
-- rivet:name: daily_revenue
-- rivet:type: sql
-- rivet:upstream: raw_orders
SELECT order_date, SUM(amount) AS revenue
FROM raw_orders
GROUP BY order_date
```

It uses an `AnnotationParser` to extract key-value pairs from `-- rivet:key: value` comments, then builds a `JointDeclaration` with the SQL body as the remaining text after annotations.

Recognized annotation keys: `name`, `type`, `catalog`, `table`, `columns`, `filter`, `engine`, `eager`, `upstream`, `tags`, `description`, `write_strategy`, `function`, `fusion_strategy`, `materialization_strategy`, `dialect`.

Quality-related annotations (`assert`, `audit`, `quality.*`) are handled by a separate `QualityParser`.

---

## Materialization Strategies (`rivet_core/strategies.py`)

Materialization strategies define how intermediate results are persisted between execution steps.

### MaterializationStrategy (ABC)

```python
class MaterializationStrategy(ABC):
    def materialize(self, data: pyarrow.Table, context: MaterializationContext) -> MaterializedRef: ...
    def evict(self, ref: MaterializedRef) -> None: ...
```

### MaterializedRef (ABC)

A handle to materialized data with guaranteed `.to_arrow()` access:

```python
class MaterializedRef:
    def to_arrow(self) -> pyarrow.Table: ...
    def schema(self) -> Schema: ...
    def row_count(self) -> int: ...
    def size_bytes(self) -> int | None: ...
    def storage_type(self) -> str: ...       # "arrow", "parquet", "engine_temp"
```

### Built-in Implementations

- `ArrowMaterialization` / `_ArrowMaterializedRef`: In-memory Arrow table. Zero-copy. Eviction sets the internal table to `None`, causing subsequent `.to_arrow()` calls to raise `RVT-401`.

- `DeferredRef`: Catalog-backed deferred reference for checkpoint read-back. Holds catalog metadata instead of an Arrow table. The actual read is deferred to downstream resolution (adapter or source plugin). When constructed with a pre-computed `cached_table`, `.to_arrow()` returns it immediately. When `cached_table` is `None` (native SQL write path), the first `.to_arrow()` call reads from the catalog and caches the result.

---

## The Plugin System

The compiler interacts with plugins through the `PluginRegistry`. Here is how each plugin type participates in compilation:

### CatalogPlugin

Used in Phase 2 (metadata resolution) and Phase 3 (introspection):

- `get_schema(catalog, table)` → `ObjectSchema` with column names, types, nullability
- `get_metadata(catalog, table)` → `ObjectMetadata` with row count, size, partitioning
- `resolve_table_reference(logical_name, catalog)` → physical table path
- `get_fingerprint(catalog, path)` → content hash for cache invalidation

### ComputeEnginePlugin

Used across multiple phases:

- `dialect` property → SQL dialect name (Phase 4)
- `get_reference_resolver()` → `ReferenceResolver` (Phase 7)
- `supports_native_assertions` → bool (Phase 5, 9)
- `execute_assertion_sql()` → run assertion SQL natively (runtime)
- `materialization_strategy_name` → `"arrow"` or `"temp_table"` (Phase 8)
- `supported_catalog_types` → dict of natively supported catalogs (Phase 10)

### ComputeEngineAdapter

Used in Phase 2 (adapter resolution) and at runtime:

- `read_dispatch(engine, catalog, joint, pushdown)` → `AdapterPushdownResult` with `Material` and `ResidualPlan`
- `write_dispatch(engine, catalog, joint, material)` → write result
- `supports_native_sql_write(write_strategy)` → bool
- Capabilities list (e.g. `["predicate_pushdown", "projection_pushdown", "limit_pushdown"]`) drives optimizer decisions

### CrossJointAdapter

Used in Phase 8 (engine boundary detection):

- `resolve_upstream(context, upstream)` → `UpstreamResolution` for cross-engine data transfer

### ReferenceResolver

Used in Phase 7 (reference resolution):

- `resolve_references(sql, joint, catalog, compiled_joints, catalog_map, fused_group_joints)` → resolved SQL string with engine-native table references

---

## Error Handling

### Error Accumulation Model

The compiler never raises exceptions for compilation errors. Instead, errors are collected as `RivetError` objects in the `PhaseState.errors` tuple. Each phase extends the tuple; no phase can discard errors from a prior phase (monotonic growth).

When compilation finishes with errors, `CompiledAssembly.success` is `False` and `diagnostics.errors` contains the full list. The executor refuses to run a failed assembly.

### Phase Failure Modes

| Phase | Failure Mode | Behavior |
|-------|-------------|----------|
| 1. DAG Pruning | Invalid target_sink or tags | `RVT-306` error, early return (only phase that terminates pipeline) |
| 2. Metadata Resolution | Missing engine, missing adapter | `RVT-401`/`RVT-402` errors, continue to next joint |
| 3. Source Introspection | Timeout, catalog error | Warning, continue with `schema=None` |
| 4. SQL Compilation | SQL parse error, invalid python callable | Error, continue to next joint |
| 5. Fusion | (Pure algorithm) | N/A |
| 6. Optimization | (Pure algorithm) | N/A |
| 7. Strategy Resolution | Invalid strategy, resolver failure | Error/warning, continue |
| 8. Materialization | (Pure algorithm) | N/A |
| 9. Engine Boundaries | Missing CrossJointAdapter | Warning, use default passthrough |
| 10. Finalization | (Assembly step) | N/A |

### Exception Safety

Phases catch all exceptions from plugin calls (catalog introspection, reference resolution) and convert them to errors or warnings. No phase propagates unhandled exceptions to the pipeline orchestrator.

### Common Error Codes

| Code | Phase | Cause |
|------|-------|-------|
| `RVT-301` | Assembly | Duplicate joint name |
| `RVT-302` | Assembly | Unknown upstream reference |
| `RVT-303` | Assembly | Source joint has upstream |
| `RVT-304` | Assembly | Sink/checkpoint has no upstream |
| `RVT-305` | Assembly | Cyclic dependency |
| `RVT-306` | Phase 1 | Invalid target_sink or tags |
| `RVT-401` | Phase 2 | No engine resolved for joint |
| `RVT-402` | Phase 2 | No adapter for engine/catalog pair |
| `RVT-504` | Phase 8 | No CrossJointAdapter for engine boundary |
| `RVT-601` | Phase 7 | Invalid fusion strategy |
| `RVT-602` | Phase 7 | Invalid materialization strategy |
| `RVT-603` | Phase 7 | Conflicting fusion strategy overrides |
| `RVT-651` | Phase 4 | Audit assertion on non-sink joint |
| `RVT-753` | Phase 4 | Non-importable Python callable |
| `RVT-760` | Phase 4 | Source SQL contains JOINs |
| `RVT-761` | Phase 4 | Source SQL contains CTEs |
| `RVT-762` | Phase 4 | Source SQL contains subqueries |

---

## From Compilation to Execution

The `Executor` (`rivet_core/executor.py`) consumes the `CompiledAssembly` and follows `execution_order` exactly. Here is how compilation artifacts map to execution:

| Compilation Artifact | Execution Use |
|---------------------|---------------|
| `execution_order` | Groups are dispatched in this exact sequence |
| `FusedGroup.execution_sql` | SQL sent to the engine via `engine_plugin.execute_sql()` |
| `FusedGroup.pushdown` | Pushed predicates/projections/limits passed to `adapter.read_dispatch()` |
| `FusedGroup.residual` | Applied post-materialization via Arrow compute (filter, slice, cast) |
| `Materialization` | Determines where `ArrowMaterialization.materialize()` is called |
| `EngineBoundary` | Triggers `CrossJointAdapter.resolve_upstream()` for data transfer |
| `FusedGroup.checkpoint_sources` | Drives checkpoint read-back via adapter or SourcePlugin |
| `CompiledJoint.checks` | Assertion checks run pre-write; audit checks run post-write |
| `parallel_execution_plan` | Groups within the same wave can execute concurrently (respecting engine concurrency limits) |

---

## Source File Map

| File | Purpose |
|------|---------|
| `rivet_core/compiler.py` | Main compilation pipeline, all 10 phases, data models |
| `rivet_core/optimizer.py` | Fusion pass, pushdown passes, cross-group optimization |
| `rivet_core/sql_parser.py` | SQL parsing, logical plan, schema inference, lineage, dialect translation |
| `rivet_core/sql_resolver.py` | Execution SQL resolution (adapter rewrites, reference resolution fallback) |
| `rivet_core/assembly.py` | Assembly DAG construction and validation |
| `rivet_core/models.py` | Core data models (Catalog, ComputeEngine, Joint, Material, Schema) |
| `rivet_core/lineage.py` | Column lineage data models and backward/forward traversal |
| `rivet_core/strategies.py` | Materialization strategy contracts (Arrow, DeferredRef) |
| `rivet_core/plugins.py` | Plugin registry and all plugin interfaces |
| `rivet_core/executor.py` | Executes compiled assembly, applies residuals, materializes |
| `rivet_core/checks.py` | Quality check models (Assertion, CompiledCheck) |
| `rivet_core/errors.py` | Error types (RivetError, SQLParseError, ExecutionError) |
| `rivet_bridge/converter.py` | JointDeclaration → core Joint conversion |
| `rivet_config/sql_parser.py` | SQL joint declaration file parser (annotations + SQL body) |
