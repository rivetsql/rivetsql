# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.17] - 2026-05-26

### Changed
- Tightened dependency lower bounds: `pyarrow>=16.0`, `sqlglot>=25.0`, `duckdb>=1.1`, `polars>=1.0`, `pyspark>=3.5`, `textual>=1.0`, `ruff>=0.9`, `mypy>=1.15`, `pytest>=8.0`
- Updated `codecov/codecov-action` from `v5` to `v6` in CI workflow
- Fix mypy config: split pyarrow into its own override with `ignore_errors = true`; add per-module `disable_error_code = ["attr-defined", "no-untyped-call"]` for all files that use pyarrow APIs with incomplete stubs; restore `ignore_missing_imports` for all packages absent from the lint job (`[dev]` only installs); fix `pad.FileFormat` annotation in `s3_source.py` and `s3_sink.py` (local import used as type annotation — changed to `Any`)

### Removed
- Deleted `rivet_core/_compiler_monolith.py` — dead code left over from the compiler-package-split refactor

## [0.1.16] - 2026-03-22

### Added
- `--engine` / `-e` flag on `rivet run` and `rivet compile` to override the profile's `default_engine` without editing `profiles.yaml`

### Changed
- Compiler pipeline phases 8 and 9 swapped: Engine Boundary Detection now runs before Materialization Determination, so materialization uses validated `engine_boundaries` instead of ad-hoc engine-name comparison
- Group stats table in `compile` and `run` output now shows the last joint name instead of the group UUID, making output easier to read
- Dialect translation pipeline refactored into composable pre-transpile and post-transpile normalization passes for extensibility

### Fixed
- Source joints without an adapter (e.g. filesystem catalog on Spark/Polars) now correctly rewrite CTE bodies to reference the joint name instead of the `table_map`-resolved physical name — previously only adapter-backed sources were rewritten, causing `TABLE_OR_VIEW_NOT_FOUND` errors on engines like PySpark
- `rivet run` no longer repeats materialization lines (⚡) for each joint in a fused group — materialization is now reported once per group
- `rivet run` no longer shows rows and timing on every joint in a fused group — rows/time are now shown only on the exit joint where they are meaningful
- Schema mismatch warning in sink writes now reports only the columns that actually differ (after type normalization) instead of dumping the entire expected/actual schema
- Introspection now resolves `table_map` aliases before calling catalog plugins, using the same resolution logic as compilation and execution — previously introspection used the raw logical name, causing all `table_map`-dependent sources to fail schema/metadata retrieval
- Sources without a catalog plugin are now correctly counted as "skipped" instead of "failed" in introspection stats
- `LATERAL VIEW EXPLODE(map_col)` in Databricks-dialect joints now translates correctly to DuckDB using `map_keys()`/`map_values()` instead of producing an invalid `UNNEST(map)` call
- Executor no longer falls back to unresolved SQL when downstream groups have checkpoint dependencies from previous groups — checkpoint sources handled by CTE injection no longer set `has_materialized_inputs`, preserving the resolved SQL with engine-native references
- `table_map` aliases in catalog profiles are now resolved at compile time for all catalog types — previously source joints with inline SQL had `__self` substituted with the unmapped table name, causing `table_map` entries to be ignored in fused SQL CTEs
- Reference resolver now resolves source joints' own table references in fused CTE groups, preventing self-referencing CTEs (e.g. `x AS (SELECT * FROM x)`) at execution time
- Catalog `table_map` lookup now falls back to all available catalogs when the joint's catalog name doesn't match any profile catalog (e.g. production "unity" vs local "datalake")
- `dialect` annotation (`-- rivet:dialect: <name>`) is now recognized in SQL joint files and propagated through compilation
- Filesystem sink `append` strategy now uses `promote_options="permissive"` when concatenating Arrow tables, so schema differences between engines (timezone info, string vs large_binary, decimal precision, integer widths) no longer cause `ArrowInvalid` errors
- Dialect-translated SQL is now used in fused CTE composition, ensuring functions like `IFF()` are translated to the target engine's syntax
- `IFF()` function (Databricks/Snowflake) is now normalized to `IF()` before sqlglot translation, fixing silent pass-through in dialects where sqlglot doesn't natively recognize it
- Checkpoint joints without an adapter (e.g. filesystem catalog on Spark) now correctly reference the joint name in fallback CTE injection instead of the `table_map`-resolved physical name, fixing `TABLE_OR_VIEW_NOT_FOUND` errors
- CTE fusion now preserves the engine's SQL dialect when extracting and re-composing CTEs, fixing `UNSUPPORTED_DATATYPE "TEXT"` errors on Spark where `STRING` was silently converted to DuckDB's `TEXT` during CTE extraction
- Fused SQL now uses dialect-translated SQL (`sql_translated`) instead of DuckDB-normalized SQL (`j.sql`), fixing `UNSUPPORTED_DATATYPE` errors (e.g. `TEXT` instead of `STRING`) on engines like PySpark
- Execution SQL resolution no longer uses catalog-resolved SQL (`sql_resolved`) when upstream data is materialized across engine boundaries, preventing bypass of in-memory input tables (e.g. source inline transforms like YAML columns/filter being ignored)

### Added
- Subgraph poisoning in the compiler: when a joint fails engine or adapter resolution in Phase 2, all its downstream dependents are skipped in Phase 4 (SQL compilation), preventing cascading error noise that obscured the root cause
- `compile_until()` public API for partial compilation — returns the intermediate `PhaseState` at any named phase, enabling targeted debugging and testing
- `AdapterDecision` traceability records on `CompiledAssembly` — each adapter lookup records the engine/catalog pair, resolution method (exact match, wildcard fallback, none), and available alternatives
- Per-phase timing in `CompilationStats.phase_durations_ms` — records execution duration of each compiler phase in milliseconds
- `PluginAnnotation` records on `CompiledAssembly` for core/plugin boundary visibility — every catalog, engine, adapter, and reference resolver invocation is annotated with phase, plugin class, operation, and result
- Exhaustive compiler internals documentation at `docs/concepts/compiler-internals.md` covering all 10 compilation phases, optimizer passes, SQL parser, lineage, plugin interactions, and data models
- `IntrospectionRecord` entries on `CompiledAssembly` — per-source introspection details including catalog type, result status, duration, and schema/stats availability
- Three verbosity tiers for `rivet compile` output: compact (v=0), normal (v=1), verbose (v=2)
- `ProgressCallback` protocol in the Executor for live progress events during pipeline execution
- `LiveRunRenderer` for real-time execution progress during `rivet run` with three verbosity tiers
- Consistent icon vocabulary for joint types (`📥` source, `🔧` transform, `📤` sink, `🔒` checkpoint)
- Output channel separation: live progress to stderr, final summary to stdout
- Compilation summary line printed to stderr before `rivet run` execution

### Changed
- Compiler internals refactored into 10 sequential phases (`prune_dag` → `resolve_metadata` → `introspect_sources` → `compile_sql` → `fusion` → `optimization` → `strategy_resolution` → `materialization` → `engine_boundaries` → `finalization`), each a pure function over an immutable `PhaseState` accumulator — no change to `compile()` signature or `CompiledAssembly` output
- Normal compile output (v=1) shows only executed SQL, omitting original/translated/resolved variants (now v=2 only)
- `rivet run` text format uses `LiveRunRenderer` for real-time progress output

## [0.1.15] - 2026-03-16

### Added
- `DuckDBReferenceResolver` for compile-time source reference resolution — rewrites logical source names in fused SQL to DuckDB-native reader functions (`read_csv_auto`, `read_parquet`, `read_json_auto`) for filesystem sources and qualified table names for DuckDB sources, enabling native SQL write for cross-catalogue pipelines (e.g. filesystem → DuckDB) without Arrow staging fallback
- Native SQL write optimization for same-backend sink/checkpoint writes — when the compute engine and catalog share the same backend (e.g., DuckDB→DuckDB), fused SQL is embedded directly into write DDL (`CREATE TABLE ... AS <fused_sql>`), eliminating the Arrow round-trip through `SinkPlugin.write()`
- `DuckDBLocalAdapter` for `(duckdb, duckdb)` engine/catalog pair — supports replace, append, and truncate_insert strategies via native SQL write
- `PostgresLocalAdapter` for `(postgres, postgres)` engine/catalog pair — supports replace, append, and truncate_insert strategies via native SQL write
- Native SQL write support for `DatabricksAdapter` (`databricks→databricks`) and `DatabricksUnityAdapter` (`databricks→unity`) — replace, append, and truncate_insert strategies execute fused SQL directly on the Databricks SQL Warehouse
- `NativeSqlWriteContext` dataclass and `supports_native_sql_write()` method on `ComputeEngineAdapter` for adapter opt-in to native SQL write
- `write_path` field on `JointExecutionResult` for observability of write path (`"native_sql"` or `"arrow_fallback"`)
- Optional `schema` filter for Databricks and Unity catalog plugins — when set, restricts explore/REPL schema listings and source declarations to the configured schema; when omitted, all schemas are visible
- `CheckpointSourceInfo` frozen dataclass and `checkpoint_sources` field on `FusedGroup` — pre-resolved catalog and adapter metadata for checkpoint-to-downstream resolution, populated at compile time (defaults to empty dict, no impact on existing pipelines)
- Compiler `_build_checkpoint_sources` step pre-resolves adapter metadata for checkpoint-to-downstream pairs on each `FusedGroup`, with compile-time warnings for missing adapters and inclusion in `rivet compile` adapter output

### Fixed
- Databricks sink FQN resolution: two-part table names (`schema.table`) on legacy catalogs no longer produce invalid four-part names (`catalog.default.schema.table`) — the sink now correctly prepends only the catalog, producing `catalog.schema.table`
- Checkpoint write failures no longer fail silently — `_dispatch_sink_write` now propagates exceptions for checkpoint joints instead of swallowing them, so CREATE TABLE errors surface immediately instead of causing a misleading `TABLE_OR_VIEW_NOT_FOUND` on the subsequent read-back
- Sink write failures are now logged with a warning instead of being silently ignored
- Databricks Arrow fallback staging view no longer uses type definitions in `CREATE VIEW` column lists — Spark/Databricks rejects `CREATE VIEW (col BIGINT, ...)` syntax; staging views now use `SELECT CAST(...) FROM VALUES ... AS _t(col_names)` instead
- Native SQL write guard now correctly treats empty `ResidualPlan` (no predicates, no limit, no casts) as equivalent to no residual — previously an empty residual object blocked native SQL write for all groups processed by the optimizer, forcing the slower Arrow fallback path even for same-backend writes (e.g. Databricks→Databricks checkpoints)
- Native SQL write now gracefully falls back to Arrow path when the fused SQL references tables that only exist in the engine connection (e.g. filesystem source → DuckDB sink) — previously this caused a silent group failure; for same-backend scenarios like Databricks→Databricks where the SQL Warehouse resolves all references, native SQL write still works directly
- Cross-group checkpoint references in downstream fused groups are now resolved via CTE injection — checkpoint CTEs are prepended to the downstream group's fused SQL at compile time using the engine's reference resolver, producing engine-native expressions (e.g. `read_parquet(...)` for DuckDB filesystem catalogs, `catalog.schema.table` for Databricks) so checkpoint and source references go through the same resolution path
- DuckDB filesystem source resolver now handles checkpoint tables that don't exist at compile time — when the file is missing but the catalog declares an explicit `format`, the resolver constructs the expected path with the format extension (e.g. `read_parquet('/path/table.parquet')`)
- Checkpoint CTE injection now scans all joints in a fused group, not just entry joints — joints with both intra-group and cross-group upstream dependencies (e.g. a joint referencing a local source AND a checkpoint from another group) were previously missed because they are not entry joints
- Sink native SQL write now correctly filters upstream materials by the exit joint's declared upstream — previously the method checked all accumulated materials from the execution wave, causing it to see multiple entries and skip native write for sinks whose upstream was not fused with them (e.g. eager upstream or assertion barrier)
- Executor no longer calls `.to_arrow()` on `DeferredRef` entries from unrelated groups when building Arrow input tables — previously, a checkpoint written via native SQL in one group caused `RVT-501` errors when a sibling group tried to eagerly materialize it during its own execution
- Sink joints now use native SQL write when fused SQL is unavailable — when a sink is in its own fused group (e.g. upstream has assertions or is on a different engine), the executor constructs `SELECT * FROM {upstream}` from the materialized upstream table instead of silently falling back to the Arrow path

### Changed
- Checkpoint joints now return a `DeferredRef` instead of eagerly reading data back into an Arrow table, enabling lazy resolution by downstream groups through the same adapter and source plugin mechanism used for source joints

### Added
- Downstream joints can now resolve checkpoint references using adapters, source plugins, or Arrow fallback — enabling cross-engine checkpoint consumption where, for example, a Spark joint reads directly from a DuckDB-written checkpoint table without an Arrow round-trip

### Changed
- Source inline SQL now uses `FROM __self` instead of `FROM {joint_name}` — `__self` is a reserved alias substituted with the table FQN at compile time
- Python joint documentation now recommends `-> Material` as the canonical return type hint across all guides, concept docs, and plugin docs
- `rivet init` source template now uses SQL with an explicit SELECT statement by default (sql and mixed styles); yaml style uses `columns` field for explicit projection
- All plugin `pyproject.toml` files now use `dev-mode-dirs = ["."]` so `pip install -e` works for every plugin, not just core
- `scripts/dev-install.sh` now installs all plugins in editable mode (`-e`) for instant source change pickup during development

### Added
- `checkpoint` joint type — writes intermediate pipeline results to a catalog table and re-exposes them for downstream joints, enabling long-pipeline staging and fan-out patterns
- Compiler support for `checkpoint` joint type: validates `catalog`/`table` fields, defaults `write_strategy` to `"replace"`, emits warning for checkpoints with no downstream consumers, and creates `checkpoint_boundary` materialization entries
- Checkpoint joints now support SQL (inline or `.sql` file) and YAML transforms (`columns`/`filter`/`limit`) identical to sink joints — enabling cross-group predicate, projection, and limit pushdown from checkpoint SQL to upstream sources
- `rivet repl execute --compile-only`: new flag that compiles the transient pipeline and prints the compilation result (joints, fused groups, resolved SQL) as JSON without executing the query
- Sink SQL parsing: sink joints with SQL now have their SQL parsed into a LogicalPlan, enabling cross-group predicate, projection, and limit pushdown from sinks to upstream sources — YAML-declared sink transforms (`columns`/`filter`/`limit`) also benefit because the bridge generates SQL for them
- `DatabricksAdapter` for `(databricks, databricks)` engine/catalog pair — enables source joints on `databricks` catalog type (both Unity namespaces and legacy `hive_metastore`) to be read/written via the Databricks Statement Execution API
- Type parser now supports `map<K,V>` complex types, resolving unknown-type warnings for Databricks/Hive columns with map types

### Fixed
- Checkpoint read-back on Databricks (and similar deferred-execution backends) no longer fails with `RVT-501 "requires the Databricks engine to read data"` — adapter read-back errors now propagate instead of silently falling through to the SourcePlugin (which always fails for deferred-execution backends); for backends where the SourcePlugin works (e.g. DuckDB), the fallback is still used
- Source joints with inline SQL transforms no longer produce circular CTE references — `SQLGenerator` now emits `FROM __self` and the compiler substitutes it with the table FQN at compile time
- `rivet repl execute` no longer produces `TABLE_OR_VIEW_NOT_FOUND` when querying known source joints (e.g. `select * from d_sku`) — source joints reconstructed from compiled_map now have their inline SQL stripped so the fusionner treats them as pure sources resolved to their FQN, matching the behavior of sources created by `preprocess_sql`
- `DatabricksReferenceResolver` no longer treats source joints with SQL (inline transforms) as CTE siblings — fixes `rivet repl execute` sending unqualified joint names to Databricks instead of fully-qualified `catalog.schema.table`, causing HTTP 404 (`RVT-503`) and `TABLE_OR_VIEW_NOT_FOUND` errors
- `DatabricksAdapter.read_dispatch` now builds SQL using the fully-qualified three-part table name instead of the partial `joint.table` value — fixes `RVT-503` / HTTP 404 errors in `rivet repl execute` for `databricks` catalog sources
- Databricks catalog legacy introspection (`get_schema`, `get_metadata`) now correctly parses two-part table names (`schema.table`) by using the catalog's `catalog` option instead of misinterpreting the schema as the catalog name
- Databricks catalog plugin: SQL-based introspection fallback for legacy catalogs (e.g. `hive_metastore`) that are not exposed through the Unity Catalog REST API — opt-in via `legacy: true` and `warehouse_id` options, using `SHOW SCHEMAS`, `SHOW TABLES`, `DESCRIBE TABLE`, and `DESCRIBE TABLE EXTENDED` through the SQL Statement Execution API
- `rivet init` now generates an `AGENTS.md` file with best-practice guidelines for AI agents working with Rivet projects
- Arrow-native Spark 4.0 conversion support in `rivet_pyspark`: new `arrow_converter` module with automatic version detection, native `toArrow()`/`createDataFrame(pyarrow.Table)` path on Spark 4.0+, and transparent pandas fallback for Spark 3.x
- All Spark ↔ Arrow conversion points (engine, Glue/S3/Unity adapters) now route through the centralized converter, eliminating duplicated pandas conversion code
- `supports_native_assertions` property and `execute_assertion_sql` method on `ComputeEnginePlugin` interface for engine-native assertion execution
- DuckDB engine plugin declares native assertion support, delegating assertion SQL to its existing `execute_sql` path
- Compiler suppresses `assertion_boundary` materialization for joints on assertion-capable engines with SQL-translatable checks, preserving fused groups
- `execution_method` field on `CheckExecutionResult` for observability of assertion execution path (`"arrow"` or `"engine_native"`)

## [0.1.14] - 2026-03-14

### Added
- Shared `FormatRegistry` in `rivet_core.formats`: canonical `FileFormat` enum, extension mappings, format detection with directory probing (local dirs and S3 prefixes), cascading resolution, validation, and per-plugin capability declarations
- IPC (Arrow/Feather) write support in filesystem sink — append, replace, and partition strategies using PyArrow IPC file writer with `.arrow` default extension
- Unified format detection across filesystem catalog, filesystem sink, S3 source, and S3 sink — all four plugins now delegate to `FormatRegistry` instead of maintaining independent format logic
- Plugin coherence audit script (`scripts/check_plugin_coherence.py`): scans all plugin packages against an expected capability matrix and produces a structured report for CI (exit 0 = coherent, exit 1 = gaps found)
- DuckDB catalog plugin now implements `test_connection` (lightweight `SELECT 1` connectivity check)
- Arrow catalog plugin now implements `test_connection` (unconditional success for in-memory catalog) and `list_children` (table and column hierarchy navigation)
- Filesystem catalog plugin now implements `test_connection` (base path existence check) and `list_children` (directory listing and schema-inferred column navigation)
- Databricks catalog plugin now implements `test_connection` (lightweight `/catalogs` API check) and `list_children` (schema → table → column hierarchy navigation)
- PostgreSQL catalog plugin now implements `test_connection` (lightweight `SELECT 1` check) and `list_children` (schema → table → column hierarchy navigation)
- REST API sink now declares `supported_strategies` and validates write strategies upfront
- Arrow sink now declares `supported_strategies` class attribute

### Fixed
- Arrow sink now raises `ExecutionError` (RVT-501) for unsupported write strategies instead of silently falling back to replace
- Arrow source now raises `ExecutionError` instead of bare `KeyError` when a table is not found
- Arrow catalog `get_schema` now raises `ExecutionError` instead of bare `NotImplementedError` for missing tables
- Arrow engine `validate` now rejects unrecognized options with `PluginValidationError` instead of silently accepting them
- Filesystem catalog `validate` now rejects unrecognized options with `PluginValidationError` instead of silently accepting them
- REST API sink now raises `ExecutionError` (RVT-501) for unsupported write strategies instead of silently accepting them
- Multi-engine execution plans no longer fail when a reference resolver from one engine type (e.g. postgres) incorrectly rewrites SQL in groups belonging to a different engine type (e.g. duckdb)
- Source inline transforms now work correctly for filesystem and other non-adapter catalogs — predicates, projections, and limits from YAML `filter`/`columns`/`limit` are applied as post-read residuals when the source plugin does not support pushdown
- Source joints with YAML `filter` or `limit` (without `columns`) now correctly generate SQL for LogicalPlan extraction — previously only `columns` triggered SQL generation

### Added
- Databricks catalog plugin now implements `test_connection` (lightweight `/catalogs` API check) and `list_children` (schema → table → column hierarchy navigation)
- PostgreSQL catalog plugin now implements `test_connection` (lightweight `SELECT 1` check) and `list_children` (schema → table → column hierarchy navigation)
- Source inline transforms: `columns`, `filter`, and `limit` YAML fields on source joints push predicates, projections, and row limits to the adapter during reads — column expressions (renames, CAST, computed) are applied as post-read residuals; both YAML and SQL forms are interchangeable
- Source inline transform validation in the compiler: single-table constraint enforcement (RVT-760, RVT-761, RVT-762), column reference warnings against introspected schema, and transformed output schema propagation for source joints with projections, renames, and CAST expressions
- Python joints now automatically resolve project-local imports — no need to set `PYTHONPATH` manually; the compiler and executor inject the project root into `sys.path` transparently
- Enhanced compilation output: `rivet compile` now displays execution SQL (the actual SQL sent to engines), detailed pushdown information per joint, and a dedicated cross-group optimizations summary section
- Sink schema inference: compiler automatically determines and attaches output schemas to sink joints based on upstream data flow, with schema confidence levels (introspected, inferred, partial, none) and conflict detection for multi-upstream sinks
- Centralized type parser in `rivet_core` for complex types (arrays and structs) across all catalog plugins, eliminating code duplication and enabling proper Arrow type mapping for nested data structures
- Complex type support for Unity Catalog, AWS Glue Catalog, DuckDB Catalog, and PostgreSQL Catalog — array and struct columns now map to Arrow list and struct types instead of defaulting to string
- PostgreSQL array syntax support (`type[]`) in addition to standard `array<T>` syntax used by other catalogs
- SQL parser now supports ARRAY and STRUCT types in type declarations using centralized type parser
- PostgreSQL engine now accepts individual connection parameters (`host`, `port`, `database`, `user`, `password`) in addition to `conninfo` string, allowing engines to use the same connection configuration as catalogs without duplication
- Engine instantiation now automatically inherits connection parameters from matching catalogs — when an engine references a catalog of the same type, connection params (host, port, database, user, password) are inherited from the catalog, with engine options taking precedence for overrides

### Changed
- Fused group display now shows individual joint SQL (original, translated, resolved) instead of duplicating execution SQL for each joint, making it clearer how joints compose into the final fused query

### Fixed
- CTE fusion bug: joints containing WITH clauses now fuse correctly — CTEs are extracted and merged into a single top-level WITH clause instead of generating invalid SQL with multiple WITH keywords
- Cross-wave table references: joints in later execution waves can now correctly reference materialized tables from earlier waves when those tables have assertion boundaries — improved SQL parser now extracts table references from CTEs, subqueries, and complex SQL patterns, not just simple FROM/JOIN clauses
- PostgreSQL plugin now works correctly when called from async contexts (REPL, explore sessions) — replaced direct `asyncio.run()` calls with safe async execution that detects running event loops and uses thread-based execution when necessary
- PostgreSQL DuckDB adapter now installs the postgres extension from the official repository instead of community repository, fixing installation failures in some environments
- PostgreSQL DuckDB adapter now correctly handles RecordBatchReader results from DuckDB by converting them to Arrow Tables, fixing `'RecordBatchReader' object has no attribute 'num_rows'` errors
- PostgreSQL engine now supports CTE fusion with PostgreSQL sources — added reference resolver that rewrites source joint references to fully-qualified `schema.table` names, allowing native PostgreSQL sources to execute without materialization
- Sink schema validation warnings now use type compatibility checking to avoid false positives for semantically equivalent types (utf8 vs string, float64 vs double, date32 vs date32[day], decimal128(38,0) vs int64) and allow string/timestamp interchangeability for date columns that sinks can handle automatically
- S3 catalog `endpoint_url` no longer gets corrupted when used with DuckDB — the scheme (`http://`/`https://`) is now stripped before passing to DuckDB's secret manager, and `USE_SSL false` is set for HTTP endpoints, fixing `https://http://localhost%3A9000` mangling with MinIO/LocalStack
- S3 catalog no longer blindly appends `.parquet` to table names — file format is now auto-detected from the table name's extension (e.g., `customers.csv` uses `read_csv_auto`), falling back to the catalog `format` option only when no recognized extension is present
- S3 DuckDB adapter now correctly handles RecordBatchReader results from DuckDB by converting them to Arrow Tables via `.read_all()`, fixing `'RecordBatchReader' object has no attribute 'num_rows'` errors
- Replaced bare `ValueError` with `ExecutionError` in DuckDB filesystem sink (`_read_file`), filesystem catalog (`_read_table`, `_read_schema_lightweight`) for unsupported format handling
- Replaced bare `NotImplementedError` with `ExecutionError` in Databricks source, Unity source (deferred `schema`/`row_count`), S3 catalog (`get_schema`), and S3 source (`to_arrow`) for unsupported delta format
- Replaced bare `ValueError` with `PluginValidationError` in REST auth (`create_auth`) and pagination (`create_paginator`) factories for unrecognized types

## [0.1.13] - 2026-03-12

### Added
- REST API catalog plugin (`rivet_rest`/`rivetsql-rest`) with authentication, pagination, JSON flattening, and Arrow conversion
- Wildcard adapter architecture: adapters can register with `target_engine_type = "*"` to work across all Arrow-compatible engines
- REST API authentication: bearer token, basic auth, API key, OAuth2 client credentials with auto-refresh
- REST API pagination: offset/limit, cursor-based, page number, Link header (RFC 8288)
- REST API predicate pushdown: translates filter conditions to query parameters where supported
- REST API sink: writes Arrow data to endpoints via POST/PUT/PATCH with batching and rate limiting
- REST API rate limiting and retry: enforces request limits, handles HTTP 429, retries transient errors with backoff
- `rivet catalog create` wizard supports interactive endpoint configuration for REST API catalogs
- Documentation: REST API plugin reference (`docs/plugins/rest.md`), REST API integration guide (`docs/guides/rest-api-integration.md`), wildcard adapter architecture (`docs/concepts/wildcard-adapters.md`)

### Fixed
- Fixed Unity catalog creation wizard saving authentication method as 'auth' instead of 'auth_type', causing validation error RVT-201
- `rivet catalog create` wizard now correctly parses optional parameter types (dict, int, float, bool) instead of storing as strings
- REPL ad-hoc queries with LIMIT now push the limit to REST API sources, stopping page fetches once limit is reached
- Optimizer `pushdown_pass` searches backwards through fused groups to find LogicalPlan when exit joint has none
- REPL query execution now works when called from within an existing event loop (e.g., Textual TUI) by running async operations in a separate thread
- REST API limit pushdown test now provides explicit schema to avoid schema inference HTTP requests interfering with request count assertions
- REST API pagination now correctly passes `response_path` to paginator, fixing bug where pagination stopped after first page when records were nested (e.g., `response_path: "results"`)

## [0.1.12] - 2026-03-11

### Added
- `rivet catalog list` now accepts a dot-separated path (e.g., `mycatalog.myschema`) to list children at any level of the catalog tree without using the interactive explorer
- SmartCache integration in `InteractiveSession`: REPL and explore sessions now use persistent catalog cache with warm-start, automatic invalidation on config file changes and profile switches
- SmartCache: unified persistent catalog metadata cache (`~/.cache/rivet/catalog/`) with per-catalog JSON files, TTL-based expiration, fingerprint-based staleness detection, LRU eviction (50 MB default), and debounced flush policy
- Cache modes: `READ_WRITE` for interactive tools (warm-start), `WRITE_ONLY` for non-interactive CLI commands (rehydrate cache for future sessions)
- Progressive search expansion: `search()` now seeds from SmartCache, applies access-priority scoring, and progressively expands unexplored branches within a time budget
- `CatalogPlugin.get_fingerprint()`: optional method for plugins to support lightweight staleness detection

### Changed
- `CatalogExplorer` now accepts optional `smart_cache` and `cache_mode` parameters for transparent cache integration
- `explore` keystroke search uses `expand=False` for instant results without network I/O
- `rivet catalog list <catalog>` now lists the catalog's children (schemas/databases) directly instead of showing the catalog info summary — no more need for `--depth 1` to drill in
- Complete test suite overhaul: reorganized from flat `tests/` into `tests/unit/`, `tests/integration/`, and `tests/e2e/` with pytest markers (`@pytest.mark.unit`, `integration`, `e2e`)
- Removed ~93K lines of redundant/duplicated test code across all plugin test directories

### Removed
- `rivet_cli.repl.catalog_cache` module retired — replaced by `rivet_core.smart_cache.SmartCache`

### Fixed
- `rivet catalog search` Phase 2 now skips irrelevant sibling schemas when their parent already produced hits — prevents budget exhaustion on cached catalogs with many sub-schemas (e.g. `datalake_silver` with ~100 children) so unexplored catalogs like `preprod_datalake_silver` get expanded in time
- Removed leftover debug logging (`stderr.write`) from catalog search Phase 2 expansion
- Join-equality predicate propagation no longer breaks when a column name is a SQL keyword like `and`, `or`, `not` — the AND-split regex now requires surrounding whitespace instead of word boundaries
- `rivet catalog search` expansion budget increased to 10s (from 2s default) for better coverage of large catalogs with high-latency backends
- `rivet catalog search` now uses `READ_WRITE` cache mode so progressive expansion can discover catalog nodes (was returning no results due to `WRITE_ONLY` mode blocking expansion)
- SmartCache deserialization: cached `CatalogNode` and `ObjectSchema` objects are now properly reconstructed from JSON after disk round-trip (was causing `AttributeError: 'dict' object has no attribute 'path'`)
- Phase 2 progressive expansion now seeds frontier from already-cached catalog levels, so branches loaded from SmartCache in Phase 1 are drilled into instead of skipped
- Phase 2 expansion no longer stops early when hit count reaches the limit — it explores until the time budget expires, then returns the best-scored matches across all discovered branches
- Phase 2 expansion uses depth-based breadth-first ordering so top-level catalog schemas are explored before deeper sub-schemas
- Fixed double catalog-name prefix in Phase 2 qualified names (`unity.unity.…` → `unity.…`)
- Phase 2 expansion no longer drills into table columns — only container nodes (schemas/databases) are queued for deeper exploration
- `rivet catalog search` Phase 2 expansion now prioritizes branches that share path segments with Phase 1 hits — e.g. if `datalake_silver.data_factory_ingest` produced results, `preprod_datalake_silver` is expanded first instead of wasting the time budget on unrelated schemas
- Fuzzy matcher now awards a strong bonus when the query appears as a contiguous substring in the candidate, so `ingestion_event` ranks far above scattered-character matches like `grs_functional_unit`
- `rivet catalog search` now filters out matches scoring 20+ points worse than the best hit, removing scattered-subsequence noise from results
- Python joint function path parsing: changed from dot-separated (`module.func`) to colon-separated (`module:func`) in `_verify_callable`, `_check_custom`, and `_execute_python_joint` — aligns with Python entry-point convention
- YAML annotation parser: added `_StringSafeLoader` that preserves boolean-like strings (`yes`, `no`, `on`, `off`) as-is instead of coercing to Python bools
- Optimizer fusion: PythonJoints now blocked from fusing as upstream (condition 6) so executor can dispatch them via `_execute_python_joint`

## [0.1.10] - 2026-03-08

### Added
- `scripts/dev-install.sh` for installing core (editable) + all plugins from local source in one command
- `concurrency_limit` documentation across all engine plugin docs (DuckDB, Polars, Postgres, PySpark, Databricks)
- Parallel execution & concurrency guide in `docs/concepts/engines.md`
- `default_concurrency_limit` property documented in plugin development guide
- DuckDB per-engine thread-safe connection pooling with `_engine_conns`, `_engine_views`, and per-engine locks

### Changed
- Inter-package dependency pins relaxed from `==` exact to `>=X.Y.0,<X.(Y+1).0` compatible ranges so editable/local installs work without all plugins on PyPI
- `scripts/bump-version.sh` updated to manage range pins automatically on minor version bumps
- Optimizer `cross_group_pushdown_pass` uses `dict[str, Any]` for kwargs (mypy fix)
- Renamed shadowed variable `existing` → `existing_lim` in limit merge logic

### Fixed
- Engine option validation now strips framework-level keys (`concurrency_limit`) before calling plugin `validate()`, preventing false BRG-204 errors that cascaded into BRG-207 unknown engine references
- Unused import `field` removed from executor, unused import `OptimizationResult` removed from tests
- Property test filter tightened to avoid substring collisions in column names
- E2e test harness and optimizer property test fixes

## [0.1.9] - 2026-03-08

### Added
- Cross-group predicate pushdown: propagates WHERE filters across materialization boundaries to upstream source reads using column lineage
- Cross-group projection pushdown: prunes unused columns at source reads when only a subset is needed downstream
- Cross-group limit pushdown: pushes LIMIT down to source adapter reads when safe
- Join-equality propagation: derives `b.col = 'value'` from `WHERE a.col = 'value'` + `ON a.col = b.col` and pushes to source groups
- `per_joint_predicates`, `per_joint_projections`, `per_joint_limits` fields on `FusedGroup` for cross-group pushdown plans
- `RunStats` and `StatsCollector` in `rivet_core.stats` for detailed per-group/per-joint execution statistics
- Engine/rivet time breakdown in REPL execute output footer
- Selective plugin loading: only imports plugins needed by the active profile (`register_optional_plugins(only=...)`)
- Plugin discovery guard (`is_discovered` property) prevents redundant entry-point scanning
- Glue catalog: parallel `list_tables` across databases with `ThreadPoolExecutor` and TTL cache
- DuckDB engine: connection pooling — reuses a single DuckDB connection instead of creating a new one per query
- `skip_catalog_probe` option on `InteractiveSession` for faster non-interactive execution
- New docs: `compilation.md`, `cross-group-predicate-pushdown.md`

### Changed
- Fusion pass now merges all eligible upstream groups for multi-input joints (e.g. JOINs), not just the largest
- Compiler adapter resolution uses a cache to avoid redundant registry lookups
- `_resolve_engine` simplified: no longer falls back to `registry.get_compute_engine()`

### Fixed
- Quote YAML name values in property tests to prevent boolean coercion (`on` → `True`)

### Performance
- Skip history persistence for temp/ephemeral directories (prevents history.json bloat from pytest runs)

## [0.1.8] - 2026-03-06

### Fixed
- PySpark 3.5: pass explicit Arrow-derived schema to `createDataFrame()` to avoid `CANNOT_DETERMINE_TYPE` errors on null/ambiguous columns

## [0.1.7] - 2026-03-06

### Fixed
- DuckDB 1.x compatibility: `.arrow()` returns `RecordBatchReader`, added `.read_all()` to all adapter call sites
- PySpark 3.5 compatibility: convert Arrow tables to pandas before `createDataFrame()` (direct Arrow support is 4.0-only)

## [0.1.6] - 2026-03-06

### Changed
- Plugin version constraints pinned with `==` instead of `>=` so upgrading core always pulls matching plugin versions

## [0.1.5] - 2026-03-06

### Fixed
- Plugin discovery now uses entry points instead of hardcoded import list
- Plugin wheels include actual Python source code (hatch build config)
- Version test no longer asserts hardcoded version string

## [0.1.4] - 2026-03-06

### Fixed
- Plugin wheels now include actual Python source code (hatch build config fix)

## [0.1.3] - 2026-03-06

### Fixed
- CLI `--version` now reads from package metadata instead of hardcoded string
- Quoted bracket notation in docs for zsh compatibility

## [0.1.2] - 2026-03-06

### Fixed
- Fixed optional dependency versions from `>=1.0.0` to `>=0.1.0` so `pip install 'rivetsql[all]'` works
- Updated ruff pre-commit hook to v0.15.4 to match CI
- Fixed import sorting in explore command

## [0.1.1] - 2026-03-06

### Fixed
- Resolved ruff lint errors across CLI and tests (dead code, unused imports, f-string prefixes, import sorting)
- Fixed missing `_initial_sql` attribute in editor cache restore test
- Resolved mypy type errors in explore command and terminal renderer
- Fixed `.gitignore` entry for `.kiro/` directory

### Changed
- Removed automated semantic-release workflow; versions are now managed manually
- Properly typed `ExploreController` renderer as `TerminalRenderer`
- Cleaned up `# type: ignore` comments in explore command

## [0.1.0] - 2025-01-01

### Added

#### Core Framework (`rivetsql-core`)
- **Joints** — declarative pipeline units defined in SQL, YAML, or Python
- **Engines** — pluggable execution backends; swap without changing pipeline logic
- **Catalogs** — unified data source/sink abstraction (files, databases, object storage)
- **Quality checks** — pre-write assertions and post-write audits with configurable failure modes
- **Cross-joint execution** — run individual joints across engine boundaries (e.g., read from Postgres, write via Polars)
- **Watermarking** — incremental/CDC pipeline support with automatic watermark tracking
- **Lineage tracking** — built-in DAG resolution and dependency graph
- **Metrics collection** — row counts, durations, and custom quality metrics per joint
- **Testing framework** — offline fixture-based unit tests with `rivet test` (no live engine required)
- **CLI** — `rivet init`, `rivet run`, `rivet test`, `rivet inspect` commands
- **Interactive REPL** — explore catalogs, run joints, and debug pipelines interactively
- **Plugin system** — install only the engines you need via entry-points

#### Engine Plugins
- **`rivetsql-duckdb`** — in-process analytical SQL via DuckDB ≥ 0.9
- **`rivetsql-postgres`** — PostgreSQL read/write via psycopg 3 (binary + pool)
- **`rivetsql-polars`** — DataFrame-based compute via Polars ≥ 0.20; optional Delta Lake support
- **`rivetsql-pyspark`** — distributed Spark execution via PySpark ≥ 3.3
- **`rivetsql-databricks`** — Databricks SQL warehouses and Unity Catalog integration
- **`rivetsql-aws`** — S3 object storage and AWS Glue catalog integration

### Known Limitations
- `rivet run --parallel` is not yet implemented; joints execute sequentially
- Databricks plugin requires a running SQL warehouse; Serverless not yet tested
