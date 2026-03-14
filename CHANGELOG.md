# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
