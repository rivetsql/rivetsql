# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.10.0] - 2026-03-08

### Added
- `scripts/dev-install.sh` for installing core (editable) + all plugins from local source in one command

### Changed
- Inter-package dependency pins relaxed from `==` exact to `>=X.Y.0,<X.(Y+1).0` compatible ranges so editable/local installs work without all plugins on PyPI
- `scripts/bump-version.sh` updated to manage range pins automatically on minor version bumps

### Fixed
- Engine option validation now strips framework-level keys (`concurrency_limit`) before calling plugin `validate()`, preventing false BRG-204 errors that cascaded into BRG-207 unknown engine references

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
