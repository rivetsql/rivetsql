# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
