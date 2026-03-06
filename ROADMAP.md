# Roadmap

This document outlines planned features and improvements for Rivet. It is not a commitment — priorities may shift based on community feedback.

## v0.2.0 — Parallel Execution

- [ ] `rivet run --parallel` — DAG-aware concurrent joint execution
- [ ] Configurable concurrency limits per engine
- [ ] Live progress display in the CLI during parallel runs

## v0.3.0 — Incremental & Streaming

- [ ] Watermark-driven incremental runs without manual configuration
- [ ] Kafka source/sink plugin (`rivetsql-kafka`)
- [ ] DuckDB-based micro-batch streaming mode

## v0.4.0 — Observability

- [ ] OpenTelemetry span export for joint-level tracing
- [ ] Prometheus metrics endpoint for pipeline health
- [ ] Built-in data quality dashboard (Textual TUI)

## v0.5.0 — Platform Integrations

- [ ] dbt project import (read dbt models as Rivet joints)
- [ ] Airflow and Prefect operator packages
- [ ] GitHub Actions reusable workflow for `rivet test`

## Backlog / Under Consideration

- Windows first-class support
- PySpark cross-joint adapter
- Databricks Serverless SQL warehouse support
- Snowflake plugin (`rivetsql-snowflake`)
- BigQuery plugin (`rivetsql-bigquery`)
- Web-based pipeline editor

---

Have an idea? [Open a discussion](https://github.com/rivetsql/rivetsql/discussions) or [file an issue](https://github.com/rivetsql/rivetsql/issues).
