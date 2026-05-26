# `rivet/` — package source root

This directory holds the **`rivetsql`** Python distribution and the source
trees of the optional plugins (`rivetsql-duckdb`, `rivetsql-postgres`,
`rivetsql-polars`, `rivetsql-pyspark`, `rivetsql-aws`, `rivetsql-databricks`,
`rivetsql-rest`).

End-user documentation lives in the [top-level README](../README.md) and at
[rivetsql.github.io/rivetsql](https://rivetsql.github.io/rivetsql).

## Layout

| Path | Distribution | Purpose |
|---|---|---|
| `pyproject.toml` | `rivetsql` | Core engine + CLI + bridge + config |
| `src/rivet_core/` | (in `rivetsql`) | Joint model, plugin ABCs, compiler, executor |
| `src/rivet_config/` | (in `rivetsql`) | Profile / project YAML parsing |
| `src/rivet_bridge/` | (in `rivetsql`) | DAG compilation, execution planning, dispatch |
| `src/rivet_cli/` | (in `rivetsql`) | `rivet` CLI + REPL |
| `src/rivet_<plugin>/` | `rivetsql-<plugin>` | One package per plugin, each with its own pyproject |

## Plugin coherence

Every plugin lives in its own published distribution. Run
`python scripts/check_plugin_coherence.py` to verify the capability matrix
in the docs matches what each plugin actually exposes.

## Building locally

```bash
cd rivet && hatch build              # builds rivetsql
cd rivet/src/rivet_duckdb && hatch build   # builds rivetsql-duckdb
```
