<div align="center">
  <h1>rivetsql-core</h1>
  <p><b>Core engine for Rivet — joints, engines, catalogs, compilation, and the bridge layer.</b></p>

  [![PyPI version](https://img.shields.io/pypi/v/rivetsql-core)](https://pypi.org/project/rivetsql-core/)
  [![Python versions](https://img.shields.io/pypi/pyversions/rivetsql-core)](https://pypi.org/project/rivetsql-core/)
  [![License](https://img.shields.io/github/license/rivetsql/rivetsql)](https://github.com/rivetsql/rivetsql/blob/main/LICENSE)
</div>

---

This is the foundational package of the [Rivet](https://github.com/rivetsql/rivetsql) framework. It provides the core abstractions — joints, engines, catalogs, compilation, and the bridge layer — that all plugins build on top of.

> **Most users should install `rivetsql` instead.** This package is for plugin authors and advanced use cases that need the core without the CLI.

---

## ⚡ Install

```sh
pip install rivetsql-core
```

Or just install the full framework:

```sh
pip install rivetsql
```

---

## 🧱 What's Inside

| Module | Purpose |
|---|---|
| `rivet_core` | Joint model, plugin ABCs, materialization, quality checks |
| `rivet_config` | Profile and project YAML parsing |
| `rivet_bridge` | DAG compilation, execution planning, engine dispatch |

---

## 🔌 Built-in Plugins

Two plugins ship with `rivetsql-core` and require no extra installation:

| Plugin | Type | Description |
|---|---|---|
| `arrow` | Catalog + Engine | In-memory PyArrow catalog for testing and intermediate materialization |
| `filesystem` | Catalog | Local filesystem supporting CSV and Parquet |

---

## 📚 Documentation

Full docs at **[rivetsql.github.io/rivet](https://rivetsql.github.io/rivet)**

---

<div align="center">
  <i>Part of the <a href="https://github.com/rivetsql/rivetsql">Rivet</a> framework.</i>
</div>
