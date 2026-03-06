<div align="center">
  <h1>rivetsql-duckdb</h1>
  <p><b>DuckDB engine and catalog plugin for Rivet.</b></p>

  [![PyPI version](https://img.shields.io/pypi/v/rivetsql-duckdb)](https://pypi.org/project/rivetsql-duckdb/)
  [![Python versions](https://img.shields.io/pypi/pyversions/rivetsql-duckdb)](https://pypi.org/project/rivetsql-duckdb/)
  [![License](https://img.shields.io/github/license/rivetsql/rivetsql)](https://github.com/rivetsql/rivetsql/blob/main/LICENSE)
</div>

---

Fast in-process SQL engine with filesystem and object store catalogs. The recommended default for local analytics, prototyping, and file-based pipelines.

---

## ⚡ Install

```sh
pip install rivetsql[duckdb]
```

---

## 🔧 Configuration

```yaml
# profiles.yaml
default:
  engines:
    - name: local
      type: duckdb
      options:
        threads: 4
        memory_limit: "8GB"
        extensions: [httpfs, parquet]
      catalogs: [warehouse, files]
  catalogs:
    - name: warehouse
      type: duckdb
      options:
        path: warehouse.duckdb
```

---

## ✨ Capabilities

| Feature | Supported |
|---|---|
| Compute engine | ✅ |
| Catalog | ✅ |
| Read | ✅ |
| Write | ✅ |
| List tables | ✅ |
| Schema introspection | ✅ |
| Test connection | ✅ |

---

## 📚 Documentation

Full docs at **[rivetsql.github.io/rivet/plugins/duckdb](https://rivetsql.github.io/rivet/plugins/duckdb/)**

---

<div align="center">
  <i>Part of the <a href="https://github.com/rivetsql/rivetsql">Rivet</a> framework.</i>
</div>
