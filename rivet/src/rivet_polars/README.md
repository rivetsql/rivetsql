<div align="center">
  <h1>rivetsql-polars</h1>
  <p><b>Polars engine plugin for Rivet.</b></p>

  [![PyPI version](https://img.shields.io/pypi/v/rivetsql-polars)](https://pypi.org/project/rivetsql-polars/)
  [![Python versions](https://img.shields.io/pypi/pyversions/rivetsql-polars)](https://pypi.org/project/rivetsql-polars/)
  [![License](https://img.shields.io/github/license/rivetsql/rivetsql)](https://github.com/rivetsql/rivetsql/blob/main/LICENSE)
</div>

---

In-process DataFrame engine backed by Polars. Executes SQL via `polars.SQLContext` with support for lazy evaluation and optional streaming. Includes adapters for S3, Glue, and Unity Catalog.

---

## ⚡ Install

```sh
pip install rivetsql[polars]
```

For Delta Lake support:

```sh
pip install rivetsql[polars] deltalake
```

---

## 🔧 Configuration

```yaml
# profiles.yaml
default:
  engines:
    - name: fast
      type: polars
      options:
        streaming: true
        n_threads: 4
      catalogs: [local]
```

---

## ✨ Capabilities

| Feature | Supported |
|---|---|
| Compute engine | ✅ |
| Read | ✅ |
| S3 adapter | ✅ |
| Glue adapter | ✅ |
| Unity adapter | ✅ |

---

## 📚 Documentation

Full docs at **[rivetsql.github.io/rivet/plugins/polars](https://rivetsql.github.io/rivet/plugins/polars/)**

---

<div align="center">
  <i>Part of the <a href="https://github.com/rivetsql/rivetsql">Rivet</a> framework.</i>
</div>
