<div align="center">
  <h1>rivetsql-postgres</h1>
  <p><b>PostgreSQL engine and catalog plugin for Rivet.</b></p>

  [![PyPI version](https://img.shields.io/pypi/v/rivetsql-postgres)](https://pypi.org/project/rivetsql-postgres/)
  [![Python versions](https://img.shields.io/pypi/pyversions/rivetsql-postgres)](https://pypi.org/project/rivetsql-postgres/)
  [![License](https://img.shields.io/github/license/rivetsql/rivetsql)](https://github.com/rivetsql/rivetsql/blob/main/LICENSE)
</div>

---

Full PostgreSQL support with async connection pooling, server-side cursors for streaming large result sets, and all seven write strategies. Uses `psycopg3` under the hood.

---

## ⚡ Install

```sh
pip install rivetsql[postgres]
```

---

## 🔧 Configuration

```yaml
# profiles.yaml
default:
  engines:
    - name: pg
      type: postgres
      options:
        statement_timeout: 60000
        pool_min_size: 2
        pool_max_size: 10
      catalogs: [warehouse]
  catalogs:
    - name: warehouse
      type: postgres
      options:
        conninfo: "host=localhost dbname=analytics"
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
| Cross-joint adapter | ✅ |

---

## 📚 Documentation

Full docs at **[rivetsql.github.io/rivet/plugins/postgres](https://rivetsql.github.io/rivet/plugins/postgres/)**

---

<div align="center">
  <i>Part of the <a href="https://github.com/rivetsql/rivetsql">Rivet</a> framework.</i>
</div>
