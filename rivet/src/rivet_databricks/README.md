<div align="center">
  <h1>rivetsql-databricks</h1>
  <p><b>Databricks engine and Unity Catalog plugin for Rivet.</b></p>

  [![PyPI version](https://img.shields.io/pypi/v/rivetsql-databricks)](https://pypi.org/project/rivetsql-databricks/)
  [![Python versions](https://img.shields.io/pypi/pyversions/rivetsql-databricks)](https://pypi.org/project/rivetsql-databricks/)
  [![License](https://img.shields.io/github/license/rivetsql/rivetsql)](https://github.com/rivetsql/rivetsql/blob/main/LICENSE)
</div>

---

Compute engine via the Databricks Statement Execution API and two catalog plugins: `unity` for Unity Catalog REST API and `databricks` for Databricks-managed catalogs.

---

## ⚡ Install

```sh
pip install rivetsql[databricks]
```

---

## 🔧 Configuration

```yaml
# profiles.yaml
default:
  engines:
    - name: dbx
      type: databricks
      options:
        warehouse_id: abc123def456
        workspace_url: https://my-workspace.cloud.databricks.com
        token: ${DATABRICKS_TOKEN}
      catalogs: [unity_catalog]
  catalogs:
    - name: unity_catalog
      type: unity
      options:
        workspace_url: https://my-workspace.cloud.databricks.com
        token: ${DATABRICKS_TOKEN}
        catalog: main
        schema: default
```

---

## ✨ Capabilities

| Feature | Supported |
|---|---|
| Compute engine | ✅ |
| Unity Catalog | ✅ |
| Databricks catalog | ✅ |
| Read | ✅ |
| Write | ✅ |
| List tables | ✅ |
| Schema introspection | ✅ |
| Test connection | ✅ |
| Cross-joint adapter | ✅ |

---

## 📚 Documentation

Full docs at **[rivetsql.github.io/rivet/plugins/unity](https://rivetsql.github.io/rivet/plugins/unity/)**

---

<div align="center">
  <i>Part of the <a href="https://github.com/rivetsql/rivetsql">Rivet</a> framework.</i>
</div>
