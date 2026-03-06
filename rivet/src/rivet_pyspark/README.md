<div align="center">
  <h1>rivetsql-pyspark</h1>
  <p><b>PySpark engine plugin for Rivet.</b></p>

  [![PyPI version](https://img.shields.io/pypi/v/rivetsql-pyspark)](https://pypi.org/project/rivetsql-pyspark/)
  [![Python versions](https://img.shields.io/pypi/pyversions/rivetsql-pyspark)](https://pypi.org/project/rivetsql-pyspark/)
  [![License](https://img.shields.io/github/license/rivetsql/rivetsql)](https://github.com/rivetsql/rivetsql/blob/main/LICENSE)
</div>

---

Distributed processing on Apache Spark. Supports both local Spark sessions and remote Spark Connect clusters for large-scale workloads.

---

## ⚡ Install

```sh
pip install rivetsql[pyspark]
```

---

## 🔧 Configuration

```yaml
# profiles.yaml
default:
  engines:
    - name: spark
      type: pyspark
      options:
        master: "local[*]"
        app_name: rivet
        config:
          spark.sql.adaptive.enabled: "true"
          spark.sql.shuffle.partitions: "200"
      catalogs: [local]
```

---

## ✨ Capabilities

| Feature | Supported |
|---|---|
| Compute engine | ✅ |
| Read | ✅ |
| Local mode | ✅ |
| Spark Connect | ✅ |

---

## 📚 Documentation

Full docs at **[rivetsql.github.io/rivet/plugins/pyspark](https://rivetsql.github.io/rivet/plugins/pyspark/)**

---

<div align="center">
  <i>Part of the <a href="https://github.com/rivetsql/rivetsql">Rivet</a> framework.</i>
</div>
