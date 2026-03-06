<div align="center">
  <h1>rivetsql-aws</h1>
  <p><b>S3 and Glue Data Catalog plugin for Rivet.</b></p>

  [![PyPI version](https://img.shields.io/pypi/v/rivetsql-aws)](https://pypi.org/project/rivetsql-aws/)
  [![Python versions](https://img.shields.io/pypi/pyversions/rivetsql-aws)](https://pypi.org/project/rivetsql-aws/)
  [![License](https://img.shields.io/github/license/rivetsql/rivetsql)](https://github.com/rivetsql/rivetsql/blob/main/LICENSE)
</div>

---

Two catalog plugins in one package: `s3` for S3 object storage and `glue` for the AWS Glue Data Catalog. Supports Parquet, CSV, and Delta Lake formats.

---

## ⚡ Install

```sh
pip install rivetsql[aws]
```

---

## 🔧 Configuration

```yaml
# profiles.yaml
default:
  catalogs:
    - name: lake
      type: s3
      options:
        bucket: my-data-lake
        prefix: raw/
        region: us-east-1
        format: parquet
    - name: catalog
      type: glue
      options:
        database: analytics
        region: us-east-1
```

---

## ✨ Capabilities

| Feature | Supported |
|---|---|
| S3 catalog | ✅ |
| Glue catalog | ✅ |
| Read | ✅ |
| Write | ✅ |
| List tables | ✅ |
| Schema introspection | ✅ |
| Test connection | ✅ |

---

## 📚 Documentation

Full docs at **[rivetsql.github.io/rivet/plugins/aws](https://rivetsql.github.io/rivet/plugins/aws/)**

---

<div align="center">
  <i>Part of the <a href="https://github.com/rivetsql/rivetsql">Rivet</a> framework.</i>
</div>
