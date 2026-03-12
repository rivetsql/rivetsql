<div align="center">
  <h1>rivetsql-rest</h1>
  <p><b>REST API catalog plugin for Rivet.</b></p>

  [![PyPI version](https://img.shields.io/pypi/v/rivetsql-rest)](https://pypi.org/project/rivetsql-rest/)
  [![Python versions](https://img.shields.io/pypi/pyversions/rivetsql-rest)](https://pypi.org/project/rivetsql-rest/)
  [![License](https://img.shields.io/github/license/rivetsql/rivetsql)](https://github.com/rivetsql/rivetsql/blob/main/LICENSE)
</div>

---

Treat REST API endpoints as tables. Handles authentication, pagination, response parsing, schema inference, and Arrow conversion automatically. Works with any Arrow-compatible engine (DuckDB, Polars, PySpark) through wildcard adapter architecture.

---

## ⚡ Install

```sh
pip install rivetsql[rest]
```

---

## 🔧 Configuration

```yaml
# profiles.yaml
default:
  engines:
    - name: local
      type: duckdb
      catalogs: [my_api]
  catalogs:
    - name: my_api
      type: rest_api
      options:
        base_url: https://api.example.com/v1
        auth: bearer
        token: ${REST_API_TOKEN}
        default_headers:
          Accept: application/json
        timeout: 30
        max_flatten_depth: 3
        rate_limit:
          requests_per_second: 10
          burst: 5
        max_retries: 3
        endpoints:
          users:
            path: /users
            method: GET
            response_path: data.users
            pagination:
              strategy: offset
              limit: 100
            filter_params:
              status: status
              created_after: since
          orders:
            path: /orders
            method: GET
            response_path: data
            pagination:
              strategy: cursor
              cursor_field: next_cursor
```

---

## ✨ Capabilities

| Feature | Supported |
|---|---|
| Catalog | ✅ |
| Read | ✅ |
| Write | ✅ |
| List tables | ✅ |
| Schema introspection | ✅ |
| Test connection | ✅ |
| Predicate pushdown | ✅ |
| Projection pushdown | ✅ |
| Limit pushdown | ✅ |

---

## 🌐 Wildcard Adapter

The REST API plugin uses a wildcard adapter (`target_engine_type = "*"`) that works with any Arrow-compatible engine. When you configure a REST API catalog with DuckDB, Polars, or PySpark, the adapter automatically handles data fetching and conversion.

**Engine Compatibility:**
- ✅ DuckDB — Arrow-compatible
- ✅ Polars — Arrow-compatible
- ✅ PySpark — Arrow-compatible
- ❌ Databricks — Not Arrow-compatible (use Unity catalog instead)

The wildcard adapter provides predicate pushdown (translates filters to query parameters), projection pushdown, and limit pushdown where supported by the API.

---

## 🔐 Authentication

Supported authentication strategies:
- `none` — No authentication
- `bearer` — Bearer token (Authorization header)
- `basic` — Basic auth (username/password)
- `api_key` — API key in header or query parameter
- `oauth2` — OAuth2 client credentials grant with auto-refresh

---

## 📄 Pagination

Supported pagination strategies:
- `offset` — Offset/limit parameters
- `cursor` — Cursor-based pagination
- `page_number` — Page number pagination
- `link_header` — RFC 8288 Link header
- `none` — Single request (no pagination)

---

## 📚 Documentation

Full docs at **[rivetsql.github.io/rivet/plugins/rest](https://rivetsql.github.io/rivet/plugins/rest/)**

---

<div align="center">
  <i>Part of the <a href="https://github.com/rivetsql/rivetsql">Rivet</a> framework.</i>
</div>
