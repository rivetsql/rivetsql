# Unity / Databricks

The `rivet-databricks` plugin provides a compute engine (`databricks`) and two catalog plugins (`unity` for Unity Catalog REST API, `databricks` for Databricks-managed catalogs).

```bash
pip install 'rivetsql[databricks]'
```

---

## Engine Configuration

The Databricks engine executes SQL via the Statement Execution API against a SQL warehouse.

```yaml
default:
  engines:
    - name: dbx
      type: databricks
      options:
        warehouse_id: abc123def456
        workspace_url: https://my-workspace.cloud.databricks.com
        token: ${DATABRICKS_TOKEN}
        wait_timeout: "30s"
        max_rows_per_chunk: 100000
      catalogs: [unity_catalog, dbx_catalog]
```

### Engine Options

| Option | Required | Type | Default | Description |
|--------|----------|------|---------|-------------|
| `warehouse_id` | yes | `str` | — | SQL warehouse ID |
| `workspace_url` | yes | `str` | — | Workspace URL (`https://...`) |
| `token` | yes | `str` | — | Personal access token |
| `wait_timeout` | no | `str` | `"30s"` | Statement execution timeout |
| `max_rows_per_chunk` | no | `int` | `100000` | Max rows per Arrow chunk |

### Supported Write Strategies

All seven: `append`, `replace`, `truncate_insert`, `merge`, `delete_insert`, `incremental_append`, `scd2`

### Cross-Engine Adapters

| Adapter | Requires | Description |
|---------|----------|-------------|
| `DatabricksUnityAdapter` | — | Read/write Unity tables through Databricks |
| `DatabricksDuckDBAdapter` | `duckdb` | Read Databricks/Unity tables from local DuckDB |
| `DatabricksCrossJointAdapter` | — | Cross-engine joins |

---

## Unity Catalog

```yaml
default:
  catalogs:
    - name: unity_catalog
      type: unity
      options:
        host: https://my-workspace.cloud.databricks.com
        catalog_name: main
        schema: default
        token: ${DATABRICKS_TOKEN}
```

### Unity Options

| Option | Required | Type | Default | Description |
|--------|----------|------|---------|-------------|
| `host` | yes | `str` | — | Unity Catalog server URL |
| `catalog_name` | yes | `str` | — | Catalog name |
| `schema` | no | `str` | `None` | Default schema |

### Unity Credentials

| Option | Description |
|--------|-------------|
| `token` | Personal access token (env: `DATABRICKS_TOKEN`) |
| `client_id` | OAuth M2M client ID |
| `client_secret` | OAuth M2M client secret |

Auth types: `pat`, `oauth_m2m`, `azure_cli`, `gcp_login`. Resolves via: explicit options → env vars → `~/.databrickscfg` → cloud-native auth.

---

## Databricks Catalog

```yaml
default:
  catalogs:
    - name: dbx_catalog
      type: databricks
      options:
        workspace_url: https://my-workspace.cloud.databricks.com
        catalog: main
        schema: default
        token: ${DATABRICKS_TOKEN}
```

### Databricks Options

| Option | Required | Type | Default | Description |
|--------|----------|------|---------|-------------|
| `workspace_url` | yes | `str` | — | Workspace URL |
| `catalog` | yes | `str` | — | Catalog name |
| `schema` | no | `str` | `"default"` | Default schema |
| `http_path` | no | `str` | `None` | SQL warehouse HTTP path |

---

## Usage Examples

### Source from Unity

=== "SQL"

    ```sql
    -- rivet:name: raw_sales
    -- rivet:type: source
    -- rivet:catalog: unity_catalog
    -- rivet:table: main.sales.transactions
    ```

=== "YAML"

    ```yaml
    name: raw_sales
    type: source
    catalog: unity_catalog
    table: main.sales.transactions
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    raw_sales = Joint(
        name="raw_sales",
        joint_type="source",
        catalog="unity_catalog",
        table="main.sales.transactions",
    )
    ```

### Sink to Databricks

=== "SQL"

    ```sql
    -- rivet:name: write_summary
    -- rivet:type: sink
    -- rivet:upstream: daily_summary
    -- rivet:catalog: dbx_catalog
    -- rivet:table: main.analytics.daily_summary
    -- rivet:write_strategy: merge
    -- rivet:merge_keys: date_key
    ```

=== "YAML"

    ```yaml
    name: write_summary
    type: sink
    upstream: daily_summary
    catalog: dbx_catalog
    table: main.analytics.daily_summary
    write_strategy:
      mode: merge
      key_columns: [date_key]
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    write_summary = Joint(
        name="write_summary",
        joint_type="sink",
        upstream=["daily_summary"],
        catalog="dbx_catalog",
        table="main.analytics.daily_summary",
        write_strategy="merge",
    )
    ```

---

## Known Limitations

- **Network dependency** — Requires Databricks workspace connectivity. Subject to warehouse auto-scaling delays.
- **Warehouse startup** — Serverless/auto-stopped warehouses may take 30-120s. Adjust `wait_timeout`.
- **Result size** — `EXTERNAL_LINKS` disposition streams Arrow IPC chunks via pre-signed URLs.
- **Auth complexity** — Multiple methods (PAT, OAuth M2M, Azure AD, GCP). Ensure correct credentials for your workspace type.
- **Three-part table names** — Unity Catalog uses `catalog.schema.table`. Include all three parts when needed.
