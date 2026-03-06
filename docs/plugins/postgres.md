# Postgres

The `rivet-postgres` plugin provides both a compute engine and a catalog for PostgreSQL. It supports async connection pooling, server-side cursors for streaming large result sets, and all seven write strategies.

```bash
pip install 'rivetsql[postgres]'
```

---

## Engine Configuration

```yaml
default:
  engines:
    - name: pg
      type: postgres
      options:
        statement_timeout: 60000
        pool_min_size: 2
        pool_max_size: 10
        application_name: rivet
        connect_timeout: 30
        fetch_batch_size: 10000
      catalogs: [warehouse]
```

### Engine Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `statement_timeout` | `int` | `None` | Query timeout in ms |
| `pool_min_size` | `int` | `1` | Min async pool connections |
| `pool_max_size` | `int` | `10` | Max async pool connections |
| `application_name` | `str` | `"rivet"` | Visible in `pg_stat_activity` |
| `connect_timeout` | `int` | `30` | Connection timeout in seconds |
| `fetch_batch_size` | `int` | `10000` | Rows per batch for server-side cursors |

### Supported Write Strategies

All seven: `append`, `replace`, `truncate_insert`, `merge`, `delete_insert`, `incremental_append`, `scd2`

### Cross-Catalog Adapters

| Adapter | Requires | Description |
|---------|----------|-------------|
| `PostgresDuckDBAdapter` | `duckdb` | Read Postgres from DuckDB via `postgres_scanner` |
| `PostgresPySparkAdapter` | `pyspark` | Read/write Postgres from PySpark via JDBC |
| `PostgresCrossJointAdapter` | — | Cross-engine joins between Postgres and other engines |

---

## Catalog Configuration

```yaml
default:
  catalogs:
    - name: warehouse
      type: postgres
      options:
        host: localhost
        database: analytics
        port: 5432
        user: rivet_user
        password: ${PGPASSWORD}
        schema: public
        ssl_mode: prefer
```

### Catalog Options

| Option | Required | Type | Default | Description |
|--------|----------|------|---------|-------------|
| `host` | yes | `str` | — | Server hostname |
| `database` | yes | `str` | — | Database name |
| `port` | no | `int` | `5432` | Server port |
| `schema` | no | `str` | `"public"` | Default schema |
| `ssl_mode` | no | `str` | `"prefer"` | SSL mode |
| `read_only` | no | `bool` | `false` | Read-only connections |

### Credential Options

| Option | Description |
|--------|-------------|
| `user` | PostgreSQL username |
| `password` | PostgreSQL password (supports `${ENV_VAR}`) |

### Capabilities

| Operation | Supported |
|-----------|:---------:|
| List tables | :material-check: |
| Get schema | :material-check: |
| Get metadata | :material-check: |
| Test connection | :material-check: |

---

## Usage Examples

### Source

=== "SQL"

    ```sql
    -- rivet:name: raw_customers
    -- rivet:type: source
    -- rivet:catalog: warehouse
    -- rivet:table: customers
    ```

=== "YAML"

    ```yaml
    name: raw_customers
    type: source
    catalog: warehouse
    table: customers
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    raw_customers = Joint(
        name="raw_customers",
        joint_type="source",
        catalog="warehouse",
        table="customers",
    )
    ```

### Sink with merge

=== "SQL"

    ```sql
    -- rivet:name: upsert_customers
    -- rivet:type: sink
    -- rivet:upstream: transformed_customers
    -- rivet:catalog: warehouse
    -- rivet:table: dim_customers
    -- rivet:write_strategy: merge
    -- rivet:merge_keys: customer_id
    ```

=== "YAML"

    ```yaml
    name: upsert_customers
    type: sink
    upstream: transformed_customers
    catalog: warehouse
    table: dim_customers
    write_strategy:
      mode: merge
      key_columns: [customer_id]
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    upsert_customers = Joint(
        name="upsert_customers",
        joint_type="sink",
        upstream=["transformed_customers"],
        catalog="warehouse",
        table="dim_customers",
        write_strategy="merge",
    )
    ```

---

## Known Limitations

- **Network dependency** — Requires a running PostgreSQL server. Connection failures produce `RVT-201` errors.
- **SSL certificates** — `verify-ca`/`verify-full` modes require valid `ssl_root_cert` path.
- **Async pool lifecycle** — Pool is created lazily and torn down automatically during execution.
- **Large result sets** — Server-side cursors stream in batches, but Arrow conversion may still require significant memory.
