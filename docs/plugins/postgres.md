# Postgres

The `rivet-postgres` plugin provides both a compute engine and a catalog for PostgreSQL. It supports async connection pooling, server-side cursors for streaming large result sets, and all seven write strategies.

```bash
pip install 'rivetsql[postgres]'
```

---

## Engine Configuration

The PostgreSQL engine automatically inherits connection parameters from PostgreSQL catalogs it references. You can configure connection in three ways:

=== "Inherit from Catalog (Recommended)"

    ```yaml
    default:
      catalogs:
        - name: warehouse
          type: postgres
          options:
            host: localhost
            port: 5432
            database: analytics
            user: rivet_user
            password: ${PGPASSWORD}
            schema: public

      engines:
        - name: pg
          type: postgres
          catalogs: [warehouse]
          options:
            # Connection params inherited from 'warehouse' catalog
            statement_timeout: 60000
            pool_min_size: 2
            pool_max_size: 10
    ```

=== "Override Catalog Settings"

    ```yaml
    default:
      catalogs:
        - name: warehouse
          type: postgres
          options:
            host: localhost
            port: 5432
            database: analytics
            user: rivet_user
            password: ${PGPASSWORD}

      engines:
        - name: pg
          type: postgres
          catalogs: [warehouse]
          options:
            # Override specific params from catalog
            database: analytics_prod
            user: engine_user
            password: ${ENGINE_PASSWORD}
            pool_min_size: 2
    ```

=== "Explicit Configuration"

    ```yaml
    default:
      engines:
        - name: pg
          type: postgres
          options:
            host: localhost
            port: 5432
            database: analytics
            user: rivet_user
            password: ${PGPASSWORD}
            statement_timeout: 60000
            pool_min_size: 2
            pool_max_size: 10
          catalogs: [warehouse]
    ```

When an engine references a catalog of the same type (e.g., `postgres` engine + `postgres` catalog), connection parameters (`host`, `port`, `database`, `user`, `password`) are automatically inherited from the catalog. Engine options always take precedence, allowing selective overrides.

### Engine Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `conninfo` | `str` | — | PostgreSQL connection string (alternative to individual parameters) |
| `host` | `str` | `"localhost"` | Server hostname (inherited from catalog if not specified) |
| `port` | `int` | `5432` | Server port (inherited from catalog if not specified) |
| `database` | `str` | `""` | Database name (inherited from catalog if not specified) |
| `user` | `str` | `""` | PostgreSQL username (inherited from catalog if not specified) |
| `password` | `str` | `""` | PostgreSQL password (inherited from catalog if not specified, supports `${ENV_VAR}`) |
| `statement_timeout` | `int` | `None` | Query timeout in ms |
| `pool_min_size` | `int` | `1` | Min async pool connections |
| `pool_max_size` | `int` | `10` | Max async pool connections |
| `application_name` | `str` | `"rivet"` | Visible in `pg_stat_activity` |
| `connect_timeout` | `int` | `30` | Connection timeout in seconds |
| `fetch_batch_size` | `int` | `10000` | Rows per batch for server-side cursors |
| `concurrency_limit` | `int` | `1` | Max fused groups executing in parallel. Match your connection pool size, typically `2`–`8`. |

Connection parameters (`host`, `port`, `database`, `user`, `password`) are automatically inherited from any PostgreSQL catalog referenced in the engine's `catalogs` list. Engine options always take precedence for overrides. You can also use `conninfo` for a complete connection string, but this disables catalog inheritance.

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

### Complex Type Support

PostgreSQL supports array types using the `type[]` syntax:

- **Arrays**: `integer[]`, `text[]`, `timestamp[]`, etc.
- Arrays are automatically mapped to Arrow list types during schema introspection

PostgreSQL does not have native struct types. JSONB columns remain mapped to `large_utf8` (string) because JSONB is schema-less and Arrow requires fixed schemas.

See [Complex Type Support](../concepts/catalogs.md#complex-type-support) for details.

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
