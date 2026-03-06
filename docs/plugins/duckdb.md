# DuckDB

The `rivet-duckdb` plugin provides both a compute engine and a catalog for DuckDB. Recommended default for local analytics, fast SQL on files, and prototyping pipelines.

```bash
pip install rivetsql[duckdb]
```

---

## Engine Configuration

```yaml
default:
  engines:
    - name: local
      type: duckdb
      options:
        threads: 4
        memory_limit: "8GB"
        temp_directory: /tmp/duckdb
        extensions: [httpfs, parquet]
      catalogs: [warehouse, files]
```

### Engine Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `threads` | `int` | System default | Threads for parallel execution |
| `memory_limit` | `str` | `"4GB"` | Max memory (e.g. `"4GB"`, `"512MB"`) |
| `temp_directory` | `str` | `None` | Spill-to-disk directory |
| `extensions` | `list[str]` | `[]` | Extensions to load at startup |

### Supported Catalog Types

| Catalog type | Capabilities |
|--------------|-------------|
| `duckdb` | projection, predicate, limit, cast pushdown; join; aggregation |
| `arrow` | projection, predicate, limit, cast pushdown; join; aggregation |
| `filesystem` | projection, predicate, limit, cast pushdown; join; aggregation |

### Cross-Catalog Adapters

| Adapter | Requires | Description |
|---------|----------|-------------|
| `S3DuckDBAdapter` | `boto3` | Read/write S3 via `httpfs` |
| `GlueDuckDBAdapter` | `boto3` | Read/write Glue-managed tables |
| `UnityDuckDBAdapter` | `requests` | Read/write Unity Catalog tables |

---

## Catalog Configuration

```yaml
default:
  catalogs:
    - name: warehouse
      type: duckdb
      options:
        path: warehouse.duckdb
        read_only: false
        schema: main
```

### Catalog Options

| Option | Required | Type | Default | Description |
|--------|----------|------|---------|-------------|
| `path` | No | `str` | `":memory:"` | Database file path, or `":memory:"` |
| `read_only` | No | `bool` | `false` | Open in read-only mode |
| `schema` | No | `str` | `None` | Default schema |

### Capabilities

| Operation | Supported |
|-----------|:---------:|
| List tables | :material-check: |
| Get schema | :material-check: |
| Get metadata | :material-check: |
| Test connection | :material-check: |

---

## File Formats

When reading from filesystem catalogs, DuckDB auto-detects the reader:

| Extension | Reader |
|-----------|--------|
| `.parquet` | `read_parquet` |
| `.csv`, `.tsv` | `read_csv_auto` |
| `.json`, `.ndjson`, `.jsonl` | `read_json_auto` |

---

## Usage Examples

### Source

=== "SQL"

    ```sql
    -- rivet:name: raw_orders
    -- rivet:type: source
    -- rivet:catalog: warehouse
    -- rivet:table: orders
    ```

=== "YAML"

    ```yaml
    name: raw_orders
    type: source
    catalog: warehouse
    table: orders
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    raw_orders = Joint(
        name="raw_orders",
        joint_type="source",
        catalog="warehouse",
        table="orders",
    )
    ```

### Transform

=== "SQL"

    ```sql
    -- rivet:name: order_totals
    -- rivet:type: sql
    -- rivet:upstream: raw_orders
    SELECT customer_id, SUM(amount) AS total
    FROM raw_orders
    GROUP BY customer_id
    ```

=== "YAML"

    ```yaml
    name: order_totals
    type: sql
    upstream: [raw_orders]
    sql: |
      SELECT customer_id, SUM(amount) AS total
      FROM raw_orders
      GROUP BY customer_id
    ```

=== "Python"

    ```python
    # joints/order_totals.py
    # rivet:name: order_totals
    # rivet:type: python
    # rivet:upstream: [raw_orders]
    import pyarrow as pa
    import pyarrow.compute as pc
    from rivet_core.models import Material

    def transform(material: Material) -> pa.Table:
        table = material.to_arrow()
        totals = table.group_by("customer_id").aggregate([("amount", "sum")])
        return totals.rename_columns(["customer_id", "total"])
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    order_totals = Joint(
        name="order_totals",
        joint_type="sql",
        upstream=["raw_orders"],
        sql="SELECT customer_id, SUM(amount) AS total FROM raw_orders GROUP BY customer_id",
    )
    ```

### Sink

=== "SQL"

    ```sql
    -- rivet:name: write_totals
    -- rivet:type: sink
    -- rivet:upstream: order_totals
    -- rivet:catalog: warehouse
    -- rivet:table: customer_totals
    -- rivet:write_strategy: replace
    ```

=== "YAML"

    ```yaml
    name: write_totals
    type: sink
    upstream: order_totals
    catalog: warehouse
    table: customer_totals
    write_strategy: replace
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    write_totals = Joint(
        name="write_totals",
        joint_type="sink",
        upstream=["order_totals"],
        catalog="warehouse",
        table="customer_totals",
        write_strategy="replace",
    )
    ```

---

## Known Limitations

- **Single-process only** — DuckDB runs in-process and cannot distribute across machines. For scale, consider PySpark or Databricks.
- **Concurrent writes** — Single writer at a time. Concurrent pipeline runs writing to the same database file will fail.
- **Extension availability** — Cross-catalog adapters require optional packages (`boto3`, `requests`). Silently skipped if not installed.
- **Memory pressure** — Large queries may exceed `memory_limit`. Set `temp_directory` to enable spill-to-disk.
