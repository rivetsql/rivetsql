# Polars

The `rivet-polars` plugin provides a compute engine backed by the Polars DataFrame library. It executes SQL via `polars.SQLContext` and supports lazy evaluation with optional streaming.

```bash
pip install 'rivetsql[polars]'
```

---

## Engine Configuration

```yaml
default:
  engines:
    - name: fast
      type: polars
      options:
        streaming: true
        n_threads: 4
        check_dtypes: true
      catalogs: [local]
```

### Engine Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `streaming` | `bool` | `false` | Enable streaming engine for reduced memory |
| `n_threads` | `int` | `None` | Threads for parallel execution |
| `check_dtypes` | `bool` | `true` | Validate data types during operations |

### Supported Catalog Types

| Catalog type | Capabilities |
|--------------|-------------|
| `arrow` | projection, predicate, limit, cast pushdown; join; aggregation |
| `filesystem` | projection, predicate, limit, cast pushdown; join; aggregation |

### Cross-Catalog Adapters

| Adapter | Requires | Description |
|---------|----------|-------------|
| `S3PolarsAdapter` | `boto3` | Read S3 objects via Polars |
| `GluePolarsAdapter` | `boto3` | Read Glue-managed tables |
| `UnityPolarsAdapter` | `requests` | Read Unity Catalog tables |

---

## Usage Examples

### Source from filesystem

=== "SQL"

    ```sql
    -- rivet:name: raw_events
    -- rivet:type: source
    -- rivet:catalog: local
    -- rivet:table: events
    ```

=== "YAML"

    ```yaml
    name: raw_events
    type: source
    catalog: local
    table: events
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    raw_events = Joint(
        name="raw_events",
        joint_type="source",
        catalog="local",
        table="events",
    )
    ```

### Transform

=== "SQL"

    ```sql
    -- rivet:name: daily_counts
    -- rivet:type: sql
    -- rivet:upstream: raw_events
    SELECT event_date, COUNT(*) AS cnt
    FROM raw_events
    GROUP BY event_date
    ```

=== "YAML"

    ```yaml
    name: daily_counts
    type: sql
    upstream: [raw_events]
    sql: |
      SELECT event_date, COUNT(*) AS cnt
      FROM raw_events
      GROUP BY event_date
    ```

=== "Python"

    ```python
    # joints/daily_counts.py
    # rivet:name: daily_counts
    # rivet:type: python
    # rivet:upstream: [raw_events]
    import pyarrow as pa
    from rivet_core.models import Material

    def transform(material: Material) -> pa.Table:
        table = material.to_arrow()
        return table.group_by("event_date").aggregate([("event_date", "count")])
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    daily_counts = Joint(
        name="daily_counts",
        joint_type="sql",
        upstream=["raw_events"],
        sql="SELECT event_date, COUNT(*) AS cnt FROM raw_events GROUP BY event_date",
    )
    ```

---

## Optional Dependencies

| Extra | Package | Purpose |
|-------|---------|---------|
| `delta` | `deltalake>=0.14` | Read and write Delta Lake tables |

```bash
pip install 'rivetsql[polars]' deltalake
```

---

## Known Limitations

- **No catalog plugin** — Polars provides only a compute engine. Pair with `filesystem` or `arrow`.
- **No native sink support** — Write operations require a catalog that provides its own sink.
- **SQL dialect** — Uses DuckDB SQL dialect via `polars.SQLContext`. Some advanced features may not be supported.
- **Streaming limitations** — Some operations may fall back to non-streaming execution.
