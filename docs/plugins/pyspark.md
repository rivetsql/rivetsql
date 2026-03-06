# PySpark

The `rivet-pyspark` plugin provides a compute engine for Apache Spark. It supports both local Spark sessions and remote Spark Connect clusters for large-scale distributed processing.

```bash
pip install 'rivetsql[pyspark]'
```

---

## Engine Configuration

```yaml
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

### Engine Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `master` | `str` | `"local[*]"` | Spark master URL |
| `app_name` | `str` | `"rivet"` | Application name in Spark UI |
| `config` | `dict` | `{}` | Arbitrary Spark config key-value pairs |
| `spark_home` | `str` | `None` | Path to Spark installation |
| `packages` | `list[str]` | `[]` | Maven coordinates to include |
| `connect_url` | `str` | `None` | Spark Connect remote URL |

### Supported Catalog Types

| Catalog type | Capabilities |
|--------------|-------------|
| `arrow` | projection, predicate, limit, cast pushdown; join; aggregation |
| `filesystem` | projection, predicate, limit, cast pushdown; join; aggregation |

### Cross-Catalog Adapters

| Adapter | Requires | Description |
|---------|----------|-------------|
| `S3PySparkAdapter` | `boto3` | Read/write S3 via Hadoop S3A |
| `GluePySparkAdapter` | `boto3` | Read/write Glue-managed tables |
| `UnityPySparkAdapter` | `requests` | Read/write Unity Catalog tables |

---

## Spark Connect

For remote clusters, use `connect_url` instead of `master`:

```yaml
default:
  engines:
    - name: remote_spark
      type: pyspark
      options:
        connect_url: "sc://spark-cluster.example.com:15002"
      catalogs: [local]
```

---

## Session Lifecycle

- **Lazy creation** — `SparkSession` is created on first use
- **Singleton reuse** — existing active sessions are reused
- **Teardown** — `spark.stop()` is called at pipeline completion (unless externally managed)

---

## Usage Examples

### Source

=== "SQL"

    ```sql
    -- rivet:name: raw_logs
    -- rivet:type: source
    -- rivet:catalog: local
    -- rivet:table: logs
    ```

=== "YAML"

    ```yaml
    name: raw_logs
    type: source
    catalog: local
    table: logs
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    raw_logs = Joint(
        name="raw_logs",
        joint_type="source",
        catalog="local",
        table="logs",
    )
    ```

### Transform

=== "SQL"

    ```sql
    -- rivet:name: error_counts
    -- rivet:type: sql
    -- rivet:upstream: raw_logs
    SELECT level, COUNT(*) AS cnt
    FROM raw_logs
    WHERE level = 'ERROR'
    GROUP BY level
    ```

=== "YAML"

    ```yaml
    name: error_counts
    type: sql
    upstream: [raw_logs]
    sql: |
      SELECT level, COUNT(*) AS cnt
      FROM raw_logs
      WHERE level = 'ERROR'
      GROUP BY level
    ```

=== "Python"

    ```python
    # joints/error_counts.py
    # rivet:name: error_counts
    # rivet:type: python
    # rivet:upstream: [raw_logs]
    import pyarrow as pa
    import pyarrow.compute as pc
    from rivet_core.models import Material

    def transform(material: Material) -> pa.Table:
        table = material.to_arrow()
        errors = table.filter(pc.equal(table["level"], "ERROR"))
        return errors.group_by("level").aggregate([("level", "count")])
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    error_counts = Joint(
        name="error_counts",
        joint_type="sql",
        upstream=["raw_logs"],
        sql="SELECT level, COUNT(*) AS cnt FROM raw_logs WHERE level = 'ERROR' GROUP BY level",
    )
    ```

---

## Known Limitations

- **No catalog plugin** — PySpark provides only a compute engine. Pair with `filesystem`, `arrow`, `s3`, or `glue`.
- **No native sink support** — Write operations require a catalog that provides its own sink.
- **JVM dependency** — Requires a Java runtime. Ensure `JAVA_HOME` is set with a compatible JDK (8 or 11).
- **Startup overhead** — SparkSession creation is slow compared to DuckDB or Polars.
- **SQL dialect** — Spark SQL dialect differs from DuckDB/PostgreSQL in some functions and syntax.
- **Arrow conversion** — Results are converted via `toArrow()` (Spark 3.3+) or `toPandas()` fallback.
