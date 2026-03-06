# AWS (S3 + Glue)

The `rivet-aws` plugin provides two catalog plugins: `s3` for S3 object storage and `glue` for the AWS Glue Data Catalog.

```bash
pip install 'rivetsql[aws]'
```

---

## S3 Catalog

The S3 catalog treats an S3 bucket (with optional prefix) as a data store for file-based tables.

```yaml
default:
  catalogs:
    - name: lake
      type: s3
      options:
        bucket: my-data-lake
        prefix: raw/
        region: us-east-1
        format: parquet
```

### S3 Options

| Option | Required | Type | Default | Description |
|--------|----------|------|---------|-------------|
| `bucket` | yes | `str` | — | S3 bucket name |
| `prefix` | no | `str` | `""` | Key prefix |
| `region` | no | `str` | `"us-east-1"` | AWS region |
| `endpoint_url` | no | `str` | `None` | Custom endpoint (MinIO, LocalStack) |
| `format` | no | `str` | `"parquet"` | Default format (`parquet`, `csv`, `json`, `orc`, `delta`) |

### S3 Credentials

| Option | Description |
|--------|-------------|
| `access_key_id` | AWS access key ID |
| `secret_access_key` | AWS secret access key |
| `session_token` | Temporary session token (STS) |
| `profile` | AWS CLI profile from `~/.aws/credentials` |
| `role_arn` | IAM role ARN to assume |
| `auth_type` | `iam_keys`, `profile`, `assume_role`, `web_identity`, `default` |

Falls back to the default AWS credential chain if no explicit credentials are provided.

---

## Glue Catalog

The Glue catalog connects to the AWS Glue Data Catalog for schema-managed access to S3-backed tables.

```yaml
default:
  catalogs:
    - name: glue_db
      type: glue
      options:
        database: analytics
        region: us-east-1
        catalog_id: "123456789012"
```

### Glue Options

| Option | Required | Type | Default | Description |
|--------|----------|------|---------|-------------|
| `database` | no | `str` | `None` | Glue database name |
| `region` | no | `str` | `"us-east-1"` | AWS region |
| `catalog_id` | no | `str` | `None` | AWS account ID for cross-account |
| `lf_enabled` | no | `bool` | `false` | Use Lake Formation vended credentials |

Uses the same credential options as S3.

---

## Engine

The AWS plugin does not provide a compute engine. Pair S3 or Glue catalogs with an engine that has cross-catalog adapters:

| Engine | S3 | Glue |
|--------|:--:|:----:|
| DuckDB | :material-check: (via `httpfs`) | :material-check: |
| Polars | :material-check: | :material-check: |
| PySpark | :material-check: (via Hadoop S3A) | :material-check: |

---

## Usage Examples

### S3 source

=== "SQL"

    ```sql
    -- rivet:name: raw_events
    -- rivet:type: source
    -- rivet:catalog: lake
    -- rivet:table: events
    ```

=== "YAML"

    ```yaml
    name: raw_events
    type: source
    catalog: lake
    table: events
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    raw_events = Joint(
        name="raw_events",
        joint_type="source",
        catalog="lake",
        table="events",
    )
    ```

### S3 sink

=== "SQL"

    ```sql
    -- rivet:name: write_results
    -- rivet:type: sink
    -- rivet:upstream: transformed
    -- rivet:catalog: lake
    -- rivet:table: results
    -- rivet:write_strategy: append
    ```

=== "YAML"

    ```yaml
    name: write_results
    type: sink
    upstream: transformed
    catalog: lake
    table: results
    write_strategy: append
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    write_results = Joint(
        name="write_results",
        joint_type="sink",
        upstream=["transformed"],
        catalog="lake",
        table="results",
        write_strategy="append",
    )
    ```

---

## Lake Formation

When `lf_enabled: true`, the plugin uses Lake Formation for temporary table-level credentials:

```yaml
default:
  catalogs:
    - name: governed
      type: glue
      options:
        database: analytics
        region: us-east-1
        lf_enabled: true
```

---

## Known Limitations

- **No compute engine** — Pair with DuckDB, Polars, or PySpark.
- **Credential complexity** — Multiple auth methods supported. Start with explicit keys for testing, then move to IAM roles.
- **S3 listing** — Listing operations on prefixes with many objects may be slow.
- **Glue API rate limits** — Heavy `list_tables`/`get_schema` usage may hit throttling. Consider caching.
- **Lake Formation** — LF-vended credentials have limited lifetime. Long pipelines may need refresh.
