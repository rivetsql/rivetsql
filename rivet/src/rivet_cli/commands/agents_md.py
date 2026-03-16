"""AGENTS.md template for rivet init."""

AGENTS_MD = """\
# Rivet — Agent Guide

This project uses [Rivet](https://github.com/rivetdata/rivet), a SQL-first
data pipeline tool. Follow these guidelines when helping the user build and
maintain pipelines.

## Getting Started

1. Check project health:
   ```
   rivet doctor --check-connections
   ```
2. Discover available data:
   ```
   rivet catalog list --depth 2 --format json
   ```
3. Compile the pipeline to verify correctness:
   ```
   rivet compile --format json
   ```

## Discovering Data

Use the CLI to explore catalogs, schemas, and tables — do not guess or
hard-code table names.

| Goal | Command |
|---|---|
| List all catalogs | `rivet catalog list` |
| List schemas in a catalog | `rivet catalog list <catalog> --depth 1` |
| List tables in a schema | `rivet catalog list <catalog>.<schema> --depth 1` |
| Show columns of a table | `rivet catalog describe <catalog>.<schema>.<table>` |
| Show columns with stats | `rivet catalog describe <catalog>.<schema>.<table> --stats` |
| Search for a table by name | `rivet catalog search <query> --format json` |
| Preview table data | `rivet repl execute -q "SELECT * FROM <table> LIMIT 10" --format json` |

When the user asks about a specific table or dataset, use `rivet catalog search`
first. Only fall back to `rivet catalog list --depth 2` when a broad overview is
needed.

## Creating Sources

Prefer generating sources from the catalog CLI over hand-writing them:

```bash
# Generate a source declaration and print to stdout
rivet catalog generate <catalog>.<schema>.<table> --stdout --format sql

# Write directly to the sources directory
rivet catalog generate <catalog>.<schema>.<table> --format sql --output sources/<name>.sql
```

Use `--format sql` by default. Use `--format yaml` only if the user explicitly
asks for YAML.

Use `--columns col1,col2,...` to select only the columns the pipeline needs.

## Writing Joints (Transforms)

Joints live in the `joints/` directory. Prefer SQL unless the user asks
otherwise or the logic cannot be expressed in SQL.

### SQL joints (default)

```sql
-- rivet:name: clean_orders
-- rivet:type: sql
SELECT
    id,
    customer_name,
    amount
FROM raw_orders
WHERE amount > 0
```

### YAML joints (when the user asks for YAML)

```yaml
name: clean_orders
type: sql
upstream:
  - raw_orders
sql: |
  SELECT id, customer_name, amount
  FROM raw_orders
  WHERE amount > 0
```

### Python joints (when SQL is not enough)

Use Python joints only for logic that cannot be expressed in SQL — ML
inference, API calls, complex row-level transformations, etc.

```python
# joints/enrich_orders.py
# rivet:upstream: [clean_orders]
from rivet_core.models import Material

def transform(material: Material) -> Material:
    table = material.to_arrow()
    # ... complex logic ...
    return table
```

## Writing Sinks

Sinks live in the `sinks/` directory. Write them by hand in SQL or YAML.

```sql
-- rivet:name: orders_clean
-- rivet:type: sink
-- rivet:catalog: local
-- rivet:table: orders_clean
-- rivet:upstream: [clean_orders]
```

## Running Queries

Use the non-interactive REPL to run ad-hoc SQL against any engine:

```bash
rivet repl execute -q "SELECT count(*) FROM raw_orders" --format json
rivet repl execute -q "SELECT * FROM raw_orders LIMIT 5" --format csv
```

Options:
- `--format {table|json|csv}` — output format (default: table)
- `--engine <name>` — target a specific engine
- `--max-rows <n>` — limit rows returned (default: 10000)

This is useful for exploring data, verifying transforms, and debugging.

## Setting Up Catalogs and Engines

Use the CLI to create new catalog and engine configurations non-interactively:

```bash
# Create a catalog
rivet catalog create \\
  --type postgres \\
  --name my_pg \\
  --option host=localhost \\
  --option port=5432 \\
  --option database=mydb \\
  --credential user=\\$PG_USER \\
  --credential password=\\$PG_PASSWORD

# Create an engine
rivet engine create \\
  --type duckdb \\
  --name analytics \\
  --catalog my_pg
```

Use `--dry-run` to preview the YAML that would be written without modifying
profiles.yaml. Use `--no-test` to skip the connection test if the database
is not reachable from the current environment.

## Validating and Running Pipelines

```bash
# Compile and inspect the pipeline DAG
rivet compile

# Compile a specific sink
rivet compile <sink_name>

# Compile and output as JSON (useful for programmatic inspection)
rivet compile --format json

# Run the full pipeline
rivet run

# Run a specific sink
rivet run <sink_name>

# Run tests
rivet test

# Run a specific test file
rivet test tests/test_transform_orders.yaml
```

## Troubleshooting

When something fails, run diagnostics:

```bash
rivet doctor --check-connections --check-schemas
```

This checks catalog connectivity and detects schema drift.

## Interactive Tools (for the user, not the agent)

Rivet includes interactive tools that are useful for manual exploration.
Recommend these to the user but do not launch them yourself:

- `rivet explore` — visual catalog browser with schema navigation
- `rivet repl` — interactive SQL REPL with syntax highlighting and
  auto-completion

## Language Priority

1. **SQL** — default for all joints and source declarations
2. **YAML** — only when the user explicitly requests it
3. **Python** — only when the logic cannot be expressed in SQL

## Key Principles

- Use the CLI to discover data — do not assume table names or schemas
- Generate sources from the catalog when possible
- Keep transforms in SQL unless there is a clear reason to use Python
- Always compile (`rivet compile`) after making changes to verify correctness
- Use `rivet doctor` proactively when errors occur
"""
