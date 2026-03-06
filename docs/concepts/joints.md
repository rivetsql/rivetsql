# Joints

A joint declares *what* to compute. It is a named, immutable unit in the pipeline DAG that describes a data transformation without executing it. Joints do not run logic — they are compiled into a `CompiledAssembly` that the executor runs deterministically.

Every joint has a unique name, a type, and a set of options that vary by type:

| Type | Purpose |
|------|---------|
| `source` | Read data from a catalog into the pipeline |
| `sql` | Transform data using a SQL query |
| `python` | Transform data using a Python function |
| `sink` | Write data from the pipeline to a catalog |

---

## Source Joint

A source joint reads a table from a catalog. It has no upstream dependencies — always a root node in the DAG.

=== "SQL"

    ```sql
    -- rivet:name: raw_orders
    -- rivet:type: source
    -- rivet:catalog: local
    -- rivet:table: orders
    ```

=== "YAML"

    ```yaml
    name: raw_orders
    type: source
    catalog: local
    table: orders
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    raw_orders = Joint(
        name="raw_orders",
        joint_type="source",
        catalog="local",
        table="orders",
    )
    ```

---

## SQL Joint

A SQL joint transforms data using a SQL query. It references upstream joints by name in the `FROM` clause. The engine resolves those references at compile time and fuses adjacent SQL joints on the same engine into a single query when possible.

=== "SQL"

    ```sql
    -- rivet:name: daily_revenue
    -- rivet:type: sql
    -- rivet:upstream: raw_orders

    SELECT
        order_date,
        SUM(amount) AS revenue
    FROM raw_orders
    WHERE status = 'completed'
    GROUP BY order_date
    ```

=== "YAML"

    ```yaml
    name: daily_revenue
    type: sql
    upstream: [raw_orders]
    sql: |
      SELECT
          order_date,
          SUM(amount) AS revenue
      FROM raw_orders
      WHERE status = 'completed'
      GROUP BY order_date
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    daily_revenue = Joint(
        name="daily_revenue",
        joint_type="sql",
        upstream=["raw_orders"],
        sql="""
        SELECT order_date, SUM(amount) AS revenue
        FROM raw_orders
        WHERE status = 'completed'
        GROUP BY order_date
        """,
    )
    ```

---

## Sink Joint

A sink joint writes the output of an upstream joint to a catalog. It is always a leaf node — nothing reads from a sink. The `write_strategy` controls how data is written (append, replace, merge, etc.).

=== "SQL"

    ```sql
    -- rivet:name: revenue_sink
    -- rivet:type: sink
    -- rivet:upstream: daily_revenue
    -- rivet:catalog: warehouse
    -- rivet:table: daily_revenue
    -- rivet:write_strategy: replace
    ```

=== "YAML"

    ```yaml
    name: revenue_sink
    type: sink
    upstream: daily_revenue
    catalog: warehouse
    table: daily_revenue
    write_strategy: replace
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    revenue_sink = Joint(
        name="revenue_sink",
        joint_type="sink",
        upstream=["daily_revenue"],
        catalog="warehouse",
        table="daily_revenue",
        write_strategy="replace",
    )
    ```

---

## Python Joint

A Python joint transforms data using a Python function. The function receives upstream `MaterializedRef` objects and must return a PyArrow `Table`. Use Python joints when SQL is insufficient — ML models, external APIs, or complex row-level logic.

!!! warning "Fusion boundary"
    Python joints break SQL fusion. Adjacent SQL joints on either side compile into separate fused groups.

=== "SQL"

    ```sql
    -- rivet:name: scored_orders
    -- rivet:type: python
    -- rivet:upstream: raw_orders
    -- rivet:function: joints.scoring:score_orders
    ```

=== "YAML"

    ```yaml
    name: scored_orders
    type: python
    upstream: [raw_orders]
    function: joints.scoring:score_orders
    ```

=== "Python"

    ```python
    # joints/scored_orders.py
    # rivet:name: scored_orders
    # rivet:type: python
    # rivet:upstream: [raw_orders]
    import pyarrow as pa
    from rivet_core.models import Material

    def transform(material: Material) -> pa.Table:
        table = material.to_arrow()
        # ... apply scoring logic ...
        return scored_table
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    scored_orders = Joint(
        name="scored_orders",
        joint_type="python",
        upstream=["raw_orders"],
        function="joints.scoring:score_orders",
    )
    ```

The handler function signature:

```python
import pyarrow as pa
from rivet_core.models import Material

def score_orders(material: Material) -> pa.Table:
    table = material.to_arrow()
    # ... apply scoring logic ...
    return scored_table
```

---

## Python Joint File Format

Python joints can be declared as standalone `.py` files using `# rivet:key: value` annotation comments. The config layer discovers `.py` files in `sources/`, `joints/`, and `sinks/` directories and parses their annotations into `JointDeclaration` objects.

### Annotation Syntax

```python
# rivet:name: my_joint
# rivet:type: python
# rivet:upstream: [raw_orders]
```

The parser reads annotations from the top of the file and stops at the first non-annotation, non-blank line. All annotation keys and value types match the SQL annotation format.

### Defaults

| Annotation | Default |
|------------|---------|
| `name` | File stem (e.g., `scoring.py` → `scoring`) |
| `type` | `python` |
| `function` | Auto-derived from file path (see below) |

### Function Auto-Derivation

When no `# rivet:function:` annotation is provided, the parser derives it from the file's module path relative to the project root, appending `:transform`:

| File Path | Derived `function` |
|---|---|
| `joints/scoring.py` | `joints.scoring:transform` |
| `joints/sub/deep.py` | `joints.sub.deep:transform` |

### Minimal Example

A `.py` joint file needs only a single annotation — name, type, and function are all auto-derived:

```python
# joints/enrich.py
# rivet:upstream: [raw_orders]

import pyarrow as pa
from rivet_core.models import Material

def transform(material: Material) -> pa.Table:
    table = material.to_arrow()
    return table.append_column("enriched", pa.array([True] * len(table)))
```

This produces: `name="enrich"`, `type="python"`, `function="joints.enrich:transform"`.

---

## Joint Lifecycle

All joints follow the same lifecycle regardless of type:

1. **Declaration** — defined in SQL annotations, YAML, or Python
2. **Bridge forward** — `rivet_bridge` converts declarations into `Joint` model objects
3. **Assembly** — joints are assembled into a DAG (must be acyclic, all upstreams must exist)
4. **Compilation** — `compile()` produces a `CompiledAssembly` (pure: no I/O)
5. **Execution** — executor follows `execution_order` and materializes each joint

See [Compilation](compilation.md) for details on the compilation pipeline.
