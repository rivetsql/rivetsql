# Write Strategies

A write strategy controls how Rivet writes data from a sink joint to a catalog target. Every sink declares a `write_strategy` that determines whether rows are appended, replaced, merged, or tracked historically. Default: `append`.

| Mode | Description |
|------|-------------|
| `append` | Add new rows without touching existing data |
| `replace` | Drop and recreate the target table |
| `truncate_insert` | Truncate then insert (preserves table object) |
| `merge` | Upsert by key columns |
| `delete_insert` | Delete matching partition then insert |
| `incremental_append` | Append only rows newer than watermark |
| `scd2` | Slowly Changing Dimension Type 2 |

---

## append

Every run adds rows to the end. Existing rows are never modified. Use for event logs, audit trails, or accumulation patterns.

=== "SQL"

    ```sql
    -- rivet:name: events_sink
    -- rivet:type: sink
    -- rivet:upstream: raw_events
    -- rivet:catalog: warehouse
    -- rivet:table: events
    -- rivet:write_strategy: {mode: append}
    ```

=== "YAML"

    ```yaml
    name: events_sink
    type: sink
    upstream: raw_events
    catalog: warehouse
    table: events
    write_strategy:
      mode: append
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    events_sink = Joint(
        name="events_sink",
        joint_type="sink",
        upstream=["raw_events"],
        catalog="warehouse",
        table="events",
        write_strategy="append",
    )
    ```

---

## replace

Drops and recreates the target on every run. The target always reflects exactly the current pipeline output. Use for reference tables or cheap full refreshes.

=== "SQL"

    ```sql
    -- rivet:write_strategy: {mode: replace}
    ```

=== "YAML"

    ```yaml
    write_strategy:
      mode: replace
    ```

=== "Rivet API"

    ```python
    write_strategy="replace"
    ```

---

## truncate_insert

Truncates the target (removes rows, keeps schema/indexes) then inserts. Unlike `replace`, the table object is preserved — permissions, constraints, and dependent views remain intact.

=== "SQL"

    ```sql
    -- rivet:write_strategy: {mode: truncate_insert}
    ```

=== "YAML"

    ```yaml
    write_strategy:
      mode: truncate_insert
    ```

=== "Rivet API"

    ```python
    write_strategy="truncate_insert"
    ```

---

## merge

Upserts rows using key columns. Matching keys are updated; new keys are inserted. Unmatched target rows are left untouched. Use for dimension tables or idempotent updates.

=== "SQL"

    ```sql
    -- rivet:name: customers_sink
    -- rivet:type: sink
    -- rivet:upstream: clean_customers
    -- rivet:catalog: warehouse
    -- rivet:table: customers
    -- rivet:write_strategy: {mode: merge, key_columns: [customer_id]}
    ```

=== "YAML"

    ```yaml
    name: customers_sink
    type: sink
    upstream: clean_customers
    catalog: warehouse
    table: customers
    write_strategy:
      mode: merge
      key_columns: [customer_id]
    ```

=== "Rivet API"

    ```python
    from rivet_core.models import Joint

    customers_sink = Joint(
        name="customers_sink",
        joint_type="sink",
        upstream=["clean_customers"],
        catalog="warehouse",
        table="customers",
        write_strategy="merge",
    )
    ```

---

## delete_insert

Deletes rows matching a partition predicate, then inserts all pipeline output. A partition-level swap — you define which slice to replace. Use for date-partitioned tables.

=== "SQL"

    ```sql
    -- rivet:write_strategy: {mode: delete_insert, partition_by: [order_date]}
    ```

=== "YAML"

    ```yaml
    write_strategy:
      mode: delete_insert
      partition_by: [order_date]
    ```

=== "Rivet API"

    ```python
    write_strategy="delete_insert"
    ```

---

## incremental_append

Appends only rows newer than the last recorded watermark. On each run, Rivet reads the watermark, filters output, appends new rows, and advances the watermark. Use for high-volume event streams.

=== "SQL"

    ```sql
    -- rivet:write_strategy: {mode: incremental_append, watermark_column: event_time}
    ```

=== "YAML"

    ```yaml
    write_strategy:
      mode: incremental_append
      watermark_column: event_time
    ```

=== "Rivet API"

    ```python
    write_strategy="incremental_append"
    ```

!!! note
    Watermarks are stored per sink name. Use `rivet watermark list` to inspect and `rivet watermark reset` to restart.

---

## scd2

Slowly Changing Dimension Type 2 tracks full row history with `valid_from`, `valid_to`, and `is_current` columns. Changed rows are closed and new records inserted. Unchanged rows are left alone.

=== "SQL"

    ```sql
    -- rivet:write_strategy: {mode: scd2, key_columns: [customer_id]}
    ```

=== "YAML"

    ```yaml
    write_strategy:
      mode: scd2
      key_columns: [customer_id]
      valid_from_column: valid_from
      valid_to_column: valid_to
      is_current_column: is_current
    ```

=== "Rivet API"

    ```python
    write_strategy="scd2"
    ```

---

## Choosing a Strategy

| If you need... | Use |
|---|---|
| Accumulate events without touching history | `append` |
| Full refresh, table can be dropped | `replace` |
| Full refresh, table must be preserved | `truncate_insert` |
| Idempotent upserts by key | `merge` |
| Partition-level replacement | `delete_insert` |
| Efficient incremental loads by timestamp | `incremental_append` |
| Full row history with open/close records | `scd2` |
