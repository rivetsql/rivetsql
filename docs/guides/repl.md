# Interactive REPL

The Rivet REPL is a full-screen terminal UI for exploring data, running ad-hoc queries, browsing catalogs, and iterating on pipeline logic.

---

## Launching

```bash
rivet repl
```

This loads your project using the `default` profile. The REPL compiles on startup and keeps in sync as you edit files.

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--profile` / `-p` | `default` | Profile to load |
| `--project` | `.` | Project directory |
| `--theme` | `rivet` | UI theme |
| `--no-watch` | off | Disable auto file reload |
| `--read-only` | off | Browse and query only |
| `--editor` | — | Pre-load a file in the editor |

```bash
rivet repl --profile staging --project /path/to/project
rivet repl --editor joints/transform_orders.sql
rivet repl --read-only
```

---

## Layout

Three panels:

- **Editor** (center) — multi-tab SQL editor with syntax highlighting and autocomplete
- **Catalog** (left) — tree view of catalogs, tables, and columns
- **Results** (bottom) — tabular output, query plan, diff view, execution logs

Use ++tab++ / ++shift+tab++ to move focus between panels.

---

## Running Queries

Type SQL in the editor and press ++ctrl+enter++ to execute against the active engine. Highlight text to run only the selection.

```sql
SELECT order_id, customer_id, total_amount
FROM orders
WHERE status = 'completed'
ORDER BY total_amount DESC
LIMIT 100
```

### Switching Engine

```
:engine duckdb_local
:engine spark_cluster
```

Or press ++ctrl+e++ for the engine selector.

### Autocomplete

Context-aware completion for table names, column names, joint names, and SQL keywords. Trigger with ++ctrl+space++.

### Formatting

Press ++ctrl+shift+f++ or run `:format` to auto-format SQL.

---

## Catalog Browsing

The Catalog panel shows all catalogs from your active profile. Each expands to show tables and columns with types.

| Key | Action |
|-----|--------|
| ++up++ / ++down++ | Move selection |
| ++enter++ | Expand / collapse |
| ++"p"++ | Preview table (first 100 rows) |
| ++"e"++ | Execute associated joint |
| ++"/"++ | Fuzzy search |
| ++"m"++ | Expand metadata |

### Searching

Press ++"/"++ to open fuzzy search. Type to filter tables and columns across all catalogs:

```
/orders
```

Matches `orders`, `raw_orders`, `orders_clean`, and any column named `order_id`.

---

## Executing Joints

Select a joint in the Catalog panel and press ++"e"++, or use the command bar:

```
:run transform_orders
:run                    # runs all sinks
```

---

## Colon Commands

Press ++colon++ to open the command bar:

| Command | Description |
|---------|-------------|
| `:quit` / `:q` | Exit |
| `:write` / `:w` | Save editor buffer |
| `:compile` / `:c` | Compile project |
| `:run [joint]` | Run all sinks or a specific joint |
| `:test [name]` | Run tests |
| `:profile <name>` | Switch profile |
| `:engine <name>` | Switch query engine |
| `:open <path>` | Open file in editor |
| `:export <fmt> <path>` | Export results |
| `:pin` / `:unpin` | Pin/clear pinned result |
| `:diff` | Toggle diff view |
| `:diffkey <cols>` | Set diff key columns |
| `:plan` | Toggle query plan view |
| `:format` | Format SQL |
| `:doctor` | Run diagnostics |
| `:history` | Show query history |
| `:refresh` | Refresh catalog trees |
| `:flush` | Flush material cache |
| `:debug [joint]` | Enter debug mode |
| `:inspect [target]` | Inspect compiled assembly |
| `:generate <name>` | Generate joint from last query |
| `:help [command]` | Show help |

---

## Exporting Results

Press ++ctrl+shift+e++ or use `:export`:

```
:export csv /tmp/results.csv
:export parquet ~/data/output.parquet
:export json ./results.json
:export clipboard
```

| Format | Extension |
|--------|-----------|
| CSV | `.csv` |
| TSV | `.tsv` |
| Parquet | `.parquet` |
| JSON | `.json` |
| JSON Lines | `.jsonl` |
| Clipboard | — (copies TSV) |

---

## Editor Tabs

| Key | Action |
|-----|--------|
| ++ctrl+t++ | New tab |
| ++ctrl+w++ | Close tab |
| ++ctrl+tab++ | Next tab |
| ++ctrl+shift+tab++ | Previous tab |

---

## File Watching

By default, the REPL watches project files. Saving a `.sql`, `.yaml`, or `.py` file triggers automatic recompile. Disable with `--no-watch`.

Manual recompile: `:compile` or ++ctrl+shift+c++.

---

## Keyboard Reference

| Key | Action |
|-----|--------|
| ++ctrl+enter++ | Execute query |
| ++ctrl+e++ | Engine selector |
| ++ctrl+t++ | New tab |
| ++ctrl+w++ | Close tab |
| ++ctrl+space++ | Autocomplete |
| ++ctrl+shift+f++ | Format SQL |
| ++ctrl+shift+e++ | Export dialog |
| ++ctrl+shift+c++ | Recompile |
| ++ctrl+h++ | Query history |
| ++ctrl+p++ | Command palette |
| ++ctrl+backslash++ | Toggle catalog panel |
| ++ctrl+r++ | Toggle results panel |
| ++tab++ | Focus next panel |
| ++ctrl+q++ | Quit |

---

## Tips

!!! tip "Pin and diff"
    Pin a result with `:pin` to keep it visible while running follow-up queries. Use `:diff` to see row-level differences between the pinned result and the latest query. Set the join key with `:diffkey order_id`.

!!! tip "Debug mode"
    `:debug transform_orders` steps through execution one fused group at a time, showing intermediate materialized results. Press ++"n"++ to step, ++"c"++ to continue, ++"q"++ to exit.
