# Smart Cache

Rivet caches catalog metadata on disk so that repeated commands don't re-fetch data from remote sources. The cache is transparent — it integrates with `CatalogExplorer` and benefits all catalog-related commands automatically.

---

## How It Works

The SmartCache stores catalog tree nodes (schemas, tables) and introspected schemas as JSON files on disk. Each catalog gets its own file, isolated from other catalogs.

```
~/.cache/rivet/catalog/{profile}/{catalog_name}_{connection_hash}.json
```

When you run `rivet catalog list`, `rivet explore`, or `rivet repl`, the cache is consulted or populated depending on the command type.

---

## Cache Modes

SmartCache operates in two modes depending on the command:

| Mode | Used by | Behavior |
|------|---------|----------|
| **READ_WRITE** | `repl`, `explore`, `catalog search` | Reads from cache on startup (warm-start), writes updates back |
| **WRITE_ONLY** | `catalog list`, `catalog describe` | Always fetches live data, writes results to cache for future sessions |

This means non-interactive commands always show live data, while interactive tools start instantly with cached data and refresh in the background.

---

## Warm Start

Interactive commands (`repl`, `explore`) load cached catalog trees immediately on startup. You see results before live introspection completes. As connections are probed and data refreshed, cached nodes are replaced with fresh data.

---

## Staleness Detection

Each cache entry has a TTL (time-to-live) of **300 seconds** by default. When an entry expires:

1. If the catalog plugin supports **fingerprinting** (a lightweight signature like an ETag or modification timestamp), SmartCache compares fingerprints:
   - Same fingerprint → TTL resets, cached data reused (no refetch)
   - Different fingerprint → entry invalidated, fresh data fetched
2. If the plugin doesn't support fingerprinting, the expired entry is refetched unconditionally.
3. If the fingerprint check fails (network error), stale data is returned with a warning.

---

## Cache Invalidation

The cache is automatically invalidated when:

- **Config files change** — editing `.yaml`/`.yml` files in the project invalidates the entire profile's cache
- **Profile switch** — switching profiles clears the previous profile's cache
- **Manual refresh** — pressing `r` in `explore` or running `:refresh` in the REPL clears and refetches a catalog
- **Connection options change** — if catalog connection options change, the connection hash changes and old entries become unreachable

You can also clear the entire cache manually:

- In the REPL: `:flush`
- Programmatically: `SmartCache.clear()`
- On disk: delete `~/.cache/rivet/catalog/`

---

## Size Management

The cache enforces a maximum size of **50 MB** by default. When the limit is exceeded, the least recently accessed entries are evicted first (LRU policy). Corrupted cache files are discarded automatically — the worst case is a cache miss, never an error.

---

## Search Integration

The `search()` method in `CatalogExplorer` uses the cache to improve results:

- **Phase 1 (instant)**: Scans all cached nodes for fuzzy matches. Frequently accessed paths get a score bonus so they rank higher. This phase runs for every search and never makes network calls.
- **Phase 2 (progressive expansion)**: Unexplored branches are expanded via live API calls within a time budget (10 seconds for `catalog search`). Expansion is prioritized using several heuristics:
  - **Hit-segment boosting**: Branches whose names overlap with Phase 1 hit paths are expanded first. For example, if `datalake_silver.data_factory_ingest` produced hits, `preprod_datalake_silver` gets priority because it contains the segment `datalake_silver`.
  - **Sibling pruning**: When a cached parent already produced Phase 1 hits, its children that have no segment relevance are skipped. This prevents dozens of irrelevant sibling schemas from consuming the expansion budget.
  - **Access-priority ordering**: Recently accessed branches (from SmartCache metadata) are expanded before cold branches.
- **Score filtering**: After both phases, results scoring 20+ points worse than the best hit are dropped. Combined with a contiguous-substring bonus in the fuzzy matcher, this eliminates scattered-subsequence noise (e.g. searching "ingestion" won't match "grs_functional_unit").

| Context | Phase 1 | Phase 2 |
|---------|---------|---------|
| `explore` keystroke search | Yes | No (instant) |
| `rivet catalog search` | Yes | Yes (10s budget) |
| REPL `search_catalog()` | Yes | Yes |

Every expansion enriches the cache, so subsequent searches start with a broader corpus.

---

## Flush Policy

Cache writes are debounced — dirty files are flushed to disk at most once every **5 seconds**. An explicit flush also happens when the session ends (REPL exit, explore close, CLI command completion). If a crash occurs between writes, at most 5 seconds of cache updates may be lost — the worst case is a cache miss on next startup.

---

## Configuration

SmartCache parameters are not currently exposed as CLI flags or config file options. The defaults are:

| Parameter | Default | Description |
|-----------|---------|-------------|
| Cache directory | `~/.cache/rivet/catalog/` | On-disk location |
| Default TTL | 300 seconds | Time before staleness check |
| Max size | 50 MB | LRU eviction threshold |
| Flush interval | 5 seconds | Debounced write interval |
