# AGENTS.md — Authoring a Rivet plugin

This file applies to any work under `rivet/src/`. If you are an AI coding
agent asked to build a new connector / source / sink / catalog / engine for
Rivet, read this file end to end before you write or edit any code.

It is the single source of truth for *what makes a plugin acceptable*.
Long-form prose and worked examples live in
`docs/plugins/development.md`; this file is the rule sheet.

> Plugins may live **in this repository** (validated, distributed alongside
> `rivet-core`) **or in an external repository** owned by a third party.
> The contracts and conventions in this file apply identically to both.
> The only difference is where the package lives and how it is shipped to
> PyPI; everything else (layout, ABCs, registration, naming) is the same.

---

## 1. Decide the shape of your plugin

Pick the ABCs you need from `rivet_core/plugins.py`. A plugin can implement
any non-empty subset of the following:

| ABC | When to implement |
|---|---|
| `CatalogPlugin` | Your plugin connects to a data store (DB, object store, REST API). |
| `ComputeEnginePlugin` | Your plugin runs SQL on an engine (DuckDB, Polars, Spark, …). |
| `ComputeEngineAdapter` | Your plugin bridges a specific `(engine, catalog)` pair. |
| `SourcePlugin` | Your plugin reads from a catalog into Arrow. |
| `SinkPlugin` | Your plugin writes Arrow into a catalog. |
| `CrossJointAdapter` | Your plugin bridges two engines at a joint boundary. |
| `ReferenceResolver` | Your engine needs SQL rewriting at compile time. |

Reference plugins by shape:

| Reference | Shape |
|---|---|
| `rivet_polars` | engine + adapter only |
| `rivet_aws` | catalog + source + sink (no engine) |
| `rivet_rest` | catalog + source + sink + adapter |
| `rivet_duckdb` | full stack (catalog + engine + adapter + source + sink) |
| `rivet_postgres` | full stack + cross-joint |

When in doubt, copy the structure of the closest reference plugin and trim.

---

## 2. Canonical directory layout

Whether in-tree (under `rivet/src/`) or in a standalone repository, the
package layout is identical:

```
rivet_<name>/
├── __init__.py          # Registration function (see §4). REQUIRED.
├── pyproject.toml       # Package metadata + entry points (see §5). REQUIRED.
├── README.md            # Short overview + usage. REQUIRED.
├── py.typed             # Empty marker for PEP 561 typing. REQUIRED.
├── engine.py            # ComputeEnginePlugin, if applicable
├── catalog.py           # CatalogPlugin, if applicable
├── source.py            # SourcePlugin, if applicable
├── sink.py              # SinkPlugin, if applicable
├── errors.py            # Plugin-specific error mapping (see §6)
└── adapters/            # Only if the plugin contributes adapters
    ├── __init__.py
    └── <catalog_or_engine>.py   # one file per bridged type
```

Hard rules:

- The error module is `errors.py`, never `_errors.py`.
- Adapters live under `adapters/`, never as flat files in the package root.
- Adapter files are named after the catalog or engine they bridge:
  `s3.py`, `glue.py`, `unity.py`, `duckdb.py`, `pyspark.py` — *not* by ABC.
- If the plugin has no adapters, the `adapters/` directory does not exist.

---

## 3. Naming conventions

- Package name: `rivet_<name>` (snake_case). PyPI distribution name:
  `rivetsql-<name>`.
- Class names use the PascalCase of `<name>`, but a few names override the
  default capitalization. The audit script `check_plugin_coherence.py`
  enforces the override table; keep it in sync if you add a plugin whose
  PascalCase isn't a simple capitalize:

  | suffix | PascalCase |
  |---|---|
  | `duckdb` | `DuckDB` |
  | `postgres` | `Postgres` |
  | `databricks` | `Databricks` |
  | `polars` | `Polars` |
  | `pyspark` | `PySpark` |
  | `aws` | `AWS` |
  | `rest` | `Rest` |

- The registration function in `__init__.py` is `<PascalCase>Plugin`
  (e.g. `def DuckDBPlugin(registry): …`).
- A `CatalogPlugin` sets `type = "<name>"`; a `ComputeEnginePlugin` sets
  `engine_type = "<name>"`. Use the snake_case `<name>`, not PascalCase.

---

## 4. Registration: the two-phase `__init__.py`

The registration function is split into two phases. **This pattern is
mandatory** because adapters often depend on optional third-party packages.

```python
"""rivet_<name> — <one-line description>."""

from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rivet_core.plugins import PluginRegistry


def <PascalCase>Plugin(registry: PluginRegistry) -> None:
    """Register all rivet_<name> components into the plugin registry."""

    # Phase 1 — core components. Always registered, no try/except.
    from rivet_<name>.catalog import <PascalCase>CatalogPlugin
    from rivet_<name>.engine import <PascalCase>ComputeEnginePlugin
    from rivet_<name>.source import <PascalCase>Source
    from rivet_<name>.sink import <PascalCase>Sink

    registry.register_catalog_plugin(<PascalCase>CatalogPlugin())
    registry.register_engine_plugin(<PascalCase>ComputeEnginePlugin())
    registry.register_source(<PascalCase>Source())
    registry.register_sink(<PascalCase>Sink())

    # Phase 2 — optional adapters. Each in its own try/except ImportError.
    try:
        from rivet_<name>.adapters.duckdb import DuckDB<PascalCase>Adapter
        registry.register_adapter(DuckDB<PascalCase>Adapter())
    except ImportError:
        pass
```

If your plugin has no optional adapters, omit Phase 2 entirely.

Phase-1 registrations must never be wrapped in `try/except`. If a core
component fails to import, the plugin is broken and the user must see the
error at startup.

---

## 5. Entry-point declaration

Use **monolithic** registration unless you have a specific reason to split.
The single function in `__init__.py` registers everything:

```toml
# pyproject.toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "rivetsql-<name>"
version = "0.1.0"
description = "<short description>"
requires-python = ">=3.11"
dependencies = [
    "rivetsql-core >=X.Y.Z",
    # plugin-specific runtime dependencies go here
]

[project.optional-dependencies]
# Optional adapter deps go in named extras.
duckdb = ["duckdb >=X.Y.Z"]

[project.entry-points."rivet.plugins"]
<name> = "rivet_<name>:<PascalCase>Plugin"

[tool.hatch.build.targets.wheel]
packages = ["rivet_<name>"]
```

Granular entry-point groups (`rivet.catalogs`, `rivet.compute_engines`,
`rivet.compute_engine_adapters`, `rivet.sources`, `rivet.sinks`,
`rivet.cross_joint_adapters`) are supported but discouraged for new plugins.
The audit script does not infer naming from them.

---

## 6. Errors and validation (will fail CI)

These rules are enforced by `rivet/scripts/check_plugin_coherence.py`.
Failing any of them blocks the merge.

**Never raise bare `Exception`, `ValueError`, `KeyError`, or
`NotImplementedError` from plugin code.** Define plugin-specific error
classes in `errors.py` and raise those. Example:

```python
# rivet_<name>/errors.py
class <PascalCase>Error(Exception):
    """Base error for rivet_<name>."""

class <PascalCase>ConnectionError(<PascalCase>Error):
    """Failure to connect to the underlying service."""

class <PascalCase>ValidationError(<PascalCase>Error):
    """Invalid catalog/engine options."""
```

`validate(self, options)` on `CatalogPlugin` and `ComputeEnginePlugin`
**must do real work**. A bare `pass` body is treated as a no-op and fails
the audit. At minimum, validate that every entry in `required_options` is
present and well-typed, and reject unknown keys.

`SourcePlugin.read(self, catalog, joint, pushdown)` **must accept the
`pushdown` parameter** (even if you ignore it on day one). Adapters should
implement `read_dispatch(..., pushdown=None)` and degrade unsupported
pushdown ops to the residual rather than raising.

`SinkPlugin` subclasses must declare a `supported_strategies` attribute
that includes at least `"append"` and `"replace"` (the baseline strategies).

---

## 7. Import-boundary rule (will fail CI)

A plugin imports **only** from `rivet_core` (and standard library +
third-party runtime deps). A plugin must not import:

- another plugin (`rivet_duckdb`, `rivet_postgres`, …)
- `rivet_config`
- `rivet_bridge`
- `rivet_cli`

Cross-plugin behaviour is mediated through the registry and the ABCs. If
you need data from another plugin, you are almost certainly building a
`CrossJointAdapter` — see `rivet_postgres` for the reference shape.

---

## 8. Catalog-introspection methods

If you implement `CatalogPlugin`, override these methods unless you have a
strong reason not to:

- `list_tables(catalog)` — required for catalog browsing.
- `get_schema(catalog, table)` — required for type-aware compilation.
- `test_connection(catalog)` — required; default falls back to
  `list_tables()` which is too expensive for production.
- `list_children(catalog, path)` — required; default filters
  `list_tables()`. Override for deep hierarchies (S3-like, filesystem).
- `get_metadata(catalog, table)` — optional, returns `None` by default.
- `get_fingerprint(catalog, path)` — optional; enables Smart Cache TTL
  refresh without refetch. Highly recommended.

---

## 9. Engine-method overrides

If you implement `ComputeEnginePlugin`, override at least these (the audit
treats them as warnings if missing — fix them anyway):

- `materialization_strategy_name` — set if your engine prefers a strategy
  other than the Arrow default.
- `default_concurrency_limit` — set to reflect your backend's natural
  parallelism.
- `get_reference_resolver` — return a `ReferenceResolver` if your engine
  needs compile-time SQL rewriting.
- `collect_metrics` — return a `PluginMetrics` if you can extract one.

`supported_catalog_types` (a class attribute, not a method) **must be
non-empty**. Each entry is `catalog_type → list[capability]` where
capabilities are typically `"read"` and `"write"`.

---

## 10. Pushdown contract (sources / adapters)

For sources and adapters, every pushdown operation should be wrapped in
`try/except` that pushes the failed op into the residual rather than
raising. The executor will reapply residuals via PyArrow.

The reference implementation is in `docs/plugins/development.md` under
"Implementing Pushdown in a Custom Adapter". Do not paraphrase it —
follow that exact template.

---

## 11. Native SQL write (sinks / adapters)

If your engine and catalog share a backend (DuckDB-on-DuckDB,
Postgres-on-Postgres, Spark-on-Delta), opt into native SQL write by
overriding `supports_native_sql_write(write_strategy)` on your adapter and
detecting `NativeSqlWriteContext` in `write_dispatch`.

This eliminates a full Arrow round-trip on the write path. See
`rivet_duckdb/adapters/duckdb.py` for the canonical example.

---

## 12. Materialized references

If your plugin returns a non-Arrow `MaterializedRef` (e.g. a Spark
DataFrame, a Postgres table reference), implement all five required
methods on the ref class:

- `to_arrow()`
- `schema()`
- `row_count()`
- `storage_type()`
- `size_bytes()`

The audit fails if any are missing.

---

## 13. Tests

Add at least one integration test under
`rivet/tests/integration/plugins/` (or in your standalone plugin's `tests/`
dir if external) that exercises plugin wiring:

- Plugin registers without raising.
- `CatalogPlugin.validate` rejects bad config.
- `ComputeEnginePlugin.execute_sql` runs a trivial `SELECT 1`.
- Round-trip: write → read returns the same Arrow data.

`rivet/tests/integration/plugins/test_plugin_wiring.py` is the closest
existing template; copy its shape.

---

## 14. Pre-merge checklist (in-tree contributions)

If you are adding a plugin **inside this repository**, you must update all
of the following in the same PR:

- [ ] `rivet/src/rivet_<name>/` — new package, complete per §2–§6.
- [ ] `rivet/src/rivet_<name>/pyproject.toml` — entry point per §5.
- [ ] `rivet/scripts/check_plugin_coherence.py` — add an entry to
      `EXPECTED_CAPABILITIES`. If your name needs a non-trivial
      PascalCase, add it to `_NAME_OVERRIDES`.
- [ ] `scripts/dev-install.sh` — add `<name>` to the plugin loop.
- [ ] `rivet/pyproject.toml` — add the plugin to
      `[project.optional-dependencies]`.
- [ ] `docs/plugins/index.md` — add a row to the capability grid + link.
- [ ] `docs/plugins/<name>.md` — new plugin page.
- [ ] `rivet/tests/integration/plugins/test_<name>_adapter.py` — at least
      one wiring test.

After making changes, run:

```bash
python rivet/scripts/check_plugin_coherence.py
python rivet/scripts/check_module_boundaries.py
python rivet/scripts/audit_dependencies.py
pytest rivet/tests/integration/plugins -q
```

All four must succeed before opening a PR.

## 15. Pre-merge checklist (external / third-party plugin)

If you are building a plugin in a **separate repository** (the user is not
contributing it to rivetsql for validation), the rules are simpler:

- [ ] §2 layout, §3 naming, §4 registration, §5 entry point, §6–§13 contracts.
- [ ] Run `python -m rivet.scripts.check_plugin_coherence --external <path>`
      against your package (once available).
- [ ] Publish to PyPI as `rivetsql-<name>`. The user installs via
      `pip install rivetsql-<name>` and Rivet discovers it automatically.

Until the `--external` mode of the coherence script ships, the most
reliable way to validate an external plugin is to clone this repository,
copy your plugin into `rivet/src/rivet_<name>/`, and run the audit there.

---

## 16. Designing before coding

For non-trivial plugins, fill in `docs/plugins/new-plugin-spec-template.md`
**before** writing code. The template covers requirements, design, and
task breakdown in the same shape used by other agent-driven workflows.
A 30-minute design pass routinely saves hours of refactoring.
