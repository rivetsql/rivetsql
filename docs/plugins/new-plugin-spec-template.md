# New plugin spec template

Copy this file before starting work on a new plugin (e.g. into
`docs/plugins/<name>-spec.md` for in-tree plugins, or into your own repo
for third-party plugins) and fill it in. The structure mirrors the
requirements / design / tasks split that scales across both human reviewers
and AI coding agents.

You do not need to use any specific tooling to consume this template —
it is plain Markdown.

---

## 0. Metadata

- **Plugin name** (PyPI distribution): `rivetsql-<name>`
- **Package name** (Python import): `rivet_<name>`
- **Author / maintainer**:
- **Repository**: in-tree under `rivet/src/rivet_<name>/` *or* external repo URL
- **Status**: draft / in review / approved / shipped

---

## 1. Requirements

### 1.1 Purpose

One paragraph: what does this plugin let a Rivet user do that they cannot
do today? Who is the target user?

### 1.2 Capabilities

Pick exactly the ABCs from `rivet_core/plugins.py` that this plugin will
implement. Mark each row.

| Capability | Implementing? | Notes |
|---|---|---|
| `CatalogPlugin` | yes / no |  |
| `ComputeEnginePlugin` | yes / no |  |
| `ComputeEngineAdapter` | yes / no | list `(engine_type, catalog_type)` pairs |
| `SourcePlugin` | yes / no |  |
| `SinkPlugin` | yes / no |  |
| `CrossJointAdapter` | yes / no | list `(consumer, producer)` pairs |
| `ReferenceResolver` | yes / no |  |

### 1.3 External dependencies

List runtime Python packages you will pin in `pyproject.toml`. For each,
state minimum supported version and why.

| Package | Min version | Reason |
|---|---|---|
|  |  |  |

### 1.4 Authentication & credentials

Describe how users authenticate. List every option that should be marked
`credential_options` on the `CatalogPlugin` (so it is masked in logs).
Identify environment-variable fallbacks (`env_var_hints`).

### 1.5 Catalog options

List `required_options`, `optional_options` (with defaults), and
`credential_options`. If the catalog supports multiple auth modes, fill
in `credential_groups`.

### 1.6 Engine options

If the plugin implements `ComputeEnginePlugin`, list options the user can
set on the engine, and state `supported_catalog_types` and the
capabilities for each.

### 1.7 Non-goals

State what is explicitly out of scope for v1. (Common non-goals:
streaming, schema evolution, secondary indexes, multi-region writes.)

### 1.8 Open questions

Anything blocking design or implementation. Resolve before §2.

---

## 2. Design

### 2.1 Module breakdown

Sketch the file tree. It must conform to `rivet/src/AGENTS.md` §2.

```
rivet_<name>/
├── __init__.py
├── pyproject.toml
├── README.md
├── py.typed
├── catalog.py        # if 1.2 says yes
├── engine.py         # if 1.2 says yes
├── source.py         # if 1.2 says yes
├── sink.py           # if 1.2 says yes
├── errors.py
└── adapters/         # if 1.2 says yes
    └── <bridged>.py
```

### 2.2 Error types

List the exception classes you will define in `errors.py`. The base must
inherit from `Exception`, never from `BaseException`.

### 2.3 Catalog design

For `CatalogPlugin`:

- How does `default_table_reference(logical_name, options)` map a Rivet
  joint name to a physical reference?
- How is `list_tables` implemented? Cost (single round-trip vs paginated)?
- How is `get_schema` implemented? Where do types come from?
- How is `test_connection` implemented (must be cheaper than `list_tables`)?
- Will you implement `get_fingerprint`? If yes, what is the fingerprint
  source (ETag, mtime, version number)?

### 2.4 Engine design

For `ComputeEnginePlugin`:

- Backend executor (in-process? remote service? subprocess?).
- How is `execute_sql` implemented? How are `input_tables` materialized
  into the backend?
- `default_concurrency_limit` — what natural parallelism does the
  backend expose?
- Do you need a `ReferenceResolver`? If yes, what is the rewriting rule?
- Materialization strategy — Arrow default or custom?

### 2.5 Source / Sink design

- For each source, describe pushdown ops it can apply natively
  (predicates, projection, limit, casts) and which it must always
  residualize.
- For each sink, list `supported_strategies` (must include `append` and
  `replace`). Describe what each strategy does at the storage layer.
- If sink can opt into native SQL write, state which strategies it
  supports natively.

### 2.6 Adapter design

For each `ComputeEngineAdapter`:

- `(target_engine_type, catalog_type)` pair.
- `capabilities` list.
- `source` (`engine_plugin` or `catalog_plugin`).
- Does it support native SQL write? If yes, which strategies?
- Cross-backend semantics (Arrow round-trip vs native reference vs zero-copy).

### 2.7 Cross-joint design

If implementing `CrossJointAdapter`, describe how the consumer engine
accesses upstream data: `arrow_passthrough`, `native_reference`, or
explicitly `unsupported`.

### 2.8 Failure modes

For each external failure (network down, auth expired, schema drift,
quota), state which exception type you raise and whether the failure is
retried by the executor.

### 2.9 Performance & limits

Throughput targets. Memory ceiling for the largest table you support in
v1. Any rate limits to respect.

---

## 3. Tasks

Break the work into commits-sized tasks. Each task should be small enough
to test in isolation. The order below is a sensible default; adjust to
your plugin's shape.

- [ ] T1 — Scaffold `rivet_<name>/` package: `__init__.py` (Phase-1
      skeleton), `pyproject.toml`, `README.md`, `errors.py`, `py.typed`.
- [ ] T2 — Implement `CatalogPlugin` (validate + instantiate +
      `default_table_reference`). Unit-test validation paths.
- [ ] T3 — Implement catalog introspection (`list_tables`, `get_schema`,
      `test_connection`).
- [ ] T4 — Implement `ComputeEnginePlugin` (validate + create_engine +
      `execute_sql`). Unit-test on a trivial `SELECT 1`.
- [ ] T5 — Implement `SourcePlugin.read` with `pushdown` parameter.
- [ ] T6 — Implement `SinkPlugin.write` with at minimum `append` and
      `replace` strategies.
- [ ] T7 — Implement `ComputeEngineAdapter`(s) under `adapters/`. Wire
      Phase-2 best-effort imports in `__init__.py`.
- [ ] T8 — (optional) Implement `CrossJointAdapter` for relevant
      consumer/producer pairs.
- [ ] T9 — Integration test under
      `rivet/tests/integration/plugins/test_<name>_adapter.py`:
      registration → simple read → simple write round-trip.
- [ ] T10 — Run audits and fix all errors:
      `python rivet/scripts/check_plugin_coherence.py` and
      `python rivet/scripts/check_module_boundaries.py`.
- [ ] T11 — In-tree only: update `EXPECTED_CAPABILITIES`,
      `dev-install.sh`, `rivet/pyproject.toml` extras, and
      `docs/plugins/index.md`. See `rivet/src/AGENTS.md` §14 for the full
      checklist.
- [ ] T12 — Write `docs/plugins/<name>.md` with a short usage example.

---

## 4. Acceptance

The plugin is ready to merge / publish when:

- All audits in §3 T10 are green (zero errors).
- Integration test from §3 T9 passes locally and in CI.
- `docs/plugins/<name>.md` is complete and renders without warnings under
  `mkdocs build --strict`.
- For in-tree plugins, the §14 checklist in `rivet/src/AGENTS.md` is fully
  ticked.
