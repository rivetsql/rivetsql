# AGENTS.md

This file orients AI coding agents (Claude Code, OpenAI Codex, Cursor, Aider,
Sourcegraph Amp, Zed, etc.) working in this repository. It is plain Markdown
and uses no vendor-specific extensions.

If your tool follows the [agents.md](https://agents.md/) convention, scoped
guidance is also available in nested `AGENTS.md` files — most importantly:

- **Building a new Rivet plugin: read `rivet/src/AGENTS.md` first.**

---

## What this repository is

`rivetsql` is a SQL transformation framework built around a plugin
architecture. The core engine lives in `rivet/src/rivet_core` and exposes a
small set of abstract base classes (ABCs). Everything else — DuckDB,
Postgres, Databricks, Polars, PySpark, AWS, REST — is a plugin package that
implements those ABCs and registers via Python entry points.

A plugin can also live in an external repository owned by a third party.
This repository curates a list of *validated* plugins, but users may install
any plugin that conforms to the contracts in `rivet_core.plugins`.

## Repository layout

```
rivetsql/
├── AGENTS.md                  # ← you are here
├── rivet/
│   ├── src/
│   │   ├── AGENTS.md          # plugin-authoring rules (read for plugin work)
│   │   ├── rivet_core/        # ABCs, registry, executor, optimizer, builtins
│   │   ├── rivet_config/      # config loader (do not import from plugins)
│   │   ├── rivet_bridge/      # orchestration glue (do not import from plugins)
│   │   ├── rivet_cli/         # `rivet` CLI entrypoint
│   │   └── rivet_<name>/      # one directory per first-party plugin
│   ├── scripts/               # audit/coherence scripts run in CI
│   └── tests/                 # unit/ and integration/
├── docs/                      # MkDocs site; plugin guides under docs/plugins/
├── scripts/                   # repo-level dev scripts (dev-install.sh, …)
└── CONTRIBUTING.md            # contribution rules and architectural invariants
```

## Setup

```bash
# Install the core package and every first-party plugin in editable mode.
./scripts/dev-install.sh

# Install only the plugins after a core upgrade:
./scripts/dev-install.sh --plugins-only
```

Python `>= 3.11`. Build/type tooling is configured via `mise.toml` and
`pyproject.toml`. Pre-commit hooks live in `.pre-commit-config.yaml`.

## Common tasks

| Goal                                   | Command                                                       |
|----------------------------------------|---------------------------------------------------------------|
| Run unit tests                         | `pytest rivet/tests/unit -q`                                  |
| Run integration tests                  | `pytest rivet/tests/integration -q`                           |
| Audit plugin coherence                 | `python rivet/scripts/check_plugin_coherence.py`              |
| Check module-import boundaries         | `python rivet/scripts/check_module_boundaries.py`             |
| Verify plugin packaging coherence      | `python rivet/scripts/check_plugin_coherence.py`              |
| Audit plugin dependencies              | `python rivet/scripts/audit_dependencies.py`                  |
| Build docs locally                     | `mkdocs serve`                                                |

## Contribution rules (must read)

- `CONTRIBUTING.md` — architectural invariants, code style, review process.
- `rivet/src/AGENTS.md` — **everything you need to author a plugin**.
- `docs/plugins/development.md` — long-form ABC reference and worked examples.
- `docs/plugins/new-plugin-spec-template.md` — fill-in-the-blanks spec for a
  new plugin. Use this before writing code so design is settled up front.

## Branch and PR conventions

- Never commit directly to `main`. Branch off with a descriptive prefix:
  `feat/…`, `fix/…`, `docs/…`, `plugin/<name>` for new plugins.
- PRs must pass `check_plugin_coherence.py` and `check_module_boundaries.py`.
- PRs that add a new in-tree plugin must also update:
  - `rivet/scripts/check_plugin_coherence.py` (`EXPECTED_CAPABILITIES`)
  - `scripts/dev-install.sh` (plugin loop)
  - `rivet/pyproject.toml` (`[project.optional-dependencies]`)
  - `docs/plugins/index.md` (capability grid + link)

The plugin-authoring guide at `rivet/src/AGENTS.md` enumerates these in a
single checklist.
