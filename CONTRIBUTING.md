# Contributing to Rivet

Thanks for your interest in contributing to Rivet! This guide covers everything you need to get started.

## Development Environment Setup

1. Clone the repository:

   ```bash
   git clone https://github.com/rivetsql/rivetsql.git
   cd rivetsql
   ```

2. Install core + plugins in editable mode (recommended for contributors):

   ```bash
   ./scripts/dev-install.sh
   ```

   Or just install core with dev/test extras:

   ```bash
   pip install -e "./rivet[dev,test]"
   ```

3. (Optional) Install a single plugin from local source:

   ```bash
   pip install -e ./rivet/src/rivet_duckdb
   ```

## Running Tests

```bash
pytest rivet/tests
```

## Linting

```bash
ruff check rivet/src rivet/tests
```

## Type Checking

```bash
mypy rivet/src rivet/tests
```

## Internal Quality Checks

The repository ships several scripts that enforce architectural rules. They run
in CI but you can also invoke them locally:

```bash
python rivet/scripts/check_module_boundaries.py
python rivet/scripts/check_plugin_coherence.py
python rivet/scripts/audit_dependencies.py
python rivet/scripts/find_dead_code.py
python rivet/scripts/flag_stale_docs.py
```

## Pull Request Process

1. Fork the repository and create a feature branch from `main`.
2. Make your changes — keep commits focused and well-described.
3. Ensure all checks pass locally: tests, linting, and type checking.
4. Open a pull request against `main` with a clear description of the change.
5. Address review feedback promptly.

## Coding Standards

- Target Python 3.11+.
- Follow the existing code style — `ruff` enforces formatting and import order.
- Use strict type annotations (`mypy --strict` is enabled).
- Write tests for new functionality.
- Keep modules small and focused.
- Prefer deletion over deprecation — no compatibility shims.
- No commented-out code or duplicate files.

## Reporting Issues

Use the [GitHub issue tracker](https://github.com/rivetsql/rivetsql/issues) to report bugs or request features. Please use the provided issue templates.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).


## Security audits

CI runs `pip-audit` against the installed dependency tree on every PR (and on
`main`). The job is **advisory** (non-blocking) so that a transient CVE in an
unrelated transitive dependency cannot stall delivery.

Triage policy when `pip-audit` reports a vulnerability:

1. Open a tracking issue with the vulnerability ID (CVE / GHSA / PYSEC) and
   the affected dependency.
2. If the project's code is exposed to the vulnerability, ship a pinned
   floor in the relevant `pyproject.toml` and reference the issue in the
   CHANGELOG entry.
3. If the project is not exposed, close the issue with a short justification
   so the next reporter can find the precedent.

The `pip-audit` step is run in its own job (`security`) and uses
`continue-on-error: true` rather than an explicit allow-list, so there is no
silent ignore list to maintain.


## `except Exception` policy

Broad `except Exception` blocks are accepted in this codebase only when at
least one of the following is true:

1. The body re-raises (`raise`) or wraps in a `RivetError` / `ExecutionError`
   and re-raises.
2. The body logs the failure with `exc_info=True` so the cause shows up at
   `RIVET_LOG_LEVEL=DEBUG`.
3. The block is immediately followed by `# noqa: BLE001 — <reason>` *and* the
   rationale is convincing (for instance, Textual lifecycle: a widget may
   not yet be composed). Drive-by suppressions without a reason will be
   rejected in review.

Silent `except Exception: pass` is never acceptable in `rivet_core`,
`rivet_config`, `rivet_bridge`, or any plugin's adapter / engine / catalog
modules. If the failure is genuinely best-effort, log it at debug.
