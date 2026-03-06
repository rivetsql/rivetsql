# Contributing to Rivet

Thanks for your interest in contributing to Rivet! This guide covers everything you need to get started.

## Development Environment Setup

1. Clone the repository:

   ```bash
   git clone https://github.com/rivetsql/rivetsql.git
   cd rivet
   ```

2. Install in editable mode with dev dependencies:

   ```bash
   pip install -e "./rivet[dev]"
   ```

3. (Optional) Install a plugin for local testing:

   ```bash
   pip install -e "./rivet/src/rivet_duckdb"
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
mypy rivet/src
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
