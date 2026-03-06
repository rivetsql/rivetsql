# CHANGELOG

<!-- version list -->

## v1.2.0 (2026-03-06)

### Features

- Add repl execute module and expand repl init
  ([`dce6737`](https://github.com/rivetsql/rivetsql/commit/dce6737e8e58e50519f3362f6fc29db989c98592))


## v1.1.0 (2026-03-06)

### Features

- Add engine commands and expand explore/catalog CLI
  ([`bc6752d`](https://github.com/rivetsql/rivetsql/commit/bc6752dbaafde4aab183eed1587bee97c78dfc6e))


## v1.0.6 (2026-03-06)

### Refactoring

- Simplify explore command and remove catalog_explorer
  ([`ebcdff7`](https://github.com/rivetsql/rivetsql/commit/ebcdff7c906258018822394718698410dc6d54d6))


## v1.0.5 (2026-03-06)

### Bug Fixes

- Expand doctor command checks
  ([`3f434c4`](https://github.com/rivetsql/rivetsql/commit/3f434c40e9baf10c61216a3d3f9fb1f234f2910f))

### Chores

- Remove rivet docs CLI command and doc generation scripts
  ([`49e06e3`](https://github.com/rivetsql/rivetsql/commit/49e06e337b2e3d682146cdfb874126812a8b6763))


## v1.0.4 (2026-03-06)

### Bug Fixes

- Update init command, errors, and tests; remove pyproject.rivetsql.toml
  ([`47eea26`](https://github.com/rivetsql/rivetsql/commit/47eea2636b740ab0e583ccd0c9560406230dda0e))

### Documentation

- Update documentation pages and mkdocs nav
  ([`523403a`](https://github.com/rivetsql/rivetsql/commit/523403aaaf635b97f11a1dd4b3248b27cbfb499e))

- Update README, getting-started, index, joints, and add python-joints guide
  ([`4c80d97`](https://github.com/rivetsql/rivetsql/commit/4c80d97f08c45e1cf0c3bd85491022e0ce0d64a7))


## v1.0.3 (2026-03-06)

### Bug Fixes

- Remove duplicate granular entry points in databricks and postgres plugins
  ([`0a6fd3a`](https://github.com/rivetsql/rivetsql/commit/0a6fd3ad6f2da9ba141fe8b8e0637c00e9c079dc))


## v1.0.2 (2026-03-06)

### Bug Fixes

- Add [project.scripts] rivet entry point to pyproject.toml
  ([`cd6f382`](https://github.com/rivetsql/rivetsql/commit/cd6f38293d1e6dee651b979a5ac46ad3d7a493e5))


## v1.0.1 (2026-03-06)

### Bug Fixes

- Add hatch editable target so pip install -e includes cli, config, bridge
  ([`1b7a3f4`](https://github.com/rivetsql/rivetsql/commit/1b7a3f41d857124159cf5dd5b75efeafb536b002))

### Testing

- Remove 37 brittle/low-value tests (internals, metrics, AST scanning, UI state)
  ([`1e749b7`](https://github.com/rivetsql/rivetsql/commit/1e749b7fa773a3828bedc1187fcea282bb27ee97))

- Remove codebase-introspection and cosmetic tests, keep only feature tests
  ([`25feb45`](https://github.com/rivetsql/rivetsql/commit/25feb4589ae5fddfe49bb5e0b22d1098562959bf))

- Remove TestReadme class - README structure is not a test concern
  ([`63331e0`](https://github.com/rivetsql/rivetsql/commit/63331e0879d711f58196df50b8439a95feb327ac))


## v1.0.0 (2026-03-06)

### Bug Fixes

- Set build_command to empty string in semantic-release config
  ([`2aecf40`](https://github.com/rivetsql/rivetsql/commit/2aecf401c4799f2ac98a4f57605f1244c619a37e))

- Sort and merge imports in test_glue_duckdb_adapter.py
  ([`b7aa21d`](https://github.com/rivetsql/rivetsql/commit/b7aa21d57ea38956b2c62eb4bf7bcf10dcb1a3cc))

### Features

- Pre-commit hooks, architecture diagram, semantic versioning, add rivet_cli to wheel
  ([`8de690e`](https://github.com/rivetsql/rivetsql/commit/8de690eb6b890fe349b492f3c13104cbeb2a188d))


## v0.1.0 (2026-03-05)

- Initial Release
