"""Init command: scaffold a new Rivet project."""

from __future__ import annotations

import sys
from pathlib import Path

from rivet_cli.app import GlobalOptions
from rivet_cli.errors import CLIError, format_cli_error
from rivet_cli.exit_codes import GENERAL_ERROR, SUCCESS
from rivet_cli.rendering.colors import BOLD, GREEN, SYM_CHECK, colorize

VALID_STYLES = ("mixed", "sql", "yaml")

# ---------------------------------------------------------------------------
# Shared templates
# ---------------------------------------------------------------------------

_RIVET_YAML = """\
# Rivet project manifest
profiles: profiles.yaml
sources: sources
joints: joints
sinks: sinks
tests: tests
quality: quality
"""

_PROFILES_YAML = """\
# Rivet profiles
default:
  catalogs:
    local:
      type: filesystem
      path: ./data
  engines:
    - name: default
      type: duckdb
      catalogs: [local]
  default_engine: default
"""

_EXAMPLE_TEST = """\
# Test: transform_orders
name: test_transform_orders
joint: transform_orders
inputs:
  raw_orders:
    rows:
      - {id: 1, customer_name: Alice, amount: 100, created_at: "2024-01-01"}
expected:
  rows:
    - {id: 1, customer_name: Alice, amount: 100, created_at: "2024-01-01"}
"""

_EXAMPLE_QUALITY = """\
# Quality checks for orders_clean
assertions:
  - type: not_null
    columns: [id, customer_name]
    severity: error
  - type: unique
    columns: [id]
    severity: error
"""

_SAMPLE_DATA_CSV = """\
id,customer_name,amount,created_at
1,Alice,100,2024-01-01
2,Bob,250,2024-01-02
3,Charlie,-10,2024-01-03
4,Diana,75,2024-01-04
5,Eve,0,2024-01-05
"""

# ---------------------------------------------------------------------------
# YAML-style templates
# ---------------------------------------------------------------------------

_SOURCE_YAML = """\
# Source: raw_orders
name: raw_orders
type: source
catalog: local
table: raw_orders.csv
"""

_JOINT_YAML = """\
# Joint: transform_orders
name: transform_orders
type: sql
upstream:
  - raw_orders
sql: |
  SELECT
      id,
      customer_name,
      amount,
      created_at
  FROM raw_orders
  WHERE amount > 0
"""

_SINK_YAML = """\
# Sink: orders_clean
name: orders_clean
type: sink
catalog: local
table: orders_clean
upstream:
  - transform_orders
"""

# ---------------------------------------------------------------------------
# SQL-style templates
# ---------------------------------------------------------------------------

_SOURCE_SQL = """\
-- rivet:name: raw_orders
-- rivet:type: source
-- rivet:catalog: local
-- rivet:table: raw_orders.csv
SELECT * FROM raw_orders
"""

_JOINT_SQL = """\
-- rivet:name: transform_orders
-- rivet:type: sql
SELECT
    id,
    customer_name,
    amount,
    created_at
FROM raw_orders
WHERE amount > 0
"""

_SINK_SQL = """\
-- rivet:name: orders_clean
-- rivet:type: sink
-- rivet:catalog: local
-- rivet:table: orders_clean
-- rivet:upstream: [transform_orders]
"""

_DIRS = ["sources", "joints", "sinks", "tests", "quality", "data"]


def _build_example_files(style: str) -> dict[str, str]:
    """Build the example file map based on declaration style."""
    files: dict[str, str] = {
        "tests/test_transform_orders.yaml": _EXAMPLE_TEST,
        "quality/orders_clean.yaml": _EXAMPLE_QUALITY,
    }
    if style == "yaml":
        files["sources/raw_orders.yaml"] = _SOURCE_YAML
        files["joints/transform_orders.yaml"] = _JOINT_YAML
        files["sinks/orders_clean.yaml"] = _SINK_YAML
    elif style == "sql":
        files["sources/raw_orders.sql"] = _SOURCE_SQL
        files["joints/transform_orders.sql"] = _JOINT_SQL
        files["sinks/orders_clean.sql"] = _SINK_SQL
    else:
        # mixed (default): sources/sinks as YAML, joints as SQL
        files["sources/raw_orders.yaml"] = _SOURCE_YAML
        files["joints/transform_orders.sql"] = _JOINT_SQL
        files["sinks/orders_clean.yaml"] = _SINK_YAML
    return files


def run_init(
    directory: str | None,
    bare: bool,
    style: str,
    globals: GlobalOptions,
) -> int:
    """Scaffold a new Rivet project.

    directory: target path (created if missing). Defaults to current dir.
    bare: create directory structure only, no example files.
    style: "sql" or "yaml" — controls how sources, joints, and sinks are declared.
    """
    if style not in VALID_STYLES:
        err = CLIError(
            code="RVT-855",
            message=f"Unknown style '{style}'. Valid options: {', '.join(VALID_STYLES)}.",
            remediation="Use --style sql or --style yaml.",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return GENERAL_ERROR

    target = Path(directory) if directory else Path(".")
    target.mkdir(parents=True, exist_ok=True)

    # Check for non-empty directory
    if any(target.iterdir()):
        err = CLIError(
            code="RVT-857",
            message=f"Directory is not empty: {target}",
            remediation="Use an empty directory, or initialize in a new path with: rivet init <directory>",
        )
        print(format_cli_error(err, globals.color), file=sys.stderr)
        return GENERAL_ERROR

    created: list[str] = []

    # Create rivet.yaml and profiles.yaml
    rivet_yaml_path = target / "rivet.yaml"
    rivet_yaml_path.write_text(_RIVET_YAML)
    created.append("rivet.yaml")

    (target / "profiles.yaml").write_text(_PROFILES_YAML)
    created.append("profiles.yaml")

    # Create directories
    for d in _DIRS:
        (target / d).mkdir(exist_ok=True)
        created.append(f"{d}/")

    # Create example files unless --bare
    if not bare:
        example_files = _build_example_files(style)
        for rel_path in sorted(example_files):
            (target / rel_path).write_text(example_files[rel_path])
            created.append(rel_path)

        # Create sample data
        data_file = target / "data" / "raw_orders.csv"
        data_file.write_text(_SAMPLE_DATA_CSV)
        created.append("data/raw_orders.csv")

    # Display summary
    color = globals.color
    print(colorize(f"{SYM_CHECK} Project initialized: {target}", GREEN, color))
    print()
    print(colorize("Created:", BOLD, color))
    for f in created:
        print(f"  {f}")
    print()
    print("Get started:")
    print("  rivet compile   — validate and inspect the pipeline")
    print("  rivet test      — run tests against fixtures")
    print("  rivet run       — execute the pipeline")

    return SUCCESS
