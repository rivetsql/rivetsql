"""E2E tests for improved CLI output feature.

Exercises the full pipeline lifecycle for the improved CLI output:
run with progress callback, quiet mode, and JSON format.

Uses a simple DuckDB pipeline: CSV source → SQL transform → filesystem sink.

Requirements: All (1.x–9.x)
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from tests.e2e.conftest import run_cli, write_joint, write_sink, write_source

# ---------------------------------------------------------------------------
# Fixture: simple DuckDB pipeline project
# ---------------------------------------------------------------------------


@pytest.fixture()
def cli_output_project(rivet_project: Path) -> Path:
    """Extend the base rivet_project with a simple source → transform → sink pipeline.

    Creates:
      - data/sales.csv          (sample input data)
      - sources/src_sales.sql   (filesystem source)
      - joints/transform.sql    (SQL transform)
      - sinks/sink_out.sql      (filesystem sink)
    """
    project = rivet_project

    # Sample input CSV
    (project / "data" / "sales.csv").write_text(
        "id,product,amount\n1,Widget,100\n2,Gadget,200\n3,Gizmo,150\n"
    )

    # Source joint reading from CSV via filesystem catalog
    write_source(project, "src_sales", catalog="local", table="sales")

    # Simple SQL transform
    write_joint(
        project,
        "transform_sales",
        "SELECT id, product, amount * 2 AS doubled FROM src_sales",
    )

    # Filesystem sink
    write_sink(
        project,
        "sink_out",
        catalog="local",
        table="output_sales",
        upstream=["transform_sales"],
    )

    return project


# ---------------------------------------------------------------------------
# E2E tests for improved CLI output
# ---------------------------------------------------------------------------


def test_run_text_format_exit_code(
    cli_output_project: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Run pipeline with default text format, verify exit code 0 and summary on stdout.

    Requirements: 5.6, 8.2
    """
    result = run_cli(cli_output_project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    # Summary line should be on stdout (requirement 8.2)
    stdout = result.stdout
    assert "joints" in stdout, f"summary missing 'joints': {stdout!r}"
    assert "groups" in stdout, f"summary missing 'groups': {stdout!r}"
    assert "materializations" in stdout, f"summary missing 'materializations': {stdout!r}"

    # Verify the summary line matches the expected format:
    # {elapsed}ms | {joints} joints | {groups} groups | {mats} materializations | {fails} failures
    summary_pattern = re.compile(
        r"\d+ms \| \d+ joints \| \d+ groups \| \d+ materializations \| \d+ failures"
    )
    assert summary_pattern.search(stdout), f"summary line not found in stdout: {stdout!r}"


def test_run_quiet_mode_no_stdout(
    cli_output_project: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Run with --format quiet, verify stdout is empty.

    Requirements: 8.4
    """
    result = run_cli(cli_output_project, ["run", "--format", "quiet"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    # Quiet mode: no stdout output at all (requirement 8.4)
    assert result.stdout == "", f"expected empty stdout in quiet mode, got: {result.stdout!r}"

    # stderr may contain errors but for a successful pipeline it should be minimal;
    # we just verify no crash occurred (exit_code == 0 above covers that).


def test_run_json_format_unchanged(
    cli_output_project: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Run with --format json, verify JSON output schema is unchanged.

    Requirements: 8.3
    """
    result = run_cli(cli_output_project, ["run", "--format", "json"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"

    # stdout must be valid JSON
    data = json.loads(result.stdout)

    # Top-level keys must match the render_run_json schema
    assert "execution" in data, f"missing 'execution' key: {list(data.keys())}"
    assert "compilation" in data, f"missing 'compilation' key: {list(data.keys())}"

    # Execution section must contain core result fields
    execution = data["execution"]
    assert "status" in execution, f"missing 'status' in execution: {list(execution.keys())}"
    assert "joint_results" in execution, (
        f"missing 'joint_results' in execution: {list(execution.keys())}"
    )
