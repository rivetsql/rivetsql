"""Error scenario E2E tests: compile-time and runtime failures.

Validates Requirements 3.1, 3.2
"""

from __future__ import annotations

from pathlib import Path

from tests.e2e.conftest import run_cli, write_joint, write_sink, write_source


def test_missing_upstream_compile_error(rivet_project: Path, capsys) -> None:
    """Compile fails when a joint declares a non-existent upstream reference.

    Validates: Requirement 3.1
    """
    project = rivet_project

    # Write a SQL joint file manually with an explicit upstream annotation
    # referencing 'missing_source' which does not exist in the project.
    joint_content = (
        "-- rivet:name: bad_joint\n"
        "-- rivet:type: sql\n"
        "-- rivet:upstream: [missing_source]\n"
        "SELECT * FROM missing_source\n"
    )
    (project / "joints" / "bad_joint.sql").write_text(joint_content)

    # Write a sink that depends on the bad joint
    write_sink(
        project,
        "bad_sink",
        catalog="local",
        table="bad_output",
        upstream=["bad_joint"],
    )

    # Compile should fail with a non-zero exit code
    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code != 0, (
        f"Expected compile to fail for missing upstream, but got exit code 0.\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    # stderr should contain an error message about the missing reference
    assert result.stderr, "Expected an error message on stderr for missing upstream"


def test_invalid_sql_runtime_error(rivet_project: Path, capsys) -> None:
    """Run fails when a joint contains SQL that references a nonexistent table.

    Compile succeeds because it doesn't execute SQL, but run fails when
    DuckDB tries to resolve the table.

    Validates: Requirement 3.2
    """
    project = rivet_project

    # Write a CSV data file so the source is valid
    csv_content = "id,value\n1,100\n2,200\n"
    (project / "data" / "items.csv").write_text(csv_content)

    # Write a valid source
    write_source(project, "src_items", catalog="local", table="items")

    # Write a good joint + sink so at least one path succeeds,
    # producing a partial_failure status (exit code 2) instead of
    # a full failure that resolves to exit code 0.
    write_joint(
        project,
        "good_query",
        "SELECT * FROM src_items",
    )
    write_sink(
        project,
        "good_sink",
        catalog="local",
        table="good_output",
        upstream=["good_query"],
    )

    # Write a SQL joint with invalid SQL — references a table that doesn't exist
    write_joint(
        project,
        "bad_query",
        "SELECT * FROM nonexistent_table_xyz",
    )

    # Write a sink depending on the bad query joint
    write_sink(
        project,
        "bad_sink",
        catalog="local",
        table="bad_output",
        upstream=["bad_query"],
    )

    # Compile should succeed (SQL is not executed at compile time)
    compile_result = run_cli(project, ["compile"], capsys)
    assert compile_result.exit_code == 0, (
        f"Expected compile to succeed, but got exit code {compile_result.exit_code}.\n"
        f"stderr: {compile_result.stderr}"
    )

    # Run with --no-fail-fast so the good path succeeds and the bad path
    # produces a partial_failure (exit code 2) rather than a hard failure
    # that resolves to exit code 0 under fail_fast=True.
    run_result = run_cli(project, ["run", "--no-fail-fast"], capsys)
    assert run_result.exit_code != 0, (
        f"Expected run to fail for invalid SQL, but got exit code 0.\n"
        f"stdout: {run_result.stdout}\nstderr: {run_result.stderr}"
    )
    # The error message about the invalid table should appear in the output
    combined_output = run_result.stdout + run_result.stderr
    assert "nonexistent_table_xyz" in combined_output, (
        f"Expected error output to mention 'nonexistent_table_xyz'.\n"
        f"stdout: {run_result.stdout}\nstderr: {run_result.stderr}"
    )
