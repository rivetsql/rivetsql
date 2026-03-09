"""``rivet compile`` validation E2E tests: compile validates without executing.

Validates Requirements 4.5, 4.6, 9.1

Note: There is no ``--check`` flag on ``rivet compile``. The compile command
is already validation-only (it never executes the pipeline). These tests
verify that ``rivet compile`` correctly validates pipeline configurations
and reports errors without producing any sink output.
"""

from __future__ import annotations

from pathlib import Path

from tests.e2e.conftest import run_cli, write_joint, write_sink, write_source


def test_compile_valid_pipeline_exits_zero(rivet_project: Path, capsys) -> None:
    """``rivet compile`` exits 0 for a valid pipeline and produces no sink data."""
    project = rivet_project

    (project / "data" / "items.csv").write_text("id,price\n1,10\n2,20\n")
    write_source(project, "src_items", catalog="local", table="items")
    write_joint(project, "double_price", "SELECT id, price * 2 AS price FROM src_items")
    write_sink(
        project, "output",
        catalog="local", table="output", upstream=["double_price"],
    )

    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    # Compile should NOT produce any sink output file
    output_csv = project / "data" / "output.csv"
    assert not output_csv.exists(), (
        "compile should not execute the pipeline or produce sink output"
    )


def test_compile_missing_upstream_exits_nonzero(rivet_project: Path, capsys) -> None:
    """``rivet compile`` exits non-zero when a joint references a missing upstream."""
    project = rivet_project

    joint_content = (
        "-- rivet:name: orphan_joint\n"
        "-- rivet:type: sql\n"
        "-- rivet:upstream: [nonexistent_source]\n"
        "SELECT * FROM nonexistent_source\n"
    )
    (project / "joints" / "orphan_joint.sql").write_text(joint_content)
    write_sink(
        project, "orphan_sink",
        catalog="local", table="orphan_out", upstream=["orphan_joint"],
    )

    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code != 0, (
        f"Expected compile to fail for missing upstream, got exit code 0.\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    assert result.stderr, "Expected error message on stderr"


def test_compile_duplicate_joint_names_exits_nonzero(rivet_project: Path, capsys) -> None:
    """``rivet compile`` exits non-zero when two joints share the same name."""
    project = rivet_project

    (project / "data" / "a.csv").write_text("id\n1\n")
    write_source(project, "src_a", catalog="local", table="a")

    # Two joints with the same name in different files
    write_joint(project, "dup_joint", "SELECT * FROM src_a")
    (project / "joints" / "dup_joint_copy.sql").write_text(
        "-- rivet:name: dup_joint\n"
        "-- rivet:type: sql\n"
        "SELECT * FROM src_a\n"
    )

    write_sink(
        project, "dup_sink",
        catalog="local", table="dup_out", upstream=["dup_joint"],
    )

    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code != 0, (
        f"Expected compile to fail for duplicate names, got exit code 0.\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )


def test_compile_then_run_produces_output(rivet_project: Path, capsys) -> None:
    """Compile alone produces no output; run after compile produces sink data."""
    project = rivet_project

    (project / "data" / "nums.csv").write_text("val\n10\n20\n30\n")
    write_source(project, "src_nums", catalog="local", table="nums")
    write_joint(project, "big_nums", "SELECT val FROM src_nums WHERE val > 15")
    write_sink(
        project, "big_out",
        catalog="local", table="big_out", upstream=["big_nums"],
    )

    # Compile: no output file
    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"
    assert not (project / "data" / "big_out.csv").exists()

    # Run: output file created
    result = run_cli(project, ["run"], capsys)
    assert result.exit_code == 0, f"run failed:\n{result.stderr}"
    assert (project / "data" / "big_out.csv").exists()
