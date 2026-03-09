"""``rivet test`` command E2E tests: test discovery, execution, and reporting.

Validates Requirements 4.4, 4.6, 9.1
"""

from __future__ import annotations

from pathlib import Path

from tests.e2e.conftest import run_cli, write_joint, write_sink, write_source


def _setup_pipeline(project: Path) -> None:
    """Create a minimal source -> joint -> sink pipeline for testing."""
    (project / "data" / "scores.csv").write_text(
        "student,score\nAlice,90\nBob,70\nCharlie,85\n"
    )
    write_source(project, "src_scores", catalog="local", table="scores")
    write_joint(
        project, "top_students",
        "SELECT student, score FROM src_scores WHERE score >= 80",
    )
    write_sink(
        project, "results",
        catalog="local", table="results", upstream=["top_students"],
    )


def test_passing_test_exits_zero(rivet_project: Path, capsys) -> None:
    """``rivet test`` exits 0 when all test definitions pass."""
    project = rivet_project
    _setup_pipeline(project)

    # Write a test definition that expects the correct output
    (project / "tests" / "top_students.test.yaml").write_text(
        "name: top_students_check\n"
        "target: top_students\n"
        "scope: joint\n"
        "inputs:\n"
        "  src_scores:\n"
        "    columns: [student, score]\n"
        "    rows:\n"
        "      - [Alice, 90]\n"
        "      - [Bob, 70]\n"
        "      - [Charlie, 85]\n"
        "expected:\n"
        "  columns: [student, score]\n"
        "  rows:\n"
        "    - [Alice, 90]\n"
        "    - [Charlie, 85]\n"
        "compare: exact\n"
    )

    # Compile first to ensure the pipeline is valid
    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    # Run rivet test
    result = run_cli(project, ["test"], capsys)
    assert result.exit_code == 0, (
        f"rivet test failed with exit code {result.exit_code}:\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )


def test_failing_test_exits_three(rivet_project: Path, capsys) -> None:
    """``rivet test`` exits 3 when a test definition fails."""
    project = rivet_project
    _setup_pipeline(project)

    # Write a test definition with WRONG expected output
    (project / "tests" / "bad_check.test.yaml").write_text(
        "name: bad_check\n"
        "target: top_students\n"
        "scope: joint\n"
        "inputs:\n"
        "  src_scores:\n"
        "    columns: [student, score]\n"
        "    rows:\n"
        "      - [Alice, 90]\n"
        "      - [Bob, 70]\n"
        "expected:\n"
        "  columns: [student, score]\n"
        "  rows:\n"
        "    - [Alice, 90]\n"
        "    - [Bob, 70]\n"
        "compare: exact\n"
    )

    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    result = run_cli(project, ["test"], capsys)
    assert result.exit_code == 3, (
        f"Expected exit code 3 (TEST_FAILURE), got {result.exit_code}:\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )


def test_no_tests_found_exits_zero(rivet_project: Path, capsys) -> None:
    """``rivet test`` exits 0 when no test definitions exist."""
    project = rivet_project
    _setup_pipeline(project)

    result = run_cli(project, ["compile"], capsys)
    assert result.exit_code == 0, f"compile failed:\n{result.stderr}"

    result = run_cli(project, ["test"], capsys)
    assert result.exit_code == 0, (
        f"Expected exit code 0 for no tests, got {result.exit_code}:\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
