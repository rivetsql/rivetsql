"""Integration tests for the test CLI command (task 10.3).

Tests end-to-end: create temp project with test YAML files, run run_test,
verify output, exit codes, filtering, and JSON format.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
import yaml

from rivet_cli.app import GlobalOptions
from rivet_cli.commands.test import run_test
from rivet_cli.exit_codes import GENERAL_ERROR, SUCCESS, TEST_FAILURE
from rivet_core.assembly import Assembly
from rivet_core.builtins.arrow_catalog import _get_shared_store
from rivet_core.models import Joint

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _globals(**overrides) -> GlobalOptions:
    defaults = dict(profile="default", project_path=Path("."), verbosity=0, color=False)
    defaults.update(overrides)
    return GlobalOptions(**defaults)


def _make_manifest(project_root: Path):
    from rivet_config.models import ProjectManifest
    return ProjectManifest(
        project_root=project_root,
        profiles_path=project_root / "profiles.yaml",
        sources_dir=project_root / "sources",
        joints_dir=project_root / "joints",
        sinks_dir=project_root / "sinks",
        quality_dir=None,
        tests_dir=project_root / "tests",
        fixtures_dir=project_root / "fixtures",
    )


def _make_config_result(project_root: Path, success: bool = True):
    cr = MagicMock()
    cr.success = success
    cr.errors = [] if success else [MagicMock(message="bad config", remediation="fix it")]
    cr.manifest = _make_manifest(project_root)
    return cr


def _simple_assembly():
    src = Joint(name="src", joint_type="source", catalog="prod")
    transform = Joint(
        name="transform", joint_type="sql", catalog="prod",
        upstream=["src"], sql="SELECT * FROM src",
    )
    return Assembly([src, transform])


def _make_bridge_result(assembly: Assembly):
    br = MagicMock()
    br.assembly = assembly
    return br


@pytest.fixture(autouse=True)
def _clean_arrow_tables():
    _get_shared_store().clear()
    yield
    _get_shared_store().clear()


def _write_test_yaml(tests_dir: Path, filename: str, docs: list[dict]) -> Path:
    """Write one or more test docs to a .test.yaml file."""
    tests_dir.mkdir(parents=True, exist_ok=True)
    path = tests_dir / filename
    content = "\n---\n".join(yaml.dump(d) for d in docs)
    path.write_text(content)
    return path


# ---------------------------------------------------------------------------
# End-to-end: passing test
# ---------------------------------------------------------------------------


class TestRunTestEndToEnd:
    def test_passing_test_returns_exit_0(self, tmp_path: Path, capsys):
        """A passing test returns exit code 0 and shows PASS in output."""
        tests_dir = tmp_path / "tests"
        _write_test_yaml(tests_dir, "basic.test.yaml", [{
            "name": "test_basic",
            "target": "transform",
            "scope": "joint",
            "inputs": {"src": {"columns": ["x"], "rows": [[1]]}},
            "expected": {"columns": ["x"], "rows": [[1]]},
        }])

        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)
        mock_table = pa.table({"x": [1]})

        with patch("rivet_config.load_config", return_value=config_result), \
             patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=mock_table), \
             patch("rivet_bridge.register_optional_plugins"):
            exit_code = run_test(
                tags=[], tag_all=False, target=None, file_paths=[],
                update_snapshots=False, fail_fast=False, format="text",
                globals=_globals(project_path=tmp_path),
            )

        assert exit_code == SUCCESS
        captured = capsys.readouterr()
        assert "PASS" in captured.out or "✓" in captured.out
        assert "test_basic" in captured.out

    def test_failing_test_returns_exit_3(self, tmp_path: Path, capsys):
        """A failing test returns exit code 3 and shows FAIL in output."""
        tests_dir = tmp_path / "tests"
        _write_test_yaml(tests_dir, "fail.test.yaml", [{
            "name": "test_fail",
            "target": "transform",
            "scope": "joint",
            "inputs": {"src": {"columns": ["x"], "rows": [[1]]}},
            "expected": {"columns": ["x"], "rows": [[99]]},
        }])

        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)
        mock_table = pa.table({"x": [1]})  # actual != expected (99)

        with patch("rivet_config.load_config", return_value=config_result), \
             patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=mock_table), \
             patch("rivet_bridge.register_optional_plugins"):
            exit_code = run_test(
                tags=[], tag_all=False, target=None, file_paths=[],
                update_snapshots=False, fail_fast=False, format="text",
                globals=_globals(project_path=tmp_path),
            )

        assert exit_code == TEST_FAILURE
        captured = capsys.readouterr()
        assert "FAIL" in captured.out or "✗" in captured.out
        assert "test_fail" in captured.out

    def test_summary_line_shown(self, tmp_path: Path, capsys):
        """Output includes a summary line with pass/fail counts."""
        tests_dir = tmp_path / "tests"
        _write_test_yaml(tests_dir, "tests.test.yaml", [
            {"name": "t1", "target": "transform", "scope": "joint",
             "inputs": {"src": {"columns": ["x"], "rows": [[1]]}},
             "expected": {"columns": ["x"], "rows": [[1]]}},
            {"name": "t2", "target": "transform", "scope": "joint",
             "inputs": {"src": {"columns": ["x"], "rows": [[2]]}},
             "expected": {"columns": ["x"], "rows": [[99]]}},
        ])

        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)

        call_count = [0]
        def mock_run_query(compiled, target):
            call_count[0] += 1
            # t1 gets [1], t2 gets [2] (won't match expected [99])
            return pa.table({"x": [call_count[0]]})

        with patch("rivet_config.load_config", return_value=config_result), \
             patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", side_effect=mock_run_query), \
             patch("rivet_bridge.register_optional_plugins"):
            run_test(
                tags=[], tag_all=False, target=None, file_paths=[],
                update_snapshots=False, fail_fast=False, format="text",
                globals=_globals(project_path=tmp_path),
            )

        captured = capsys.readouterr()
        assert "total" in captured.out

    def test_no_tests_found_returns_success(self, tmp_path: Path, capsys):
        """When no tests match, returns SUCCESS and prints a message."""
        config_result = _make_config_result(tmp_path)

        with patch("rivet_config.load_config", return_value=config_result), \
             patch("rivet_bridge.register_optional_plugins"):
            exit_code = run_test(
                tags=[], tag_all=False, target=None, file_paths=[],
                update_snapshots=False, fail_fast=False, format="text",
                globals=_globals(project_path=tmp_path),
            )

        assert exit_code == SUCCESS
        captured = capsys.readouterr()
        assert "No tests found" in captured.err

    def test_config_failure_returns_general_error(self, tmp_path: Path):
        """Config load failure returns GENERAL_ERROR."""
        config_result = _make_config_result(tmp_path, success=False)

        with patch("rivet_config.load_config", return_value=config_result), \
             patch("rivet_bridge.register_optional_plugins"):
            exit_code = run_test(
                tags=[], tag_all=False, target=None, file_paths=[],
                update_snapshots=False, fail_fast=False, format="text",
                globals=_globals(project_path=tmp_path),
            )

        assert exit_code == GENERAL_ERROR


# ---------------------------------------------------------------------------
# Tag filtering
# ---------------------------------------------------------------------------


class TestTagFiltering:
    def test_tag_filter_runs_only_matching_tests(self, tmp_path: Path, capsys):
        """--tag runs only tests with the matching tag."""
        tests_dir = tmp_path / "tests"
        _write_test_yaml(tests_dir, "tagged.test.yaml", [
            {"name": "tagged_test", "target": "transform", "scope": "joint",
             "tags": ["smoke"],
             "inputs": {"src": {"columns": ["x"], "rows": [[1]]}},
             "expected": {"columns": ["x"], "rows": [[1]]}},
            {"name": "untagged_test", "target": "transform", "scope": "joint",
             "inputs": {"src": {"columns": ["x"], "rows": [[2]]}},
             "expected": {"columns": ["x"], "rows": [[2]]}},
        ])

        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)
        mock_table = pa.table({"x": [1]})

        with patch("rivet_config.load_config", return_value=config_result), \
             patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=mock_table), \
             patch("rivet_bridge.register_optional_plugins"):
            exit_code = run_test(
                tags=["smoke"], tag_all=False, target=None, file_paths=[],
                update_snapshots=False, fail_fast=False, format="text",
                globals=_globals(project_path=tmp_path),
            )

        assert exit_code == SUCCESS
        captured = capsys.readouterr()
        assert "tagged_test" in captured.out
        assert "untagged_test" not in captured.out

    def test_tag_filter_no_match_returns_success(self, tmp_path: Path, capsys):
        """--tag with no matching tests returns SUCCESS."""
        tests_dir = tmp_path / "tests"
        _write_test_yaml(tests_dir, "t.test.yaml", [{
            "name": "t1", "target": "transform", "scope": "joint",
            "tags": ["other"],
            "inputs": {"src": {"columns": ["x"], "rows": [[1]]}},
        }])

        config_result = _make_config_result(tmp_path)

        with patch("rivet_config.load_config", return_value=config_result), \
             patch("rivet_bridge.register_optional_plugins"):
            exit_code = run_test(
                tags=["nonexistent"], tag_all=False, target=None, file_paths=[],
                update_snapshots=False, fail_fast=False, format="text",
                globals=_globals(project_path=tmp_path),
            )

        assert exit_code == SUCCESS
        captured = capsys.readouterr()
        assert "No tests found" in captured.err


# ---------------------------------------------------------------------------
# Target filtering
# ---------------------------------------------------------------------------


class TestTargetFiltering:
    def test_target_filter_runs_only_matching_tests(self, tmp_path: Path, capsys):
        """--target runs only tests targeting the given joint."""
        src = Joint(name="src", joint_type="source", catalog="prod")
        j1 = Joint(name="j1", joint_type="sql", catalog="prod",
                   upstream=["src"], sql="SELECT * FROM src")
        j2 = Joint(name="j2", joint_type="sql", catalog="prod",
                   upstream=["src"], sql="SELECT * FROM src")
        assembly = Assembly([src, j1, j2])

        tests_dir = tmp_path / "tests"
        _write_test_yaml(tests_dir, "targets.test.yaml", [
            {"name": "test_j1", "target": "j1", "scope": "joint",
             "inputs": {"src": {"columns": ["x"], "rows": [[1]]}},
             "expected": {"columns": ["x"], "rows": [[1]]}},
            {"name": "test_j2", "target": "j2", "scope": "joint",
             "inputs": {"src": {"columns": ["x"], "rows": [[2]]}},
             "expected": {"columns": ["x"], "rows": [[2]]}},
        ])

        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)
        mock_table = pa.table({"x": [1]})

        with patch("rivet_config.load_config", return_value=config_result), \
             patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=mock_table), \
             patch("rivet_bridge.register_optional_plugins"):
            exit_code = run_test(
                tags=[], tag_all=False, target="j1", file_paths=[],
                update_snapshots=False, fail_fast=False, format="text",
                globals=_globals(project_path=tmp_path),
            )

        assert exit_code == SUCCESS
        captured = capsys.readouterr()
        assert "test_j1" in captured.out
        assert "test_j2" not in captured.out


# ---------------------------------------------------------------------------
# JSON format output
# ---------------------------------------------------------------------------


class TestJsonFormat:
    def test_json_format_produces_valid_json(self, tmp_path: Path, capsys):
        """--format json outputs valid JSON."""
        tests_dir = tmp_path / "tests"
        _write_test_yaml(tests_dir, "json.test.yaml", [{
            "name": "json_test",
            "target": "transform",
            "scope": "joint",
            "inputs": {"src": {"columns": ["x"], "rows": [[5]]}},
            "expected": {"columns": ["x"], "rows": [[5]]},
        }])

        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)
        mock_table = pa.table({"x": [5]})

        with patch("rivet_config.load_config", return_value=config_result), \
             patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=mock_table), \
             patch("rivet_bridge.register_optional_plugins"):
            run_test(
                tags=[], tag_all=False, target=None, file_paths=[],
                update_snapshots=False, fail_fast=False, format="json",
                globals=_globals(project_path=tmp_path),
            )

        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["name"] == "json_test"
        assert data[0]["passed"] is True

    def test_json_format_includes_failure_details(self, tmp_path: Path, capsys):
        """--format json includes comparison_result for failures."""
        tests_dir = tmp_path / "tests"
        _write_test_yaml(tests_dir, "json_fail.test.yaml", [{
            "name": "json_fail",
            "target": "transform",
            "scope": "joint",
            "inputs": {"src": {"columns": ["x"], "rows": [[1]]}},
            "expected": {"columns": ["x"], "rows": [[99]]},
        }])

        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)
        mock_table = pa.table({"x": [1]})

        with patch("rivet_config.load_config", return_value=config_result), \
             patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=mock_table), \
             patch("rivet_bridge.register_optional_plugins"):
            exit_code = run_test(
                tags=[], tag_all=False, target=None, file_paths=[],
                update_snapshots=False, fail_fast=False, format="json",
                globals=_globals(project_path=tmp_path),
            )

        assert exit_code == TEST_FAILURE
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data[0]["passed"] is False
        assert "comparison_result" in data[0]
        assert data[0]["comparison_result"]["passed"] is False

    def test_json_format_multiple_results(self, tmp_path: Path, capsys):
        """--format json outputs all results as a JSON array."""
        tests_dir = tmp_path / "tests"
        _write_test_yaml(tests_dir, "multi.test.yaml", [
            {"name": "t1", "target": "transform", "scope": "joint",
             "inputs": {"src": {"columns": ["x"], "rows": [[1]]}},
             "expected": {"columns": ["x"], "rows": [[1]]}},
            {"name": "t2", "target": "transform", "scope": "joint",
             "inputs": {"src": {"columns": ["x"], "rows": [[2]]}},
             "expected": {"columns": ["x"], "rows": [[2]]}},
        ])

        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)
        mock_table = pa.table({"x": [1]})

        with patch("rivet_config.load_config", return_value=config_result), \
             patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=mock_table), \
             patch("rivet_bridge.register_optional_plugins"):
            run_test(
                tags=[], tag_all=False, target=None, file_paths=[],
                update_snapshots=False, fail_fast=False, format="json",
                globals=_globals(project_path=tmp_path),
            )

        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert len(data) == 2
        assert {d["name"] for d in data} == {"t1", "t2"}


# ---------------------------------------------------------------------------
# Exit codes
# ---------------------------------------------------------------------------


class TestExitCodes:
    def test_exit_0_all_pass(self, tmp_path: Path):
        """Returns 0 when all tests pass."""
        tests_dir = tmp_path / "tests"
        _write_test_yaml(tests_dir, "pass.test.yaml", [{
            "name": "pass_test",
            "target": "transform",
            "scope": "joint",
            "inputs": {"src": {"columns": ["x"], "rows": [[1]]}},
            "expected": {"columns": ["x"], "rows": [[1]]},
        }])

        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)
        mock_table = pa.table({"x": [1]})

        with patch("rivet_config.load_config", return_value=config_result), \
             patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=mock_table), \
             patch("rivet_bridge.register_optional_plugins"):
            exit_code = run_test(
                tags=[], tag_all=False, target=None, file_paths=[],
                update_snapshots=False, fail_fast=False, format="text",
                globals=_globals(project_path=tmp_path),
            )

        assert exit_code == SUCCESS

    def test_exit_3_any_failure(self, tmp_path: Path):
        """Returns 3 when any test fails."""
        tests_dir = tmp_path / "tests"
        _write_test_yaml(tests_dir, "mixed.test.yaml", [
            {"name": "pass_test", "target": "transform", "scope": "joint",
             "inputs": {"src": {"columns": ["x"], "rows": [[1]]}},
             "expected": {"columns": ["x"], "rows": [[1]]}},
            {"name": "fail_test", "target": "transform", "scope": "joint",
             "inputs": {"src": {"columns": ["x"], "rows": [[1]]}},
             "expected": {"columns": ["x"], "rows": [[99]]}},
        ])

        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)
        mock_table = pa.table({"x": [1]})

        with patch("rivet_config.load_config", return_value=config_result), \
             patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=mock_table), \
             patch("rivet_bridge.register_optional_plugins"):
            exit_code = run_test(
                tags=[], tag_all=False, target=None, file_paths=[],
                update_snapshots=False, fail_fast=False, format="text",
                globals=_globals(project_path=tmp_path),
            )

        assert exit_code == TEST_FAILURE

    def test_exit_1_on_config_error(self, tmp_path: Path):
        """Returns GENERAL_ERROR (1) when config loading fails."""
        config_result = _make_config_result(tmp_path, success=False)

        with patch("rivet_config.load_config", return_value=config_result), \
             patch("rivet_bridge.register_optional_plugins"):
            exit_code = run_test(
                tags=[], tag_all=False, target=None, file_paths=[],
                update_snapshots=False, fail_fast=False, format="text",
                globals=_globals(project_path=tmp_path),
            )

        assert exit_code == GENERAL_ERROR


# ---------------------------------------------------------------------------
# Verbose output
# ---------------------------------------------------------------------------


class TestVerboseOutput:
    def test_verbose_flag_accepted(self, tmp_path: Path, capsys):
        """verbosity=1 is accepted without error."""
        tests_dir = tmp_path / "tests"
        _write_test_yaml(tests_dir, "v.test.yaml", [{
            "name": "verbose_test",
            "target": "transform",
            "scope": "joint",
            "inputs": {"src": {"columns": ["x"], "rows": [[1]]}},
            "expected": {"columns": ["x"], "rows": [[1]]},
        }])

        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)
        mock_table = pa.table({"x": [1]})

        with patch("rivet_config.load_config", return_value=config_result), \
             patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=mock_table), \
             patch("rivet_bridge.register_optional_plugins"):
            exit_code = run_test(
                tags=[], tag_all=False, target=None, file_paths=[],
                update_snapshots=False, fail_fast=False, format="text",
                globals=_globals(project_path=tmp_path, verbosity=1),
            )

        assert exit_code == SUCCESS
        captured = capsys.readouterr()
        assert "verbose_test" in captured.out

    def test_failure_shows_diff_in_output(self, tmp_path: Path, capsys):
        """Failing test output includes diff details."""
        tests_dir = tmp_path / "tests"
        _write_test_yaml(tests_dir, "diff.test.yaml", [{
            "name": "diff_test",
            "target": "transform",
            "scope": "joint",
            "inputs": {"src": {"columns": ["x"], "rows": [[1]]}},
            "expected": {"columns": ["x"], "rows": [[99]]},
        }])

        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)
        mock_table = pa.table({"x": [1]})

        with patch("rivet_config.load_config", return_value=config_result), \
             patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=mock_table), \
             patch("rivet_bridge.register_optional_plugins"):
            run_test(
                tags=[], tag_all=False, target=None, file_paths=[],
                update_snapshots=False, fail_fast=False, format="text",
                globals=_globals(project_path=tmp_path, verbosity=1),
            )

        captured = capsys.readouterr()
        # Should show the test name and some failure info
        assert "diff_test" in captured.out


# ---------------------------------------------------------------------------
# File path filtering
# ---------------------------------------------------------------------------


class TestFilePathFiltering:
    def test_file_path_filter_runs_only_tests_from_file(self, tmp_path: Path, capsys):
        """Providing file paths runs only tests from those files."""
        tests_dir = tmp_path / "tests"
        file_a = _write_test_yaml(tests_dir, "a.test.yaml", [{
            "name": "test_a",
            "target": "transform",
            "scope": "joint",
            "inputs": {"src": {"columns": ["x"], "rows": [[1]]}},
            "expected": {"columns": ["x"], "rows": [[1]]},
        }])
        _write_test_yaml(tests_dir, "b.test.yaml", [{
            "name": "test_b",
            "target": "transform",
            "scope": "joint",
            "inputs": {"src": {"columns": ["x"], "rows": [[2]]}},
            "expected": {"columns": ["x"], "rows": [[2]]},
        }])

        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)
        mock_table = pa.table({"x": [1]})

        with patch("rivet_config.load_config", return_value=config_result), \
             patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=mock_table), \
             patch("rivet_bridge.register_optional_plugins"):
            exit_code = run_test(
                tags=[], tag_all=False, target=None, file_paths=[file_a],
                update_snapshots=False, fail_fast=False, format="text",
                globals=_globals(project_path=tmp_path),
            )

        assert exit_code == SUCCESS
        captured = capsys.readouterr()
        assert "test_a" in captured.out
        assert "test_b" not in captured.out
