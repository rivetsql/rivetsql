"""Tests for run_single_test (task 8.2)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from rivet_cli.commands.test import run_single_test
from rivet_core.assembly import Assembly
from rivet_core.builtins.arrow_catalog import _get_shared_store
from rivet_core.models import Joint
from rivet_core.plugins import PluginRegistry
from rivet_core.testing.models import TestDef, TestResult


@pytest.fixture(autouse=True)
def _clean_arrow_tables():
    _get_shared_store().clear()
    yield
    _get_shared_store().clear()


def _registry() -> PluginRegistry:
    r = PluginRegistry()
    r.register_builtins()
    return r


def _make_config_result(project_root: Path):
    """Create a mock config_result."""
    from rivet_config.models import ProjectManifest

    manifest = ProjectManifest(
        project_root=project_root,
        profiles_path=project_root / "profiles.yaml",
        sources_dir=project_root / "sources",
        joints_dir=project_root / "joints",
        sinks_dir=project_root / "sinks",
        quality_dir=None,
        tests_dir=project_root / "tests",
        fixtures_dir=project_root / "fixtures",
    )
    config_result = MagicMock()
    config_result.manifest = manifest
    config_result.success = True
    return config_result


def _make_bridge_result(assembly: Assembly):
    """Create a mock BridgeResult."""
    bridge_result = MagicMock()
    bridge_result.assembly = assembly
    return bridge_result


def _simple_assembly():
    """A simple src → transform assembly."""
    src = Joint(name="src", joint_type="source", catalog="prod")
    transform = Joint(
        name="transform", joint_type="sql", catalog="prod",
        upstream=["src"], sql="SELECT * FROM src",
    )
    return Assembly([src, transform])


# ---------------------------------------------------------------------------
# Basic execution tests
# ---------------------------------------------------------------------------


class TestRunSingleTestBasic:
    def test_returns_test_result(self, tmp_path: Path):
        """run_single_test always returns a TestResult."""
        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)

        td = TestDef(
            name="test_basic",
            target="transform",
            scope="joint",
            inputs={"src": {"columns": ["x"], "rows": [[1]]}},
        )

        with patch("rivet_bridge.build_assembly", return_value=bridge_result):
            result = run_single_test(td, config_result, _registry())

        assert isinstance(result, TestResult)
        assert result.name == "test_basic"
        assert result.duration_ms >= 0

    def test_assertion_only_test_passes(self, tmp_path: Path):
        """Test with no expected passes if execution succeeds."""
        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)

        td = TestDef(
            name="test_no_expected",
            target="transform",
            scope="joint",
            inputs={"src": {"columns": ["x"], "rows": [[1]]}},
            expected=None,
        )

        with patch("rivet_bridge.build_assembly", return_value=bridge_result):
            result = run_single_test(td, config_result, _registry())

        assert isinstance(result, TestResult)
        assert result.name == "test_no_expected"
        assert result.passed is True

    def test_passing_comparison(self, tmp_path: Path):
        """Test passes when actual matches expected."""
        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)

        td = TestDef(
            name="test_pass",
            target="transform",
            scope="joint",
            inputs={"src": {"columns": ["x"], "rows": [[42]]}},
            expected={"columns": ["x"], "rows": [[42]]},
        )

        mock_table = pa.table({"x": [42]})
        with patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=mock_table):
            result = run_single_test(td, config_result, _registry())

        assert result.passed is True
        assert result.comparison_result is not None
        assert result.comparison_result.passed is True

    def test_failing_comparison(self, tmp_path: Path):
        """Test fails when actual doesn't match expected."""
        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)

        td = TestDef(
            name="test_fail",
            target="transform",
            scope="joint",
            inputs={"src": {"columns": ["x"], "rows": [[1]]}},
            expected={"columns": ["x"], "rows": [[99]]},
        )

        mock_table = pa.table({"x": [1]})
        with patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=mock_table):
            result = run_single_test(td, config_result, _registry())

        assert result.passed is False
        assert result.comparison_result is not None
        assert result.comparison_result.passed is False


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestRunSingleTestErrors:
    def test_rvt910_on_target_not_found(self, tmp_path: Path):
        """Missing target joint produces RVT-910."""
        assembly = Assembly([Joint(name="src", joint_type="source", catalog="prod")])
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)

        td = TestDef(name="test_err", target="nonexistent", inputs={})

        with patch("rivet_bridge.build_assembly", return_value=bridge_result):
            result = run_single_test(td, config_result, _registry())

        assert result.passed is False
        assert "RVT-910" in result.error

    def test_compilation_failure(self, tmp_path: Path):
        """Compilation failure returns a failed TestResult."""
        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)

        td = TestDef(
            name="test_compile_fail",
            target="transform",
            scope="joint",
            inputs={"src": {"columns": ["x"], "rows": [[1]]}},
        )

        mock_compiled = MagicMock()
        mock_compiled.success = False
        mock_compiled.errors = [MagicMock(message="bad SQL")]

        with patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.compile", return_value=mock_compiled):
            result = run_single_test(td, config_result, _registry())

        assert result.passed is False
        assert "Compilation failed" in result.error

    def test_bridge_failure_caught(self, tmp_path: Path):
        """Bridge errors are caught and reported as RVT-910."""
        config_result = _make_config_result(tmp_path)

        td = TestDef(name="test_bridge_fail", target="t", inputs={})

        with patch("rivet_bridge.build_assembly", side_effect=Exception("bridge broke")):
            result = run_single_test(td, config_result, _registry())

        assert result.passed is False
        assert "RVT-910" in result.error
        assert "bridge broke" in result.error


# ---------------------------------------------------------------------------
# Multi-target tests
# ---------------------------------------------------------------------------


class TestRunSingleTestMultiTarget:
    def test_multi_target_all_pass(self, tmp_path: Path):
        """Multi-target test passes when all targets match."""
        src = Joint(name="src", joint_type="source", catalog="prod")
        t1 = Joint(name="t1", joint_type="sql", catalog="prod", upstream=["src"], sql="SELECT * FROM src")
        t2 = Joint(name="t2", joint_type="sql", catalog="prod", upstream=["src"], sql="SELECT * FROM src")
        assembly = Assembly([src, t1, t2])
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)

        td = TestDef(
            name="test_multi",
            target="t1",
            targets={
                "t1": {"expected": {"columns": ["x"], "rows": [[1]]}},
                "t2": {"expected": {"columns": ["x"], "rows": [[1]]}},
            },
            scope="joint",
            inputs={"src": {"columns": ["x"], "rows": [[1]]}},
        )

        mock_table = pa.table({"x": [1]})
        with patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=mock_table):
            result = run_single_test(td, config_result, _registry())

        assert isinstance(result, TestResult)
        assert result.passed is True

    def test_multi_target_fails_if_any_fails(self, tmp_path: Path):
        """Multi-target test fails if any target comparison fails."""
        src = Joint(name="src", joint_type="source", catalog="prod")
        t1 = Joint(name="t1", joint_type="sql", catalog="prod", upstream=["src"], sql="SELECT * FROM src")
        t2 = Joint(name="t2", joint_type="sql", catalog="prod", upstream=["src"], sql="SELECT * FROM src")
        assembly = Assembly([src, t1, t2])
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)

        td = TestDef(
            name="test_multi_fail",
            target="t1",
            targets={
                "t1": {"expected": {"columns": ["x"], "rows": [[1]]}},
                "t2": {"expected": {"columns": ["x"], "rows": [[99]]}},
            },
            scope="joint",
            inputs={"src": {"columns": ["x"], "rows": [[1]]}},
        )

        mock_table = pa.table({"x": [1]})
        with patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=mock_table):
            result = run_single_test(td, config_result, _registry())

        assert result.passed is False
        assert result.comparison_result is not None


# ---------------------------------------------------------------------------
# update_snapshots parameter
# ---------------------------------------------------------------------------


class TestRunSingleTestSnapshots:
    def test_update_snapshots_param_accepted(self, tmp_path: Path):
        """run_single_test accepts update_snapshots parameter without error."""
        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)

        td = TestDef(
            name="test_snap",
            target="transform",
            scope="joint",
            inputs={"src": {"columns": ["x"], "rows": [[1]]}},
        )

        with patch("rivet_bridge.build_assembly", return_value=bridge_result):
            result = run_single_test(td, config_result, _registry(), update_snapshots=True)

        assert isinstance(result, TestResult)

    def test_snapshot_creates_parquet_file(self, tmp_path: Path):
        """update_snapshots writes actual output as Parquet to expected file path."""
        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)

        snapshot_path = "snapshots/output.parquet"
        td = TestDef(
            name="test_snap_create",
            target="transform",
            scope="joint",
            inputs={"src": {"columns": ["x"], "rows": [[7]]}},
            expected={"file": snapshot_path},
        )

        mock_table = pa.table({"x": [7]})
        with patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=mock_table):
            result = run_single_test(td, config_result, _registry(), update_snapshots=True)

        assert result.passed is True
        written = tmp_path / snapshot_path
        assert written.exists()
        import pyarrow.parquet as pq
        loaded = pq.read_table(str(written))
        assert loaded.equals(mock_table)

    def test_snapshot_overwrites_existing_file(self, tmp_path: Path):
        """update_snapshots overwrites an existing snapshot file."""
        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)

        snapshot_path = "snapshots/output.parquet"
        full_path = tmp_path / snapshot_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        # Write old data
        import pyarrow.parquet as pq
        pq.write_table(pa.table({"x": [99]}), str(full_path))

        td = TestDef(
            name="test_snap_overwrite",
            target="transform",
            scope="joint",
            inputs={"src": {"columns": ["x"], "rows": [[42]]}},
            expected={"file": snapshot_path},
        )

        new_table = pa.table({"x": [42]})
        with patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=new_table):
            result = run_single_test(td, config_result, _registry(), update_snapshots=True)

        assert result.passed is True
        loaded = pq.read_table(str(full_path))
        assert loaded.equals(new_table)

    def test_snapshot_creates_parent_directories(self, tmp_path: Path):
        """update_snapshots creates parent directories if they don't exist."""
        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)

        snapshot_path = "deep/nested/dir/output.parquet"
        td = TestDef(
            name="test_snap_dirs",
            target="transform",
            scope="joint",
            inputs={"src": {"columns": ["x"], "rows": [[1]]}},
            expected={"file": snapshot_path},
        )

        mock_table = pa.table({"x": [1]})
        with patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=mock_table):
            result = run_single_test(td, config_result, _registry(), update_snapshots=True)

        assert result.passed is True
        assert (tmp_path / snapshot_path).exists()

    def test_inline_expected_not_modified_on_snapshot_update(self, tmp_path: Path):
        """update_snapshots does not modify inline expected blocks."""
        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)

        # Inline expected (no 'file' key) — should still compare normally
        td = TestDef(
            name="test_snap_inline",
            target="transform",
            scope="joint",
            inputs={"src": {"columns": ["x"], "rows": [[5]]}},
            expected={"columns": ["x"], "rows": [[5]]},
        )

        mock_table = pa.table({"x": [5]})
        with patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=mock_table):
            result = run_single_test(td, config_result, _registry(), update_snapshots=True)

        # Should compare normally (pass), not skip comparison
        assert result.passed is True
        assert result.comparison_result is not None

    def test_no_snapshot_without_flag(self, tmp_path: Path):
        """Without update_snapshots, file-based expected is compared normally."""
        assembly = _simple_assembly()
        config_result = _make_config_result(tmp_path)
        bridge_result = _make_bridge_result(assembly)

        snapshot_path = "snapshots/output.parquet"
        # Write a matching parquet file
        full_path = tmp_path / snapshot_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        import pyarrow.parquet as pq
        expected_table = pa.table({"x": [10]})
        pq.write_table(expected_table, str(full_path))

        td = TestDef(
            name="test_no_snap_flag",
            target="transform",
            scope="joint",
            inputs={"src": {"columns": ["x"], "rows": [[10]]}},
            expected={"file": snapshot_path},
        )

        mock_table = pa.table({"x": [10]})
        with patch("rivet_bridge.build_assembly", return_value=bridge_result), \
             patch("rivet_core.executor.Executor.run_query_sync", return_value=mock_table):
            result = run_single_test(td, config_result, _registry(), update_snapshots=False)

        # Should compare (pass), not skip
        assert result.passed is True
        assert result.comparison_result is not None
