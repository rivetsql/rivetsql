"""Tests for task 29.5: SQLContext fusion — one fresh SQLContext per fused group, discard after use."""

from __future__ import annotations

from unittest.mock import patch

import polars as pl
import pyarrow as pa
import pytest

from rivet_core.errors import ExecutionError
from rivet_core.models import Joint, Material
from rivet_polars.engine import PolarsComputeEnginePlugin, PolarsLazyMaterializedRef


def _joint(name: str, sql: str) -> Joint:
    return Joint(name=name, joint_type="sql", sql=sql)


class TestExecuteFusedGroup:
    """Property 17: one fresh SQLContext per fused group, discarded after use."""

    def test_single_joint_group_returns_material(self):
        plugin = PolarsComputeEnginePlugin()
        joints = [_joint("j1", "SELECT * FROM src")]
        upstream = {"src": pl.LazyFrame({"a": [1, 2, 3]})}
        result = plugin.execute_fused_group(joints, upstream)
        assert isinstance(result, Material)
        assert result.state == "deferred"

    def test_single_joint_group_result_is_lazy(self):
        plugin = PolarsComputeEnginePlugin()
        joints = [_joint("j1", "SELECT a FROM src WHERE a > 1")]
        upstream = {"src": pl.LazyFrame({"a": [1, 2, 3]})}
        result = plugin.execute_fused_group(joints, upstream)
        assert isinstance(result.materialized_ref, PolarsLazyMaterializedRef)
        assert isinstance(result.materialized_ref._lazy_frame, pl.LazyFrame)

    def test_single_joint_group_correct_data(self):
        plugin = PolarsComputeEnginePlugin()
        joints = [_joint("j1", "SELECT a FROM src WHERE a > 1")]
        upstream = {"src": pl.LazyFrame({"a": [1, 2, 3]})}
        result = plugin.execute_fused_group(joints, upstream)
        table = result.to_arrow()
        assert isinstance(table, pa.Table)
        assert table.column("a").to_pylist() == [2, 3]

    def test_terminal_joint_sql_is_used(self):
        """Only the terminal joint's SQL is executed against the SQLContext."""
        plugin = PolarsComputeEnginePlugin()
        j1 = _joint("j1", "SELECT * FROM src")
        j2 = _joint("j2", "SELECT a FROM src WHERE a = 3")
        upstream = {"src": pl.LazyFrame({"a": [1, 2, 3]})}
        result = plugin.execute_fused_group([j1, j2], upstream)
        table = result.to_arrow()
        assert table.column("a").to_pylist() == [3]

    def test_result_material_name_is_terminal_joint_name(self):
        plugin = PolarsComputeEnginePlugin()
        joints = [_joint("my_joint", "SELECT 1 AS x")]
        result = plugin.execute_fused_group(joints, {})
        assert result.name == "my_joint"

    def test_multiple_upstream_frames_registered(self):
        plugin = PolarsComputeEnginePlugin()
        joints = [_joint("j1", "SELECT a.x, b.y FROM a JOIN b ON a.id = b.id")]
        upstream = {
            "a": pl.LazyFrame({"id": [1, 2], "x": [10, 20]}),
            "b": pl.LazyFrame({"id": [1, 2], "y": [100, 200]}),
        }
        result = plugin.execute_fused_group(joints, upstream)
        table = result.to_arrow()
        assert table.num_rows == 2

    def test_fresh_sqlcontext_per_call(self):
        """Each call to execute_fused_group creates a fresh SQLContext."""
        plugin = PolarsComputeEnginePlugin()
        created_contexts = []

        original_sqlcontext = pl.SQLContext

        class TrackingSQLContext(original_sqlcontext):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                created_contexts.append(self)

        joints = [_joint("j1", "SELECT * FROM src")]
        upstream = {"src": pl.LazyFrame({"a": [1]})}

        with patch("polars.SQLContext", TrackingSQLContext):
            plugin.execute_fused_group(joints, upstream)
            plugin.execute_fused_group(joints, upstream)

        assert len(created_contexts) == 2
        assert created_contexts[0] is not created_contexts[1]

    def test_invalid_sql_raises_rve_501(self):
        plugin = PolarsComputeEnginePlugin()
        joints = [_joint("j1", "SELECT * FROM nonexistent_table")]
        with pytest.raises(ExecutionError) as exc_info:
            plugin.execute_fused_group(joints, {})
        assert exc_info.value.error.code == "RVT-501"

    def test_error_includes_joint_name(self):
        plugin = PolarsComputeEnginePlugin()
        joints = [_joint("failing_joint", "SELECT * FROM missing")]
        with pytest.raises(ExecutionError) as exc_info:
            plugin.execute_fused_group(joints, {})
        assert "failing_joint" in str(exc_info.value.error.context)

    def test_deferred_no_collection_until_to_arrow(self):
        """Material is deferred — LazyFrame not collected until to_arrow() is called."""
        plugin = PolarsComputeEnginePlugin()
        joints = [_joint("j1", "SELECT * FROM src")]
        upstream = {"src": pl.LazyFrame({"a": [1, 2, 3]})}
        result = plugin.execute_fused_group(joints, upstream)
        # Verify it's still a LazyFrame (not collected)
        ref = result.materialized_ref
        assert isinstance(ref, PolarsLazyMaterializedRef)
        assert isinstance(ref._lazy_frame, pl.LazyFrame)
        # Now collect
        table = result.to_arrow()
        assert table.num_rows == 3

    def test_empty_upstream_with_literal_sql(self):
        plugin = PolarsComputeEnginePlugin()
        joints = [_joint("j1", "SELECT 42 AS answer")]
        result = plugin.execute_fused_group(joints, {})
        table = result.to_arrow()
        assert table.column("answer").to_pylist() == [42]
