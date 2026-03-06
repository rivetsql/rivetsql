"""Tests for task 29.6: Streaming mode — LazyFrame.collect(streaming=True)."""

from __future__ import annotations

from unittest.mock import MagicMock

import polars as pl
import pyarrow as pa

from rivet_core.models import Joint
from rivet_polars.engine import PolarsComputeEnginePlugin, PolarsLazyMaterializedRef


def _joint(name: str, sql: str) -> Joint:
    return Joint(name=name, joint_type="sql", sql=sql)


class TestPolarsLazyMaterializedRefStreaming:
    """PolarsLazyMaterializedRef uses collect(engine='streaming') when streaming=True."""

    def test_streaming_false_uses_default_collect(self):
        lf = MagicMock(spec=pl.LazyFrame)
        lf.collect.return_value = pl.DataFrame({"a": [1, 2, 3]})
        ref = PolarsLazyMaterializedRef(lf, streaming=False)
        ref.to_arrow()
        lf.collect.assert_called_once_with()

    def test_streaming_true_uses_streaming_engine(self):
        lf = MagicMock(spec=pl.LazyFrame)
        lf.collect.return_value = pl.DataFrame({"a": [1, 2, 3]})
        ref = PolarsLazyMaterializedRef(lf, streaming=True)
        ref.to_arrow()
        lf.collect.assert_called_once_with(engine="streaming")

    def test_streaming_default_is_false(self):
        lf = MagicMock(spec=pl.LazyFrame)
        lf.collect.return_value = pl.DataFrame({"a": [1]})
        ref = PolarsLazyMaterializedRef(lf)
        ref.to_arrow()
        lf.collect.assert_called_once_with()

    def test_streaming_true_returns_correct_data(self):
        lf = pl.LazyFrame({"x": [10, 20, 30]})
        ref = PolarsLazyMaterializedRef(lf, streaming=True)
        table = ref.to_arrow()
        assert isinstance(table, pa.Table)
        assert table.num_rows == 3
        assert table.column("x").to_pylist() == [10, 20, 30]


class TestPolarsEngineStreamingOption:
    """When streaming=True is in engine options, execute_sql and execute_fused_group pass it through."""

    def test_execute_sql_streaming_false_by_default(self):
        plugin = PolarsComputeEnginePlugin()
        upstream = {"t": pl.LazyFrame({"a": [1, 2, 3]})}
        result = plugin.execute_sql_lazy("SELECT * FROM t", upstream)
        ref = result.materialized_ref
        assert isinstance(ref, PolarsLazyMaterializedRef)
        assert ref._streaming is False

    def test_execute_sql_streaming_true_when_option_set(self):
        plugin = PolarsComputeEnginePlugin()
        upstream = {"t": pl.LazyFrame({"a": [1, 2, 3]})}
        result = plugin.execute_sql_lazy("SELECT * FROM t", upstream, streaming=True)
        ref = result.materialized_ref
        assert isinstance(ref, PolarsLazyMaterializedRef)
        assert ref._streaming is True

    def test_execute_fused_group_streaming_false_by_default(self):
        plugin = PolarsComputeEnginePlugin()
        joints = [_joint("j1", "SELECT * FROM src")]
        upstream = {"src": pl.LazyFrame({"a": [1, 2, 3]})}
        result = plugin.execute_fused_group(joints, upstream)
        ref = result.materialized_ref
        assert isinstance(ref, PolarsLazyMaterializedRef)
        assert ref._streaming is False

    def test_execute_fused_group_streaming_true_when_option_set(self):
        plugin = PolarsComputeEnginePlugin()
        joints = [_joint("j1", "SELECT * FROM src")]
        upstream = {"src": pl.LazyFrame({"a": [1, 2, 3]})}
        result = plugin.execute_fused_group(joints, upstream, streaming=True)
        ref = result.materialized_ref
        assert isinstance(ref, PolarsLazyMaterializedRef)
        assert ref._streaming is True

    def test_streaming_true_collect_called_with_streaming_engine(self):
        """End-to-end: streaming=True flows through to collect(engine='streaming')."""
        plugin = PolarsComputeEnginePlugin()
        upstream = {"t": pl.LazyFrame({"a": [1, 2, 3]})}
        result = plugin.execute_sql_lazy("SELECT * FROM t", upstream, streaming=True)
        # Verify the actual collect call uses streaming engine
        ref = result.materialized_ref
        lf_mock = MagicMock(spec=pl.LazyFrame)
        lf_mock.collect.return_value = pl.DataFrame({"a": [1, 2, 3]})
        ref._lazy_frame = lf_mock
        ref.to_arrow()
        lf_mock.collect.assert_called_once_with(engine="streaming")

    def test_streaming_false_collect_called_without_streaming(self):
        """End-to-end: streaming=False flows through to collect() without streaming."""
        plugin = PolarsComputeEnginePlugin()
        upstream = {"t": pl.LazyFrame({"a": [1, 2, 3]})}
        result = plugin.execute_sql_lazy("SELECT * FROM t", upstream, streaming=False)
        ref = result.materialized_ref
        lf_mock = MagicMock(spec=pl.LazyFrame)
        lf_mock.collect.return_value = pl.DataFrame({"a": [1, 2, 3]})
        ref._lazy_frame = lf_mock
        ref.to_arrow()
        lf_mock.collect.assert_called_once_with()
