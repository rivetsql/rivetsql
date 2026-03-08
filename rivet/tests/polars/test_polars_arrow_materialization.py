"""Tests for task 29.7: Arrow materialization — LazyFrame.collect().to_arrow() only on to_arrow() call."""

from __future__ import annotations

from unittest.mock import MagicMock

import polars as pl
import pyarrow as pa

from rivet_polars.engine import PolarsComputeEnginePlugin, PolarsLazyMaterializedRef


class TestArrowMaterializationDeferred:
    """LazyFrame.collect().to_arrow() is only called when to_arrow() is invoked."""

    def test_collect_not_called_on_construction(self):
        lf = MagicMock(spec=pl.LazyFrame)
        PolarsLazyMaterializedRef(lf)
        lf.collect.assert_not_called()

    def test_collect_called_exactly_once_on_to_arrow(self):
        df = pl.DataFrame({"a": [1, 2, 3]})
        lf = MagicMock(spec=pl.LazyFrame)
        lf.collect.return_value = df
        ref = PolarsLazyMaterializedRef(lf)
        ref.to_arrow()
        lf.collect.assert_called_once_with()

    def test_to_arrow_calls_collect_then_to_arrow_on_dataframe(self):
        df_mock = MagicMock()
        df_mock.to_arrow.return_value = pa.table({"a": [1, 2, 3]})
        lf = MagicMock(spec=pl.LazyFrame)
        lf.collect.return_value = df_mock
        ref = PolarsLazyMaterializedRef(lf)
        result = ref.to_arrow()
        lf.collect.assert_called_once_with()
        df_mock.to_arrow.assert_called_once_with()
        assert isinstance(result, pa.Table)

    def test_to_arrow_returns_pyarrow_table(self):
        lf = pl.LazyFrame({"x": [10, 20], "y": ["a", "b"]})
        ref = PolarsLazyMaterializedRef(lf)
        result = ref.to_arrow()
        assert isinstance(result, pa.Table)
        assert result.num_rows == 2
        assert result.column("x").to_pylist() == [10, 20]

    def test_execute_sql_material_not_collected_until_to_arrow(self):
        """Material from execute_sql is deferred — no collection until to_arrow()."""
        plugin = PolarsComputeEnginePlugin()
        upstream = {"t": pl.LazyFrame({"a": [1, 2, 3]})}
        result = plugin.execute_sql_lazy("SELECT * FROM t", upstream)
        ref = result.materialized_ref
        assert isinstance(ref, PolarsLazyMaterializedRef)
        # Replace the lazy frame with a mock to track collect calls
        lf_mock = MagicMock(spec=pl.LazyFrame)
        lf_mock.collect.return_value = pl.DataFrame({"a": [1, 2, 3]})
        ref._lazy_frame = lf_mock
        # Not yet collected
        lf_mock.collect.assert_not_called()
        # Now trigger materialization
        ref.to_arrow()
        lf_mock.collect.assert_called_once_with()

    def test_execute_fused_group_material_not_collected_until_to_arrow(self):
        """Material from execute_fused_group is deferred — no collection until to_arrow()."""
        from rivet_core.models import Joint

        plugin = PolarsComputeEnginePlugin()
        joints = [Joint(name="j1", joint_type="sql", sql="SELECT * FROM src")]
        upstream = {"src": pl.LazyFrame({"a": [1, 2, 3]})}
        result = plugin.execute_fused_group(joints, upstream)
        ref = result.materialized_ref
        assert isinstance(ref, PolarsLazyMaterializedRef)
        lf_mock = MagicMock(spec=pl.LazyFrame)
        lf_mock.collect.return_value = pl.DataFrame({"a": [1, 2, 3]})
        ref._lazy_frame = lf_mock
        lf_mock.collect.assert_not_called()
        ref.to_arrow()
        lf_mock.collect.assert_called_once_with()

    def test_multiple_to_arrow_calls_collect_once(self):
        """Multiple to_arrow() calls collect only once (cached)."""
        df = pl.DataFrame({"a": [1]})
        lf = MagicMock(spec=pl.LazyFrame)
        lf.collect.return_value = df
        ref = PolarsLazyMaterializedRef(lf)
        ref.to_arrow()
        ref.to_arrow()
        assert lf.collect.call_count == 1
