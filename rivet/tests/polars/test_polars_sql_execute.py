"""Tests for task 29.4: Execute SQL via polars.SQLContext, return deferred Material backed by LazyFrame."""

from __future__ import annotations

import polars as pl
import pyarrow as pa
import pytest

from rivet_core.models import Material
from rivet_polars.engine import PolarsComputeEnginePlugin, PolarsLazyMaterializedRef


class TestPolarsLazyMaterializedRef:
    """Test the LazyFrame-backed MaterializedRef."""

    def test_to_arrow_returns_pyarrow_table(self):
        lf = pl.LazyFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        ref = PolarsLazyMaterializedRef(lf)
        result = ref.to_arrow()
        assert isinstance(result, pa.Table)
        assert result.num_rows == 3

    def test_deferred_until_to_arrow(self):
        """LazyFrame should not be collected until to_arrow() is called."""
        lf = pl.LazyFrame({"a": [1, 2, 3]})
        ref = PolarsLazyMaterializedRef(lf)
        # The ref holds a LazyFrame, not a collected DataFrame
        assert ref._lazy_frame is lf
        # Now collect
        table = ref.to_arrow()
        assert table.num_rows == 3

    def test_schema_property(self):
        lf = pl.LazyFrame({"x": [1], "y": [2.0]})
        ref = PolarsLazyMaterializedRef(lf)
        schema = ref.schema
        assert len(schema.columns) == 2
        assert schema.columns[0].name == "x"
        assert schema.columns[1].name == "y"

    def test_row_count(self):
        lf = pl.LazyFrame({"a": [10, 20, 30]})
        ref = PolarsLazyMaterializedRef(lf)
        assert ref.row_count == 3

    def test_size_bytes_returns_int_or_none(self):
        lf = pl.LazyFrame({"a": [1]})
        ref = PolarsLazyMaterializedRef(lf)
        result = ref.size_bytes
        assert result is None or isinstance(result, int)

    def test_storage_type(self):
        lf = pl.LazyFrame({"a": [1]})
        ref = PolarsLazyMaterializedRef(lf)
        assert ref.storage_type == "polars_lazy"


class TestPolarsExecuteSQL:
    """Test SQL execution via polars.SQLContext."""

    def test_execute_sql_returns_material(self):
        plugin = PolarsComputeEnginePlugin()
        upstream = {"source": pl.LazyFrame({"id": [1, 2, 3], "val": [10, 20, 30]})}
        result = plugin.execute_sql_lazy("SELECT id, val FROM source WHERE val > 10", upstream)
        assert isinstance(result, Material)
        assert result.state == "deferred"

    def test_execute_sql_material_has_materialized_ref(self):
        plugin = PolarsComputeEnginePlugin()
        upstream = {"t": pl.LazyFrame({"a": [1, 2]})}
        result = plugin.execute_sql_lazy("SELECT * FROM t", upstream)
        assert result.materialized_ref is not None
        assert isinstance(result.materialized_ref, PolarsLazyMaterializedRef)

    def test_execute_sql_to_arrow_returns_correct_data(self):
        plugin = PolarsComputeEnginePlugin()
        upstream = {"data": pl.LazyFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})}
        result = plugin.execute_sql_lazy("SELECT x, y FROM data WHERE x >= 2", upstream)
        table = result.to_arrow()
        assert isinstance(table, pa.Table)
        assert table.num_rows == 2
        assert table.column("x").to_pylist() == [2, 3]

    def test_execute_sql_with_no_upstream(self):
        plugin = PolarsComputeEnginePlugin()
        result = plugin.execute_sql_lazy("SELECT 1 AS val", {})
        table = result.to_arrow()
        assert table.num_rows == 1

    def test_execute_sql_with_multiple_upstreams(self):
        plugin = PolarsComputeEnginePlugin()
        upstream = {
            "a": pl.LazyFrame({"id": [1, 2], "name": ["x", "y"]}),
            "b": pl.LazyFrame({"id": [2, 3], "score": [90, 80]}),
        }
        result = plugin.execute_sql_lazy(
            "SELECT a.id, a.name, b.score FROM a INNER JOIN b ON a.id = b.id",
            upstream,
        )
        table = result.to_arrow()
        assert table.num_rows == 1
        assert table.column("id").to_pylist() == [2]

    def test_execute_sql_invalid_table_raises(self):
        """Referencing a table not registered in SQLContext should raise RVT-501."""
        from rivet_core.errors import ExecutionError

        plugin = PolarsComputeEnginePlugin()
        with pytest.raises(ExecutionError) as exc_info:
            plugin.execute_sql_lazy("SELECT * FROM nonexistent", {})
        assert exc_info.value.error.code == "RVT-501"

    def test_execute_sql_result_is_lazy(self):
        """The Material should be backed by a LazyFrame, not eagerly collected."""
        plugin = PolarsComputeEnginePlugin()
        upstream = {"t": pl.LazyFrame({"a": [1, 2, 3]})}
        result = plugin.execute_sql_lazy("SELECT * FROM t", upstream)
        ref = result.materialized_ref
        assert isinstance(ref, PolarsLazyMaterializedRef)
        assert isinstance(ref._lazy_frame, pl.LazyFrame)

    def test_execute_sql_aggregation(self):
        plugin = PolarsComputeEnginePlugin()
        upstream = {"t": pl.LazyFrame({"grp": ["a", "a", "b"], "val": [1, 2, 3]})}
        result = plugin.execute_sql_lazy("SELECT grp, SUM(val) AS total FROM t GROUP BY grp", upstream)
        table = result.to_arrow()
        assert table.num_rows == 2
