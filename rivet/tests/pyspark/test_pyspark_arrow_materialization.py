"""Tests for task 32.8: Arrow materialization via DataFrame.toArrow() (Spark >= 3.3) or toPandas() fallback."""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow

from rivet_pyspark.engine import SparkDataFrameMaterializedRef


class TestToArrowPreferred:
    """When DataFrame has toArrow(), it is used directly."""

    def test_to_arrow_calls_toArrow_when_available(self):
        expected = pyarrow.table({"x": [1, 2, 3]})
        df = MagicMock()
        df.toArrow.return_value = expected

        ref = SparkDataFrameMaterializedRef(df)
        result = ref.to_arrow()

        df.toArrow.assert_called_once()
        assert result is expected

    def test_to_arrow_does_not_call_toPandas_when_toArrow_available(self):
        df = MagicMock()
        df.toArrow.return_value = pyarrow.table({"a": [1]})

        ref = SparkDataFrameMaterializedRef(df)
        ref.to_arrow()

        df.toPandas.assert_not_called()

    def test_to_arrow_returns_pyarrow_table(self):
        expected = pyarrow.table({"col": ["a", "b"]})
        df = MagicMock()
        df.toArrow.return_value = expected

        ref = SparkDataFrameMaterializedRef(df)
        result = ref.to_arrow()

        assert isinstance(result, pyarrow.Table)


class TestToPandasFallback:
    """When DataFrame lacks toArrow(), toPandas() is used as fallback."""

    def test_fallback_to_toPandas_when_toArrow_absent(self):
        arrow_table = pyarrow.table({"x": [10, 20]})
        pandas_mock = MagicMock()

        df = MagicMock(spec=[])  # no toArrow attribute
        df.toPandas = MagicMock(return_value=pandas_mock)

        from unittest.mock import patch
        with patch("rivet_pyspark.engine._pandas_df_to_arrow", return_value=arrow_table) as mock_convert:
            ref = SparkDataFrameMaterializedRef(df)
            result = ref.to_arrow()

        df.toPandas.assert_called_once()
        mock_convert.assert_called_once_with(pandas_mock)
        assert isinstance(result, pyarrow.Table)

    def test_fallback_result_matches_pandas_data(self):
        arrow_table = pyarrow.table({"a": [1, 2], "b": ["x", "y"]})
        pandas_mock = MagicMock()

        df = MagicMock(spec=[])
        df.toPandas = MagicMock(return_value=pandas_mock)

        from unittest.mock import patch
        with patch("rivet_pyspark.engine._pandas_df_to_arrow", return_value=arrow_table):
            ref = SparkDataFrameMaterializedRef(df)
            result = ref.to_arrow()

        assert result.column_names == ["a", "b"]
        assert result.num_rows == 2


class TestMaterializedRefContract:
    """SparkDataFrameMaterializedRef satisfies the MaterializedRef contract."""

    def test_storage_type_is_spark_dataframe(self):
        df = MagicMock()
        ref = SparkDataFrameMaterializedRef(df)
        assert ref.storage_type == "spark_dataframe"

    def test_size_bytes_is_none(self):
        df = MagicMock()
        ref = SparkDataFrameMaterializedRef(df)
        assert ref.size_bytes is None

    def test_schema_returns_schema_object(self):
        df = MagicMock()
        df.schema.fields = []
        ref = SparkDataFrameMaterializedRef(df)
        from rivet_core.models import Schema
        assert isinstance(ref.schema, Schema)

    def test_row_count_delegates_to_df_count(self):
        df = MagicMock()
        df.count.return_value = 42
        ref = SparkDataFrameMaterializedRef(df)
        assert ref.row_count == 42
