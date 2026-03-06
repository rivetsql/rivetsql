"""Tests for rivet_core.interactive.profiler."""

from datetime import date, datetime

import pyarrow as pa
import pytest

from rivet_core.interactive.profiler import Profiler


@pytest.fixture
def profiler() -> Profiler:
    return Profiler()


class TestProfilerNumeric:
    def test_numeric_stats(self, profiler: Profiler) -> None:
        table = pa.table({"x": [1, 2, 3, 4, None]})
        result = profiler.profile(table)
        assert result.row_count == 5
        assert result.column_count == 1
        col = result.columns[0]
        assert col.name == "x"
        assert col.null_count == 1
        assert col.null_pct == pytest.approx(20.0)
        assert col.distinct_count == 5  # includes null
        assert col.min == 1
        assert col.max == 4
        assert col.mean == pytest.approx(2.5)
        assert col.median is not None
        assert col.stddev is not None
        assert col.histogram is not None
        assert len(col.histogram) == 8
        assert sum(col.histogram) == 4  # non-null count

    def test_float_column(self, profiler: Profiler) -> None:
        table = pa.table({"f": pa.array([1.5, 2.5, 3.5], type=pa.float64())})
        result = profiler.profile(table)
        col = result.columns[0]
        assert col.min == pytest.approx(1.5)
        assert col.max == pytest.approx(3.5)
        assert col.mean == pytest.approx(2.5)


class TestProfilerString:
    def test_string_stats(self, profiler: Profiler) -> None:
        table = pa.table({"s": ["a", "bb", "ccc", None, "a"]})
        result = profiler.profile(table)
        col = result.columns[0]
        assert col.name == "s"
        assert col.null_count == 1
        assert col.null_pct == pytest.approx(20.0)
        # min/max/mean are string lengths
        assert col.min == 1  # min length
        assert col.max == 3  # max length
        assert col.mean == pytest.approx(1.75)  # avg length of non-null
        assert col.top_values is not None
        # "a" appears twice, should be first
        assert col.top_values[0] == ("a", 2)
        assert col.histogram is None


class TestProfilerBoolean:
    def test_boolean_stats(self, profiler: Profiler) -> None:
        table = pa.table({"b": [True, True, False, None]})
        result = profiler.profile(table)
        col = result.columns[0]
        assert col.name == "b"
        assert col.null_count == 1
        assert col.null_pct == pytest.approx(25.0)
        assert col.top_values is not None
        true_count = dict(col.top_values)[True]
        false_count = dict(col.top_values)[False]
        assert true_count == 2
        assert false_count == 1
        # null% + true% + false% == 100%
        total = col.null_pct + (true_count / 4 * 100) + (false_count / 4 * 100)
        assert total == pytest.approx(100.0)


class TestProfilerTemporal:
    def test_date_stats(self, profiler: Profiler) -> None:
        dates = [date(2024, 1, 1), date(2024, 6, 15), date(2024, 12, 31), None]
        table = pa.table({"d": pa.array(dates, type=pa.date32())})
        result = profiler.profile(table)
        col = result.columns[0]
        assert col.name == "d"
        assert col.null_count == 1
        assert col.min == date(2024, 1, 1)
        assert col.max == date(2024, 12, 31)
        assert col.histogram is not None
        assert len(col.histogram) == 8
        assert sum(col.histogram) == 3

    def test_timestamp_stats(self, profiler: Profiler) -> None:
        ts = [datetime(2024, 1, 1), datetime(2024, 6, 1), datetime(2024, 12, 1)]
        table = pa.table({"t": pa.array(ts, type=pa.timestamp("us"))})
        result = profiler.profile(table)
        col = result.columns[0]
        assert col.histogram is not None
        assert sum(col.histogram) == 3


class TestProfilerEdgeCases:
    def test_empty_table(self, profiler: Profiler) -> None:
        table = pa.table({"x": pa.array([], type=pa.int64()), "s": pa.array([], type=pa.string())})
        result = profiler.profile(table)
        assert result.row_count == 0
        assert result.column_count == 2
        assert len(result.columns) == 2
        assert result.columns[0].null_pct == 0.0
        assert result.columns[1].null_pct == 0.0

    def test_all_nulls_numeric(self, profiler: Profiler) -> None:
        table = pa.table({"x": pa.array([None, None, None], type=pa.int64())})
        result = profiler.profile(table)
        col = result.columns[0]
        assert col.null_count == 3
        assert col.null_pct == pytest.approx(100.0)
        assert col.min is None
        assert col.max is None

    def test_single_value_numeric(self, profiler: Profiler) -> None:
        table = pa.table({"x": [5, 5, 5]})
        result = profiler.profile(table)
        col = result.columns[0]
        assert col.min == 5
        assert col.max == 5
        assert col.histogram is not None
        # All values in first bin when min == max
        assert col.histogram[0] == 3

    def test_multi_column_table(self, profiler: Profiler) -> None:
        table = pa.table({
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "active": [True, False, True],
            "created": pa.array(
                [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)],
                type=pa.date32(),
            ),
        })
        result = profiler.profile(table)
        assert result.column_count == 4
        assert len(result.columns) == 4
        assert result.columns[0].name == "id"
        assert result.columns[1].name == "name"
        assert result.columns[2].name == "active"
        assert result.columns[3].name == "created"

    def test_unsupported_type_fallback(self, profiler: Profiler) -> None:
        # Binary type as unsupported
        table = pa.table({"b": pa.array([b"abc", b"def"], type=pa.binary())})
        result = profiler.profile(table)
        col = result.columns[0]
        assert col.name == "b"
        assert col.null_count == 0
        assert col.min is None
        assert col.max is None
