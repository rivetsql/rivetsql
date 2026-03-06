"""Unit tests for rivet_core.testing.comparison."""

from __future__ import annotations

from datetime import UTC

import pyarrow as pa

from rivet_core.testing.comparison import compare_tables


def _table(cols: dict[str, list]) -> pa.Table:
    return pa.table(cols)


# ── Exact mode ──────────────────────────────────────────────────────────


class TestExactComparison:
    def test_identical_tables_pass(self):
        t = _table({"a": [1, 2], "b": ["x", "y"]})
        r = compare_tables(t, t, mode="exact")
        assert r.passed

    def test_different_row_order_fails(self):
        a = _table({"a": [1, 2]})
        e = _table({"a": [2, 1]})
        r = compare_tables(a, e, mode="exact")
        assert not r.passed

    def test_different_values_fail(self):
        a = _table({"a": [1, 3]})
        e = _table({"a": [1, 2]})
        r = compare_tables(a, e, mode="exact")
        assert not r.passed
        assert r.diff is not None

    def test_row_count_mismatch(self):
        a = _table({"a": [1, 2, 3]})
        e = _table({"a": [1, 2]})
        r = compare_tables(a, e, mode="exact")
        assert not r.passed
        assert "Row count" in r.message


# ── Unordered mode ──────────────────────────────────────────────────────


class TestUnorderedComparison:
    def test_permuted_rows_pass(self):
        a = _table({"a": [2, 1], "b": ["y", "x"]})
        e = _table({"a": [1, 2], "b": ["x", "y"]})
        r = compare_tables(a, e, mode="unordered")
        assert r.passed

    def test_different_values_fail(self):
        a = _table({"a": [1, 3]})
        e = _table({"a": [1, 2]})
        r = compare_tables(a, e, mode="unordered")
        assert not r.passed

    def test_row_count_mismatch(self):
        a = _table({"a": [1]})
        e = _table({"a": [1, 2]})
        r = compare_tables(a, e, mode="unordered")
        assert not r.passed


# ── Schema-only mode ────────────────────────────────────────────────────


class TestSchemaOnlyComparison:
    def test_same_schema_different_data_passes(self):
        a = _table({"a": [1], "b": ["x"]})
        e = _table({"a": [99], "b": ["z"]})
        r = compare_tables(a, e, mode="schema_only")
        assert r.passed

    def test_different_schema_fails(self):
        a = _table({"a": [1]})
        e = _table({"a": ["x"]})
        r = compare_tables(a, e, mode="schema_only")
        assert not r.passed
        assert "RVT-904" in r.message

    def test_missing_column_fails(self):
        a = _table({"a": [1]})
        e = _table({"a": [1], "b": [2]})
        r = compare_tables(a, e, mode="schema_only")
        assert not r.passed
        assert "RVT-904" in r.message


# ── Column matching ─────────────────────────────────────────────────────


class TestColumnMatching:
    def test_reordered_columns_pass(self):
        a = _table({"b": ["x", "y"], "a": [1, 2]})
        e = _table({"a": [1, 2], "b": ["x", "y"]})
        r = compare_tables(a, e, mode="exact")
        assert r.passed

    def test_extra_columns_ignored(self):
        a = _table({"a": [1], "b": [2], "extra": [99]})
        e = _table({"a": [1], "b": [2]})
        r = compare_tables(a, e, mode="exact")
        assert r.passed

    def test_missing_columns_fail(self):
        a = _table({"a": [1]})
        e = _table({"a": [1], "b": [2]})
        r = compare_tables(a, e, mode="exact")
        assert not r.passed
        assert "RVT-904" in r.message


# ── Empty tables ────────────────────────────────────────────────────────


class TestEmptyTables:
    def test_both_empty_passes(self):
        pa.schema([("a", pa.int64())])
        a = pa.table({"a": pa.array([], type=pa.int64())})
        e = pa.table({"a": pa.array([], type=pa.int64())})
        r = compare_tables(a, e, mode="exact")
        assert r.passed

    def test_empty_actual_non_empty_expected_fails(self):
        a = pa.table({"a": pa.array([], type=pa.int64())})
        e = _table({"a": [1]})
        r = compare_tables(a, e, mode="exact")
        assert not r.passed

    def test_non_empty_actual_empty_expected_fails(self):
        a = _table({"a": [1]})
        e = pa.table({"a": pa.array([], type=pa.int64())})
        r = compare_tables(a, e, mode="exact")
        assert not r.passed


# ── Custom comparison ───────────────────────────────────────────────────


class TestCustomComparison:
    def test_custom_function_called(self, tmp_path, monkeypatch):

        # Create a temp module with a comparison function
        mod_file = tmp_path / "my_cmp.py"
        mod_file.write_text(
            "from rivet_core.testing.models import ComparisonResult\n"
            "def cmp(actual, expected):\n"
            "    return ComparisonResult(passed=True, message='custom ok')\n"
        )
        monkeypatch.syspath_prepend(str(tmp_path))

        t = _table({"a": [1]})
        r = compare_tables(t, t, mode="custom", compare_function="my_cmp.cmp")
        assert r.passed
        assert r.message == "custom ok"

    def test_non_importable_function_rvt906(self):
        t = _table({"a": [1]})
        r = compare_tables(t, t, mode="custom", compare_function="no.such.module.fn")
        assert not r.passed
        assert "RVT-906" in r.message

    def test_no_function_path_rvt906(self):
        t = _table({"a": [1]})
        r = compare_tables(t, t, mode="custom", compare_function=None)
        assert not r.passed
        assert "RVT-906" in r.message


# ── Diff output ─────────────────────────────────────────────────────────


class TestDiffOutput:
    def test_diff_contains_first_5_rows(self):
        a = _table({"a": list(range(10))})
        e = _table({"a": list(range(10, 20))})
        r = compare_tables(a, e, mode="exact")
        assert not r.passed
        assert r.diff is not None
        assert len(r.diff) == 5
        assert all("row" in d and "column" in d and "expected" in d and "actual" in d for d in r.diff)

    def test_diff_row_and_column_info(self):
        a = _table({"a": [1, 99], "b": ["x", "y"]})
        e = _table({"a": [1, 2], "b": ["x", "y"]})
        r = compare_tables(a, e, mode="exact")
        assert r.diff is not None
        assert len(r.diff) == 1
        assert r.diff[0]["row"] == 1
        assert r.diff[0]["column"] == "a"
        assert r.diff[0]["expected"] == 2
        assert r.diff[0]["actual"] == 99


# ── Null handling ───────────────────────────────────────────────────────


class TestNullHandling:
    def test_null_equals_null_default_true(self):
        a = pa.table({"a": pa.array([1, None], type=pa.int64())})
        e = pa.table({"a": pa.array([1, None], type=pa.int64())})
        r = compare_tables(a, e, mode="exact")
        assert r.passed

    def test_null_equals_null_false(self):
        a = pa.table({"a": pa.array([1, None], type=pa.int64())})
        e = pa.table({"a": pa.array([1, None], type=pa.int64())})
        r = compare_tables(a, e, mode="exact", options={"null_equals_null": False})
        assert not r.passed


# ── Float tolerance ─────────────────────────────────────────────────────


class TestFloatTolerance:
    def test_absolute_tolerance_passes(self):
        a = pa.table({"v": pa.array([1.0000001], type=pa.float64())})
        e = pa.table({"v": pa.array([1.0], type=pa.float64())})
        r = compare_tables(a, e, mode="exact", options={"tolerance": 1e-5})
        assert r.passed

    def test_absolute_tolerance_fails_when_exceeded(self):
        a = pa.table({"v": pa.array([1.1], type=pa.float64())})
        e = pa.table({"v": pa.array([1.0], type=pa.float64())})
        r = compare_tables(a, e, mode="exact", options={"tolerance": 1e-5})
        assert not r.passed

    def test_relative_tolerance_passes(self):
        a = pa.table({"v": pa.array([1.000001], type=pa.float64())})
        e = pa.table({"v": pa.array([1.0], type=pa.float64())})
        r = compare_tables(a, e, mode="exact", options={"relative_tolerance": 1e-4})
        assert r.passed

    def test_either_tolerance_satisfied_passes(self):
        # absolute fails but relative passes
        a = pa.table({"v": pa.array([1.001], type=pa.float64())})
        e = pa.table({"v": pa.array([1.0], type=pa.float64())})
        r = compare_tables(a, e, mode="exact", options={"tolerance": 1e-9, "relative_tolerance": 0.01})
        assert r.passed


# ── ignore_columns ──────────────────────────────────────────────────────


class TestIgnoreColumns:
    def test_ignored_column_difference_passes(self):
        a = _table({"a": [1], "b": [99]})
        e = _table({"a": [1], "b": [2]})
        r = compare_tables(a, e, mode="exact", options={"ignore_columns": ["b"]})
        assert r.passed

    def test_non_ignored_column_difference_fails(self):
        a = _table({"a": [1], "b": [99]})
        e = _table({"a": [2], "b": [99]})
        r = compare_tables(a, e, mode="exact", options={"ignore_columns": ["b"]})
        assert not r.passed


# ── sort_by for unordered ───────────────────────────────────────────────


class TestSortBy:
    def test_sort_by_subset_of_columns(self):
        a = _table({"a": [2, 1], "b": ["y", "x"]})
        e = _table({"a": [1, 2], "b": ["x", "y"]})
        r = compare_tables(a, e, mode="unordered", options={"sort_by": ["a"]})
        assert r.passed

    def test_sort_by_with_different_values_fails(self):
        a = _table({"a": [2, 1], "b": ["y", "x"]})
        e = _table({"a": [1, 3], "b": ["x", "y"]})
        r = compare_tables(a, e, mode="unordered", options={"sort_by": ["a"]})
        assert not r.passed


# ── timestamp_tolerance ─────────────────────────────────────────────────


class TestTimestampTolerance:
    def test_timestamps_within_tolerance_pass(self):
        from datetime import datetime

        t1 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        t2 = datetime(2024, 1, 1, 0, 0, 0, 500000, tzinfo=UTC)  # 0.5s later
        a = pa.table({"ts": pa.array([t1], type=pa.timestamp("us", tz="UTC"))})
        e = pa.table({"ts": pa.array([t2], type=pa.timestamp("us", tz="UTC"))})
        r = compare_tables(a, e, mode="exact", options={"timestamp_tolerance": "1s"})
        assert r.passed

    def test_timestamps_outside_tolerance_fail(self):
        from datetime import datetime

        t1 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        t2 = datetime(2024, 1, 1, 0, 0, 5, tzinfo=UTC)  # 5s later
        a = pa.table({"ts": pa.array([t1], type=pa.timestamp("us", tz="UTC"))})
        e = pa.table({"ts": pa.array([t2], type=pa.timestamp("us", tz="UTC"))})
        r = compare_tables(a, e, mode="exact", options={"timestamp_tolerance": "1s"})
        assert not r.passed
