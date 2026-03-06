"""Tests for rivet_core.interactive.differ."""

import pyarrow as pa
import pytest

from rivet_core.interactive.differ import Differ


@pytest.fixture
def differ():
    return Differ()


def make_table(rows: list[dict]) -> pa.Table:
    if not rows:
        return pa.table({"id": pa.array([], type=pa.int64()), "val": pa.array([], type=pa.int64())})
    return pa.Table.from_pylist(rows)


# --- keyed diff ---

def test_identical_tables_all_unchanged(differ):
    t = make_table([{"id": 1, "val": 10}, {"id": 2, "val": 20}])
    result = differ.diff(t, t, key_columns=["id"])
    assert result.added.num_rows == 0
    assert result.removed.num_rows == 0
    assert result.changed == []
    assert result.unchanged_count == 2


def test_added_rows(differ):
    baseline = make_table([{"id": 1, "val": 10}])
    current = make_table([{"id": 1, "val": 10}, {"id": 2, "val": 20}])
    result = differ.diff(baseline, current, key_columns=["id"])
    assert result.added.num_rows == 1
    assert result.added.column("id")[0].as_py() == 2
    assert result.removed.num_rows == 0
    assert result.unchanged_count == 1


def test_removed_rows(differ):
    baseline = make_table([{"id": 1, "val": 10}, {"id": 2, "val": 20}])
    current = make_table([{"id": 1, "val": 10}])
    result = differ.diff(baseline, current, key_columns=["id"])
    assert result.removed.num_rows == 1
    assert result.removed.column("id")[0].as_py() == 2
    assert result.added.num_rows == 0
    assert result.unchanged_count == 1


def test_changed_rows(differ):
    baseline = make_table([{"id": 1, "val": 10}, {"id": 2, "val": 20}])
    current = make_table([{"id": 1, "val": 99}, {"id": 2, "val": 20}])
    result = differ.diff(baseline, current, key_columns=["id"])
    assert len(result.changed) == 1
    assert result.changed[0].key == {"id": 1}
    assert result.changed[0].changes == {"val": (10, 99)}
    assert result.unchanged_count == 1


def test_default_key_is_first_column(differ):
    baseline = make_table([{"id": 1, "val": 10}])
    current = make_table([{"id": 2, "val": 20}])
    result = differ.diff(baseline, current)
    assert result.key_columns == ["id"]
    assert result.added.num_rows == 1
    assert result.removed.num_rows == 1


def test_categorization_completeness(differ):
    """added + removed + changed + unchanged == distinct keys across both tables."""
    baseline = make_table([{"id": 1, "val": 10}, {"id": 2, "val": 20}, {"id": 3, "val": 30}])
    current = make_table([{"id": 2, "val": 99}, {"id": 3, "val": 30}, {"id": 4, "val": 40}])
    result = differ.diff(baseline, current, key_columns=["id"])
    total = result.added.num_rows + result.removed.num_rows + len(result.changed) + result.unchanged_count
    all_keys = {1, 2, 3, 4}
    assert total == len(all_keys)


# --- empty tables ---

def test_both_empty(differ):
    t = make_table([])
    result = differ.diff(t, t)
    assert result.added.num_rows == 0
    assert result.removed.num_rows == 0
    assert result.changed == []
    assert result.unchanged_count == 0


def test_baseline_empty(differ):
    baseline = make_table([])
    current = make_table([{"id": 1, "val": 10}])
    result = differ.diff(baseline, current, key_columns=["id"])
    assert result.added.num_rows == 1
    assert result.removed.num_rows == 0


def test_current_empty(differ):
    baseline = make_table([{"id": 1, "val": 10}])
    current = make_table([])
    result = differ.diff(baseline, current, key_columns=["id"])
    assert result.removed.num_rows == 1
    assert result.added.num_rows == 0


# --- positional diff (no columns) ---

def test_positional_diff_no_key_columns(differ):
    pa.schema([("val", pa.int64())])
    baseline = pa.table({"val": [1, 2, 3]})
    current = pa.table({"val": [1, 99, 3]})
    result = differ.diff(baseline, current, key_columns=[])
    assert result.key_columns == []
    assert len(result.changed) == 1
    assert result.changed[0].changes == {"val": (2, 99)}
    assert result.unchanged_count == 2


def test_positional_diff_current_longer(differ):
    baseline = pa.table({"val": [1, 2]})
    current = pa.table({"val": [1, 2, 3]})
    result = differ.diff(baseline, current, key_columns=[])
    assert result.added.num_rows == 1
    assert result.unchanged_count == 2


def test_positional_diff_baseline_longer(differ):
    baseline = pa.table({"val": [1, 2, 3]})
    current = pa.table({"val": [1, 2]})
    result = differ.diff(baseline, current, key_columns=[])
    assert result.removed.num_rows == 1
    assert result.unchanged_count == 2


# --- key_columns in result ---

def test_key_columns_stored_in_result(differ):
    t = make_table([{"id": 1, "val": 10}])
    result = differ.diff(t, t, key_columns=["id"])
    assert result.key_columns == ["id"]
