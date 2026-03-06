"""Property-based tests for Differ categorization completeness.

Property 7: Diff categorization completeness.

Properties verified:
- For any two Arrow tables, every row is accounted for exactly once:
  added + removed + changed + unchanged == distinct keys across both tables.
- Positional diff: added + removed + changed + unchanged == max(len(baseline), len(current)).
- No row is double-counted across categories.

Validates: Requirements 13.4, 13.6
"""

from __future__ import annotations

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.interactive.differ import Differ

# ── Strategies ────────────────────────────────────────────────────────────────

_int_val = st.integers(min_value=0, max_value=1000)
_key_val = st.integers(min_value=1, max_value=50)  # small range to force overlaps


def _rows_strategy(min_rows: int = 0, max_rows: int = 20) -> st.SearchStrategy[list[dict]]:
    """Generate a list of rows with unique integer 'id' keys and an integer 'val'."""
    return st.lists(
        st.fixed_dictionaries({"id": _key_val, "val": _int_val}),
        min_size=min_rows,
        max_size=max_rows,
    ).map(lambda rows: list({r["id"]: r for r in rows}.values()))  # deduplicate by id


def _make_table(rows: list[dict]) -> pa.Table:
    if not rows:
        return pa.table({"id": pa.array([], type=pa.int64()), "val": pa.array([], type=pa.int64())})
    return pa.Table.from_pylist(rows)


# ── Property 7a: keyed diff — every key accounted for exactly once ────────────


@given(baseline_rows=_rows_strategy(), current_rows=_rows_strategy())
@settings(max_examples=200)
def test_keyed_diff_categorization_completeness(
    baseline_rows: list[dict], current_rows: list[dict]
) -> None:
    """added + removed + changed + unchanged == distinct keys across both tables (Req 13.4)."""
    differ = Differ()
    baseline = _make_table(baseline_rows)
    current = _make_table(current_rows)

    result = differ.diff(baseline, current, key_columns=["id"])

    total = (
        result.added.num_rows
        + result.removed.num_rows
        + len(result.changed)
        + result.unchanged_count
    )

    baseline_keys = {r["id"] for r in baseline_rows}
    current_keys = {r["id"] for r in current_rows}
    distinct_keys = baseline_keys | current_keys

    assert total == len(distinct_keys), (
        f"total={total} != distinct_keys={len(distinct_keys)}: "
        f"added={result.added.num_rows}, removed={result.removed.num_rows}, "
        f"changed={len(result.changed)}, unchanged={result.unchanged_count}"
    )


# ── Property 7b: no key appears in more than one category ────────────────────


@given(baseline_rows=_rows_strategy(), current_rows=_rows_strategy())
@settings(max_examples=200)
def test_keyed_diff_no_double_counting(
    baseline_rows: list[dict], current_rows: list[dict]
) -> None:
    """No key appears in more than one diff category (Req 13.4)."""
    differ = Differ()
    baseline = _make_table(baseline_rows)
    current = _make_table(current_rows)

    result = differ.diff(baseline, current, key_columns=["id"])

    added_keys = set(result.added.column("id").to_pylist())
    removed_keys = set(result.removed.column("id").to_pylist())
    changed_keys = {cr.key["id"] for cr in result.changed}

    # No overlap between categories
    assert added_keys.isdisjoint(removed_keys), "Keys appear in both added and removed"
    assert added_keys.isdisjoint(changed_keys), "Keys appear in both added and changed"
    assert removed_keys.isdisjoint(changed_keys), "Keys appear in both removed and changed"


# ── Property 7c: positional diff — every position accounted for ───────────────


@given(
    baseline_vals=st.lists(_int_val, min_size=0, max_size=20),
    current_vals=st.lists(_int_val, min_size=0, max_size=20),
)
@settings(max_examples=200)
def test_positional_diff_categorization_completeness(
    baseline_vals: list[int], current_vals: list[int]
) -> None:
    """Positional: added + removed + changed + unchanged == max(len(baseline), len(current)) (Req 13.6)."""
    differ = Differ()
    baseline = pa.table({"val": pa.array(baseline_vals, type=pa.int64())})
    current = pa.table({"val": pa.array(current_vals, type=pa.int64())})

    result = differ.diff(baseline, current, key_columns=[])

    total = (
        result.added.num_rows
        + result.removed.num_rows
        + len(result.changed)
        + result.unchanged_count
    )

    expected = max(len(baseline_vals), len(current_vals))
    assert total == expected, (
        f"total={total} != max_rows={expected}: "
        f"added={result.added.num_rows}, removed={result.removed.num_rows}, "
        f"changed={len(result.changed)}, unchanged={result.unchanged_count}"
    )


# ── Property 7d: default key (first column) behaves like explicit key ─────────


@given(baseline_rows=_rows_strategy(min_rows=1), current_rows=_rows_strategy(min_rows=1))
@settings(max_examples=100)
def test_default_key_equals_explicit_first_column(
    baseline_rows: list[dict], current_rows: list[dict]
) -> None:
    """diff() with key_columns=None uses first column, same as key_columns=['id'] (Req 13.6)."""
    differ = Differ()
    baseline = _make_table(baseline_rows)
    current = _make_table(current_rows)

    result_default = differ.diff(baseline, current)
    result_explicit = differ.diff(baseline, current, key_columns=["id"])

    assert result_default.key_columns == result_explicit.key_columns
    assert result_default.added.num_rows == result_explicit.added.num_rows
    assert result_default.removed.num_rows == result_explicit.removed.num_rows
    assert len(result_default.changed) == len(result_explicit.changed)
    assert result_default.unchanged_count == result_explicit.unchanged_count
