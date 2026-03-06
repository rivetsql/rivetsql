"""Property-based tests for rivet_core.testing.comparison.

Properties verified:
- Reflexivity: exact comparison of a table with itself always passes.
- Permutation-invariance: unordered comparison is invariant to row permutations.
- Column-order independence: comparison result is unaffected by column order.
- Schema-only ignores row data: schema_only passes for any two tables with the same schema.
- Tolerance symmetry: compare(a, b) == compare(b, a) under float tolerance.
- Ignore-columns monotonicity: adding more ignored columns cannot turn a pass into a fail.

Requirements: 4.1, 4.2, 4.3, 4.5, 5.1, 5.4
"""

from __future__ import annotations

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.testing.comparison import compare_tables

# ── Strategies ────────────────────────────────────────────────────────────────

_int_val = st.integers(min_value=-1000, max_value=1000)
_float_val = st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False)
_str_val = st.text(max_size=10)
_col_name = st.text(alphabet=st.characters(whitelist_categories=("Ll",)), min_size=1, max_size=6)


def _int_table_strategy(
    min_rows: int = 0, max_rows: int = 20, min_cols: int = 1, max_cols: int = 4
) -> st.SearchStrategy[pa.Table]:
    """Generate Arrow tables with integer columns."""
    col_names = st.lists(
        _col_name, min_size=min_cols, max_size=max_cols, unique=True
    )

    def build(names: list[str]) -> st.SearchStrategy[pa.Table]:
        {
            name: st.lists(_int_val, min_size=0, max_size=0)  # placeholder
            for name in names
        }
        # Build rows as lists per column
        n_rows = st.integers(min_value=min_rows, max_value=max_rows)

        def make_table(n: int) -> pa.Table:
            arrays = {name: pa.array([0] * n, type=pa.int64()) for name in names}
            return pa.table(arrays)

        return n_rows.map(make_table)

    return col_names.flatmap(build)


def _table_with_data(
    min_rows: int = 0, max_rows: int = 15
) -> st.SearchStrategy[pa.Table]:
    """Generate tables with 1-3 integer columns and actual data."""
    @st.composite
    def _build(draw: st.DrawFn) -> pa.Table:
        n_cols = draw(st.integers(min_value=1, max_value=3))
        names = draw(
            st.lists(_col_name, min_size=n_cols, max_size=n_cols, unique=True)
        )
        n_rows = draw(st.integers(min_value=min_rows, max_value=max_rows))
        cols = {
            name: pa.array(
                draw(st.lists(_int_val, min_size=n_rows, max_size=n_rows)),
                type=pa.int64(),
            )
            for name in names
        }
        return pa.table(cols)

    return _build()


def _float_table_pair(
    min_rows: int = 1, max_rows: int = 10
) -> st.SearchStrategy[tuple[pa.Table, pa.Table]]:
    """Generate a pair of float tables with the same schema."""
    @st.composite
    def _build(draw: st.DrawFn) -> tuple[pa.Table, pa.Table]:
        n_cols = draw(st.integers(min_value=1, max_value=3))
        names = draw(
            st.lists(_col_name, min_size=n_cols, max_size=n_cols, unique=True)
        )
        n_rows = draw(st.integers(min_value=min_rows, max_value=max_rows))
        cols_a = {
            name: pa.array(
                draw(st.lists(_float_val, min_size=n_rows, max_size=n_rows)),
                type=pa.float64(),
            )
            for name in names
        }
        cols_b = {
            name: pa.array(
                draw(st.lists(_float_val, min_size=n_rows, max_size=n_rows)),
                type=pa.float64(),
            )
            for name in names
        }
        return pa.table(cols_a), pa.table(cols_b)

    return _build()


# ── Property 1: Reflexivity ───────────────────────────────────────────────────


@given(table=_table_with_data(min_rows=0, max_rows=20))
@settings(max_examples=200)
def test_exact_reflexivity(table: pa.Table) -> None:
    """Exact comparison of any table with itself always passes (Req 4.1)."""
    result = compare_tables(table, table, mode="exact")
    assert result.passed, f"Reflexivity failed: {result.message}"


# ── Property 2: Permutation-invariance ───────────────────────────────────────


@given(table=_table_with_data(min_rows=1, max_rows=20))
@settings(max_examples=200)
def test_unordered_permutation_invariant(table: pa.Table) -> None:
    """Unordered comparison passes for any row permutation of the same table (Req 4.2)."""
    import random

    n = table.num_rows
    indices = list(range(n))
    random.shuffle(indices)
    shuffled = table.take(indices)

    result = compare_tables(shuffled, table, mode="unordered")
    assert result.passed, (
        f"Permutation-invariance failed for permutation {indices}: {result.message}"
    )


@given(
    table=_table_with_data(min_rows=1, max_rows=20),
    seed=st.integers(min_value=0, max_value=2**31 - 1),
)
@settings(max_examples=200)
def test_unordered_permutation_invariant_seeded(table: pa.Table, seed: int) -> None:
    """Unordered comparison passes for any seeded row permutation (Req 4.2)."""
    import random

    rng = random.Random(seed)
    n = table.num_rows
    indices = list(range(n))
    rng.shuffle(indices)
    shuffled = table.take(indices)

    result = compare_tables(shuffled, table, mode="unordered")
    assert result.passed, (
        f"Permutation-invariance failed (seed={seed}): {result.message}"
    )


# ── Property 3: Column-order independence ─────────────────────────────────────


@given(table=_table_with_data(min_rows=0, max_rows=20))
@settings(max_examples=200)
def test_column_order_independence(table: pa.Table) -> None:
    """Comparison result is unaffected by column order (Req 4.5)."""
    import random

    names = table.column_names
    if len(names) < 2:
        # Single column — trivially order-independent; just verify it passes
        result = compare_tables(table, table, mode="exact")
        assert result.passed
        return

    shuffled_names = names[:]
    random.shuffle(shuffled_names)
    reordered = table.select(shuffled_names)

    result = compare_tables(reordered, table, mode="exact")
    assert result.passed, (
        f"Column-order independence failed (order={shuffled_names}): {result.message}"
    )


@given(
    table=_table_with_data(min_rows=0, max_rows=20),
    seed=st.integers(min_value=0, max_value=2**31 - 1),
)
@settings(max_examples=200)
def test_column_order_independence_seeded(table: pa.Table, seed: int) -> None:
    """Column-order independence with seeded shuffle (Req 4.5)."""
    import random

    names = table.column_names
    rng = random.Random(seed)
    shuffled_names = names[:]
    rng.shuffle(shuffled_names)
    reordered = table.select(shuffled_names)

    result = compare_tables(reordered, table, mode="exact")
    assert result.passed, (
        f"Column-order independence failed (seed={seed}, order={shuffled_names}): {result.message}"
    )


# ── Property 4: Schema-only ignores row data ──────────────────────────────────


@given(
    table_a=_table_with_data(min_rows=0, max_rows=20),
    n_rows_b=st.integers(min_value=0, max_value=20),
)
@settings(max_examples=200)
def test_schema_only_ignores_row_data(table_a: pa.Table, n_rows_b: int) -> None:
    """Schema-only comparison passes for any two tables with the same schema (Req 4.3)."""
    # Build table_b with same schema but different (arbitrary) row count
    cols_b = {
        name: pa.array([0] * n_rows_b, type=table_a.schema.field(name).type)
        for name in table_a.column_names
    }
    table_b = pa.table(cols_b)

    result = compare_tables(table_a, table_b, mode="schema_only")
    assert result.passed, (
        f"Schema-only failed despite identical schemas: {result.message}"
    )


# ── Property 5: Tolerance symmetry ───────────────────────────────────────────


@given(pair=_float_table_pair(min_rows=1, max_rows=10))
@settings(max_examples=200)
def test_tolerance_symmetry(pair: tuple[pa.Table, pa.Table]) -> None:
    """compare(a, b) == compare(b, a) under float tolerance (Req 5.1, 5.2)."""
    table_a, table_b = pair
    options = {"tolerance": 1e-3, "relative_tolerance": 1e-3}

    r_ab = compare_tables(table_a, table_b, mode="exact", options=options)
    r_ba = compare_tables(table_b, table_a, mode="exact", options=options)

    assert r_ab.passed == r_ba.passed, (
        f"Tolerance symmetry violated: compare(a,b).passed={r_ab.passed}, "
        f"compare(b,a).passed={r_ba.passed}"
    )


# ── Property 6: Ignore-columns monotonicity ───────────────────────────────────


@given(
    table_a=_table_with_data(min_rows=0, max_rows=20),
    table_b=_table_with_data(min_rows=0, max_rows=20),
)
@settings(max_examples=200)
def test_ignore_columns_monotonicity(table_a: pa.Table, table_b: pa.Table) -> None:
    """Adding more columns to ignore_columns cannot turn a pass into a fail (Req 5.4).

    If compare(a, b, ignore_columns=S) passes, then for any S' ⊇ S,
    compare(a, b, ignore_columns=S') also passes.
    """
    # Only meaningful when both tables share at least one column name
    shared = [c for c in table_a.column_names if c in table_b.column_names]
    if not shared:
        return

    # Start with no ignored columns
    base_result = compare_tables(table_a, table_b, mode="exact", options={"ignore_columns": []})
    if not base_result.passed:
        # Base already fails — try ignoring all shared columns
        compare_tables(
            table_a, table_b, mode="exact", options={"ignore_columns": shared}
        )
        # Monotonicity: if ignoring all shared cols passes, that's fine.
        # We just verify: if ignoring a subset passes, ignoring a superset also passes.
        # Since base fails, we can't assert anything about monotonicity from base.
        return

    # Base passes with empty ignore set. Now add columns one by one and verify still passes.
    for i in range(len(shared)):
        ignore_set = shared[: i + 1]
        result = compare_tables(
            table_a, table_b, mode="exact", options={"ignore_columns": ignore_set}
        )
        assert result.passed, (
            f"Monotonicity violated: passed with ignore_columns=[], "
            f"failed with ignore_columns={ignore_set}: {result.message}"
        )


@given(
    table=_table_with_data(min_rows=1, max_rows=20),
    n_extra=st.integers(min_value=1, max_value=5),
)
@settings(max_examples=200)
def test_ignore_columns_monotonicity_self_comparison(
    table: pa.Table, n_extra: int
) -> None:
    """Self-comparison always passes; adding ignore_columns keeps it passing (Req 5.4)."""
    names = table.column_names
    # ignore progressively more columns (up to all of them)
    for i in range(min(n_extra, len(names))):
        ignore_set = names[: i + 1]
        result = compare_tables(
            table, table, mode="exact", options={"ignore_columns": ignore_set}
        )
        assert result.passed, (
            f"Self-comparison with ignore_columns={ignore_set} failed: {result.message}"
        )
