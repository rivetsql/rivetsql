"""Property tests for Check SQL generation and classification.

Property 5: Residual check classification — SQL-translatable vs residual types.
Property 4: Check SQL round-trip correctness — generate SQL, execute on DuckDB, compare to Arrow path.
"""

from __future__ import annotations

import duckdb
import pyarrow
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.checks import CompiledCheck
from rivet_core.executor import (
    _SQL_TRANSLATABLE_CHECKS,
    _execute_check,
    _generate_check_sql,
    _interpret_check_sql_result,
    _is_sql_translatable,
)

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_SQL_TYPES = st.sampled_from(sorted(_SQL_TRANSLATABLE_CHECKS))
_RESIDUAL_TYPES = st.sampled_from(["custom", "schema", "freshness", "relationship"])
_COLUMN_NAMES = st.sampled_from(["a", "b", "c", "x", "y"])


def _make_check(type: str, config: dict | None = None) -> CompiledCheck:
    return CompiledCheck(type=type, severity="error", config=config or {}, phase="assertion")


# ---------------------------------------------------------------------------
# Property 5: Residual check classification
# ---------------------------------------------------------------------------


@given(check_type=_SQL_TYPES)
@settings(max_examples=100)
def test_sql_translatable_types_classified_correctly(check_type: str) -> None:
    """Every SQL-translatable type returns True from _is_sql_translatable."""
    check = _make_check(
        check_type,
        {"column": "a", "columns": ["a"], "values": ["x"], "expression": "a > 0", "min": 0},
    )
    assert _is_sql_translatable(check) is True


@given(check_type=_RESIDUAL_TYPES)
@settings(max_examples=100)
def test_residual_types_classified_correctly(check_type: str) -> None:
    """Every residual type returns False from _is_sql_translatable."""
    check = _make_check(check_type)
    assert _is_sql_translatable(check) is False


# ---------------------------------------------------------------------------
# Property 4: Check SQL round-trip correctness
# ---------------------------------------------------------------------------

# Strategy for nullable integer columns with controlled data.
# We require at least one non-null value so the Arrow column has int64 type
# (an all-None list produces a null-typed column that pyarrow can't count_distinct on).
_int_values = st.integers(min_value=-100, max_value=100)
_nullable_int = st.one_of(st.none(), _int_values)
_row_data = st.lists(_nullable_int, min_size=1, max_size=50).filter(
    lambda xs: any(x is not None for x in xs)
)


def _interpret_result(result_table: pyarrow.Table) -> int:
    """Extract the result_value from a single-row check SQL result."""
    return result_table.column("result_value")[0].as_py()


@given(data=_row_data)
@settings(max_examples=100)
def test_not_null_sql_roundtrip(data: list[int | None]) -> None:
    """not_null SQL produces same pass/fail as Arrow-based check."""
    table = pyarrow.table({"col": data})
    check = _make_check("not_null", {"column": "col"})

    arrow_result = _execute_check(check, table)
    sqls = _generate_check_sql(check, "t")

    con = duckdb.connect(":memory:")
    con.register("t", table)
    result = con.execute(sqls[0]).fetch_arrow_table()
    null_count = _interpret_result(result)

    assert (null_count == 0) == arrow_result.passed


@given(data=_row_data)
@settings(max_examples=100)
def test_unique_sql_roundtrip(data: list[int | None]) -> None:
    """unique SQL produces same pass/fail as Arrow-based check."""
    table = pyarrow.table({"col": data})
    check = _make_check("unique", {"column": "col"})

    arrow_result = _execute_check(check, table)
    sqls = _generate_check_sql(check, "t")

    con = duckdb.connect(":memory:")
    con.register("t", table)
    result = con.execute(sqls[0]).fetch_arrow_table()
    dup_count = _interpret_result(result)

    assert (dup_count == 0) == arrow_result.passed


@given(
    data=_row_data,
    min_count=st.integers(min_value=0, max_value=60),
    max_count=st.one_of(st.none(), st.integers(min_value=0, max_value=60)),
)
@settings(max_examples=100)
def test_row_count_sql_roundtrip(
    data: list[int | None], min_count: int, max_count: int | None
) -> None:
    """row_count SQL produces same pass/fail as Arrow-based check."""
    table = pyarrow.table({"col": data})
    cfg: dict = {"min": min_count}
    if max_count is not None:
        cfg["max"] = max_count
    check = _make_check("row_count", cfg)

    arrow_result = _execute_check(check, table)
    sqls = _generate_check_sql(check, "t")

    con = duckdb.connect(":memory:")
    con.register("t", table)
    result = con.execute(sqls[0]).fetch_arrow_table()
    actual_count = _interpret_result(result)

    passed = actual_count >= min_count and (max_count is None or actual_count <= max_count)
    assert passed == arrow_result.passed


@given(
    data=st.lists(
        st.one_of(st.none(), st.sampled_from(["a", "b", "c", "d"])), min_size=0, max_size=50
    ),
    accepted=st.lists(st.sampled_from(["a", "b", "c", "d"]), min_size=1, max_size=4, unique=True),
)
@settings(max_examples=100)
def test_accepted_values_sql_roundtrip(data: list[str | None], accepted: list[str]) -> None:
    """accepted_values SQL produces same pass/fail as Arrow-based check."""
    table = pyarrow.table({"col": data})
    check = _make_check("accepted_values", {"column": "col", "values": accepted})

    arrow_result = _execute_check(check, table)
    sqls = _generate_check_sql(check, "t")

    con = duckdb.connect(":memory:")
    con.register("t", table)
    result = con.execute(sqls[0]).fetch_arrow_table()
    invalid_count = _interpret_result(result)

    assert (invalid_count == 0) == arrow_result.passed


# Non-null integer data for expression tests — avoids the known NULL-handling
# divergence between SQL three-valued logic (NULL excluded from WHERE NOT ...)
# and Arrow compute (NULL rows counted as failing).
_nonnull_row_data = st.lists(_int_values, min_size=0, max_size=50)


@given(data=_nonnull_row_data)
@settings(max_examples=100)
def test_expression_sql_roundtrip(data: list[int]) -> None:
    """expression SQL produces same pass/fail as Arrow-based check."""
    table = pyarrow.table({"col": data})
    check = _make_check("expression", {"expression": "col > 0"})

    arrow_result = _execute_check(check, table)
    sqls = _generate_check_sql(check, "t")

    con = duckdb.connect(":memory:")
    con.register("t", table)
    result = con.execute(sqls[0]).fetch_arrow_table()
    failing_count = _interpret_result(result)

    assert (failing_count == 0) == arrow_result.passed


# ---------------------------------------------------------------------------
# Property 6: Assertion result consistency across execution paths
# ---------------------------------------------------------------------------


@given(data=_row_data)
@settings(max_examples=100)
def test_assertion_result_consistency_not_null(data: list[int | None]) -> None:
    """Engine-native and Arrow paths produce same passed/severity/phase for not_null."""
    table = pyarrow.table({"col": data})
    check = _make_check("not_null", {"column": "col"})

    arrow_result = _execute_check(check, table)

    sqls = _generate_check_sql(check, "t")
    con = duckdb.connect(":memory:")
    con.register("t", table)
    result = con.execute(sqls[0]).fetch_arrow_table()
    native_result = _interpret_check_sql_result(check, result)

    assert native_result.passed == arrow_result.passed
    assert native_result.severity == arrow_result.severity
    assert native_result.phase == arrow_result.phase


@given(data=_row_data)
@settings(max_examples=100)
def test_assertion_result_consistency_unique(data: list[int | None]) -> None:
    """Engine-native and Arrow paths produce same passed/severity/phase for unique."""
    table = pyarrow.table({"col": data})
    check = _make_check("unique", {"column": "col"})

    arrow_result = _execute_check(check, table)

    sqls = _generate_check_sql(check, "t")
    con = duckdb.connect(":memory:")
    con.register("t", table)
    result = con.execute(sqls[0]).fetch_arrow_table()
    native_result = _interpret_check_sql_result(check, result)

    assert native_result.passed == arrow_result.passed
    assert native_result.severity == arrow_result.severity
    assert native_result.phase == arrow_result.phase


@given(
    data=_row_data,
    min_count=st.integers(min_value=0, max_value=60),
    max_count=st.one_of(st.none(), st.integers(min_value=0, max_value=60)),
)
@settings(max_examples=100)
def test_assertion_result_consistency_row_count(
    data: list[int | None], min_count: int, max_count: int | None
) -> None:
    """Engine-native and Arrow paths produce same passed/severity/phase for row_count."""
    table = pyarrow.table({"col": data})
    cfg: dict = {"min": min_count}
    if max_count is not None:
        cfg["max"] = max_count
    check = _make_check("row_count", cfg)

    arrow_result = _execute_check(check, table)

    sqls = _generate_check_sql(check, "t")
    con = duckdb.connect(":memory:")
    con.register("t", table)
    result = con.execute(sqls[0]).fetch_arrow_table()
    native_result = _interpret_check_sql_result(check, result)

    assert native_result.passed == arrow_result.passed
    assert native_result.severity == arrow_result.severity
    assert native_result.phase == arrow_result.phase


@given(
    data=st.lists(
        st.one_of(st.none(), st.sampled_from(["a", "b", "c", "d"])), min_size=0, max_size=50
    ),
    accepted=st.lists(st.sampled_from(["a", "b", "c", "d"]), min_size=1, max_size=4, unique=True),
)
@settings(max_examples=100)
def test_assertion_result_consistency_accepted_values(
    data: list[str | None], accepted: list[str]
) -> None:
    """Engine-native and Arrow paths produce same passed/severity/phase for accepted_values."""
    table = pyarrow.table({"col": data})
    check = _make_check("accepted_values", {"column": "col", "values": accepted})

    arrow_result = _execute_check(check, table)

    sqls = _generate_check_sql(check, "t")
    con = duckdb.connect(":memory:")
    con.register("t", table)
    result = con.execute(sqls[0]).fetch_arrow_table()
    native_result = _interpret_check_sql_result(check, result)

    assert native_result.passed == arrow_result.passed
    assert native_result.severity == arrow_result.severity
    assert native_result.phase == arrow_result.phase


@given(data=_nonnull_row_data)
@settings(max_examples=100)
def test_assertion_result_consistency_expression(data: list[int]) -> None:
    """Engine-native and Arrow paths produce same passed/severity/phase for expression."""
    table = pyarrow.table({"col": data})
    check = _make_check("expression", {"expression": "col > 0"})

    arrow_result = _execute_check(check, table)

    sqls = _generate_check_sql(check, "t")
    con = duckdb.connect(":memory:")
    con.register("t", table)
    result = con.execute(sqls[0]).fetch_arrow_table()
    native_result = _interpret_check_sql_result(check, result)

    assert native_result.passed == arrow_result.passed
    assert native_result.severity == arrow_result.severity
    assert native_result.phase == arrow_result.phase


# ---------------------------------------------------------------------------
# Property 9: Execution method field reflects actual path
# ---------------------------------------------------------------------------


@given(data=_row_data)
@settings(max_examples=100)
def test_execution_method_engine_native(data: list[int | None]) -> None:
    """Engine-native path sets execution_method to 'engine_native'."""
    table = pyarrow.table({"col": data})
    check = _make_check("not_null", {"column": "col"})

    sqls = _generate_check_sql(check, "t")
    con = duckdb.connect(":memory:")
    con.register("t", table)
    result = con.execute(sqls[0]).fetch_arrow_table()
    native_result = _interpret_check_sql_result(check, result)

    assert native_result.execution_method == "engine_native"


@given(data=_row_data)
@settings(max_examples=100)
def test_execution_method_arrow(data: list[int | None]) -> None:
    """Arrow-based path sets execution_method to 'arrow'."""
    table = pyarrow.table({"col": data})
    check = _make_check("not_null", {"column": "col"})

    arrow_result = _execute_check(check, table)

    assert arrow_result.execution_method == "arrow"
