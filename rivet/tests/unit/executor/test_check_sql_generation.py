"""Unit tests for Check SQL generation and classification."""

from __future__ import annotations

import pytest

from rivet_core.checks import CompiledCheck
from rivet_core.executor import _generate_check_sql, _is_sql_translatable


def _make_check(type: str, config: dict | None = None) -> CompiledCheck:
    return CompiledCheck(type=type, severity="error", config=config or {}, phase="assertion")


class TestIsSqlTranslatable:
    def test_residual_types_not_translatable(self) -> None:
        for t in ("custom", "schema", "freshness", "relationship"):
            assert _is_sql_translatable(_make_check(t)) is False

    def test_sql_translatable_types(self) -> None:
        for t in ("not_null", "unique", "row_count", "accepted_values", "expression"):
            assert _is_sql_translatable(_make_check(t)) is True


class TestGenerateCheckSql:
    def test_not_null_sql_single_column(self) -> None:
        check = _make_check("not_null", {"column": "email"})
        sqls = _generate_check_sql(check, "my_table")
        assert len(sqls) == 1
        assert sqls[0] == "SELECT COUNT(*) AS result_value FROM my_table WHERE email IS NULL"

    def test_not_null_sql_multi_column(self) -> None:
        check = _make_check("not_null", {"columns": ["a", "b"]})
        sqls = _generate_check_sql(check, "t")
        assert len(sqls) == 2
        assert sqls[0] == "SELECT COUNT(*) AS result_value FROM t WHERE a IS NULL"
        assert sqls[1] == "SELECT COUNT(*) AS result_value FROM t WHERE b IS NULL"

    def test_not_null_missing_columns_raises(self) -> None:
        check = _make_check("not_null", {})
        with pytest.raises(ValueError, match="not_null check requires"):
            _generate_check_sql(check, "t")

    def test_unique_sql_single_column(self) -> None:
        check = _make_check("unique", {"column": "id"})
        sqls = _generate_check_sql(check, "t")
        assert len(sqls) == 1
        assert "COUNT(DISTINCT id)" in sqls[0]
        assert "id IS NOT NULL" in sqls[0]

    def test_unique_sql_multi_column(self) -> None:
        check = _make_check("unique", {"columns": ["x", "y"]})
        sqls = _generate_check_sql(check, "t")
        assert len(sqls) == 2
        assert "COUNT(DISTINCT x)" in sqls[0]
        assert "COUNT(DISTINCT y)" in sqls[1]

    def test_unique_missing_columns_raises(self) -> None:
        check = _make_check("unique", {})
        with pytest.raises(ValueError, match="unique check requires"):
            _generate_check_sql(check, "t")

    def test_row_count_sql(self) -> None:
        check = _make_check("row_count", {"min": 1, "max": 100})
        sqls = _generate_check_sql(check, "data")
        assert len(sqls) == 1
        assert sqls[0] == "SELECT COUNT(*) AS result_value FROM data"

    def test_accepted_values_sql(self) -> None:
        check = _make_check(
            "accepted_values", {"column": "status", "values": ["active", "inactive"]}
        )
        sqls = _generate_check_sql(check, "t")
        assert len(sqls) == 1
        assert "status IS NOT NULL" in sqls[0]
        assert "NOT IN ('active', 'inactive')" in sqls[0]

    def test_accepted_values_missing_column_raises(self) -> None:
        check = _make_check("accepted_values", {"values": ["a"]})
        with pytest.raises(ValueError, match="accepted_values check requires 'column'"):
            _generate_check_sql(check, "t")

    def test_accepted_values_missing_values_raises(self) -> None:
        check = _make_check("accepted_values", {"column": "c"})
        with pytest.raises(ValueError, match="accepted_values check requires 'values'"):
            _generate_check_sql(check, "t")

    def test_expression_sql(self) -> None:
        check = _make_check("expression", {"expression": "price > 0"})
        sqls = _generate_check_sql(check, "t")
        assert len(sqls) == 1
        assert sqls[0] == "SELECT COUNT(*) AS result_value FROM t WHERE NOT (price > 0)"

    def test_expression_missing_expression_raises(self) -> None:
        check = _make_check("expression", {})
        with pytest.raises(ValueError, match="expression check requires"):
            _generate_check_sql(check, "t")

    def test_unsupported_type_raises(self) -> None:
        check = _make_check("custom", {"function": "mod:fn"})
        with pytest.raises(ValueError, match="Cannot generate SQL for check type"):
            _generate_check_sql(check, "t")
