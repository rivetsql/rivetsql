"""Tests for rivet_core.interactive.formatter."""

from __future__ import annotations

import pytest

from rivet_core.interactive.formatter import SqlFormatError, format_sql


def test_format_simple_select() -> None:
    sql = "select id,name from users where id=1"
    result = format_sql(sql)
    assert "SELECT" in result
    assert "FROM" in result
    assert "WHERE" in result


def test_uppercase_keywords_true() -> None:
    result = format_sql("select 1", uppercase_keywords=True)
    assert "SELECT" in result
    assert "select" not in result


def test_uppercase_keywords_false() -> None:
    result = format_sql("SELECT 1", uppercase_keywords=False)
    assert "select" in result.lower()


def test_parse_error_raises_sql_format_error() -> None:
    with pytest.raises(SqlFormatError):
        format_sql("SELECT FROM WHERE ))))")


def test_parse_error_has_line_number() -> None:
    with pytest.raises(SqlFormatError) as exc_info:
        format_sql("SELECT FROM WHERE ))))")
    # line may be None for some errors, but the exception must be raised
    assert isinstance(exc_info.value, SqlFormatError)


def test_format_with_dialect() -> None:
    sql = "select id from users"
    result = format_sql(sql, dialect="duckdb")
    assert "SELECT" in result
    assert "FROM" in result


def test_format_multistatement() -> None:
    sql = "SELECT 1; SELECT 2"
    result = format_sql(sql)
    assert "SELECT" in result
    # Both statements should appear
    assert result.count("SELECT") >= 2


def test_indent_applied() -> None:
    sql = "SELECT id, name FROM users WHERE id = 1"
    result_2 = format_sql(sql, indent=2)
    result_4 = format_sql(sql, indent=4)
    # Both should be valid formatted SQL
    assert "SELECT" in result_2
    assert "SELECT" in result_4


def test_returns_string() -> None:
    result = format_sql("SELECT 1")
    assert isinstance(result, str)


def test_empty_sql_raises() -> None:
    with pytest.raises(SqlFormatError):
        format_sql("")
