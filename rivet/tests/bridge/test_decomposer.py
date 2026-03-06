"""Tests for SQLDecomposer."""

from __future__ import annotations

import pytest

from rivet_bridge.decomposer import SQLDecomposer
from rivet_config import ColumnDecl


@pytest.fixture
def decomposer() -> SQLDecomposer:
    return SQLDecomposer()


class TestCanDecompose:
    """Tests for can_decompose — simple SQL detection."""

    def test_simple_select_star(self, decomposer: SQLDecomposer) -> None:
        assert decomposer.can_decompose("SELECT * FROM users") is True

    def test_simple_select_columns(self, decomposer: SQLDecomposer) -> None:
        assert decomposer.can_decompose("SELECT id, name FROM users") is True

    def test_select_with_where(self, decomposer: SQLDecomposer) -> None:
        assert decomposer.can_decompose("SELECT id FROM users WHERE active = true") is True

    def test_select_with_alias(self, decomposer: SQLDecomposer) -> None:
        assert decomposer.can_decompose("SELECT UPPER(name) AS name FROM users") is True

    def test_rejects_join(self, decomposer: SQLDecomposer) -> None:
        assert decomposer.can_decompose("SELECT * FROM a JOIN b ON a.id = b.id") is False

    def test_rejects_left_join(self, decomposer: SQLDecomposer) -> None:
        assert decomposer.can_decompose("SELECT * FROM a LEFT JOIN b ON a.id = b.id") is False

    def test_rejects_subquery(self, decomposer: SQLDecomposer) -> None:
        assert decomposer.can_decompose("SELECT * FROM (SELECT * FROM t) AS sub") is False

    def test_rejects_cte(self, decomposer: SQLDecomposer) -> None:
        assert decomposer.can_decompose("WITH cte AS (SELECT 1) SELECT * FROM cte") is False

    def test_rejects_window_function(self, decomposer: SQLDecomposer) -> None:
        assert decomposer.can_decompose("SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t") is False

    def test_rejects_group_by(self, decomposer: SQLDecomposer) -> None:
        assert decomposer.can_decompose("SELECT count(*) FROM t GROUP BY x") is False

    def test_rejects_having(self, decomposer: SQLDecomposer) -> None:
        assert decomposer.can_decompose("SELECT x, count(*) FROM t GROUP BY x HAVING count(*) > 1") is False

    def test_rejects_union(self, decomposer: SQLDecomposer) -> None:
        assert decomposer.can_decompose("SELECT * FROM a UNION SELECT * FROM b") is False

    def test_rejects_multiple_tables(self, decomposer: SQLDecomposer) -> None:
        assert decomposer.can_decompose("SELECT * FROM a, b") is False

    def test_rejects_distinct(self, decomposer: SQLDecomposer) -> None:
        assert decomposer.can_decompose("SELECT DISTINCT id FROM t") is False

    def test_rejects_order_by(self, decomposer: SQLDecomposer) -> None:
        assert decomposer.can_decompose("SELECT * FROM t ORDER BY id") is False

    def test_rejects_limit(self, decomposer: SQLDecomposer) -> None:
        assert decomposer.can_decompose("SELECT * FROM t LIMIT 10") is False

    def test_rejects_invalid_sql(self, decomposer: SQLDecomposer) -> None:
        assert decomposer.can_decompose("NOT VALID SQL !!!") is False


class TestDecompose:
    """Tests for decompose — extracting columns, filter, table."""

    def test_select_star(self, decomposer: SQLDecomposer) -> None:
        columns, filter_, table = decomposer.decompose("SELECT * FROM users")
        assert columns is None
        assert filter_ is None
        assert table == "users"

    def test_plain_columns(self, decomposer: SQLDecomposer) -> None:
        columns, filter_, table = decomposer.decompose("SELECT id, name FROM users")
        assert columns == [
            ColumnDecl(name="id", expression=None),
            ColumnDecl(name="name", expression=None),
        ]
        assert filter_ is None
        assert table == "users"

    def test_expression_alias(self, decomposer: SQLDecomposer) -> None:
        columns, filter_, table = decomposer.decompose("SELECT UPPER(name) AS upper_name FROM users")
        assert columns is not None
        assert len(columns) == 1
        assert columns[0].name == "upper_name"
        assert columns[0].expression is not None
        assert "UPPER" in columns[0].expression.upper()
        assert table == "users"

    def test_with_filter(self, decomposer: SQLDecomposer) -> None:
        columns, filter_, table = decomposer.decompose("SELECT id FROM users WHERE active = TRUE")
        assert columns == [ColumnDecl(name="id", expression=None)]
        assert filter_ is not None
        assert "active" in filter_.lower()
        assert table == "users"

    def test_mixed_columns(self, decomposer: SQLDecomposer) -> None:
        sql = "SELECT id, UPPER(name) AS upper_name, age FROM people"
        columns, filter_, table = decomposer.decompose(sql)
        assert columns is not None
        assert len(columns) == 3
        assert columns[0] == ColumnDecl(name="id", expression=None)
        assert columns[1].name == "upper_name"
        assert columns[1].expression is not None
        assert columns[2] == ColumnDecl(name="age", expression=None)
        assert table == "people"

    def test_preserves_column_order(self, decomposer: SQLDecomposer) -> None:
        sql = "SELECT z, a, m FROM t"
        columns, _, _ = decomposer.decompose(sql)
        assert columns is not None
        assert [c.name for c in columns] == ["z", "a", "m"]

    def test_select_star_with_filter(self, decomposer: SQLDecomposer) -> None:
        columns, filter_, table = decomposer.decompose("SELECT * FROM orders WHERE total > 100")
        assert columns is None
        assert filter_ is not None
        assert "total" in filter_.lower()
        assert table == "orders"
