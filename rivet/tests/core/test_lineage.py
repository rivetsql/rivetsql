"""Tests for column-level lineage extraction (task 9.5)."""

from __future__ import annotations

import pytest

from rivet_core.lineage import ColumnLineage, ColumnOrigin
from rivet_core.models import Column, Schema
from rivet_core.sql_parser import SQLParser


@pytest.fixture
def parser() -> SQLParser:
    return SQLParser()


@pytest.fixture
def upstream_schemas() -> dict[str, Schema]:
    return {
        "orders": Schema(
            columns=[
                Column(name="id", type="int64", nullable=False),
                Column(name="amount", type="float64", nullable=True),
                Column(name="status", type="utf8", nullable=True),
                Column(name="created_at", type="timestamp[us]", nullable=True),
            ]
        ),
        "customers": Schema(
            columns=[
                Column(name="id", type="int64", nullable=False),
                Column(name="name", type="utf8", nullable=True),
            ]
        ),
    }


class TestExtractLineage:
    """Tests for SQLParser.extract_lineage()."""

    def test_direct_column(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT id FROM orders")
        lineage = parser.extract_lineage(ast, {}, joint_name="src")
        assert len(lineage) == 1
        assert lineage[0].output_column == "id"
        assert lineage[0].transform == "direct"
        assert lineage[0].expression is None

    def test_renamed_column(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT id AS order_id FROM orders")
        lineage = parser.extract_lineage(ast, {}, joint_name="src")
        assert len(lineage) == 1
        assert lineage[0].output_column == "order_id"
        assert lineage[0].transform == "renamed"
        assert lineage[0].origins[0].column == "id"

    def test_literal(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT 1 AS one FROM orders")
        lineage = parser.extract_lineage(ast, {}, joint_name="src")
        assert len(lineage) == 1
        assert lineage[0].output_column == "one"
        assert lineage[0].transform == "literal"
        assert lineage[0].origins == []

    def test_expression(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT UPPER(name) AS upper_name FROM customers")
        lineage = parser.extract_lineage(ast, {}, joint_name="src")
        assert len(lineage) == 1
        assert lineage[0].output_column == "upper_name"
        assert lineage[0].transform == "expression"
        assert len(lineage[0].origins) == 1
        assert lineage[0].origins[0].column == "name"

    def test_aggregation(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT SUM(amount) AS total FROM orders GROUP BY status")
        lineage = parser.extract_lineage(ast, {}, joint_name="src")
        total = next(l for l in lineage if l.output_column == "total")
        assert total.transform == "aggregation"
        assert any(o.column == "amount" for o in total.origins)

    def test_window_function(self, parser: SQLParser) -> None:
        ast = parser.parse(
            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM orders"
        )
        lineage = parser.extract_lineage(ast, {}, joint_name="src")
        rn = next(l for l in lineage if l.output_column == "rn")
        assert rn.transform == "window"

    def test_multi_column(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT amount * id AS product FROM orders")
        lineage = parser.extract_lineage(ast, {}, joint_name="src")
        assert len(lineage) == 1
        assert lineage[0].output_column == "product"
        assert lineage[0].transform == "multi_column"
        assert len(lineage[0].origins) == 2

    def test_multiple_projections(self, parser: SQLParser) -> None:
        ast = parser.parse("SELECT id, amount, 'x' AS lit FROM orders")
        lineage = parser.extract_lineage(ast, {}, joint_name="src")
        assert len(lineage) == 3
        transforms = {l.output_column: l.transform for l in lineage}
        assert transforms["id"] == "direct"
        assert transforms["amount"] == "direct"
        assert transforms["lit"] == "literal"

    def test_star_expansion(
        self, parser: SQLParser, upstream_schemas: dict[str, Schema]
    ) -> None:
        ast = parser.parse("SELECT * FROM orders")
        lineage = parser.extract_lineage(ast, upstream_schemas, joint_name="src")
        assert len(lineage) == 4
        assert all(l.transform == "direct" for l in lineage)
        names = [l.output_column for l in lineage]
        assert "id" in names
        assert "amount" in names

    def test_table_qualified_origins(
        self, parser: SQLParser, upstream_schemas: dict[str, Schema]
    ) -> None:
        ast = parser.parse(
            "SELECT orders.id, customers.name FROM orders JOIN customers ON orders.id = customers.id"
        )
        lineage = parser.extract_lineage(ast, upstream_schemas, joint_name="src")
        id_lineage = next(l for l in lineage if l.output_column == "orders.id")
        assert id_lineage.origins[0].joint == "orders"
        assert id_lineage.origins[0].column == "id"

    def test_no_select_returns_empty(self, parser: SQLParser) -> None:
        """Non-select AST returns empty lineage."""
        import sqlglot.expressions as exp

        # Create a bare expression that has no Select
        node = exp.Literal.number(1)
        lineage = parser.extract_lineage(node, {}, joint_name="src")
        assert lineage == []


class TestColumnOriginDataclass:
    def test_frozen(self) -> None:
        o = ColumnOrigin(joint="j", column="c")
        with pytest.raises(AttributeError):
            o.joint = "x"  # type: ignore[misc]

    def test_equality(self) -> None:
        a = ColumnOrigin(joint="j", column="c")
        b = ColumnOrigin(joint="j", column="c")
        assert a == b


class TestColumnLineageDataclass:
    def test_frozen(self) -> None:
        l = ColumnLineage(output_column="x", transform="direct", origins=[], expression=None)
        with pytest.raises(AttributeError):
            l.transform = "y"  # type: ignore[misc]
