"""Tests for SQLGenerator."""

from __future__ import annotations

from pathlib import Path

import pytest

from rivet_bridge.sql_gen import SQLGenerator
from rivet_config import ColumnDecl, JointDeclaration


@pytest.fixture
def gen() -> SQLGenerator:
    return SQLGenerator()


SRC = Path("sources/test.yaml")


def _source(
    name: str = "raw_users",
    columns: list[ColumnDecl] | None = None,
    filter: str | None = None,
    table: str | None = None,
) -> JointDeclaration:
    return JointDeclaration(
        name=name,
        joint_type="source",
        source_path=SRC,
        columns=columns,
        filter=filter,
        table=table,
    )


def _sink(
    name: str = "output_users",
    columns: list[ColumnDecl] | None = None,
    filter: str | None = None,
    upstream: list[str] | None = None,
) -> JointDeclaration:
    return JointDeclaration(
        name=name,
        joint_type="sink",
        source_path=SRC,
        columns=columns,
        filter=filter,
        upstream=upstream,
    )


class TestSourceJoints:
    """Requirements 3.1–3.7: SQL generation from YAML source declarations."""

    def test_select_star_when_columns_none(self, gen: SQLGenerator) -> None:
        """Req 3.4: columns=None → SELECT *."""
        sql, errors = gen.generate(_source(), set())
        assert not errors
        assert "SELECT" in sql.upper()
        assert "*" in sql
        assert "raw_users" in sql

    def test_bare_columns(self, gen: SQLGenerator) -> None:
        """Req 3.2: expression=None → bare column reference."""
        decl = _source(columns=[
            ColumnDecl(name="id", expression=None),
            ColumnDecl(name="name", expression=None),
        ])
        sql, errors = gen.generate(decl, set())
        assert not errors
        assert "id" in sql
        assert "name" in sql

    def test_expression_columns(self, gen: SQLGenerator) -> None:
        """Req 3.3: non-None expression → <expression> AS <name>."""
        decl = _source(columns=[
            ColumnDecl(name="upper_name", expression="UPPER(name)"),
        ])
        sql, errors = gen.generate(decl, set())
        assert not errors
        upper_sql = sql.upper()
        assert "UPPER" in upper_sql
        assert "AS" in upper_sql

    def test_mixed_columns(self, gen: SQLGenerator) -> None:
        """Req 3.1, 3.2, 3.3: mix of bare and expression columns."""
        decl = _source(columns=[
            ColumnDecl(name="id", expression=None),
            ColumnDecl(name="upper_name", expression="UPPER(name)"),
        ])
        sql, errors = gen.generate(decl, set())
        assert not errors
        assert "id" in sql
        assert "UPPER" in sql.upper()

    def test_with_filter(self, gen: SQLGenerator) -> None:
        """Req 3.5: filter → WHERE clause."""
        decl = _source(
            columns=[ColumnDecl(name="id", expression=None)],
            filter="active = TRUE",
        )
        sql, errors = gen.generate(decl, set())
        assert not errors
        assert "WHERE" in sql.upper()

    def test_table_ref_uses_table_field(self, gen: SQLGenerator) -> None:
        """Req 3.6: table field present → use as FROM reference."""
        decl = _source(table="schema.users")
        sql, errors = gen.generate(decl, set())
        assert not errors
        # Should reference the table, not the joint name
        assert "users" in sql

    def test_table_ref_defaults_to_name(self, gen: SQLGenerator) -> None:
        """Req 3.6: no table field → use joint name."""
        decl = _source(name="my_source")
        sql, errors = gen.generate(decl, set())
        assert not errors
        assert "my_source" in sql

    def test_column_order_preserved(self, gen: SQLGenerator) -> None:
        """Req 3.7: column order matches declaration order."""
        decl = _source(columns=[
            ColumnDecl(name="z_col", expression=None),
            ColumnDecl(name="a_col", expression=None),
            ColumnDecl(name="m_col", expression=None),
        ])
        sql, errors = gen.generate(decl, set())
        assert not errors
        z_pos = sql.index("z_col")
        a_pos = sql.index("a_col")
        m_pos = sql.index("m_col")
        assert z_pos < a_pos < m_pos


class TestSinkJoints:
    """Requirements 4.1–4.5: SQL generation from YAML sink declarations."""

    def test_sink_select_from_upstream(self, gen: SQLGenerator) -> None:
        """Req 4.1, 4.2: sink references upstream joint in FROM."""
        decl = _sink(upstream=["transformed_users"])
        sql, errors = gen.generate(decl, set())
        assert not errors
        assert "transformed_users" in sql

    def test_sink_with_columns(self, gen: SQLGenerator) -> None:
        """Req 4.1: sink with columns."""
        decl = _sink(
            columns=[ColumnDecl(name="id", expression=None)],
            upstream=["transformed_users"],
        )
        sql, errors = gen.generate(decl, set())
        assert not errors
        assert "id" in sql
        assert "transformed_users" in sql

    def test_sink_with_filter(self, gen: SQLGenerator) -> None:
        """Req 4.3: sink with filter."""
        decl = _sink(
            columns=[ColumnDecl(name="id", expression=None)],
            filter="status = 'active'",
            upstream=["transformed_users"],
        )
        sql, errors = gen.generate(decl, set())
        assert not errors
        assert "WHERE" in sql.upper()

    def test_sink_select_star(self, gen: SQLGenerator) -> None:
        """Req 4.4: columns=None on sink → SELECT *."""
        decl = _sink(upstream=["transformed_users"])
        sql, errors = gen.generate(decl, set())
        assert not errors
        assert "*" in sql

    def test_sink_multiple_upstreams_uses_first(self, gen: SQLGenerator) -> None:
        """Req 4.5: multiple upstreams → use first."""
        decl = _sink(upstream=["first_joint", "second_joint"])
        sql, errors = gen.generate(decl, set())
        assert not errors
        assert "first_joint" in sql


class TestErrorHandling:
    """BRG-101 on generation failure."""

    def test_brg_101_on_failure(self, gen: SQLGenerator) -> None:
        """Invalid expression produces BRG-101."""
        decl = _source(columns=[
            ColumnDecl(name="bad", expression="INVALID((("),
        ])
        sql, errors = gen.generate(decl, set())
        assert len(errors) == 1
        assert errors[0].code == "BRG-101"
        assert errors[0].joint_name == "raw_users"
