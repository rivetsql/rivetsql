"""Unit tests for limit YAML field parsing, SQL generation, and SQL decomposition.

Tests YAMLParser limit validation, SQLGenerator LIMIT emission, and
SQLDecomposer LIMIT extraction.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from rivet_bridge.decomposer import SQLDecomposer
from rivet_bridge.sql_gen import SQLGenerator
from rivet_config.models import ColumnDecl, JointDeclaration
from rivet_config.yaml_parser import YAMLParser

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PARSER = YAMLParser()
_GEN = SQLGenerator()
_DECOMP = SQLDecomposer()


def _write_yaml(tmp_path: Path, content: dict) -> Path:
    p = tmp_path / "joint.yaml"
    p.write_text(yaml.dump(content))
    return p


def _decl(
    name: str = "src",
    columns: list[ColumnDecl] | None = None,
    filter_: str | None = None,
    limit: int | None = None,
    table: str = "orders",
) -> JointDeclaration:
    return JointDeclaration(
        name=name,
        joint_type="source",
        source_path=Path("test.yaml"),
        catalog="warehouse",
        table=table,
        columns=columns,
        filter=filter_,
        limit=limit,
    )


# ===================================================================
# 9.9 — Limit YAML field parsing
# ===================================================================


class TestLimitParsing:
    """YAMLParser validates the limit field."""

    def test_valid_limit(self, tmp_path: Path) -> None:
        p = _write_yaml(
            tmp_path,
            {
                "name": "src",
                "type": "source",
                "catalog": "wh",
                "limit": 100,
            },
        )
        decl, errors = _PARSER.parse(p)
        assert not errors
        assert decl is not None
        assert decl.limit == 100

    def test_non_integer_limit_string(self, tmp_path: Path) -> None:
        p = _write_yaml(
            tmp_path,
            {
                "name": "src",
                "type": "source",
                "catalog": "wh",
                "limit": "abc",
            },
        )
        _, errors = _PARSER.parse(p)
        assert any("limit" in e.message.lower() for e in errors)

    def test_non_integer_limit_float(self, tmp_path: Path) -> None:
        p = _write_yaml(
            tmp_path,
            {
                "name": "src",
                "type": "source",
                "catalog": "wh",
                "limit": 3.14,
            },
        )
        _, errors = _PARSER.parse(p)
        assert any("limit" in e.message.lower() for e in errors)

    def test_negative_limit(self, tmp_path: Path) -> None:
        p = _write_yaml(
            tmp_path,
            {
                "name": "src",
                "type": "source",
                "catalog": "wh",
                "limit": -5,
            },
        )
        _, errors = _PARSER.parse(p)
        assert any("limit" in e.message.lower() for e in errors)

    def test_zero_limit(self, tmp_path: Path) -> None:
        p = _write_yaml(
            tmp_path,
            {
                "name": "src",
                "type": "source",
                "catalog": "wh",
                "limit": 0,
            },
        )
        _, errors = _PARSER.parse(p)
        assert any("limit" in e.message.lower() for e in errors)

    def test_boolean_limit_rejected(self, tmp_path: Path) -> None:
        """bool is a subclass of int in Python — must be explicitly rejected."""
        p = _write_yaml(
            tmp_path,
            {
                "name": "src",
                "type": "source",
                "catalog": "wh",
                "limit": True,
            },
        )
        _, errors = _PARSER.parse(p)
        assert any("limit" in e.message.lower() for e in errors)

    def test_no_limit_field(self, tmp_path: Path) -> None:
        p = _write_yaml(
            tmp_path,
            {
                "name": "src",
                "type": "source",
                "catalog": "wh",
            },
        )
        decl, errors = _PARSER.parse(p)
        assert not errors
        assert decl is not None
        assert decl.limit is None


# ===================================================================
# 9.9 — SQLGenerator emits LIMIT clause
# ===================================================================


class TestSQLGeneratorLimit:
    """SQLGenerator.generate() emits LIMIT when declaration.limit is set."""

    def test_limit_emitted(self) -> None:
        decl = _decl(limit=100)
        sql, errors = _GEN.generate(decl, set())
        assert not errors
        assert "100" in sql
        # Verify it's a valid LIMIT clause (case-insensitive)
        assert "LIMIT" in sql.upper()

    def test_no_limit_no_clause(self) -> None:
        decl = _decl(limit=None)
        sql, errors = _GEN.generate(decl, set())
        assert not errors
        assert "LIMIT" not in sql.upper()

    def test_limit_with_columns_and_filter(self) -> None:
        decl = _decl(
            columns=[ColumnDecl(name="id", expression=None)],
            filter_="status = 'active'",
            limit=50,
        )
        sql, errors = _GEN.generate(decl, set())
        assert not errors
        upper = sql.upper()
        assert "LIMIT" in upper
        assert "WHERE" in upper
        assert "50" in sql


# ===================================================================
# 9.9 — SQLDecomposer extracts LIMIT value
# ===================================================================


class TestSQLDecomposerLimit:
    """SQLDecomposer.decompose() extracts LIMIT from simple SQL."""

    def test_extracts_limit(self) -> None:
        cols, filt, table, limit = _DECOMP.decompose("SELECT id, name FROM orders LIMIT 100")
        assert limit == 100

    def test_no_limit_returns_none(self) -> None:
        cols, filt, table, limit = _DECOMP.decompose("SELECT id FROM orders")
        assert limit is None

    def test_limit_with_where(self) -> None:
        cols, filt, table, limit = _DECOMP.decompose("SELECT * FROM orders WHERE id > 0 LIMIT 50")
        assert limit == 50
        assert filt is not None

    def test_can_decompose_with_limit(self) -> None:
        assert _DECOMP.can_decompose("SELECT * FROM orders LIMIT 10")

    def test_can_decompose_without_limit(self) -> None:
        assert _DECOMP.can_decompose("SELECT id FROM orders")
