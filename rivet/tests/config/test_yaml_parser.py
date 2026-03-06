"""Tests for YAMLParser."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from rivet_config.models import (
    JOINT_NAME_MAX_LENGTH,
    ColumnDecl,
    WriteStrategyDecl,
)
from rivet_config.yaml_parser import YAMLParser


@pytest.fixture
def parser() -> YAMLParser:
    return YAMLParser()


def _write_yaml(tmp_path: Path, data: dict) -> Path:
    p = tmp_path / "joint.yaml"
    p.write_text(yaml.dump(data))
    return p


class TestValidParsing:
    def test_source_joint(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {"name": "raw_users", "type": "source", "catalog": "pg"})
        decl, errors = parser.parse(p)
        assert not errors
        assert decl is not None
        assert decl.name == "raw_users"
        assert decl.joint_type == "source"
        assert decl.catalog == "pg"
        assert decl.source_path == p

    def test_sql_joint(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {"name": "transform", "type": "sql", "sql": "SELECT 1"})
        decl, errors = parser.parse(p)
        assert not errors
        assert decl is not None
        assert decl.sql == "SELECT 1"

    def test_sink_joint(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {
            "name": "out_users", "type": "sink", "catalog": "pg", "table": "users",
        })
        decl, errors = parser.parse(p)
        assert not errors
        assert decl is not None
        assert decl.table == "users"
        # Default write strategy for sinks.
        assert decl.write_strategy == WriteStrategyDecl(mode="append", options={})

    def test_python_joint(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {"name": "py_joint", "type": "python", "function": "my_mod.run"})
        decl, errors = parser.parse(p)
        assert not errors
        assert decl is not None
        assert decl.function == "my_mod.run"

    def test_all_optional_fields(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {
            "name": "full_joint", "type": "sql", "sql": "SELECT 1",
            "engine": "spark", "eager": True, "upstream": ["a", "b"],
            "tags": ["t1"], "description": "desc", "filter": "x > 1",
            "fusion_strategy": "fuse", "materialization_strategy": "mat",
        })
        decl, errors = parser.parse(p)
        assert not errors
        assert decl is not None
        assert decl.engine == "spark"
        assert decl.eager is True
        assert decl.upstream == ["a", "b"]
        assert decl.tags == ["t1"]
        assert decl.description == "desc"
        assert decl.filter == "x > 1"
        assert decl.fusion_strategy == "fuse"
        assert decl.materialization_strategy == "mat"


class TestColumnParsing:
    def test_plain_strings(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {
            "name": "src", "type": "source", "catalog": "pg",
            "columns": ["id", "name"],
        })
        decl, errors = parser.parse(p)
        assert not errors
        assert decl is not None
        assert decl.columns == [
            ColumnDecl("id", None),
            ColumnDecl("name", None),
        ]

    def test_alias_expression_mapping(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {
            "name": "src", "type": "source", "catalog": "pg",
            "columns": [{"full_name": "first || last"}],
        })
        decl, errors = parser.parse(p)
        assert not errors
        assert decl is not None
        assert decl.columns == [ColumnDecl("full_name", "first || last")]

    def test_mixed_columns(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {
            "name": "src", "type": "source", "catalog": "pg",
            "columns": ["id", {"full_name": "first || last"}],
        })
        decl, errors = parser.parse(p)
        assert not errors
        assert decl.columns == [
            ColumnDecl("id", None),
            ColumnDecl("full_name", "first || last"),
        ]

    def test_omitted_columns_means_select_all(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {"name": "src", "type": "source", "catalog": "pg"})
        decl, errors = parser.parse(p)
        assert not errors
        assert decl.columns is None

    def test_empty_columns_rejected(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {
            "name": "src", "type": "source", "catalog": "pg", "columns": [],
        })
        _, errors = parser.parse(p)
        assert any("empty" in e.message.lower() for e in errors)

    def test_order_preserved(self, parser: YAMLParser, tmp_path: Path) -> None:
        cols = ["z_col", "a_col", "m_col"]
        p = _write_yaml(tmp_path, {
            "name": "src", "type": "source", "catalog": "pg", "columns": cols,
        })
        decl, errors = parser.parse(p)
        assert not errors
        assert [c.name for c in decl.columns] == cols


class TestWriteStrategy:
    def test_explicit_mode(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {
            "name": "out", "type": "sink", "catalog": "pg", "table": "t",
            "write_strategy": {"mode": "merge", "key": ["id"]},
        })
        decl, errors = parser.parse(p)
        assert not errors
        assert decl.write_strategy == WriteStrategyDecl(mode="merge", options={"key": ["id"]})

    def test_default_append_for_sink(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {
            "name": "out", "type": "sink", "catalog": "pg", "table": "t",
        })
        decl, errors = parser.parse(p)
        assert not errors
        assert decl.write_strategy == WriteStrategyDecl(mode="append", options={})

    def test_no_default_for_non_sink(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {"name": "src", "type": "source", "catalog": "pg"})
        decl, errors = parser.parse(p)
        assert not errors
        assert decl.write_strategy is None

    def test_invalid_mode_rejected(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {
            "name": "out", "type": "sink", "catalog": "pg", "table": "t",
            "write_strategy": {"mode": "bad_mode"},
        })
        _, errors = parser.parse(p)
        assert any("bad_mode" in e.message for e in errors)

    def test_missing_mode_rejected(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {
            "name": "out", "type": "sink", "catalog": "pg", "table": "t",
            "write_strategy": {"key": ["id"]},
        })
        _, errors = parser.parse(p)
        assert any("mode" in e.message.lower() for e in errors)


class TestValidation:
    def test_missing_name(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {"type": "sql", "sql": "SELECT 1"})
        _, errors = parser.parse(p)
        assert any("name" in e.message.lower() for e in errors)

    def test_missing_type(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {"name": "x", "sql": "SELECT 1"})
        _, errors = parser.parse(p)
        assert any("type" in e.message.lower() for e in errors)

    def test_invalid_type(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {"name": "x", "type": "bogus"})
        _, errors = parser.parse(p)
        assert any("bogus" in e.message for e in errors)

    def test_invalid_name_pattern(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {"name": "Bad-Name", "type": "sql", "sql": "SELECT 1"})
        _, errors = parser.parse(p)
        assert any("Bad-Name" in e.message for e in errors)

    def test_name_too_long(self, parser: YAMLParser, tmp_path: Path) -> None:
        long_name = "a" * (JOINT_NAME_MAX_LENGTH + 1)
        p = _write_yaml(tmp_path, {"name": long_name, "type": "sql", "sql": "SELECT 1"})
        _, errors = parser.parse(p)
        assert any("exceeds" in e.message.lower() for e in errors)

    def test_name_at_max_length_ok(self, parser: YAMLParser, tmp_path: Path) -> None:
        name = "a" * JOINT_NAME_MAX_LENGTH
        p = _write_yaml(tmp_path, {"name": name, "type": "sql", "sql": "SELECT 1"})
        decl, errors = parser.parse(p)
        assert not errors
        assert decl.name == name

    def test_unrecognized_keys(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {
            "name": "x", "type": "sql", "sql": "SELECT 1", "bogus_key": 42,
        })
        _, errors = parser.parse(p)
        assert any("bogus_key" in e.message for e in errors)

    def test_missing_catalog_for_source(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {"name": "src", "type": "source"})
        _, errors = parser.parse(p)
        assert any("catalog" in e.message for e in errors)

    def test_missing_catalog_and_table_for_sink(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {"name": "out", "type": "sink"})
        _, errors = parser.parse(p)
        assert any("catalog" in e.message for e in errors)
        assert any("table" in e.message for e in errors)

    def test_missing_sql_for_sql_type(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {"name": "q", "type": "sql"})
        _, errors = parser.parse(p)
        assert any("sql" in e.message for e in errors)

    def test_missing_function_for_python_type(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {"name": "py", "type": "python"})
        _, errors = parser.parse(p)
        assert any("function" in e.message for e in errors)

    def test_invalid_yaml(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = tmp_path / "bad.yaml"
        p.write_text(": : :\n  bad yaml {{{}}")
        _, errors = parser.parse(p)
        assert errors

    def test_non_mapping_yaml(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = tmp_path / "list.yaml"
        p.write_text("- item1\n- item2\n")
        _, errors = parser.parse(p)
        assert any("mapping" in e.message.lower() for e in errors)

    def test_missing_file(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = tmp_path / "nonexistent.yaml"
        _, errors = parser.parse(p)
        assert errors

    def test_collect_all_errors(self, parser: YAMLParser, tmp_path: Path) -> None:
        """Multiple errors are collected, not fail-fast."""
        p = _write_yaml(tmp_path, {
            "name": "Bad-Name", "type": "bogus", "unknown_key": 1,
        })
        _, errors = parser.parse(p)
        # Should have errors for: invalid name, invalid type, unrecognized key
        assert len(errors) >= 3

    def test_source_file_set(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {"name": "x", "type": "sql", "sql": "SELECT 1"})
        decl, _ = parser.parse(p)
        assert decl.source_path == p

    def test_eager_defaults_false(self, parser: YAMLParser, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {"name": "x", "type": "sql", "sql": "SELECT 1"})
        decl, _ = parser.parse(p)
        assert decl.eager is False
