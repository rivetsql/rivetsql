"""Tests for SQLParser."""

from __future__ import annotations

from pathlib import Path

import pytest

from rivet_config.sql_parser import SQLParser


@pytest.fixture
def parser():
    return SQLParser()


@pytest.fixture
def tmp_sql(tmp_path):
    """Helper to write a SQL file and return its path."""
    def _write(content: str, name: str = "test_joint.sql") -> Path:
        p = tmp_path / name
        p.write_text(content)
        return p
    return _write


# --- Basic parsing ---

class TestBasicParsing:
    def test_minimal_sql_file(self, parser, tmp_sql):
        p = tmp_sql("SELECT 1")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl is not None
        assert decl.name == "test_joint"  # derived from filename stem
        assert decl.joint_type == "sql"
        assert decl.sql == "SELECT 1"
        assert decl.source_path == p

    def test_with_name_annotation(self, parser, tmp_sql):
        p = tmp_sql("-- rivet:name: my_joint\nSELECT 1")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.name == "my_joint"

    def test_with_type_annotation(self, parser, tmp_sql):
        p = tmp_sql("-- rivet:name: my_source\n-- rivet:type: source\n-- rivet:catalog: main\nSELECT 1")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.joint_type == "source"

    def test_default_type_is_sql(self, parser, tmp_sql):
        p = tmp_sql("-- rivet:name: my_joint\nSELECT 1")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.joint_type == "sql"

    def test_sql_body_extraction(self, parser, tmp_sql):
        content = "-- rivet:name: my_joint\n-- rivet:type: sql\nSELECT *\nFROM users\nWHERE active = true"
        p = tmp_sql(content)
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.sql == "SELECT *\nFROM users\nWHERE active = true"

    def test_name_derived_from_filename(self, parser, tmp_sql):
        p = tmp_sql("SELECT 1", name="raw_products.sql")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.name == "raw_products"

    def test_all_annotation_fields(self, parser, tmp_sql):
        content = (
            "-- rivet:name: my_joint\n"
            "-- rivet:type: sql\n"
            "-- rivet:engine: spark\n"
            "-- rivet:eager: true\n"
            "-- rivet:upstream: [source_a, source_b]\n"
            "-- rivet:tags: [etl, daily]\n"
            "-- rivet:description: A test joint\n"
            "-- rivet:fusion_strategy: merge\n"
            "-- rivet:materialization_strategy: view\n"
            "SELECT 1"
        )
        p = tmp_sql(content)
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.engine == "spark"
        assert decl.eager is True
        assert decl.upstream == ["source_a", "source_b"]
        assert decl.tags == ["etl", "daily"]
        assert decl.description == "A test joint"
        assert decl.fusion_strategy == "merge"
        assert decl.materialization_strategy == "view"


# --- Type-specific required field validation ---

class TestTypeValidation:
    def test_source_requires_catalog(self, parser, tmp_sql):
        p = tmp_sql("-- rivet:name: s\n-- rivet:type: source\nSELECT 1")
        decl, errors = parser.parse(p)
        assert decl is None
        assert any("catalog" in e.message for e in errors)

    def test_sink_requires_catalog_and_table(self, parser, tmp_sql):
        p = tmp_sql("-- rivet:name: s\n-- rivet:type: sink\nSELECT 1")
        decl, errors = parser.parse(p)
        assert decl is None
        assert any("catalog" in e.message for e in errors)
        assert any("table" in e.message for e in errors)

    def test_sql_type_requires_sql_body(self, parser, tmp_sql):
        # File with only annotations, no SQL body
        p = tmp_sql("-- rivet:name: s\n-- rivet:type: sql\n")
        decl, errors = parser.parse(p)
        assert decl is None
        assert any("SQL body" in e.message for e in errors)

    def test_python_requires_function(self, parser, tmp_sql):
        p = tmp_sql("-- rivet:name: s\n-- rivet:type: python\nSELECT 1")
        decl, errors = parser.parse(p)
        assert decl is None
        assert any("function" in e.message for e in errors)

    def test_invalid_type(self, parser, tmp_sql):
        p = tmp_sql("-- rivet:name: s\n-- rivet:type: invalid\nSELECT 1")
        decl, errors = parser.parse(p)
        assert decl is None
        assert any("Invalid joint type" in e.message for e in errors)

    def test_source_with_catalog_succeeds(self, parser, tmp_sql):
        p = tmp_sql("-- rivet:name: s\n-- rivet:type: source\n-- rivet:catalog: main\nSELECT 1")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.catalog == "main"

    def test_sink_with_all_required(self, parser, tmp_sql):
        content = "-- rivet:name: s\n-- rivet:type: sink\n-- rivet:catalog: main\n-- rivet:table: output\nSELECT 1"
        p = tmp_sql(content)
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.catalog == "main"
        assert decl.table == "output"


# --- Name validation ---

class TestNameValidation:
    def test_invalid_name_uppercase(self, parser, tmp_sql):
        p = tmp_sql("-- rivet:name: MyJoint\nSELECT 1")
        decl, errors = parser.parse(p)
        assert decl is None
        assert any("Invalid joint name" in e.message for e in errors)

    def test_invalid_name_starts_with_digit(self, parser, tmp_sql):
        p = tmp_sql("-- rivet:name: 1joint\nSELECT 1")
        decl, errors = parser.parse(p)
        assert decl is None
        assert any("Invalid joint name" in e.message for e in errors)

    def test_name_too_long(self, parser, tmp_sql):
        long_name = "a" * 129
        p = tmp_sql(f"-- rivet:name: {long_name}\nSELECT 1")
        decl, errors = parser.parse(p)
        assert decl is None
        assert any("exceeds maximum length" in e.message for e in errors)


# --- Unrecognized annotation keys ---

class TestUnrecognizedKeys:
    def test_unrecognized_key_produces_error(self, parser, tmp_sql):
        p = tmp_sql("-- rivet:name: j\n-- rivet:bogus: value\nSELECT 1")
        decl, errors = parser.parse(p)
        assert decl is None
        assert any("Unrecognized annotation key 'bogus'" in e.message for e in errors)

    def test_unrecognized_key_includes_line_number(self, parser, tmp_sql):
        p = tmp_sql("-- rivet:name: j\n-- rivet:bogus: value\nSELECT 1")
        _, errors = parser.parse(p)
        bogus_errors = [e for e in errors if "bogus" in e.message]
        assert bogus_errors[0].line_number == 2


# --- Quality annotation keys are not rejected ---

class TestQualityAnnotations:
    def test_assert_annotation_not_rejected(self, parser, tmp_sql):
        p = tmp_sql("-- rivet:name: j\n-- rivet:assert: not_null(col)\nSELECT 1")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl is not None

    def test_audit_annotation_not_rejected(self, parser, tmp_sql):
        p = tmp_sql("-- rivet:name: j\n-- rivet:audit: row_count(min=1)\nSELECT 1")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl is not None


# --- Write strategy ---

class TestWriteStrategy:
    def test_sink_defaults_to_append(self, parser, tmp_sql):
        content = "-- rivet:name: s\n-- rivet:type: sink\n-- rivet:catalog: main\n-- rivet:table: out\nSELECT 1"
        p = tmp_sql(content)
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.write_strategy is not None
        assert decl.write_strategy.mode == "append"

    def test_explicit_write_strategy(self, parser, tmp_sql):
        content = "-- rivet:name: s\n-- rivet:type: sink\n-- rivet:catalog: main\n-- rivet:table: out\n-- rivet:write_strategy: {mode: replace}\nSELECT 1"
        p = tmp_sql(content)
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.write_strategy.mode == "replace"

    def test_invalid_write_strategy_mode(self, parser, tmp_sql):
        content = "-- rivet:name: s\n-- rivet:type: sink\n-- rivet:catalog: main\n-- rivet:table: out\n-- rivet:write_strategy: {mode: bad}\nSELECT 1"
        p = tmp_sql(content)
        decl, errors = parser.parse(p)
        assert decl is None
        assert any("Invalid write strategy mode" in e.message for e in errors)

    def test_non_sink_no_default_write_strategy(self, parser, tmp_sql):
        p = tmp_sql("-- rivet:name: j\nSELECT 1")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.write_strategy is None


# --- Columns ---

class TestColumns:
    def test_columns_annotation(self, parser, tmp_sql):
        content = "-- rivet:name: s\n-- rivet:type: source\n-- rivet:catalog: main\n-- rivet:columns: [id, name]\nSELECT 1"
        p = tmp_sql(content)
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.columns is not None
        assert len(decl.columns) == 2
        assert decl.columns[0].name == "id"
        assert decl.columns[0].expression is None

    def test_no_columns_means_none(self, parser, tmp_sql):
        p = tmp_sql("-- rivet:name: j\nSELECT 1")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.columns is None


# --- File read errors ---

class TestFileErrors:
    def test_missing_file(self, parser, tmp_path):
        p = tmp_path / "nonexistent.sql"
        decl, errors = parser.parse(p)
        assert decl is None
        assert len(errors) == 1
        assert "Failed to read" in errors[0].message
