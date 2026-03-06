"""Tests for PythonParser."""

from __future__ import annotations

from pathlib import Path

import pytest

from rivet_config.python_parser import PythonParser


@pytest.fixture
def tmp_py(tmp_path):
    """Helper to write a .py file and return its path."""
    def _write(content: str, name: str = "test_joint.py", subdir: str | None = None) -> Path:
        if subdir:
            d = tmp_path / subdir
            d.mkdir(parents=True, exist_ok=True)
            p = d / name
        else:
            p = tmp_path / name
        p.write_text(content)
        return p
    return _write


@pytest.fixture
def parser(tmp_path):
    return PythonParser(project_root=tmp_path)


# --- Basic parsing ---

class TestBasicParsing:
    def test_minimal_py_file(self, parser, tmp_py):
        p = tmp_py("# rivet:name: my_joint\ndef transform(): pass")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl is not None
        assert decl.name == "my_joint"
        assert decl.joint_type == "python"
        assert decl.sql is None
        assert decl.source_format == "python"
        assert decl.source_path == p

    def test_name_derived_from_filename(self, parser, tmp_py):
        p = tmp_py("# rivet:type: python\ndef transform(): pass", name="scoring.py")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.name == "scoring"

    def test_default_type_is_python(self, parser, tmp_py):
        p = tmp_py("# rivet:name: my_joint\ndef transform(): pass")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.joint_type == "python"

    def test_sql_is_always_none(self, parser, tmp_py):
        p = tmp_py("# rivet:name: my_joint\ndef transform(): pass")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.sql is None

    def test_source_format_is_python(self, parser, tmp_py):
        p = tmp_py("# rivet:name: my_joint\ndef transform(): pass")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.source_format == "python"

    def test_all_annotation_fields(self, parser, tmp_py):
        content = (
            "# rivet:name: my_joint\n"
            "# rivet:type: python\n"
            "# rivet:engine: spark\n"
            "# rivet:eager: true\n"
            "# rivet:upstream: [source_a, source_b]\n"
            "# rivet:tags: [etl, daily]\n"
            "# rivet:description: A test joint\n"
            "# rivet:fusion_strategy: merge\n"
            "# rivet:materialization_strategy: view\n"
            "def transform(): pass"
        )
        p = tmp_py(content)
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.engine == "spark"
        assert decl.eager is True
        assert decl.upstream == ["source_a", "source_b"]
        assert decl.tags == ["etl", "daily"]
        assert decl.description == "A test joint"
        assert decl.fusion_strategy == "merge"
        assert decl.materialization_strategy == "view"


# --- Function auto-derivation ---

class TestFunctionDerivation:
    def test_auto_derive_from_module_path(self, parser, tmp_py):
        p = tmp_py("# rivet:name: scoring\ndef transform(): pass", name="scoring.py", subdir="joints")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.function == "joints.scoring:transform"

    def test_auto_derive_nested_path(self, parser, tmp_py):
        p = tmp_py("# rivet:name: deep\ndef transform(): pass", name="deep.py", subdir="joints/sub")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.function == "joints.sub.deep:transform"

    def test_explicit_function_preserved(self, parser, tmp_py):
        p = tmp_py("# rivet:name: my_joint\n# rivet:function: my_module:my_func\ndef transform(): pass")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.function == "my_module:my_func"

    def test_auto_derive_top_level(self, parser, tmp_py):
        p = tmp_py("# rivet:name: scoring\ndef transform(): pass", name="scoring.py")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl.function == "scoring:transform"


# --- Type-specific required field validation ---

class TestTypeValidation:
    def test_source_requires_catalog(self, parser, tmp_py):
        p = tmp_py("# rivet:name: s\n# rivet:type: source\ndef transform(): pass")
        decl, errors = parser.parse(p)
        assert decl is None
        assert any("catalog" in e.message for e in errors)

    def test_sink_requires_catalog_and_table(self, parser, tmp_py):
        p = tmp_py("# rivet:name: s\n# rivet:type: sink\ndef transform(): pass")
        decl, errors = parser.parse(p)
        assert decl is None
        assert any("catalog" in e.message for e in errors)
        assert any("table" in e.message for e in errors)

    def test_invalid_type(self, parser, tmp_py):
        p = tmp_py("# rivet:name: s\n# rivet:type: invalid\ndef transform(): pass")
        decl, errors = parser.parse(p)
        assert decl is None
        assert any("Invalid joint type" in e.message for e in errors)

    def test_sql_type_in_py_file_errors(self, parser, tmp_py):
        p = tmp_py("# rivet:name: s\n# rivet:type: sql\ndef transform(): pass")
        decl, errors = parser.parse(p)
        assert decl is None
        assert any("SQL body" in e.message or "sql" in e.message.lower() for e in errors)


# --- Name validation ---

class TestNameValidation:
    def test_invalid_name_uppercase(self, parser, tmp_py):
        p = tmp_py("# rivet:name: MyJoint\ndef transform(): pass")
        decl, errors = parser.parse(p)
        assert decl is None
        assert any("Invalid joint name" in e.message for e in errors)

    def test_invalid_name_starts_with_digit(self, parser, tmp_py):
        p = tmp_py("# rivet:name: 1joint\ndef transform(): pass")
        decl, errors = parser.parse(p)
        assert decl is None
        assert any("Invalid joint name" in e.message for e in errors)

    def test_name_too_long(self, parser, tmp_py):
        long_name = "a" * 129
        p = tmp_py(f"# rivet:name: {long_name}\ndef transform(): pass")
        decl, errors = parser.parse(p)
        assert decl is None
        assert any("exceeds maximum length" in e.message for e in errors)


# --- Unrecognized annotation keys ---

class TestUnrecognizedKeys:
    def test_unrecognized_key_produces_error(self, parser, tmp_py):
        p = tmp_py("# rivet:name: j\n# rivet:bogus: value\ndef transform(): pass")
        decl, errors = parser.parse(p)
        assert decl is None
        assert any("Unrecognized annotation key 'bogus'" in e.message for e in errors)

    def test_unrecognized_key_lists_recognized(self, parser, tmp_py):
        p = tmp_py("# rivet:name: j\n# rivet:bogus: value\ndef transform(): pass")
        _, errors = parser.parse(p)
        bogus_errors = [e for e in errors if "bogus" in e.message]
        assert "Recognized keys" in bogus_errors[0].remediation


# --- No annotations ---

class TestNoAnnotations:
    def test_no_annotations_produces_error(self, parser, tmp_py):
        p = tmp_py("def transform(): pass")
        decl, errors = parser.parse(p)
        assert decl is None
        assert any("No rivet annotations" in e.message for e in errors)

    def test_empty_file_produces_error(self, parser, tmp_py):
        p = tmp_py("")
        decl, errors = parser.parse(p)
        assert decl is None
        assert any("No rivet annotations" in e.message for e in errors)


# --- Quality annotation keys are not rejected ---

class TestQualityAnnotations:
    def test_assert_annotation_not_rejected(self, parser, tmp_py):
        p = tmp_py("# rivet:name: j\n# rivet:assert: not_null(col)\ndef transform(): pass")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl is not None

    def test_audit_annotation_not_rejected(self, parser, tmp_py):
        p = tmp_py("# rivet:name: j\n# rivet:audit: row_count(min=1)\ndef transform(): pass")
        decl, errors = parser.parse(p)
        assert errors == []
        assert decl is not None


# --- File read errors ---

class TestFileErrors:
    def test_missing_file(self, parser, tmp_path):
        p = tmp_path / "nonexistent.py"
        decl, errors = parser.parse(p)
        assert decl is None
        assert len(errors) == 1
        assert "Failed to read" in errors[0].message
