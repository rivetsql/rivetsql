"""Tests for AnnotationParser with comment_prefix='python'."""

from __future__ import annotations

from pathlib import Path

from rivet_config.annotations import AnnotationParser

PARSER = AnnotationParser()
FILE = Path("test.py")


def parse(lines: list[str]):
    return PARSER.parse(lines, FILE, comment_prefix="python")


# --- Basic parsing with Python prefix ---

def test_no_annotations_python():
    annotations, first_code, errors = parse(["def transform(): pass"])
    assert annotations == []
    assert first_code == 0
    assert errors == []


def test_single_annotation_python():
    annotations, first_code, errors = parse(["# rivet:name: my_joint\n", "def transform(): pass\n"])
    assert len(annotations) == 1
    assert annotations[0].key == "name"
    assert annotations[0].value == "my_joint"
    assert annotations[0].line_number == 1
    assert first_code == 1
    assert errors == []


def test_multiple_annotations_python():
    lines = [
        "# rivet:name: my_joint\n",
        "# rivet:type: python\n",
        "def transform(): pass\n",
    ]
    annotations, first_code, errors = parse(lines)
    assert len(annotations) == 2
    assert annotations[0].key == "name"
    assert annotations[1].key == "type"
    assert first_code == 2
    assert errors == []


def test_blank_lines_skipped_python():
    lines = [
        "# rivet:name: x\n",
        "\n",
        "def transform(): pass\n",
    ]
    _, first_code, _ = parse(lines)
    assert first_code == 2


def test_all_annotations_no_code_python():
    lines = ["# rivet:name: x\n", "# rivet:type: python\n"]
    annotations, first_code, errors = parse(lines)
    assert len(annotations) == 2
    assert first_code == 2
    assert errors == []


# --- Value type parsing with Python prefix ---

def test_bool_true_python():
    annotations, _, errors = parse(["# rivet:eager: true\n"])
    assert errors == []
    assert annotations[0].value is True


def test_bool_false_python():
    annotations, _, errors = parse(["# rivet:eager: false\n"])
    assert errors == []
    assert annotations[0].value is False


def test_list_value_python():
    annotations, _, errors = parse(["# rivet:upstream: [joint_a, joint_b]\n"])
    assert errors == []
    assert annotations[0].value == ["joint_a", "joint_b"]


def test_dict_value_python():
    annotations, _, errors = parse(["# rivet:write_strategy: {mode: replace}\n"])
    assert errors == []
    assert annotations[0].value == {"mode": "replace"}


def test_string_value_python():
    annotations, _, errors = parse(["# rivet:description: some text here\n"])
    assert errors == []
    assert annotations[0].value == "some text here"


# --- Error cases ---

def test_malformed_yaml_value_produces_error_python():
    lines = ["# rivet:write_strategy: {bad: yaml: here}\n", "def transform(): pass\n"]
    annotations, first_code, errors = parse(lines)
    assert len(errors) == 1
    assert errors[0].source_file == FILE
    assert errors[0].line_number == 1
    assert "write_strategy" in errors[0].message


# --- SQL prefix does NOT match Python annotations ---

def test_sql_prefix_ignores_python_annotations():
    lines = ["# rivet:name: my_joint\n", "def transform(): pass\n"]
    annotations, first_code, errors = PARSER.parse(lines, FILE, comment_prefix="sql")
    assert annotations == []
    assert first_code == 0
    assert errors == []


# --- Python prefix does NOT match SQL annotations ---

def test_python_prefix_ignores_sql_annotations():
    lines = ["-- rivet:name: my_joint\n", "SELECT 1\n"]
    annotations, first_code, errors = PARSER.parse(lines, FILE, comment_prefix="python")
    assert annotations == []
    assert first_code == 0
    assert errors == []


# --- Default comment_prefix is "sql" (backward compat) ---

def test_default_prefix_is_sql():
    lines = ["-- rivet:name: my_joint\n", "SELECT 1\n"]
    annotations, first_code, errors = PARSER.parse(lines, FILE)
    assert len(annotations) == 1
    assert annotations[0].key == "name"
