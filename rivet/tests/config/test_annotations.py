"""Tests for AnnotationParser."""

from __future__ import annotations

from pathlib import Path

from rivet_config.annotations import AnnotationParser

PARSER = AnnotationParser()
FILE = Path("test.sql")


def parse(lines: list[str]):
    return PARSER.parse(lines, FILE)


# --- Basic parsing ---

def test_no_annotations():
    annotations, first_sql, errors = parse(["SELECT 1"])
    assert annotations == []
    assert first_sql == 0
    assert errors == []


def test_single_annotation():
    annotations, first_sql, errors = parse(["-- rivet:name: my_joint\n", "SELECT 1\n"])
    assert len(annotations) == 1
    assert annotations[0].key == "name"
    assert annotations[0].value == "my_joint"
    assert annotations[0].line_number == 1
    assert first_sql == 1
    assert errors == []


def test_multiple_annotations():
    lines = [
        "-- rivet:name: my_joint\n",
        "-- rivet:type: sql\n",
        "SELECT 1\n",
    ]
    annotations, first_sql, errors = parse(lines)
    assert len(annotations) == 2
    assert annotations[0].key == "name"
    assert annotations[1].key == "type"
    assert first_sql == 2
    assert errors == []


def test_all_annotations_no_sql():
    lines = ["-- rivet:name: x\n", "-- rivet:type: sql\n"]
    annotations, first_sql, errors = parse(lines)
    assert len(annotations) == 2
    assert first_sql == 2
    assert errors == []


def test_empty_lines():
    annotations, first_sql, errors = parse([])
    assert annotations == []
    assert first_sql == 0
    assert errors == []


# --- Value type parsing ---

def test_bool_true():
    annotations, _, errors = parse(["-- rivet:eager: true\n"])
    assert errors == []
    assert annotations[0].value is True


def test_bool_false():
    annotations, _, errors = parse(["-- rivet:eager: false\n"])
    assert errors == []
    assert annotations[0].value is False


def test_list_value():
    annotations, _, errors = parse(["-- rivet:upstream: [joint_a, joint_b]\n"])
    assert errors == []
    assert annotations[0].value == ["joint_a", "joint_b"]


def test_dict_value():
    annotations, _, errors = parse(["-- rivet:write_strategy: {mode: replace}\n"])
    assert errors == []
    assert annotations[0].value == {"mode": "replace"}


def test_string_value():
    annotations, _, errors = parse(["-- rivet:description: some text here\n"])
    assert errors == []
    assert annotations[0].value == "some text here"


def test_empty_list():
    annotations, _, errors = parse(["-- rivet:upstream: []\n"])
    assert errors == []
    assert annotations[0].value == []


# --- Error cases ---

def test_malformed_yaml_value_produces_error():
    lines = ["-- rivet:write_strategy: {bad: yaml: here}\n", "SELECT 1\n"]
    annotations, first_sql, errors = parse(lines)
    assert len(errors) == 1
    assert errors[0].source_file == FILE
    assert errors[0].line_number == 1
    assert "write_strategy" in errors[0].message


def test_error_includes_file_path():
    fp = Path("some/path/joint.sql")
    _, _, errors = PARSER.parse(["-- rivet:write_strategy: {bad: yaml: here}\n"], fp)
    assert errors[0].source_file == fp


# --- first_sql_line_index correctness ---

def test_first_sql_line_after_annotations():
    lines = [
        "-- rivet:name: x\n",
        "-- rivet:type: sql\n",
        "SELECT *\n",
        "FROM t\n",
    ]
    _, first_sql, _ = parse(lines)
    assert first_sql == 2


def test_blank_lines_before_sql_skipped():
    lines = [
        "-- rivet:name: x\n",
        "\n",
        "SELECT 1\n",
    ]
    # blank line is skipped, SQL starts at index 2
    _, first_sql, _ = parse(lines)
    assert first_sql == 2


def test_line_number_is_1_indexed():
    lines = ["-- rivet:name: x\n"]
    annotations, _, _ = parse(lines)
    assert annotations[0].line_number == 1


def test_second_annotation_line_number():
    lines = ["-- rivet:name: x\n", "-- rivet:type: sql\n"]
    annotations, _, _ = parse(lines)
    assert annotations[1].line_number == 2
