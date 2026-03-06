"""Tests for QualityParser."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from rivet_config.annotations import ParsedAnnotation
from rivet_config.quality import QualityParser


@pytest.fixture
def parser() -> QualityParser:
    return QualityParser()


# --- parse_inline ---


class TestParseInline:
    def test_assertions_and_audits(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "joint.yaml"
        fp.touch()
        raw = {
            "assertions": [
                {"type": "not_null", "columns": ["id"]},
                {"type": "unique", "columns": ["id"]},
            ],
            "audits": [
                {"type": "row_count", "min": 1},
            ],
        }
        checks, errors = parser.parse_inline(raw, fp)
        assert not errors
        assert len(checks) == 3
        assert checks[0].phase == "assertion"
        assert checks[0].check_type == "not_null"
        assert checks[0].source == "inline"
        assert checks[1].phase == "assertion"
        assert checks[2].phase == "audit"
        assert checks[2].check_type == "row_count"

    def test_severity_defaults_to_error(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.yaml"
        fp.touch()
        raw = {"assertions": [{"type": "not_null", "columns": ["x"]}]}
        checks, errors = parser.parse_inline(raw, fp)
        assert not errors
        assert checks[0].severity == "error"

    def test_severity_override(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.yaml"
        fp.touch()
        raw = {"assertions": [{"type": "not_null", "columns": ["x"], "severity": "warning"}]}
        checks, errors = parser.parse_inline(raw, fp)
        assert not errors
        assert checks[0].severity == "warning"

    def test_unrecognized_type(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.yaml"
        fp.touch()
        raw = {"assertions": [{"type": "bogus"}]}
        checks, errors = parser.parse_inline(raw, fp)
        assert len(errors) == 1
        assert "bogus" in errors[0].message

    def test_missing_type(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.yaml"
        fp.touch()
        raw = {"assertions": [{"columns": ["x"]}]}
        checks, errors = parser.parse_inline(raw, fp)
        assert len(errors) == 1
        assert "type" in errors[0].message

    def test_missing_required_param(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.yaml"
        fp.touch()
        raw = {"assertions": [{"type": "not_null"}]}
        checks, errors = parser.parse_inline(raw, fp)
        assert len(errors) == 1
        assert "columns" in errors[0].message

    def test_empty_quality_block(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.yaml"
        fp.touch()
        checks, errors = parser.parse_inline({}, fp)
        assert not errors
        assert checks == []

    def test_config_excludes_type_and_severity(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.yaml"
        fp.touch()
        raw = {"assertions": [{"type": "not_null", "columns": ["a"], "severity": "warning"}]}
        checks, errors = parser.parse_inline(raw, fp)
        assert not errors
        assert "type" not in checks[0].config
        assert "severity" not in checks[0].config
        assert checks[0].config["columns"] == ["a"]


# --- parse_sql_annotations ---


class TestParseSqlAnnotations:
    def test_assert_annotation(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.sql"
        fp.touch()
        annotations = [
            ParsedAnnotation(key="assert", value="not_null(id)", line_number=1),
        ]
        checks, errors = parser.parse_sql_annotations(annotations, fp)
        assert not errors
        assert len(checks) == 1
        assert checks[0].check_type == "not_null"
        assert checks[0].phase == "assertion"
        assert checks[0].config["columns"] == ["id"]
        assert checks[0].source == "sql_annotation"

    def test_audit_annotation(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.sql"
        fp.touch()
        annotations = [
            ParsedAnnotation(key="audit", value="row_count(min=1)", line_number=1),
        ]
        checks, errors = parser.parse_sql_annotations(annotations, fp)
        assert not errors
        assert checks[0].phase == "audit"
        assert checks[0].config["min"] == 1

    def test_keyword_args(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.sql"
        fp.touch()
        annotations = [
            ParsedAnnotation(key="assert", value="accepted_values(column=status, values=[active, inactive])", line_number=1),
        ]
        checks, errors = parser.parse_sql_annotations(annotations, fp)
        assert not errors
        assert checks[0].config["column"] == "status"
        assert checks[0].config["values"] == ["active", "inactive"]

    def test_severity_in_args(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.sql"
        fp.touch()
        annotations = [
            ParsedAnnotation(key="assert", value="not_null(id, severity=warning)", line_number=1),
        ]
        checks, errors = parser.parse_sql_annotations(annotations, fp)
        assert not errors
        assert checks[0].severity == "warning"
        assert "severity" not in checks[0].config

    def test_unrecognized_type(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.sql"
        fp.touch()
        annotations = [
            ParsedAnnotation(key="assert", value="bogus(x)", line_number=5),
        ]
        checks, errors = parser.parse_sql_annotations(annotations, fp)
        assert len(errors) == 1
        assert "bogus" in errors[0].message
        assert errors[0].line_number == 5

    def test_malformed_annotation(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.sql"
        fp.touch()
        annotations = [
            ParsedAnnotation(key="assert", value="not_null", line_number=3),
        ]
        checks, errors = parser.parse_sql_annotations(annotations, fp)
        assert len(errors) == 1
        assert "TYPE(ARGS)" in errors[0].message

    def test_skips_non_quality_annotations(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.sql"
        fp.touch()
        annotations = [
            ParsedAnnotation(key="name", value="my_joint", line_number=1),
            ParsedAnnotation(key="assert", value="not_null(id)", line_number=2),
        ]
        checks, errors = parser.parse_sql_annotations(annotations, fp)
        assert not errors
        assert len(checks) == 1

    def test_default_severity(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.sql"
        fp.touch()
        annotations = [
            ParsedAnnotation(key="assert", value="not_null(id)", line_number=1),
        ]
        checks, errors = parser.parse_sql_annotations(annotations, fp)
        assert checks[0].severity == "error"


# --- parse_dedicated_file ---


class TestParseDedicatedFile:
    def test_flat_list(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "raw_products.yaml"
        fp.write_text(yaml.dump([
            {"type": "not_null", "columns": ["id"]},
            {"type": "unique", "columns": ["id"]},
        ]))
        checks, errors = parser.parse_dedicated_file(fp)
        assert not errors
        assert len(checks) == 2
        assert all(c.phase == "assertion" for c in checks)
        assert all(c.source == "dedicated" for c in checks)

    def test_sectioned_format(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "raw_products.yaml"
        fp.write_text(yaml.dump({
            "assertions": [{"type": "not_null", "columns": ["id"]}],
            "audits": [{"type": "row_count", "min": 1}],
        }))
        checks, errors = parser.parse_dedicated_file(fp)
        assert not errors
        assert len(checks) == 2
        assert checks[0].phase == "assertion"
        assert checks[1].phase == "audit"

    def test_with_joint_field(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "checks.yaml"
        fp.write_text(yaml.dump({
            "joint": "my_joint",
            "assertions": [{"type": "not_null", "columns": ["id"]}],
        }))
        checks, errors = parser.parse_dedicated_file(fp, target_joint="my_joint")
        assert not errors
        assert len(checks) == 1

    def test_unrecognized_type_in_file(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "q.yaml"
        fp.write_text(yaml.dump([{"type": "bogus"}]))
        checks, errors = parser.parse_dedicated_file(fp)
        assert len(errors) == 1
        assert "bogus" in errors[0].message

    def test_missing_file(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "nonexistent.yaml"
        checks, errors = parser.parse_dedicated_file(fp)
        assert len(errors) == 1

    def test_invalid_yaml(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "bad.yaml"
        fp.write_text(": : :")
        checks, errors = parser.parse_dedicated_file(fp)
        assert len(errors) >= 1


# --- parse_colocated_file ---


class TestParseColocatedFile:
    def test_flat_list(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "my_joint_quality.yaml"
        fp.write_text(yaml.dump([{"type": "not_null", "columns": ["id"]}]))
        checks, errors = parser.parse_colocated_file(fp)
        assert not errors
        assert len(checks) == 1
        assert checks[0].source == "colocated"

    def test_sectioned(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "my_joint_quality.yaml"
        fp.write_text(yaml.dump({
            "assertions": [{"type": "unique", "columns": ["id"]}],
            "audits": [{"type": "row_count", "min": 0}],
        }))
        checks, errors = parser.parse_colocated_file(fp)
        assert not errors
        assert len(checks) == 2
        assert checks[0].phase == "assertion"
        assert checks[1].phase == "audit"


# --- Argument parsing edge cases ---


class TestArgumentParsing:
    def test_multiple_positional(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.sql"
        fp.touch()
        annotations = [
            ParsedAnnotation(key="assert", value="unique(col_a, col_b)", line_number=1),
        ]
        checks, errors = parser.parse_sql_annotations(annotations, fp)
        assert not errors
        assert checks[0].config["columns"] == ["col_a", "col_b"]

    def test_mixed_positional_and_keyword(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.sql"
        fp.touch()
        annotations = [
            ParsedAnnotation(key="assert", value="not_null(id, severity=warning)", line_number=1),
        ]
        checks, errors = parser.parse_sql_annotations(annotations, fp)
        assert not errors
        assert checks[0].config["columns"] == ["id"]
        assert checks[0].severity == "warning"

    def test_empty_args(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.sql"
        fp.touch()
        annotations = [
            ParsedAnnotation(key="assert", value="row_count()", line_number=1),
        ]
        checks, errors = parser.parse_sql_annotations(annotations, fp)
        assert not errors
        assert checks[0].config == {}

    def test_numeric_coercion(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.sql"
        fp.touch()
        annotations = [
            ParsedAnnotation(key="assert", value="row_count(min=1, max=100)", line_number=1),
        ]
        checks, errors = parser.parse_sql_annotations(annotations, fp)
        assert not errors
        assert checks[0].config["min"] == 1
        assert checks[0].config["max"] == 100

    def test_expression_check_with_sql(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.sql"
        fp.touch()
        annotations = [
            ParsedAnnotation(key="assert", value="expression(sql=count(*) > 0)", line_number=1),
        ]
        checks, errors = parser.parse_sql_annotations(annotations, fp)
        assert not errors
        assert checks[0].config["sql"] == "count(*) > 0"

    def test_freshness_check(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.sql"
        fp.touch()
        annotations = [
            ParsedAnnotation(key="assert", value="freshness(column=updated_at, max_age=48h)", line_number=1),
        ]
        checks, errors = parser.parse_sql_annotations(annotations, fp)
        assert not errors
        assert checks[0].config["column"] == "updated_at"
        assert checks[0].config["max_age"] == "48h"

    def test_relationship_check(self, parser: QualityParser, tmp_path: Path) -> None:
        fp = tmp_path / "j.sql"
        fp.touch()
        annotations = [
            ParsedAnnotation(key="assert", value="relationship(column=user_id, to=users.id)", line_number=1),
        ]
        checks, errors = parser.parse_sql_annotations(annotations, fp)
        assert not errors
        assert checks[0].config["column"] == "user_id"
        assert checks[0].config["to"] == "users.id"
