"""Unit tests for rivet_core.testing.models."""

from pathlib import Path

import pytest

from rivet_core.testing.models import ComparisonResult, TestDef, TestResult


class TestComparisonResult:
    def test_field_access(self):
        cr = ComparisonResult(passed=True, message="ok", diff=[{"col": "a"}])
        assert cr.passed is True
        assert cr.message == "ok"
        assert cr.diff == [{"col": "a"}]

    def test_defaults(self):
        cr = ComparisonResult(passed=False, message="fail")
        assert cr.diff is None

    def test_immutability(self):
        cr = ComparisonResult(passed=True, message="ok")
        with pytest.raises(AttributeError):
            cr.passed = False  # type: ignore[misc]


class TestTestResult:
    def test_field_access(self):
        cr = ComparisonResult(passed=True, message="ok")
        tr = TestResult(name="t", passed=True, duration_ms=1.0, comparison_result=cr)
        assert tr.name == "t"
        assert tr.passed is True
        assert tr.duration_ms == 1.0
        assert tr.comparison_result is cr

    def test_defaults(self):
        tr = TestResult(name="t", passed=False, duration_ms=0.0)
        assert tr.comparison_result is None
        assert tr.check_results == []
        assert tr.error is None

    def test_immutability(self):
        tr = TestResult(name="t", passed=True, duration_ms=0.0)
        with pytest.raises(AttributeError):
            tr.passed = False  # type: ignore[misc]


class TestTestDef:
    def test_defaults(self):
        td = TestDef(name="t", target="j")
        assert td.scope == "joint"
        assert td.compare == "exact"
        assert td.inputs == {}
        assert td.expected is None
        assert td.tags == []
        assert td.description is None
        assert td.options == {}
        assert td.extends is None
        assert td.source_file is None
        assert td.engine is None
        assert td.targets is None
        assert td.compare_function is None

    def test_empty_name_rejected(self):
        with pytest.raises(ValueError, match="name"):
            TestDef(name="", target="j")

    def test_empty_target_rejected(self):
        with pytest.raises(ValueError, match="target"):
            TestDef(name="t", target="")

    def test_all_fields(self):
        td = TestDef(
            name="t",
            target="j",
            targets={"j": {"expected": {}}},
            scope="assembly",
            inputs={"src": {"file": "a.csv"}},
            expected={"file": "out.parquet"},
            compare="unordered",
            compare_function="my.mod.fn",
            tags=["smoke"],
            description="desc",
            options={"tolerance": 0.01},
            extends="base",
            source_file=Path("tests/t.test.yaml"),
            engine="duckdb",
        )
        assert td.scope == "assembly"
        assert td.compare == "unordered"
        assert td.tags == ["smoke"]
        assert td.source_file == Path("tests/t.test.yaml")

    def test_immutability(self):
        td = TestDef(name="t", target="j")
        with pytest.raises(AttributeError):
            td.name = "x"  # type: ignore[misc]
