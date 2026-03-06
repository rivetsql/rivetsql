"""Tests for test text renderer (rendering/test_text.py)."""

from __future__ import annotations

import json
import re

from rivet_cli.rendering.colors import GREEN, RED
from rivet_cli.rendering.test_text import (
    CaseResult,
    render_test_results,
    render_test_results_json,
    render_test_text,
)
from rivet_core.testing.models import ComparisonResult, TestResult

ANSI_RE = re.compile(r"\033\[")


class TestRenderTestText:
    def test_pass_result(self):
        r = CaseResult(name="test_one", passed=True, duration_ms=5.0)
        out = render_test_text([r], 0, False)
        assert "PASS" in out
        assert "test_one" in out
        assert "5ms" in out

    def test_fail_result_with_diff(self):
        r = CaseResult(name="test_two", passed=False, duration_ms=12.0,
                       diff="expected 10 rows, got 8\n- missing: row_x")
        out = render_test_text([r], 0, False)
        assert "FAIL" in out
        assert "expected 10 rows" in out
        assert "missing: row_x" in out

    def test_summary_counts(self):
        results = [
            CaseResult(name="t1", passed=True, duration_ms=5.0),
            CaseResult(name="t2", passed=False, duration_ms=10.0),
            CaseResult(name="t3", passed=True, duration_ms=3.0),
        ]
        out = render_test_text(results, 0, False)
        assert "1 failed" in out
        assert "2 passed" in out
        assert "3 total" in out

    def test_summary_total_time(self):
        results = [
            CaseResult(name="t1", passed=True, duration_ms=5.0),
            CaseResult(name="t2", passed=True, duration_ms=10.0),
        ]
        out = render_test_text(results, 0, False)
        assert "15ms" in out

    def test_verbose_shows_joint_and_inputs(self):
        r = CaseResult(name="t1", passed=True, duration_ms=1.0,
                       target_joint="my_joint", inputs={"src": "data"},
                       comparison="exact")
        out = render_test_text([r], 1, False)
        assert "joint: my_joint" in out
        assert "inputs: src" in out
        assert "comparison: exact" in out


class TestProperty3TestText:
    def test_no_ansi_when_color_false(self):
        results = [
            CaseResult(name="t1", passed=True, duration_ms=5.0),
            CaseResult(name="t2", passed=False, duration_ms=10.0, diff="diff"),
        ]
        out = render_test_text(results, 0, False)
        assert not ANSI_RE.search(out)


class TestProperty6TestText:
    def test_pass_green(self):
        r = CaseResult(name="t1", passed=True, duration_ms=1.0)
        out = render_test_text([r], 0, True)
        assert GREEN in out

    def test_fail_red(self):
        r = CaseResult(name="t1", passed=False, duration_ms=1.0)
        out = render_test_text([r], 0, True)
        assert RED in out


# ---------------------------------------------------------------------------
# Tests for render_test_results (TestResult-based renderer)
# ---------------------------------------------------------------------------


class TestRenderTestResults:
    def test_pass_shows_check_name_duration(self):
        r = TestResult(name="my_test", passed=True, duration_ms=42.0)
        out = render_test_results([r], 0, False)
        assert "✓" in out or "PASS" in out
        assert "my_test" in out
        assert "42ms" in out

    def test_fail_shows_x_name_duration(self):
        r = TestResult(name="bad_test", passed=False, duration_ms=7.0,
                       comparison_result=ComparisonResult(passed=False, message="row count mismatch"))
        out = render_test_results([r], 0, False)
        assert "✗" in out or "FAIL" in out
        assert "bad_test" in out
        assert "7ms" in out

    def test_fail_shows_comparison_message(self):
        r = TestResult(name="t", passed=False, duration_ms=1.0,
                       comparison_result=ComparisonResult(passed=False, message="expected 3 rows, got 1"))
        out = render_test_results([r], 0, False)
        assert "expected 3 rows, got 1" in out

    def test_schema_mismatch_message(self):
        r = TestResult(name="t", passed=False, duration_ms=1.0,
                       comparison_result=ComparisonResult(
                           passed=False,
                           message="schema mismatch: expected columns ['id', 'name'], got ['id']",
                       ))
        out = render_test_results([r], 0, False)
        assert "schema mismatch" in out

    def test_row_count_mismatch_message(self):
        r = TestResult(name="t", passed=False, duration_ms=1.0,
                       comparison_result=ComparisonResult(
                           passed=False,
                           message="row count mismatch: expected 5 rows, got 3",
                       ))
        out = render_test_results([r], 0, False)
        assert "row count mismatch" in out
        assert "expected 5 rows, got 3" in out

    def test_value_mismatch_diff_rendered(self):
        diff = [
            {"row": 0, "column": "amount", "expected": 100, "actual": 99},
            {"row": 1, "column": "amount", "expected": 200, "actual": 201},
        ]
        r = TestResult(name="t", passed=False, duration_ms=1.0,
                       comparison_result=ComparisonResult(
                           passed=False, message="value mismatch", diff=diff))
        out = render_test_results([r], 0, False)
        assert "amount" in out
        assert "100" in out
        assert "99" in out

    def test_diff_capped_at_5_rows(self):
        diff = [{"row": i, "column": "x", "expected": i, "actual": i + 1} for i in range(10)]
        r = TestResult(name="t", passed=False, duration_ms=1.0,
                       comparison_result=ComparisonResult(passed=False, message="mismatch", diff=diff))
        out = render_test_results([r], 0, False)
        # Only first 5 rows rendered
        assert out.count("expected") <= 6  # 5 diff rows + 1 in summary message

    def test_error_shown_for_execution_error(self):
        r = TestResult(name="t", passed=False, duration_ms=1.0, error="RVT-910: joint failed")
        out = render_test_results([r], 0, False)
        assert "RVT-910" in out

    def test_summary_pass_fail_counts(self):
        results = [
            TestResult(name="a", passed=True, duration_ms=5.0),
            TestResult(name="b", passed=False, duration_ms=3.0,
                       comparison_result=ComparisonResult(passed=False, message="fail")),
            TestResult(name="c", passed=True, duration_ms=2.0),
        ]
        out = render_test_results(results, 0, False)
        assert "2 passed" in out
        assert "1 failed" in out
        assert "3 total" in out

    def test_summary_total_duration(self):
        results = [
            TestResult(name="a", passed=True, duration_ms=10.0),
            TestResult(name="b", passed=True, duration_ms=20.0),
        ]
        out = render_test_results(results, 0, False)
        assert "30ms" in out

    def test_no_ansi_when_color_false(self):
        results = [
            TestResult(name="a", passed=True, duration_ms=1.0),
            TestResult(name="b", passed=False, duration_ms=1.0,
                       comparison_result=ComparisonResult(passed=False, message="fail")),
        ]
        out = render_test_results(results, 0, False)
        assert not ANSI_RE.search(out)

    def test_color_enabled(self):
        r = TestResult(name="t", passed=True, duration_ms=1.0)
        out = render_test_results([r], 0, True)
        assert GREEN in out

    def test_fail_color_red(self):
        r = TestResult(name="t", passed=False, duration_ms=1.0,
                       comparison_result=ComparisonResult(passed=False, message="x"))
        out = render_test_results([r], 0, True)
        assert RED in out


# ---------------------------------------------------------------------------
# Tests for render_test_results_json
# ---------------------------------------------------------------------------


class TestRenderTestResultsJson:
    def test_json_pass(self):
        r = TestResult(name="t", passed=True, duration_ms=5.0)
        data = json.loads(render_test_results_json([r]))
        assert len(data) == 1
        assert data[0]["name"] == "t"
        assert data[0]["passed"] is True
        assert data[0]["duration_ms"] == 5.0

    def test_json_fail_with_comparison(self):
        cr = ComparisonResult(passed=False, message="row count mismatch: expected 3, got 1",
                              diff=[{"row": 0, "column": "id", "expected": 1, "actual": 2}])
        r = TestResult(name="fail_test", passed=False, duration_ms=2.0, comparison_result=cr)
        data = json.loads(render_test_results_json([r]))
        assert data[0]["passed"] is False
        assert data[0]["comparison_result"]["message"] == "row count mismatch: expected 3, got 1"
        assert data[0]["comparison_result"]["diff"][0]["column"] == "id"

    def test_json_error(self):
        r = TestResult(name="t", passed=False, duration_ms=1.0, error="RVT-910: failed")
        data = json.loads(render_test_results_json([r]))
        assert data[0]["error"] == "RVT-910: failed"

    def test_json_multiple_results(self):
        results = [
            TestResult(name="a", passed=True, duration_ms=1.0),
            TestResult(name="b", passed=False, duration_ms=2.0,
                       comparison_result=ComparisonResult(passed=False, message="fail")),
        ]
        data = json.loads(render_test_results_json(results))
        assert len(data) == 2
        assert data[0]["name"] == "a"
        assert data[1]["name"] == "b"

    def test_json_is_valid_json(self):
        results = [TestResult(name="t", passed=True, duration_ms=1.0)]
        out = render_test_results_json(results)
        parsed = json.loads(out)
        assert isinstance(parsed, list)
