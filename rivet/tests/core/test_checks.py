"""Tests for checks.py — Assertion, AssertionResult, CompiledCheck."""

from __future__ import annotations

import pytest

from rivet_core.checks import CHECK_TYPES, Assertion, AssertionResult, CompiledCheck


class TestAssertion:
    def test_defaults(self) -> None:
        a = Assertion(type="not_null")
        assert a.severity == "error"
        assert a.config == {}
        assert a.phase == "assertion"

    def test_all_nine_check_types(self) -> None:
        for check_type in CHECK_TYPES:
            a = Assertion(type=check_type)
            assert a.type == check_type

    def test_warning_severity(self) -> None:
        a = Assertion(type="unique", severity="warning")
        assert a.severity == "warning"

    def test_audit_phase(self) -> None:
        a = Assertion(type="row_count", phase="audit")
        assert a.phase == "audit"

    def test_config_stored(self) -> None:
        a = Assertion(type="accepted_values", config={"values": [1, 2, 3]})
        assert a.config == {"values": [1, 2, 3]}

    def test_frozen(self) -> None:
        a = Assertion(type="not_null")
        with pytest.raises(Exception):  # noqa: B017
            a.type = "unique"  # type: ignore[misc]

    def test_invalid_type_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid check type"):
            Assertion(type="nonexistent")

    def test_invalid_severity_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid severity"):
            Assertion(type="not_null", severity="critical")

    def test_invalid_phase_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid phase"):
            Assertion(type="not_null", phase="pre_write")


class TestAssertionResult:
    def test_passed(self) -> None:
        r = AssertionResult(passed=True, message="All good")
        assert r.passed is True
        assert r.message == "All good"
        assert r.details is None
        assert r.failing_rows is None

    def test_failed_with_details(self) -> None:
        r = AssertionResult(passed=False, message="3 nulls found", details={"column": "id"}, failing_rows=3)
        assert r.passed is False
        assert r.failing_rows == 3
        assert r.details == {"column": "id"}

    def test_frozen(self) -> None:
        r = AssertionResult(passed=True, message="ok")
        with pytest.raises(Exception):  # noqa: B017
            r.passed = False  # type: ignore[misc]


class TestCompiledCheck:
    def test_fields(self) -> None:
        c = CompiledCheck(type="not_null", severity="error", config={"column": "id"}, phase="assertion")
        assert c.type == "not_null"
        assert c.severity == "error"
        assert c.config == {"column": "id"}
        assert c.phase == "assertion"

    def test_audit_phase(self) -> None:
        c = CompiledCheck(type="row_count", severity="warning", config={}, phase="audit")
        assert c.phase == "audit"

    def test_frozen(self) -> None:
        c = CompiledCheck(type="unique", severity="error", config={}, phase="assertion")
        with pytest.raises(Exception):  # noqa: B017
            c.type = "not_null"  # type: ignore[misc]
