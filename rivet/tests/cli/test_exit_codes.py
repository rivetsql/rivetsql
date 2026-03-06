"""Unit tests for exit codes and resolve_exit_code (Property 5)."""

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_cli.exit_codes import (
    ASSERTION_FAILURE,
    AUDIT_FAILURE,
    GENERAL_ERROR,
    INTERRUPTED,
    PARTIAL_FAILURE,
    SUCCESS,
    TEST_FAILURE,
    USAGE_ERROR,
    resolve_exit_code,
)


class TestExitCodeConstants:
    def test_values(self) -> None:
        assert SUCCESS == 0
        assert GENERAL_ERROR == 1
        assert PARTIAL_FAILURE == 2
        assert TEST_FAILURE == 3
        assert ASSERTION_FAILURE == 4
        assert AUDIT_FAILURE == 5
        assert USAGE_ERROR == 10
        assert INTERRUPTED == 130


class TestResolveExitCode:
    def test_no_failures(self) -> None:
        assert resolve_exit_code(False, False, False) == SUCCESS

    def test_partial_only(self) -> None:
        assert resolve_exit_code(False, False, True) == PARTIAL_FAILURE

    def test_audit_only(self) -> None:
        assert resolve_exit_code(False, True, False) == AUDIT_FAILURE

    def test_assertion_only(self) -> None:
        assert resolve_exit_code(True, False, False) == ASSERTION_FAILURE

    def test_assertion_beats_audit(self) -> None:
        assert resolve_exit_code(True, True, False) == ASSERTION_FAILURE

    def test_assertion_beats_partial(self) -> None:
        assert resolve_exit_code(True, False, True) == ASSERTION_FAILURE

    def test_assertion_beats_all(self) -> None:
        assert resolve_exit_code(True, True, True) == ASSERTION_FAILURE

    def test_audit_beats_partial(self) -> None:
        assert resolve_exit_code(False, True, True) == AUDIT_FAILURE

    @given(
        has_assertion=st.booleans(),
        has_audit=st.booleans(),
        has_partial=st.booleans(),
    )
    @settings(max_examples=100)
    def test_property5_priority(
        self, has_assertion: bool, has_audit: bool, has_partial: bool
    ) -> None:
        """Property 5: assertion > audit > partial > success."""
        result = resolve_exit_code(has_assertion, has_audit, has_partial)
        if has_assertion:
            assert result == ASSERTION_FAILURE
        elif has_audit:
            assert result == AUDIT_FAILURE
        elif has_partial:
            assert result == PARTIAL_FAILURE
        else:
            assert result == SUCCESS
