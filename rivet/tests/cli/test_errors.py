"""Unit tests for CLI error types and formatting."""

import dataclasses

import pytest

from rivet_cli.errors import (
    RVT_850,
    RVT_851,
    RVT_852,
    RVT_853,
    RVT_854,
    RVT_855,
    RVT_856,
    CLIError,
    format_cli_error,
    format_upstream_error,
)


class TestCLIError:
    def test_field_access(self) -> None:
        err = CLIError(code="RVT-850", message="not found", remediation="run init")
        assert err.code == "RVT-850"
        assert err.message == "not found"
        assert err.remediation == "run init"

    def test_immutability(self) -> None:
        err = CLIError(code="RVT-850", message="msg", remediation="fix")
        with pytest.raises(dataclasses.FrozenInstanceError):
            err.code = "RVT-999"  # type: ignore[misc]


class TestErrorCodeConstants:
    def test_constants(self) -> None:
        assert RVT_850 == "RVT-850"
        assert RVT_851 == "RVT-851"
        assert RVT_852 == "RVT-852"
        assert RVT_853 == "RVT-853"
        assert RVT_854 == "RVT-854"
        assert RVT_855 == "RVT-855"
        assert RVT_856 == "RVT-856"


class TestFormatCLIError:
    def test_with_color(self) -> None:
        err = CLIError(code="RVT-850", message="rivet.yaml not found", remediation="Run 'rivet init'")
        result = format_cli_error(err, color=True)
        assert "[RVT-850]" in result
        assert "rivet.yaml not found" in result
        assert "→ Run 'rivet init'" in result
        assert "\033[31m" in result  # RED prefix

    def test_without_color(self) -> None:
        err = CLIError(code="RVT-850", message="rivet.yaml not found", remediation="Run 'rivet init'")
        result = format_cli_error(err, color=False)
        assert "[RVT-850]" in result
        assert "rivet.yaml not found" in result
        assert "→ Run 'rivet init'" in result
        assert "\033[" not in result

    def test_no_remediation(self) -> None:
        err = CLIError(code="RVT-851", message="Unknown command", remediation="")
        result = format_cli_error(err, color=False)
        assert "[RVT-851]" in result
        assert "→" not in result


class TestFormatUpstreamError:
    def test_delegates_to_format_cli_error(self) -> None:
        result = format_upstream_error("BRG-100", "bridge error", "check config", color=False)
        assert "[BRG-100]" in result
        assert "bridge error" in result
        assert "→ check config" in result

    def test_none_remediation(self) -> None:
        result = format_upstream_error("RVT-100", "core error", None, color=False)
        assert "[RVT-100]" in result
        assert "→" not in result
