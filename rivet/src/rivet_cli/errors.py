"""CLI error codes (RVT-850 through RVT-899) and formatting."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CLIError:
    """A CLI-layer error with code and actionable message."""

    code: str
    message: str
    remediation: str


# Error code constants
RVT_850 = "RVT-850"  # rivet.yaml not found
RVT_851 = "RVT-851"  # Unknown command or subcommand
RVT_852 = "RVT-852"  # Invalid flag or option value
RVT_853 = "RVT-853"  # Profile not found
RVT_854 = "RVT-854"  # Tag filter matched no joints
RVT_855 = "RVT-855"  # Project directory not initialized
RVT_856 = "RVT-856"  # Output format not supported for command
RVT_880 = "RVT-880"  # Target profile not found
RVT_881 = "RVT-881"  # Missing required options in non-interactive mode
RVT_882 = "RVT-882"  # No catalog plugins discovered
RVT_883 = "RVT-883"  # Failed to write catalog to profile (permissions, disk)
RVT_884 = "RVT-884"  # Catalog name invalid or conflicts with existing
RVT_885 = "RVT-885"  # Plugin validation failed
RVT_886 = "RVT-886"  # Connection test failed or timed out
RVT_857 = "RVT-857"  # Target directory is not empty
RVT_890 = "RVT-890"  # Engine not found
RVT_891 = "RVT-891"  # Engine name invalid or conflicts with existing


def format_cli_error(error: CLIError, color: bool) -> str:
    """Format a CLI error for stderr display."""
    prefix = "rivet | ERROR"
    if color:
        prefix = f"\033[31m{prefix}\033[0m"
    line1 = f"{prefix} [{error.code}] {error.message}"
    if error.remediation:
        return f"{line1}\nrivet |   → {error.remediation}"
    return line1


def format_cli_warning(message: str, remediation: str | None, color: bool) -> str:
    """Format a warning for stderr display."""
    prefix = "rivet | WARN"
    if color:
        prefix = f"\033[33m{prefix}\033[0m"
    line1 = f"{prefix} {message}"
    if remediation:
        return f"{line1}\nrivet |   → {remediation}"
    return line1


def format_upstream_error(
    code: str, message: str, remediation: str | None, color: bool
) -> str:
    """Format an upstream (config/bridge/core) error for stderr display."""
    return format_cli_error(CLIError(code=code, message=message, remediation=remediation or ""), color)
