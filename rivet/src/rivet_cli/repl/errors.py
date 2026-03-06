"""REPL error code registry (RVT-860 through RVT-867).

Requirements: 34.1, 34.2
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ReplError:
    """A REPL-layer error with code, description, and actionable remediation."""

    code: str
    description: str
    remediation: str

    def format_text(self) -> str:
        """Return a single-line text representation for copy/display."""
        return f"[{self.code}] {self.description} — {self.remediation}"


# ---------------------------------------------------------------------------
# Error code constants
# ---------------------------------------------------------------------------

RVT_860 = "RVT-860"  # TUI initialization failure
RVT_861 = "RVT-861"  # File watcher error
RVT_862 = "RVT-862"  # Editor cache corruption
RVT_863 = "RVT-863"  # Theme not found
RVT_864 = "RVT-864"  # Keymap not found or invalid
RVT_865 = "RVT-865"  # Export failed
RVT_866 = "RVT-866"  # Debug mode error
RVT_867 = "RVT-867"  # Profile switch failed


# ---------------------------------------------------------------------------
# Registry — maps code → (description, remediation)
# ---------------------------------------------------------------------------

REPL_ERROR_REGISTRY: dict[str, tuple[str, str]] = {
    RVT_860: (
        "TUI initialization failure",
        "Check that your terminal supports Textual (256-color, UTF-8). Run with a supported terminal.",
    ),
    RVT_861: (
        "File watcher error",
        "Check file permissions and inotify limits. The REPL will auto-retry.",
    ),
    RVT_862: (
        "Editor cache corruption",
        "The editor cache was deleted and reset. Your SQL files are unaffected.",
    ),
    RVT_863: (
        "Theme not found",
        "The requested theme was not found. Falling back to the default 'rivet' theme.",
    ),
    RVT_864: (
        "Keymap not found or invalid",
        "The requested keymap was not found or has a binding conflict. Falling back to the default keymap.",
    ),
    RVT_865: (
        "Export failed",
        "Check the output path, available disk space, and that the format is supported.",
    ),
    RVT_866: (
        "Debug mode error",
        "The breakpoint references a joint that does not exist. Check the joint name and try again.",
    ),
    RVT_867: (
        "Profile switch failed",
        "Could not connect to the catalog for the selected profile. Remaining on the current profile.",
    ),
}


def make_repl_error(code: str, detail: str | None = None) -> ReplError:
    """Construct a ReplError from a registry code, optionally appending detail."""
    description, remediation = REPL_ERROR_REGISTRY[code]
    if detail:
        description = f"{description}: {detail}"
    return ReplError(code=code, description=description, remediation=remediation)
