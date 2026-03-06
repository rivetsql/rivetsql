"""Theme loader for the Rivet REPL.

Built-in themes (Requirements 27.1):
  rivet            — default dark
  rivet-light      — light
  rivet-high-contrast — WCAG AAA, no color-only indicators, bold borders

Custom themes (Requirements 27.3):
  Specified via repl.theme and repl.theme_path in rivet.yaml or
  ~/.config/rivet/config.yaml as a path to a Textual CSS (.tcss) file.

Fallback (Requirements 27.4):
  If a specified theme is not found, logs RVT-863 and returns the default
  rivet theme path.
"""

from __future__ import annotations

import logging
from pathlib import Path

_logger = logging.getLogger(__name__)

_THEMES_DIR = Path(__file__).parent

# Map of built-in theme names to their .tcss file paths.
BUILTIN_THEMES: dict[str, Path] = {
    "rivet": _THEMES_DIR / "rivet.tcss",
    "rivet-light": _THEMES_DIR / "rivet_light.tcss",
    "rivet-high-contrast": _THEMES_DIR / "high_contrast.tcss",
}

_DEFAULT_THEME = "rivet"


def resolve_theme(theme_name: str, theme_path: str | None = None) -> Path:
    """Return the Path to the .tcss file for the requested theme.

    Resolution order:
    1. If theme_path is provided and the file exists, use it directly.
    2. If theme_name matches a built-in theme, use that.
    3. Otherwise log RVT-863 and fall back to the default 'rivet' theme.

    Args:
        theme_name: Theme name from config (e.g. "rivet", "rivet-light").
        theme_path: Optional explicit path to a custom .tcss file.

    Returns:
        Path to the resolved .tcss file (always valid).
    """
    # 1. Explicit custom path takes priority.
    if theme_path:
        custom = Path(theme_path)
        if custom.is_file():
            return custom
        _logger.error(
            "RVT-863: custom theme file not found: %s — falling back to default theme '%s'",
            theme_path,
            _DEFAULT_THEME,
        )
        return BUILTIN_THEMES[_DEFAULT_THEME]

    # 2. Built-in theme lookup.
    if theme_name in BUILTIN_THEMES:
        return BUILTIN_THEMES[theme_name]

    # 3. Unknown theme name — RVT-863 fallback.
    _logger.error(
        "RVT-863: theme '%s' not found — falling back to default theme '%s'",
        theme_name,
        _DEFAULT_THEME,
    )
    return BUILTIN_THEMES[_DEFAULT_THEME]
