"""ANSI color utilities and symbols for rivet-cli."""

GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
BLUE = "\033[34m"
DIM = "\033[2m"
CYAN = "\033[36m"
MAGENTA = "\033[35m"
BOLD = "\033[1m"
RESET = "\033[0m"

SYM_CHECK = "✓"
SYM_WARN = "⚠"
SYM_ERROR = "✗"
SYM_ASSERT = "◆"
SYM_AUDIT = "●"
SYM_MATERIALIZE = "⚡"
SYM_NOT_APPLICABLE = "·"


def colorize(text: str, color: str, enabled: bool) -> str:
    """Wrap text in ANSI color codes if enabled."""
    if not enabled:
        return text
    return f"{color}{text}{RESET}"
