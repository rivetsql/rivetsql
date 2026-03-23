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

# Joint type icons
ICON_SOURCE = "📥"
ICON_TRANSFORM = "🔧"
ICON_SINK = "📤"
ICON_CHECKPOINT = "🔒"

_JOINT_ICON_MAP: dict[str, str] = {
    "source": ICON_SOURCE,
    "sql": ICON_TRANSFORM,
    "python": ICON_TRANSFORM,
    "sink": ICON_SINK,
    "checkpoint": ICON_CHECKPOINT,
}


def joint_icon(joint_type: str) -> str:
    """Map a joint type string to its corresponding icon."""
    return _JOINT_ICON_MAP.get(joint_type, ICON_TRANSFORM)


def colorize(text: str, color: str, enabled: bool) -> str:
    """Wrap text in ANSI color codes if enabled."""
    if not enabled:
        return text
    return f"{color}{text}{RESET}"
