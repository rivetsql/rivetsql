"""History serialization/deserialization for the interactive REPL session.

Persists QueryHistoryEntry list to ~/.cache/rivet/repl/history.json,
keyed by project path. Caps at 1,000 entries per project, retaining most recent.

Requirements: 21.1, 21.3
"""

from __future__ import annotations

import json
import logging
import tempfile
from datetime import datetime
from pathlib import Path

from rivet_core.interactive.types import QueryHistoryEntry

logger = logging.getLogger(__name__)

HISTORY_CAP = 1_000
_HISTORY_DIR = Path.home() / ".cache" / "rivet" / "repl"
_HISTORY_FILE = _HISTORY_DIR / "history.json"

_TEMP_DIR = Path(tempfile.gettempdir()).resolve()


def _is_ephemeral(project_path: Path) -> bool:
    """Return True for temp/ephemeral directories that should not persist history."""
    try:
        resolved = str(project_path.resolve())
    except OSError:
        return True
    return str(_TEMP_DIR) in resolved


def _project_key(project_path: Path) -> str:
    return str(project_path.resolve())


def load_history(project_path: Path) -> list[QueryHistoryEntry]:
    """Load history entries for the given project from disk.

    Returns an empty list if the file does not exist or is corrupt.
    """
    try:
        raw = json.loads(_HISTORY_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return []
    except Exception:
        logger.debug("Failed to read history file", exc_info=True)
        return []

    key = _project_key(project_path)
    entries_raw = raw.get(key, [])
    entries: list[QueryHistoryEntry] = []
    for item in entries_raw:
        try:
            entries.append(
                QueryHistoryEntry(
                    timestamp=datetime.fromisoformat(item["timestamp"]),
                    action_type=item["action_type"],
                    name=item["name"],
                    row_count=item.get("row_count"),
                    duration_ms=float(item["duration_ms"]),
                    status=item["status"],
                )
            )
        except Exception:
            logger.debug("Skipping malformed history entry", exc_info=True)
    return entries


def save_history(project_path: Path, entries: list[QueryHistoryEntry]) -> None:
    """Save history entries for the given project to disk.

    Caps at HISTORY_CAP entries, retaining the most recent.
    Merges with any existing entries for other projects.
    Skips persistence for ephemeral/temp directories (e.g. pytest).
    """
    if _is_ephemeral(project_path):
        return
    # Load existing data for all projects
    try:
        existing = json.loads(_HISTORY_FILE.read_text(encoding="utf-8"))
        if not isinstance(existing, dict):
            existing = {}
    except FileNotFoundError:
        existing = {}
    except Exception:
        logger.debug("Failed to read history file for merge", exc_info=True)
        existing = {}

    key = _project_key(project_path)
    capped = entries[-HISTORY_CAP:] if len(entries) > HISTORY_CAP else entries
    existing[key] = [
        {
            "timestamp": e.timestamp.isoformat(),
            "action_type": e.action_type,
            "name": e.name,
            "row_count": e.row_count,
            "duration_ms": e.duration_ms,
            "status": e.status,
        }
        for e in capped
    ]

    try:
        _HISTORY_DIR.mkdir(parents=True, exist_ok=True)
        _HISTORY_FILE.write_text(json.dumps(existing, indent=2), encoding="utf-8")
    except Exception:
        logger.debug("Failed to write history file", exc_info=True)
