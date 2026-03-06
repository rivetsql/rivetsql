"""Editor state persistence for the Rivet REPL.

Persists ad-hoc tab contents, cursor positions, and tab order to
~/.cache/rivet/repl/editor_cache.json and restores them on REPL restart.

On corruption (invalid JSON, version mismatch), the cache file is deleted
and a fresh empty state is returned (RVT-862).

Requirements: 33.1, 34.2
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

_CACHE_DIR = Path.home() / ".cache" / "rivet" / "repl"
_CACHE_FILE = _CACHE_DIR / "editor_cache.json"
_SCHEMA_VERSION = 1


@dataclass
class CachedTab:
    """Persisted state for a single ad-hoc editor tab."""

    title: str
    content: str
    cursor_row: int = 0
    cursor_col: int = 0


@dataclass
class EditorCacheState:
    """Full editor cache: ordered list of ad-hoc tabs and the active tab index."""

    tabs: list[CachedTab]
    active_index: int = 0


def load_editor_cache() -> EditorCacheState:
    """Load editor cache from disk.

    Returns an empty EditorCacheState if the file does not exist.
    On corruption, deletes the file and returns an empty state (RVT-862).
    """
    try:
        raw = json.loads(_CACHE_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return EditorCacheState(tabs=[])
    except Exception:
        logger.debug("Editor cache unreadable; deleting (RVT-862)", exc_info=True)
        _delete_cache()
        return EditorCacheState(tabs=[])

    try:
        if not isinstance(raw, dict):
            raise ValueError("root must be a dict")
        if raw.get("version") != _SCHEMA_VERSION:
            raise ValueError(f"unsupported version: {raw.get('version')!r}")

        tabs = [
            CachedTab(
                title=str(t["title"]),
                content=str(t["content"]),
                cursor_row=int(t.get("cursor_row", 0)),
                cursor_col=int(t.get("cursor_col", 0)),
            )
            for t in raw.get("tabs", [])
        ]
        active_index = int(raw.get("active_index", 0))
        # Clamp to valid range
        if tabs:
            active_index = max(0, min(active_index, len(tabs) - 1))
        else:
            active_index = 0
        return EditorCacheState(tabs=tabs, active_index=active_index)
    except Exception:
        logger.debug("Editor cache corrupt; deleting (RVT-862)", exc_info=True)
        _delete_cache()
        return EditorCacheState(tabs=[])


def save_editor_cache(state: EditorCacheState) -> None:
    """Persist editor cache to disk."""
    payload = {
        "version": _SCHEMA_VERSION,
        "active_index": state.active_index,
        "tabs": [asdict(t) for t in state.tabs],
    }
    try:
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        _CACHE_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception:
        logger.debug("Failed to write editor cache", exc_info=True)


def _delete_cache() -> None:
    try:
        _CACHE_FILE.unlink(missing_ok=True)
    except Exception:
        logger.debug("Failed to delete corrupt editor cache", exc_info=True)
