"""Catalog tree cache persistence for the REPL.

Caches introspected catalog trees to ~/.cache/rivet/repl/catalog_cache.json,
keyed by profile + catalog name + connection hash. Used as the initial tree
on startup before live introspection completes.

Requirements: 33.2, 33.4
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_CACHE_DIR = Path.home() / ".cache" / "rivet" / "repl"
_CACHE_FILE = _CACHE_DIR / "catalog_cache.json"


def _connection_hash(catalog_options: dict[str, Any]) -> str:
    """Compute a stable hash of catalog connection options."""
    serialized = json.dumps(catalog_options, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def _cache_key(profile: str, catalog_name: str, connection_hash: str) -> str:
    return f"{profile}:{catalog_name}:{connection_hash}"


def load_catalog_cache(
    profile: str,
    catalog_name: str,
    catalog_options: dict[str, Any],
) -> list[dict[str, Any]] | None:
    """Load cached catalog tree nodes for the given profile + catalog.

    Returns the cached node list, or None if not found or corrupt.
    """
    try:
        raw = json.loads(_CACHE_FILE.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except Exception:
        logger.debug("Failed to read catalog cache file", exc_info=True)
        return None

    key = _cache_key(profile, catalog_name, _connection_hash(catalog_options))
    entry = raw.get(key)
    if not isinstance(entry, list):
        return None
    return entry


def save_catalog_cache(
    profile: str,
    catalog_name: str,
    catalog_options: dict[str, Any],
    nodes: list[dict[str, Any]],
) -> None:
    """Save catalog tree nodes for the given profile + catalog to disk."""
    try:
        existing = json.loads(_CACHE_FILE.read_text(encoding="utf-8"))
        if not isinstance(existing, dict):
            existing = {}
    except FileNotFoundError:
        existing = {}
    except Exception:
        logger.debug("Failed to read catalog cache for merge", exc_info=True)
        existing = {}

    key = _cache_key(profile, catalog_name, _connection_hash(catalog_options))
    existing[key] = nodes

    try:
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        _CACHE_FILE.write_text(json.dumps(existing, indent=2), encoding="utf-8")
    except Exception:
        logger.debug("Failed to write catalog cache file", exc_info=True)


def invalidate_catalog_cache(
    profile: str,
    catalog_name: str,
    catalog_options: dict[str, Any],
) -> None:
    """Invalidate the cache entry for a specific profile + catalog."""
    try:
        raw = json.loads(_CACHE_FILE.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return
    except FileNotFoundError:
        return
    except Exception:
        logger.debug("Failed to read catalog cache for invalidation", exc_info=True)
        return

    key = _cache_key(profile, catalog_name, _connection_hash(catalog_options))
    if key in raw:
        del raw[key]
        try:
            _CACHE_FILE.write_text(json.dumps(raw, indent=2), encoding="utf-8")
        except Exception:
            logger.debug("Failed to write catalog cache after invalidation", exc_info=True)


def invalidate_profile_cache(profile: str) -> None:
    """Invalidate all cache entries for a given profile (e.g., on profile switch)."""
    try:
        raw = json.loads(_CACHE_FILE.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return
    except FileNotFoundError:
        return
    except Exception:
        logger.debug("Failed to read catalog cache for profile invalidation", exc_info=True)
        return

    prefix = f"{profile}:"
    keys_to_remove = [k for k in raw if k.startswith(prefix)]
    if not keys_to_remove:
        return
    for k in keys_to_remove:
        del raw[k]
    try:
        _CACHE_FILE.write_text(json.dumps(raw, indent=2), encoding="utf-8")
    except Exception:
        logger.debug("Failed to write catalog cache after profile invalidation", exc_info=True)
