"""Unified persistent cache for catalog metadata.

Stores catalog tree nodes, schemas, and metadata on disk under
``~/.cache/rivet/catalog/``, with one JSON file per catalog per profile.
Integrates transparently with ``CatalogExplorer`` via an optional
constructor parameter.

SmartCache is a pure cache — it does not call plugins or perform
staleness checks.  It reports whether entries are expired and lets
the caller (``CatalogExplorer``) decide what to do.

Flush policy: dirty catalog files are flushed to disk at most once
per ``flush_interval`` seconds after a write.  Callers should also
call ``flush()`` on teardown for durability.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_CACHE_DIR = Path.home() / ".cache" / "rivet" / "catalog"
_DISK_VERSION = 1


class CacheMode(Enum):
    """Controls how ``CatalogExplorer`` interacts with ``SmartCache``."""

    READ_WRITE = "read_write"
    WRITE_ONLY = "write_only"


@dataclass
class CacheEntry:
    """A single cached item with metadata."""

    data: Any
    fingerprint: str | None
    created_at: float
    last_accessed: float
    ttl: float
    entry_type: str


@dataclass
class CacheResult:
    """Returned by :meth:`SmartCache.get` — includes data and expiry status."""

    data: Any
    fingerprint: str | None
    expired: bool


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _entry_key(entry_type: str, path: tuple[str, ...]) -> str:
    """Build the flat string key used inside a catalog's ``entries`` dict."""
    return f"{entry_type}::{'.'.join(path)}"


def _serialize_entry(entry: CacheEntry) -> dict[str, Any]:
    """Serialize a ``CacheEntry`` to a JSON-compatible dict."""
    return asdict(entry)


def _deserialize_entry(d: dict[str, Any]) -> CacheEntry:
    """Deserialize a dict back into a ``CacheEntry``."""
    return CacheEntry(
        data=d["data"],
        fingerprint=d.get("fingerprint"),
        created_at=d["created_at"],
        last_accessed=d["last_accessed"],
        ttl=d["ttl"],
        entry_type=d["entry_type"],
    )


# ---------------------------------------------------------------------------
# SmartCache
# ---------------------------------------------------------------------------


class SmartCache:
    """Unified persistent cache for catalog metadata.

    Stores one JSON file per catalog per profile under *cache_dir*::

        {cache_dir}/{profile}/{catalog_name}_{connection_hash}.json

    SmartCache is a pure cache — it does not call plugins or perform
    staleness checks.  It reports whether entries are expired and lets
    the caller (``CatalogExplorer``) decide what to do.

    Parameters
    ----------
    profile:
        Active Rivet profile name.
    cache_dir:
        Root directory for cache files.  Defaults to
        ``~/.cache/rivet/catalog``.
    default_ttl:
        Default time-to-live in seconds for new entries.
    max_size_bytes:
        Maximum total serialized size across all catalogs before LRU
        eviction kicks in.
    flush_interval:
        Minimum seconds between automatic (debounced) flushes.
    """

    def __init__(
        self,
        profile: str,
        cache_dir: Path | None = None,
        default_ttl: float = 300.0,
        max_size_bytes: int = 50 * 1024 * 1024,
        flush_interval: float = 5.0,
    ) -> None:
        self._profile = profile
        self._cache_dir = (cache_dir or _DEFAULT_CACHE_DIR) / profile
        self._default_ttl = default_ttl
        self._max_size_bytes = max_size_bytes
        self._flush_interval = flush_interval

        # catalog file key → {entry_key → CacheEntry}
        self._catalogs: dict[str, dict[str, CacheEntry]] = {}
        # catalog file key → (catalog_name, connection_hash)
        self._catalog_meta: dict[str, tuple[str, str]] = {}
        # catalog file keys that have unsaved changes
        self._dirty: set[str] = set()
        # timestamp of last flush
        self._last_flush_time: float = 0.0

        self._load_from_disk()

    # ------------------------------------------------------------------
    # Disk I/O
    # ------------------------------------------------------------------

    @staticmethod
    def _file_key(catalog_name: str, connection_hash: str) -> str:
        """Return the key used to identify a catalog file in memory."""
        return f"{catalog_name}_{connection_hash}"

    def _file_path(self, file_key: str) -> Path:
        return self._cache_dir / f"{file_key}.json"

    def _load_from_disk(self) -> None:
        """Load all per-catalog JSON files for this profile from disk."""
        if not self._cache_dir.is_dir():
            return
        for fp in self._cache_dir.iterdir():
            if fp.suffix != ".json":
                continue
            try:
                raw = json.loads(fp.read_text(encoding="utf-8"))
                if not isinstance(raw, dict) or raw.get("version") != _DISK_VERSION:
                    logger.warning("Discarding cache file %s: unsupported version or format", fp)
                    fp.unlink(missing_ok=True)
                    continue
                catalog_name = raw["catalog_name"]
                connection_hash = raw["connection_hash"]
                fk = self._file_key(catalog_name, connection_hash)
                entries: dict[str, CacheEntry] = {}
                for ek, ed in raw.get("entries", {}).items():
                    try:
                        entries[ek] = _deserialize_entry(ed)
                    except Exception:
                        logger.warning("Discarding corrupted entry %s in %s", ek, fp)
                self._catalogs[fk] = entries
                self._catalog_meta[fk] = (catalog_name, connection_hash)
            except Exception:
                logger.warning("Discarding corrupted cache file %s", fp, exc_info=True)
                try:
                    fp.unlink(missing_ok=True)
                except OSError:
                    pass

    def _serialize_catalog(self, file_key: str) -> str:
        """Serialize a single catalog's entries to a JSON string."""
        catalog_name, connection_hash = self._catalog_meta[file_key]
        entries_raw: dict[str, Any] = {}
        for ek, entry in self._catalogs[file_key].items():
            entries_raw[ek] = _serialize_entry(entry)
        doc = {
            "version": _DISK_VERSION,
            "catalog_name": catalog_name,
            "connection_hash": connection_hash,
            "entries": entries_raw,
        }
        return json.dumps(doc, indent=2, default=str)

    def _write_catalog_file(self, file_key: str) -> None:
        """Write a single catalog file to disk."""
        try:
            self._cache_dir.mkdir(parents=True, exist_ok=True)
            self._file_path(file_key).write_text(
                self._serialize_catalog(file_key), encoding="utf-8"
            )
        except Exception:
            logger.warning(
                "Failed to write cache file %s",
                self._file_path(file_key),
                exc_info=True,
            )

    def _maybe_debounced_flush(self) -> None:
        """Flush dirty files if enough time has elapsed since the last flush."""
        now = time.monotonic()
        if now - self._last_flush_time >= self._flush_interval:
            self.flush()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(
        self,
        entry_type: str,
        catalog_name: str,
        connection_hash: str,
        path: tuple[str, ...],
    ) -> CacheResult | None:
        """Return cached data with expiry status, or ``None`` on miss.

        Updates ``last_accessed`` on hit.  Does **not** perform staleness
        checks or call plugins — that is the caller's responsibility.
        """
        fk = self._file_key(catalog_name, connection_hash)
        catalog_entries = self._catalogs.get(fk)
        if catalog_entries is None:
            return None
        ek = _entry_key(entry_type, path)
        entry = catalog_entries.get(ek)
        if entry is None:
            return None
        now = time.time()
        entry.last_accessed = now
        expired = (now - entry.created_at) >= entry.ttl
        return CacheResult(data=entry.data, fingerprint=entry.fingerprint, expired=expired)

    def put(
        self,
        entry_type: str,
        catalog_name: str,
        connection_hash: str,
        path: tuple[str, ...],
        data: Any,
        fingerprint: str | None = None,
    ) -> None:
        """Store or update a cache entry.  Marks the catalog file as dirty."""
        fk = self._file_key(catalog_name, connection_hash)
        if fk not in self._catalogs:
            self._catalogs[fk] = {}
            self._catalog_meta[fk] = (catalog_name, connection_hash)
        ek = _entry_key(entry_type, path)
        now = time.time()
        self._catalogs[fk][ek] = CacheEntry(
            data=data,
            fingerprint=fingerprint,
            created_at=now,
            last_accessed=now,
            ttl=self._default_ttl,
            entry_type=entry_type,
        )
        self._dirty.add(fk)
        self._evict_if_needed()
        self._maybe_debounced_flush()

    def reset_ttl(
        self,
        entry_type: str,
        catalog_name: str,
        connection_hash: str,
        path: tuple[str, ...],
    ) -> None:
        """Reset the TTL on an existing entry (fingerprint matched)."""
        fk = self._file_key(catalog_name, connection_hash)
        catalog_entries = self._catalogs.get(fk)
        if catalog_entries is None:
            return
        ek = _entry_key(entry_type, path)
        entry = catalog_entries.get(ek)
        if entry is None:
            return
        entry.created_at = time.time()
        self._dirty.add(fk)
        self._maybe_debounced_flush()

    def invalidate_entry(
        self,
        entry_type: str,
        catalog_name: str,
        connection_hash: str,
        path: tuple[str, ...],
    ) -> None:
        """Remove a single cache entry."""
        fk = self._file_key(catalog_name, connection_hash)
        catalog_entries = self._catalogs.get(fk)
        if catalog_entries is None:
            return
        ek = _entry_key(entry_type, path)
        if ek in catalog_entries:
            del catalog_entries[ek]
            self._dirty.add(fk)
            self._maybe_debounced_flush()

    def invalidate_catalog(self, catalog_name: str, connection_hash: str) -> None:
        """Remove all entries for a catalog and delete its file."""
        fk = self._file_key(catalog_name, connection_hash)
        self._catalogs.pop(fk, None)
        self._catalog_meta.pop(fk, None)
        self._dirty.discard(fk)
        fp = self._file_path(fk)
        try:
            fp.unlink(missing_ok=True)
        except OSError:
            pass

    def invalidate_profile(self) -> None:
        """Remove all entries for the current profile."""
        for fk in list(self._catalogs):
            catalog_name, connection_hash = self._catalog_meta[fk]
            self.invalidate_catalog(catalog_name, connection_hash)

    def clear(self) -> None:
        """Remove all cached data from memory and disk."""
        self._catalogs.clear()
        self._catalog_meta.clear()
        self._dirty.clear()
        if self._cache_dir.is_dir():
            for fp in self._cache_dir.iterdir():
                try:
                    fp.unlink(missing_ok=True)
                except OSError:
                    pass
            try:
                self._cache_dir.rmdir()
            except OSError:
                pass

    def flush(self) -> None:
        """Write all dirty catalog files to disk immediately."""
        for fk in list(self._dirty):
            if fk in self._catalogs:
                self._write_catalog_file(fk)
        self._dirty.clear()
        self._last_flush_time = time.monotonic()

    def get_all_children(
        self,
        order_by_access: bool = False,
    ) -> list[tuple[tuple[str, tuple[str, ...]], list[Any], float]]:
        """Return all cached children entries.

        Each element is ``((catalog_name, path_tuple), nodes, last_accessed)``.

        When *order_by_access* is ``True``, entries are sorted by
        ``last_accessed`` descending (most recent first).
        """
        results: list[tuple[tuple[str, tuple[str, ...]], list[Any], float]] = []
        for fk, entries in self._catalogs.items():
            catalog_name, _conn_hash = self._catalog_meta[fk]
            for ek, entry in entries.items():
                if entry.entry_type != "children":
                    continue
                # Parse the entry key back to a path tuple
                # Format: "children::a.b.c"
                path_str = ek.split("::", 1)[1] if "::" in ek else ""
                path_tuple = tuple(path_str.split(".")) if path_str else ()
                results.append(((catalog_name, path_tuple), entry.data, entry.last_accessed))
        if order_by_access:
            results.sort(key=lambda r: r[2], reverse=True)
        return results

    def get_last_accessed(
        self,
        entry_type: str,
        catalog_name: str,
        connection_hash: str,
        path: tuple[str, ...],
    ) -> float | None:
        """Return the ``last_accessed`` timestamp for an entry, or ``None``."""
        fk = self._file_key(catalog_name, connection_hash)
        catalog_entries = self._catalogs.get(fk)
        if catalog_entries is None:
            return None
        ek = _entry_key(entry_type, path)
        entry = catalog_entries.get(ek)
        if entry is None:
            return None
        return entry.last_accessed

    def get_access_rank(
        self,
        entry_type: str,
        catalog_name: str,
        connection_hash: str,
        path: tuple[str, ...],
    ) -> int | None:
        """Return the rank (0-based) of this entry among all children entries
        sorted by ``last_accessed`` descending.

        Returns ``None`` if the entry is not cached.
        """
        fk = self._file_key(catalog_name, connection_hash)
        catalog_entries = self._catalogs.get(fk)
        if catalog_entries is None:
            return None
        ek = _entry_key(entry_type, path)
        if ek not in catalog_entries:
            return None

        # Collect all children entries across all catalogs, sorted by
        # last_accessed descending.
        all_children: list[tuple[str, str, float]] = []
        for cfk, centries in self._catalogs.items():
            for cek, centry in centries.items():
                if centry.entry_type == "children":
                    all_children.append((cfk, cek, centry.last_accessed))
        all_children.sort(key=lambda x: x[2], reverse=True)

        for rank, (cfk, cek, _) in enumerate(all_children):
            if cfk == fk and cek == ek:
                return rank
        return None  # pragma: no cover — should not happen

    @property
    def stats(self) -> dict[str, int]:
        """Return cache statistics."""
        total_entries = sum(len(e) for e in self._catalogs.values())
        total_catalogs = len(self._catalogs)
        dirty_catalogs = len(self._dirty)
        return {
            "total_entries": total_entries,
            "total_catalogs": total_catalogs,
            "dirty_catalogs": dirty_catalogs,
            "total_size_bytes": self._total_size(),
        }

    # ------------------------------------------------------------------
    # LRU eviction
    # ------------------------------------------------------------------

    def _total_size(self) -> int:
        """Estimate total serialized size of all entries."""
        total = 0
        for fk in self._catalogs:
            if fk in self._catalog_meta:
                try:
                    total += len(self._serialize_catalog(fk).encode("utf-8"))
                except Exception:
                    pass
        return total

    def _evict_if_needed(self) -> None:
        """Evict least-recently-accessed entries until within size bounds."""
        while self._total_size() > self._max_size_bytes:
            # Find the LRU entry across all catalogs
            lru_fk: str | None = None
            lru_ek: str | None = None
            lru_time = float("inf")
            for fk, entries in self._catalogs.items():
                for ek, entry in entries.items():
                    if entry.last_accessed < lru_time:
                        lru_time = entry.last_accessed
                        lru_fk = fk
                        lru_ek = ek
            if lru_fk is None or lru_ek is None:
                break  # nothing left to evict
            del self._catalogs[lru_fk][lru_ek]
            self._dirty.add(lru_fk)
            # Clean up empty catalog dicts
            if not self._catalogs[lru_fk]:
                del self._catalogs[lru_fk]
                self._catalog_meta.pop(lru_fk, None)
                self._dirty.discard(lru_fk)
                fp = self._file_path(lru_fk)
                try:
                    fp.unlink(missing_ok=True)
                except OSError:
                    pass
