"""Integration tests for SmartCache — real file I/O against temp directories."""

from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from rivet_core.smart_cache import CacheResult, SmartCache


@pytest.fixture
def cache_dir(tmp_path: Path) -> Path:
    return tmp_path / "cache"


@pytest.fixture
def cache(cache_dir: Path) -> SmartCache:
    return SmartCache(
        profile="test",
        cache_dir=cache_dir,
        default_ttl=300.0,
        max_size_bytes=50 * 1024 * 1024,
        flush_interval=5.0,
    )


CONN_HASH = "abc123"
CATALOG = "mycat"


def test_put_and_get_children(cache: SmartCache) -> None:
    children = [{"name": "t1", "node_type": "table", "path": ["s1", "t1"]}]
    cache.put("children", CATALOG, CONN_HASH, ("s1",), children)

    result = cache.get("children", CATALOG, CONN_HASH, ("s1",))
    assert result is not None
    assert isinstance(result, CacheResult)
    assert result.data == children
    assert result.expired is False


def test_put_and_get_schema(cache: SmartCache) -> None:
    schema = {"path": ["s1", "t1"], "columns": [{"name": "id", "type": "int64"}]}
    cache.put("schema", CATALOG, CONN_HASH, ("s1", "t1"), schema)

    result = cache.get("schema", CATALOG, CONN_HASH, ("s1", "t1"))
    assert result is not None
    assert result.data == schema
    assert result.expired is False


def test_cache_miss_returns_none(cache: SmartCache) -> None:
    result = cache.get("children", CATALOG, CONN_HASH, ("nonexistent",))
    assert result is None


def test_get_returns_expired_flag(cache_dir: Path) -> None:
    cache = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=1.0)
    cache.put("children", CATALOG, CONN_HASH, ("s1",), [{"name": "t1"}])

    # Patch time.time to simulate TTL expiry
    original_time = time.time()
    with patch("rivet_core.smart_cache.time") as mock_time:
        mock_time.time.return_value = original_time + 2.0
        mock_time.monotonic.return_value = time.monotonic()
        result = cache.get("children", CATALOG, CONN_HASH, ("s1",))

    assert result is not None
    assert result.expired is True


def test_reset_ttl_clears_expired(cache_dir: Path) -> None:
    cache = SmartCache(profile="test", cache_dir=cache_dir, default_ttl=1.0)
    cache.put("children", CATALOG, CONN_HASH, ("s1",), [{"name": "t1"}])

    # Advance past TTL
    original_time = time.time()
    with patch("rivet_core.smart_cache.time") as mock_time:
        mock_time.time.return_value = original_time + 2.0
        mock_time.monotonic.return_value = time.monotonic()
        result = cache.get("children", CATALOG, CONN_HASH, ("s1",))
        assert result is not None
        assert result.expired is True

        # Reset TTL — created_at becomes "now" (original_time + 2.0)
        cache.reset_ttl("children", CATALOG, CONN_HASH, ("s1",))

    # After reset, entry should not be expired (created_at was reset)
    result = cache.get("children", CATALOG, CONN_HASH, ("s1",))
    assert result is not None
    assert result.expired is False


def test_flush_persists_to_disk(cache_dir: Path) -> None:
    cache = SmartCache(profile="test", cache_dir=cache_dir)
    cache.put("children", CATALOG, CONN_HASH, ("s1",), [{"name": "t1"}])
    cache.flush()

    # Create a new instance — should recover entries from disk
    cache2 = SmartCache(profile="test", cache_dir=cache_dir)
    result = cache2.get("children", CATALOG, CONN_HASH, ("s1",))
    assert result is not None
    assert result.data == [{"name": "t1"}]


def test_debounced_flush_timing(cache_dir: Path) -> None:
    cache = SmartCache(profile="test", cache_dir=cache_dir, flush_interval=10.0)
    cache.put("children", CATALOG, CONN_HASH, ("s1",), [{"name": "t1"}])

    # File should have been written because _last_flush_time starts at 0
    # and monotonic() - 0 >= 10.0 is likely false on first call.
    # Actually, _last_flush_time=0.0 and monotonic() is large, so first
    # put triggers a flush. Let's verify the file exists after first put.
    profile_dir = cache_dir / "test"
    files = list(profile_dir.glob("*.json")) if profile_dir.exists() else []
    assert len(files) == 1  # first put triggers debounced flush

    # Now do another put — should NOT flush again (within interval)
    cache.put("children", CATALOG, CONN_HASH, ("s2",), [{"name": "t2"}])
    # Read the file — it should still only have s1 (s2 is dirty but not flushed)
    raw = json.loads(files[0].read_text(encoding="utf-8"))
    assert "children::s2" not in raw["entries"]

    # Explicit flush writes everything
    cache.flush()
    raw = json.loads(files[0].read_text(encoding="utf-8"))
    assert "children::s2" in raw["entries"]


def test_per_catalog_file_isolation(cache: SmartCache, cache_dir: Path) -> None:
    cache.put("children", "cat_a", "hash_a", ("s1",), [{"name": "t1"}])
    cache.put("children", "cat_b", "hash_b", ("s1",), [{"name": "t2"}])
    cache.flush()

    profile_dir = cache_dir / "test"
    files = sorted(f.name for f in profile_dir.glob("*.json"))
    assert len(files) == 2
    assert "cat_a_hash_a.json" in files
    assert "cat_b_hash_b.json" in files


def test_corrupted_file_starts_empty(cache_dir: Path) -> None:
    # Write a valid catalog file and a corrupted one
    profile_dir = cache_dir / "test"
    profile_dir.mkdir(parents=True)

    # Valid file
    valid = {
        "version": 1,
        "catalog_name": "good",
        "connection_hash": "h1",
        "entries": {
            "children::s1": {
                "data": [{"name": "t1"}],
                "fingerprint": None,
                "created_at": time.time(),
                "last_accessed": time.time(),
                "ttl": 300.0,
                "entry_type": "children",
            }
        },
    }
    (profile_dir / "good_h1.json").write_text(json.dumps(valid), encoding="utf-8")

    # Corrupted file
    (profile_dir / "bad_h2.json").write_text("NOT VALID JSON{{{", encoding="utf-8")

    cache = SmartCache(profile="test", cache_dir=cache_dir)

    # Good catalog should be loaded
    result = cache.get("children", "good", "h1", ("s1",))
    assert result is not None
    assert result.data == [{"name": "t1"}]

    # Bad catalog should be empty (file was discarded)
    result = cache.get("children", "bad", "h2", ("s1",))
    assert result is None
    # Corrupted file should have been deleted
    assert not (profile_dir / "bad_h2.json").exists()


def test_invalidate_catalog_removes_entries(cache: SmartCache) -> None:
    cache.put("children", "cat_a", "ha", ("s1",), [{"name": "t1"}])
    cache.put("children", "cat_b", "hb", ("s1",), [{"name": "t2"}])

    cache.invalidate_catalog("cat_a", "ha")

    assert cache.get("children", "cat_a", "ha", ("s1",)) is None
    result = cache.get("children", "cat_b", "hb", ("s1",))
    assert result is not None
    assert result.data == [{"name": "t2"}]


def test_invalidate_catalog_deletes_file(cache: SmartCache, cache_dir: Path) -> None:
    cache.put("children", CATALOG, CONN_HASH, ("s1",), [{"name": "t1"}])
    cache.flush()

    profile_dir = cache_dir / "test"
    fp = profile_dir / f"{CATALOG}_{CONN_HASH}.json"
    assert fp.exists()

    cache.invalidate_catalog(CATALOG, CONN_HASH)
    assert not fp.exists()


def test_invalidate_profile_removes_all(cache: SmartCache) -> None:
    cache.put("children", "cat_a", "ha", ("s1",), [{"name": "t1"}])
    cache.put("children", "cat_b", "hb", ("s1",), [{"name": "t2"}])

    cache.invalidate_profile()

    assert cache.get("children", "cat_a", "ha", ("s1",)) is None
    assert cache.get("children", "cat_b", "hb", ("s1",)) is None
    assert cache.stats["total_entries"] == 0


def test_clear_removes_from_disk(cache: SmartCache, cache_dir: Path) -> None:
    cache.put("children", CATALOG, CONN_HASH, ("s1",), [{"name": "t1"}])
    cache.flush()

    profile_dir = cache_dir / "test"
    assert any(profile_dir.glob("*.json"))

    cache.clear()

    # Memory is empty
    assert cache.stats["total_entries"] == 0
    # Disk files are gone
    if profile_dir.exists():
        assert not any(profile_dir.glob("*.json"))


def test_old_repl_cache_ignored(cache_dir: Path, tmp_path: Path) -> None:
    # Create an old-format cache file in a different location
    old_dir = tmp_path / "old_cache" / "repl"
    old_dir.mkdir(parents=True)
    old_file = old_dir / "catalog_cache.json"
    old_file.write_text(
        json.dumps({"test:mycat:abc123": [{"name": "old_table"}]}),
        encoding="utf-8",
    )

    # SmartCache should not read from the old location
    cache = SmartCache(profile="test", cache_dir=cache_dir)
    result = cache.get("children", "mycat", "abc123", ())
    assert result is None


def test_invalidate_entry_removes_single(cache: SmartCache) -> None:
    cache.put("children", CATALOG, CONN_HASH, ("s1",), [{"name": "t1"}])
    cache.put("children", CATALOG, CONN_HASH, ("s2",), [{"name": "t2"}])
    cache.put("schema", CATALOG, CONN_HASH, ("s1", "t1"), {"columns": []})

    cache.invalidate_entry("children", CATALOG, CONN_HASH, ("s1",))

    # s1 children gone
    assert cache.get("children", CATALOG, CONN_HASH, ("s1",)) is None
    # s2 children still there
    result = cache.get("children", CATALOG, CONN_HASH, ("s2",))
    assert result is not None
    assert result.data == [{"name": "t2"}]
    # schema still there
    result = cache.get("schema", CATALOG, CONN_HASH, ("s1", "t1"))
    assert result is not None
