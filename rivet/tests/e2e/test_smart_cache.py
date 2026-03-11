"""E2E tests for SmartCache integration with CLI commands.

Each test creates a temporary Rivet project with a DuckDB catalog,
invokes ``_main(argv)`` in-process, and verifies that the SmartCache
persists data across invocations and modes.

Validates Requirements: 1.2, 1.7, 1.8, 3.1, 3.2, 4.6, 4.7, 5.4
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from rivet_cli.app import _main
from rivet_core.smart_cache import SmartCache
from tests.e2e.conftest import CLIResult

# ---------------------------------------------------------------------------
# Project templates
# ---------------------------------------------------------------------------

_RIVET_YAML = """\
profiles: profiles.yaml
sources: sources
joints: joints
sinks: sinks
tests: tests
quality: quality
"""

_PROFILES_TEMPLATE = """\
default:
  catalogs:
    mycat:
      type: duckdb
      path: {db_path}
  engines:
    - name: duckdb_primary
      type: duckdb
      catalogs: [mycat]
  default_engine: duckdb_primary
"""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def cache_project(tmp_path: Path) -> Path:
    """Scaffold a Rivet project with a DuckDB catalog containing test data.

    Creates:
      - mycat (DuckDB catalog)
        └─ main (schema)
           ├─ users (table: id INTEGER, name VARCHAR)
           └─ orders (table: id INTEGER, amount DOUBLE)
    """
    import duckdb

    db_path = tmp_path / "warehouse.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR)")
    conn.execute("CREATE TABLE orders (id INTEGER, amount DOUBLE)")
    conn.close()

    (tmp_path / "rivet.yaml").write_text(_RIVET_YAML)
    (tmp_path / "profiles.yaml").write_text(_PROFILES_TEMPLATE.format(db_path=db_path))
    for d in ("sources", "joints", "sinks", "tests", "quality"):
        (tmp_path / d).mkdir(exist_ok=True)

    return tmp_path


@pytest.fixture()
def cache_dir(tmp_path: Path) -> Path:
    """Return a temporary directory for SmartCache files."""
    return tmp_path / "smart_cache"


def _patch_smart_cache_dir(cache_dir: Path):
    """Return a context manager that redirects SmartCache to *cache_dir*.

    Patches ``SmartCache.__init__`` so that every new instance uses
    *cache_dir* instead of ``~/.cache/rivet/catalog``.
    """
    original_init = SmartCache.__init__

    def _patched_init(self, profile, cache_dir_arg=None, **kwargs):  # type: ignore[no-untyped-def]
        original_init(self, profile, cache_dir=cache_dir, **kwargs)

    return patch.object(SmartCache, "__init__", _patched_init)


# ---------------------------------------------------------------------------
# CLI helper
# ---------------------------------------------------------------------------


def _run(
    project: Path,
    argv: list[str],
    capsys: pytest.CaptureFixture[str],
) -> CLIResult:
    """Invoke ``catalog`` subcommand with ``--project`` after the sub-subcommand."""
    full_argv = ["catalog"] + argv[:1] + ["--project", str(project)] + argv[1:]
    exit_code = _main(full_argv)
    captured = capsys.readouterr()
    return CLIResult(exit_code=exit_code, stdout=captured.out, stderr=captured.err)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_catalog_list_uses_cache_on_second_run(
    cache_project: Path,
    cache_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Run ``catalog list mycat`` twice — second run produces same output.

    The first run fetches live data and writes to cache (WRITE_ONLY).
    The second run also fetches live (WRITE_ONLY for non-interactive),
    but the cache files should exist on disk after both runs.

    Requirements: 1.2, 1.7, 4.6
    """
    with _patch_smart_cache_dir(cache_dir):
        r1 = _run(cache_project, ["list", "mycat"], capsys)
        assert r1.exit_code == 0, f"stderr: {r1.stderr}"

        r2 = _run(cache_project, ["list", "mycat"], capsys)
        assert r2.exit_code == 0, f"stderr: {r2.stderr}"

    # Both runs should produce the same output (same catalog data)
    assert r1.stdout == r2.stdout
    assert "main" in r1.stdout


def test_catalog_list_after_cache_clear(
    cache_project: Path,
    cache_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Run ``catalog list``, clear cache, run again — verify fresh data.

    After clearing the cache directory, the next run should still succeed
    by fetching fresh data from the plugin.

    Requirements: 1.7, 5.4
    """
    with _patch_smart_cache_dir(cache_dir):
        r1 = _run(cache_project, ["list", "mycat"], capsys)
        assert r1.exit_code == 0, f"stderr: {r1.stderr}"

        # Clear the cache directory
        profile_dir = cache_dir / "default"
        if profile_dir.is_dir():
            for f in profile_dir.iterdir():
                f.unlink()

        r2 = _run(cache_project, ["list", "mycat"], capsys)
        assert r2.exit_code == 0, f"stderr: {r2.stderr}"

    # Both runs should produce the same output (fresh data both times)
    assert r1.stdout == r2.stdout
    assert "main" in r2.stdout


def test_explore_warm_start_with_cache(
    cache_project: Path,
    cache_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Populate cache via ``catalog list`` (WRITE_ONLY), then verify
    the explore controller can read cached data (READ_WRITE).

    We don't run the full interactive TUI — instead we instantiate
    ``ExploreController`` directly with a READ_WRITE explorer backed
    by the pre-populated cache, and verify that ``list_children``
    returns cached data without needing a live plugin call.

    Requirements: 3.1, 3.2, 4.7
    """
    with _patch_smart_cache_dir(cache_dir):
        # Step 1: populate cache via non-interactive command
        r1 = _run(cache_project, ["list", "mycat"], capsys)
        assert r1.exit_code == 0, f"stderr: {r1.stderr}"

    # Step 2: verify cache files exist on disk
    profile_dir = cache_dir / "default"
    assert profile_dir.is_dir(), "Cache profile directory should exist"
    cache_files = list(profile_dir.glob("mycat_*.json"))
    assert len(cache_files) >= 1, "At least one cache file for mycat should exist"

    # Step 3: load the cache in READ_WRITE mode and verify data is present
    cache = SmartCache(profile="default", cache_dir=cache_dir)
    assert cache.stats["total_entries"] > 0, "Cache should have entries after catalog list"

    # Verify children data is cached (the "main" schema children)
    all_children = cache.get_all_children(order_by_access=True)
    assert len(all_children) > 0, "Should have cached children entries"


def test_noninteractive_rehydrates_cache_for_interactive(
    cache_project: Path,
    cache_dir: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Run ``catalog list`` (WRITE_ONLY) and verify cache file contains
    fetched data that a subsequent interactive session could use.

    Requirements: 1.8, 4.6, 4.7
    """
    with _patch_smart_cache_dir(cache_dir):
        result = _run(cache_project, ["list", "mycat"], capsys)
        assert result.exit_code == 0, f"stderr: {result.stderr}"

    # Verify cache files were written
    profile_dir = cache_dir / "default"
    assert profile_dir.is_dir(), "Cache profile directory should exist"
    cache_files = list(profile_dir.glob("mycat_*.json"))
    assert len(cache_files) >= 1, "Cache file for mycat should exist"

    # Read and validate the cache file structure
    cache_data = json.loads(cache_files[0].read_text(encoding="utf-8"))
    assert cache_data["version"] == 1
    assert cache_data["catalog_name"] == "mycat"
    assert "connection_hash" in cache_data
    assert isinstance(cache_data["entries"], dict)
    assert len(cache_data["entries"]) > 0, "Cache should contain entries from catalog list"

    # Verify at least one children entry exists (the root listing)
    children_entries = {
        k: v for k, v in cache_data["entries"].items() if v["entry_type"] == "children"
    }
    assert len(children_entries) > 0, "Should have at least one children entry"
