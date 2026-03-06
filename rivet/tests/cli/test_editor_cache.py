"""Property test for editor cache serialization round-trip.

Feature: cli-repl, Property 19: Cache serialization round-trip
Validates: Requirements 33.1, 33.2
"""

from __future__ import annotations

import json
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_cli.repl.editor_cache import (
    _SCHEMA_VERSION,
    CachedTab,
    EditorCacheState,
    load_editor_cache,
    save_editor_cache,
)

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_tab_st = st.builds(
    CachedTab,
    title=st.text(min_size=1, max_size=50),
    content=st.text(max_size=200),
    cursor_row=st.integers(min_value=0, max_value=100),
    cursor_col=st.integers(min_value=0, max_value=200),
)

_state_st = st.builds(
    EditorCacheState,
    tabs=st.lists(_tab_st, max_size=10),
    active_index=st.integers(min_value=0, max_value=9),
)


# ---------------------------------------------------------------------------
# Property 19: Cache serialization round-trip
# ---------------------------------------------------------------------------


@given(_state_st)
@settings(max_examples=100)
def test_editor_cache_round_trip(state: EditorCacheState) -> None:
    """Property 19: serializing and deserializing editor cache produces equivalent state."""
    import shutil
    import tempfile

    import rivet_cli.repl.editor_cache as ec

    tmp = Path(tempfile.mkdtemp())
    cache_file = tmp / "editor_cache.json"
    orig_dir = ec._CACHE_DIR
    orig_file = ec._CACHE_FILE
    ec._CACHE_DIR = tmp
    ec._CACHE_FILE = cache_file
    try:
        # Clamp active_index to valid range (mirrors load logic)
        if state.tabs:
            expected_index = max(0, min(state.active_index, len(state.tabs) - 1))
        else:
            expected_index = 0
        clamped = EditorCacheState(tabs=state.tabs, active_index=expected_index)

        save_editor_cache(clamped)
        loaded = load_editor_cache()

        assert loaded.active_index == clamped.active_index
        assert len(loaded.tabs) == len(clamped.tabs)
        for orig, restored in zip(clamped.tabs, loaded.tabs):
            assert restored.title == orig.title
            assert restored.content == orig.content
            assert restored.cursor_row == orig.cursor_row
            assert restored.cursor_col == orig.cursor_col
    finally:
        ec._CACHE_DIR = orig_dir
        ec._CACHE_FILE = orig_file
        shutil.rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# Missing file → empty state
# ---------------------------------------------------------------------------


def test_load_missing_file_returns_empty(tmp_path: Path, monkeypatch) -> None:
    cache_file = tmp_path / "editor_cache.json"
    monkeypatch.setattr("rivet_cli.repl.editor_cache._CACHE_DIR", tmp_path)
    monkeypatch.setattr("rivet_cli.repl.editor_cache._CACHE_FILE", cache_file)

    state = load_editor_cache()
    assert state.tabs == []
    assert state.active_index == 0


# ---------------------------------------------------------------------------
# Corruption → delete and return empty (RVT-862)
# ---------------------------------------------------------------------------


def test_corrupt_json_returns_empty_and_deletes(tmp_path: Path, monkeypatch) -> None:
    cache_file = tmp_path / "editor_cache.json"
    cache_file.write_text("not valid json", encoding="utf-8")
    monkeypatch.setattr("rivet_cli.repl.editor_cache._CACHE_DIR", tmp_path)
    monkeypatch.setattr("rivet_cli.repl.editor_cache._CACHE_FILE", cache_file)

    state = load_editor_cache()
    assert state.tabs == []
    assert not cache_file.exists()


def test_wrong_version_returns_empty_and_deletes(tmp_path: Path, monkeypatch) -> None:
    cache_file = tmp_path / "editor_cache.json"
    cache_file.write_text(
        json.dumps({"version": 999, "tabs": [], "active_index": 0}), encoding="utf-8"
    )
    monkeypatch.setattr("rivet_cli.repl.editor_cache._CACHE_DIR", tmp_path)
    monkeypatch.setattr("rivet_cli.repl.editor_cache._CACHE_FILE", cache_file)

    state = load_editor_cache()
    assert state.tabs == []
    assert not cache_file.exists()


def test_non_dict_root_returns_empty_and_deletes(tmp_path: Path, monkeypatch) -> None:
    cache_file = tmp_path / "editor_cache.json"
    cache_file.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
    monkeypatch.setattr("rivet_cli.repl.editor_cache._CACHE_DIR", tmp_path)
    monkeypatch.setattr("rivet_cli.repl.editor_cache._CACHE_FILE", cache_file)

    state = load_editor_cache()
    assert state.tabs == []
    assert not cache_file.exists()


# ---------------------------------------------------------------------------
# Active index clamping
# ---------------------------------------------------------------------------


def test_active_index_clamped_to_valid_range(tmp_path: Path, monkeypatch) -> None:
    cache_file = tmp_path / "editor_cache.json"
    payload = {
        "version": _SCHEMA_VERSION,
        "active_index": 999,
        "tabs": [
            {"title": "q1", "content": "SELECT 1", "cursor_row": 0, "cursor_col": 0}
        ],
    }
    cache_file.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr("rivet_cli.repl.editor_cache._CACHE_DIR", tmp_path)
    monkeypatch.setattr("rivet_cli.repl.editor_cache._CACHE_FILE", cache_file)

    state = load_editor_cache()
    assert state.active_index == 0  # clamped from 999 to 0 (only 1 tab)


def test_active_index_zero_when_no_tabs(tmp_path: Path, monkeypatch) -> None:
    cache_file = tmp_path / "editor_cache.json"
    payload = {"version": _SCHEMA_VERSION, "active_index": 5, "tabs": []}
    cache_file.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr("rivet_cli.repl.editor_cache._CACHE_DIR", tmp_path)
    monkeypatch.setattr("rivet_cli.repl.editor_cache._CACHE_FILE", cache_file)

    state = load_editor_cache()
    assert state.active_index == 0
    assert state.tabs == []
