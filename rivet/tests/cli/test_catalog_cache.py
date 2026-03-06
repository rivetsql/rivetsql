"""Tests for catalog_cache.py — catalog tree cache persistence.

Feature: cli-repl, Property 19: Cache serialization round-trip
Validates: Requirements 33.2, 33.4
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import patch

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_cli.repl.catalog_cache import (
    _cache_key,
    _connection_hash,
    invalidate_catalog_cache,
    invalidate_profile_cache,
    load_catalog_cache,
    save_catalog_cache,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_node_strategy = st.fixed_dictionaries(
    {
        "name": st.text(alphabet="abcdefghijklmnopqrstuvwxyz_", min_size=1, max_size=12),
        "node_type": st.sampled_from(["schema", "table", "view", "column"]),
        "path": st.lists(
            st.text(alphabet="abcdefghijklmnopqrstuvwxyz_", min_size=1, max_size=8),
            min_size=1,
            max_size=4,
        ),
    }
)

_options_strategy = st.dictionaries(
    st.text(alphabet="abcdefghijklmnopqrstuvwxyz_", min_size=1, max_size=8),
    st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789", min_size=1, max_size=16),
    max_size=4,
)


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


def test_connection_hash_stable():
    opts = {"host": "localhost", "port": "5432", "db": "mydb"}
    assert _connection_hash(opts) == _connection_hash(opts)


def test_connection_hash_order_independent():
    opts1 = {"host": "localhost", "port": "5432"}
    opts2 = {"port": "5432", "host": "localhost"}
    assert _connection_hash(opts1) == _connection_hash(opts2)


def test_connection_hash_different_options():
    opts1 = {"host": "localhost"}
    opts2 = {"host": "remotehost"}
    assert _connection_hash(opts1) != _connection_hash(opts2)


def test_cache_key_format():
    key = _cache_key("prod", "pg", "abc123")
    assert key == "prod:pg:abc123"


def test_load_returns_none_when_file_missing(tmp_path: Path):
    cache_file = tmp_path / "catalog_cache.json"
    with patch("rivet_cli.repl.catalog_cache._CACHE_FILE", cache_file):
        result = load_catalog_cache("prod", "pg", {"host": "localhost"})
    assert result is None


def test_save_and_load_round_trip(tmp_path: Path):
    cache_file = tmp_path / "catalog_cache.json"
    nodes = [{"name": "public", "node_type": "schema", "path": ["pg", "public"]}]
    opts = {"host": "localhost", "port": "5432"}

    with patch("rivet_cli.repl.catalog_cache._CACHE_FILE", cache_file), \
         patch("rivet_cli.repl.catalog_cache._CACHE_DIR", tmp_path):
        save_catalog_cache("prod", "pg", opts, nodes)
        result = load_catalog_cache("prod", "pg", opts)

    assert result == nodes


def test_load_returns_none_for_different_options(tmp_path: Path):
    cache_file = tmp_path / "catalog_cache.json"
    nodes = [{"name": "public", "node_type": "schema", "path": ["pg", "public"]}]
    opts1 = {"host": "localhost"}
    opts2 = {"host": "remotehost"}

    with patch("rivet_cli.repl.catalog_cache._CACHE_FILE", cache_file), \
         patch("rivet_cli.repl.catalog_cache._CACHE_DIR", tmp_path):
        save_catalog_cache("prod", "pg", opts1, nodes)
        result = load_catalog_cache("prod", "pg", opts2)

    assert result is None


def test_load_returns_none_for_different_profile(tmp_path: Path):
    cache_file = tmp_path / "catalog_cache.json"
    nodes = [{"name": "public", "node_type": "schema", "path": ["pg", "public"]}]
    opts = {"host": "localhost"}

    with patch("rivet_cli.repl.catalog_cache._CACHE_FILE", cache_file), \
         patch("rivet_cli.repl.catalog_cache._CACHE_DIR", tmp_path):
        save_catalog_cache("prod", "pg", opts, nodes)
        result = load_catalog_cache("staging", "pg", opts)

    assert result is None


def test_save_preserves_other_entries(tmp_path: Path):
    cache_file = tmp_path / "catalog_cache.json"
    nodes1 = [{"name": "public", "node_type": "schema", "path": ["pg", "public"]}]
    nodes2 = [{"name": "main", "node_type": "schema", "path": ["duckdb", "main"]}]
    opts = {"host": "localhost"}

    with patch("rivet_cli.repl.catalog_cache._CACHE_FILE", cache_file), \
         patch("rivet_cli.repl.catalog_cache._CACHE_DIR", tmp_path):
        save_catalog_cache("prod", "pg", opts, nodes1)
        save_catalog_cache("prod", "duckdb", opts, nodes2)
        result1 = load_catalog_cache("prod", "pg", opts)
        result2 = load_catalog_cache("prod", "duckdb", opts)

    assert result1 == nodes1
    assert result2 == nodes2


def test_invalidate_removes_entry(tmp_path: Path):
    cache_file = tmp_path / "catalog_cache.json"
    nodes = [{"name": "public", "node_type": "schema", "path": ["pg", "public"]}]
    opts = {"host": "localhost"}

    with patch("rivet_cli.repl.catalog_cache._CACHE_FILE", cache_file), \
         patch("rivet_cli.repl.catalog_cache._CACHE_DIR", tmp_path):
        save_catalog_cache("prod", "pg", opts, nodes)
        invalidate_catalog_cache("prod", "pg", opts)
        result = load_catalog_cache("prod", "pg", opts)

    assert result is None


def test_invalidate_preserves_other_entries(tmp_path: Path):
    cache_file = tmp_path / "catalog_cache.json"
    nodes1 = [{"name": "public", "node_type": "schema", "path": ["pg", "public"]}]
    nodes2 = [{"name": "main", "node_type": "schema", "path": ["duckdb", "main"]}]
    opts = {"host": "localhost"}

    with patch("rivet_cli.repl.catalog_cache._CACHE_FILE", cache_file), \
         patch("rivet_cli.repl.catalog_cache._CACHE_DIR", tmp_path):
        save_catalog_cache("prod", "pg", opts, nodes1)
        save_catalog_cache("prod", "duckdb", opts, nodes2)
        invalidate_catalog_cache("prod", "pg", opts)
        result1 = load_catalog_cache("prod", "pg", opts)
        result2 = load_catalog_cache("prod", "duckdb", opts)

    assert result1 is None
    assert result2 == nodes2


def test_invalidate_profile_removes_all_profile_entries(tmp_path: Path):
    cache_file = tmp_path / "catalog_cache.json"
    nodes = [{"name": "public", "node_type": "schema", "path": ["pg", "public"]}]
    opts = {"host": "localhost"}

    with patch("rivet_cli.repl.catalog_cache._CACHE_FILE", cache_file), \
         patch("rivet_cli.repl.catalog_cache._CACHE_DIR", tmp_path):
        save_catalog_cache("prod", "pg", opts, nodes)
        save_catalog_cache("prod", "duckdb", opts, nodes)
        save_catalog_cache("staging", "pg", opts, nodes)
        invalidate_profile_cache("prod")
        assert load_catalog_cache("prod", "pg", opts) is None
        assert load_catalog_cache("prod", "duckdb", opts) is None
        # staging entry should remain
        assert load_catalog_cache("staging", "pg", opts) == nodes


def test_load_handles_corrupt_file(tmp_path: Path):
    cache_file = tmp_path / "catalog_cache.json"
    cache_file.write_text("not valid json", encoding="utf-8")

    with patch("rivet_cli.repl.catalog_cache._CACHE_FILE", cache_file):
        result = load_catalog_cache("prod", "pg", {})

    assert result is None


def test_save_handles_corrupt_existing_file(tmp_path: Path):
    cache_file = tmp_path / "catalog_cache.json"
    cache_file.write_text("not valid json", encoding="utf-8")
    nodes = [{"name": "public", "node_type": "schema", "path": ["pg", "public"]}]

    with patch("rivet_cli.repl.catalog_cache._CACHE_FILE", cache_file), \
         patch("rivet_cli.repl.catalog_cache._CACHE_DIR", tmp_path):
        # Should not raise
        save_catalog_cache("prod", "pg", {}, nodes)
        result = load_catalog_cache("prod", "pg", {})

    assert result == nodes


# ---------------------------------------------------------------------------
# Property 19: Cache serialization round-trip
# ---------------------------------------------------------------------------


@given(
    profile=st.text(alphabet="abcdefghijklmnopqrstuvwxyz_", min_size=1, max_size=12),
    catalog_name=st.text(alphabet="abcdefghijklmnopqrstuvwxyz_", min_size=1, max_size=12),
    catalog_options=_options_strategy,
    nodes=st.lists(_node_strategy, min_size=0, max_size=10),
)
@settings(max_examples=100)
def test_property_19_catalog_cache_round_trip(
    profile: str,
    catalog_name: str,
    catalog_options: dict[str, str],
    nodes: list[dict[str, Any]],
) -> None:
    """Property 19: Cache serialization round-trip.

    For any catalog cache state (tree structure, profile key), serializing to JSON
    and deserializing should produce equivalent state.

    Validates: Requirements 33.2, 33.4
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        cache_file = tmp_path / "catalog_cache.json"

        with patch("rivet_cli.repl.catalog_cache._CACHE_FILE", cache_file), \
             patch("rivet_cli.repl.catalog_cache._CACHE_DIR", tmp_path):
            save_catalog_cache(profile, catalog_name, catalog_options, nodes)
            result = load_catalog_cache(profile, catalog_name, catalog_options)

    assert result == nodes


@given(
    profile=st.text(alphabet="abcdefghijklmnopqrstuvwxyz_", min_size=1, max_size=12),
    catalog_name=st.text(alphabet="abcdefghijklmnopqrstuvwxyz_", min_size=1, max_size=12),
    catalog_options=_options_strategy,
    nodes=st.lists(_node_strategy, min_size=1, max_size=10),
)
@settings(max_examples=100)
def test_property_19_invalidation_clears_entry(
    profile: str,
    catalog_name: str,
    catalog_options: dict[str, str],
    nodes: list[dict[str, Any]],
) -> None:
    """Property 19 (invalidation): After invalidation, cache returns None.

    Validates: Requirements 33.4
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        cache_file = tmp_path / "catalog_cache.json"

        with patch("rivet_cli.repl.catalog_cache._CACHE_FILE", cache_file), \
             patch("rivet_cli.repl.catalog_cache._CACHE_DIR", tmp_path):
            save_catalog_cache(profile, catalog_name, catalog_options, nodes)
            invalidate_catalog_cache(profile, catalog_name, catalog_options)
            result = load_catalog_cache(profile, catalog_name, catalog_options)

    assert result is None


@given(
    profile=st.text(alphabet="abcdefghijklmnopqrstuvwxyz_", min_size=1, max_size=12),
    catalog_names=st.lists(
        st.text(alphabet="abcdefghijklmnopqrstuvwxyz_", min_size=1, max_size=12),
        min_size=1,
        max_size=5,
        unique=True,
    ),
    catalog_options=_options_strategy,
    nodes=st.lists(_node_strategy, min_size=1, max_size=5),
)
@settings(max_examples=50)
def test_property_19_profile_invalidation_clears_all(
    profile: str,
    catalog_names: list[str],
    catalog_options: dict[str, str],
    nodes: list[dict[str, Any]],
) -> None:
    """Property 19 (profile invalidation): After profile invalidation, all entries for that profile are gone.

    Validates: Requirements 33.4
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        cache_file = tmp_path / "catalog_cache.json"

        with patch("rivet_cli.repl.catalog_cache._CACHE_FILE", cache_file), \
             patch("rivet_cli.repl.catalog_cache._CACHE_DIR", tmp_path):
            for name in catalog_names:
                save_catalog_cache(profile, name, catalog_options, nodes)
            invalidate_profile_cache(profile)
            for name in catalog_names:
                assert load_catalog_cache(profile, name, catalog_options) is None
