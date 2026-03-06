"""Tests for MaterialCache."""

from unittest.mock import MagicMock

from rivet_core.interactive.material_cache import MaterialCache
from rivet_core.models import Material


def _make_material(name: str) -> Material:
    m = MagicMock(spec=Material)
    m.name = name
    return m


def test_get_miss_returns_none():
    cache = MaterialCache()
    assert cache.get("missing") is None


def test_put_and_get_round_trip():
    cache = MaterialCache()
    mat = _make_material("joint_a")
    cache.put("joint_a", mat)
    assert cache.get("joint_a") is mat


def test_put_overwrites():
    cache = MaterialCache()
    m1 = _make_material("j")
    m2 = _make_material("j")
    cache.put("j", m1)
    cache.put("j", m2)
    assert cache.get("j") is m2


def test_invalidate_removes_specified():
    cache = MaterialCache()
    cache.put("a", _make_material("a"))
    cache.put("b", _make_material("b"))
    cache.put("c", _make_material("c"))
    cache.invalidate(["a", "c"])
    assert cache.get("a") is None
    assert cache.get("b") is not None
    assert cache.get("c") is None


def test_invalidate_missing_is_noop():
    cache = MaterialCache()
    cache.put("a", _make_material("a"))
    cache.invalidate(["nonexistent"])
    assert cache.get("a") is not None


def test_clear_removes_all():
    cache = MaterialCache()
    cache.put("a", _make_material("a"))
    cache.put("b", _make_material("b"))
    cache.clear()
    assert cache.get("a") is None
    assert cache.get("b") is None
    assert len(cache) == 0


def test_contains():
    cache = MaterialCache()
    cache.put("x", _make_material("x"))
    assert "x" in cache
    assert "y" not in cache


def test_len():
    cache = MaterialCache()
    assert len(cache) == 0
    cache.put("a", _make_material("a"))
    cache.put("b", _make_material("b"))
    assert len(cache) == 2
    cache.invalidate(["a"])
    assert len(cache) == 1
