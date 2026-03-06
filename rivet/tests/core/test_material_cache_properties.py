"""Property-based tests for MaterialCache.

Property 6: MaterialCache round-trip and selective invalidation.

Properties verified:
- Any joint name put into the cache can be retrieved (round-trip).
- Invalidating a subset of joints removes exactly those joints and no others.
- clear() removes all entries.

Validates: Requirements 20.1, 20.2, 20.3, 20.4
"""

from __future__ import annotations

from unittest.mock import MagicMock

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.interactive.material_cache import MaterialCache
from rivet_core.models import Material

# ── Strategies ────────────────────────────────────────────────────────────────

_joint_name = st.from_regex(r"[a-z][a-z0-9_]{0,19}", fullmatch=True)
_joint_names = st.lists(_joint_name, min_size=1, max_size=20, unique=True)


def _make_material() -> Material:
    return MagicMock(spec=Material)


# ── Property 6a: round-trip ───────────────────────────────────────────────────


@given(names=_joint_names)
@settings(max_examples=100)
def test_put_get_round_trip(names: list[str]) -> None:
    """Every material put into the cache is retrievable by the same key (Req 20.1)."""
    cache = MaterialCache()
    materials = {name: _make_material() for name in names}
    for name, mat in materials.items():
        cache.put(name, mat)
    for name, mat in materials.items():
        assert cache.get(name) is mat


# ── Property 6b: selective invalidation ──────────────────────────────────────


@given(names=_joint_names, to_invalidate=st.data())
@settings(max_examples=100)
def test_selective_invalidation(names: list[str], to_invalidate: st.DataObject) -> None:
    """Invalidating a subset removes exactly those joints and no others (Req 20.2)."""
    cache = MaterialCache()
    for name in names:
        cache.put(name, _make_material())

    # Pick a random subset to invalidate (may be empty)
    subset = to_invalidate.draw(
        st.lists(st.sampled_from(names), max_size=len(names), unique=True)
    )
    kept = [n for n in names if n not in subset]

    cache.invalidate(subset)

    for name in subset:
        assert cache.get(name) is None, f"{name!r} should have been invalidated"
    for name in kept:
        assert cache.get(name) is not None, f"{name!r} should still be cached"


# ── Property 6c: clear removes all ───────────────────────────────────────────


@given(names=_joint_names)
@settings(max_examples=50)
def test_clear_removes_all(names: list[str]) -> None:
    """clear() removes every entry (Req 20.3, 20.4)."""
    cache = MaterialCache()
    for name in names:
        cache.put(name, _make_material())

    cache.clear()

    assert len(cache) == 0
    for name in names:
        assert cache.get(name) is None


# ── Property 6d: invalidate unknown names is a no-op ─────────────────────────


@given(names=_joint_names, unknown=_joint_names)
@settings(max_examples=50)
def test_invalidate_unknown_is_noop(names: list[str], unknown: list[str]) -> None:
    """Invalidating names not in the cache does not affect existing entries (Req 20.1)."""
    cache = MaterialCache()
    for name in names:
        cache.put(name, _make_material())

    # Only invalidate names that are NOT in the cache
    truly_unknown = [n for n in unknown if n not in names]
    cache.invalidate(truly_unknown)

    for name in names:
        assert cache.get(name) is not None
