"""Property-based tests for Plugin Discovery Guard.

Property 10: Plugin Discovery Idempotence

Calling ``discover_plugins()`` N times (N >= 2) shall load entry points
exactly once and ``is_discovered`` shall be ``True`` after the first call.

Validates: Requirements 6.2, 6.3
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.plugins import PluginRegistry


def _mock_entry_points_factory(call_counter: dict[str, int]):
    """Return a side-effect that counts how many times each group is queried."""

    def _side_effect(*, group: str):
        call_counter[group] = call_counter.get(group, 0) + 1
        return []

    return _side_effect


# ── Property 10a: entry point loading happens exactly once ────────────────────


@given(n_calls=st.integers(min_value=2, max_value=10))
@settings(max_examples=100)
def test_discover_plugins_loads_entry_points_exactly_once(n_calls: int) -> None:
    """Calling discover_plugins() N times loads entry points exactly once."""
    reg = PluginRegistry()
    call_counter: dict[str, int] = {}

    with patch(
        "rivet_core.plugins.entry_points",
        side_effect=_mock_entry_points_factory(call_counter),
    ):
        for _ in range(n_calls):
            reg.discover_plugins()

    # entry_points() is called once per group during the single discovery pass.
    # On subsequent calls, discover_plugins() returns immediately.
    # So each group should have been queried exactly once.
    for group, count in call_counter.items():
        assert count == 1, (
            f"entry_points(group={group!r}) called {count} times, expected 1"
        )


# ── Property 10b: is_discovered is True after first call ─────────────────────


@given(n_calls=st.integers(min_value=1, max_value=10))
@settings(max_examples=100)
def test_is_discovered_true_after_first_call(n_calls: int) -> None:
    """is_discovered is True after the first discover_plugins() call."""
    reg = PluginRegistry()
    assert not reg.is_discovered

    with patch("rivet_core.plugins.entry_points", return_value=[]):
        reg.discover_plugins()
        assert reg.is_discovered

        # Remains True on subsequent calls
        for _ in range(n_calls - 1):
            reg.discover_plugins()
            assert reg.is_discovered


# ── Property 10c: is_discovered is False before discovery ─────────────────────


def test_is_discovered_false_before_discovery() -> None:
    """A fresh PluginRegistry has is_discovered == False."""
    reg = PluginRegistry()
    assert not reg.is_discovered


# ── Property 10d: failed discovery does not set discovered flag ───────────────


def test_failed_discovery_does_not_set_discovered() -> None:
    """If discover_plugins() raises, is_discovered remains False."""
    reg = PluginRegistry()

    bad_ep = MagicMock()
    bad_ep.name = "bad_plugin"
    bad_ep.load.side_effect = ImportError("boom")

    def _side_effect(*, group: str):
        if group == "rivet.plugins":
            return [bad_ep]
        return []

    with patch("rivet_core.plugins.entry_points", side_effect=_side_effect):
        try:
            reg.discover_plugins()
        except Exception:
            pass

    assert not reg.is_discovered
