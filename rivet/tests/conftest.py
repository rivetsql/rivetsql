"""Shared test fixtures for rivet-core tests."""

from hypothesis import settings

# Disable deadline globally — many property tests exceed the default 200ms
# when running in the full suite due to CPU contention.
settings.register_profile("ci", deadline=None)
settings.load_profile("ci")
