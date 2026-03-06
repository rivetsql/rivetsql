"""Property test for credential caching with pre-expiry refresh.

Feature: cross-storage-adapters, Property 3: Credential caching with pre-expiry refresh

For any AWSCredentialResolver with credential_cache=true:
- Calling resolve() twice within the credential's validity window SHALL return
  the same cached AWSCredentials object.
- When the credential is within 60 seconds of expiry, resolve() SHALL return
  a freshly resolved credential.

Validates: Requirements 2.4
"""

from __future__ import annotations

import time

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_aws.credentials import AWSCredentialResolver

# The refresh buffer defined in credentials.py
_REFRESH_BUFFER = 60


def _make_resolver(expiry: float | None = None) -> AWSCredentialResolver:
    """Create a resolver with explicit options (always resolves)."""
    return AWSCredentialResolver(
        {"access_key_id": "K", "secret_access_key": "S"},
        region="us-east-1",
    )


# ── Static unit tests ─────────────────────────────────────────────────────────


def test_same_object_returned_when_valid():
    """Calling resolve() twice returns the same cached object when not expired."""
    resolver = _make_resolver()
    c1 = resolver.resolve()
    c2 = resolver.resolve()
    assert c1 is c2


def test_fresh_object_returned_when_expired():
    """When cached credential is expired, resolve() returns a new object."""
    resolver = _make_resolver()
    c1 = resolver.resolve()
    # Force expiry (past the refresh buffer)
    c1.expiry = time.time() - 1
    resolver._cached = c1
    c2 = resolver.resolve()
    assert c2 is not c1


def test_fresh_object_returned_within_refresh_buffer():
    """When credential expires within 60 s, resolve() returns a new object."""
    resolver = _make_resolver()
    c1 = resolver.resolve()
    # Set expiry to 30 s from now — within the 60-s buffer
    c1.expiry = time.time() + 30
    resolver._cached = c1
    c2 = resolver.resolve()
    assert c2 is not c1


def test_cache_disabled_always_returns_new_object():
    """With credential_cache=False, every resolve() call returns a new object."""
    resolver = AWSCredentialResolver(
        {"access_key_id": "K", "secret_access_key": "S", "credential_cache": False},
        region="us-east-1",
    )
    c1 = resolver.resolve()
    c2 = resolver.resolve()
    assert c1 is not c2


# ── Property tests ────────────────────────────────────────────────────────────


@settings(max_examples=100, deadline=None)
@given(
    seconds_until_expiry=st.floats(
        min_value=_REFRESH_BUFFER + 1,
        max_value=3600,
        allow_nan=False,
        allow_infinity=False,
    )
)
def test_property_cached_when_well_before_expiry(seconds_until_expiry: float):
    """Property: when expiry is more than 60 s away, the same object is returned."""
    resolver = _make_resolver()
    c1 = resolver.resolve()
    # Set a future expiry well outside the refresh buffer
    c1.expiry = time.time() + seconds_until_expiry
    resolver._cached = c1
    c2 = resolver.resolve()
    assert c2 is c1, (
        f"Expected cached credential to be returned when expiry is "
        f"{seconds_until_expiry:.1f}s away (> {_REFRESH_BUFFER}s buffer)"
    )


@settings(max_examples=100, deadline=None)
@given(
    seconds_until_expiry=st.floats(
        min_value=-3600,
        max_value=_REFRESH_BUFFER - 1,
        allow_nan=False,
        allow_infinity=False,
    )
)
def test_property_refreshed_when_near_or_past_expiry(seconds_until_expiry: float):
    """Property: when expiry is within 60 s (or past), a fresh credential is returned."""
    resolver = _make_resolver()
    c1 = resolver.resolve()
    # Set expiry within the refresh buffer (or already past)
    c1.expiry = time.time() + seconds_until_expiry
    resolver._cached = c1
    c2 = resolver.resolve()
    assert c2 is not c1, (
        f"Expected fresh credential when expiry is {seconds_until_expiry:.1f}s away "
        f"(<= {_REFRESH_BUFFER}s buffer)"
    )


@settings(max_examples=100, deadline=None)
@given(
    expiry_offset=st.floats(
        min_value=_REFRESH_BUFFER + 1,
        max_value=3600,
        allow_nan=False,
        allow_infinity=False,
    )
)
def test_property_no_expiry_always_cached(expiry_offset: float):
    """Property: credentials with no expiry (expiry=None) are always served from cache."""
    resolver = _make_resolver()
    c1 = resolver.resolve()
    # expiry=None means non-expiring (e.g. long-lived IAM user keys)
    assert c1.expiry is None
    c2 = resolver.resolve()
    assert c2 is c1, "Non-expiring credentials should always be served from cache"
