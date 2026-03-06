"""Property test for S3 sink write strategy acceptance.

Feature: cross-storage-adapters, Property 8: S3 sink write strategy acceptance
Generate random strategy strings; verify acceptance iff in valid set.
Validates: Requirements 5.6
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_aws.s3_sink import _ALL_STRATEGIES, _parse_sink_options
from rivet_core.errors import PluginValidationError

# Strategy: any non-empty string NOT in the valid strategy set
_invalid_strategy = st.text(min_size=1).filter(lambda s: s not in _ALL_STRATEGIES)

# Strategy: any valid strategy string
_valid_strategy = st.sampled_from(sorted(_ALL_STRATEGIES))

_BASE_CAT_OPTS = {"bucket": "test-bucket", "sink_options": {"path": "output/"}}
_DELTA_CAT_OPTS = {"bucket": "test-bucket", "sink_options": {"path": "output/", "format": "delta"}}


class _FakeJoint:
    table = None
    write_strategy = None


@settings(max_examples=100, deadline=None)
@given(strategy=_invalid_strategy)
def test_property_invalid_strategy_raises(strategy: str) -> None:
    """Property 8: any strategy string not in the valid set raises PluginValidationError."""
    cat_opts = {**_BASE_CAT_OPTS, "sink_options": {"path": "output/", "write_strategy": strategy}}
    with pytest.raises(PluginValidationError) as exc_info:
        _parse_sink_options(cat_opts, _FakeJoint())
    assert exc_info.value.error.code == "RVT-202"


@settings(max_examples=100, deadline=None)
@given(strategy=_valid_strategy)
def test_property_valid_strategy_accepted(strategy: str) -> None:
    """Property 8: every strategy in the valid set is accepted without error."""
    # merge/scd2 require delta format
    if strategy in ("merge", "scd2"):
        cat_opts = {**_DELTA_CAT_OPTS, "sink_options": {"path": "output/", "format": "delta", "write_strategy": strategy}}
    else:
        cat_opts = {**_BASE_CAT_OPTS, "sink_options": {"path": "output/", "write_strategy": strategy}}
    opts = _parse_sink_options(cat_opts, _FakeJoint())
    assert opts["write_strategy"] == strategy
