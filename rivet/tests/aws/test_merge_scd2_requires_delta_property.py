"""Property test for merge/scd2 requires Delta format.

Feature: cross-storage-adapters, Property 9: merge/scd2 requires Delta format
Validates Requirements 5.8, 7.8 — merge/scd2 without delta format raises RVT-202;
merge/scd2 with delta format (S3) is accepted.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_aws.s3_sink import _DELTA_ONLY_STRATEGIES, _VALID_FORMATS, _parse_sink_options
from rivet_core.errors import PluginValidationError

_NON_DELTA_FORMATS = _VALID_FORMATS - {"delta"}


@settings(max_examples=100, deadline=None)
@given(
    strategy=st.sampled_from(sorted(_DELTA_ONLY_STRATEGIES)),
    fmt=st.sampled_from(sorted(_NON_DELTA_FORMATS)),
)
def test_property_merge_scd2_without_delta_raises_rvt202(strategy: str, fmt: str) -> None:
    """Property 9: merge/scd2 on non-delta format raises PluginValidationError RVT-202."""
    cat_opts = {
        "bucket": "my-bucket",
        "sink_options": {"path": "output/", "write_strategy": strategy, "format": fmt},
    }

    class _FakeJoint:
        table = None
        write_strategy = None

    with pytest.raises(PluginValidationError) as exc_info:
        _parse_sink_options(cat_opts, _FakeJoint())
    assert exc_info.value.error.code == "RVT-202"


@settings(max_examples=100, deadline=None)
@given(strategy=st.sampled_from(sorted(_DELTA_ONLY_STRATEGIES)))
def test_property_merge_scd2_with_delta_accepted(strategy: str) -> None:
    """Property 9: merge/scd2 with format=delta is accepted without error."""
    cat_opts = {
        "bucket": "my-bucket",
        "sink_options": {"path": "output/", "write_strategy": strategy, "format": "delta"},
    }

    class _FakeJoint:
        table = None
        write_strategy = None

    opts = _parse_sink_options(cat_opts, _FakeJoint())
    assert opts["write_strategy"] == strategy
    assert opts["format"] == "delta"
