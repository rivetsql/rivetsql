"""Property test for S3 invalid format rejection.

Feature: cross-storage-adapters, Property 6: Invalid format rejection
Validates Requirement 3.7 — any format string not in the valid set must raise
PluginValidationError with code RVT-201.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_aws.s3_catalog import _VALID_FORMATS, S3CatalogPlugin
from rivet_core.errors import PluginValidationError

_PLUGIN = S3CatalogPlugin()

# Strategy: any non-empty string that is NOT a valid format
_invalid_format = st.text(min_size=1).filter(lambda s: s not in _VALID_FORMATS)


@settings(max_examples=100, deadline=None)
@given(fmt=_invalid_format)
def test_property_invalid_format_raises_rvt201(fmt: str) -> None:
    """Property 6: any format string not in the valid set raises PluginValidationError RVT-201."""
    with pytest.raises(PluginValidationError) as exc_info:
        _PLUGIN.validate({"bucket": "my-bucket", "format": fmt})
    assert exc_info.value.error.code == "RVT-201"
