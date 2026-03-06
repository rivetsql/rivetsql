"""Property 11: Glue sink write strategy acceptance.

Generate random strategy strings; verify acceptance iff in valid set (merge/scd2 rejected).
Validates: Requirements 7.7
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_aws.glue_sink import SUPPORTED_STRATEGIES, UNSUPPORTED_STRATEGIES, _validate_sink_options
from rivet_core.errors import PluginValidationError

_BASE_OPTIONS = {"table": "my_table"}


@settings(max_examples=100, deadline=None)
@given(strategy=st.sampled_from(sorted(SUPPORTED_STRATEGIES)))
def test_supported_strategies_accepted(strategy: str) -> None:
    """Every strategy in SUPPORTED_STRATEGIES must not raise."""
    _validate_sink_options({**_BASE_OPTIONS, "write_strategy": strategy})


@settings(max_examples=100, deadline=None)
@given(strategy=st.sampled_from(sorted(UNSUPPORTED_STRATEGIES)))
def test_unsupported_strategies_rejected_with_rvt202(strategy: str) -> None:
    """merge and scd2 must raise PluginValidationError with code RVT-202."""
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_sink_options({**_BASE_OPTIONS, "write_strategy": strategy})
    assert "RVT-202" in str(exc_info.value)


@settings(max_examples=100, deadline=None)
@given(
    strategy=st.text(
        alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_"),
        min_size=1,
        max_size=30,
    ).filter(lambda s: s not in SUPPORTED_STRATEGIES and s not in UNSUPPORTED_STRATEGIES)
)
def test_unknown_strategies_rejected(strategy: str) -> None:
    """Any strategy not in the known sets must raise PluginValidationError."""
    with pytest.raises(PluginValidationError):
        _validate_sink_options({**_BASE_OPTIONS, "write_strategy": strategy})
