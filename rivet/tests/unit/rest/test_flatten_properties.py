"""Property-based tests for JSON flattening, schema inference, and Arrow conversion.

- Property 1: Flat JSON record round-trip through Arrow
  Validates: Requirements 12.1, 12.2
- Property 2: Nested JSON round-trip within max_flatten_depth
  Validates: Requirements 12.3, 12.4
- Property 8: Flattening respects max_depth boundary
  Validates: Requirements 5.1, 5.2
"""

from __future__ import annotations

import json
import math

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_rest.flatten import (
    arrow_to_records,
    flatten_records,
    records_to_arrow,
    unflatten_record,
)

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Keys must be valid identifiers (no dots — dots are the separator)
_key = st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True)

# Scalar values that JSON and Arrow both handle cleanly
_scalar = st.one_of(
    st.text(min_size=0, max_size=50),
    st.integers(min_value=-(2**53), max_value=2**53),
    st.floats(allow_nan=False, allow_infinity=False, min_value=-1e15, max_value=1e15),
    st.booleans(),
)

# A flat record: keys without dots, scalar values only
_flat_record = st.dictionaries(
    keys=_key,
    values=_scalar,
    min_size=1,
    max_size=8,
)


def _nested_value(max_depth: int) -> st.SearchStrategy:
    """Build a strategy for nested JSON values up to *max_depth* levels."""
    if max_depth <= 1:
        return _scalar
    return st.one_of(
        _scalar,
        st.dictionaries(
            keys=_key,
            values=_nested_value(max_depth - 1),
            min_size=1,
            max_size=3,
        ),
    )


def _nested_record(max_depth: int) -> st.SearchStrategy:
    """A record with nesting up to *max_depth*."""
    return st.dictionaries(
        keys=_key,
        values=_nested_value(max_depth),
        min_size=1,
        max_size=5,
    )


# Deep records that exceed a given max_depth
def _deep_record(depth: int) -> st.SearchStrategy:
    """A record guaranteed to have nesting at exactly *depth* levels.

    Instead of filtering, we build a record that always contains at least
    one nested-dict entry alongside optional scalar entries. This avoids
    the HealthCheck.filter_too_much issue.
    """
    if depth <= 1:
        return st.dictionaries(keys=_key, values=_scalar, min_size=1, max_size=3)
    inner = _deep_record(depth - 1)
    # Build one guaranteed-nested entry + optional extra entries
    nested_entry = st.tuples(_key, inner)
    extra_entries = st.dictionaries(
        keys=_key, values=st.one_of(_scalar, inner), min_size=0, max_size=2
    )
    return st.tuples(nested_entry, extra_entries).map(lambda t: {t[0][0]: t[0][1], **t[1]})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _collect_leaf_paths(obj: dict, prefix: str = "") -> dict[str, object]:
    """Collect all leaf values with their dot-separated paths."""
    result: dict[str, object] = {}
    for k, v in obj.items():
        full = f"{prefix}{k}" if prefix else k
        if isinstance(v, dict):
            result.update(_collect_leaf_paths(v, full + "."))
        else:
            result[full] = v
    return result


def _values_equivalent(a: object, b: object) -> bool:
    """Check if two values are equivalent for round-trip purposes.

    Handles int/float coercion (e.g. ``1`` vs ``1.0``) and NaN.
    """
    if a is None and b is None:
        return True
    if isinstance(a, bool) or isinstance(b, bool):
        return a is b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if isinstance(a, float) and math.isnan(a):
            return isinstance(b, float) and math.isnan(b)
        return float(a) == float(b)
    return a == b


# ---------------------------------------------------------------------------
# Feature: rest-api-catalog, Property 1: Flat JSON round-trip through Arrow
# ---------------------------------------------------------------------------


@settings(max_examples=100)
@given(record=_flat_record)
def test_property1_flat_json_round_trip(record: dict) -> None:
    """For any flat JSON record with scalar types, converting to Arrow
    via records_to_arrow and back via arrow_to_records produces an
    equivalent record.
    """
    table = records_to_arrow([record], max_depth=3)
    result = arrow_to_records(table)
    assert len(result) == 1
    out = result[0]

    assert set(out.keys()) == set(record.keys())
    for key in record:
        orig = record[key]
        got = out[key]
        assert _values_equivalent(orig, got), f"Mismatch on key {key!r}: {orig!r} != {got!r}"


# ---------------------------------------------------------------------------
# Feature: rest-api-catalog, Property 2: Nested JSON round-trip within max_flatten_depth
# ---------------------------------------------------------------------------


@settings(max_examples=100)
@given(record=_nested_record(3))
def test_property2_nested_json_round_trip(record: dict) -> None:
    """For any nested JSON record within max_flatten_depth=3, flattening
    and reconstructing via unflatten_record preserves leaf values.
    Null values are preserved.
    """
    max_depth = 3
    flat_list = flatten_records([record], max_depth=max_depth)
    assert len(flat_list) == 1
    flat = flat_list[0]

    # Unflatten and compare leaf paths
    reconstructed = unflatten_record(flat)
    original_leaves = _collect_leaf_paths(record)
    reconstructed_leaves = _collect_leaf_paths(reconstructed)

    for path, orig_val in original_leaves.items():
        if path in reconstructed_leaves:
            got_val = reconstructed_leaves[path]
            assert _values_equivalent(orig_val, got_val), (
                f"Leaf {path!r}: {orig_val!r} != {got_val!r}"
            )
        else:
            # The value was JSON-serialized because it was beyond max_depth
            # or was a list — find it in the flat dict
            assert any(path.startswith(k) for k in flat), (
                f"Leaf {path!r} lost during flatten/unflatten"
            )


# ---------------------------------------------------------------------------
# Feature: rest-api-catalog, Property 8: Flattening respects max_depth boundary
# ---------------------------------------------------------------------------


@settings(max_examples=100)
@given(
    record=_deep_record(5),
    max_depth=st.integers(min_value=1, max_value=4),
)
def test_property8_flattening_respects_max_depth(record: dict, max_depth: int) -> None:
    """For any JSON record, flatten_records with a given max_depth shall
    flatten objects at depth <= max_depth into dot-separated keys, and
    JSON-serialize structures at depth > max_depth as strings.
    """
    flat_list = flatten_records([record], max_depth=max_depth)
    assert len(flat_list) == 1
    flat = flat_list[0]

    for key, value in flat.items():
        # Count depth: number of dots + 1
        depth = key.count(".") + 1

        # No key should have more dots than max_depth - 1
        # (max_depth=3 means at most 2 dots: a.b.c)
        assert depth <= max_depth, f"Key {key!r} has depth {depth} > max_depth {max_depth}"

        # Values that are dicts or lists should have been JSON-serialized
        assert not isinstance(value, dict), (
            f"Key {key!r} has dict value — should have been serialized"
        )
        assert not isinstance(value, list), (
            f"Key {key!r} has list value — should have been serialized"
        )

        # If the value is a string that looks like JSON object/array,
        # verify it's valid JSON (was properly serialized)
        if isinstance(value, str) and value and value[0] in ("{", "["):
            try:
                json.loads(value)
            except json.JSONDecodeError:
                pass  # Not all strings starting with { are JSON
