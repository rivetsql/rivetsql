"""Unit tests for Arrow type normalization in schema comparison."""

from rivet_core.executor import _normalize_arrow_type


def test_utf8_string_equivalence():
    """utf8 and string are semantically equivalent."""
    assert _normalize_arrow_type("utf8") == "utf8"
    assert _normalize_arrow_type("string") == "utf8"


def test_float64_double_equivalence():
    """float64 and double are semantically equivalent."""
    assert _normalize_arrow_type("float64") == "float64"
    assert _normalize_arrow_type("double") == "float64"


def test_float32_float_equivalence():
    """float32 and float are semantically equivalent."""
    assert _normalize_arrow_type("float32") == "float32"
    assert _normalize_arrow_type("float") == "float32"


def test_large_string_normalization():
    """large_string normalizes to large_utf8."""
    assert _normalize_arrow_type("large_string") == "large_utf8"
    assert _normalize_arrow_type("large_utf8") == "large_utf8"


def test_date32_with_unit():
    """date32[day] normalizes to date32."""
    assert _normalize_arrow_type("date32") == "date32"
    assert _normalize_arrow_type("date32[day]") == "date32"


def test_decimal128_to_int64():
    """decimal128(38, 0) from SQL aggregations normalizes to int64."""
    assert _normalize_arrow_type("decimal128(38, 0)") == "int64"
    assert _normalize_arrow_type("int64") == "int64"


def test_timestamp_unit_preservation():
    """Timestamp types preserve unit but ignore timezone."""
    assert _normalize_arrow_type("timestamp[s]") == "timestamp[s]"
    assert _normalize_arrow_type("timestamp[ms]") == "timestamp[ms]"
    assert _normalize_arrow_type("timestamp[us]") == "timestamp[us]"
    assert _normalize_arrow_type("timestamp[ns]") == "timestamp[ns]"


def test_timestamp_timezone_stripped():
    """Timestamp with timezone info is normalized to base timestamp."""
    # Note: This test assumes timezone info comes after the unit
    # e.g., "timestamp[s, tz=UTC]" -> "timestamp[s]"
    assert _normalize_arrow_type("timestamp[s, tz=UTC]") == "timestamp[s]"
    assert _normalize_arrow_type("timestamp[ms, tz=America/New_York]") == "timestamp[ms]"


def test_whitespace_handling():
    """Whitespace is stripped from type strings."""
    assert _normalize_arrow_type("  utf8  ") == "utf8"
    assert _normalize_arrow_type(" string ") == "utf8"


def test_passthrough_for_unknown_types():
    """Unknown types pass through unchanged."""
    assert _normalize_arrow_type("int32") == "int32"
    assert _normalize_arrow_type("bool") == "bool"
    assert _normalize_arrow_type("binary") == "binary"


def test_complex_types_passthrough():
    """Complex types like list and struct pass through."""
    assert _normalize_arrow_type("list<int64>") == "list<int64>"
    assert _normalize_arrow_type("struct<a: int64, b: utf8>") == "struct<a: int64, b: utf8>"
