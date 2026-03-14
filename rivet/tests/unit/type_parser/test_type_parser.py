"""Unit tests for the centralized type parser.

Tests cover:
- Primitive type mapping (including parameterized types)
- Array parsing (standard array<T> syntax)
- PostgreSQL array parsing (type[] syntax)
- Struct parsing (with field order preservation)
- Nested complex types
- Error handling (malformed input, unknown types)
- Whitespace normalization
"""

from __future__ import annotations

import warnings

from rivet_core.type_parser import parse_type

# Sample primitive mappings for testing
UNITY_MAPPING = {
    "bigint": "int64",
    "int": "int32",
    "smallint": "int16",
    "tinyint": "int8",
    "float": "float32",
    "double": "float64",
    "string": "large_utf8",
    "boolean": "bool",
    "date": "date32",
    "timestamp": "timestamp[us]",
    "timestamp_ntz": "timestamp[us]",
    "decimal": "decimal128",
    "binary": "large_binary",
}

PG_MAPPING = {
    "integer": "int32",
    "bigint": "int64",
    "smallint": "int16",
    "text": "large_utf8",
    "varchar": "large_utf8",
    "boolean": "bool",
    "date": "date32",
    "timestamp": "timestamp[us]",
    "real": "float32",
    "double precision": "float64",
}


# ============================================================================
# Task 7.1: Tests for primitive type mapping
# ============================================================================


def test_primitive_int() -> None:
    """Test mapping of int primitive type."""
    result = parse_type("int", UNITY_MAPPING)
    assert result == "int32"


def test_primitive_bigint() -> None:
    """Test mapping of bigint primitive type."""
    result = parse_type("bigint", UNITY_MAPPING)
    assert result == "int64"


def test_primitive_string() -> None:
    """Test mapping of string primitive type."""
    result = parse_type("string", UNITY_MAPPING)
    assert result == "large_utf8"


def test_primitive_boolean() -> None:
    """Test mapping of boolean primitive type."""
    result = parse_type("boolean", UNITY_MAPPING)
    assert result == "bool"


def test_primitive_date() -> None:
    """Test mapping of date primitive type."""
    result = parse_type("date", UNITY_MAPPING)
    assert result == "date32"


def test_primitive_timestamp() -> None:
    """Test mapping of timestamp primitive type."""
    result = parse_type("timestamp", UNITY_MAPPING)
    assert result == "timestamp[us]"


def test_primitive_float() -> None:
    """Test mapping of float primitive type."""
    result = parse_type("float", UNITY_MAPPING)
    assert result == "float32"


def test_primitive_double() -> None:
    """Test mapping of double primitive type."""
    result = parse_type("double", UNITY_MAPPING)
    assert result == "float64"


def test_primitive_case_insensitive() -> None:
    """Test that primitive type mapping is case-insensitive."""
    assert parse_type("INT", UNITY_MAPPING) == "int32"
    assert parse_type("String", UNITY_MAPPING) == "large_utf8"
    assert parse_type("BIGINT", UNITY_MAPPING) == "int64"


def test_parameterized_decimal() -> None:
    """Test parameterized decimal type strips parameters."""
    result = parse_type("decimal(10,2)", UNITY_MAPPING)
    assert result == "decimal128"


def test_parameterized_varchar() -> None:
    """Test parameterized varchar type strips parameters."""
    result = parse_type("varchar(255)", PG_MAPPING)
    assert result == "large_utf8"


def test_parameterized_decimal_with_spaces() -> None:
    """Test parameterized decimal with spaces in parameters."""
    result = parse_type("decimal(10, 2)", UNITY_MAPPING)
    assert result == "decimal128"


# ============================================================================
# Task 7.2: Tests for array parsing
# ============================================================================


def test_array_of_int() -> None:
    """Test array<int> parsing."""
    result = parse_type("array<int>", UNITY_MAPPING)
    assert result == "list<int32>"


def test_array_of_string() -> None:
    """Test array<string> parsing."""
    result = parse_type("array<string>", UNITY_MAPPING)
    assert result == "list<large_utf8>"


def test_array_of_bigint() -> None:
    """Test array<bigint> parsing."""
    result = parse_type("array<bigint>", UNITY_MAPPING)
    assert result == "list<int64>"


def test_array_of_float() -> None:
    """Test array<float> parsing."""
    result = parse_type("array<float>", UNITY_MAPPING)
    assert result == "list<float32>"


def test_array_of_boolean() -> None:
    """Test array<boolean> parsing."""
    result = parse_type("array<boolean>", UNITY_MAPPING)
    assert result == "list<bool>"


def test_array_of_date() -> None:
    """Test array<date> parsing."""
    result = parse_type("array<date>", UNITY_MAPPING)
    assert result == "list<date32>"


def test_array_of_timestamp() -> None:
    """Test array<timestamp> parsing."""
    result = parse_type("array<timestamp>", UNITY_MAPPING)
    assert result == "list<timestamp[us]>"


def test_nested_array_two_levels() -> None:
    """Test nested array parsing: array<array<int>>."""
    result = parse_type("array<array<int>>", UNITY_MAPPING)
    assert result == "list<list<int32>>"


def test_nested_array_three_levels() -> None:
    """Test deeply nested array parsing: array<array<array<string>>>."""
    result = parse_type("array<array<array<string>>>", UNITY_MAPPING)
    assert result == "list<list<list<large_utf8>>>"


def test_array_with_whitespace() -> None:
    """Test array parsing with extra whitespace."""
    result = parse_type("array< string >", UNITY_MAPPING)
    assert result == "list<large_utf8>"


# ============================================================================
# Task 7.3: Tests for PostgreSQL array parsing
# ============================================================================


def test_postgres_array_integer() -> None:
    """Test PostgreSQL integer[] syntax."""
    result = parse_type("integer[]", PG_MAPPING)
    assert result == "list<int32>"


def test_postgres_array_text() -> None:
    """Test PostgreSQL text[] syntax."""
    result = parse_type("text[]", PG_MAPPING)
    assert result == "list<large_utf8>"


def test_postgres_array_timestamp() -> None:
    """Test PostgreSQL timestamp[] syntax."""
    result = parse_type("timestamp[]", PG_MAPPING)
    assert result == "list<timestamp[us]>"


def test_postgres_array_bigint() -> None:
    """Test PostgreSQL bigint[] syntax."""
    result = parse_type("bigint[]", PG_MAPPING)
    assert result == "list<int64>"


def test_postgres_array_boolean() -> None:
    """Test PostgreSQL boolean[] syntax."""
    result = parse_type("boolean[]", PG_MAPPING)
    assert result == "list<bool>"


def test_postgres_array_varchar() -> None:
    """Test PostgreSQL varchar[] syntax."""
    result = parse_type("varchar[]", PG_MAPPING)
    assert result == "list<large_utf8>"


def test_postgres_array_date() -> None:
    """Test PostgreSQL date[] syntax."""
    result = parse_type("date[]", PG_MAPPING)
    assert result == "list<date32>"


def test_postgres_nested_array() -> None:
    """Test PostgreSQL nested array syntax is not supported (would need array[][], which is not standard)."""
    # PostgreSQL doesn't use nested [] syntax, but we can test that type[][] works
    result = parse_type("integer[][]", PG_MAPPING)
    # This should parse as array of (integer[])
    assert result == "list<list<int32>>"


# ============================================================================
# Task 7.4: Tests for struct parsing
# ============================================================================


def test_struct_single_field() -> None:
    """Test struct with single field."""
    result = parse_type("struct<name:string>", UNITY_MAPPING)
    assert result == "struct<name:large_utf8>"


def test_struct_two_fields() -> None:
    """Test struct with two fields."""
    result = parse_type("struct<name:string,age:int>", UNITY_MAPPING)
    assert result == "struct<name:large_utf8,age:int32>"


def test_struct_five_fields() -> None:
    """Test struct with five fields."""
    result = parse_type(
        "struct<id:bigint,name:string,active:boolean,score:float,created:date>",
        UNITY_MAPPING,
    )
    assert result == "struct<id:int64,name:large_utf8,active:bool,score:float32,created:date32>"


def test_struct_field_order_preserved() -> None:
    """Test that struct field order is preserved."""
    result = parse_type("struct<z:int,a:string,m:boolean>", UNITY_MAPPING)
    assert result == "struct<z:int32,a:large_utf8,m:bool>"
    # Verify order by checking the string directly
    assert result.index("z:") < result.index("a:") < result.index("m:")


def test_nested_struct() -> None:
    """Test nested struct: struct<outer:struct<inner:int>>."""
    result = parse_type("struct<outer:struct<inner:int>>", UNITY_MAPPING)
    assert result == "struct<outer:struct<inner:int32>>"


def test_nested_struct_multiple_levels() -> None:
    """Test deeply nested struct."""
    result = parse_type(
        "struct<level1:struct<level2:struct<level3:int>>>",
        UNITY_MAPPING,
    )
    assert result == "struct<level1:struct<level2:struct<level3:int32>>>"


def test_struct_with_whitespace() -> None:
    """Test struct parsing with extra whitespace."""
    result = parse_type("struct< name : string , age : int >", UNITY_MAPPING)
    assert result == "struct<name:large_utf8,age:int32>"


# ============================================================================
# Task 7.5: Tests for nested complex types
# ============================================================================


def test_array_of_struct() -> None:
    """Test array<struct<...>> parsing."""
    result = parse_type("array<struct<name:string,age:int>>", UNITY_MAPPING)
    assert result == "list<struct<name:large_utf8,age:int32>>"


def test_array_of_struct_complex() -> None:
    """Test array of struct with multiple fields."""
    result = parse_type(
        "array<struct<type:string,number:string,date:timestamp_ntz>>",
        UNITY_MAPPING,
    )
    assert result == "list<struct<type:large_utf8,number:large_utf8,date:timestamp[us]>>"


def test_struct_with_array_field() -> None:
    """Test struct<field:array<...>> parsing."""
    result = parse_type("struct<items:array<int>>", UNITY_MAPPING)
    assert result == "struct<items:list<int32>>"


def test_struct_with_multiple_array_fields() -> None:
    """Test struct with multiple array fields."""
    result = parse_type(
        "struct<tags:array<string>,scores:array<float>>",
        UNITY_MAPPING,
    )
    assert result == "struct<tags:list<large_utf8>,scores:list<float32>>"


def test_array_of_struct_with_array_field() -> None:
    """Test array<struct<field:array<...>>>."""
    result = parse_type(
        "array<struct<name:string,tags:array<string>>>",
        UNITY_MAPPING,
    )
    assert result == "list<struct<name:large_utf8,tags:list<large_utf8>>>"


def test_nested_complex_three_levels() -> None:
    """Test three levels of nesting."""
    result = parse_type(
        "array<struct<data:array<int>>>",
        UNITY_MAPPING,
    )
    assert result == "list<struct<data:list<int32>>>"


def test_struct_with_nested_struct_and_array() -> None:
    """Test struct containing both nested struct and array."""
    result = parse_type(
        "struct<metadata:struct<id:int>,tags:array<string>>",
        UNITY_MAPPING,
    )
    assert result == "struct<metadata:struct<id:int32>,tags:list<large_utf8>>"


def test_deeply_nested_valid() -> None:
    """Test nesting up to 5 levels (well within the 10 level limit)."""
    result = parse_type(
        "array<array<array<array<array<int>>>>>",
        UNITY_MAPPING,
    )
    assert result == "list<list<list<list<list<int32>>>>>"


# ============================================================================
# Task 7.6: Tests for error handling
# ============================================================================


def test_unmatched_opening_bracket_array() -> None:
    """Test unmatched opening bracket in array type."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = parse_type("array<string", UNITY_MAPPING)
        assert result == "large_utf8"
        assert len(w) == 1
        assert "unmatched brackets" in str(w[0].message).lower()


def test_unmatched_opening_bracket_struct() -> None:
    """Test unmatched opening bracket in struct type."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = parse_type("struct<name:string", UNITY_MAPPING)
        assert result == "large_utf8"
        assert len(w) == 1
        assert "unmatched brackets" in str(w[0].message).lower()


def test_invalid_struct_missing_colon() -> None:
    """Test invalid struct syntax with missing colon."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = parse_type("struct<name string>", UNITY_MAPPING)
        assert result == "large_utf8"
        assert len(w) == 1
        assert "missing colon" in str(w[0].message).lower()


def test_invalid_struct_missing_field_name() -> None:
    """Test invalid struct syntax with missing field name."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = parse_type("struct<:string>", UNITY_MAPPING)
        assert result == "large_utf8"
        assert len(w) == 1
        assert "empty field name" in str(w[0].message).lower()


def test_empty_string_input() -> None:
    """Test empty string input returns large_utf8 without warning."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = parse_type("", UNITY_MAPPING)
        assert result == "large_utf8"
        assert len(w) == 0  # No warning for empty input


def test_none_input() -> None:
    """Test None input returns large_utf8 without warning."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = parse_type(None, UNITY_MAPPING)
        assert result == "large_utf8"
        assert len(w) == 0  # No warning for None input


def test_whitespace_only_input() -> None:
    """Test whitespace-only input returns large_utf8 without warning."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = parse_type("   ", UNITY_MAPPING)
        assert result == "large_utf8"
        assert len(w) == 0  # No warning for whitespace-only input


def test_unknown_primitive_type() -> None:
    """Test unknown primitive type defaults to large_utf8 with warning."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = parse_type("unknown_type", UNITY_MAPPING)
        assert result == "large_utf8"
        assert len(w) == 1
        assert "unknown primitive type" in str(w[0].message).lower()


def test_array_of_unknown_type() -> None:
    """Test array of unknown type defaults element to large_utf8 with warning."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = parse_type("array<unknown_type>", UNITY_MAPPING)
        assert result == "list<large_utf8>"
        assert len(w) == 1
        assert "unknown primitive type" in str(w[0].message).lower()


def test_struct_with_unknown_field_type() -> None:
    """Test struct with unknown field type defaults field to large_utf8 with warning."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = parse_type("struct<name:unknown_type>", UNITY_MAPPING)
        assert result == "struct<name:large_utf8>"
        assert len(w) == 1
        assert "unknown primitive type" in str(w[0].message).lower()


def test_no_exceptions_raised_on_malformed_input() -> None:
    """Test that no exceptions are raised for various malformed inputs."""
    malformed_inputs = [
        "array<",
        "struct<",
        "array<>",
        "struct<>",
        "array<<int>>",
        "struct<<name:string>>",
        "array<struct<name:string>",
        "struct<array<int>>",  # Missing field name
    ]

    for malformed in malformed_inputs:
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            # Should not raise exception
            result = parse_type(malformed, UNITY_MAPPING)
            assert isinstance(result, str)  # Should return a string


def test_depth_limit_exceeded() -> None:
    """Test that deeply nested types beyond 10 levels default to large_utf8."""
    # Create a type nested 12 levels deep (depth check happens at _depth > 10)
    deeply_nested = "array<" * 12 + "int" + ">" * 12

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        parse_type(deeply_nested, UNITY_MAPPING)
        # The outer levels parse successfully, but at depth 11 it hits the limit
        # So we should see a warning about depth exceeded
        assert len(w) >= 1
        assert any("depth exceeded" in str(warning.message).lower() for warning in w)


def test_warn_on_unknown_false() -> None:
    """Test that warn_on_unknown=False suppresses warnings."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = parse_type("unknown_type", UNITY_MAPPING, warn_on_unknown=False)
        assert result == "large_utf8"
        assert len(w) == 0  # No warnings


# ============================================================================
# Task 7.7: Tests for whitespace normalization
# ============================================================================


def test_whitespace_around_array_brackets() -> None:
    """Test whitespace normalization around array brackets."""
    assert parse_type("array< int >", UNITY_MAPPING) == "list<int32>"
    assert parse_type("array<  int  >", UNITY_MAPPING) == "list<int32>"
    assert parse_type("array <int>", UNITY_MAPPING) == "list<int32>"


def test_whitespace_around_struct_delimiters() -> None:
    """Test whitespace normalization in struct types."""
    result = parse_type("struct< name : string , age : int >", UNITY_MAPPING)
    assert result == "struct<name:large_utf8,age:int32>"


def test_tabs_in_type_string() -> None:
    """Test that tabs are normalized to spaces."""
    result = parse_type("struct<name:\tstring,\tage:\tint>", UNITY_MAPPING)
    assert result == "struct<name:large_utf8,age:int32>"


def test_multiple_spaces_normalized() -> None:
    """Test that multiple spaces are normalized to single space."""
    result = parse_type("array<    string    >", UNITY_MAPPING)
    assert result == "list<large_utf8>"


def test_whitespace_in_nested_types() -> None:
    """Test whitespace normalization in nested complex types."""
    result = parse_type(
        "array< struct< name : string , tags : array< string > > >",
        UNITY_MAPPING,
    )
    assert result == "list<struct<name:large_utf8,tags:list<large_utf8>>>"


def test_leading_trailing_whitespace() -> None:
    """Test that leading and trailing whitespace is handled."""
    assert parse_type("  int  ", UNITY_MAPPING) == "int32"
    assert parse_type("\tstring\t", UNITY_MAPPING) == "large_utf8"


def test_whitespace_consistency() -> None:
    """Test that different whitespace variations produce the same result."""
    type1 = parse_type("struct<name:string,age:int>", UNITY_MAPPING)
    type2 = parse_type("struct< name : string , age : int >", UNITY_MAPPING)
    type3 = parse_type("struct<  name  :  string  ,  age  :  int  >", UNITY_MAPPING)

    assert type1 == type2 == type3
