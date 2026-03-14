"""Property-based tests for type parser.

These tests use hypothesis to verify universal properties across all valid inputs.
Each test runs a minimum of 100 iterations to ensure comprehensive coverage.

Feature: catalog-complex-types
"""

from __future__ import annotations

import warnings

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.type_parser import parse_type

# Test configuration: minimum 100 iterations per property test
PROPERTY_TEST_SETTINGS = settings(max_examples=100)


# ============================================================================
# Hypothesis Strategies for Generating Test Data
# ============================================================================


@st.composite
def primitive_type_name(draw: st.DrawFn) -> str:
    """Generate a primitive type name."""
    return draw(
        st.sampled_from(
            [
                "int",
                "bigint",
                "string",
                "float",
                "double",
                "boolean",
                "date",
                "timestamp",
                "decimal",
                "varchar",
            ]
        )
    )


@st.composite
def primitive_mapping(draw: st.DrawFn) -> dict[str, str]:
    """Generate a primitive type mapping dictionary."""
    return {
        "int": "int32",
        "bigint": "int64",
        "string": "large_utf8",
        "float": "float32",
        "double": "float64",
        "boolean": "bool",
        "date": "date32",
        "timestamp": "timestamp[us]",
        "decimal": "decimal128",
        "varchar": "large_utf8",
    }


@st.composite
def field_name(draw: st.DrawFn) -> str:
    """Generate a valid struct field name."""
    return draw(
        st.text(
            alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd")), min_size=1, max_size=20
        )
    )


@st.composite
def struct_fields(draw: st.DrawFn, mapping: dict[str, str]) -> list[tuple[str, str]]:
    """Generate a list of struct field (name, type) tuples."""
    num_fields = draw(st.integers(min_value=1, max_value=5))
    fields = []
    for _ in range(num_fields):
        name = draw(field_name())
        type_name = draw(st.sampled_from(list(mapping.keys())))
        fields.append((name, type_name))
    return fields


@st.composite
def whitespace_variant(draw: st.DrawFn, type_str: str) -> str:
    """Generate a whitespace variant of a type string."""
    # Add random spaces around delimiters
    ws = draw(st.sampled_from(["", " ", "  ", "\t", " \t "]))
    result = type_str
    for delimiter in ["<", ">", ",", ":"]:
        result = result.replace(delimiter, f"{ws}{delimiter}{ws}")
    return result


@st.composite
def malformed_type(draw: st.DrawFn) -> str:
    """Generate a malformed type string."""
    result: str = draw(
        st.one_of(
            st.just("array<string"),  # Unmatched bracket
            st.just("struct<field"),  # Unmatched bracket
            st.just("struct<name>"),  # Missing colon
            st.just("struct<:string>"),  # Missing field name
            st.just("struct<name:>"),  # Missing field type
            st.just("array<>"),  # Empty array
            st.just("struct<>"),  # Empty struct
            st.just("unknown_type"),  # Unknown type
            st.text(
                alphabet=st.characters(blacklist_characters="<>:,"), min_size=1, max_size=20
            ),  # Random text
        )
    )
    return result


# ============================================================================
# Property 2: Array Type Parsing
# ============================================================================


@given(primitive=primitive_type_name(), mapping=primitive_mapping())
@settings(max_examples=100)
@pytest.mark.property
def test_property_array_parsing(primitive: str, mapping: dict[str, str]) -> None:
    """Feature: catalog-complex-types, Property 2: Array Type Parsing

    For any primitive type in the mapping, wrapping it in array<...> should
    produce list<arrow_type> where arrow_type is the mapped primitive.

    Validates: Requirements 2.1, 2.2, 2.5
    """
    native_type = f"array<{primitive}>"
    expected = f"list<{mapping[primitive]}>"
    result = parse_type(native_type, mapping)
    assert result == expected, f"Expected {expected}, got {result} for {native_type}"


@given(primitive=primitive_type_name(), mapping=primitive_mapping())
@settings(max_examples=100)
@pytest.mark.property
def test_property_nested_array_parsing(primitive: str, mapping: dict[str, str]) -> None:
    """Feature: catalog-complex-types, Property 2: Array Type Parsing (Nested)

    For any primitive type, nested arrays should parse recursively.

    Validates: Requirements 2.2, 2.5
    """
    native_type = f"array<array<{primitive}>>"
    expected = f"list<list<{mapping[primitive]}>>"
    result = parse_type(native_type, mapping)
    assert result == expected, f"Expected {expected}, got {result} for {native_type}"


# ============================================================================
# Property 3: PostgreSQL Array Syntax
# ============================================================================


@given(primitive=primitive_type_name(), mapping=primitive_mapping())
@settings(max_examples=100)
@pytest.mark.property
def test_property_postgres_array_parsing(primitive: str, mapping: dict[str, str]) -> None:
    """Feature: catalog-complex-types, Property 3: PostgreSQL Array Syntax

    For any primitive type in the mapping, appending [] should produce
    list<arrow_type> where arrow_type is the mapped primitive.

    Validates: Requirements 2.1.4, 2.1.5, 2.1.6
    """
    native_type = f"{primitive}[]"
    expected = f"list<{mapping[primitive]}>"
    result = parse_type(native_type, mapping)
    assert result == expected, f"Expected {expected}, got {result} for {native_type}"


# ============================================================================
# Property 4: Struct Type Parsing
# ============================================================================


@given(fields=struct_fields(primitive_mapping().example()), mapping=primitive_mapping())
@settings(max_examples=100)
@pytest.mark.property
def test_property_struct_parsing(fields: list[tuple[str, str]], mapping: dict[str, str]) -> None:
    """Feature: catalog-complex-types, Property 4: Struct Type Parsing

    For any collection of field name and type pairs, formatting them as
    struct<field1:type1,field2:type2,...> should produce a struct type
    with all fields correctly mapped and order preserved.

    Validates: Requirements 3.1, 3.2, 3.3, 3.5, 3.6
    """
    # Build native type string
    field_strs = [f"{name}:{type_name}" for name, type_name in fields]
    native_type = f"struct<{','.join(field_strs)}>"

    # Build expected Arrow type
    expected_fields = [f"{name}:{mapping[type_name]}" for name, type_name in fields]
    expected = f"struct<{','.join(expected_fields)}>"

    result = parse_type(native_type, mapping)
    assert result == expected, f"Expected {expected}, got {result} for {native_type}"


@given(fields=struct_fields(primitive_mapping().example()), mapping=primitive_mapping())
@settings(max_examples=100)
@pytest.mark.property
def test_property_struct_field_order_preservation(
    fields: list[tuple[str, str]], mapping: dict[str, str]
) -> None:
    """Feature: catalog-complex-types, Property 4: Struct Field Order Preservation

    Struct field order should be preserved during parsing.

    Validates: Requirements 3.5
    """
    # Build native type string
    field_strs = [f"{name}:{type_name}" for name, type_name in fields]
    native_type = f"struct<{','.join(field_strs)}>"

    result = parse_type(native_type, mapping)

    # Extract field names from result
    # Result format: struct<field1:type1,field2:type2,...>
    result_content = result[7:-1]  # Remove "struct<" and ">"
    result_fields = []
    for field_str in result_content.split(","):
        field_name = field_str.split(":")[0]
        result_fields.append(field_name)

    # Extract expected field names
    expected_fields = [name for name, _ in fields]

    assert result_fields == expected_fields, (
        f"Field order not preserved: expected {expected_fields}, got {result_fields}"
    )


# ============================================================================
# Property 5: Nested Complex Types
# ============================================================================


@given(
    primitive=primitive_type_name(),
    mapping=primitive_mapping(),
    depth=st.integers(min_value=1, max_value=10),
)
@settings(max_examples=100)
@pytest.mark.property
def test_property_nested_array_depth(primitive: str, mapping: dict[str, str], depth: int) -> None:
    """Feature: catalog-complex-types, Property 5: Nested Complex Types

    For any valid complex type, nesting it within arrays should parse
    correctly at arbitrary depths up to 10 levels.

    Validates: Requirements 4.1, 4.2, 4.3, 4.5
    """
    # Build nested array type
    native_type = primitive
    expected = mapping[primitive]
    for _ in range(depth):
        native_type = f"array<{native_type}>"
        expected = f"list<{expected}>"

    result = parse_type(native_type, mapping)
    assert result == expected, (
        f"Expected {expected}, got {result} for {native_type} at depth {depth}"
    )


@given(primitive=primitive_type_name(), mapping=primitive_mapping())
@settings(max_examples=100)
@pytest.mark.property
def test_property_array_of_struct(primitive: str, mapping: dict[str, str]) -> None:
    """Feature: catalog-complex-types, Property 5: Array of Struct

    Arrays of structs should parse correctly.

    Validates: Requirements 4.1
    """
    native_type = f"array<struct<field:{primitive}>>"
    expected = f"list<struct<field:{mapping[primitive]}>>"
    result = parse_type(native_type, mapping)
    assert result == expected, f"Expected {expected}, got {result} for {native_type}"


@given(primitive=primitive_type_name(), mapping=primitive_mapping())
@settings(max_examples=100)
@pytest.mark.property
def test_property_struct_with_array_field(primitive: str, mapping: dict[str, str]) -> None:
    """Feature: catalog-complex-types, Property 5: Struct with Array Field

    Structs with array fields should parse correctly.

    Validates: Requirements 4.2
    """
    native_type = f"struct<items:array<{primitive}>>"
    expected = f"struct<items:list<{mapping[primitive]}>>"
    result = parse_type(native_type, mapping)
    assert result == expected, f"Expected {expected}, got {result} for {native_type}"


@given(mapping=primitive_mapping())
@settings(max_examples=100)
@pytest.mark.property
def test_property_depth_limit_protection(mapping: dict[str, str]) -> None:
    """Feature: catalog-complex-types, Property 5: Depth Limit Protection

    Types nested beyond 10 levels should default to large_utf8 with warning.

    Validates: Requirements 4.3, 4.5
    """
    # Build a type nested 12 levels deep (depth counter starts at 0, so we need 12 to exceed 10)
    native_type = "int"
    for _ in range(12):
        native_type = f"array<{native_type}>"

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = parse_type(native_type, mapping)

        # The outermost arrays should parse, but the innermost should hit depth limit
        # At depth 11, it should return large_utf8
        # So we expect: list<list<list<list<list<list<list<list<list<list<list<large_utf8>>>>>>>>>>>
        assert "large_utf8" in result, (
            f"Expected large_utf8 somewhere in deeply nested type, got {result}"
        )

        # Should issue a warning
        assert len(w) > 0, "Expected warning for deeply nested type"
        assert "depth exceeded" in str(w[0].message).lower(), (
            f"Expected depth warning, got {w[0].message}"
        )


# ============================================================================
# Property 6: Error Handling Never Raises Exceptions
# ============================================================================


@given(malformed=malformed_type(), mapping=primitive_mapping())
@settings(max_examples=100)
@pytest.mark.property
def test_property_error_handling_no_exceptions(malformed: str, mapping: dict[str, str]) -> None:
    """Feature: catalog-complex-types, Property 6: Error Handling Never Raises Exceptions

    For any input string (including malformed, empty, or null), calling parse_type()
    should never raise an exception and should always return a valid Arrow type string.

    Validates: Requirements 5.1, 5.2, 5.4, 5.5
    """
    # Should not raise any exception
    try:
        result = parse_type(malformed, mapping)
        # Should return a valid string
        assert isinstance(result, str), f"Expected string result, got {type(result)}"
        # Should not be empty
        assert result, "Result should not be empty string"
    except Exception as e:
        pytest.fail(f"parse_type raised exception for input '{malformed}': {e}")


@given(mapping=primitive_mapping())
@settings(max_examples=100)
@pytest.mark.property
def test_property_empty_input_handling(mapping: dict[str, str]) -> None:
    """Feature: catalog-complex-types, Property 6: Empty Input Handling

    Empty or null input should return large_utf8 without warnings.

    Validates: Requirements 5.2
    """
    for empty_input in [None, "", "   ", "\t", "\n"]:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = parse_type(empty_input, mapping)

            assert result == "large_utf8", (
                f"Expected large_utf8 for empty input '{empty_input}', got {result}"
            )
            # Should not issue warnings for empty input
            assert len(w) == 0, f"Expected no warnings for empty input, got {len(w)} warnings"


# ============================================================================
# Property 7: Whitespace Normalization
# ============================================================================


@given(primitive=primitive_type_name(), mapping=primitive_mapping())
@settings(max_examples=100)
@pytest.mark.property
def test_property_whitespace_normalization_array(primitive: str, mapping: dict[str, str]) -> None:
    """Feature: catalog-complex-types, Property 7: Whitespace Normalization

    For any valid type string, adding or removing whitespace around delimiters
    should produce the same parsed Arrow type.

    Validates: Requirements 7.3
    """
    # Test various whitespace patterns for array types
    variants = [
        f"array<{primitive}>",
        f"array< {primitive} >",
        f"array<  {primitive}  >",
        f"array<\t{primitive}\t>",
        f"array < {primitive} >",
    ]

    expected = f"list<{mapping[primitive]}>"
    for variant in variants:
        result = parse_type(variant, mapping)
        assert result == expected, (
            f"Whitespace variant '{variant}' produced {result}, expected {expected}"
        )


@given(primitive=primitive_type_name(), mapping=primitive_mapping())
@settings(max_examples=100)
@pytest.mark.property
def test_property_whitespace_normalization_struct(primitive: str, mapping: dict[str, str]) -> None:
    """Feature: catalog-complex-types, Property 7: Whitespace Normalization (Struct)

    Whitespace normalization should work for struct types.

    Validates: Requirements 7.3
    """
    # Test various whitespace patterns for struct types
    variants = [
        f"struct<field:{primitive}>",
        f"struct< field : {primitive} >",
        f"struct<  field  :  {primitive}  >",
        f"struct<\tfield\t:\t{primitive}\t>",
        f"struct < field : {primitive} >",
    ]

    expected = f"struct<field:{mapping[primitive]}>"
    for variant in variants:
        result = parse_type(variant, mapping)
        assert result == expected, (
            f"Whitespace variant '{variant}' produced {result}, expected {expected}"
        )


# ============================================================================
# Property 8: Parser Idempotence
# ============================================================================


@given(primitive=primitive_type_name(), mapping=primitive_mapping())
@settings(max_examples=100)
@pytest.mark.property
def test_property_parser_idempotence_primitive(primitive: str, mapping: dict[str, str]) -> None:
    """Feature: catalog-complex-types, Property 8: Parser Idempotence

    For any valid native type string, parsing it multiple times with the same
    primitive mapping should always produce identical results.

    Validates: Requirements 7.1, 7.5
    """
    # Parse the same type multiple times
    results = [parse_type(primitive, mapping) for _ in range(5)]

    # All results should be identical
    assert all(r == results[0] for r in results), (
        f"Parser not idempotent: got different results {results}"
    )


@given(primitive=primitive_type_name(), mapping=primitive_mapping())
@settings(max_examples=100)
@pytest.mark.property
def test_property_parser_idempotence_array(primitive: str, mapping: dict[str, str]) -> None:
    """Feature: catalog-complex-types, Property 8: Parser Idempotence (Array)

    Parser idempotence for array types.

    Validates: Requirements 7.1, 7.5
    """
    native_type = f"array<{primitive}>"

    # Parse the same type multiple times
    results = [parse_type(native_type, mapping) for _ in range(5)]

    # All results should be identical
    assert all(r == results[0] for r in results), (
        f"Parser not idempotent for arrays: got different results {results}"
    )


@given(primitive=primitive_type_name(), mapping=primitive_mapping())
@settings(max_examples=100)
@pytest.mark.property
def test_property_parser_idempotence_struct(primitive: str, mapping: dict[str, str]) -> None:
    """Feature: catalog-complex-types, Property 8: Parser Idempotence (Struct)

    Parser idempotence for struct types.

    Validates: Requirements 7.1, 7.5
    """
    native_type = f"struct<field:{primitive}>"

    # Parse the same type multiple times
    results = [parse_type(native_type, mapping) for _ in range(5)]

    # All results should be identical
    assert all(r == results[0] for r in results), (
        f"Parser not idempotent for structs: got different results {results}"
    )
