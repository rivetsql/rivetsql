"""Centralized type parser for catalog complex types.

This module provides a recursive parser for complex type syntax (arrays and structs)
used by various catalog systems. It supports:
- Standard array syntax: array<T>
- PostgreSQL array syntax: type[]
- Struct syntax: struct<field:type,...>
- Nested complex types with arbitrary depth

The parser is designed to be fail-safe: invalid input defaults to large_utf8
with warnings, and never raises exceptions that would break schema introspection.
"""

from __future__ import annotations

import logging
import re
import warnings

_logger = logging.getLogger(__name__)


def parse_type(
    native_type: str | None,
    primitive_mapping: dict[str, str],
    *,
    warn_on_unknown: bool = True,
    _depth: int = 0,
) -> str:
    """Parse a catalog native type string to an Arrow type string.

    This function recursively parses complex type syntax from various catalog systems
    and converts them to Arrow type representations. It handles:
    - Primitive types (mapped via primitive_mapping)
    - Arrays: array<T> or type[] (PostgreSQL)
    - Structs: struct<field:type,...>
    - Nested complex types

    Args:
        native_type: The native type string from the catalog (e.g., "array<string>", "integer[]")
        primitive_mapping: Dict mapping lowercase primitive type names to Arrow type names
        warn_on_unknown: Whether to issue warnings for unknown types (default: True)
        _depth: Internal recursion depth counter (do not set manually)

    Returns:
        Arrow type string (e.g., "list<large_utf8>", "struct<field:int32>")
        Defaults to "large_utf8" for unparseable or unknown types.

    Examples:
        >>> mapping = {"string": "large_utf8", "int": "int32"}
        >>> parse_type("array<string>", mapping)
        "list<large_utf8>"

        >>> parse_type("integer[]", {"integer": "int32"})
        "list<int32>"

        >>> parse_type("struct<name:string,age:int>", {"string": "large_utf8", "int": "int32"})
        "struct<name:large_utf8,age:int32>"

    Note:
        This function never raises exceptions. Invalid input defaults to "large_utf8"
        with optional warnings.
    """
    # Depth limit protection (max 10 levels)
    if _depth > 10:
        if warn_on_unknown:
            warnings.warn(
                f"Type nesting depth exceeded 10 levels; defaulting to large_utf8. "
                f"Type: {native_type}",
                stacklevel=2,
            )
        return "large_utf8"

    # Handle empty/null input (no warning per requirement 5.3)
    if not native_type or not native_type.strip():
        return "large_utf8"

    # Normalize whitespace and handle potential errors
    try:
        normalized = _normalize_whitespace(native_type)
        normalized_lower = normalized.lower()
    except Exception:
        # If normalization fails, default to large_utf8 with warning
        if warn_on_unknown:
            warnings.warn(
                f"Failed to normalize type string '{native_type}'; defaulting to large_utf8.",
                stacklevel=2,
            )
        return "large_utf8"

    # Detect PostgreSQL array syntax: type[]
    if normalized.endswith("[]"):
        base_type = normalized[:-2].strip()
        return _parse_postgres_array(base_type, primitive_mapping, warn_on_unknown, _depth)

    # Detect standard array syntax: array<T>
    if normalized_lower.startswith("array<"):
        if not normalized.endswith(">"):
            if warn_on_unknown:
                warnings.warn(
                    f"Malformed array type (unmatched brackets): '{native_type}'; "
                    f"defaulting to large_utf8.",
                    stacklevel=2,
                )
            return "large_utf8"
        content = normalized[6:-1].strip()  # Extract content between array< and >
        return _parse_array(content, primitive_mapping, warn_on_unknown, _depth)

    # Detect struct syntax: struct<field:type,...> or STRUCT(field type, ...)
    if normalized_lower.startswith("struct<"):
        if not normalized.endswith(">"):
            if warn_on_unknown:
                warnings.warn(
                    f"Malformed struct type (unmatched brackets): '{native_type}'; "
                    f"defaulting to large_utf8.",
                    stacklevel=2,
                )
            return "large_utf8"
        content = normalized[7:-1].strip()  # Extract content between struct< and >
        return _parse_struct(content, primitive_mapping, warn_on_unknown, _depth, syntax="angle")

    if normalized_lower.startswith("struct("):
        if not normalized.endswith(")"):
            if warn_on_unknown:
                warnings.warn(
                    f"Malformed struct type (unmatched parentheses): '{native_type}'; "
                    f"defaulting to large_utf8.",
                    stacklevel=2,
                )
            return "large_utf8"
        content = normalized[7:-1].strip()  # Extract content between struct( and )
        return _parse_struct(content, primitive_mapping, warn_on_unknown, _depth, syntax="paren")

    # Otherwise, treat as primitive type
    return _map_primitive(normalized, primitive_mapping, warn_on_unknown)


def _normalize_whitespace(type_str: str) -> str:
    """Remove extra whitespace around delimiters.

    Handles spaces, tabs, and multiple spaces around commas, colons, and angle brackets.
    Preserves single spaces inside STRUCT() parentheses for DuckDB syntax.

    Args:
        type_str: The type string to normalize

    Returns:
        Normalized type string with consistent whitespace
    """
    # Replace tabs and multiple spaces with single space
    normalized = re.sub(r"\s+", " ", type_str)
    # Remove spaces around angle brackets, colons, and commas
    # But be careful not to remove spaces inside STRUCT() which uses "field TYPE" syntax
    normalized = re.sub(r"\s*([<>:,])\s*", r"\1", normalized)
    # Remove spaces after opening parentheses and before closing parentheses
    normalized = re.sub(r"\(\s+", "(", normalized)
    normalized = re.sub(r"\s+\)", ")", normalized)
    return normalized.strip()


def _is_primitive(type_str: str, primitive_mapping: dict[str, str]) -> bool:
    """Check if a type is a primitive (exists in mapping).

    Args:
        type_str: The type string to check
        primitive_mapping: Dict mapping primitive type names to Arrow types

    Returns:
        True if the type is in the primitive mapping, False otherwise
    """
    lower = type_str.lower().strip()
    # Strip parameters for parameterized types like decimal(10,2)
    base = lower.split("(")[0].strip()
    return base in primitive_mapping


def _map_primitive(
    type_str: str,
    primitive_mapping: dict[str, str],
    warn_on_unknown: bool,
) -> str:
    """Map a primitive type, handling parameterized types like decimal(10,2).

    Args:
        type_str: The primitive type string
        primitive_mapping: Dict mapping primitive type names to Arrow types
        warn_on_unknown: Whether to warn on unknown types

    Returns:
        Arrow type string, or "large_utf8" if unknown
    """
    lower = type_str.lower().strip()
    # Strip parameters for parameterized types like decimal(10,2), varchar(255)
    base = lower.split("(")[0].strip()

    if base in primitive_mapping:
        return primitive_mapping[base]

    # Unknown primitive type
    if warn_on_unknown and type_str:
        warnings.warn(
            f"Unknown primitive type '{type_str}'; defaulting to large_utf8.",
            stacklevel=3,
        )
    return "large_utf8"


def _parse_array(
    content: str,
    primitive_mapping: dict[str, str],
    warn_on_unknown: bool,
    depth: int,
) -> str:
    """Parse array<T> syntax recursively.

    Args:
        content: The content between array< and >
        primitive_mapping: Dict mapping primitive type names to Arrow types
        warn_on_unknown: Whether to warn on unknown types
        depth: Current recursion depth

    Returns:
        Arrow list type string (e.g., "list<int32>")
    """
    if not content or not content.strip():
        if warn_on_unknown:
            warnings.warn(
                "Empty array type content; defaulting to list<large_utf8>.",
                stacklevel=3,
            )
        return "list<large_utf8>"

    # Recursively parse the element type
    try:
        element_type = parse_type(
            content, primitive_mapping, warn_on_unknown=warn_on_unknown, _depth=depth + 1
        )
        return f"list<{element_type}>"
    except Exception:
        # Catch any unexpected errors during recursive parsing
        if warn_on_unknown:
            warnings.warn(
                f"Failed to parse array element type '{content}'; defaulting to list<large_utf8>.",
                stacklevel=3,
            )
        return "list<large_utf8>"


def _parse_postgres_array(
    base_type: str,
    primitive_mapping: dict[str, str],
    warn_on_unknown: bool,
    depth: int,
) -> str:
    """Parse PostgreSQL type[] syntax.

    Args:
        base_type: The base type before []
        primitive_mapping: Dict mapping primitive type names to Arrow types
        warn_on_unknown: Whether to warn on unknown types
        depth: Current recursion depth

    Returns:
        Arrow list type string (e.g., "list<int32>")
    """
    if not base_type or not base_type.strip():
        if warn_on_unknown:
            warnings.warn(
                "Empty PostgreSQL array base type; defaulting to list<large_utf8>.",
                stacklevel=3,
            )
        return "list<large_utf8>"

    # Recursively parse the base type
    try:
        element_type = parse_type(
            base_type, primitive_mapping, warn_on_unknown=warn_on_unknown, _depth=depth + 1
        )
        return f"list<{element_type}>"
    except Exception:
        # Catch any unexpected errors during recursive parsing
        if warn_on_unknown:
            warnings.warn(
                f"Failed to parse PostgreSQL array base type '{base_type}'; defaulting to list<large_utf8>.",
                stacklevel=3,
            )
        return "list<large_utf8>"


def _parse_struct(
    content: str,
    primitive_mapping: dict[str, str],
    warn_on_unknown: bool,
    depth: int,
    *,
    syntax: str = "angle",
) -> str:
    """Parse struct<field:type,...> or STRUCT(field type,...) syntax recursively.

    Args:
        content: The content between struct delimiters
        primitive_mapping: Dict mapping primitive type names to Arrow types
        warn_on_unknown: Whether to warn on unknown types
        depth: Current recursion depth
        syntax: "angle" for struct<field:type> or "paren" for STRUCT(field type)

    Returns:
        Arrow struct type string (e.g., "struct<name:large_utf8,age:int32>")
    """
    if not content or not content.strip():
        if warn_on_unknown:
            warnings.warn(
                "Empty struct type content; defaulting to large_utf8.",
                stacklevel=3,
            )
        return "large_utf8"

    # Split fields respecting nested brackets
    try:
        if syntax == "paren":
            fields = _split_struct_fields_paren(content)
        else:
            fields = _split_struct_fields(content)
    except Exception:
        # Catch any unexpected errors during field splitting
        if warn_on_unknown:
            warnings.warn(
                f"Failed to parse struct fields in 'struct<{content}>'; defaulting to large_utf8.",
                stacklevel=3,
            )
        return "large_utf8"

    if not fields:
        if warn_on_unknown:
            warnings.warn(
                f"Invalid struct syntax (no fields found): 'struct<{content}>'; "
                f"defaulting to large_utf8.",
                stacklevel=3,
            )
        return "large_utf8"

    parsed_fields: list[str] = []
    for field in fields:
        if syntax == "paren":
            # DuckDB syntax: "field_name" TYPE or field_name TYPE
            # Split on first space to separate name from type
            parts = field.split(None, 1)
            if len(parts) != 2:
                if warn_on_unknown:
                    warnings.warn(
                        f"Invalid struct field syntax: '{field}'; defaulting to large_utf8.",
                        stacklevel=3,
                    )
                return "large_utf8"
            field_name = parts[0].strip()
            field_type = parts[1].strip()
        else:
            # Standard syntax: field:type
            if ":" not in field:
                if warn_on_unknown:
                    warnings.warn(
                        f"Invalid struct field syntax (missing colon): '{field}'; "
                        f"defaulting to large_utf8.",
                        stacklevel=3,
                    )
                return "large_utf8"

            # Split only on the first colon to handle nested types with colons
            parts = field.split(":", 1)
            if len(parts) != 2:
                if warn_on_unknown:
                    warnings.warn(
                        f"Invalid struct field syntax: '{field}'; defaulting to large_utf8.",
                        stacklevel=3,
                    )
                return "large_utf8"

            field_name = parts[0].strip()
            field_type = parts[1].strip()

        # Strip quotes from field names (DuckDB may quote field names)
        if (
            field_name.startswith('"')
            and field_name.endswith('"')
            or field_name.startswith("'")
            and field_name.endswith("'")
        ):
            field_name = field_name[1:-1]

        if not field_name:
            if warn_on_unknown:
                warnings.warn(
                    f"Invalid struct field (empty field name): '{field}'; "
                    f"defaulting to large_utf8.",
                    stacklevel=3,
                )
            return "large_utf8"

        if not field_type:
            if warn_on_unknown:
                warnings.warn(
                    f"Invalid struct field (empty field type): '{field}'; "
                    f"defaulting to large_utf8.",
                    stacklevel=3,
                )
            return "large_utf8"

        # Recursively parse the field type
        try:
            arrow_field_type = parse_type(
                field_type, primitive_mapping, warn_on_unknown=warn_on_unknown, _depth=depth + 1
            )
            parsed_fields.append(f"{field_name}:{arrow_field_type}")
        except Exception:
            # Catch any unexpected errors during recursive parsing
            if warn_on_unknown:
                warnings.warn(
                    f"Failed to parse struct field type '{field_type}' for field '{field_name}'; "
                    f"defaulting to large_utf8.",
                    stacklevel=3,
                )
            return "large_utf8"

    return f"struct<{','.join(parsed_fields)}>"


def _split_struct_fields(content: str) -> list[str]:
    """Split struct fields on commas, respecting nested brackets.

    Args:
        content: The struct content to split

    Returns:
        List of field strings

    Raises:
        ValueError: If bracket nesting is malformed (negative depth)
    """
    fields: list[str] = []
    current_field: list[str] = []
    bracket_depth = 0

    for char in content:
        if char in "<(":
            bracket_depth += 1
            current_field.append(char)
        elif char in ">)":
            bracket_depth -= 1
            # Check for unmatched closing brackets
            if bracket_depth < 0:
                raise ValueError(f"Unmatched closing bracket in struct content: {content}")
            current_field.append(char)
        elif char == "," and bracket_depth == 0:
            # Top-level comma - field separator
            field_str = "".join(current_field).strip()
            if field_str:
                fields.append(field_str)
            current_field = []
        else:
            current_field.append(char)

    # Add the last field
    field_str = "".join(current_field).strip()
    if field_str:
        fields.append(field_str)

    # Check for unmatched opening brackets
    if bracket_depth != 0:
        raise ValueError(f"Unmatched opening bracket in struct content: {content}")

    return fields


def _split_struct_fields_paren(content: str) -> list[str]:
    """Split DuckDB-style struct fields on commas, respecting nested parentheses and brackets.

    DuckDB uses STRUCT(field_name TYPE, field_name TYPE) syntax.

    Args:
        content: The struct content to split

    Returns:
        List of field strings

    Raises:
        ValueError: If bracket/parenthesis nesting is malformed
    """
    fields: list[str] = []
    current_field: list[str] = []
    depth = 0

    for char in content:
        if char in "<([":
            depth += 1
            current_field.append(char)
        elif char in ">)]":
            depth -= 1
            if depth < 0:
                raise ValueError(f"Unmatched closing bracket/paren in struct content: {content}")
            current_field.append(char)
        elif char == "," and depth == 0:
            # Top-level comma - field separator
            field_str = "".join(current_field).strip()
            if field_str:
                fields.append(field_str)
            current_field = []
        else:
            current_field.append(char)

    # Add the last field
    field_str = "".join(current_field).strip()
    if field_str:
        fields.append(field_str)

    if depth != 0:
        raise ValueError(f"Unmatched opening bracket/paren in struct content: {content}")

    return fields
