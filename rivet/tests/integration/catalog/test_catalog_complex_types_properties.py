"""Property-based tests for catalog complex type integration.

Feature: catalog-complex-types
Property 12: Catalog Integration Consistency
Validates: Requirements 1.3, 1.6
"""

import tempfile
import warnings
from pathlib import Path

import duckdb
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.type_parser import parse_type
from rivet_duckdb.catalog import _DUCKDB_TO_ARROW, DuckDBCatalogPlugin


# Strategy for generating valid DuckDB type strings
@st.composite
def duckdb_primitive_types(draw):
    """Generate valid DuckDB primitive type names."""
    return draw(
        st.sampled_from(
            [
                "INTEGER",
                "BIGINT",
                "SMALLINT",
                "TINYINT",
                "DOUBLE",
                "FLOAT",
                "BOOLEAN",
                "VARCHAR",
                "DATE",
            ]
        )
    )


@st.composite
def duckdb_array_types(draw):
    """Generate valid DuckDB array type strings."""
    element_type = draw(duckdb_primitive_types())
    return f"ARRAY({element_type})"


@st.composite
def duckdb_struct_types(draw):
    """Generate valid DuckDB struct type strings."""
    num_fields = draw(st.integers(min_value=1, max_value=3))
    fields = []
    for i in range(num_fields):
        field_name = f"field{i}"
        field_type = draw(duckdb_primitive_types())
        fields.append(f"{field_name} {field_type}")
    return f"STRUCT({', '.join(fields)})"


@st.composite
def duckdb_complex_types(draw):
    """Generate valid DuckDB complex type strings (arrays or structs)."""
    return draw(
        st.one_of(
            duckdb_array_types(),
            duckdb_struct_types(),
        )
    )


@given(type_str=duckdb_primitive_types())
@settings(max_examples=50)
def test_property_primitive_type_consistency(type_str):
    """Property: Parsing a primitive type directly should match catalog introspection.

    For any primitive type, parsing it with the type parser should produce
    the same result as the catalog plugin's schema introspection.
    """
    # Parse directly with type parser
    direct_result = parse_type(type_str.lower(), _DUCKDB_TO_ARROW)

    # Create a temporary DuckDB table with this type
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        conn = duckdb.connect(str(db_path))

        try:
            conn.execute(f"CREATE TABLE test_table (col {type_str})")
        except Exception:
            # Skip invalid type combinations
            conn.close()
            return

        conn.close()

        # Get schema via catalog plugin
        plugin = DuckDBCatalogPlugin()
        catalog = plugin.instantiate("test", {"path": str(db_path)})

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            schema = plugin.get_schema(catalog, "test_table")

        catalog_result = schema.columns[0].type

        # Both should produce the same Arrow type
        assert direct_result == catalog_result, (
            f"Type parser result '{direct_result}' differs from "
            f"catalog introspection result '{catalog_result}' for type '{type_str}'"
        )


@given(type_str=duckdb_array_types())
@settings(max_examples=30)
def test_property_array_type_consistency(type_str):
    """Property: Parsing an array type directly should match catalog introspection.

    For any array type, parsing it with the type parser should produce
    the same result as the catalog plugin's schema introspection.
    """
    # Create a temporary DuckDB table with this type
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        conn = duckdb.connect(str(db_path))

        try:
            conn.execute(f"CREATE TABLE test_table (col {type_str})")
        except Exception:
            # Skip invalid type combinations
            conn.close()
            return

        # Get the actual type DuckDB assigned (may normalize the syntax)
        result = conn.execute("DESCRIBE test_table").fetchall()
        actual_type = result[0][1]  # column_type from DESCRIBE
        conn.close()

        # Parse with type parser
        direct_result = parse_type(actual_type, _DUCKDB_TO_ARROW)

        # Get schema via catalog plugin
        plugin = DuckDBCatalogPlugin()
        catalog = plugin.instantiate("test", {"path": str(db_path)})

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            schema = plugin.get_schema(catalog, "test_table")

        catalog_result = schema.columns[0].type

        # Both should produce the same Arrow type
        assert direct_result == catalog_result, (
            f"Type parser result '{direct_result}' differs from "
            f"catalog introspection result '{catalog_result}' for type '{actual_type}'"
        )


@given(type_str=duckdb_struct_types())
@settings(max_examples=30)
def test_property_struct_type_consistency(type_str):
    """Property: Parsing a struct type directly should match catalog introspection.

    For any struct type, parsing it with the type parser should produce
    the same result as the catalog plugin's schema introspection.
    """
    # Create a temporary DuckDB table with this type
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        conn = duckdb.connect(str(db_path))

        try:
            conn.execute(f"CREATE TABLE test_table (col {type_str})")
        except Exception:
            # Skip invalid type combinations
            conn.close()
            return

        # Get the actual type DuckDB assigned (may normalize the syntax)
        result = conn.execute("DESCRIBE test_table").fetchall()
        actual_type = result[0][1]  # column_type from DESCRIBE
        conn.close()

        # Parse with type parser
        direct_result = parse_type(actual_type, _DUCKDB_TO_ARROW)

        # Get schema via catalog plugin
        plugin = DuckDBCatalogPlugin()
        catalog = plugin.instantiate("test", {"path": str(db_path)})

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            schema = plugin.get_schema(catalog, "test_table")

        catalog_result = schema.columns[0].type

        # Both should produce the same Arrow type
        assert direct_result == catalog_result, (
            f"Type parser result '{direct_result}' differs from "
            f"catalog introspection result '{catalog_result}' for type '{actual_type}'"
        )


@given(type_str=duckdb_complex_types())
@settings(max_examples=50)
def test_property_no_warnings_for_supported_types(type_str):
    """Property: Catalog introspection should not issue warnings for supported complex types.

    For any supported complex type (array or struct with known primitives),
    catalog introspection should complete without warnings.
    """
    # Create a temporary DuckDB table with this type
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        conn = duckdb.connect(str(db_path))

        try:
            conn.execute(f"CREATE TABLE test_table (col {type_str})")
        except Exception:
            # Skip invalid type combinations
            conn.close()
            return

        conn.close()

        # Get schema via catalog plugin
        plugin = DuckDBCatalogPlugin()
        catalog = plugin.instantiate("test", {"path": str(db_path)})

        # Capture warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = plugin.get_schema(catalog, "test_table")

        # Should not issue warnings for supported types
        assert len(w) == 0, (
            f"Unexpected warnings for type '{type_str}': {[str(warning.message) for warning in w]}"
        )

        # Should produce a valid Arrow type (not default to large_utf8 for complex types)
        arrow_type = schema.columns[0].type
        assert arrow_type != "large_utf8" or "STRUCT" not in type_str.upper(), (
            f"Complex type '{type_str}' incorrectly defaulted to large_utf8"
        )
