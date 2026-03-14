"""Integration tests for catalog complex type support.

Tests verify that catalog plugins correctly parse complex types (arrays, structs)
during schema introspection and produce proper Arrow types without warnings.
"""

import tempfile
import warnings
from pathlib import Path

import duckdb
import pytest

from rivet_duckdb.catalog import DuckDBCatalogPlugin


@pytest.fixture
def temp_duckdb():
    """Create a temporary DuckDB database with complex type tables."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        conn = duckdb.connect(str(db_path))

        # Create tables with various complex types
        conn.execute("""
            CREATE TABLE array_types (
                id INTEGER,
                tags VARCHAR[],
                scores INTEGER[],
                nested_arrays INTEGER[][]
            )
        """)

        conn.execute("""
            CREATE TABLE struct_types (
                id INTEGER,
                person STRUCT(name VARCHAR, age INTEGER),
                address STRUCT(street VARCHAR, city VARCHAR, zip INTEGER)
            )
        """)

        conn.execute("""
            CREATE TABLE nested_complex_types (
                id INTEGER,
                users STRUCT(name VARCHAR, age INTEGER)[],
                metadata STRUCT(tags VARCHAR[], count INTEGER)
            )
        """)

        conn.execute("""
            CREATE TABLE primitive_types (
                id INTEGER,
                name VARCHAR,
                score DOUBLE,
                active BOOLEAN
            )
        """)

        conn.close()
        yield db_path


def test_duckdb_array_types(temp_duckdb):
    """Test DuckDB catalog correctly parses array types."""
    plugin = DuckDBCatalogPlugin()
    catalog = plugin.instantiate("test_catalog", {"path": str(temp_duckdb)})

    # Capture warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        schema = plugin.get_schema(catalog, "array_types")

    # Verify no warnings for supported array types
    assert len(w) == 0, f"Unexpected warnings: {[str(warning.message) for warning in w]}"

    # Verify column types
    columns = {col.name: col.type for col in schema.columns}

    assert columns["id"] == "int32"
    assert columns["tags"] == "list<large_utf8>"
    assert columns["scores"] == "list<int32>"
    assert columns["nested_arrays"] == "list<list<int32>>"


def test_duckdb_struct_types(temp_duckdb):
    """Test DuckDB catalog correctly parses struct types."""
    plugin = DuckDBCatalogPlugin()
    catalog = plugin.instantiate("test_catalog", {"path": str(temp_duckdb)})

    # Capture warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        schema = plugin.get_schema(catalog, "struct_types")

    # Verify no warnings for supported struct types
    assert len(w) == 0, f"Unexpected warnings: {[str(warning.message) for warning in w]}"

    # Verify column types
    columns = {col.name: col.type for col in schema.columns}

    assert columns["id"] == "int32"
    assert columns["person"] == "struct<name:large_utf8,age:int32>"
    assert columns["address"] == "struct<street:large_utf8,city:large_utf8,zip:int32>"


def test_duckdb_nested_complex_types(temp_duckdb):
    """Test DuckDB catalog correctly parses nested complex types."""
    plugin = DuckDBCatalogPlugin()
    catalog = plugin.instantiate("test_catalog", {"path": str(temp_duckdb)})

    # Capture warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        schema = plugin.get_schema(catalog, "nested_complex_types")

    # Verify no warnings for supported nested types
    assert len(w) == 0, f"Unexpected warnings: {[str(warning.message) for warning in w]}"

    # Verify column types
    columns = {col.name: col.type for col in schema.columns}

    assert columns["id"] == "int32"
    assert columns["users"] == "list<struct<name:large_utf8,age:int32>>"
    assert columns["metadata"] == "struct<tags:list<large_utf8>,count:int32>"


def test_duckdb_primitive_types_no_warnings(temp_duckdb):
    """Test DuckDB catalog produces no warnings for primitive types."""
    plugin = DuckDBCatalogPlugin()
    catalog = plugin.instantiate("test_catalog", {"path": str(temp_duckdb)})

    # Capture warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        schema = plugin.get_schema(catalog, "primitive_types")

    # Verify no warnings
    assert len(w) == 0, f"Unexpected warnings: {[str(warning.message) for warning in w]}"

    # Verify column types
    columns = {col.name: col.type for col in schema.columns}

    assert columns["id"] == "int32"
    assert columns["name"] == "large_utf8"
    assert columns["score"] == "float64"
    assert columns["active"] == "bool"


def test_duckdb_complex_types_preserve_native_type(temp_duckdb):
    """Test DuckDB catalog preserves native type information for complex types."""
    plugin = DuckDBCatalogPlugin()
    catalog = plugin.instantiate("test_catalog", {"path": str(temp_duckdb)})

    schema = plugin.get_schema(catalog, "array_types")

    # Verify native types are preserved
    for col in schema.columns:
        assert col.native_type is not None
        assert isinstance(col.native_type, str)
        assert len(col.native_type) > 0


def test_duckdb_list_tables_with_complex_types(temp_duckdb):
    """Test DuckDB catalog list_tables works with complex type tables."""
    plugin = DuckDBCatalogPlugin()
    catalog = plugin.instantiate("test_catalog", {"path": str(temp_duckdb)})

    # Capture warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        nodes = plugin.list_tables(catalog)

    # Verify no warnings
    assert len(w) == 0, f"Unexpected warnings: {[str(warning.message) for warning in w]}"

    # Verify all tables are listed
    table_names = {node.name for node in nodes}
    assert "array_types" in table_names
    assert "struct_types" in table_names
    assert "nested_complex_types" in table_names
    assert "primitive_types" in table_names


def test_duckdb_get_metadata_with_complex_types(temp_duckdb):
    """Test DuckDB catalog get_metadata works with complex type tables."""
    plugin = DuckDBCatalogPlugin()
    catalog = plugin.instantiate("test_catalog", {"path": str(temp_duckdb)})

    # Capture warnings
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        metadata = plugin.get_metadata(catalog, "nested_complex_types")

    # Verify no warnings
    assert len(w) == 0, f"Unexpected warnings: {[str(warning.message) for warning in w]}"

    # Verify metadata is returned
    assert metadata is not None
    assert metadata.node_type == "table"
    assert metadata.format == "duckdb"
