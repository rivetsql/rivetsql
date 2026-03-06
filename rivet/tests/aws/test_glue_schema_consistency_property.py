"""Property test for Glue schema consistency.

Feature: cross-storage-adapters, Property 18: Glue schema consistency
Generate mock Glue StorageDescriptor schemas and corresponding Parquet data;
verify compatibility between the schema returned by get_schema() and the Parquet data.
Validates: Requirements 12.2
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_aws.glue_catalog import GlueCatalogPlugin, _glue_type_to_arrow
from rivet_core.models import Catalog

_plugin = GlueCatalogPlugin()

# Glue type string → (PyArrow DataType for writing Parquet, expected _glue_type_to_arrow output)
_GLUE_TYPES: list[tuple[str, pa.DataType, str]] = [
    ("bigint", pa.int64(), "int64"),
    ("int", pa.int32(), "int32"),
    ("smallint", pa.int16(), "int16"),
    ("float", pa.float32(), "float32"),
    ("double", pa.float64(), "float64"),
    ("boolean", pa.bool_(), "bool"),
    ("string", pa.large_utf8(), "large_utf8"),
    ("date", pa.date32(), "date32"),
]

_col_name_st = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)
_glue_type_entry_st = st.sampled_from(_GLUE_TYPES)


def _make_catalog() -> Catalog:
    return Catalog(
        name="glue_cat",
        type="glue",
        options={"database": "mydb", "region": "us-east-1"},
    )


def _make_glue_response(columns: list[dict]) -> dict:
    return {
        "Table": {
            "Name": "test_table",
            "StorageDescriptor": {
                "Columns": columns,
                "Location": "s3://bucket/db/test_table/",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                },
            },
            "PartitionKeys": [],
        }
    }


def _make_parquet_bytes(col_names: list[str], arrow_types: list[pa.DataType]) -> bytes:
    """Write a minimal Parquet file with one row of data for the given schema."""
    fields = [pa.field(name, dtype) for name, dtype in zip(col_names, arrow_types)]
    schema = pa.schema(fields)
    arrays = []
    for dtype in arrow_types:
        if pa.types.is_integer(dtype):
            arrays.append(pa.array([1], type=dtype))
        elif pa.types.is_floating(dtype):
            arrays.append(pa.array([1.0], type=dtype))
        elif pa.types.is_boolean(dtype):
            arrays.append(pa.array([True], type=dtype))
        elif pa.types.is_date(dtype):
            arrays.append(pa.array([0], type=dtype))
        else:
            arrays.append(pa.array(["a"], type=dtype))
    table = pa.table(dict(zip(col_names, arrays)), schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


@settings(max_examples=100, deadline=None)
@given(
    col_entries=st.lists(
        st.tuples(_col_name_st, _glue_type_entry_st),
        min_size=1,
        max_size=6,
    ).filter(lambda entries: len({e[0] for e in entries}) == len(entries))  # unique names
)
def test_property_glue_schema_compatible_with_parquet(
    col_entries: list[tuple[str, tuple[str, pa.DataType, str]]],
) -> None:
    """Property 18: Glue get_schema() returns a schema compatible with corresponding Parquet data.

    For each generated Glue StorageDescriptor, the Arrow type strings in the returned schema
    must match the expected Arrow type strings from _glue_type_to_arrow.
    """
    col_names = [e[0] for e in col_entries]
    glue_type_strs = [e[1][0] for e in col_entries]
    expected_arrow_type_strs = [e[1][2] for e in col_entries]

    glue_columns = [{"Name": name, "Type": gtype} for name, gtype in zip(col_names, glue_type_strs)]

    mock_client = MagicMock()
    mock_client.get_table.return_value = _make_glue_response(glue_columns)

    catalog = _make_catalog()

    with patch("rivet_aws.glue_catalog._make_glue_client_for_table", return_value=mock_client):
        # Suppress divergence check (best-effort, won't block get_schema)
        with patch("rivet_aws.s3_catalog._build_s3fs", side_effect=Exception("no s3")):
            schema = _plugin.get_schema(catalog, "test_table")

    # Column count and order preserved
    assert len(schema.columns) == len(col_names)
    assert [c.name for c in schema.columns] == col_names

    # Each column's Arrow type matches the expected mapping from Glue type
    for col, expected_type_str in zip(schema.columns, expected_arrow_type_strs):
        assert col.type == expected_type_str, (
            f"Column '{col.name}': schema type '{col.type}' != expected '{expected_type_str}' "
            f"(Glue type: '{col.native_type}')"
        )
        # Also verify consistency with _glue_type_to_arrow
        assert col.type == _glue_type_to_arrow(col.native_type), (
            f"Column '{col.name}': col.type '{col.type}' != _glue_type_to_arrow('{col.native_type}') "
            f"= '{_glue_type_to_arrow(col.native_type)}'"
        )


@settings(max_examples=100, deadline=None)
@given(
    col_entries=st.lists(
        st.tuples(_col_name_st, _glue_type_entry_st),
        min_size=1,
        max_size=6,
    ).filter(lambda entries: len({e[0] for e in entries}) == len(entries))
)
def test_property_glue_schema_readable_as_parquet(
    col_entries: list[tuple[str, tuple[str, pa.DataType, str]]],
) -> None:
    """Property 18 (round-trip): Schema from get_schema() can be used to read Parquet data
    written with the corresponding Arrow types without type errors.
    """
    col_names = [e[0] for e in col_entries]
    glue_type_strs = [e[1][0] for e in col_entries]
    arrow_types = [e[1][1] for e in col_entries]

    glue_columns = [{"Name": name, "Type": gtype} for name, gtype in zip(col_names, glue_type_strs)]
    parquet_bytes = _make_parquet_bytes(col_names, arrow_types)

    mock_client = MagicMock()
    mock_client.get_table.return_value = _make_glue_response(glue_columns)

    catalog = _make_catalog()

    with patch("rivet_aws.glue_catalog._make_glue_client_for_table", return_value=mock_client):
        with patch("rivet_aws.s3_catalog._build_s3fs", side_effect=Exception("no s3")):
            schema = _plugin.get_schema(catalog, "test_table")

    # Build an Arrow schema from the returned column type strings
    arrow_schema = pa.schema([
        pa.field(col.name, pa.lib.ensure_type(col.type))
        for col in schema.columns
    ])

    # Read the Parquet file using the schema from get_schema() — must not raise
    table = pq.read_table(io.BytesIO(parquet_bytes), schema=arrow_schema)

    assert table.num_rows == 1
    assert table.num_columns == len(col_names)
    assert table.schema.names == col_names
