"""Property test for S3 Parquet schema round-trip.

Feature: cross-storage-adapters, Property 17: S3 Parquet schema round-trip

For any Parquet file in an S3 catalog, the schema returned by
S3CatalogPlugin.get_schema() SHALL equal the schema embedded in the Parquet
file footer (column names, types, and nullability match).

Validates: Requirements 12.1, 12.3
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_aws.s3_catalog import S3CatalogPlugin
from rivet_core.models import Catalog

# ── Arrow type strategies ────────────────────────────────────────────────────

_LEAF_TYPES = st.sampled_from([
    pa.int8(),
    pa.int16(),
    pa.int32(),
    pa.int64(),
    pa.uint8(),
    pa.uint16(),
    pa.uint32(),
    pa.uint64(),
    pa.float32(),
    pa.float64(),
    pa.bool_(),
    pa.string(),
    pa.large_string(),
    pa.binary(),
    pa.date32(),
    pa.timestamp("us"),
    pa.timestamp("ms"),
])


@st.composite
def arrow_field(draw: st.DrawFn) -> pa.Field:
    name = draw(st.text(alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_"), min_size=1, max_size=20))
    dtype = draw(_LEAF_TYPES)
    nullable = draw(st.booleans())
    return pa.field(name, dtype, nullable=nullable)


@st.composite
def arrow_schema(draw: st.DrawFn) -> pa.Schema:
    fields = draw(st.lists(arrow_field(), min_size=1, max_size=10))
    # Deduplicate field names (Parquet requires unique column names)
    seen: set[str] = set()
    unique_fields: list[pa.Field] = []
    for f in fields:
        if f.name not in seen:
            seen.add(f.name)
            unique_fields.append(f)
    if not unique_fields:
        unique_fields = [pa.field("col0", pa.int64())]
    return pa.schema(unique_fields)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_parquet_bytes(schema: pa.Schema) -> bytes:
    """Write a minimal Parquet file with one null row for the given schema."""
    arrays = [pa.array([None], type=f.type) for f in schema]
    table = pa.table({f.name: arr for f, arr in zip(schema, arrays)})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _mock_fs(parquet_bytes: bytes) -> MagicMock:
    mock_fs = MagicMock()
    mock_fs.open_input_file.return_value.__enter__ = lambda s: io.BytesIO(parquet_bytes)
    mock_fs.open_input_file.return_value.__exit__ = MagicMock(return_value=False)
    return mock_fs


def _make_catalog(bucket: str = "test-bucket") -> Catalog:
    return Catalog(name="test", type="s3", options={"bucket": bucket, "format": "parquet"})


# ── Property test ────────────────────────────────────────────────────────────

@settings(max_examples=100, deadline=None)
@given(schema=arrow_schema())
def test_s3_parquet_schema_roundtrip(schema: pa.Schema) -> None:
    """Property 17: get_schema() returns schema equal to Parquet footer schema.

    For any Arrow schema written to Parquet, S3CatalogPlugin.get_schema()
    must return column names, types, and nullability matching the footer.
    """
    raw = _make_parquet_bytes(schema)
    footer_schema = pq.read_schema(io.BytesIO(raw))

    catalog = _make_catalog()
    plugin = S3CatalogPlugin()

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=_mock_fs(raw)):
        result = plugin.get_schema(catalog, "table")

    # Column count must match
    assert len(result.columns) == len(footer_schema)

    for col, field in zip(result.columns, footer_schema):
        # Column names preserved (no reordering)
        assert col.name == field.name, (
            f"Column name mismatch: got {col.name!r}, expected {field.name!r}"
        )
        # Types match footer exactly (no widening)
        assert col.type == str(field.type), (
            f"Type mismatch for column {col.name!r}: got {col.type!r}, expected {str(field.type)!r}"
        )
        # Nullability preserved
        assert col.nullable == field.nullable, (
            f"Nullability mismatch for column {col.name!r}: got {col.nullable}, expected {field.nullable}"
        )
