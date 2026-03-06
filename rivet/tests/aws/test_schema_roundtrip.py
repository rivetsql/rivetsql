"""Tests for task 11.1: Schema round-trip consistency.

Validates Requirements 12.1, 12.2, 12.3, 12.4:
- S3 get_schema() returns Arrow schema matching Parquet footer (no type widening or column reordering)
- CSV/JSON sampling produces compatible schema for reads
- Glue get_schema() logs warning when StorageDescriptor schema diverges from Parquet footer
"""

from __future__ import annotations

import io
import logging
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from rivet_aws.glue_catalog import GlueCatalogPlugin
from rivet_aws.s3_catalog import S3CatalogPlugin
from rivet_core.models import Catalog


def _make_s3_catalog(options: dict) -> Catalog:
    return Catalog(name="test_s3", type="s3", options=options)


def _make_glue_catalog(options: dict) -> Catalog:
    return Catalog(name="test_glue", type="glue", options=options)


def _parquet_bytes(schema: pa.Schema, data: dict) -> bytes:
    table = pa.table(data, schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _mock_fs_for_parquet(parquet_bytes: bytes) -> MagicMock:
    mock_fs = MagicMock()
    mock_fs.open_input_file.return_value.__enter__ = lambda s: io.BytesIO(parquet_bytes)
    mock_fs.open_input_file.return_value.__exit__ = MagicMock(return_value=False)
    return mock_fs


def _mock_glue_response(glue_columns, location, partition_keys=None):
    return {
        "Table": {
            "Name": "test_table",
            "StorageDescriptor": {
                "Columns": glue_columns,
                "Location": location,
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                },
            },
            "PartitionKeys": partition_keys or [],
        }
    }


def _mock_s3fs_with_parquet(raw: bytes) -> MagicMock:
    mock_fi = MagicMock()
    mock_fi.type = pafs.FileType.File
    mock_fi.path = "bucket/db/test_table/part-0.parquet"

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [mock_fi]
    mock_fs.open_input_file.return_value.__enter__ = lambda s: io.BytesIO(raw)
    mock_fs.open_input_file.return_value.__exit__ = MagicMock(return_value=False)
    return mock_fs


# ── S3 Parquet schema round-trip (Req 12.1, 12.3) ───────────────────────────


class TestS3ParquetSchemaRoundTrip:

    def test_column_order_preserved(self):
        schema = pa.schema([
            pa.field("z_col", pa.int32()),
            pa.field("a_col", pa.string()),
            pa.field("m_col", pa.float64()),
        ])
        raw = _parquet_bytes(schema, {"z_col": [1], "a_col": ["x"], "m_col": [1.0]})
        catalog = _make_s3_catalog({"bucket": "b", "format": "parquet"})

        with patch("rivet_aws.s3_catalog._build_s3fs", return_value=_mock_fs_for_parquet(raw)):
            result = S3CatalogPlugin().get_schema(catalog, "tbl")

        assert [c.name for c in result.columns] == ["z_col", "a_col", "m_col"]

    def test_types_match_footer_exactly(self):
        schema = pa.schema([
            pa.field("i32", pa.int32()),
            pa.field("i64", pa.int64()),
            pa.field("f32", pa.float32()),
            pa.field("f64", pa.float64()),
            pa.field("s", pa.string()),
            pa.field("b", pa.bool_()),
            pa.field("ts", pa.timestamp("us")),
        ])
        data = {
            "i32": pa.array([1], type=pa.int32()),
            "i64": pa.array([1], type=pa.int64()),
            "f32": pa.array([1.0], type=pa.float32()),
            "f64": pa.array([1.0], type=pa.float64()),
            "s": ["a"],
            "b": [True],
            "ts": pa.array([1000000], type=pa.timestamp("us")),
        }
        raw = _parquet_bytes(schema, data)
        catalog = _make_s3_catalog({"bucket": "b", "format": "parquet"})

        with patch("rivet_aws.s3_catalog._build_s3fs", return_value=_mock_fs_for_parquet(raw)):
            result = S3CatalogPlugin().get_schema(catalog, "tbl")

        footer_schema = pq.read_schema(io.BytesIO(raw))
        for col, field in zip(result.columns, footer_schema):
            assert col.name == field.name
            assert col.type == str(field.type)

    def test_nullability_preserved(self):
        schema = pa.schema([
            pa.field("nullable_col", pa.int64(), nullable=True),
            pa.field("required_col", pa.int64(), nullable=False),
        ])
        data = {
            "nullable_col": pa.array([1], type=pa.int64()),
            "required_col": pa.array([2], type=pa.int64()),
        }
        raw = _parquet_bytes(schema, data)
        catalog = _make_s3_catalog({"bucket": "b", "format": "parquet"})

        with patch("rivet_aws.s3_catalog._build_s3fs", return_value=_mock_fs_for_parquet(raw)):
            result = S3CatalogPlugin().get_schema(catalog, "tbl")

        footer_schema = pq.read_schema(io.BytesIO(raw))
        for col, field in zip(result.columns, footer_schema):
            assert col.nullable == field.nullable

    def test_no_type_widening(self):
        schema = pa.schema([pa.field("x", pa.int32())])
        raw = _parquet_bytes(schema, {"x": pa.array([1], type=pa.int32())})
        catalog = _make_s3_catalog({"bucket": "b", "format": "parquet"})

        with patch("rivet_aws.s3_catalog._build_s3fs", return_value=_mock_fs_for_parquet(raw)):
            result = S3CatalogPlugin().get_schema(catalog, "tbl")

        assert result.columns[0].type == "int32"


# ── CSV/JSON schema compatibility (Req 12.2) ────────────────────────────────


class TestCSVJsonSchemaCompatibility:

    def test_csv_schema_inferred(self):
        csv_content = b"id,name,value\n1,alice,3.14\n2,bob,2.72\n"
        catalog = _make_s3_catalog({"bucket": "b", "format": "csv"})

        mock_fs = MagicMock()
        mock_fs.open_input_file.return_value.__enter__ = lambda s: io.BytesIO(csv_content)
        mock_fs.open_input_file.return_value.__exit__ = MagicMock(return_value=False)

        with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
            result = S3CatalogPlugin().get_schema(catalog, "data")

        col_names = [c.name for c in result.columns]
        assert "id" in col_names
        assert "name" in col_names
        assert "value" in col_names

    def test_json_schema_inferred(self):
        json_content = b'{"id": 1, "name": "alice"}\n{"id": 2, "name": "bob"}\n'
        catalog = _make_s3_catalog({"bucket": "b", "format": "json"})

        mock_fs = MagicMock()
        mock_fs.open_input_file.return_value.__enter__ = lambda s: io.BytesIO(json_content)
        mock_fs.open_input_file.return_value.__exit__ = MagicMock(return_value=False)

        with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
            result = S3CatalogPlugin().get_schema(catalog, "data")

        col_names = [c.name for c in result.columns]
        assert "id" in col_names
        assert "name" in col_names


# ── Glue schema divergence warning (Req 12.4) ───────────────────────────────


class TestGlueSchemaFooterDivergenceWarning:

    def test_warns_on_column_name_mismatch(self, caplog):
        glue_cols = [
            {"Name": "id", "Type": "bigint"},
            {"Name": "user_name", "Type": "string"},
        ]
        parquet_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
        ])
        raw = _parquet_bytes(parquet_schema, {"id": [1], "name": ["a"]})

        mock_glue_client = MagicMock()
        mock_glue_client.get_table.return_value = _mock_glue_response(
            glue_cols, "s3://bucket/db/test_table/"
        )
        mock_fs = _mock_s3fs_with_parquet(raw)
        catalog = _make_glue_catalog({"database": "mydb", "region": "us-east-1"})

        with (
            patch("rivet_aws.glue_catalog._make_glue_client_for_table", return_value=mock_glue_client),
            patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs),
            caplog.at_level(logging.WARNING, logger="rivet_aws.glue_catalog"),
        ):
            schema = GlueCatalogPlugin().get_schema(catalog, "test_table")

        assert len(schema.columns) == 2
        assert any("diverges from Parquet footer" in r.message for r in caplog.records)

    def test_warns_on_column_type_mismatch(self, caplog):
        glue_cols = [
            {"Name": "id", "Type": "int"},  # maps to int32
            {"Name": "value", "Type": "string"},
        ]
        parquet_schema = pa.schema([
            pa.field("id", pa.int64()),  # different from int32
            pa.field("value", pa.string()),
        ])
        raw = _parquet_bytes(parquet_schema, {"id": [1], "value": ["a"]})

        mock_glue_client = MagicMock()
        mock_glue_client.get_table.return_value = _mock_glue_response(
            glue_cols, "s3://bucket/db/test_table/"
        )
        mock_fs = _mock_s3fs_with_parquet(raw)
        catalog = _make_glue_catalog({"database": "mydb", "region": "us-east-1"})

        with (
            patch("rivet_aws.glue_catalog._make_glue_client_for_table", return_value=mock_glue_client),
            patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs),
            caplog.at_level(logging.WARNING, logger="rivet_aws.glue_catalog"),
        ):
            schema = GlueCatalogPlugin().get_schema(catalog, "test_table")

        assert len(schema.columns) == 2
        assert any("diverges from Parquet footer" in r.message for r in caplog.records)

    def test_no_warning_when_schemas_match(self, caplog):
        glue_cols = [
            {"Name": "id", "Type": "bigint"},
            {"Name": "name", "Type": "string"},
        ]
        parquet_schema = pa.schema([
            pa.field("id", pa.int64()),
            pa.field("name", pa.large_utf8()),  # Glue string -> large_utf8
        ])
        raw = _parquet_bytes(parquet_schema, {"id": [1], "name": ["a"]})

        mock_glue_client = MagicMock()
        mock_glue_client.get_table.return_value = _mock_glue_response(
            glue_cols, "s3://bucket/db/test_table/"
        )
        mock_fs = _mock_s3fs_with_parquet(raw)
        catalog = _make_glue_catalog({"database": "mydb", "region": "us-east-1"})

        with (
            patch("rivet_aws.glue_catalog._make_glue_client_for_table", return_value=mock_glue_client),
            patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs),
            caplog.at_level(logging.WARNING, logger="rivet_aws.glue_catalog"),
        ):
            schema = GlueCatalogPlugin().get_schema(catalog, "test_table")

        assert len(schema.columns) == 2
        assert not any("diverges from Parquet footer" in r.message for r in caplog.records)

    def test_no_warning_when_no_parquet_files(self, caplog):
        glue_cols = [{"Name": "id", "Type": "bigint"}]

        mock_glue_client = MagicMock()
        mock_glue_client.get_table.return_value = _mock_glue_response(
            glue_cols, "s3://bucket/db/test_table/"
        )
        mock_fs = MagicMock()
        mock_fs.get_file_info.return_value = []

        catalog = _make_glue_catalog({"database": "mydb", "region": "us-east-1"})

        with (
            patch("rivet_aws.glue_catalog._make_glue_client_for_table", return_value=mock_glue_client),
            patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs),
            caplog.at_level(logging.WARNING, logger="rivet_aws.glue_catalog"),
        ):
            schema = GlueCatalogPlugin().get_schema(catalog, "test_table")

        assert len(schema.columns) == 1
        assert not any("diverges" in r.message for r in caplog.records)

    def test_no_crash_when_s3_unreachable(self):
        glue_cols = [{"Name": "id", "Type": "bigint"}]

        mock_glue_client = MagicMock()
        mock_glue_client.get_table.return_value = _mock_glue_response(
            glue_cols, "s3://bucket/db/test_table/"
        )
        catalog = _make_glue_catalog({"database": "mydb", "region": "us-east-1"})

        with (
            patch("rivet_aws.glue_catalog._make_glue_client_for_table", return_value=mock_glue_client),
            patch("rivet_aws.s3_catalog._build_s3fs", side_effect=Exception("connection failed")),
        ):
            schema = GlueCatalogPlugin().get_schema(catalog, "test_table")

        assert len(schema.columns) == 1
        assert schema.columns[0].name == "id"

    def test_no_warning_for_non_s3_location(self, caplog):
        glue_cols = [{"Name": "id", "Type": "bigint"}]

        mock_glue_client = MagicMock()
        mock_glue_client.get_table.return_value = _mock_glue_response(
            glue_cols, "hdfs://namenode/db/test_table/"
        )
        catalog = _make_glue_catalog({"database": "mydb", "region": "us-east-1"})

        with (
            patch("rivet_aws.glue_catalog._make_glue_client_for_table", return_value=mock_glue_client),
            caplog.at_level(logging.WARNING, logger="rivet_aws.glue_catalog"),
        ):
            schema = GlueCatalogPlugin().get_schema(catalog, "test_table")

        assert len(schema.columns) == 1
        assert not any("diverges" in r.message for r in caplog.records)
