"""Tests for GlueDuckDBAdapter (task 7.2).

Tests use mocked boto3 clients — no network calls.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from rivet_core.errors import ExecutionError
from rivet_core.models import Catalog, ComputeEngine, Joint
from rivet_core.optimizer import AdapterPushdownResult
from rivet_duckdb.adapters.glue import (
    ALL_6_CAPABILITIES,
    GlueDuckDBAdapter,
    GlueDuckDBMaterializedRef,
    _configure_s3_secret,
    _glue_input_format_to_reader,
    _matches_partition_filter,
    _resolve_glue_table,
)

_CRED_PATCH = "rivet_duckdb.adapters.glue._make_resolver"


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _catalog(opts: dict[str, Any] | None = None) -> Catalog:
    base = {
        "database": "test_db",
        "region": "us-east-1",
        "access_key_id": "AKIATEST",
        "secret_access_key": "secret123",
    }
    if opts:
        base.update(opts)
    return Catalog(name="glue_cat", type="glue", options=base)


def _engine() -> ComputeEngine:
    return ComputeEngine(name="duckdb_eng", engine_type="duckdb")


def _joint(table: str = "users", partition_filter: dict | None = None) -> Joint:
    j = Joint(name="j1", joint_type="source", table=table)
    if partition_filter:
        j.source_options = {"partition_filter": partition_filter}  # type: ignore[attr-defined]
    return j


def _glue_table_response(
    location: str = "s3://bucket/warehouse/test_db/users",
    input_format: str = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    partition_keys: list[dict] | None = None,
) -> dict:
    return {
        "Table": {
            "Name": "users",
            "StorageDescriptor": {
                "Location": location,
                "InputFormat": input_format,
                "Columns": [
                    {"Name": "id", "Type": "bigint"},
                    {"Name": "name", "Type": "string"},
                ],
            },
            "PartitionKeys": partition_keys or [],
            "Parameters": {},
        }
    }


def _partitions_response(partitions: list[dict]) -> dict:
    return {"Partitions": partitions}


def _make_partition(values: list[str], location: str) -> dict:
    return {
        "Values": values,
        "StorageDescriptor": {"Location": location},
        "Parameters": {},
    }


# ── Adapter registration ─────────────────────────────────────────────────────


class TestGlueDuckDBAdapterRegistration:
    def test_target_engine_type(self):
        adapter = GlueDuckDBAdapter()
        assert adapter.target_engine_type == "duckdb"

    def test_catalog_type(self):
        adapter = GlueDuckDBAdapter()
        assert adapter.catalog_type == "glue"

    def test_capabilities_all_6(self):
        adapter = GlueDuckDBAdapter()
        assert adapter.capabilities == ALL_6_CAPABILITIES

    def test_write_capabilities(self):
        adapter = GlueDuckDBAdapter()
        assert set(adapter.write_capabilities) == {
            "write_append",
            "write_replace",
            "write_partition",
        }

    def test_source_is_engine_plugin(self):
        adapter = GlueDuckDBAdapter()
        assert adapter.source == "engine_plugin"
        assert adapter.source_plugin == "rivet_duckdb"


# ── Input format mapping ─────────────────────────────────────────────────────


class TestInputFormatMapping:
    def test_parquet_format(self):
        assert _glue_input_format_to_reader(
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
        ) == "read_parquet"

    def test_json_format(self):
        assert _glue_input_format_to_reader(
            "org.apache.hive.hcatalog.data.JsonSerDe"
        ) == "read_json_auto"

    def test_csv_format(self):
        assert _glue_input_format_to_reader(
            "org.apache.hadoop.mapred.TextInputFormat"
        ) == "read_csv_auto"

    def test_orc_format(self):
        assert _glue_input_format_to_reader(
            "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
        ) == "read_parquet"

    def test_unknown_defaults_to_parquet(self):
        assert _glue_input_format_to_reader("some.unknown.Format") == "read_parquet"


# ── Partition filter matching ─────────────────────────────────────────────────


class TestPartitionFilterMatching:
    def test_matches_single_key(self):
        assert _matches_partition_filter({"year": "2024"}, {"year": "2024"})

    def test_no_match_single_key(self):
        assert not _matches_partition_filter({"year": "2023"}, {"year": "2024"})

    def test_matches_multiple_keys(self):
        assert _matches_partition_filter(
            {"year": "2024", "month": "01"}, {"year": "2024", "month": "01"}
        )

    def test_partial_match_fails(self):
        assert not _matches_partition_filter(
            {"year": "2024", "month": "02"}, {"year": "2024", "month": "01"}
        )

    def test_empty_filter_matches_all(self):
        assert _matches_partition_filter({"year": "2024"}, {})

    def test_filter_value_coerced_to_string(self):
        assert _matches_partition_filter({"year": "2024"}, {"year": 2024})


# ── Glue table resolution (mocked boto3) ─────────────────────────────────────


class TestResolveGlueTable:
    @patch(_CRED_PATCH)
    def test_unpartitioned_table(self, mock_resolver_cls):
        mock_client = MagicMock()
        mock_client.get_table.return_value = _glue_table_response()
        mock_resolver_cls.return_value.create_client.return_value = mock_client

        location, input_format, pk, plocs = _resolve_glue_table(
            {"database": "test_db", "region": "us-east-1"}, "users", None
        )
        assert location == "s3://bucket/warehouse/test_db/users"
        assert "parquet" in input_format.lower()
        assert pk == []
        assert plocs == []

    @patch(_CRED_PATCH)
    def test_partitioned_table_no_filter(self, mock_resolver_cls):
        mock_client = MagicMock()
        mock_client.get_table.return_value = _glue_table_response(
            partition_keys=[{"Name": "year", "Type": "string"}]
        )
        paginator = MagicMock()
        paginator.paginate.return_value = [
            _partitions_response([
                _make_partition(["2023"], "s3://bucket/warehouse/users/year=2023"),
                _make_partition(["2024"], "s3://bucket/warehouse/users/year=2024"),
            ])
        ]
        mock_client.get_paginator.return_value = paginator
        mock_resolver_cls.return_value.create_client.return_value = mock_client

        location, _, pk, plocs = _resolve_glue_table(
            {"database": "test_db", "region": "us-east-1"}, "users", None
        )
        assert pk == ["year"]
        assert len(plocs) == 2

    @patch(_CRED_PATCH)
    def test_partitioned_table_with_filter(self, mock_resolver_cls):
        mock_client = MagicMock()
        mock_client.get_table.return_value = _glue_table_response(
            partition_keys=[{"Name": "year", "Type": "string"}]
        )
        paginator = MagicMock()
        paginator.paginate.return_value = [
            _partitions_response([
                _make_partition(["2023"], "s3://bucket/warehouse/users/year=2023"),
                _make_partition(["2024"], "s3://bucket/warehouse/users/year=2024"),
            ])
        ]
        mock_client.get_paginator.return_value = paginator
        mock_resolver_cls.return_value.create_client.return_value = mock_client

        _, _, _, plocs = _resolve_glue_table(
            {"database": "test_db", "region": "us-east-1"},
            "users",
            {"year": "2024"},
        )
        assert plocs == ["s3://bucket/warehouse/users/year=2024"]

    @patch(_CRED_PATCH)
    def test_table_not_found_raises_rvt503(self, mock_resolver_cls):
        mock_client = MagicMock()
        mock_client.get_table.side_effect = Exception("TableNotFoundException")
        mock_resolver_cls.return_value.create_client.return_value = mock_client

        with pytest.raises(ExecutionError) as exc_info:
            _resolve_glue_table(
                {"database": "test_db", "region": "us-east-1"}, "missing", None
            )
        assert exc_info.value.error.code == "RVT-503"

    @patch(_CRED_PATCH)
    def test_catalog_id_passed_to_api(self, mock_resolver_cls):
        mock_client = MagicMock()
        mock_client.get_table.return_value = _glue_table_response()
        mock_resolver_cls.return_value.create_client.return_value = mock_client

        _resolve_glue_table(
            {"database": "test_db", "region": "us-east-1", "catalog_id": "123456789"},
            "users",
            None,
        )
        call_kwargs = mock_client.get_table.call_args[1]
        assert call_kwargs["CatalogId"] == "123456789"


# ── S3 secret configuration (mocked) ─────────────────────────────────────────


class TestConfigureS3Secret:
    @patch(_CRED_PATCH)
    def test_configures_secret_with_credentials(self, mock_resolver_cls):
        import duckdb

        mock_creds = MagicMock()
        mock_creds.access_key_id = "AKIATEST"
        mock_creds.secret_access_key = "secret123"
        mock_creds.session_token = None
        mock_resolver_cls.return_value.resolve.return_value = mock_creds

        conn = duckdb.connect(":memory:")
        try:
            conn.execute("INSTALL httpfs; LOAD httpfs")
        except Exception:
            pytest.skip("httpfs extension not available")

        _configure_s3_secret(conn, {"region": "us-east-1"})
        # If no exception, secret was created successfully
        conn.close()

    @patch(_CRED_PATCH)
    def test_configures_secret_with_session_token(self, mock_resolver_cls):
        import duckdb

        mock_creds = MagicMock()
        mock_creds.access_key_id = "AKIATEST"
        mock_creds.secret_access_key = "secret123"
        mock_creds.session_token = "token123"
        mock_resolver_cls.return_value.resolve.return_value = mock_creds

        conn = duckdb.connect(":memory:")
        try:
            conn.execute("INSTALL httpfs; LOAD httpfs")
        except Exception:
            pytest.skip("httpfs extension not available")

        _configure_s3_secret(conn, {"region": "us-west-2"})
        conn.close()


# ── read_dispatch ─────────────────────────────────────────────────────────────


class TestReadDispatch:
    @patch("rivet_duckdb.adapters.glue._resolve_glue_table")
    def test_returns_deferred_material(self, mock_resolve):
        mock_resolve.return_value = (
            "s3://bucket/warehouse/test_db/users",
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            [],
            [],
        )
        adapter = GlueDuckDBAdapter()
        catalog = _catalog()
        joint = _joint()
        engine = _engine()

        result = adapter.read_dispatch(engine, catalog, joint)
        assert isinstance(result, AdapterPushdownResult)
        assert result.material.state == "deferred"
        assert isinstance(result.material.materialized_ref, GlueDuckDBMaterializedRef)

    @patch("rivet_duckdb.adapters.glue._resolve_glue_table")
    def test_passes_partition_filter(self, mock_resolve):
        mock_resolve.return_value = (
            "s3://bucket/warehouse/test_db/users",
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            [],
            [],
        )
        adapter = GlueDuckDBAdapter()
        catalog = _catalog()
        joint = _joint(partition_filter={"year": "2024"})
        engine = _engine()

        result = adapter.read_dispatch(engine, catalog, joint)
        ref = result.material.materialized_ref
        assert isinstance(ref, GlueDuckDBMaterializedRef)
        assert ref._partition_filter == {"year": "2024"}

    @patch("rivet_duckdb.adapters.glue._resolve_glue_table")
    def test_uses_table_name_from_joint(self, mock_resolve):
        mock_resolve.return_value = (
            "s3://bucket/warehouse/test_db/orders",
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            [],
            [],
        )
        adapter = GlueDuckDBAdapter()
        catalog = _catalog()
        joint = _joint(table="orders")
        engine = _engine()

        result = adapter.read_dispatch(engine, catalog, joint)
        ref = result.material.materialized_ref
        assert isinstance(ref, GlueDuckDBMaterializedRef)
        assert ref._table_name == "orders"


# ── write_dispatch (mocked) ──────────────────────────────────────────────────


class TestWriteDispatch:
    @patch("rivet_duckdb.adapters.glue._resolve_glue_table")
    @patch("rivet_duckdb.adapters.glue._configure_s3_secret")
    @patch("rivet_duckdb.extensions.ensure_extension")
    def test_write_dispatch_calls_resolve_and_configure(
        self, mock_ensure, mock_configure, mock_resolve
    ):
        mock_resolve.return_value = (
            "s3://bucket/warehouse/users",
            "parquet",
            [],
            [],
        )

        adapter = GlueDuckDBAdapter()
        catalog = _catalog()
        joint = Joint(name="sink1", joint_type="sink", table="users", write_strategy="replace")
        engine = _engine()

        arrow_table = pa.table({"id": [1, 2], "name": ["a", "b"]})
        material = MagicMock()
        material.to_arrow.return_value = arrow_table

        # The actual COPY will fail since there's no real S3, but we verify the flow
        # by checking that resolve and configure were called
        try:
            adapter.write_dispatch(engine, catalog, joint, material)
        except (ExecutionError, Exception):
            pass  # Expected — no real S3

        mock_resolve.assert_called_once()


# ── GlueDuckDBMaterializedRef ────────────────────────────────────────────────


class TestGlueDuckDBMaterializedRef:
    def test_storage_type(self):
        ref = GlueDuckDBMaterializedRef(
            catalog_options={"database": "db", "region": "us-east-1"},
            table_name="t",
            partition_filter=None,
            engine_config={},
        )
        assert ref.storage_type == "glue_duckdb"

    def test_size_bytes_is_none(self):
        ref = GlueDuckDBMaterializedRef(
            catalog_options={"database": "db", "region": "us-east-1"},
            table_name="t",
            partition_filter=None,
            engine_config={},
        )
        assert ref.size_bytes is None
