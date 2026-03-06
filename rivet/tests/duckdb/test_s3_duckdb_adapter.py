"""Tests for S3DuckDBAdapter (task 7.1).

Tests use mocked DuckDB connections to avoid real S3 calls.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from rivet_core.errors import ExecutionError
from rivet_core.models import Catalog, Joint, Material
from rivet_core.optimizer import AdapterPushdownResult
from rivet_duckdb.adapters.s3 import (
    _FORMAT_TO_READER,
    S3DuckDBAdapter,
    _build_s3_path,
    _configure_s3_secret,
    _S3DuckDBMaterializedRef,
)
from rivet_duckdb.engine import ALL_6_CAPABILITIES

# ── Adapter registration ──────────────────────────────────────────────────────


class TestS3DuckDBAdapterRegistration:
    def test_target_engine_type(self):
        adapter = S3DuckDBAdapter()
        assert adapter.target_engine_type == "duckdb"

    def test_catalog_type(self):
        adapter = S3DuckDBAdapter()
        assert adapter.catalog_type == "s3"

    def test_source_is_engine_plugin(self):
        adapter = S3DuckDBAdapter()
        assert adapter.source == "engine_plugin"

    def test_source_plugin_is_rivet_duckdb(self):
        adapter = S3DuckDBAdapter()
        assert adapter.source_plugin == "rivet_duckdb"

    def test_capabilities_include_all_6(self):
        adapter = S3DuckDBAdapter()
        for cap in ALL_6_CAPABILITIES:
            assert cap in adapter.capabilities

    def test_capabilities_include_write_strategies(self):
        adapter = S3DuckDBAdapter()
        assert "write_append" in adapter.capabilities
        assert "write_replace" in adapter.capabilities
        assert "write_partition" in adapter.capabilities

    def test_no_merge_or_scd2_write(self):
        adapter = S3DuckDBAdapter()
        assert "write_merge" not in adapter.capabilities
        assert "write_scd2" not in adapter.capabilities


# ── S3 path building ──────────────────────────────────────────────────────────


class TestBuildS3Path:
    def test_parquet_no_prefix(self):
        opts = {"bucket": "my-bucket", "format": "parquet"}
        assert _build_s3_path(opts, "users") == "s3://my-bucket/users.parquet"

    def test_parquet_with_prefix(self):
        opts = {"bucket": "my-bucket", "prefix": "raw", "format": "parquet"}
        assert _build_s3_path(opts, "users") == "s3://my-bucket/raw/users.parquet"

    def test_csv_format(self):
        opts = {"bucket": "b", "format": "csv"}
        assert _build_s3_path(opts, "t") == "s3://b/t.csv"

    def test_json_format(self):
        opts = {"bucket": "b", "format": "json"}
        assert _build_s3_path(opts, "t") == "s3://b/t.json"

    def test_delta_format_no_extension(self):
        opts = {"bucket": "b", "format": "delta"}
        assert _build_s3_path(opts, "t") == "s3://b/t"

    def test_default_format_is_parquet(self):
        opts = {"bucket": "b"}
        assert _build_s3_path(opts, "t") == "s3://b/t.parquet"

    def test_wildcard_when_no_table(self):
        opts = {"bucket": "b", "format": "parquet"}
        assert _build_s3_path(opts, None) == "s3://b/*.parquet"


# ── S3 secret configuration ──────────────────────────────────────────────────


class TestConfigureS3Secret:
    def test_basic_credentials(self):
        conn = MagicMock()
        opts = {
            "access_key_id": "AKID",
            "secret_access_key": "SECRET",
            "region": "us-west-2",
        }
        _configure_s3_secret(conn, opts)
        sql = conn.execute.call_args[0][0]
        assert "TYPE S3" in sql
        assert "KEY_ID 'AKID'" in sql
        assert "SECRET 'SECRET'" in sql
        assert "REGION 'us-west-2'" in sql

    def test_session_token_included(self):
        conn = MagicMock()
        opts = {
            "access_key_id": "AKID",
            "secret_access_key": "SECRET",
            "session_token": "TOK",
        }
        _configure_s3_secret(conn, opts)
        sql = conn.execute.call_args[0][0]
        assert "SESSION_TOKEN 'TOK'" in sql

    def test_endpoint_url(self):
        conn = MagicMock()
        opts = {"endpoint_url": "http://localhost:9000", "region": "us-east-1"}
        _configure_s3_secret(conn, opts)
        sql = conn.execute.call_args[0][0]
        assert "ENDPOINT 'http://localhost:9000'" in sql

    def test_path_style_access(self):
        conn = MagicMock()
        opts = {"path_style_access": True, "region": "us-east-1"}
        _configure_s3_secret(conn, opts)
        sql = conn.execute.call_args[0][0]
        assert "URL_STYLE 'path'" in sql

    def test_no_credentials_still_sets_region(self):
        conn = MagicMock()
        opts = {"region": "eu-west-1"}
        _configure_s3_secret(conn, opts)
        sql = conn.execute.call_args[0][0]
        assert "REGION 'eu-west-1'" in sql
        assert "KEY_ID" not in sql

    def test_default_region(self):
        conn = MagicMock()
        _configure_s3_secret(conn, {})
        sql = conn.execute.call_args[0][0]
        assert "REGION 'us-east-1'" in sql


# ── Format to reader mapping ─────────────────────────────────────────────────


class TestFormatToReader:
    def test_parquet(self):
        assert _FORMAT_TO_READER["parquet"] == "read_parquet"

    def test_csv(self):
        assert _FORMAT_TO_READER["csv"] == "read_csv_auto"

    def test_json(self):
        assert _FORMAT_TO_READER["json"] == "read_json_auto"

    def test_orc_not_supported(self):
        assert "orc" not in _FORMAT_TO_READER


# ── read_dispatch ─────────────────────────────────────────────────────────────


class TestReadDispatch:
    def test_returns_adapter_pushdown_result(self):
        adapter = S3DuckDBAdapter()
        catalog = Catalog(name="s3_cat", type="s3", options={"bucket": "b", "format": "parquet"})
        joint = Joint(name="src", joint_type="source", catalog="s3_cat", table="users")
        result = adapter.read_dispatch(None, catalog, joint)
        assert isinstance(result, AdapterPushdownResult)
        assert isinstance(result.material, Material)
        assert result.material.state == "deferred"
        assert result.material.name == "src"
        assert result.material.catalog == "s3_cat"

    def test_materialized_ref_is_s3_type(self):
        adapter = S3DuckDBAdapter()
        catalog = Catalog(name="c", type="s3", options={"bucket": "b"})
        joint = Joint(name="j", joint_type="source", catalog="c", table="t")
        result = adapter.read_dispatch(None, catalog, joint)
        assert isinstance(result.material.materialized_ref, _S3DuckDBMaterializedRef)

    def test_read_dispatch_with_sql(self):
        adapter = S3DuckDBAdapter()
        catalog = Catalog(name="c", type="s3", options={"bucket": "b"})
        joint = Joint(name="j", joint_type="source", catalog="c", sql="SELECT 1")
        result = adapter.read_dispatch(None, catalog, joint)
        assert result.material.materialized_ref._sql == "SELECT 1"

    def test_read_dispatch_with_table(self):
        adapter = S3DuckDBAdapter()
        catalog = Catalog(name="c", type="s3", options={"bucket": "b"})
        joint = Joint(name="j", joint_type="source", catalog="c", table="orders")
        result = adapter.read_dispatch(None, catalog, joint)
        assert result.material.materialized_ref._table == "orders"


# ── write_dispatch ────────────────────────────────────────────────────────────


class TestWriteDispatch:
    @patch("rivet_duckdb.adapters.s3.duckdb")
    @patch("rivet_duckdb.adapters.s3.ensure_extension")
    def test_write_parquet_calls_copy(self, mock_ensure, mock_duckdb):
        conn = MagicMock()
        mock_duckdb.connect.return_value = conn

        adapter = S3DuckDBAdapter()
        catalog = Catalog(name="c", type="s3", options={"bucket": "b", "format": "parquet"})
        joint = Joint(name="sink", joint_type="sink", catalog="c", table="out", write_strategy="replace")
        material = MagicMock()
        material.to_arrow.return_value = pa.table({"x": [1]})

        adapter.write_dispatch(None, catalog, joint, material)

        mock_ensure.assert_called_once_with(conn, "httpfs")
        # Verify COPY was called with PARQUET format
        copy_calls = [c for c in conn.execute.call_args_list if "COPY" in str(c)]
        assert len(copy_calls) > 0

    @patch("rivet_duckdb.adapters.s3.duckdb")
    @patch("rivet_duckdb.adapters.s3.ensure_extension")
    def test_write_unsupported_format_raises(self, mock_ensure, mock_duckdb):
        conn = MagicMock()
        mock_duckdb.connect.return_value = conn

        adapter = S3DuckDBAdapter()
        catalog = Catalog(name="c", type="s3", options={"bucket": "b", "format": "orc"})
        joint = Joint(name="sink", joint_type="sink", catalog="c", table="out", write_strategy="replace")
        material = MagicMock()
        material.to_arrow.return_value = pa.table({"x": [1]})

        with pytest.raises(ExecutionError) as exc_info:
            adapter.write_dispatch(None, catalog, joint, material)
        assert exc_info.value.error.code == "RVT-501"


# ── MaterializedRef S3 ────────────────────────────────────────────────────────


class TestS3DuckDBMaterializedRef:
    def test_storage_type(self):
        ref = _S3DuckDBMaterializedRef({"bucket": "b"}, None, "t")
        assert ref.storage_type == "s3"

    def test_size_bytes_is_none(self):
        ref = _S3DuckDBMaterializedRef({"bucket": "b"}, None, "t")
        assert ref.size_bytes is None

    @patch("rivet_duckdb.adapters.s3.duckdb")
    @patch("rivet_duckdb.adapters.s3.ensure_extension")
    def test_to_arrow_with_sql(self, mock_ensure, mock_duckdb):
        """When SQL is provided, it is executed directly."""
        arrow_table = pa.table({"a": [1, 2]})
        conn = MagicMock()
        conn.execute.return_value.arrow.return_value = arrow_table
        mock_duckdb.connect.return_value = conn

        ref = _S3DuckDBMaterializedRef({"bucket": "b", "region": "us-east-1"}, "SELECT 1", None)
        result = ref.to_arrow()
        assert result == arrow_table

    @patch("rivet_duckdb.adapters.s3.duckdb")
    @patch("rivet_duckdb.adapters.s3.ensure_extension")
    def test_to_arrow_with_table_uses_reader(self, mock_ensure, mock_duckdb):
        """When table is provided, the format-appropriate reader is used."""
        arrow_table = pa.table({"a": [1]})
        conn = MagicMock()
        conn.execute.return_value.arrow.return_value = arrow_table
        mock_duckdb.connect.return_value = conn

        ref = _S3DuckDBMaterializedRef(
            {"bucket": "b", "format": "parquet", "region": "us-east-1"}, None, "users"
        )
        ref.to_arrow()

        # Find the call that uses read_parquet
        read_calls = [
            c for c in conn.execute.call_args_list
            if "read_parquet" in str(c)
        ]
        assert len(read_calls) > 0

    @patch("rivet_duckdb.adapters.s3.duckdb")
    @patch("rivet_duckdb.adapters.s3.ensure_extension")
    def test_to_arrow_unsupported_format_raises(self, mock_ensure, mock_duckdb):
        conn = MagicMock()
        mock_duckdb.connect.return_value = conn

        ref = _S3DuckDBMaterializedRef(
            {"bucket": "b", "format": "orc", "region": "us-east-1"}, None, "t"
        )
        with pytest.raises(ExecutionError) as exc_info:
            ref.to_arrow()
        assert exc_info.value.error.code == "RVT-501"

    @patch("rivet_duckdb.adapters.s3.duckdb")
    @patch("rivet_duckdb.adapters.s3.ensure_extension")
    def test_to_arrow_connection_error_raises_rvt501(self, mock_ensure, mock_duckdb):
        conn = MagicMock()
        conn.execute.side_effect = RuntimeError("connection failed")
        mock_duckdb.connect.return_value = conn

        ref = _S3DuckDBMaterializedRef({"bucket": "b", "region": "us-east-1"}, "SELECT 1", None)
        with pytest.raises(ExecutionError) as exc_info:
            ref.to_arrow()
        assert exc_info.value.error.code == "RVT-501"


# ── PluginRegistry integration ────────────────────────────────────────────────


class TestRegistryIntegration:
    def test_adapter_registers_in_registry(self):
        from rivet_core.plugins import PluginRegistry

        registry = PluginRegistry()
        adapter = S3DuckDBAdapter()
        registry.register_adapter(adapter)
        resolved = registry.get_adapter("duckdb", "s3")
        assert resolved is adapter

    def test_capabilities_resolved_via_registry(self):
        from rivet_core.plugins import PluginRegistry

        registry = PluginRegistry()
        adapter = S3DuckDBAdapter()
        registry.register_adapter(adapter)
        caps = registry.resolve_capabilities("duckdb", "s3")
        assert caps is not None
        assert "projection_pushdown" in caps
        assert "write_append" in caps
