"""Tests for S3PolarsAdapter (task 30.1).

Tests use mocked polars/s3fs/deltalake to avoid real S3 calls.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from rivet_core.errors import ExecutionError, PluginValidationError
from rivet_core.models import Catalog, Joint, Material
from rivet_core.optimizer import EMPTY_RESIDUAL, AdapterPushdownResult
from rivet_polars.adapters.s3 import (
    S3PolarsAdapter,
    _build_s3_path,
    _build_storage_options,
)
from rivet_polars.engine import ALL_6_CAPABILITIES


def _catalog(options: dict) -> Catalog:
    return Catalog(name="my_s3", type="s3", options=options)


def _source_joint(table: str = "users") -> Joint:
    return Joint(name="j1", joint_type="source", catalog="my_s3", table=table)


def _sink_joint(table: str = "users", write_strategy: str = "replace") -> Joint:
    j = Joint(name="j1", joint_type="sink", catalog="my_s3", table=table)
    j.write_strategy = write_strategy
    return j


def _material() -> Material:
    arrow_table = pa.table({"x": [1, 2]})
    mock_ref = MagicMock()
    mock_ref.to_arrow.return_value = arrow_table
    return Material(name="j1", catalog="my_s3", materialized_ref=mock_ref, state="ready")


# ── Adapter registration ──────────────────────────────────────────────────────


class TestS3PolarsAdapterRegistration:
    def test_target_engine_type(self):
        assert S3PolarsAdapter().target_engine_type == "polars"

    def test_catalog_type(self):
        assert S3PolarsAdapter().catalog_type == "s3"

    def test_source_is_engine_plugin(self):
        assert S3PolarsAdapter().source == "engine_plugin"

    def test_source_plugin_is_rivet_polars(self):
        assert S3PolarsAdapter().source_plugin == "rivet_polars"

    def test_capabilities_include_all_6(self):
        adapter = S3PolarsAdapter()
        for cap in ALL_6_CAPABILITIES:
            assert cap in adapter.capabilities

    def test_capabilities_include_write_strategies(self):
        adapter = S3PolarsAdapter()
        assert "write_append" in adapter.capabilities
        assert "write_replace" in adapter.capabilities
        assert "write_partition" in adapter.capabilities


# ── storage_options building ──────────────────────────────────────────────────


class TestBuildStorageOptions:
    def test_empty_options_returns_empty_dict(self):
        result = _build_storage_options({"bucket": "my-bucket"})
        assert isinstance(result, dict)

    def test_explicit_credentials(self):
        opts = {"bucket": "b", "access_key_id": "AKID", "secret_access_key": "SECRET"}
        result = _build_storage_options(opts)
        assert result["aws_access_key_id"] == "AKID"
        assert result["aws_secret_access_key"] == "SECRET"

    def test_session_token_included(self):
        opts = {
            "bucket": "b",
            "access_key_id": "AKID",
            "secret_access_key": "SECRET",
            "session_token": "TOKEN",
        }
        result = _build_storage_options(opts)
        assert result["aws_session_token"] == "TOKEN"

    def test_region_included(self):
        result = _build_storage_options({"bucket": "b", "region": "eu-west-1"})
        assert result["region_name"] == "eu-west-1"

    def test_endpoint_url_included(self):
        result = _build_storage_options({"bucket": "b", "endpoint_url": "http://localhost:9000"})
        assert result["endpoint_url"] == "http://localhost:9000"

    def test_no_credentials_omits_keys(self):
        result = _build_storage_options({"bucket": "b"})
        assert "aws_access_key_id" not in result
        assert "aws_secret_access_key" not in result


# ── S3 path building ──────────────────────────────────────────────────────────


class TestBuildS3Path:
    def test_parquet_no_prefix(self):
        assert _build_s3_path({"bucket": "my-bucket", "format": "parquet"}, "users") == "s3://my-bucket/users.parquet"

    def test_parquet_with_prefix(self):
        assert _build_s3_path({"bucket": "my-bucket", "prefix": "raw", "format": "parquet"}, "users") == "s3://my-bucket/raw/users.parquet"

    def test_csv_format(self):
        assert _build_s3_path({"bucket": "b", "format": "csv"}, "t") == "s3://b/t.csv"

    def test_json_format(self):
        assert _build_s3_path({"bucket": "b", "format": "json"}, "t") == "s3://b/t.json"

    def test_delta_format_no_extension(self):
        assert _build_s3_path({"bucket": "b", "format": "delta"}, "t") == "s3://b/t"

    def test_delta_format_with_prefix(self):
        assert _build_s3_path({"bucket": "b", "prefix": "warehouse", "format": "delta"}, "t") == "s3://b/warehouse/t"


# ── read_dispatch ─────────────────────────────────────────────────────────────


class TestReadDispatch:
    def test_read_dispatch_returns_material(self):
        adapter = S3PolarsAdapter()
        catalog = _catalog({"bucket": "b", "format": "parquet"})
        joint = _source_joint()

        mock_lf = MagicMock()
        mock_lf.collect.return_value.to_arrow.return_value = pa.table({"x": [1]})

        with patch("polars.scan_parquet", return_value=mock_lf):
            result = adapter.read_dispatch(None, catalog, joint)

        assert isinstance(result, AdapterPushdownResult)
        assert isinstance(result.material, Material)
        assert result.material.name == "j1"
        assert result.residual == EMPTY_RESIDUAL

    def test_read_dispatch_parquet_calls_scan_parquet(self):
        adapter = S3PolarsAdapter()
        catalog = _catalog({"bucket": "b", "format": "parquet"})
        joint = _source_joint()

        mock_lf = MagicMock()
        with patch("polars.scan_parquet", return_value=mock_lf) as mock_scan:
            adapter.read_dispatch(None, catalog, joint)
            mock_scan.assert_called_once()
            assert "s3://b/users.parquet" in str(mock_scan.call_args)

    def test_read_dispatch_csv_calls_scan_csv(self):
        adapter = S3PolarsAdapter()
        catalog = _catalog({"bucket": "b", "format": "csv"})
        joint = _source_joint()

        mock_lf = MagicMock()
        with patch("polars.scan_csv", return_value=mock_lf) as mock_scan:
            adapter.read_dispatch(None, catalog, joint)
            mock_scan.assert_called_once()

    def test_read_dispatch_delta_calls_scan_delta(self):
        adapter = S3PolarsAdapter()
        catalog = _catalog({"bucket": "b", "format": "delta"})
        joint = _source_joint()

        mock_lf = MagicMock()
        with patch("polars.scan_delta", return_value=mock_lf) as mock_scan:
            adapter.read_dispatch(None, catalog, joint)
            mock_scan.assert_called_once()

    def test_read_dispatch_unsupported_format_raises(self):
        adapter = S3PolarsAdapter()
        catalog = _catalog({"bucket": "b", "format": "orc"})
        joint = _source_joint()

        with pytest.raises(ExecutionError) as exc_info:
            adapter.read_dispatch(None, catalog, joint)
        assert exc_info.value.error.code == "RVT-501"

    def test_read_dispatch_to_arrow_collects(self):
        adapter = S3PolarsAdapter()
        catalog = _catalog({"bucket": "b", "format": "parquet"})
        joint = _source_joint()

        expected = pa.table({"x": [1, 2]})
        mock_lf = MagicMock()
        mock_lf.collect.return_value.to_arrow.return_value = expected

        with patch("polars.scan_parquet", return_value=mock_lf):
            result = adapter.read_dispatch(None, catalog, joint)

        assert result.material.materialized_ref.to_arrow().equals(expected)


# ── write_dispatch ────────────────────────────────────────────────────────────


class TestWriteDispatch:
    def test_write_parquet_calls_write_parquet(self):
        adapter = S3PolarsAdapter()
        catalog = _catalog({"bucket": "b", "format": "parquet"})
        joint = _sink_joint(write_strategy="replace")
        mat = _material()

        mock_df = MagicMock()
        with patch("polars.from_arrow", return_value=mock_df):
            adapter.write_dispatch(None, catalog, joint, mat)
            mock_df.write_parquet.assert_called_once()

    def test_write_csv_calls_write_csv(self):
        adapter = S3PolarsAdapter()
        catalog = _catalog({"bucket": "b", "format": "csv"})
        joint = _sink_joint(write_strategy="replace")
        mat = _material()

        mock_df = MagicMock()
        with patch("polars.from_arrow", return_value=mock_df):
            adapter.write_dispatch(None, catalog, joint, mat)
            mock_df.write_csv.assert_called_once()

    def test_write_delta_calls_write_delta(self):
        adapter = S3PolarsAdapter()
        catalog = _catalog({"bucket": "b", "format": "delta"})
        joint = _sink_joint(write_strategy="replace")
        mat = _material()

        mock_df = MagicMock()
        with patch("polars.from_arrow", return_value=mock_df):
            adapter.write_dispatch(None, catalog, joint, mat)
            mock_df.write_delta.assert_called_once()

    def test_write_unsupported_format_raises(self):
        adapter = S3PolarsAdapter()
        catalog = _catalog({"bucket": "b", "format": "orc"})
        joint = _sink_joint(write_strategy="replace")
        mat = _material()

        mock_df = MagicMock()
        with patch("polars.from_arrow", return_value=mock_df):
            with pytest.raises(ExecutionError) as exc_info:
                adapter.write_dispatch(None, catalog, joint, mat)
            assert exc_info.value.error.code == "RVT-501"


# ── Delta validation ──────────────────────────────────────────────────────────


class TestDeltaValidation:
    """Task 30.4: Fail at validation time if deltalake not installed and Delta requested."""

    def test_validate_delta_format_with_deltalake_installed(self):
        adapter = S3PolarsAdapter()
        with patch("importlib.util.find_spec", return_value=MagicMock()):
            adapter.validate_catalog_options({"bucket": "b", "format": "delta"})

    def test_validate_delta_format_without_deltalake_raises(self):
        adapter = S3PolarsAdapter()
        with patch("importlib.util.find_spec", return_value=None):
            with pytest.raises(PluginValidationError) as exc_info:
                adapter.validate_catalog_options({"bucket": "b", "format": "delta"})
            assert exc_info.value.error.code == "RVT-201"
            msg = exc_info.value.error.message.lower()
            assert "delta" in msg or "deltalake" in msg

    def test_validate_non_delta_format_does_not_check_deltalake(self):
        adapter = S3PolarsAdapter()
        with patch("importlib.util.find_spec", return_value=None):
            # Should not raise even without deltalake
            adapter.validate_catalog_options({"bucket": "b", "format": "parquet"})


# ── s3fs listing ─────────────────────────────────────────────────────────────


class TestS3fsListing:
    """S3PolarsAdapter uses s3fs for path listing."""

    def _mock_s3fs(self, glob_return: list) -> tuple:
        """Return (mock_module, mock_fs) for patching sys.modules['s3fs']."""
        mock_fs = MagicMock()
        mock_fs.glob.return_value = glob_return
        mock_s3fs_module = MagicMock()
        mock_s3fs_module.S3FileSystem.return_value = mock_fs
        return mock_s3fs_module, mock_fs

    def test_list_paths_uses_s3fs(self):
        adapter = S3PolarsAdapter()
        catalog_options = {"bucket": "b", "prefix": "raw", "format": "parquet"}

        mock_module, mock_fs = self._mock_s3fs(["b/raw/users.parquet", "b/raw/orders.parquet"])

        with patch.dict("sys.modules", {"s3fs": mock_module}):
            paths = adapter.list_paths(catalog_options)

        assert len(paths) == 2
        mock_fs.glob.assert_called_once()

    def test_list_paths_instantiates_s3fs(self):
        adapter = S3PolarsAdapter()
        catalog_options = {
            "bucket": "b",
            "access_key_id": "AKID",
            "secret_access_key": "SECRET",
        }

        mock_module, mock_fs = self._mock_s3fs([])

        with patch.dict("sys.modules", {"s3fs": mock_module}):
            adapter.list_paths(catalog_options)
            assert mock_module.S3FileSystem.called
