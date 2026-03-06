"""Tests for S3Sink plugin (task 17.2): sink options path, format, write_strategy,
partition_by, compression, overwrite_files."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from rivet_aws.s3_sink import (
    _ALL_STRATEGIES,
    _BASE_STRATEGIES,
    _DELTA_ONLY_STRATEGIES,
    S3Sink,
    _build_delta_storage_options,
    _parse_sink_options,
    _resolve_sink_path,
)
from rivet_core.errors import ExecutionError, PluginValidationError
from rivet_core.models import Catalog, Joint, Material
from rivet_core.plugins import SinkPlugin
from rivet_core.strategies import MaterializedRef

# ── Fixtures ──────────────────────────────────────────────────────────────────

def _catalog(options: dict | None = None) -> Catalog:
    return Catalog(name="my-s3", type="s3", options=options or {"bucket": "my-bucket"})


def _joint(name: str = "test_joint", table: str | None = None, write_strategy: str | None = None) -> Joint:
    return Joint(
        name=name,
        joint_type="sink",
        catalog="my-s3",
        table=table,
        write_strategy=write_strategy,
        upstream=["upstream_joint"],
    )


def _material(table: pa.Table | None = None) -> Material:
    if table is None:
        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})

    class _Ref(MaterializedRef):
        def __init__(self, t: pa.Table) -> None:
            self._t = t

        def to_arrow(self) -> pa.Table:
            return self._t

        @property
        def schema(self) -> Any:
            return None

        @property
        def row_count(self) -> int:
            return self._t.num_rows

        @property
        def size_bytes(self) -> int | None:
            return None

        @property
        def storage_type(self) -> str:
            return "s3"

    from typing import Any
    mat = Material(name="test_joint", catalog="my-s3", state="materialized", materialized_ref=_Ref(table))
    return mat


# ── Plugin contract ───────────────────────────────────────────────────────────

def test_s3_sink_is_sink_plugin():
    assert isinstance(S3Sink(), SinkPlugin)


def test_s3_sink_catalog_type():
    assert S3Sink().catalog_type == "s3"


def test_s3_sink_supported_strategies_includes_base():
    sink = S3Sink()
    for s in _BASE_STRATEGIES:
        assert s in sink.supported_strategies


def test_s3_sink_supported_strategies_includes_delta():
    sink = S3Sink()
    for s in _DELTA_ONLY_STRATEGIES:
        assert s in sink.supported_strategies


# ── _resolve_sink_path ────────────────────────────────────────────────────────

def test_resolve_sink_path_explicit_full_uri():
    path = _resolve_sink_path({"bucket": "b"}, {"path": "s3://other-bucket/output/"})
    assert path == "s3://other-bucket/output/"


def test_resolve_sink_path_relative():
    path = _resolve_sink_path({"bucket": "b"}, {"path": "output/data"})
    assert path == "s3://b/output/data"


def test_resolve_sink_path_relative_with_prefix():
    path = _resolve_sink_path({"bucket": "b", "prefix": "raw"}, {"path": "users"})
    assert path == "s3://b/raw/users"


def test_resolve_sink_path_no_path_uses_bucket():
    path = _resolve_sink_path({"bucket": "b"}, {})
    assert path == "s3://b"


def test_resolve_sink_path_no_path_with_prefix():
    path = _resolve_sink_path({"bucket": "b", "prefix": "output"}, {})
    assert path == "s3://b/output"


# ── _parse_sink_options ───────────────────────────────────────────────────────

def test_parse_sink_options_defaults():
    cat_opts = {"bucket": "b", "sink_options": {"path": "out/"}}
    joint = _joint()
    opts = _parse_sink_options(cat_opts, joint)
    assert opts["format"] == "parquet"
    assert opts["write_strategy"] == "replace"
    assert opts["compression"] == "snappy"
    assert opts["overwrite_files"] is True
    assert opts["partition_by"] is None


def test_parse_sink_options_format_from_sink_options():
    cat_opts = {"bucket": "b", "sink_options": {"path": "out/", "format": "csv"}}
    opts = _parse_sink_options(cat_opts, _joint())
    assert opts["format"] == "csv"


def test_parse_sink_options_format_from_catalog():
    cat_opts = {"bucket": "b", "format": "orc", "sink_options": {"path": "out/"}}
    opts = _parse_sink_options(cat_opts, _joint())
    assert opts["format"] == "orc"


def test_parse_sink_options_sink_format_overrides_catalog():
    cat_opts = {"bucket": "b", "format": "parquet", "sink_options": {"path": "out/", "format": "csv"}}
    opts = _parse_sink_options(cat_opts, _joint())
    assert opts["format"] == "csv"


def test_parse_sink_options_write_strategy_from_sink_options():
    cat_opts = {"bucket": "b", "sink_options": {"path": "out/", "write_strategy": "append"}}
    opts = _parse_sink_options(cat_opts, _joint())
    assert opts["write_strategy"] == "append"


def test_parse_sink_options_write_strategy_from_joint():
    cat_opts = {"bucket": "b", "sink_options": {"path": "out/"}}
    opts = _parse_sink_options(cat_opts, _joint(write_strategy="append"))
    assert opts["write_strategy"] == "append"


def test_parse_sink_options_partition_by_list():
    cat_opts = {"bucket": "b", "sink_options": {"path": "out/", "partition_by": ["year", "month"]}}
    opts = _parse_sink_options(cat_opts, _joint())
    assert opts["partition_by"] == ["year", "month"]


def test_parse_sink_options_partition_by_string_coerced_to_list():
    cat_opts = {"bucket": "b", "sink_options": {"path": "out/", "partition_by": "year"}}
    opts = _parse_sink_options(cat_opts, _joint())
    assert opts["partition_by"] == ["year"]


def test_parse_sink_options_compression():
    cat_opts = {"bucket": "b", "sink_options": {"path": "out/", "compression": "gzip"}}
    opts = _parse_sink_options(cat_opts, _joint())
    assert opts["compression"] == "gzip"


def test_parse_sink_options_overwrite_files_false():
    cat_opts = {"bucket": "b", "sink_options": {"path": "out/", "overwrite_files": False}}
    opts = _parse_sink_options(cat_opts, _joint())
    assert opts["overwrite_files"] is False


def test_parse_sink_options_joint_table_as_path():
    cat_opts = {"bucket": "b"}
    opts = _parse_sink_options(cat_opts, _joint(table="output/users"))
    assert "output/users" in opts["path"]


def test_parse_sink_options_sink_options_path_overrides_joint_table():
    cat_opts = {"bucket": "b", "sink_options": {"path": "explicit/path"}}
    opts = _parse_sink_options(cat_opts, _joint(table="ignored"))
    assert "explicit/path" in opts["path"]


def test_parse_sink_options_invalid_format_raises():
    cat_opts = {"bucket": "b", "sink_options": {"path": "out/", "format": "avro"}}
    with pytest.raises(PluginValidationError) as exc_info:
        _parse_sink_options(cat_opts, _joint())
    assert exc_info.value.error.code == "RVT-202"
    assert "avro" in exc_info.value.error.message


def test_parse_sink_options_merge_without_delta_raises():
    cat_opts = {"bucket": "b", "sink_options": {"path": "out/", "write_strategy": "merge", "format": "parquet"}}
    with pytest.raises(PluginValidationError) as exc_info:
        _parse_sink_options(cat_opts, _joint())
    assert exc_info.value.error.code == "RVT-202"
    assert "delta" in exc_info.value.error.message.lower()


def test_parse_sink_options_scd2_without_delta_raises():
    cat_opts = {"bucket": "b", "sink_options": {"path": "out/", "write_strategy": "scd2", "format": "parquet"}}
    with pytest.raises(PluginValidationError) as exc_info:
        _parse_sink_options(cat_opts, _joint())
    assert exc_info.value.error.code == "RVT-202"


def test_parse_sink_options_merge_with_delta_ok():
    cat_opts = {"bucket": "b", "sink_options": {"path": "out/", "write_strategy": "merge", "format": "delta"}}
    opts = _parse_sink_options(cat_opts, _joint())
    assert opts["write_strategy"] == "merge"
    assert opts["format"] == "delta"


def test_parse_sink_options_unsupported_strategy_raises():
    cat_opts = {"bucket": "b", "sink_options": {"path": "out/", "write_strategy": "unknown_strategy"}}
    with pytest.raises(PluginValidationError) as exc_info:
        _parse_sink_options(cat_opts, _joint())
    assert exc_info.value.error.code == "RVT-202"


# ── S3Sink.write() ────────────────────────────────────────────────────────────

def test_write_calls_write_dataset_for_parquet():
    """write() calls pyarrow dataset writer for parquet format."""
    cat = _catalog({"bucket": "b", "sink_options": {"path": "output/"}})
    joint = _joint()
    mat = _material()

    with patch("rivet_aws.s3_sink._build_s3fs") as mock_fs, \
         patch("pyarrow.dataset.write_dataset") as mock_write:
        mock_fs.return_value = MagicMock()
        S3Sink().write(cat, joint, mat, "replace")

    mock_write.assert_called_once()


def test_write_calls_write_dataset_for_csv():
    cat = _catalog({"bucket": "b", "sink_options": {"path": "output/", "format": "csv"}})
    joint = _joint()
    mat = _material()

    with patch("rivet_aws.s3_sink._build_s3fs") as mock_fs, \
         patch("pyarrow.dataset.write_dataset") as mock_write:
        mock_fs.return_value = MagicMock()
        S3Sink().write(cat, joint, mat, "replace")

    mock_write.assert_called_once()


def test_write_json_format_raises():
    cat = _catalog({"bucket": "b", "sink_options": {"path": "output/", "format": "json"}})
    joint = _joint()
    mat = _material()

    with patch("rivet_aws.s3_sink._build_s3fs") as mock_fs:
        mock_fs.return_value = MagicMock()
        with pytest.raises(ExecutionError) as exc_info:
            S3Sink().write(cat, joint, mat, "replace")
    assert "json" in exc_info.value.error.message.lower()


def test_write_replace_uses_delete_matching():
    cat = _catalog({"bucket": "b", "sink_options": {"path": "output/"}})
    joint = _joint()
    mat = _material()

    with patch("rivet_aws.s3_sink._build_s3fs") as mock_fs, \
         patch("pyarrow.dataset.write_dataset") as mock_write:
        mock_fs.return_value = MagicMock()
        S3Sink().write(cat, joint, mat, "replace")

    call_kwargs = mock_write.call_args.kwargs
    assert call_kwargs["existing_data_behavior"] == "delete_matching"


def test_write_append_uses_overwrite_or_ignore():
    cat = _catalog({"bucket": "b", "sink_options": {"path": "output/"}})
    joint = _joint()
    mat = _material()

    with patch("rivet_aws.s3_sink._build_s3fs") as mock_fs, \
         patch("pyarrow.dataset.write_dataset") as mock_write:
        mock_fs.return_value = MagicMock()
        S3Sink().write(cat, joint, mat, "append")

    call_kwargs = mock_write.call_args.kwargs
    assert call_kwargs["existing_data_behavior"] == "overwrite_or_ignore"


def test_write_with_partition_by_passes_partitioning():
    cat = _catalog({"bucket": "b", "sink_options": {"path": "output/", "partition_by": ["year"]}})
    joint = _joint()
    table = pa.table({"id": [1, 2], "year": ["2024", "2024"]})
    mat = _material(table)

    with patch("rivet_aws.s3_sink._build_s3fs") as mock_fs, \
         patch("pyarrow.dataset.write_dataset") as mock_write, \
         patch("pyarrow.dataset.partitioning") as mock_partitioning:
        mock_fs.return_value = MagicMock()
        mock_partitioning.return_value = MagicMock()
        S3Sink().write(cat, joint, mat, "replace")

    mock_partitioning.assert_called_once()
    call_kwargs = mock_write.call_args.kwargs
    assert call_kwargs["partitioning"] is not None


def test_write_without_partition_by_no_partitioning():
    cat = _catalog({"bucket": "b", "sink_options": {"path": "output/"}})
    joint = _joint()
    mat = _material()

    with patch("rivet_aws.s3_sink._build_s3fs") as mock_fs, \
         patch("pyarrow.dataset.write_dataset") as mock_write:
        mock_fs.return_value = MagicMock()
        S3Sink().write(cat, joint, mat, "replace")

    call_kwargs = mock_write.call_args.kwargs
    assert call_kwargs.get("partitioning") is None


def test_write_delta_calls_write_deltalake():
    cat = _catalog({"bucket": "b", "sink_options": {"path": "s3://b/delta_table/", "format": "delta"}})
    joint = _joint()
    mat = _material()

    mock_deltalake = MagicMock()
    with patch.dict("sys.modules", {"deltalake": mock_deltalake}):
        S3Sink().write(cat, joint, mat, "replace")

    mock_deltalake.write_deltalake.assert_called_once()


def test_write_delta_merge_strategy():
    cat = _catalog({"bucket": "b", "sink_options": {"path": "s3://b/delta_table/", "format": "delta"}})
    joint = _joint()
    mat = _material()

    mock_deltalake = MagicMock()
    with patch.dict("sys.modules", {"deltalake": mock_deltalake}):
        S3Sink().write(cat, joint, mat, "merge")

    call_kwargs = mock_deltalake.write_deltalake.call_args.kwargs
    assert call_kwargs["mode"] == "merge"


def test_write_delta_missing_package_raises():
    cat = _catalog({"bucket": "b", "sink_options": {"path": "s3://b/delta_table/", "format": "delta"}})
    joint = _joint()
    mat = _material()

    with patch.dict("sys.modules", {"deltalake": None}):
        with pytest.raises(ExecutionError) as exc_info:
            S3Sink().write(cat, joint, mat, "replace")
    assert "deltalake" in exc_info.value.error.message.lower()


def test_write_merge_without_delta_raises_at_write():
    """merge strategy without delta format raises PluginValidationError."""
    cat = _catalog({"bucket": "b", "sink_options": {"path": "output/", "format": "parquet"}})
    joint = _joint()
    mat = _material()

    with pytest.raises(PluginValidationError) as exc_info:
        S3Sink().write(cat, joint, mat, "merge")
    assert exc_info.value.error.code == "RVT-202"


def test_write_unsupported_strategy_raises():
    cat = _catalog({"bucket": "b", "sink_options": {"path": "output/"}})
    joint = _joint()
    mat = _material()

    with pytest.raises((ExecutionError, PluginValidationError)):
        S3Sink().write(cat, joint, mat, "unknown_strategy")


# ── _build_delta_storage_options ─────────────────────────────────────────────

def test_build_delta_storage_options_with_keys():
    opts = _build_delta_storage_options({
        "region": "eu-west-1",
        "access_key_id": "AKID",
        "secret_access_key": "SECRET",
        "session_token": "TOKEN",
    })
    assert opts["AWS_REGION"] == "eu-west-1"
    assert opts["AWS_ACCESS_KEY_ID"] == "AKID"
    assert opts["AWS_SECRET_ACCESS_KEY"] == "SECRET"
    assert opts["AWS_SESSION_TOKEN"] == "TOKEN"


def test_build_delta_storage_options_with_endpoint():
    opts = _build_delta_storage_options({
        "endpoint_url": "http://localhost:9000",
    })
    assert opts["AWS_ENDPOINT_URL"] == "http://localhost:9000"


def test_build_delta_storage_options_defaults():
    opts = _build_delta_storage_options({"bucket": "b"})
    assert opts["AWS_REGION"] == "us-east-1"
    assert "AWS_ACCESS_KEY_ID" not in opts


# ── Strategy constants ────────────────────────────────────────────────────────

def test_base_strategies_set():
    assert "append" in _BASE_STRATEGIES
    assert "replace" in _BASE_STRATEGIES
    assert "partition" in _BASE_STRATEGIES
    assert "truncate_insert" in _BASE_STRATEGIES
    assert "delete_insert" in _BASE_STRATEGIES
    assert "incremental_append" in _BASE_STRATEGIES


def test_delta_only_strategies_set():
    assert "merge" in _DELTA_ONLY_STRATEGIES
    assert "scd2" in _DELTA_ONLY_STRATEGIES


def test_all_strategies_union():
    assert _ALL_STRATEGIES == _BASE_STRATEGIES | _DELTA_ONLY_STRATEGIES
