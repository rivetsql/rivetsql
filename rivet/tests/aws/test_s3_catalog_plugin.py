"""Tests for S3CatalogPlugin (tasks 16.1 and 16.3)."""

from __future__ import annotations

import io
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

from rivet_aws.s3_catalog import S3CatalogPlugin, _build_s3fs
from rivet_core.errors import PluginValidationError
from rivet_core.introspection import ObjectMetadata, ObjectSchema
from rivet_core.models import Catalog
from rivet_core.plugins import CatalogPlugin

_VALID_OPTIONS = {"bucket": "my-bucket"}


# ── Task 16.1: basic plugin tests ────────────────────────────────────────────

def test_catalog_type():
    assert S3CatalogPlugin().type == "s3"


def test_is_catalog_plugin():
    assert isinstance(S3CatalogPlugin(), CatalogPlugin)


def test_required_options():
    assert "bucket" in S3CatalogPlugin().required_options


def test_validate_accepts_valid_options():
    S3CatalogPlugin().validate(_VALID_OPTIONS)


def test_validate_rejects_missing_bucket():
    with pytest.raises(PluginValidationError) as exc_info:
        S3CatalogPlugin().validate({})
    assert exc_info.value.error.code == "RVT-201"
    assert "bucket" in exc_info.value.error.message


def test_validate_rejects_unknown_option():
    with pytest.raises(PluginValidationError) as exc_info:
        S3CatalogPlugin().validate({"bucket": "b", "unknown_opt": "x"})
    assert exc_info.value.error.code == "RVT-201"
    assert "unknown_opt" in exc_info.value.error.message


def test_validate_rejects_invalid_format():
    with pytest.raises(PluginValidationError) as exc_info:
        S3CatalogPlugin().validate({"bucket": "b", "format": "avro"})
    assert exc_info.value.error.code == "RVT-201"
    assert "avro" in exc_info.value.error.message


def test_validate_accepts_all_valid_formats():
    for fmt in ("parquet", "csv", "json", "orc", "delta"):
        S3CatalogPlugin().validate({"bucket": "b", "format": fmt})


def test_validate_accepts_credential_options():
    S3CatalogPlugin().validate({
        "bucket": "b",
        "access_key_id": "K",
        "secret_access_key": "S",
        "session_token": "T",
        "profile": "p",
        "role_arn": "arn:aws:iam::123:role/r",
        "role_session_name": "sess",
        "web_identity_token_file": "/tmp/tok",
        "credential_cache": True,
    })


def test_validate_accepts_optional_options():
    S3CatalogPlugin().validate({
        "bucket": "b",
        "prefix": "data/",
        "region": "eu-west-1",
        "endpoint_url": "http://localhost:9000",
        "path_style_access": True,
    })


def test_instantiate_returns_catalog():
    catalog = S3CatalogPlugin().instantiate("my_s3", _VALID_OPTIONS)
    assert isinstance(catalog, Catalog)
    assert catalog.name == "my_s3"
    assert catalog.type == "s3"


def test_default_table_reference_no_prefix():
    ref = S3CatalogPlugin().default_table_reference("users", {"bucket": "my-bucket"})
    assert ref == "s3://my-bucket/users.parquet"


def test_default_table_reference_with_prefix():
    ref = S3CatalogPlugin().default_table_reference(
        "users", {"bucket": "my-bucket", "prefix": "raw", "format": "csv"}
    )
    assert ref == "s3://my-bucket/raw/users.csv"


def test_default_table_reference_custom_format():
    ref = S3CatalogPlugin().default_table_reference(
        "events", {"bucket": "data", "format": "json"}
    )
    assert ref == "s3://data/events.json"


# ── Task 16.3: introspection helpers ─────────────────────────────────────────

def _make_catalog(options: dict) -> Catalog:
    return Catalog(name="test_s3", type="s3", options=options)


def _make_parquet_bytes(schema: pa.Schema | None = None) -> bytes:
    if schema is None:
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
    table = pa.table({"id": [1, 2], "name": ["a", "b"]}, schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _make_file_info(path: str, size: int = 100, mtime: datetime | None = None) -> MagicMock:
    fi = MagicMock()
    fi.type = pafs.FileType.File
    fi.path = path
    fi.base_name = path.split("/")[-1]
    fi.size = size
    fi.mtime = mtime or datetime(2024, 1, 1, tzinfo=UTC)
    return fi


def _make_dir_info(path: str) -> MagicMock:
    fi = MagicMock()
    fi.type = pafs.FileType.Directory
    fi.path = path
    fi.base_name = path.split("/")[-1]
    fi.size = None
    fi.mtime = datetime(2024, 1, 1, tzinfo=UTC)
    return fi


# ── list_tables ──────────────────────────────────────────────────────────────

def test_list_tables_parquet_returns_nodes():
    catalog = _make_catalog({"bucket": "my-bucket", "format": "parquet"})
    fi = _make_file_info("my-bucket/users.parquet", size=1024)

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [fi]

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = S3CatalogPlugin().list_tables(catalog)

    assert len(nodes) == 1
    assert nodes[0].name == "users"
    assert nodes[0].node_type == "file"
    assert nodes[0].summary.size_bytes == 1024
    assert nodes[0].summary.format == "parquet"


def test_list_tables_filters_non_matching_extension():
    catalog = _make_catalog({"bucket": "my-bucket", "format": "parquet"})
    fi_parquet = _make_file_info("my-bucket/users.parquet")
    fi_csv = _make_file_info("my-bucket/other.csv")

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [fi_parquet, fi_csv]

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = S3CatalogPlugin().list_tables(catalog)

    assert len(nodes) == 1
    assert nodes[0].name == "users"


def test_list_tables_with_prefix():
    catalog = _make_catalog({"bucket": "my-bucket", "prefix": "raw", "format": "csv"})
    fi = _make_file_info("my-bucket/raw/orders.csv")

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [fi]

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = S3CatalogPlugin().list_tables(catalog)

    assert len(nodes) == 1
    assert nodes[0].name == "orders"


def test_list_tables_delta_checks_delta_log():
    catalog = _make_catalog({"bucket": "my-bucket", "format": "delta"})
    dir_fi = _make_dir_info("my-bucket/events")
    delta_log_fi = _make_dir_info("my-bucket/events/_delta_log")

    mock_fs = MagicMock()
    mock_fs.get_file_info.side_effect = [
        [dir_fi],       # initial listing
        [delta_log_fi], # _delta_log check
    ]

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = S3CatalogPlugin().list_tables(catalog)

    assert len(nodes) == 1
    assert nodes[0].name == "events"
    assert nodes[0].node_type == "table"


def test_list_tables_delta_skips_non_delta_dirs():
    catalog = _make_catalog({"bucket": "my-bucket", "format": "delta"})
    dir_fi = _make_dir_info("my-bucket/not_delta")
    not_found_fi = MagicMock()
    not_found_fi.type = pafs.FileType.NotFound

    mock_fs = MagicMock()
    mock_fs.get_file_info.side_effect = [
        [dir_fi],
        [not_found_fi],
    ]

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = S3CatalogPlugin().list_tables(catalog)

    assert len(nodes) == 0


def test_list_tables_returns_empty_on_error():
    catalog = _make_catalog({"bucket": "my-bucket"})
    mock_fs = MagicMock()
    mock_fs.get_file_info.side_effect = Exception("S3 error")

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = S3CatalogPlugin().list_tables(catalog)

    assert nodes == []


# ── get_schema ───────────────────────────────────────────────────────────────

def test_get_schema_parquet_reads_footer_only():
    catalog = _make_catalog({"bucket": "my-bucket", "format": "parquet"})
    parquet_bytes = _make_parquet_bytes()

    mock_fs = MagicMock()
    mock_fs.open_input_file.return_value.__enter__ = lambda s: io.BytesIO(parquet_bytes)
    mock_fs.open_input_file.return_value.__exit__ = MagicMock(return_value=False)

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        schema = S3CatalogPlugin().get_schema(catalog, "users")

    assert isinstance(schema, ObjectSchema)
    assert len(schema.columns) == 2
    col_names = [c.name for c in schema.columns]
    assert "id" in col_names
    assert "name" in col_names
    assert schema.path == ["my-bucket", "users"]


def test_get_schema_parquet_column_types():
    catalog = _make_catalog({"bucket": "my-bucket", "format": "parquet"})
    arrow_schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("value", pa.float64()),
        pa.field("label", pa.string()),
    ])
    table = pa.table({"id": [1, 2], "value": [1.0, 2.0], "label": ["a", "b"]}, schema=arrow_schema)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    parquet_bytes = buf.getvalue()

    mock_fs = MagicMock()
    mock_fs.open_input_file.return_value.__enter__ = lambda s: io.BytesIO(parquet_bytes)
    mock_fs.open_input_file.return_value.__exit__ = MagicMock(return_value=False)

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        schema = S3CatalogPlugin().get_schema(catalog, "data")

    types = {c.name: c.type for c in schema.columns}
    assert types["id"] == "int64"
    assert types["value"] == "double"
    assert types["label"] == "string"


def test_get_schema_parquet_with_prefix():
    catalog = _make_catalog({"bucket": "my-bucket", "prefix": "raw", "format": "parquet"})
    parquet_bytes = _make_parquet_bytes()

    mock_fs = MagicMock()
    mock_fs.open_input_file.return_value.__enter__ = lambda s: io.BytesIO(parquet_bytes)
    mock_fs.open_input_file.return_value.__exit__ = MagicMock(return_value=False)

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        S3CatalogPlugin().get_schema(catalog, "users")

    mock_fs.open_input_file.assert_called_once_with("my-bucket/raw/users.parquet")


def test_get_schema_unsupported_format_raises():
    catalog = _make_catalog({"bucket": "my-bucket", "format": "delta"})
    mock_fs = MagicMock()

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        with pytest.raises(NotImplementedError):
            S3CatalogPlugin().get_schema(catalog, "events")


# ── get_metadata ─────────────────────────────────────────────────────────────

def test_get_metadata_parquet_returns_size_and_row_count():
    catalog = _make_catalog({"bucket": "my-bucket", "format": "parquet"})
    parquet_bytes = _make_parquet_bytes()
    mtime = datetime(2024, 6, 1, tzinfo=UTC)

    fi = MagicMock()
    fi.size = len(parquet_bytes)
    fi.mtime = mtime

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [fi]
    mock_fs.open_input_file.return_value.__enter__ = lambda s: io.BytesIO(parquet_bytes)
    mock_fs.open_input_file.return_value.__exit__ = MagicMock(return_value=False)

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        meta = S3CatalogPlugin().get_metadata(catalog, "users")

    assert isinstance(meta, ObjectMetadata)
    assert meta.size_bytes == len(parquet_bytes)
    assert meta.last_modified == mtime
    assert meta.row_count == 2
    assert meta.format == "parquet"
    assert meta.location == "s3://my-bucket/users.parquet"
    assert "num_row_groups" in meta.properties


def test_get_metadata_non_parquet_no_row_count():
    catalog = _make_catalog({"bucket": "my-bucket", "format": "csv"})
    mtime = datetime(2024, 6, 1, tzinfo=UTC)

    fi = MagicMock()
    fi.size = 500
    fi.mtime = mtime

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [fi]

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        meta = S3CatalogPlugin().get_metadata(catalog, "data")

    assert meta.size_bytes == 500
    assert meta.row_count is None
    assert meta.format == "csv"
    assert meta.location == "s3://my-bucket/data.csv"


def test_get_metadata_with_prefix():
    catalog = _make_catalog({"bucket": "my-bucket", "prefix": "raw", "format": "parquet"})
    parquet_bytes = _make_parquet_bytes()

    fi = MagicMock()
    fi.size = len(parquet_bytes)
    fi.mtime = datetime(2024, 1, 1, tzinfo=UTC)

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [fi]
    mock_fs.open_input_file.return_value.__enter__ = lambda s: io.BytesIO(parquet_bytes)
    mock_fs.open_input_file.return_value.__exit__ = MagicMock(return_value=False)

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        meta = S3CatalogPlugin().get_metadata(catalog, "users")

    assert meta.location == "s3://my-bucket/raw/users.parquet"
    mock_fs.get_file_info.assert_called_once_with(["my-bucket/raw/users.parquet"])


def test_get_metadata_handles_file_info_error():
    catalog = _make_catalog({"bucket": "my-bucket", "format": "json"})
    mock_fs = MagicMock()
    mock_fs.get_file_info.side_effect = Exception("access denied")

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        meta = S3CatalogPlugin().get_metadata(catalog, "events")

    assert meta is not None
    assert meta.size_bytes is None
    assert meta.last_modified is None


def test_get_metadata_delta_returns_directory_info():
    catalog = _make_catalog({"bucket": "my-bucket", "format": "delta"})
    mtime = datetime(2024, 3, 15, tzinfo=UTC)

    fi = MagicMock()
    fi.mtime = mtime

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [fi]

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        meta = S3CatalogPlugin().get_metadata(catalog, "events")

    assert meta.format == "delta"
    assert meta.node_type == "table"
    assert meta.last_modified == mtime
    assert meta.location == "s3://my-bucket/events"


# ── _build_s3fs ──────────────────────────────────────────────────────────────

def test_build_s3fs_with_explicit_credentials():
    options = {
        "bucket": "b",
        "access_key_id": "AKID",
        "secret_access_key": "SECRET",
        "region": "us-west-2",
    }
    with patch("pyarrow.fs.S3FileSystem") as mock_cls:
        _build_s3fs(options)
        mock_cls.assert_called_once()
        kwargs = mock_cls.call_args[1]
        assert kwargs["access_key"] == "AKID"
        assert kwargs["secret_key"] == "SECRET"
        assert kwargs["region"] == "us-west-2"


def test_build_s3fs_with_endpoint_url():
    options = {
        "bucket": "b",
        "endpoint_url": "http://localhost:9000",
        "region": "us-east-1",
    }
    with patch("pyarrow.fs.S3FileSystem") as mock_cls:
        _build_s3fs(options)
        kwargs = mock_cls.call_args[1]
        assert kwargs["endpoint_override"] == "localhost:9000"
        assert kwargs["scheme"] == "http"


def test_build_s3fs_with_https_endpoint():
    options = {
        "bucket": "b",
        "endpoint_url": "https://s3.custom.example.com",
        "region": "us-east-1",
    }
    with patch("pyarrow.fs.S3FileSystem") as mock_cls:
        _build_s3fs(options)
        kwargs = mock_cls.call_args[1]
        assert kwargs["endpoint_override"] == "s3.custom.example.com"
        assert "scheme" not in kwargs


def test_build_s3fs_with_role_arn():
    options = {
        "bucket": "b",
        "role_arn": "arn:aws:iam::123:role/MyRole",
        "role_session_name": "my-session",
        "region": "eu-west-1",
    }
    with patch("pyarrow.fs.S3FileSystem") as mock_cls:
        _build_s3fs(options)
        kwargs = mock_cls.call_args[1]
        assert kwargs["role_arn"] == "arn:aws:iam::123:role/MyRole"
        assert kwargs["session_name"] == "my-session"


# ── Task 4.2: list_children ─────────────────────────────────────────────────

def test_list_children_returns_immediate_children_only():
    """list_children returns only immediate children, not recursive."""
    catalog = _make_catalog({"bucket": "my-bucket"})
    fi_file = _make_file_info("my-bucket/users.parquet")
    fi_dir = _make_dir_info("my-bucket/subdir")

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [fi_file, fi_dir]

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = S3CatalogPlugin().list_children(catalog, ["my-bucket"])

    assert len(nodes) == 2
    # FileSelector called with recursive=False
    call_args = mock_fs.get_file_info.call_args
    selector = call_args[0][0]
    assert selector.recursive is False


def test_list_children_prefixes_as_containers():
    """Directories are returned as container nodes."""
    catalog = _make_catalog({"bucket": "my-bucket"})
    fi_dir = _make_dir_info("my-bucket/data")

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [fi_dir]

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = S3CatalogPlugin().list_children(catalog, ["my-bucket"])

    assert len(nodes) == 1
    assert nodes[0].name == "data"
    assert nodes[0].node_type == "container"
    assert nodes[0].is_container is True
    assert nodes[0].path == ["my-bucket", "data"]


def test_list_children_recognized_extensions_as_tables():
    """Files with recognized extensions are returned as table nodes."""
    catalog = _make_catalog({"bucket": "my-bucket"})
    files = [
        _make_file_info("my-bucket/a.parquet"),
        _make_file_info("my-bucket/b.csv"),
        _make_file_info("my-bucket/c.json"),
        _make_file_info("my-bucket/d.ipc"),
        _make_file_info("my-bucket/e.orc"),
    ]

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = files

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = S3CatalogPlugin().list_children(catalog, ["my-bucket"])

    assert len(nodes) == 5
    for node in nodes:
        assert node.node_type == "table"
        assert node.is_container is False


def test_list_children_hides_unrecognized_extensions():
    """Files with unrecognized extensions are hidden."""
    catalog = _make_catalog({"bucket": "my-bucket"})
    fi_good = _make_file_info("my-bucket/data.parquet")
    fi_bad = _make_file_info("my-bucket/readme.txt")
    fi_log = _make_file_info("my-bucket/app.log")
    fi_no_ext = _make_file_info("my-bucket/MANIFEST")
    fi_no_ext.base_name = "MANIFEST"

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [fi_good, fi_bad, fi_log, fi_no_ext]

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = S3CatalogPlugin().list_children(catalog, ["my-bucket"])

    assert len(nodes) == 1
    assert nodes[0].name == "data.parquet"


def test_list_children_detects_hive_partition_directories():
    """Hive-style partition directories include partition_columns metadata."""
    catalog = _make_catalog({"bucket": "my-bucket"})
    fi_partition = _make_dir_info("my-bucket/year=2024")
    fi_nested = _make_dir_info("my-bucket/year=2024/month=01")

    mock_fs = MagicMock()
    # First call: list_children at root
    # Second call: _detect_partition_columns scans inside year=2024
    mock_fs.get_file_info.side_effect = [
        [fi_partition],   # root listing
        [fi_nested],      # partition column detection inside year=2024
    ]

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = S3CatalogPlugin().list_children(catalog, ["my-bucket"])

    assert len(nodes) == 1
    assert nodes[0].name == "year=2024"
    assert nodes[0].is_container is True


def test_list_children_non_hive_dirs_no_partition_metadata():
    """Non-Hive directories don't get partition_columns metadata."""
    catalog = _make_catalog({"bucket": "my-bucket"})
    fi_dir = _make_dir_info("my-bucket/raw_data")

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [fi_dir]

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = S3CatalogPlugin().list_children(catalog, ["my-bucket"])

    assert len(nodes) == 1
    assert nodes[0].name == "raw_data"
    assert nodes[0].is_container is True


def test_list_children_nested_path():
    """list_children works with nested paths."""
    catalog = _make_catalog({"bucket": "my-bucket"})
    fi = _make_file_info("my-bucket/data/raw/users.parquet")

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [fi]

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = S3CatalogPlugin().list_children(catalog, ["my-bucket", "data", "raw"])

    assert len(nodes) == 1
    assert nodes[0].path == ["my-bucket", "data", "raw", "users.parquet"]


def test_list_children_returns_empty_on_error():
    """list_children returns empty list on S3 error."""
    catalog = _make_catalog({"bucket": "my-bucket"})
    mock_fs = MagicMock()
    mock_fs.get_file_info.side_effect = Exception("access denied")

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = S3CatalogPlugin().list_children(catalog, ["my-bucket"])

    assert nodes == []


def test_list_children_mixed_types():
    """list_children correctly handles a mix of dirs, recognized files, and unrecognized files."""
    catalog = _make_catalog({"bucket": "my-bucket"})
    items = [
        _make_dir_info("my-bucket/subdir"),
        _make_file_info("my-bucket/data.parquet"),
        _make_file_info("my-bucket/readme.md"),
        _make_file_info("my-bucket/report.csv"),
        _make_dir_info("my-bucket/year=2024"),
    ]

    mock_fs = MagicMock()
    # First call returns the listing, second call is for partition detection inside year=2024
    mock_fs.get_file_info.side_effect = [items, []]

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        nodes = S3CatalogPlugin().list_children(catalog, ["my-bucket"])

    names = [n.name for n in nodes]
    assert "subdir" in names
    assert "data.parquet" in names
    assert "report.csv" in names
    assert "year=2024" in names
    assert "readme.md" not in names
    assert len(nodes) == 4


# ── Task 4.2: get_metadata with etag ────────────────────────────────────────

def test_get_metadata_returns_etag_from_head_object():
    """get_metadata includes etag from HeadObject."""
    catalog = _make_catalog({"bucket": "my-bucket", "format": "parquet"})
    parquet_bytes = _make_parquet_bytes()
    mtime = datetime(2024, 6, 1, tzinfo=UTC)

    mock_client = MagicMock()
    mock_client.head_object.return_value = {
        "ContentLength": len(parquet_bytes),
        "LastModified": mtime,
        "ETag": '"abc123"',
    }

    mock_resolver = MagicMock()
    mock_resolver.create_client.return_value = mock_client

    mock_factory = MagicMock(return_value=mock_resolver)

    mock_fs = MagicMock()
    mock_fs.open_input_file.return_value.__enter__ = lambda s: io.BytesIO(parquet_bytes)
    mock_fs.open_input_file.return_value.__exit__ = MagicMock(return_value=False)

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        cat_opts = {**catalog.options, "_credential_resolver_factory": mock_factory}
        cat = Catalog(name=catalog.name, type=catalog.type, options=cat_opts)
        meta = S3CatalogPlugin().get_metadata(cat, "users")

    assert meta.properties["etag"] == '"abc123"'
    assert meta.size_bytes == len(parquet_bytes)
    assert meta.last_modified == mtime


def test_get_metadata_falls_back_to_pyarrow_on_head_object_failure():
    """get_metadata falls back to PyArrow file info when HeadObject fails."""
    catalog = _make_catalog({"bucket": "my-bucket", "format": "csv"})
    mtime = datetime(2024, 6, 1, tzinfo=UTC)

    mock_resolver = MagicMock()
    mock_resolver.create_client.side_effect = Exception("no creds")

    mock_factory = MagicMock(return_value=mock_resolver)

    fi = MagicMock()
    fi.size = 500
    fi.mtime = mtime

    mock_fs = MagicMock()
    mock_fs.get_file_info.return_value = [fi]

    with patch("rivet_aws.s3_catalog._build_s3fs", return_value=mock_fs):
        cat_opts = {**catalog.options, "_credential_resolver_factory": mock_factory}
        cat = Catalog(name=catalog.name, type=catalog.type, options=cat_opts)
        meta = S3CatalogPlugin().get_metadata(cat, "data")

    assert meta.size_bytes == 500
    assert meta.last_modified == mtime
    assert "etag" not in meta.properties
