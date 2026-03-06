"""Tests for S3Source plugin (task 17.1): source options path, partition_columns, format, schema."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from rivet_aws.s3_source import S3DeferredMaterializedRef, S3Source, _resolve_s3_path
from rivet_core.models import Catalog, Joint, Material
from rivet_core.plugins import SourcePlugin

# ── Fixtures ──────────────────────────────────────────────────────────────────

def _catalog(options: dict | None = None) -> Catalog:
    return Catalog(name="my-s3", type="s3", options=options or {"bucket": "my-bucket"})


def _joint(name: str = "test_joint", table: str | None = None) -> Joint:
    return Joint(name=name, joint_type="source", catalog="my-s3", table=table)


# ── Plugin contract ───────────────────────────────────────────────────────────

def test_s3_source_is_source_plugin():
    assert isinstance(S3Source(), SourcePlugin)


def test_s3_source_catalog_type():
    assert S3Source().catalog_type == "s3"


# ── read() returns Material ───────────────────────────────────────────────────

def test_read_returns_material():
    mat = S3Source().read(_catalog(), _joint(), None)
    assert isinstance(mat, Material)


def test_read_material_name_matches_joint():
    mat = S3Source().read(_catalog(), _joint("my_joint"), None)
    assert mat.name == "my_joint"


def test_read_material_catalog_matches():
    mat = S3Source().read(_catalog(), _joint(), None)
    assert mat.catalog == "my-s3"


def test_read_material_state_is_deferred():
    mat = S3Source().read(_catalog(), _joint(), None)
    assert mat.state == "deferred"


def test_read_material_has_materialized_ref():
    mat = S3Source().read(_catalog(), _joint(), None)
    assert mat.materialized_ref is not None
    assert isinstance(mat.materialized_ref, S3DeferredMaterializedRef)


# ── _resolve_s3_path ──────────────────────────────────────────────────────────

def test_resolve_path_default_parquet():
    path = _resolve_s3_path({"bucket": "my-bucket"}, {})
    assert path == "s3://my-bucket/*.parquet"


def test_resolve_path_with_prefix():
    path = _resolve_s3_path({"bucket": "my-bucket", "prefix": "raw"}, {})
    assert path == "s3://my-bucket/raw/*.parquet"


def test_resolve_path_format_override():
    path = _resolve_s3_path({"bucket": "my-bucket"}, {"format": "csv"})
    assert path == "s3://my-bucket/*.csv"


def test_resolve_path_catalog_format():
    path = _resolve_s3_path({"bucket": "my-bucket", "format": "json"}, {})
    assert path == "s3://my-bucket/*.json"


def test_resolve_path_explicit_path_relative():
    path = _resolve_s3_path({"bucket": "my-bucket"}, {"path": "data/users.parquet"})
    assert path == "s3://my-bucket/data/users.parquet"


def test_resolve_path_explicit_path_with_prefix():
    path = _resolve_s3_path({"bucket": "my-bucket", "prefix": "raw"}, {"path": "users.parquet"})
    assert path == "s3://my-bucket/raw/users.parquet"


def test_resolve_path_explicit_full_s3_uri():
    path = _resolve_s3_path({"bucket": "my-bucket"}, {"path": "s3://other-bucket/data/*.parquet"})
    assert path == "s3://other-bucket/data/*.parquet"


def test_resolve_path_glob_pattern():
    path = _resolve_s3_path({"bucket": "my-bucket", "prefix": "data"}, {"path": "year=2024/**/*.parquet"})
    assert path == "s3://my-bucket/data/year=2024/**/*.parquet"


# ── Source options stored in catalog source_options ───────────────────────────

def test_read_uses_source_options_path():
    cat = _catalog({"bucket": "b", "source_options": {"path": "events/*.parquet"}})
    mat = S3Source().read(cat, _joint(), None)
    ref = mat.materialized_ref
    assert isinstance(ref, S3DeferredMaterializedRef)
    assert ref._s3_path == "s3://b/events/*.parquet"


def test_read_uses_source_options_format():
    cat = _catalog({"bucket": "b", "source_options": {"format": "csv"}})
    mat = S3Source().read(cat, _joint(), None)
    ref = mat.materialized_ref
    assert isinstance(ref, S3DeferredMaterializedRef)
    assert ref._fmt == "csv"


def test_read_uses_source_options_partition_columns():
    cat = _catalog({"bucket": "b", "source_options": {"partition_columns": ["year", "month"]}})
    mat = S3Source().read(cat, _joint(), None)
    ref = mat.materialized_ref
    assert isinstance(ref, S3DeferredMaterializedRef)
    assert ref._partition_columns == ["year", "month"]


def test_read_uses_source_options_schema():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
    cat = _catalog({"bucket": "b", "source_options": {"schema": schema}})
    mat = S3Source().read(cat, _joint(), None)
    ref = mat.materialized_ref
    assert isinstance(ref, S3DeferredMaterializedRef)
    assert ref._schema == schema


def test_read_joint_table_used_as_path():
    """joint.table acts as path when no source_options.path is set."""
    cat = _catalog({"bucket": "b"})
    mat = S3Source().read(cat, _joint(table="users/data.parquet"), None)
    ref = mat.materialized_ref
    assert isinstance(ref, S3DeferredMaterializedRef)
    assert "users/data.parquet" in ref._s3_path


def test_read_source_options_path_takes_precedence_over_joint_table():
    """source_options.path takes precedence over joint.table."""
    cat = _catalog({"bucket": "b", "source_options": {"path": "explicit/*.parquet"}})
    mat = S3Source().read(cat, _joint(table="ignored.parquet"), None)
    ref = mat.materialized_ref
    assert isinstance(ref, S3DeferredMaterializedRef)
    assert "explicit" in ref._s3_path


# ── S3DeferredMaterializedRef properties ─────────────────────────────────────

def test_deferred_ref_storage_type():
    ref = S3DeferredMaterializedRef(
        s3_path="s3://b/data/*.parquet",
        fmt="parquet",
        catalog_options={"bucket": "b"},
        partition_columns=None,
        schema=None,
    )
    assert ref.storage_type == "s3"


def test_deferred_ref_size_bytes_is_none():
    ref = S3DeferredMaterializedRef(
        s3_path="s3://b/data/*.parquet",
        fmt="parquet",
        catalog_options={"bucket": "b"},
        partition_columns=None,
        schema=None,
    )
    assert ref.size_bytes is None


def test_deferred_ref_schema_uses_explicit_schema():
    schema = pa.schema([pa.field("id", pa.int64())])
    ref = S3DeferredMaterializedRef(
        s3_path="s3://b/data/*.parquet",
        fmt="parquet",
        catalog_options={"bucket": "b"},
        partition_columns=None,
        schema=schema,
    )
    result = ref.schema
    assert result.columns[0].name == "id"
    assert result.columns[0].type == "int64"


def test_deferred_ref_to_arrow_parquet(tmp_path):
    """to_arrow() reads a parquet file via pyarrow dataset with a mock filesystem."""
    table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})

    mock_dataset = MagicMock()
    mock_dataset.to_table.return_value = table

    with patch("rivet_aws.s3_source.S3DeferredMaterializedRef._build_s3fs") as mock_fs, \
         patch("pyarrow.dataset.dataset", return_value=mock_dataset):
        mock_fs.return_value = MagicMock()
        ref = S3DeferredMaterializedRef(
            s3_path="s3://b/data/*.parquet",
            fmt="parquet",
            catalog_options={"bucket": "b"},
            partition_columns=None,
            schema=None,
        )
        result = ref.to_arrow()

    assert result.num_rows == 3
    assert result.column_names == ["id", "name"]


def test_deferred_ref_to_arrow_csv(tmp_path):
    """to_arrow() uses CsvFileFormat for csv format."""
    import pyarrow.dataset as pad

    table = pa.table({"x": [1, 2]})
    mock_dataset = MagicMock()
    mock_dataset.to_table.return_value = table

    with patch("rivet_aws.s3_source.S3DeferredMaterializedRef._build_s3fs") as mock_fs, \
         patch("pyarrow.dataset.dataset", return_value=mock_dataset) as mock_ds:
        mock_fs.return_value = MagicMock()
        ref = S3DeferredMaterializedRef(
            s3_path="s3://b/data/*.csv",
            fmt="csv",
            catalog_options={"bucket": "b"},
            partition_columns=None,
            schema=None,
        )
        result = ref.to_arrow()

    assert result.num_rows == 2
    # Verify CsvFileFormat was used
    call_kwargs = mock_ds.call_args
    assert isinstance(call_kwargs.kwargs.get("format") or call_kwargs.args[2] if len(call_kwargs.args) > 2 else call_kwargs.kwargs.get("format"), pad.CsvFileFormat)


def test_deferred_ref_to_arrow_with_partition_columns():
    """to_arrow() passes partitioning to dataset when partition_columns set."""
    table = pa.table({"id": [1], "year": ["2024"]})
    mock_dataset = MagicMock()
    mock_dataset.to_table.return_value = table

    with patch("rivet_aws.s3_source.S3DeferredMaterializedRef._build_s3fs") as mock_fs, \
         patch("pyarrow.dataset.dataset", return_value=mock_dataset), \
         patch("pyarrow.dataset.partitioning") as mock_partitioning:
        mock_fs.return_value = MagicMock()
        mock_partitioning.return_value = MagicMock()
        ref = S3DeferredMaterializedRef(
            s3_path="s3://b/data/",
            fmt="parquet",
            catalog_options={"bucket": "b"},
            partition_columns=["year"],
            schema=None,
        )
        ref.to_arrow()

    mock_partitioning.assert_called_once()


def test_deferred_ref_delta_raises_not_implemented():
    """Delta format raises NotImplementedError since it requires engine adapter."""
    ref = S3DeferredMaterializedRef(
        s3_path="s3://b/delta_table/",
        fmt="delta",
        catalog_options={"bucket": "b"},
        partition_columns=None,
        schema=None,
    )
    with pytest.raises(NotImplementedError, match="delta"):
        with patch.object(ref, "_build_s3fs", return_value=MagicMock()):
            ref.to_arrow()


def test_deferred_ref_row_count():
    """row_count calls to_arrow() and returns num_rows."""
    table = pa.table({"id": [1, 2, 3, 4, 5]})
    mock_dataset = MagicMock()
    mock_dataset.to_table.return_value = table

    with patch("rivet_aws.s3_source.S3DeferredMaterializedRef._build_s3fs") as mock_fs, \
         patch("pyarrow.dataset.dataset", return_value=mock_dataset):
        mock_fs.return_value = MagicMock()
        ref = S3DeferredMaterializedRef(
            s3_path="s3://b/data/*.parquet",
            fmt="parquet",
            catalog_options={"bucket": "b"},
            partition_columns=None,
            schema=None,
        )
        assert ref.row_count == 5


# ── Format override: source format overrides catalog format ───────────────────

def test_format_override_in_source_options():
    """Source format option overrides catalog format."""
    cat = _catalog({"bucket": "b", "format": "parquet", "source_options": {"format": "csv"}})
    mat = S3Source().read(cat, _joint(), None)
    ref = mat.materialized_ref
    assert isinstance(ref, S3DeferredMaterializedRef)
    assert ref._fmt == "csv"


def test_catalog_format_used_when_no_source_format():
    """Catalog format is used when source options don't specify format."""
    cat = _catalog({"bucket": "b", "format": "orc"})
    mat = S3Source().read(cat, _joint(), None)
    ref = mat.materialized_ref
    assert isinstance(ref, S3DeferredMaterializedRef)
    assert ref._fmt == "orc"


def test_default_format_is_parquet():
    """Default format is parquet when neither catalog nor source specifies it."""
    cat = _catalog({"bucket": "b"})
    mat = S3Source().read(cat, _joint(), None)
    ref = mat.materialized_ref
    assert isinstance(ref, S3DeferredMaterializedRef)
    assert ref._fmt == "parquet"
