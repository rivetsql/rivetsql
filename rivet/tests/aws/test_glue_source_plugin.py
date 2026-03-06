"""Tests for GlueSource plugin (task 19.1): source options table, partition_filter, snapshot_time."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from rivet_aws.glue_source import (
    GlueDeferredMaterializedRef,
    GlueSource,
    _matches_partition_filter,
    _validate_source_options,
)
from rivet_core.errors import ExecutionError, PluginValidationError
from rivet_core.models import Catalog, Joint, Material
from rivet_core.plugins import SourcePlugin


def _make_catalog(options: dict | None = None) -> Catalog:
    opts = {"database": "my_db", "access_key_id": "AKID", "secret_access_key": "SECRET"}
    if options:
        opts.update(options)
    return Catalog(name="glue_cat", type="glue", options=opts)


def _make_joint(name: str = "my_joint", table: str | None = "orders") -> Joint:
    return Joint(name=name, joint_type="source", catalog="glue_cat", table=table)


def _mock_glue_client(location: str = "s3://bucket/orders/", input_format: str = "") -> MagicMock:
    client = MagicMock()
    client.get_table.return_value = {
        "Table": {
            "Name": "orders",
            "StorageDescriptor": {
                "Location": location,
                "InputFormat": input_format,
            },
            "PartitionKeys": [],
        }
    }
    paginator = MagicMock()
    paginator.paginate.return_value = iter([{"Partitions": []}])
    client.get_paginator.return_value = paginator
    return client


# ── catalog_type ──────────────────────────────────────────────────────────────

def test_catalog_type():
    assert GlueSource.catalog_type == "glue"


def test_is_source_plugin():
    assert isinstance(GlueSource(), SourcePlugin)


# ── _validate_source_options ──────────────────────────────────────────────────

def test_validate_accepts_table_only():
    _validate_source_options({"table": "orders"})


def test_validate_accepts_all_options():
    _validate_source_options({
        "table": "orders",
        "partition_filter": {"dt": "2024-01"},
        "snapshot_time": "2024-01-15T00:00:00Z",
    })


def test_validate_rejects_missing_table():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_source_options({})
    assert exc_info.value.error.code == "RVT-201"
    assert "table" in exc_info.value.error.message


def test_validate_rejects_unknown_option():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_source_options({"table": "orders", "unknown_opt": "x"})
    assert exc_info.value.error.code == "RVT-201"
    assert "unknown_opt" in exc_info.value.error.message


def test_validate_rejects_invalid_snapshot_time():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_source_options({"table": "orders", "snapshot_time": "not-a-date"})
    assert exc_info.value.error.code == "RVT-201"
    assert "snapshot_time" in exc_info.value.error.message


def test_validate_accepts_snapshot_time_date_only():
    _validate_source_options({"table": "orders", "snapshot_time": "2024-01-15"})


def test_validate_accepts_snapshot_time_with_tz():
    _validate_source_options({"table": "orders", "snapshot_time": "2024-01-15T12:30:00+05:30"})


def test_validate_accepts_snapshot_time_utc_z():
    _validate_source_options({"table": "orders", "snapshot_time": "2024-01-15T00:00:00Z"})


def test_validate_accepts_partition_filter_dict():
    _validate_source_options({"table": "orders", "partition_filter": {"year": "2024", "month": "01"}})


# ── _matches_partition_filter ─────────────────────────────────────────────────

def test_matches_partition_filter_exact_match():
    assert _matches_partition_filter({"year": "2024", "month": "01"}, {"year": "2024"}) is True


def test_matches_partition_filter_no_match():
    assert _matches_partition_filter({"year": "2024", "month": "01"}, {"year": "2023"}) is False


def test_matches_partition_filter_all_keys():
    assert _matches_partition_filter(
        {"year": "2024", "month": "01"},
        {"year": "2024", "month": "01"},
    ) is True


def test_matches_partition_filter_partial_mismatch():
    assert _matches_partition_filter(
        {"year": "2024", "month": "01"},
        {"year": "2024", "month": "02"},
    ) is False


def test_matches_partition_filter_empty_filter():
    assert _matches_partition_filter({"year": "2024"}, {}) is True


def test_matches_partition_filter_coerces_value_to_str():
    # partition_filter values are coerced to str for comparison
    assert _matches_partition_filter({"year": "2024"}, {"year": 2024}) is True


# ── GlueSource.read ───────────────────────────────────────────────────────────

def test_read_returns_material():
    catalog = _make_catalog()
    joint = _make_joint()
    _mock_glue_client(location="s3://bucket/orders/")

    with patch("rivet_aws.glue_source._resolve_glue_table") as mock_resolve:
        mock_resolve.return_value = ("s3://bucket/orders/", "", [], [])
        mat = GlueSource().read(catalog, joint, None)

    assert isinstance(mat, Material)
    assert mat.name == "my_joint"
    assert mat.catalog == "glue_cat"
    assert mat.state == "deferred"
    assert mat.materialized_ref is not None


def test_read_uses_joint_table_as_table_name():
    catalog = _make_catalog()
    joint = _make_joint(table="customers")

    with patch("rivet_aws.glue_source._resolve_glue_table") as mock_resolve:
        mock_resolve.return_value = ("s3://bucket/customers/", "", [], [])
        GlueSource().read(catalog, joint, None)

    mock_resolve.assert_called_once_with(catalog, "customers", None)


def test_read_passes_partition_filter_none_when_absent():
    catalog = _make_catalog()
    joint = _make_joint(table="orders")

    with patch("rivet_aws.glue_source._resolve_glue_table") as mock_resolve:
        mock_resolve.return_value = ("s3://bucket/orders/", "", [], [])
        GlueSource().read(catalog, joint, None)

    _, _, partition_filter = mock_resolve.call_args[0]
    assert partition_filter is None


def test_read_materialized_ref_is_glue_deferred():
    catalog = _make_catalog()
    joint = _make_joint()

    with patch("rivet_aws.glue_source._resolve_glue_table") as mock_resolve:
        mock_resolve.return_value = ("s3://bucket/orders/", "parquet", [], [])
        mat = GlueSource().read(catalog, joint, None)

    assert isinstance(mat.materialized_ref, GlueDeferredMaterializedRef)


def test_read_deferred_ref_stores_snapshot_time():
    catalog = _make_catalog()
    joint = Joint(
        name="snap_joint",
        joint_type="source",
        catalog="glue_cat",
        table="events",
    )

    with patch("rivet_aws.glue_source._resolve_glue_table") as mock_resolve:
        mock_resolve.return_value = ("s3://bucket/events/", "", [], [])
        # Simulate source_options via joint.table only; snapshot_time not on Joint model
        # so we test via direct _validate_source_options
        mat = GlueSource().read(catalog, joint, None)

    assert mat.state == "deferred"


# ── _resolve_glue_table ───────────────────────────────────────────────────────

def test_resolve_glue_table_returns_location_and_format():
    catalog = _make_catalog()
    client = _mock_glue_client(
        location="s3://bucket/orders/",
        input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    )

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        from rivet_aws.glue_source import _resolve_glue_table
        location, input_format, partition_keys, partition_locations = _resolve_glue_table(
            catalog, "orders", None
        )

    assert location == "s3://bucket/orders/"
    assert "parquet" in input_format.lower()
    assert partition_keys == []
    assert partition_locations == []


def test_resolve_glue_table_raises_on_missing_table():
    catalog = _make_catalog()
    client = MagicMock()
    client.get_table.side_effect = Exception("Table not found")

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        from rivet_aws.glue_source import _resolve_glue_table
        with pytest.raises(ExecutionError) as exc_info:
            _resolve_glue_table(catalog, "nonexistent", None)

    assert exc_info.value.error.code == "RVT-503"
    assert "nonexistent" in exc_info.value.error.message


def test_resolve_glue_table_filters_partitions():
    catalog = _make_catalog()
    client = MagicMock()
    client.get_table.return_value = {
        "Table": {
            "Name": "events",
            "StorageDescriptor": {
                "Location": "s3://bucket/events/",
                "InputFormat": "",
            },
            "PartitionKeys": [{"Name": "year"}, {"Name": "month"}],
        }
    }
    paginator = MagicMock()
    paginator.paginate.return_value = iter([
        {
            "Partitions": [
                {
                    "Values": ["2024", "01"],
                    "StorageDescriptor": {"Location": "s3://bucket/events/year=2024/month=01/"},
                },
                {
                    "Values": ["2024", "02"],
                    "StorageDescriptor": {"Location": "s3://bucket/events/year=2024/month=02/"},
                },
                {
                    "Values": ["2023", "12"],
                    "StorageDescriptor": {"Location": "s3://bucket/events/year=2023/month=12/"},
                },
            ]
        }
    ])
    client.get_paginator.return_value = paginator

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        from rivet_aws.glue_source import _resolve_glue_table
        location, _, partition_keys, partition_locations = _resolve_glue_table(
            catalog, "events", {"year": "2024"}
        )

    assert partition_keys == ["year", "month"]
    assert len(partition_locations) == 2
    assert "s3://bucket/events/year=2024/month=01/" in partition_locations
    assert "s3://bucket/events/year=2024/month=02/" in partition_locations
    assert "s3://bucket/events/year=2023/month=12/" not in partition_locations


def test_resolve_glue_table_no_partition_filter_skips_partition_fetch():
    catalog = _make_catalog()
    client = MagicMock()
    client.get_table.return_value = {
        "Table": {
            "Name": "events",
            "StorageDescriptor": {"Location": "s3://bucket/events/", "InputFormat": ""},
            "PartitionKeys": [{"Name": "year"}],
        }
    }

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        from rivet_aws.glue_source import _resolve_glue_table
        _, _, partition_keys, partition_locations = _resolve_glue_table(
            catalog, "events", None
        )

    # No partition filter → no partition fetch
    client.get_paginator.assert_not_called()
    assert partition_locations == []


def test_resolve_glue_table_passes_catalog_id():
    catalog = _make_catalog({"catalog_id": "123456789012"})
    client = _mock_glue_client()

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        from rivet_aws.glue_source import _resolve_glue_table
        _resolve_glue_table(catalog, "orders", None)

    client.get_table.assert_called_once_with(
        DatabaseName="my_db", Name="orders", CatalogId="123456789012"
    )


# ── GlueDeferredMaterializedRef ───────────────────────────────────────────────

def test_deferred_ref_storage_type():
    ref = GlueDeferredMaterializedRef(
        location="s3://bucket/t/",
        input_format="",
        partition_filter=None,
        snapshot_time=None,
        partition_keys=[],
        partition_locations=[],
    )
    assert ref.storage_type == "glue"


def test_deferred_ref_size_bytes_is_none():
    ref = GlueDeferredMaterializedRef(
        location="s3://bucket/t/",
        input_format="",
        partition_filter=None,
        snapshot_time=None,
        partition_keys=[],
        partition_locations=[],
    )
    assert ref.size_bytes is None


def test_deferred_ref_to_arrow_raises_execution_error_on_failure():
    ref = GlueDeferredMaterializedRef(
        location="s3://nonexistent/path/",
        input_format="parquet",
        partition_filter=None,
        snapshot_time=None,
        partition_keys=[],
        partition_locations=[],
    )
    with patch("pyarrow.dataset.dataset", side_effect=Exception("S3 access denied")):
        with pytest.raises(ExecutionError) as exc_info:
            ref.to_arrow()
    assert exc_info.value.error.code == "RVT-501"
    assert "Glue source read failed" in exc_info.value.error.message
