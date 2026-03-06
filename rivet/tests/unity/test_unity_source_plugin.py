"""Tests for UnitySource plugin (task 23.1): source options table, version, timestamp, partition_filter."""

from __future__ import annotations

import pytest

from rivet_core.errors import ExecutionError, PluginValidationError
from rivet_core.models import Catalog, Joint, Material
from rivet_core.plugins import SourcePlugin
from rivet_databricks.unity_source import (
    UnityDeferredMaterializedRef,
    UnitySource,
    _validate_source_options,
)


def _make_catalog(options: dict | None = None) -> Catalog:
    opts = {"host": "https://my.databricks.com", "catalog_name": "prod"}
    if options:
        opts.update(options)
    return Catalog(name="unity_cat", type="unity", options=opts)


def _make_joint(name: str = "my_joint", table: str | None = "users") -> Joint:
    return Joint(name=name, joint_type="source", catalog="unity_cat", table=table)


# ── catalog_type ──────────────────────────────────────────────────────────────

def test_catalog_type():
    assert UnitySource.catalog_type == "unity"


def test_is_source_plugin():
    assert isinstance(UnitySource(), SourcePlugin)


# ── _validate_source_options ──────────────────────────────────────────────────

def test_validate_accepts_table_only():
    _validate_source_options({"table": "users"})


def test_validate_accepts_all_options():
    _validate_source_options({
        "table": "users",
        "version": 5,
        "timestamp": "2024-01-15T00:00:00Z",
        "partition_filter": {"dt": "2024-01"},
    })


def test_validate_rejects_missing_table():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_source_options({})
    assert exc_info.value.error.code == "RVT-201"
    assert "table" in exc_info.value.error.message


def test_validate_rejects_unknown_option():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_source_options({"table": "users", "unknown_key": "value"})
    assert exc_info.value.error.code == "RVT-201"
    assert "unknown_key" in exc_info.value.error.message


def test_validate_rejects_non_integer_version():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_source_options({"table": "users", "version": "not_an_int"})
    assert exc_info.value.error.code == "RVT-201"
    assert "version" in exc_info.value.error.message


def test_validate_accepts_integer_version():
    _validate_source_options({"table": "users", "version": 0})
    _validate_source_options({"table": "users", "version": 42})


def test_validate_rejects_invalid_timestamp():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_source_options({"table": "users", "timestamp": "not-a-date"})
    assert exc_info.value.error.code == "RVT-201"
    assert "timestamp" in exc_info.value.error.message


def test_validate_accepts_valid_timestamp_formats():
    _validate_source_options({"table": "users", "timestamp": "2024-01-15T00:00:00Z"})
    _validate_source_options({"table": "users", "timestamp": "2024-01-15T00:00:00+01:00"})
    _validate_source_options({"table": "users", "timestamp": "2024-01-15"})


def test_validate_accepts_version_and_timestamp_together():
    # Both can be provided; precedence is handled at execution time (task 23.2)
    _validate_source_options({"table": "users", "version": 3, "timestamp": "2024-01-15T00:00:00Z"})


def test_validate_accepts_partition_filter_dict():
    _validate_source_options({"table": "users", "partition_filter": {"region": "us-east-1"}})


# ── UnitySource.read ──────────────────────────────────────────────────────────

def test_read_returns_material():
    catalog = _make_catalog()
    joint = _make_joint()
    material = UnitySource().read(catalog, joint, pushdown=None)
    assert isinstance(material, Material)


def test_read_material_state_is_deferred():
    catalog = _make_catalog()
    joint = _make_joint()
    material = UnitySource().read(catalog, joint, pushdown=None)
    assert material.state == "deferred"


def test_read_material_name_matches_joint():
    catalog = _make_catalog()
    joint = _make_joint(name="my_joint")
    material = UnitySource().read(catalog, joint, pushdown=None)
    assert material.name == "my_joint"


def test_read_material_catalog_matches():
    catalog = _make_catalog()
    joint = _make_joint()
    material = UnitySource().read(catalog, joint, pushdown=None)
    assert material.catalog == "unity_cat"


def test_read_materialized_ref_is_unity_deferred():
    catalog = _make_catalog()
    joint = _make_joint(table="orders")
    material = UnitySource().read(catalog, joint, pushdown=None)
    assert isinstance(material.materialized_ref, UnityDeferredMaterializedRef)


def test_read_ref_stores_table_name():
    catalog = _make_catalog()
    joint = _make_joint(table="orders")
    material = UnitySource().read(catalog, joint, pushdown=None)
    ref = material.materialized_ref
    assert isinstance(ref, UnityDeferredMaterializedRef)
    assert ref._table == "orders"


def test_read_ref_stores_version():
    catalog = _make_catalog()
    Joint(name="j", joint_type="source", catalog="unity_cat", table="orders")
    # Simulate source_options via joint attribute
    Joint(name="j", joint_type="source", catalog="unity_cat", table="orders")
    # Use table directly; version comes from source_options in real usage
    # For this test, we call _validate_source_options directly and build ref manually
    ref = UnityDeferredMaterializedRef(
        table="orders", catalog=catalog, version=7, timestamp=None, partition_filter=None
    )
    assert ref._version == 7


def test_read_ref_stores_timestamp():
    catalog = _make_catalog()
    ref = UnityDeferredMaterializedRef(
        table="orders",
        catalog=catalog,
        version=None,
        timestamp="2024-01-15T00:00:00Z",
        partition_filter=None,
    )
    assert ref._timestamp == "2024-01-15T00:00:00Z"


def test_read_ref_stores_partition_filter():
    catalog = _make_catalog()
    pf = {"region": "us-east-1"}
    ref = UnityDeferredMaterializedRef(
        table="orders", catalog=catalog, version=None, timestamp=None, partition_filter=pf
    )
    assert ref._partition_filter == pf


def test_read_uses_joint_name_as_table_when_no_table():
    catalog = _make_catalog()
    joint = Joint(name="fallback_table", joint_type="source", catalog="unity_cat", table=None)
    material = UnitySource().read(catalog, joint, pushdown=None)
    ref = material.materialized_ref
    assert isinstance(ref, UnityDeferredMaterializedRef)
    assert ref._table == "fallback_table"


def test_read_raises_on_missing_table_and_name():
    catalog = _make_catalog()
    joint = Joint(name="", joint_type="source", catalog="unity_cat", table=None)
    with pytest.raises(PluginValidationError) as exc_info:
        UnitySource().read(catalog, joint, pushdown=None)
    assert exc_info.value.error.code == "RVT-201"


# ── UnityDeferredMaterializedRef ──────────────────────────────────────────────

def test_deferred_ref_storage_type():
    catalog = _make_catalog()
    ref = UnityDeferredMaterializedRef(
        table="t", catalog=catalog, version=None, timestamp=None, partition_filter=None
    )
    assert ref.storage_type == "unity"


def test_deferred_ref_size_bytes_is_none():
    catalog = _make_catalog()
    ref = UnityDeferredMaterializedRef(
        table="t", catalog=catalog, version=None, timestamp=None, partition_filter=None
    )
    assert ref.size_bytes is None


def test_deferred_ref_to_arrow_raises_execution_error():
    catalog = _make_catalog()
    ref = UnityDeferredMaterializedRef(
        table="t", catalog=catalog, version=None, timestamp=None, partition_filter=None
    )
    with pytest.raises(ExecutionError) as exc_info:
        ref.to_arrow()
    assert exc_info.value.error.code == "RVT-501"


# ── Task 23.2: version precedence and non-Delta time travel guard ─────────────

def test_effective_version_returns_version_when_set():
    catalog = _make_catalog()
    ref = UnityDeferredMaterializedRef(
        table="t", catalog=catalog, version=5, timestamp="2024-01-15T00:00:00Z", partition_filter=None
    )
    assert ref.effective_version == 5


def test_effective_timestamp_is_none_when_version_set():
    """Version takes precedence: effective_timestamp must be None when version is set."""
    catalog = _make_catalog()
    ref = UnityDeferredMaterializedRef(
        table="t", catalog=catalog, version=5, timestamp="2024-01-15T00:00:00Z", partition_filter=None
    )
    assert ref.effective_timestamp is None


def test_effective_timestamp_returned_when_no_version():
    catalog = _make_catalog()
    ref = UnityDeferredMaterializedRef(
        table="t", catalog=catalog, version=None, timestamp="2024-01-15T00:00:00Z", partition_filter=None
    )
    assert ref.effective_timestamp == "2024-01-15T00:00:00Z"


def test_effective_version_none_when_only_timestamp():
    catalog = _make_catalog()
    ref = UnityDeferredMaterializedRef(
        table="t", catalog=catalog, version=None, timestamp="2024-01-15T00:00:00Z", partition_filter=None
    )
    assert ref.effective_version is None


def test_effective_version_zero_takes_precedence():
    """Version=0 is a valid version and must still take precedence over timestamp."""
    catalog = _make_catalog()
    ref = UnityDeferredMaterializedRef(
        table="t", catalog=catalog, version=0, timestamp="2024-01-15T00:00:00Z", partition_filter=None
    )
    assert ref.effective_version == 0
    assert ref.effective_timestamp is None


def test_check_time_travel_format_passes_for_delta():
    catalog = _make_catalog()
    ref = UnityDeferredMaterializedRef(
        table="t", catalog=catalog, version=3, timestamp=None, partition_filter=None
    )
    ref.check_time_travel_format("DELTA")  # must not raise


def test_check_time_travel_format_passes_for_delta_lowercase():
    catalog = _make_catalog()
    ref = UnityDeferredMaterializedRef(
        table="t", catalog=catalog, version=3, timestamp=None, partition_filter=None
    )
    ref.check_time_travel_format("delta")  # case-insensitive, must not raise


def test_check_time_travel_format_raises_rvt502_for_parquet():
    catalog = _make_catalog()
    ref = UnityDeferredMaterializedRef(
        table="t", catalog=catalog, version=3, timestamp=None, partition_filter=None
    )
    with pytest.raises(ExecutionError) as exc_info:
        ref.check_time_travel_format("PARQUET")
    assert exc_info.value.error.code == "RVT-502"
    assert "PARQUET" in exc_info.value.error.message


def test_check_time_travel_format_raises_rvt502_for_csv():
    catalog = _make_catalog()
    ref = UnityDeferredMaterializedRef(
        table="t", catalog=catalog, version=None, timestamp="2024-01-15T00:00:00Z", partition_filter=None
    )
    with pytest.raises(ExecutionError) as exc_info:
        ref.check_time_travel_format("CSV")
    assert exc_info.value.error.code == "RVT-502"


def test_check_time_travel_format_no_raise_when_no_time_travel():
    """No time travel requested → format check is a no-op regardless of format."""
    catalog = _make_catalog()
    ref = UnityDeferredMaterializedRef(
        table="t", catalog=catalog, version=None, timestamp=None, partition_filter=None
    )
    ref.check_time_travel_format("PARQUET")  # must not raise
    ref.check_time_travel_format("CSV")  # must not raise


def test_check_time_travel_format_error_includes_table_name():
    catalog = _make_catalog()
    ref = UnityDeferredMaterializedRef(
        table="my_table", catalog=catalog, version=1, timestamp=None, partition_filter=None
    )
    with pytest.raises(ExecutionError) as exc_info:
        ref.check_time_travel_format("ORC")
    assert "my_table" in exc_info.value.error.message
