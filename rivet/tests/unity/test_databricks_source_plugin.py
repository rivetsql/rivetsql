"""Tests for DatabricksSource plugin (task 26.1): time travel and Change Data Feed."""

from __future__ import annotations

import pytest

from rivet_core.errors import ExecutionError, PluginValidationError
from rivet_core.models import Catalog, Joint, Material
from rivet_core.plugins import SourcePlugin
from rivet_databricks.databricks_source import (
    DatabricksDeferredMaterializedRef,
    DatabricksSource,
    _validate_source_options,
    build_source_sql,
)


def _make_catalog() -> Catalog:
    return Catalog(
        name="db_cat",
        type="databricks",
        options={"workspace_url": "https://my.databricks.com", "catalog": "main"},
    )


def _make_joint(name: str = "j", table: str | None = "orders") -> Joint:
    return Joint(name=name, joint_type="source", catalog="db_cat", table=table)


# ── catalog_type ──────────────────────────────────────────────────────────────

def test_catalog_type():
    assert DatabricksSource.catalog_type == "databricks"


def test_is_source_plugin():
    assert isinstance(DatabricksSource(), SourcePlugin)


# ── _validate_source_options ──────────────────────────────────────────────────

def test_validate_accepts_table_only():
    _validate_source_options({"table": "orders"})


def test_validate_accepts_integer_version():
    _validate_source_options({"table": "orders", "version": 5})


def test_validate_accepts_timestamp_version():
    _validate_source_options({"table": "orders", "version": "2024-01-15T00:00:00Z"})


def test_validate_accepts_change_data_feed():
    _validate_source_options({"table": "orders", "change_data_feed": True})


def test_validate_rejects_missing_table():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_source_options({})
    assert exc_info.value.error.code == "RVT-201"
    assert "table" in exc_info.value.error.message


def test_validate_rejects_unknown_option():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_source_options({"table": "t", "unknown": "x"})
    assert exc_info.value.error.code == "RVT-201"
    assert "unknown" in exc_info.value.error.message


def test_validate_rejects_invalid_version():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_source_options({"table": "t", "version": "not-a-date"})
    assert exc_info.value.error.code == "RVT-201"
    assert "version" in exc_info.value.error.message


def test_validate_rejects_non_bool_cdf():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_source_options({"table": "t", "change_data_feed": "yes"})
    assert exc_info.value.error.code == "RVT-201"
    assert "change_data_feed" in exc_info.value.error.message


# ── build_source_sql ──────────────────────────────────────────────────────────

def test_sql_plain_select():
    assert build_source_sql("orders") == "SELECT * FROM orders"


def test_sql_version_as_of_integer():
    sql = build_source_sql("orders", version=5)
    assert sql == "SELECT * FROM orders VERSION AS OF 5"


def test_sql_timestamp_as_of():
    sql = build_source_sql("orders", version="2024-01-15T00:00:00Z")
    assert sql == "SELECT * FROM orders TIMESTAMP AS OF '2024-01-15T00:00:00Z'"


def test_sql_table_changes_no_version():
    sql = build_source_sql("orders", change_data_feed=True)
    assert sql == "SELECT * FROM table_changes('orders')"


def test_sql_table_changes_with_integer_version():
    sql = build_source_sql("orders", version=3, change_data_feed=True)
    assert sql == "SELECT * FROM table_changes('orders', 3)"


def test_sql_table_changes_with_timestamp_version():
    sql = build_source_sql("orders", version="2024-01-15", change_data_feed=True)
    assert sql == "SELECT * FROM table_changes('orders', '2024-01-15')"


def test_sql_version_zero():
    sql = build_source_sql("orders", version=0)
    assert sql == "SELECT * FROM orders VERSION AS OF 0"


# ── DatabricksSource.read ─────────────────────────────────────────────────────

def test_read_returns_deferred_material():
    material = DatabricksSource().read(_make_catalog(), _make_joint(), pushdown=None)
    assert isinstance(material, Material)
    assert material.state == "deferred"


def test_read_material_name_matches_joint():
    material = DatabricksSource().read(_make_catalog(), _make_joint(name="my_j"), pushdown=None)
    assert material.name == "my_j"


def test_read_material_catalog_matches():
    material = DatabricksSource().read(_make_catalog(), _make_joint(), pushdown=None)
    assert material.catalog == "db_cat"


def test_read_ref_is_databricks_deferred():
    material = DatabricksSource().read(_make_catalog(), _make_joint(), pushdown=None)
    assert isinstance(material.materialized_ref, DatabricksDeferredMaterializedRef)


def test_read_ref_sql_plain():
    material = DatabricksSource().read(_make_catalog(), _make_joint(table="orders"), pushdown=None)
    ref = material.materialized_ref
    assert isinstance(ref, DatabricksDeferredMaterializedRef)
    assert ref.sql == "SELECT * FROM orders"


def test_read_uses_joint_name_as_table_fallback():
    joint = Joint(name="fallback_tbl", joint_type="source", catalog="db_cat", table=None)
    material = DatabricksSource().read(_make_catalog(), joint, pushdown=None)
    ref = material.materialized_ref
    assert isinstance(ref, DatabricksDeferredMaterializedRef)
    assert ref._table == "fallback_tbl"


def test_read_raises_on_missing_table_and_name():
    joint = Joint(name="", joint_type="source", catalog="db_cat", table=None)
    with pytest.raises(PluginValidationError) as exc_info:
        DatabricksSource().read(_make_catalog(), joint, pushdown=None)
    assert exc_info.value.error.code == "RVT-201"


# ── DatabricksDeferredMaterializedRef ─────────────────────────────────────────

def test_ref_storage_type():
    ref = DatabricksDeferredMaterializedRef("t", "SELECT * FROM t", None, False)
    assert ref.storage_type == "databricks"


def test_ref_size_bytes_is_none():
    ref = DatabricksDeferredMaterializedRef("t", "SELECT * FROM t", None, False)
    assert ref.size_bytes is None


def test_ref_to_arrow_raises():
    ref = DatabricksDeferredMaterializedRef("t", "SELECT * FROM t", None, False)
    with pytest.raises(ExecutionError) as exc_info:
        ref.to_arrow()
    assert exc_info.value.error.code == "RVT-501"


# ── check_time_travel_format (RVT-504) ───────────────────────────────────────

def test_check_time_travel_passes_for_delta():
    ref = DatabricksDeferredMaterializedRef("t", "", version=5, change_data_feed=False)
    ref.check_time_travel_format("DELTA")  # no raise


def test_check_time_travel_passes_for_delta_lowercase():
    ref = DatabricksDeferredMaterializedRef("t", "", version=5, change_data_feed=False)
    ref.check_time_travel_format("delta")  # no raise


def test_check_time_travel_raises_rvt504_for_parquet():
    ref = DatabricksDeferredMaterializedRef("t", "", version=5, change_data_feed=False)
    with pytest.raises(ExecutionError) as exc_info:
        ref.check_time_travel_format("PARQUET")
    assert exc_info.value.error.code == "RVT-504"
    assert "PARQUET" in exc_info.value.error.message


def test_check_time_travel_raises_rvt504_for_csv():
    ref = DatabricksDeferredMaterializedRef("t", "", version="2024-01-15", change_data_feed=False)
    with pytest.raises(ExecutionError) as exc_info:
        ref.check_time_travel_format("CSV")
    assert exc_info.value.error.code == "RVT-504"


def test_check_time_travel_no_raise_when_no_version():
    ref = DatabricksDeferredMaterializedRef("t", "", version=None, change_data_feed=False)
    ref.check_time_travel_format("PARQUET")  # no raise


def test_check_time_travel_error_includes_table_name():
    ref = DatabricksDeferredMaterializedRef("my_table", "", version=1, change_data_feed=False)
    with pytest.raises(ExecutionError) as exc_info:
        ref.check_time_travel_format("ORC")
    assert "my_table" in exc_info.value.error.message


# ── check_cdf_enabled (RVT-505) ──────────────────────────────────────────────

def test_check_cdf_passes_when_enabled():
    ref = DatabricksDeferredMaterializedRef("t", "", version=None, change_data_feed=True)
    ref.check_cdf_enabled(True)  # no raise


def test_check_cdf_raises_rvt505_when_not_enabled():
    ref = DatabricksDeferredMaterializedRef("t", "", version=None, change_data_feed=True)
    with pytest.raises(ExecutionError) as exc_info:
        ref.check_cdf_enabled(False)
    assert exc_info.value.error.code == "RVT-505"
    assert "Change Data Feed" in exc_info.value.error.message


def test_check_cdf_no_raise_when_not_requested():
    ref = DatabricksDeferredMaterializedRef("t", "", version=None, change_data_feed=False)
    ref.check_cdf_enabled(False)  # no raise


def test_check_cdf_error_includes_table_name():
    ref = DatabricksDeferredMaterializedRef("my_table", "", version=None, change_data_feed=True)
    with pytest.raises(ExecutionError) as exc_info:
        ref.check_cdf_enabled(False)
    assert "my_table" in exc_info.value.error.message
