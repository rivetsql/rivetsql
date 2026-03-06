"""Tests for UnitySink plugin (task 23.3): sink options table, write_strategy, merge_key, partition_by, format, create_table."""

from __future__ import annotations

from unittest.mock import patch

import pyarrow as pa
import pytest

from rivet_core.errors import ExecutionError, PluginValidationError
from rivet_core.models import Catalog, Joint, Material
from rivet_core.plugins import SinkPlugin
from rivet_core.strategies import _ArrowMaterializedRef
from rivet_databricks.unity_sink import (
    SUPPORTED_STRATEGIES,
    UnitySink,
    _arrow_type_to_unity,
    _build_table_def,
    _ensure_table_exists,
    _validate_sink_options,
)


def _make_catalog() -> Catalog:
    return Catalog(
        name="unity_cat",
        type="unity",
        options={"host": "https://my.uc.com", "catalog_name": "prod", "token": "tok123"},
    )


def _make_joint(name: str = "my_sink", table: str | None = "users") -> Joint:
    return Joint(name=name, joint_type="sink", catalog="unity_cat", table=table, upstream=["src"])


# ── catalog_type and plugin contract ──────────────────────────────────────────

def test_catalog_type():
    assert UnitySink.catalog_type == "unity"


def test_is_sink_plugin():
    assert isinstance(UnitySink(), SinkPlugin)


def test_supported_strategies_declared():
    expected = {"append", "replace", "merge", "truncate_insert", "delete_insert",
                "incremental_append", "scd2", "partition"}
    assert UnitySink.supported_strategies == expected


# ── _validate_sink_options: table ─────────────────────────────────────────────

def test_validate_accepts_table_only():
    _validate_sink_options({"table": "users"})


def test_validate_rejects_missing_table():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_sink_options({})
    assert exc_info.value.error.code == "RVT-201"
    assert "table" in exc_info.value.error.message


def test_validate_rejects_unknown_option():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_sink_options({"table": "users", "bogus": "val"})
    assert exc_info.value.error.code == "RVT-201"
    assert "bogus" in exc_info.value.error.message


# ── _validate_sink_options: write_strategy ────────────────────────────────────

def test_validate_accepts_all_strategies():
    for s in SUPPORTED_STRATEGIES:
        opts = {"table": "t", "write_strategy": s}
        if s in ("merge", "delete_insert", "scd2"):
            opts["merge_key"] = ["id"]
        _validate_sink_options(opts)


def test_validate_rejects_unknown_strategy():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_sink_options({"table": "t", "write_strategy": "upsert_magic"})
    assert exc_info.value.error.code == "RVT-201"
    assert "upsert_magic" in exc_info.value.error.message


def test_validate_default_strategy_is_replace():
    # No write_strategy → defaults to "replace", should not raise
    _validate_sink_options({"table": "t"})


# ── _validate_sink_options: merge_key ─────────────────────────────────────────

def test_validate_merge_requires_merge_key():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_sink_options({"table": "t", "write_strategy": "merge"})
    assert exc_info.value.error.code == "RVT-207"


def test_validate_delete_insert_requires_merge_key():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_sink_options({"table": "t", "write_strategy": "delete_insert"})
    assert exc_info.value.error.code == "RVT-207"


def test_validate_scd2_requires_merge_key():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_sink_options({"table": "t", "write_strategy": "scd2"})
    assert exc_info.value.error.code == "RVT-207"


def test_validate_merge_key_must_be_list():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_sink_options({"table": "t", "write_strategy": "merge", "merge_key": "id"})
    assert exc_info.value.error.code == "RVT-201"
    assert "merge_key" in exc_info.value.error.message


def test_validate_merge_key_accepted_as_list():
    _validate_sink_options({"table": "t", "write_strategy": "merge", "merge_key": ["id"]})


def test_validate_merge_key_ignored_for_append():
    # merge_key is optional for non-merge strategies; providing it is fine
    _validate_sink_options({"table": "t", "write_strategy": "append", "merge_key": ["id"]})


# ── _validate_sink_options: partition_by ──────────────────────────────────────

def test_validate_partition_by_accepted_as_list():
    _validate_sink_options({"table": "t", "partition_by": ["year", "month"]})


def test_validate_partition_by_must_be_list():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_sink_options({"table": "t", "partition_by": "year"})
    assert exc_info.value.error.code == "RVT-201"
    assert "partition_by" in exc_info.value.error.message


# ── _validate_sink_options: format ────────────────────────────────────────────

def test_validate_default_format_is_delta():
    # No format → defaults to "delta", should not raise
    _validate_sink_options({"table": "t"})


def test_validate_accepts_valid_formats():
    for fmt in ("delta", "parquet", "csv", "json"):
        _validate_sink_options({"table": "t", "format": fmt})


def test_validate_rejects_invalid_format():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_sink_options({"table": "t", "format": "avro"})
    assert exc_info.value.error.code == "RVT-201"
    assert "avro" in exc_info.value.error.message


# ── _validate_sink_options: create_table ──────────────────────────────────────

def test_validate_create_table_default_true():
    _validate_sink_options({"table": "t"})


def test_validate_create_table_accepts_bool():
    _validate_sink_options({"table": "t", "create_table": True})
    _validate_sink_options({"table": "t", "create_table": False})


def test_validate_create_table_rejects_non_bool():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_sink_options({"table": "t", "create_table": "yes"})
    assert exc_info.value.error.code == "RVT-201"
    assert "create_table" in exc_info.value.error.message


# ── _validate_sink_options: all options together ──────────────────────────────

def test_validate_all_options_together():
    _validate_sink_options({
        "table": "orders",
        "write_strategy": "merge",
        "merge_key": ["order_id"],
        "partition_by": ["year"],
        "format": "delta",
        "create_table": True,
    })


# ── UnitySink.write ──────────────────────────────────────────────────────────

def test_write_raises_execution_error_for_adapter_required():
    """UnitySink.write delegates to adapters; direct write raises ExecutionError."""
    import pyarrow as pa

    from rivet_core.strategies import _ArrowMaterializedRef

    catalog = _make_catalog()
    joint = _make_joint(table="orders")
    ref = _ArrowMaterializedRef(pa.table({"id": [1]}))
    mat = Material(name="orders", catalog="unity_cat", materialized_ref=ref, state="materialized")

    with patch("rivet_databricks.unity_sink._ensure_table_exists"):
        with pytest.raises(ExecutionError) as exc_info:
            UnitySink().write(catalog, joint, mat, strategy="replace")
        assert exc_info.value.error.code == "RVT-501"
        assert "adapter" in exc_info.value.error.message.lower()


def test_write_uses_joint_name_as_table_fallback():
    """When joint.table is None, joint.name is used as table name."""
    import pyarrow as pa

    from rivet_core.strategies import _ArrowMaterializedRef

    catalog = _make_catalog()
    joint = _make_joint(name="fallback_table", table=None)
    ref = _ArrowMaterializedRef(pa.table({"id": [1]}))
    mat = Material(name="m", catalog="unity_cat", materialized_ref=ref, state="materialized")

    with patch("rivet_databricks.unity_sink._ensure_table_exists"):
        with pytest.raises(ExecutionError) as exc_info:
            UnitySink().write(catalog, joint, mat, strategy="replace")
        assert "fallback_table" in exc_info.value.error.message


def test_write_validates_options_before_dispatch():
    """Invalid strategy should raise PluginValidationError, not ExecutionError."""
    import pyarrow as pa

    from rivet_core.strategies import _ArrowMaterializedRef

    catalog = _make_catalog()
    joint = _make_joint(table="t")
    ref = _ArrowMaterializedRef(pa.table({"id": [1]}))
    mat = Material(name="m", catalog="unity_cat", materialized_ref=ref, state="materialized")

    with pytest.raises(PluginValidationError) as exc_info:
        UnitySink().write(catalog, joint, mat, strategy="bad_strategy")
    assert exc_info.value.error.code == "RVT-201"


# ── Task 23.4: Table creation via POST /tables when create_table=true ─────────

# ── _arrow_type_to_unity ──────────────────────────────────────────────────────

class TestArrowTypeToUnity:
    def test_int_types(self):
        assert _arrow_type_to_unity(pa.int8()) == "TINYINT"
        assert _arrow_type_to_unity(pa.int16()) == "SMALLINT"
        assert _arrow_type_to_unity(pa.int32()) == "INT"
        assert _arrow_type_to_unity(pa.int64()) == "BIGINT"

    def test_float_types(self):
        assert _arrow_type_to_unity(pa.float32()) == "FLOAT"
        assert _arrow_type_to_unity(pa.float64()) == "DOUBLE"

    def test_string_types(self):
        assert _arrow_type_to_unity(pa.utf8()) == "STRING"
        assert _arrow_type_to_unity(pa.large_utf8()) == "STRING"
        assert _arrow_type_to_unity(pa.string()) == "STRING"

    def test_bool(self):
        assert _arrow_type_to_unity(pa.bool_()) == "BOOLEAN"

    def test_date(self):
        assert _arrow_type_to_unity(pa.date32()) == "DATE"

    def test_timestamp(self):
        assert _arrow_type_to_unity(pa.timestamp("us")) == "TIMESTAMP"
        assert _arrow_type_to_unity(pa.timestamp("ns", tz="UTC")) == "TIMESTAMP"

    def test_binary(self):
        assert _arrow_type_to_unity(pa.binary()) == "BINARY"

    def test_unknown_falls_back_to_string(self):
        assert _arrow_type_to_unity(pa.list_(pa.int32())) == "STRING"


# ── _build_table_def ─────────────────────────────────────────────────────────

class TestBuildTableDef:
    def test_basic_table_def(self):
        schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.utf8()),
        ])
        result = _build_table_def("users", "prod", "default", schema, "delta")
        assert result["name"] == "users"
        assert result["catalog_name"] == "prod"
        assert result["schema_name"] == "default"
        assert result["table_type"] == "MANAGED"
        assert result["data_source_format"] == "DELTA"
        assert len(result["columns"]) == 2
        assert result["columns"][0]["name"] == "id"
        assert result["columns"][0]["type_text"] == "BIGINT"
        assert result["columns"][0]["nullable"] is False
        assert result["columns"][1]["name"] == "name"
        assert result["columns"][1]["type_text"] == "STRING"
        assert "partition_columns" not in result

    def test_with_partition_by(self):
        schema = pa.schema([pa.field("id", pa.int64()), pa.field("year", pa.int32())])
        result = _build_table_def("events", "prod", "raw", schema, "parquet", partition_by=["year"])
        assert result["partition_columns"] == ["year"]
        assert result["data_source_format"] == "PARQUET"

    def test_column_positions(self):
        schema = pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.int32()), pa.field("c", pa.int32())])
        result = _build_table_def("t", "c", "s", schema, "delta")
        assert [col["position"] for col in result["columns"]] == [0, 1, 2]


# ── _ensure_table_exists ─────────────────────────────────────────────────────


def _make_material() -> Material:
    ref = _ArrowMaterializedRef(pa.table({"id": [1, 2], "name": ["a", "b"]}))
    return Material(name="m", catalog="unity_cat", materialized_ref=ref, state="materialized")


class TestEnsureTableExists:
    def test_skips_creation_when_table_exists(self, monkeypatch):
        monkeypatch.setattr(
            "rivet_databricks.client.UnityCatalogClient.get_table",
            lambda self, full_name: {"name": "users"},
        )
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        catalog = _make_catalog()
        material = _make_material()
        _ensure_table_exists(catalog, "prod.default.users", material, {"table": "users"})
        # No exception means create_table was not called

    def test_creates_table_when_not_found(self, monkeypatch):
        from rivet_core.errors import RivetError

        created = {}

        def fake_get_table(self, full_name):
            raise ExecutionError(RivetError(code="RVT-503", message="Not found"))

        def fake_create_table(self, table_def):
            created.update(table_def)
            return table_def

        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", fake_get_table)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.create_table", fake_create_table)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        catalog = _make_catalog()
        material = _make_material()
        _ensure_table_exists(catalog, "prod.default.users", material, {"table": "users", "format": "delta"})

        assert created["name"] == "users"
        assert created["catalog_name"] == "prod"
        assert created["schema_name"] == "default"
        assert created["data_source_format"] == "DELTA"
        assert len(created["columns"]) == 2

    def test_creates_table_with_partition_by(self, monkeypatch):
        from rivet_core.errors import RivetError

        created = {}

        def fake_get_table(self, full_name):
            raise ExecutionError(RivetError(code="RVT-503", message="Not found"))

        def fake_create_table(self, table_def):
            created.update(table_def)
            return table_def

        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", fake_get_table)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.create_table", fake_create_table)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        catalog = _make_catalog()
        material = _make_material()
        _ensure_table_exists(
            catalog, "prod.default.users", material,
            {"table": "users", "format": "parquet", "partition_by": ["name"]},
        )

        assert created["partition_columns"] == ["name"]
        assert created["data_source_format"] == "PARQUET"

    def test_propagates_non_404_errors(self, monkeypatch):
        from rivet_core.errors import RivetError

        def fake_get_table(self, full_name):
            raise ExecutionError(RivetError(code="RVT-502", message="Auth failed"))

        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.get_table", fake_get_table)
        monkeypatch.setattr("rivet_databricks.client.UnityCatalogClient.close", lambda self: None)

        catalog = _make_catalog()
        material = _make_material()
        with pytest.raises(ExecutionError) as exc_info:
            _ensure_table_exists(catalog, "prod.default.users", material, {"table": "users"})
        assert exc_info.value.error.code == "RVT-502"


# ── UnitySink.write with create_table ─────────────────────────────────────────


class TestWriteTableCreation:
    @patch("rivet_databricks.unity_sink._ensure_table_exists")
    def test_write_calls_ensure_table_exists_by_default(self, mock_ensure):
        catalog = _make_catalog()
        joint = _make_joint(table="orders")
        material = _make_material()

        with pytest.raises(ExecutionError) as exc_info:
            UnitySink().write(catalog, joint, material, strategy="replace")
        assert exc_info.value.error.code == "RVT-501"
        mock_ensure.assert_called_once()
        # Verify full name was built from catalog options
        call_args = mock_ensure.call_args
        assert call_args[0][1] == "prod.default.orders"

    @patch("rivet_databricks.unity_sink._ensure_table_exists")
    def test_write_skips_creation_when_create_table_false(self, mock_ensure):
        catalog = _make_catalog()
        joint = Joint(
            name="my_sink", joint_type="sink", catalog="unity_cat",
            table="orders", upstream=["src"],
        )
        # Inject create_table=False via sink_options
        joint.sink_options = {"create_table": False}  # type: ignore[attr-defined]
        material = _make_material()

        with pytest.raises(ExecutionError):
            UnitySink().write(catalog, joint, material, strategy="replace")
        mock_ensure.assert_not_called()

    @patch("rivet_databricks.unity_sink._ensure_table_exists")
    def test_write_uses_fully_qualified_name_as_is(self, mock_ensure):
        catalog = _make_catalog()
        joint = _make_joint(table="prod.raw.events")
        material = _make_material()

        with pytest.raises(ExecutionError):
            UnitySink().write(catalog, joint, material, strategy="replace")
        call_args = mock_ensure.call_args
        assert call_args[0][1] == "prod.raw.events"

    @patch("rivet_databricks.unity_sink._ensure_table_exists")
    def test_write_qualifies_unqualified_table_name(self, mock_ensure):
        catalog = Catalog(
            name="unity_cat", type="unity",
            options={"host": "https://my.uc.com", "catalog_name": "mycat", "schema": "myschema", "token": "t"},
        )
        joint = _make_joint(table="mytable")
        material = _make_material()

        with pytest.raises(ExecutionError):
            UnitySink().write(catalog, joint, material, strategy="replace")
        call_args = mock_ensure.call_args
        assert call_args[0][1] == "mycat.myschema.mytable"
