"""Tests for GlueSink plugin (task 19.2 + 19.4): sink options table, write_strategy, partition_by,
format, compression, create_table, update_schema, lf_tags, and post-write partition sync."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from rivet_aws.glue_sink import (
    SUPPORTED_STRATEGIES,
    UNSUPPORTED_STRATEGIES,
    GlueSink,
    _arrow_type_to_glue,
    _build_partition_storage_descriptor,
    _create_glue_table,
    _extract_partition_values,
    _format_to_serde,
    _get_existing_partitions,
    _sync_partitions,
    _table_exists,
    _validate_sink_options,
)
from rivet_core.errors import ExecutionError, PluginValidationError
from rivet_core.models import Catalog, Joint, Material
from rivet_core.plugins import SinkPlugin
from rivet_core.strategies import MaterializedRef

# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_catalog(options: dict | None = None) -> Catalog:
    opts = {"database": "my_db", "access_key_id": "AKID", "secret_access_key": "SECRET"}
    if options:
        opts.update(options)
    return Catalog(name="glue_cat", type="glue", options=opts)


def _make_joint(name: str = "my_joint", table: str | None = "orders") -> Joint:
    return Joint(name=name, joint_type="sink", catalog="glue_cat", table=table)


def _make_material(table: pa.Table | None = None) -> Material:
    if table is None:
        table = pa.table({"id": [1, 2], "name": ["a", "b"]})

    class _Ref(MaterializedRef):
        def __init__(self, t: pa.Table) -> None:
            self._t = t

        def to_arrow(self) -> pa.Table:
            return self._t

        @property
        def schema(self) -> None:
            return None

        @property
        def row_count(self) -> int:
            return self._t.num_rows

        @property
        def size_bytes(self) -> int | None:
            return None

        @property
        def storage_type(self) -> str:
            return "test"

    return Material(name="mat", catalog="glue_cat", materialized_ref=_Ref(table), state="materialized")


def _mock_glue_client(table_exists: bool = True, location: str = "s3://bucket/db/orders/") -> MagicMock:
    client = MagicMock()
    if table_exists:
        client.get_table.return_value = {
            "Table": {
                "Name": "orders",
                "StorageDescriptor": {"Location": location},
            }
        }
    else:
        client.get_table.side_effect = Exception("Table not found")
    return client


# ── catalog_type and plugin contract ─────────────────────────────────────────

def test_catalog_type():
    assert GlueSink.catalog_type == "glue"


def test_is_sink_plugin():
    assert isinstance(GlueSink(), SinkPlugin)


def test_supported_strategies_set():
    assert {"append", "replace", "delete_insert", "incremental_append", "truncate_insert"} == SUPPORTED_STRATEGIES


def test_unsupported_strategies_set():
    assert {"merge", "scd2"} == UNSUPPORTED_STRATEGIES


# ── _validate_sink_options ────────────────────────────────────────────────────

def test_validate_accepts_table_only():
    _validate_sink_options({"table": "orders"})


def test_validate_accepts_all_options():
    _validate_sink_options({
        "table": "orders",
        "write_strategy": "append",
        "partition_by": ["year", "month"],
        "format": "parquet",
        "compression": "snappy",
        "create_table": True,
        "update_schema": False,
        "lf_tags": {"team": "data"},
    })


def test_validate_rejects_missing_table():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_sink_options({})
    assert exc_info.value.error.code == "RVT-201"
    assert "table" in exc_info.value.error.message


def test_validate_rejects_unknown_option():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_sink_options({"table": "orders", "unknown_opt": "x"})
    assert exc_info.value.error.code == "RVT-201"
    assert "unknown_opt" in exc_info.value.error.message


def test_validate_rejects_merge_strategy():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_sink_options({"table": "orders", "write_strategy": "merge"})
    assert exc_info.value.error.code == "RVT-202"
    assert "merge" in exc_info.value.error.message


def test_validate_rejects_scd2_strategy():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_sink_options({"table": "orders", "write_strategy": "scd2"})
    assert exc_info.value.error.code == "RVT-202"
    assert "scd2" in exc_info.value.error.message


def test_validate_rejects_unknown_strategy():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_sink_options({"table": "orders", "write_strategy": "upsert"})
    assert exc_info.value.error.code == "RVT-201"
    assert "upsert" in exc_info.value.error.message


def test_validate_accepts_all_supported_strategies():
    for strategy in SUPPORTED_STRATEGIES:
        _validate_sink_options({"table": "orders", "write_strategy": strategy})


def test_validate_rejects_invalid_format():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_sink_options({"table": "orders", "format": "delta"})
    assert exc_info.value.error.code == "RVT-201"
    assert "delta" in exc_info.value.error.message


def test_validate_accepts_valid_formats():
    for fmt in ("parquet", "csv", "json", "orc"):
        _validate_sink_options({"table": "orders", "format": fmt})


def test_validate_rejects_invalid_compression():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_sink_options({"table": "orders", "compression": "brotli"})
    assert exc_info.value.error.code == "RVT-201"
    assert "brotli" in exc_info.value.error.message


def test_validate_accepts_valid_compressions():
    for comp in ("snappy", "gzip", "zstd", "lz4", "none", "uncompressed"):
        _validate_sink_options({"table": "orders", "compression": comp})


def test_validate_rejects_lf_tags_non_dict():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_sink_options({"table": "orders", "lf_tags": ["tag1"]})
    assert exc_info.value.error.code == "RVT-201"
    assert "lf_tags" in exc_info.value.error.message


def test_validate_rejects_partition_by_non_list():
    with pytest.raises(PluginValidationError) as exc_info:
        _validate_sink_options({"table": "orders", "partition_by": "year"})
    assert exc_info.value.error.code == "RVT-201"
    assert "partition_by" in exc_info.value.error.message


# ── _format_to_serde ──────────────────────────────────────────────────────────

def test_format_to_serde_parquet():
    inp, out, serde = _format_to_serde("parquet")
    assert "parquet" in inp.lower()
    assert "parquet" in out.lower()
    assert "parquet" in serde.lower()


def test_format_to_serde_orc():
    inp, out, serde = _format_to_serde("orc")
    assert "orc" in inp.lower()


def test_format_to_serde_json():
    inp, out, serde = _format_to_serde("json")
    assert "Text" in inp or "text" in inp.lower()


def test_format_to_serde_csv():
    inp, out, serde = _format_to_serde("csv")
    assert "Text" in inp or "text" in inp.lower()


# ── _arrow_type_to_glue ───────────────────────────────────────────────────────

def test_arrow_type_to_glue_int64():
    assert _arrow_type_to_glue("int64") == "bigint"


def test_arrow_type_to_glue_int32():
    assert _arrow_type_to_glue("int32") == "int"


def test_arrow_type_to_glue_float64():
    assert _arrow_type_to_glue("float64") == "double"


def test_arrow_type_to_glue_large_utf8():
    assert _arrow_type_to_glue("large_utf8") == "string"


def test_arrow_type_to_glue_bool():
    assert _arrow_type_to_glue("bool") == "boolean"


def test_arrow_type_to_glue_timestamp():
    assert _arrow_type_to_glue("timestamp[us]") == "timestamp"


def test_arrow_type_to_glue_unknown_defaults_to_string():
    assert _arrow_type_to_glue("unknown_type") == "string"


# ── _table_exists ─────────────────────────────────────────────────────────────

def test_table_exists_returns_true_when_found():
    catalog = _make_catalog()
    client = _mock_glue_client(table_exists=True)
    assert _table_exists(client, catalog, "orders") is True


def test_table_exists_returns_false_when_not_found():
    catalog = _make_catalog()
    client = _mock_glue_client(table_exists=False)
    assert _table_exists(client, catalog, "orders") is False


def test_table_exists_passes_catalog_id():
    catalog = _make_catalog({"catalog_id": "123456789012"})
    client = _mock_glue_client(table_exists=True)
    _table_exists(client, catalog, "orders")
    client.get_table.assert_called_once_with(
        DatabaseName="my_db", Name="orders", CatalogId="123456789012"
    )


# ── _create_glue_table ────────────────────────────────────────────────────────

def test_create_glue_table_calls_create_table():
    catalog = _make_catalog()
    client = MagicMock()
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.large_utf8())])
    _create_glue_table(
        client=client,
        catalog=catalog,
        table_name="orders",
        location="s3://bucket/db/orders/",
        fmt="parquet",
        compression="snappy",
        partition_by=[],
        arrow_schema=schema,
    )
    client.create_table.assert_called_once()
    call_kwargs = client.create_table.call_args[1]
    assert call_kwargs["DatabaseName"] == "my_db"
    assert call_kwargs["TableInput"]["Name"] == "orders"


def test_create_glue_table_with_partition_keys():
    catalog = _make_catalog()
    client = MagicMock()
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("year", pa.large_utf8()),
        pa.field("month", pa.large_utf8()),
    ])
    _create_glue_table(
        client=client,
        catalog=catalog,
        table_name="events",
        location="s3://bucket/db/events/",
        fmt="parquet",
        compression="snappy",
        partition_by=["year", "month"],
        arrow_schema=schema,
    )
    call_kwargs = client.create_table.call_args[1]
    table_input = call_kwargs["TableInput"]
    # Partition keys should be in PartitionKeys, not Columns
    partition_key_names = [pk["Name"] for pk in table_input["PartitionKeys"]]
    column_names = [c["Name"] for c in table_input["StorageDescriptor"]["Columns"]]
    assert "year" in partition_key_names
    assert "month" in partition_key_names
    assert "year" not in column_names
    assert "month" not in column_names
    assert "id" in column_names


def test_create_glue_table_passes_catalog_id():
    catalog = _make_catalog({"catalog_id": "123456789012"})
    client = MagicMock()
    schema = pa.schema([pa.field("id", pa.int64())])
    _create_glue_table(
        client=client,
        catalog=catalog,
        table_name="orders",
        location="s3://bucket/db/orders/",
        fmt="parquet",
        compression="snappy",
        partition_by=[],
        arrow_schema=schema,
    )
    call_kwargs = client.create_table.call_args[1]
    assert call_kwargs["CatalogId"] == "123456789012"


# ── GlueSink.write ────────────────────────────────────────────────────────────

def test_write_calls_create_table_when_not_exists():
    catalog = _make_catalog()
    joint = _make_joint(table="orders")
    material = _make_material()
    client = _mock_glue_client(table_exists=False)

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        GlueSink().write(catalog, joint, material, "replace")

    client.create_table.assert_called_once()


def test_write_skips_create_table_when_exists():
    catalog = _make_catalog()
    joint = _make_joint(table="orders")
    material = _make_material()
    client = _mock_glue_client(table_exists=True)

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        GlueSink().write(catalog, joint, material, "replace")

    client.create_table.assert_not_called()


def test_write_raises_when_table_missing_and_create_table_false():
    catalog = _make_catalog()
    material = _make_material()
    client = _mock_glue_client(table_exists=False)

    class _Joint:
        name = "my_joint"
        table = "orders"
        sink_options = {"table": "orders", "create_table": False, "write_strategy": "replace"}

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        with pytest.raises(ExecutionError) as exc_info:
            GlueSink().write(catalog, _Joint(), material, "replace")  # type: ignore[arg-type]

    assert exc_info.value.error.code == "RVT-506"


def test_write_calls_update_schema_when_flag_set():
    catalog = _make_catalog()
    material = _make_material()
    client = _mock_glue_client(table_exists=True)

    class _Joint:
        name = "my_joint"
        table = "orders"
        sink_options = {"table": "orders", "update_schema": True, "write_strategy": "replace"}

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        with patch("rivet_aws.glue_sink._update_glue_schema") as mock_update:
            GlueSink().write(catalog, _Joint(), material, "replace")  # type: ignore[arg-type]

    mock_update.assert_called_once()


def test_write_does_not_call_update_schema_by_default():
    catalog = _make_catalog()
    joint = _make_joint(table="orders")
    material = _make_material()
    client = _mock_glue_client(table_exists=True)

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        with patch("rivet_aws.glue_sink._update_glue_schema") as mock_update:
            GlueSink().write(catalog, joint, material, "replace")

    mock_update.assert_not_called()


def test_write_applies_lf_tags_when_provided():
    catalog = _make_catalog()
    material = _make_material()
    client = _mock_glue_client(table_exists=True)

    class _Joint:
        name = "my_joint"
        table = "orders"
        sink_options = {"table": "orders", "lf_tags": {"team": "data"}, "write_strategy": "replace"}

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        with patch("rivet_aws.glue_sink._apply_lf_tags") as mock_lf:
            GlueSink().write(catalog, _Joint(), material, "replace")  # type: ignore[arg-type]

    mock_lf.assert_called_once()
    _, _, table_name, lf_tags = mock_lf.call_args[0]
    assert table_name == "orders"
    assert lf_tags == {"team": "data"}


def test_write_does_not_apply_lf_tags_when_absent():
    catalog = _make_catalog()
    joint = _make_joint(table="orders")
    material = _make_material()
    client = _mock_glue_client(table_exists=True)

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        with patch("rivet_aws.glue_sink._apply_lf_tags") as mock_lf:
            GlueSink().write(catalog, joint, material, "replace")

    mock_lf.assert_not_called()


def test_write_rejects_merge_strategy():
    catalog = _make_catalog()
    joint = _make_joint(table="orders")
    material = _make_material()

    with pytest.raises(PluginValidationError) as exc_info:
        GlueSink().write(catalog, joint, material, "merge")

    assert exc_info.value.error.code == "RVT-202"


def test_write_rejects_scd2_strategy():
    catalog = _make_catalog()
    joint = _make_joint(table="orders")
    material = _make_material()

    with pytest.raises(PluginValidationError) as exc_info:
        GlueSink().write(catalog, joint, material, "scd2")

    assert exc_info.value.error.code == "RVT-202"


def test_write_accepts_all_supported_strategies():
    catalog = _make_catalog()
    material = _make_material()
    client = _mock_glue_client(table_exists=True)

    for strategy in SUPPORTED_STRATEGIES:
        joint = _make_joint(table="orders")
        with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
            # Should not raise
            GlueSink().write(catalog, joint, material, strategy)


def test_write_uses_joint_name_as_table_when_no_table():
    catalog = _make_catalog()
    joint = Joint(name="fallback_table", joint_type="sink", catalog="glue_cat", table=None)
    material = _make_material()
    client = _mock_glue_client(table_exists=True)

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        with patch("rivet_aws.glue_sink._table_exists", return_value=True) as mock_exists:
            GlueSink().write(catalog, joint, material, "replace")

    # The table name used should be "fallback_table"
    mock_exists.assert_called_once()
    _, _, table_name = mock_exists.call_args[0]
    assert table_name == "fallback_table"


def test_write_creates_table_with_bucket_from_catalog():
    catalog = _make_catalog({"bucket": "my-bucket", "prefix": "data"})
    joint = _make_joint(table="new_table")
    material = _make_material()
    client = _mock_glue_client(table_exists=False)

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        GlueSink().write(catalog, joint, material, "replace")

    call_kwargs = client.create_table.call_args[1]
    location = call_kwargs["TableInput"]["StorageDescriptor"]["Location"]
    assert "my-bucket" in location
    assert "new_table" in location


# ── _extract_partition_values ─────────────────────────────────────────────────

def test_extract_partition_values_empty_partition_by():
    table = pa.table({"id": [1, 2], "year": ["2024", "2025"]})
    assert _extract_partition_values(table, []) == []


def test_extract_partition_values_single_column():
    table = pa.table({"id": [1, 2, 3], "year": ["2024", "2024", "2025"]})
    result = _extract_partition_values(table, ["year"])
    assert len(result) == 2
    assert {"year": "2024"} in result
    assert {"year": "2025"} in result


def test_extract_partition_values_multi_column():
    table = pa.table({
        "id": [1, 2, 3],
        "year": ["2024", "2024", "2025"],
        "month": ["01", "02", "01"],
    })
    result = _extract_partition_values(table, ["year", "month"])
    assert len(result) == 3
    assert {"year": "2024", "month": "01"} in result
    assert {"year": "2024", "month": "02"} in result
    assert {"year": "2025", "month": "01"} in result


def test_extract_partition_values_deduplicates():
    table = pa.table({"id": [1, 2, 3], "year": ["2024", "2024", "2024"]})
    result = _extract_partition_values(table, ["year"])
    assert len(result) == 1
    assert result[0] == {"year": "2024"}


# ── _build_partition_storage_descriptor ───────────────────────────────────────

def test_build_partition_storage_descriptor_location():
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("year", pa.large_utf8())])
    sd = _build_partition_storage_descriptor(
        location="s3://bucket/db/orders",
        partition_values={"year": "2024"},
        fmt="parquet",
        compression="snappy",
        arrow_schema=schema,
        partition_by=["year"],
    )
    assert sd["Location"] == "s3://bucket/db/orders/year=2024"
    # Partition columns excluded from Columns
    col_names = [c["Name"] for c in sd["Columns"]]
    assert "year" not in col_names
    assert "id" in col_names


def test_build_partition_storage_descriptor_multi_key():
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("year", pa.large_utf8()),
        pa.field("month", pa.large_utf8()),
    ])
    sd = _build_partition_storage_descriptor(
        location="s3://bucket/db/events/",
        partition_values={"year": "2024", "month": "03"},
        fmt="parquet",
        compression="snappy",
        arrow_schema=schema,
        partition_by=["year", "month"],
    )
    assert sd["Location"] == "s3://bucket/db/events/year=2024/month=03"


# ── _get_existing_partitions ──────────────────────────────────────────────────

def test_get_existing_partitions_returns_dict():
    catalog = _make_catalog()
    client = MagicMock()
    paginator = MagicMock()
    client.get_paginator.return_value = paginator
    paginator.paginate.return_value = [
        {"Partitions": [
            {"Values": ["2024", "01"], "StorageDescriptor": {"Location": "s3://b/p1"}},
            {"Values": ["2024", "02"], "StorageDescriptor": {"Location": "s3://b/p2"}},
        ]}
    ]
    result = _get_existing_partitions(client, catalog, "orders")
    assert ("2024", "01") in result
    assert ("2024", "02") in result
    assert len(result) == 2


def test_get_existing_partitions_passes_catalog_id():
    catalog = _make_catalog({"catalog_id": "123456789012"})
    client = MagicMock()
    paginator = MagicMock()
    client.get_paginator.return_value = paginator
    paginator.paginate.return_value = [{"Partitions": []}]
    _get_existing_partitions(client, catalog, "orders")
    paginator.paginate.assert_called_once_with(
        DatabaseName="my_db", TableName="orders", CatalogId="123456789012"
    )


# ── _sync_partitions ─────────────────────────────────────────────────────────

def test_sync_partitions_noop_without_partition_by():
    client = MagicMock()
    catalog = _make_catalog()
    table = pa.table({"id": [1, 2]})
    _sync_partitions(client, catalog, "orders", "s3://b/orders", [], table, "parquet", "snappy")
    client.batch_create_partition.assert_not_called()
    client.update_partition.assert_not_called()


def test_sync_partitions_creates_new_partitions():
    catalog = _make_catalog()
    client = MagicMock()
    paginator = MagicMock()
    client.get_paginator.return_value = paginator
    paginator.paginate.return_value = [{"Partitions": []}]  # no existing partitions

    table = pa.table({"id": [1, 2], "year": ["2024", "2025"]})
    _sync_partitions(
        client, catalog, "orders", "s3://b/orders", ["year"], table, "parquet", "snappy"
    )
    client.batch_create_partition.assert_called_once()
    call_kwargs = client.batch_create_partition.call_args[1]
    assert call_kwargs["DatabaseName"] == "my_db"
    assert call_kwargs["TableName"] == "orders"
    assert len(call_kwargs["PartitionInputList"]) == 2


def test_sync_partitions_updates_existing_partitions():
    catalog = _make_catalog()
    client = MagicMock()
    paginator = MagicMock()
    client.get_paginator.return_value = paginator
    paginator.paginate.return_value = [
        {"Partitions": [
            {"Values": ["2024"], "StorageDescriptor": {"Location": "s3://b/orders/year=2024"}},
        ]}
    ]

    table = pa.table({"id": [1], "year": ["2024"]})
    _sync_partitions(
        client, catalog, "orders", "s3://b/orders", ["year"], table, "parquet", "snappy"
    )
    client.update_partition.assert_called_once()
    call_kwargs = client.update_partition.call_args[1]
    assert call_kwargs["PartitionValueList"] == ["2024"]
    client.batch_create_partition.assert_not_called()


def test_sync_partitions_mixed_new_and_existing():
    catalog = _make_catalog()
    client = MagicMock()
    paginator = MagicMock()
    client.get_paginator.return_value = paginator
    paginator.paginate.return_value = [
        {"Partitions": [
            {"Values": ["2024"], "StorageDescriptor": {"Location": "s3://b/orders/year=2024"}},
        ]}
    ]

    table = pa.table({"id": [1, 2], "year": ["2024", "2025"]})
    _sync_partitions(
        client, catalog, "orders", "s3://b/orders", ["year"], table, "parquet", "snappy"
    )
    # 2024 exists → update, 2025 new → batch create
    client.update_partition.assert_called_once()
    client.batch_create_partition.assert_called_once()
    batch_kwargs = client.batch_create_partition.call_args[1]
    assert len(batch_kwargs["PartitionInputList"]) == 1
    assert batch_kwargs["PartitionInputList"][0]["Values"] == ["2025"]


def test_sync_partitions_passes_catalog_id():
    catalog = _make_catalog({"catalog_id": "123456789012"})
    client = MagicMock()
    paginator = MagicMock()
    client.get_paginator.return_value = paginator
    paginator.paginate.return_value = [{"Partitions": []}]

    table = pa.table({"id": [1], "year": ["2024"]})
    _sync_partitions(
        client, catalog, "orders", "s3://b/orders", ["year"], table, "parquet", "snappy"
    )
    batch_kwargs = client.batch_create_partition.call_args[1]
    assert batch_kwargs["CatalogId"] == "123456789012"


def test_sync_partitions_batches_over_100():
    catalog = _make_catalog()
    client = MagicMock()
    paginator = MagicMock()
    client.get_paginator.return_value = paginator
    paginator.paginate.return_value = [{"Partitions": []}]

    # Create 150 unique partitions
    years = [str(i) for i in range(150)]
    ids = list(range(150))
    table = pa.table({"id": ids, "year": years})
    _sync_partitions(
        client, catalog, "orders", "s3://b/orders", ["year"], table, "parquet", "snappy"
    )
    # Should be called twice: 100 + 50
    assert client.batch_create_partition.call_count == 2
    first_batch = client.batch_create_partition.call_args_list[0][1]["PartitionInputList"]
    second_batch = client.batch_create_partition.call_args_list[1][1]["PartitionInputList"]
    assert len(first_batch) == 100
    assert len(second_batch) == 50


# ── GlueSink.write post-write integration ─────────────────────────────────────

def test_write_calls_sync_partitions_with_partition_by():
    catalog = _make_catalog()
    material = _make_material(pa.table({"id": [1, 2], "year": ["2024", "2025"]}))
    client = _mock_glue_client(table_exists=True)

    class _Joint:
        name = "my_joint"
        table = "orders"
        sink_options = {
            "table": "orders",
            "write_strategy": "append",
            "partition_by": ["year"],
        }

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        with patch("rivet_aws.glue_sink._sync_partitions") as mock_sync:
            GlueSink().write(catalog, _Joint(), material, "append")  # type: ignore[arg-type]

    mock_sync.assert_called_once()
    _, kwargs = mock_sync.call_args
    if not kwargs:
        args = mock_sync.call_args[0]
        assert args[4] == ["year"]  # partition_by
    else:
        assert kwargs.get("partition_by") == ["year"]


def test_write_calls_sync_partitions_without_partition_by():
    catalog = _make_catalog()
    joint = _make_joint(table="orders")
    material = _make_material()
    client = _mock_glue_client(table_exists=True)

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        with patch("rivet_aws.glue_sink._sync_partitions") as mock_sync:
            GlueSink().write(catalog, joint, material, "replace")

    # Still called, but with empty partition_by → noop inside
    mock_sync.assert_called_once()


def test_write_update_schema_calls_update_table_post_write():
    """update_schema should call UpdateTable even for newly created tables."""
    catalog = _make_catalog()
    material = _make_material()
    client = _mock_glue_client(table_exists=False)

    class _Joint:
        name = "my_joint"
        table = "new_table"
        sink_options = {"table": "new_table", "update_schema": True, "write_strategy": "replace"}

    with patch("rivet_aws.glue_catalog._make_glue_client", return_value=client):
        with patch("rivet_aws.glue_sink._sync_partitions"):
            with patch("rivet_aws.glue_sink._update_glue_schema") as mock_update:
                GlueSink().write(catalog, _Joint(), material, "replace")  # type: ignore[arg-type]

    mock_update.assert_called_once()
