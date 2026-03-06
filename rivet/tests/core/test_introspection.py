"""Tests for introspection data models."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from rivet_core.introspection import (
    CatalogNode,
    ColumnDetail,
    ColumnStatistics,
    NodeSummary,
    ObjectMetadata,
    ObjectSchema,
    PartitionInfo,
    PartitionValue,
)


def test_node_summary_frozen() -> None:
    summary = NodeSummary(
        row_count=100,
        size_bytes=1024,
        format="parquet",
        last_modified=None,
        owner="alice",
        comment="test table",
    )
    with pytest.raises((AttributeError, TypeError)):
        summary.row_count = 200  # type: ignore[misc]


def test_catalog_node_fields() -> None:
    summary = NodeSummary(
        row_count=50, size_bytes=512, format="csv", last_modified=None, owner=None, comment=None
    )
    node = CatalogNode(
        name="orders",
        node_type="table",
        path=["my_catalog", "orders"],
        is_container=False,
        children_count=None,
        summary=summary,
    )
    assert node.name == "orders"
    assert node.node_type == "table"
    assert node.path == ["my_catalog", "orders"]
    assert not node.is_container
    assert node.children_count is None
    assert node.summary is summary


def test_catalog_node_container() -> None:
    node = CatalogNode(
        name="my_schema",
        node_type="schema",
        path=["my_catalog", "my_schema"],
        is_container=True,
        children_count=5,
        summary=None,
    )
    assert node.is_container
    assert node.children_count == 5
    assert node.summary is None


def test_catalog_node_frozen() -> None:
    node = CatalogNode(
        name="t", node_type="table", path=["t"], is_container=False, children_count=None, summary=None
    )
    with pytest.raises((AttributeError, TypeError)):
        node.name = "other"  # type: ignore[misc]


def test_column_detail_fields() -> None:
    col = ColumnDetail(
        name="id",
        type="int64",
        native_type="BIGINT",
        nullable=False,
        default=None,
        comment="primary key",
        is_primary_key=True,
        is_partition_key=False,
    )
    assert col.name == "id"
    assert col.type == "int64"
    assert col.native_type == "BIGINT"
    assert not col.nullable
    assert col.is_primary_key
    assert not col.is_partition_key


def test_column_detail_frozen() -> None:
    col = ColumnDetail(
        name="x", type="utf8", native_type=None, nullable=True,
        default=None, comment=None, is_primary_key=False, is_partition_key=False
    )
    with pytest.raises((AttributeError, TypeError)):
        col.name = "y"  # type: ignore[misc]


def test_object_schema_fields() -> None:
    col = ColumnDetail(
        name="id", type="int32", native_type=None, nullable=False,
        default=None, comment=None, is_primary_key=True, is_partition_key=False
    )
    schema = ObjectSchema(
        path=["catalog", "table"],
        node_type="table",
        columns=[col],
        primary_key=["id"],
        comment="main table",
    )
    assert schema.path == ["catalog", "table"]
    assert schema.node_type == "table"
    assert len(schema.columns) == 1
    assert schema.primary_key == ["id"]
    assert schema.comment == "main table"


def test_object_schema_frozen() -> None:
    schema = ObjectSchema(path=[], node_type="table", columns=[], primary_key=None, comment=None)
    with pytest.raises((AttributeError, TypeError)):
        schema.node_type = "view"  # type: ignore[misc]


def test_column_statistics_fields() -> None:
    stats = ColumnStatistics(
        column="amount",
        min_value="0.0",
        max_value="9999.99",
        distinct_count=500,
        null_count=3,
        avg_length=None,
    )
    assert stats.column == "amount"
    assert stats.min_value == "0.0"
    assert stats.max_value == "9999.99"
    assert stats.distinct_count == 500
    assert stats.null_count == 3
    assert stats.avg_length is None


def test_partition_value_fields() -> None:
    pv = PartitionValue(
        values={"year": "2024", "month": "01"},
        row_count=1000,
        size_bytes=2048,
        last_modified=None,
        location="/data/year=2024/month=01",
    )
    assert pv.values == {"year": "2024", "month": "01"}
    assert pv.row_count == 1000
    assert pv.location == "/data/year=2024/month=01"


def test_partition_info_fields() -> None:
    pv = PartitionValue(values={"dt": "2024-01-01"}, row_count=None, size_bytes=None, last_modified=None, location=None)
    info = PartitionInfo(columns=["dt"], partitions=[pv])
    assert info.columns == ["dt"]
    assert len(info.partitions) == 1


def test_object_metadata_fields() -> None:
    now = datetime(2024, 1, 1, tzinfo=UTC)
    stats = ColumnStatistics(column="id", min_value="1", max_value="100", distinct_count=100, null_count=0, avg_length=None)
    meta = ObjectMetadata(
        path=["catalog", "table"],
        node_type="table",
        row_count=100,
        size_bytes=4096,
        last_modified=now,
        created_at=now,
        format="parquet",
        compression="snappy",
        owner="alice",
        comment="test",
        location="/data/table",
        column_statistics=[stats],
        partitioning=None,
        properties={"key": "value"},
    )
    assert meta.row_count == 100
    assert meta.format == "parquet"
    assert meta.compression == "snappy"
    assert meta.owner == "alice"
    assert len(meta.column_statistics) == 1
    assert meta.partitioning is None
    assert meta.properties == {"key": "value"}


def test_object_metadata_default_properties() -> None:
    meta = ObjectMetadata(
        path=[],
        node_type="table",
        row_count=None,
        size_bytes=None,
        last_modified=None,
        created_at=None,
        format=None,
        compression=None,
        owner=None,
        comment=None,
        location=None,
        column_statistics=[],
        partitioning=None,
    )
    assert meta.properties == {}


def test_object_metadata_frozen() -> None:
    meta = ObjectMetadata(
        path=[], node_type="table", row_count=None, size_bytes=None,
        last_modified=None, created_at=None, format=None, compression=None,
        owner=None, comment=None, location=None, column_statistics=[], partitioning=None,
    )
    with pytest.raises((AttributeError, TypeError)):
        meta.row_count = 1  # type: ignore[misc]
