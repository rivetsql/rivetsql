"""Introspection data models for catalog discovery."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class NodeSummary:
    """Summary metadata for a catalog node."""

    row_count: int | None
    size_bytes: int | None
    format: str | None
    last_modified: datetime | None
    owner: str | None
    comment: str | None


@dataclass(frozen=True)
class CatalogNode:
    """A node in the catalog hierarchy (table, view, file, schema, etc.)."""

    name: str
    node_type: str  # "catalog", "database", "schema", "table", "view", "file", etc.
    path: list[str]
    is_container: bool
    children_count: int | None
    summary: NodeSummary | None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ColumnDetail:
    """Detailed column information from catalog introspection."""

    name: str
    type: str  # Arrow type name
    native_type: str | None
    nullable: bool
    default: str | None
    comment: str | None
    is_primary_key: bool
    is_partition_key: bool


@dataclass(frozen=True)
class ObjectSchema:
    """Schema of a catalog object (table, view, file)."""

    path: list[str]
    node_type: str
    columns: list[ColumnDetail]
    primary_key: list[str] | None
    comment: str | None


@dataclass(frozen=True)
class ColumnStatistics:
    """Statistics for a single column."""

    column: str
    min_value: str | None
    max_value: str | None
    distinct_count: int | None
    null_count: int | None
    avg_length: float | None


@dataclass(frozen=True)
class PartitionValue:
    """A single partition with its values and metadata."""

    values: dict[str, str]
    row_count: int | None
    size_bytes: int | None
    last_modified: datetime | None
    location: str | None


@dataclass(frozen=True)
class PartitionInfo:
    """Partitioning information for a catalog object."""

    columns: list[str]
    partitions: list[PartitionValue]


@dataclass(frozen=True)
class ObjectMetadata:
    """Metadata for a catalog object."""

    path: list[str]
    node_type: str
    row_count: int | None
    size_bytes: int | None
    last_modified: datetime | None
    created_at: datetime | None
    format: str | None
    compression: str | None
    owner: str | None
    comment: str | None
    location: str | None
    column_statistics: list[ColumnStatistics]
    partitioning: PartitionInfo | None
    properties: dict[str, str] = field(default_factory=dict)
