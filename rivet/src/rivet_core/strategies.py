"""Materialization strategy contracts.

Defines the ABC for pluggable materialization strategies and the built-in
ArrowMaterialization (default: in-memory Arrow table, zero-copy .to_arrow()).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyarrow

from rivet_core.errors import ExecutionError, RivetError

if TYPE_CHECKING:
    from rivet_core.models import Schema


class MaterializedRef:
    """A handle to materialized data with guaranteed .to_arrow() access."""

    @abstractmethod
    def to_arrow(self) -> pyarrow.Table:
        """Return the materialized data as a PyArrow Table."""
        ...

    @property
    @abstractmethod
    def schema(self) -> Schema:
        """Column schema of the materialized data."""
        ...

    @property
    @abstractmethod
    def row_count(self) -> int:
        """Number of rows in the materialized data."""
        ...

    @property
    @abstractmethod
    def size_bytes(self) -> int | None:
        """Approximate memory size in bytes, or None if unknown."""
        ...

    @property
    @abstractmethod
    def storage_type(self) -> str:
        """Storage backend identifier, e.g. 'arrow', 'parquet', 'engine_temp'."""
        ...


@dataclass
class MaterializationContext:
    """Context passed to a MaterializationStrategy during materialization."""

    joint_name: str
    strategy_name: str
    options: dict[str, Any]


class MaterializationStrategy(ABC):
    """ABC for pluggable materialization strategies."""

    @abstractmethod
    def materialize(self, data: pyarrow.Table, context: MaterializationContext) -> MaterializedRef:
        """Persist data and return a MaterializedRef handle."""
        ...

    @abstractmethod
    def evict(self, ref: MaterializedRef) -> None:
        """Release the storage backing ref. After eviction, ref.to_arrow() must raise."""
        ...


class _ArrowMaterializedRef(MaterializedRef):
    """In-memory Arrow table ref. Eviction sets _table to None."""

    def __init__(self, table: pyarrow.Table) -> None:
        self._table: pyarrow.Table | None = table

    def to_arrow(self) -> pyarrow.Table:
        if self._table is None:
            raise ExecutionError(
                RivetError(
                    code="RVT-401",
                    message="MaterializedRef has been evicted and is no longer accessible.",
                    remediation="Do not access a MaterializedRef after it has been evicted.",
                )
            )
        return self._table

    @property
    def schema(self) -> Schema:
        from rivet_core.models import Column, Schema

        if self._table is None:
            raise RuntimeError("MaterializedRef has been evicted.")
        columns = [
            Column(name=field.name, type=str(field.type), nullable=field.nullable)
            for field in self._table.schema
        ]
        return Schema(columns=columns)

    @property
    def row_count(self) -> int:
        if self._table is None:
            raise RuntimeError("MaterializedRef has been evicted.")
        return self._table.num_rows  # type: ignore[no-any-return]

    @property
    def size_bytes(self) -> int | None:
        if self._table is None:
            raise RuntimeError("MaterializedRef has been evicted.")
        return self._table.nbytes  # type: ignore[no-any-return]

    @property
    def storage_type(self) -> str:
        return "arrow"


class ArrowMaterialization(MaterializationStrategy):
    """Default strategy: materialize into an in-memory Arrow table.

    .to_arrow() is zero-copy — returns the stored table directly.
    """

    def materialize(self, data: pyarrow.Table, context: MaterializationContext) -> MaterializedRef:
        return _ArrowMaterializedRef(data)

    def evict(self, ref: MaterializedRef) -> None:
        if isinstance(ref, _ArrowMaterializedRef):
            ref._table = None
