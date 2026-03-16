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
    from rivet_core.plugins import PluginRegistry


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


class DeferredRef(MaterializedRef):
    """Catalog-backed deferred ref for checkpoint read-back.

    Holds catalog metadata instead of an Arrow table. The actual read
    is deferred to downstream resolution (adapter or source plugin).
    ``.to_arrow()`` is the last-resort fallback, reading via SourcePlugin.

    When constructed with a pre-computed ``cached_table`` (Arrow fallback
    write path), ``.to_arrow()`` returns it immediately without reading
    from the catalog.  When ``cached_table`` is ``None`` (native SQL write
    path), the first ``.to_arrow()`` call reads from the catalog and caches
    the result.
    """

    def __init__(
        self,
        catalog_name: str,
        catalog_type: str,
        table_name: str,
        catalog_options: dict[str, Any],
        registry: PluginRegistry | None = None,
        cached_table: pyarrow.Table | None = None,
    ) -> None:
        self.catalog_name = catalog_name
        self.catalog_type = catalog_type
        self.table_name = table_name
        self.catalog_options = catalog_options
        self._registry = registry
        self._cached_table: pyarrow.Table | None = cached_table

    def to_arrow(self) -> pyarrow.Table:
        """Return cached table if available, otherwise read from catalog via SourcePlugin."""
        if self._cached_table is not None:
            return self._cached_table

        if self._registry is None:
            raise ExecutionError(
                RivetError(
                    code="RVT-501",
                    message=(
                        f"DeferredRef for table '{self.table_name}' in catalog "
                        f"'{self.catalog_name}': no PluginRegistry available to read data."
                    ),
                    remediation="Ensure a PluginRegistry is provided when constructing DeferredRef.",
                )
            )

        source = self._registry._sources.get(self.catalog_type)
        if not source:
            raise ExecutionError(
                RivetError(
                    code="RVT-501",
                    message=(
                        f"DeferredRef for table '{self.table_name}' in catalog "
                        f"'{self.catalog_name}': no SourcePlugin registered for "
                        f"catalog type '{self.catalog_type}'."
                    ),
                    context={
                        "catalog": self.catalog_name,
                        "catalog_type": self.catalog_type,
                        "table": self.table_name,
                    },
                    remediation=f"Register a SourcePlugin for catalog type '{self.catalog_type}'.",
                )
            )

        from rivet_core.models import Catalog, Joint

        cat = Catalog(name=self.catalog_name, type=self.catalog_type, options=self.catalog_options)
        joint = Joint(
            name=self.table_name,
            joint_type="source",
            catalog=self.catalog_name,
            table=self.table_name,
        )

        try:
            mat = source.read(cat, joint, None)
        except Exception as exc:
            raise ExecutionError(
                RivetError(
                    code="RVT-501",
                    message=(
                        f"DeferredRef for table '{self.table_name}' in catalog "
                        f"'{self.catalog_name}': read failed: {exc}"
                    ),
                    context={
                        "catalog": self.catalog_name,
                        "catalog_type": self.catalog_type,
                        "table": self.table_name,
                    },
                    remediation="Check catalog connectivity and table existence.",
                )
            ) from exc

        if mat.materialized_ref is None:
            raise ExecutionError(
                RivetError(
                    code="RVT-501",
                    message=(
                        f"DeferredRef for table '{self.table_name}' in catalog "
                        f"'{self.catalog_name}': read returned no data."
                    ),
                    context={
                        "catalog": self.catalog_name,
                        "catalog_type": self.catalog_type,
                        "table": self.table_name,
                    },
                    remediation="Check that the catalog table was written successfully.",
                )
            )

        self._cached_table = mat.to_arrow()
        return self._cached_table

    @property
    def schema(self) -> Schema:
        from rivet_core.models import Column, Schema

        tbl = self.to_arrow()
        columns = [
            Column(name=field.name, type=str(field.type), nullable=field.nullable)
            for field in tbl.schema
        ]
        return Schema(columns=columns)

    @property
    def row_count(self) -> int:
        return self.to_arrow().num_rows  # type: ignore[no-any-return]

    @property
    def size_bytes(self) -> int | None:
        return self.to_arrow().nbytes  # type: ignore[no-any-return]

    @property
    def storage_type(self) -> str:
        return "catalog_deferred"


class ArrowMaterialization(MaterializationStrategy):
    """Default strategy: materialize into an in-memory Arrow table.

    .to_arrow() is zero-copy — returns the stored table directly.
    """

    def materialize(self, data: pyarrow.Table, context: MaterializationContext) -> MaterializedRef:
        return _ArrowMaterializedRef(data)

    def evict(self, ref: MaterializedRef) -> None:
        if isinstance(ref, _ArrowMaterializedRef):
            ref._table = None
