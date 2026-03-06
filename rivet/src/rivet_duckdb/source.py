"""DuckDB source plugin: execute SQL against DuckDB, return deferred Material."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow

from rivet_core.models import Material
from rivet_core.plugins import SourcePlugin
from rivet_core.strategies import MaterializedRef

if TYPE_CHECKING:
    from rivet_core.models import Catalog, Joint


class DuckDBDeferredMaterializedRef(MaterializedRef):
    """MaterializedRef backed by a DuckDB query. Executes only on to_arrow()."""

    def __init__(self, path: str, read_only: bool, sql: str) -> None:
        self._path = path
        self._read_only = read_only
        self._sql = sql

    def to_arrow(self) -> pyarrow.Table:
        import duckdb

        from rivet_core.errors import ExecutionError, plugin_error

        try:
            conn = duckdb.connect(self._path, read_only=self._read_only)
            try:
                result = conn.execute(self._sql).arrow().read_all()
            finally:
                conn.close()
        except Exception as exc:
            raise ExecutionError(
                plugin_error(
                    "RVT-501",
                    f"DuckDB source query failed: {exc}",
                    plugin_name="rivet_duckdb",
                    plugin_type="source",
                    remediation="Check that the SQL is valid and the table exists in the DuckDB database.",
                    sql=self._sql,
                    path=self._path,
                )
            ) from exc
        return result

    @property
    def schema(self) -> Any:
        import duckdb

        from rivet_core.models import Column, Schema

        conn = duckdb.connect(self._path, read_only=self._read_only)
        try:
            desc = conn.execute(f"DESCRIBE ({self._sql})").fetchall()
        finally:
            conn.close()
        return Schema(
            columns=[
                Column(name=row[0], type=row[1], nullable=row[3] == "YES")
                for row in desc
            ]
        )

    @property
    def row_count(self) -> int:
        return self.to_arrow().num_rows  # type: ignore[no-any-return]

    @property
    def size_bytes(self) -> int | None:
        return None

    @property
    def storage_type(self) -> str:
        return "duckdb"


class DuckDBSource(SourcePlugin):
    """Source plugin for duckdb catalog type.

    Executes source SQL against the DuckDB database and returns a deferred Material.
    Data is not fetched until .to_arrow() is called on the returned Material.
    """

    catalog_type = "duckdb"

    def read(self, catalog: Catalog, joint: Joint, pushdown: Any | None) -> Material:
        path = catalog.options.get("path", ":memory:")
        read_only = catalog.options.get("read_only", False)
        if joint.sql:
            sql = joint.sql
        elif joint.table:
            sql = f"SELECT * FROM {joint.table}"
        else:
            sql = "SELECT 1"

        ref = DuckDBDeferredMaterializedRef(path=path, read_only=read_only, sql=sql)
        return Material(
            name=joint.name,
            catalog=catalog.name,
            materialized_ref=ref,
            state="deferred",
        )
