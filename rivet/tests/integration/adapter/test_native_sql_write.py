"""Integration tests for DuckDBLocalAdapter native SQL write.

Exercises the adapter directly with real DuckDB connections, verifying
that native SQL write produces correct data for all supported strategies
and that Arrow fallback works when NativeSqlWriteContext is not used.

Requirements: 2.2, 3.1, 3.2, 3.3, 3.4, 3.5, 6.1
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pytest

from rivet_core.models import Catalog, Joint
from rivet_core.plugins import NativeSqlWriteContext
from rivet_duckdb.adapters.duckdb_local import DuckDBLocalAdapter


@pytest.fixture()
def adapter() -> DuckDBLocalAdapter:
    return DuckDBLocalAdapter()


@pytest.fixture()
def db_path(tmp_path: Path) -> str:
    return str(tmp_path / "test.duckdb")


def _make_ctx(
    db_path: str,
    sql: str,
    target: str,
    strategy: str,
    input_tables: dict[str, pa.Table] | None = None,
) -> NativeSqlWriteContext:
    cat = Catalog(name="testcat", type="duckdb", options={"path": db_path})
    joint = Joint(name=target, joint_type="sink", catalog="testcat", table=target)
    return NativeSqlWriteContext(
        fused_sql=sql,
        target_table=target,
        write_strategy=strategy,
        input_tables=input_tables or {},
        engine=None,
        catalog=cat,
        joint=joint,
    )


def _read_table(db_path: str, table: str) -> pa.Table:
    conn = duckdb.connect(db_path, read_only=True)
    try:
        result = conn.execute(f"SELECT * FROM {table}").arrow()
        if hasattr(result, "read_all"):
            result = result.read_all()
        return result
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Strategy tests
# ---------------------------------------------------------------------------


def test_duckdb_local_adapter_replace(adapter: DuckDBLocalAdapter, db_path: str) -> None:
    """Native SQL write with 'replace' creates table with correct data."""
    ctx = _make_ctx(db_path, "SELECT 1 AS id, 'hello' AS name", "out_tbl", "replace")
    adapter.write_dispatch(None, ctx.catalog, ctx.joint, ctx)

    result = _read_table(db_path, "out_tbl")
    assert result.num_rows == 1
    assert result.column("id").to_pylist() == [1]
    assert result.column("name").to_pylist() == ["hello"]

    # Replace again with different data
    ctx2 = _make_ctx(db_path, "SELECT 2 AS id, 'world' AS name", "out_tbl", "replace")
    adapter.write_dispatch(None, ctx2.catalog, ctx2.joint, ctx2)

    result2 = _read_table(db_path, "out_tbl")
    assert result2.num_rows == 1
    assert result2.column("id").to_pylist() == [2]


def test_duckdb_local_adapter_append(adapter: DuckDBLocalAdapter, db_path: str) -> None:
    """Native SQL write with 'append' accumulates rows across writes."""
    ctx1 = _make_ctx(db_path, "SELECT 1 AS id", "out_tbl", "append")
    adapter.write_dispatch(None, ctx1.catalog, ctx1.joint, ctx1)
    assert _read_table(db_path, "out_tbl").num_rows == 1

    ctx2 = _make_ctx(db_path, "SELECT 2 AS id", "out_tbl", "append")
    adapter.write_dispatch(None, ctx2.catalog, ctx2.joint, ctx2)

    result = _read_table(db_path, "out_tbl")
    assert result.num_rows == 2
    assert sorted(result.column("id").to_pylist()) == [1, 2]


def test_duckdb_local_adapter_truncate_insert(adapter: DuckDBLocalAdapter, db_path: str) -> None:
    """Native SQL write with 'truncate_insert' replaces data but keeps table."""
    ctx1 = _make_ctx(db_path, "SELECT 1 AS id", "out_tbl", "truncate_insert")
    adapter.write_dispatch(None, ctx1.catalog, ctx1.joint, ctx1)
    assert _read_table(db_path, "out_tbl").num_rows == 1

    ctx2 = _make_ctx(
        db_path, "SELECT 2 AS id UNION ALL SELECT 3 AS id", "out_tbl", "truncate_insert"
    )
    adapter.write_dispatch(None, ctx2.catalog, ctx2.joint, ctx2)

    result = _read_table(db_path, "out_tbl")
    assert result.num_rows == 2
    assert sorted(result.column("id").to_pylist()) == [2, 3]


# ---------------------------------------------------------------------------
# Arrow fallback
# ---------------------------------------------------------------------------


def test_duckdb_local_adapter_arrow_fallback(adapter: DuckDBLocalAdapter, db_path: str) -> None:
    """write_dispatch with a non-NativeSqlWriteContext material uses Arrow fallback."""
    from rivet_core.strategies import MaterializedRef

    class _SimpleRef(MaterializedRef):
        def __init__(self, tbl: pa.Table) -> None:
            self._tbl = tbl

        def to_arrow(self) -> pa.Table:
            return self._tbl

        @property
        def schema(self) -> Any:
            return None

        @property
        def row_count(self) -> int:
            return self._tbl.num_rows

        @property
        def size_bytes(self) -> int | None:
            return None

        @property
        def storage_type(self) -> str:
            return "arrow"

    arrow_tbl = pa.table({"id": [10, 20], "val": ["a", "b"]})
    ref = _SimpleRef(arrow_tbl)
    cat = Catalog(name="testcat", type="duckdb", options={"path": db_path})
    joint = Joint(name="fallback_tbl", joint_type="sink", catalog="testcat", table="fallback_tbl")

    adapter.write_dispatch(None, cat, joint, ref)

    result = _read_table(db_path, "fallback_tbl")
    assert result.num_rows == 2
    assert sorted(result.column("id").to_pylist()) == [10, 20]


# ---------------------------------------------------------------------------
# supports_native_sql_write matrix
# ---------------------------------------------------------------------------


def test_supports_native_sql_write_matrix(adapter: DuckDBLocalAdapter) -> None:
    """Verify supported and unsupported strategies."""
    assert adapter.supports_native_sql_write("replace") is True
    assert adapter.supports_native_sql_write("append") is True
    assert adapter.supports_native_sql_write("truncate_insert") is True

    assert adapter.supports_native_sql_write("merge") is False
    assert adapter.supports_native_sql_write("scd2") is False
    assert adapter.supports_native_sql_write("delete_insert") is False
    assert adapter.supports_native_sql_write("partition") is False
    assert adapter.supports_native_sql_write("incremental_append") is False


# ---------------------------------------------------------------------------
# Upstream tables registration
# ---------------------------------------------------------------------------


def test_native_write_with_upstream_tables(adapter: DuckDBLocalAdapter, db_path: str) -> None:
    """Upstream Arrow tables are registered and referenceable in fused SQL."""
    upstream = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    ctx = _make_ctx(
        db_path,
        "SELECT x, y FROM src_data WHERE x > 1",
        "out_tbl",
        "replace",
        input_tables={"src_data": upstream},
    )
    adapter.write_dispatch(None, ctx.catalog, ctx.joint, ctx)

    result = _read_table(db_path, "out_tbl")
    assert result.num_rows == 2
    assert sorted(result.column("x").to_pylist()) == [2, 3]
