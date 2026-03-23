"""Property tests for DeferredRef checkpoint deferred resolution.

Covers Properties 1, 2, 3 from the checkpoint-source-resolution design document.

- Property 1: Checkpoint write-then-deferred-read round trip
- Property 2: DeferredRef.to_arrow() caching is idempotent
- Property 3: DeferredRef properties are consistent with materialized data

Uses real DuckDB filesystem catalog writes via SinkPlugin, then constructs
DeferredRef and verifies round-trip, caching identity, and property consistency.
"""

from __future__ import annotations

import tempfile
from typing import Any

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.builtins.filesystem_catalog import FilesystemSource
from rivet_core.models import Catalog, Joint, Material
from rivet_core.plugins import PluginRegistry
from rivet_core.strategies import DeferredRef, _ArrowMaterializedRef
from rivet_duckdb.filesystem_sink import FilesystemSink

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------


@st.composite
def _arrow_table(draw: st.DrawFn) -> pa.Table:
    """Generate an Arrow table with a random mix of int, float, and string columns."""
    n_int = draw(st.integers(min_value=0, max_value=3))
    n_float = draw(st.integers(min_value=0, max_value=3))
    n_str = draw(st.integers(min_value=0, max_value=3))

    total = n_int + n_float + n_str
    if total == 0:
        n_int = 1

    n_rows = draw(st.integers(min_value=0, max_value=50))

    columns: dict[str, Any] = {}
    col_idx = 0

    for _ in range(n_int):
        name = f"col_int_{col_idx}"
        values = draw(
            st.lists(
                st.integers(min_value=-(2**31), max_value=2**31 - 1),
                min_size=n_rows,
                max_size=n_rows,
            )
        )
        columns[name] = pa.array(values, type=pa.int64())
        col_idx += 1

    for _ in range(n_float):
        name = f"col_float_{col_idx}"
        values = draw(
            st.lists(
                st.floats(allow_nan=False, allow_infinity=False, min_value=-1e10, max_value=1e10),
                min_size=n_rows,
                max_size=n_rows,
            )
        )
        columns[name] = pa.array(values, type=pa.float64())
        col_idx += 1

    for _ in range(n_str):
        name = f"col_str_{col_idx}"
        values = draw(
            st.lists(
                st.text(
                    min_size=0,
                    max_size=20,
                    alphabet=st.characters(whitelist_categories=("L", "N", "Z")),
                ),
                min_size=n_rows,
                max_size=n_rows,
            )
        )
        columns[name] = pa.array(values, type=pa.utf8())
        col_idx += 1

    return pa.table(columns)


def _write_to_filesystem(table: pa.Table, tmpdir: str, table_name: str = "cp_test") -> None:
    """Write an Arrow table to a filesystem catalog via SinkPlugin."""
    catalog = Catalog(
        name="local",
        type="filesystem",
        options={"path": tmpdir, "format": "parquet"},
    )
    joint = Joint(
        name=table_name,
        joint_type="checkpoint",
        catalog="local",
        table=table_name,
        upstream=["fake_upstream"],
    )
    material = Material(
        name=table_name,
        catalog="local",
        state="materialized",
        materialized_ref=_ArrowMaterializedRef(table),
    )
    sink = FilesystemSink()
    sink.write(catalog, joint, material, "replace")


def _make_registry() -> PluginRegistry:
    """Create a PluginRegistry with filesystem source registered."""
    reg = PluginRegistry()
    reg.register_source(FilesystemSource())
    return reg


# ---------------------------------------------------------------------------
# Property 1: Checkpoint write-then-deferred-read round trip
# ---------------------------------------------------------------------------


@given(table=_arrow_table())
@settings(max_examples=50)
def test_property1_checkpoint_write_then_deferred_read_round_trip(table: pa.Table) -> None:
    """For any Arrow table written to a checkpoint catalog table, constructing a
    DeferredRef from the checkpoint's catalog metadata and calling .to_arrow()
    produces a table with the same row count, column names, and data values.

    Validates: Requirements 1.2, 7.4, 8.1, 8.2, 8.3, 8.4
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        _write_to_filesystem(table, tmpdir)

        ref = DeferredRef(
            catalog_name="local",
            catalog_type="filesystem",
            table_name="cp_test",
            catalog_options={"path": tmpdir, "format": "parquet"},
            registry=_make_registry(),
        )

        result = ref.to_arrow()

        assert result.num_rows == table.num_rows
        assert result.column_names == table.column_names

        # Verify data values match
        for col_name in table.column_names:
            original = table.column(col_name).to_pylist()
            read_back = result.column(col_name).to_pylist()
            assert read_back == original, f"Data mismatch in column '{col_name}'"


# ---------------------------------------------------------------------------
# Property 2: DeferredRef.to_arrow() caching is idempotent
# ---------------------------------------------------------------------------


@given(table=_arrow_table())
@settings(max_examples=50)
def test_property2_deferred_ref_to_arrow_caching_idempotent(table: pa.Table) -> None:
    """Calling .to_arrow() multiple times returns the exact same pyarrow.Table
    object (identity, not just equality), and only the first call reads from
    the catalog.

    Validates: Requirements 1.3
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        _write_to_filesystem(table, tmpdir)

        ref = DeferredRef(
            catalog_name="local",
            catalog_type="filesystem",
            table_name="cp_test",
            catalog_options={"path": tmpdir, "format": "parquet"},
            registry=_make_registry(),
        )

        first_call = ref.to_arrow()
        second_call = ref.to_arrow()
        third_call = ref.to_arrow()

        # Identity check — same object, not just equal
        assert first_call is second_call
        assert second_call is third_call


# ---------------------------------------------------------------------------
# Property 3: DeferredRef properties are consistent with materialized data
# ---------------------------------------------------------------------------


@given(table=_arrow_table())
@settings(max_examples=50)
def test_property3_deferred_ref_properties_consistent(table: pa.Table) -> None:
    """For any DeferredRef backed by a catalog table, .row_count matches the
    table's row count, .schema column names match, .storage_type is
    'catalog_deferred', and accessing properties on an unread ref triggers
    .to_arrow().

    Validates: Requirements 1.4, 1.5, 1.6
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        _write_to_filesystem(table, tmpdir)

        ref = DeferredRef(
            catalog_name="local",
            catalog_type="filesystem",
            table_name="cp_test",
            catalog_options={"path": tmpdir, "format": "parquet"},
            registry=_make_registry(),
        )

        # storage_type is always available without reading
        assert ref.storage_type == "catalog_deferred"

        # No pre-computed table is cached yet
        assert ref.to_arrow_if_cached() is None
        assert ref.has_cached_arrow() is False

        # Accessing row_count triggers .to_arrow()
        assert ref.row_count == table.num_rows

        # After row_count, Arrow data is cached
        assert ref.to_arrow_if_cached() is not None
        assert ref.has_cached_arrow() is True

        # Schema column names match
        ref_col_names = [c.name for c in ref.schema.columns]
        assert ref_col_names == table.column_names

        # size_bytes is non-negative
        size = ref.size_bytes
        assert size is not None
        assert size >= 0


# ---------------------------------------------------------------------------
# Property 10: Write-path-aware caching preserves Arrow data from fallback write
# ---------------------------------------------------------------------------


@given(table=_arrow_table())
@settings(max_examples=50)
def test_property10_write_path_aware_caching(table: pa.Table) -> None:
    """For the Arrow fallback write path, DeferredRef(cached_table=table) stores
    the table in its Arrow cache and .to_arrow() returns the exact same object
    (identity). For the native SQL write path, DeferredRef(cached_table=None)
    starts without cached Arrow data and .to_arrow() reads from the catalog and caches.

    Validates: Requirements 1.3, 2.2, 2.3
    """
    # --- Arrow fallback path: cached_table provided ---
    ref_cached = DeferredRef(
        catalog_name="local",
        catalog_type="filesystem",
        table_name="cp_test",
        catalog_options={"path": "/unused"},
        registry=None,
        cached_table=table,
    )

    assert ref_cached.to_arrow_if_cached() is table
    assert ref_cached.has_cached_arrow() is True
    assert ref_cached.to_arrow() is table
    # Second call also returns same object
    assert ref_cached.to_arrow() is table

    # --- Native SQL path: cached_table=None, reads from catalog ---
    with tempfile.TemporaryDirectory() as tmpdir:
        _write_to_filesystem(table, tmpdir)

        ref_deferred = DeferredRef(
            catalog_name="local",
            catalog_type="filesystem",
            table_name="cp_test",
            catalog_options={"path": tmpdir, "format": "parquet"},
            registry=_make_registry(),
            cached_table=None,
        )

        assert ref_deferred.to_arrow_if_cached() is None
        assert ref_deferred.has_cached_arrow() is False

        result = ref_deferred.to_arrow()

        # After first call, Arrow data is cached
        assert ref_deferred.to_arrow_if_cached() is not None
        assert ref_deferred.to_arrow_if_cached() is result
        assert ref_deferred.has_cached_arrow() is True

        # Subsequent calls return same object
        assert ref_deferred.to_arrow() is result

        # Data matches
        assert result.num_rows == table.num_rows
        assert result.column_names == table.column_names
