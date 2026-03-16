"""Property tests for checkpoint write-then-read round-trip.

Covers Property 5 from the checkpoint-joint design document.

- Property 5: Write-then-read round-trip
  Validates: Requirements 10.1, 10.2, 10.3, 10.4
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.builtins.filesystem_catalog import FilesystemSource
from rivet_core.models import Catalog, Joint, Material
from rivet_core.strategies import _ArrowMaterializedRef
from rivet_duckdb.filesystem_sink import FilesystemSink

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------


@st.composite
def _arrow_table(draw: st.DrawFn) -> pa.Table:
    """Generate an Arrow table with a random mix of int, float, and string columns.

    At least one column is always present. Row count varies from 0 to 50.
    """
    n_int = draw(st.integers(min_value=0, max_value=3))
    n_float = draw(st.integers(min_value=0, max_value=3))
    n_str = draw(st.integers(min_value=0, max_value=3))

    # Ensure at least one column
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


# ---------------------------------------------------------------------------
# Property Tests
# ---------------------------------------------------------------------------


# Feature: checkpoint-joint, Property 5: write-then-read round-trip
@given(table=_arrow_table())
@settings(max_examples=50)
def test_property5_checkpoint_round_trip(table: pa.Table) -> None:
    """For any Arrow table written by a checkpoint joint to a filesystem catalog,
    reading it back SHALL produce a table with the same number of rows and the
    same column names as the original table.

    Uses real FilesystemSink and FilesystemSource — no mocks.

    **Validates: Requirements 10.1, 10.2, 10.3, 10.4**
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        catalog = Catalog(
            name="local",
            type="filesystem",
            options={"path": tmpdir, "format": "parquet"},
        )

        # Joint used for both write and read
        joint = Joint(
            name="cp_test",
            joint_type="checkpoint",
            catalog="local",
            table="cp_test",
            upstream=["fake_upstream"],
        )

        # Write via SinkPlugin
        material = Material(
            name="cp_test",
            catalog="local",
            state="materialized",
            materialized_ref=_ArrowMaterializedRef(table),
        )
        sink = FilesystemSink()
        sink.write(catalog, joint, material, "replace")

        # Verify the file was written
        written_path = Path(tmpdir) / "cp_test.parquet"
        assert written_path.exists(), f"Expected parquet file at {written_path}"

        # Read back via SourcePlugin
        source = FilesystemSource()
        read_material = source.read(catalog, joint, pushdown=None)
        read_table = read_material.to_arrow()

        # Property: same row count
        assert read_table.num_rows == table.num_rows, (
            f"Row count mismatch: wrote {table.num_rows}, read {read_table.num_rows}"
        )

        # Property: same column names
        assert read_table.column_names == table.column_names, (
            f"Column name mismatch: wrote {table.column_names}, read {read_table.column_names}"
        )
