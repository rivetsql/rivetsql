"""Property tests for _checkpoint_read_back returning DeferredRef with write-path-aware caching.

Property 4: _checkpoint_read_back returns DeferredRef with correct metadata
and write-path-aware caching.

For any checkpoint joint with catalog metadata, _checkpoint_read_back returns
a DeferredRef whose fields match the catalog metadata. When result_table is
provided (Arrow fallback path), the DeferredRef carries it as cached_table.
When result_table is None (native SQL path), cached_table is None.

Validates: Requirements 2.1, 2.2, 2.3, 2.4
"""

from __future__ import annotations

import asyncio
from typing import Any

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.compiler import CompiledCatalog, CompiledJoint
from rivet_core.executor import Executor
from rivet_core.strategies import DeferredRef

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_NAMES = st.text(
    alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="_"),
    min_size=1,
    max_size=20,
)
_CATALOG_TYPES = st.sampled_from(["filesystem", "glue", "unity", "hive"])


@st.composite
def _catalog_metadata(draw: st.DrawFn) -> dict[str, Any]:
    """Generate random catalog metadata for a checkpoint joint."""
    catalog_name = draw(_NAMES)
    catalog_type = draw(_CATALOG_TYPES)
    table_name = draw(_NAMES)
    option_keys = draw(st.lists(_NAMES, max_size=3, unique=True))
    option_vals = draw(st.lists(_NAMES, min_size=len(option_keys), max_size=len(option_keys)))
    options = dict(zip(option_keys, option_vals))
    return {
        "catalog_name": catalog_name,
        "catalog_type": catalog_type,
        "table_name": table_name,
        "options": options,
    }


def _make_compiled_joint(name: str, catalog: str, table: str) -> CompiledJoint:
    """Build a minimal CompiledJoint for checkpoint read-back testing."""
    return CompiledJoint(
        name=name,
        type="checkpoint",
        catalog=catalog,
        catalog_type=None,
        engine="test_engine",
        engine_resolution=None,
        adapter=None,
        sql=None,
        sql_translated=None,
        sql_resolved=None,
        sql_dialect=None,
        engine_dialect=None,
        upstream=[],
        eager=False,
        table=table,
        write_strategy=None,
        function=None,
        source_file=None,
        logical_plan=None,
        output_schema=None,
        column_lineage=[],
        optimizations=[],
        checks=[],
        fused_group_id=None,
        tags=[],
        description=None,
        fusion_strategy_override=None,
        materialization_strategy_override=None,
    )


def _make_arrow_table() -> pa.Table:
    """Build a small Arrow table for testing."""
    return pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})


# ---------------------------------------------------------------------------
# Property 4: _checkpoint_read_back returns DeferredRef with correct metadata
# ---------------------------------------------------------------------------


@given(meta=_catalog_metadata())
@settings(max_examples=100)
def test_checkpoint_read_back_returns_deferred_ref_with_correct_metadata(
    meta: dict[str, Any],
) -> None:
    """_checkpoint_read_back returns a DeferredRef whose catalog fields match
    the CompiledCatalog metadata, regardless of write path."""
    cj = _make_compiled_joint(
        name="cp_joint",
        catalog="test_cat",
        table=meta["table_name"],
    )
    catalog_map = {
        "test_cat": CompiledCatalog(
            name=meta["catalog_name"],
            type=meta["catalog_type"],
            options=meta["options"],
        ),
    }

    executor = Executor()
    ref = asyncio.run(executor._checkpoint_read_back(cj, catalog_map))

    assert isinstance(ref, DeferredRef)
    assert ref.catalog_name == meta["catalog_name"]
    assert ref.catalog_type == meta["catalog_type"]
    assert ref.table_name == meta["table_name"]
    assert ref.catalog_options == meta["options"]
    assert ref.storage_type == "catalog_deferred"


@given(meta=_catalog_metadata())
@settings(max_examples=50)
def test_checkpoint_read_back_native_sql_path_has_no_cached_table(
    meta: dict[str, Any],
) -> None:
    """Native SQL write path: result_table=None leaves no cached Arrow table."""
    cj = _make_compiled_joint(name="cp", catalog="cat", table=meta["table_name"])
    catalog_map = {
        "cat": CompiledCatalog(
            name=meta["catalog_name"],
            type=meta["catalog_type"],
            options=meta["options"],
        ),
    }

    executor = Executor()
    ref = asyncio.run(executor._checkpoint_read_back(cj, catalog_map, result_table=None))

    assert isinstance(ref, DeferredRef)
    assert ref.to_arrow_if_cached() is None
    assert ref.has_cached_arrow() is False


@given(meta=_catalog_metadata())
@settings(max_examples=50)
def test_checkpoint_read_back_arrow_fallback_path_carries_cached_table(
    meta: dict[str, Any],
) -> None:
    """Arrow fallback write path: result_table=table is cached by identity."""
    table = _make_arrow_table()
    cj = _make_compiled_joint(name="cp", catalog="cat", table=meta["table_name"])
    catalog_map = {
        "cat": CompiledCatalog(
            name=meta["catalog_name"],
            type=meta["catalog_type"],
            options=meta["options"],
        ),
    }

    executor = Executor()
    ref = asyncio.run(executor._checkpoint_read_back(cj, catalog_map, result_table=table))

    assert isinstance(ref, DeferredRef)
    assert ref.to_arrow_if_cached() is table
    assert ref.has_cached_arrow() is True
    # .to_arrow() returns the same object (identity, not just equality)
    assert ref.to_arrow() is table
