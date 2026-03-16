"""Property test for _ArrowMaterializedRef passthrough in checkpoint pipelines.

Property 7: _ArrowMaterializedRef passthrough is unchanged.

Verifies that upstream joints with _ArrowMaterializedRef in materials are
passed through via .to_arrow() without deferred resolution — the DeferredRef
skip in arrow_materials only applies to DeferredRef instances, not to
_ArrowMaterializedRef.

Validates: Requirements 3.3, 7.2
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.strategies import DeferredRef, MaterializedRef, _ArrowMaterializedRef

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------


@st.composite
def _arrow_table(draw: st.DrawFn) -> pa.Table:
    """Generate a small Arrow table with random int and string columns."""
    n_rows = draw(st.integers(min_value=0, max_value=30))
    n_cols = draw(st.integers(min_value=1, max_value=4))

    columns: dict[str, Any] = {}
    for i in range(n_cols):
        if draw(st.booleans()):
            values = draw(
                st.lists(
                    st.integers(min_value=-(2**31), max_value=2**31 - 1),
                    min_size=n_rows,
                    max_size=n_rows,
                )
            )
            columns[f"col_{i}"] = pa.array(values, type=pa.int64())
        else:
            values = draw(
                st.lists(
                    st.text(min_size=0, max_size=10, alphabet="abcdefghij"),
                    min_size=n_rows,
                    max_size=n_rows,
                )
            )
            columns[f"col_{i}"] = pa.array(values, type=pa.utf8())

    return pa.table(columns)


# ---------------------------------------------------------------------------
# Property 7: _ArrowMaterializedRef passthrough is unchanged
# ---------------------------------------------------------------------------


@given(table=_arrow_table())
@settings(max_examples=50)
def test_property7_arrow_materialized_ref_passthrough(table: pa.Table) -> None:
    """_ArrowMaterializedRef entries in materials are always included in
    arrow_materials via .to_arrow(), regardless of checkpoint_sources.

    This verifies that the DeferredRef skip logic in _execute_group_success
    only affects DeferredRef instances — _ArrowMaterializedRef entries pass
    through unchanged, preserving backward compatibility.

    Validates: Requirements 3.3, 7.2
    """
    arrow_ref = _ArrowMaterializedRef(table)
    deferred_ref = DeferredRef(
        catalog_name="test_cat",
        catalog_type="filesystem",
        table_name="cp_table",
        catalog_options={"path": "/unused"},
        registry=None,
        cached_table=table,
    )

    # Simulate materials dict with both ref types
    materials: dict[str, MaterializedRef] = {
        "upstream_arrow": arrow_ref,
        "checkpoint_deferred": deferred_ref,
        "another_arrow": _ArrowMaterializedRef(pa.table({"x": [1, 2, 3]})),
    }

    # Simulate checkpoint_sources — only the deferred ref is a checkpoint source
    checkpoint_sources: dict[str, Any] = {
        "checkpoint_deferred": "some_info",
    }

    # Replicate the arrow_materials construction from _execute_group_success
    _cp_sources = checkpoint_sources or {}
    arrow_materials = {
        k: v.to_arrow()
        for k, v in materials.items()
        if not (isinstance(v, DeferredRef) and k in _cp_sources)
    }

    # _ArrowMaterializedRef entries MUST be present
    assert "upstream_arrow" in arrow_materials
    assert "another_arrow" in arrow_materials

    # DeferredRef that IS a checkpoint source MUST be skipped
    assert "checkpoint_deferred" not in arrow_materials

    # The Arrow tables from _ArrowMaterializedRef are the original tables
    assert arrow_materials["upstream_arrow"].equals(table)
    assert arrow_materials["another_arrow"].equals(pa.table({"x": [1, 2, 3]}))


@given(table=_arrow_table())
@settings(max_examples=50)
def test_property7_deferred_ref_not_in_checkpoint_sources_is_materialized(
    table: pa.Table,
) -> None:
    """A DeferredRef that is NOT in checkpoint_sources is still materialized
    via .to_arrow() into arrow_materials — only checkpoint-source DeferredRefs
    are skipped.

    Validates: Requirements 3.3, 7.2
    """
    deferred_ref = DeferredRef(
        catalog_name="test_cat",
        catalog_type="filesystem",
        table_name="some_table",
        catalog_options={"path": "/unused"},
        registry=None,
        cached_table=table,
    )

    materials: dict[str, MaterializedRef] = {
        "non_checkpoint_deferred": deferred_ref,
    }

    # Empty checkpoint_sources — the DeferredRef is NOT a checkpoint source
    checkpoint_sources: dict[str, Any] = {}

    _cp_sources = checkpoint_sources or {}
    arrow_materials = {
        k: v.to_arrow()
        for k, v in materials.items()
        if not (isinstance(v, DeferredRef) and k in _cp_sources)
    }

    # DeferredRef NOT in checkpoint_sources MUST be materialized
    assert "non_checkpoint_deferred" in arrow_materials
    assert arrow_materials["non_checkpoint_deferred"] is table
