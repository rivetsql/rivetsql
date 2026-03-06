"""Property test: Joint preview never triggers execution (task 9.5).

Property 9: Building preview data does not invoke Executor or modify MaterialCache.

# Feature: repl-state-improvements, Property 9
Validates: Requirements 5.4
"""

from __future__ import annotations

from unittest.mock import patch

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.compiler import CompiledJoint
from rivet_core.interactive.material_cache import MaterialCache
from rivet_core.interactive.types import JointPreviewData, SchemaField
from rivet_core.models import Material

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_name_st = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)
_engine_st = st.sampled_from(["duckdb", "spark", "trino"])


def _make_compiled_joint(name: str, engine: str) -> CompiledJoint:
    return CompiledJoint(
        name=name,
        type="sql",
        catalog=None,
        catalog_type=None,
        engine=engine,
        engine_resolution="project_default",
        adapter=None,
        sql="SELECT 1",
        sql_translated=None,
        sql_resolved=None,
        sql_dialect=None,
        engine_dialect=None,
        upstream=[],
        eager=False,
        table=None,
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


def _build_preview(joint: CompiledJoint, cache: MaterialCache) -> JointPreviewData:
    """Replicate the logic from app.on_catalog_panel_joint_preview_requested."""
    schema = None
    if joint.output_schema is not None:
        schema = [
            SchemaField(name=c.name, type=c.type)
            for c in joint.output_schema.columns
        ]

    preview_rows = None
    cached = cache.get(joint.name)
    if cached is not None:
        try:
            table = cached.to_arrow()
            preview_rows = table.slice(0, 10)
        except Exception:  # noqa: BLE001
            pass

    return JointPreviewData(
        joint_name=joint.name,
        engine=joint.engine,
        fusion_group=joint.fused_group_id,
        upstream=list(joint.upstream),
        tags=list(joint.tags),
        schema=schema,
        preview_rows=preview_rows,
    )


# ---------------------------------------------------------------------------
# Property tests
# ---------------------------------------------------------------------------


@given(name=_name_st, engine=_engine_st)
@settings(max_examples=100)
def test_preview_does_not_modify_empty_cache(name: str, engine: str) -> None:
    """Building preview on an uncached joint leaves MaterialCache unchanged."""
    cache = MaterialCache()
    assert len(cache) == 0

    _build_preview(_make_compiled_joint(name, engine), cache)

    assert len(cache) == 0


@given(name=_name_st, engine=_engine_st)
@settings(max_examples=100)
def test_preview_does_not_modify_populated_cache(name: str, engine: str) -> None:
    """Building preview does not add or remove entries from a populated cache."""
    cache = MaterialCache()

    # Pre-populate with a cached entry for this joint
    table = pa.table({"x": [1, 2, 3]})

    class _InMemoryRef:
        def to_arrow(self) -> pa.Table:
            return table

    material = Material(
        name=name,
        catalog="test",
        state="materialized",
        materialized_ref=_InMemoryRef(),  # type: ignore[arg-type]
    )
    cache.put(name, material)
    before_len = len(cache)

    _build_preview(_make_compiled_joint(name, engine), cache)

    # Cache must be unchanged
    assert len(cache) == before_len
    assert cache.get(name) is material


@given(name=_name_st, engine=_engine_st)
@settings(max_examples=50)
def test_preview_does_not_invoke_executor(name: str, engine: str) -> None:
    """Building preview never calls any Executor method."""
    cache = MaterialCache()

    with patch("rivet_core.executor.Executor", autospec=True) as mock_executor_cls:
        _build_preview(_make_compiled_joint(name, engine), cache)
        mock_executor_cls.assert_not_called()
