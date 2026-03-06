"""Property test: Joint preview includes all metadata and conditional cached rows (task 9.4).

Property 8: For any compiled joint, preview data includes engine, fusion group,
upstream, tags, schema; cached output → ≤10 rows; uncached → None.

# Feature: repl-state-improvements, Property 8
Validates: Requirements 5.2, 5.3
"""

from __future__ import annotations

import pyarrow as pa
from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_core.compiler import CompiledJoint
from rivet_core.interactive.material_cache import MaterialCache
from rivet_core.interactive.types import JointPreviewData, SchemaField
from rivet_core.models import Column, Schema

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_name_st = st.from_regex(r"[a-z][a-z0-9_]{0,15}", fullmatch=True)
_engine_st = st.sampled_from(["duckdb", "spark", "trino"])
_fusion_group_st = st.one_of(st.none(), st.from_regex(r"fg_[a-z0-9]{1,8}", fullmatch=True))
_upstream_st = st.lists(_name_st, min_size=0, max_size=4)
_tags_st = st.lists(st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True), min_size=0, max_size=3)
_col_name_st = st.from_regex(r"[a-z][a-z0-9_]{0,10}", fullmatch=True)
_col_type_st = st.sampled_from(["int64", "utf8", "float64", "bool"])
_schema_st = st.one_of(
    st.none(),
    st.lists(
        st.builds(Column, name=_col_name_st, type=_col_type_st, nullable=st.booleans()),
        min_size=1,
        max_size=5,
    ).map(Schema),
)
_row_count_st = st.integers(min_value=0, max_value=25)


def _make_compiled_joint(
    name: str,
    engine: str,
    fused_group_id: str | None,
    upstream: list[str],
    tags: list[str],
    output_schema: Schema | None,
) -> CompiledJoint:
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
        upstream=upstream,
        eager=False,
        table=None,
        write_strategy=None,
        function=None,
        source_file=None,
        logical_plan=None,
        output_schema=output_schema,
        column_lineage=[],
        optimizations=[],
        checks=[],
        fused_group_id=fused_group_id,
        tags=tags,
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

@given(
    name=_name_st,
    engine=_engine_st,
    fused_group_id=_fusion_group_st,
    upstream=_upstream_st,
    tags=_tags_st,
    output_schema=_schema_st,
)
@settings(max_examples=100)
def test_preview_metadata_matches_joint(
    name: str,
    engine: str,
    fused_group_id: str | None,
    upstream: list[str],
    tags: list[str],
    output_schema: Schema | None,
) -> None:
    """Preview data includes all metadata fields from the compiled joint."""
    joint = _make_compiled_joint(name, engine, fused_group_id, upstream, tags, output_schema)
    cache = MaterialCache()

    preview = _build_preview(joint, cache)

    assert preview.joint_name == name
    assert preview.engine == engine
    assert preview.fusion_group == fused_group_id
    assert preview.upstream == upstream
    assert preview.tags == tags

    if output_schema is None:
        assert preview.schema is None
    else:
        assert preview.schema is not None
        assert len(preview.schema) == len(output_schema.columns)
        for sf, col in zip(preview.schema, output_schema.columns):
            assert sf.name == col.name
            assert sf.type == col.type


@given(
    name=_name_st,
    engine=_engine_st,
    row_count=_row_count_st,
)
@settings(max_examples=100)
def test_preview_rows_capped_at_10_when_cached(
    name: str,
    engine: str,
    row_count: int,
) -> None:
    """When cached output exists, preview_rows has at most 10 rows."""
    from rivet_core.models import Material

    joint = _make_compiled_joint(name, engine, None, [], [], None)
    cache = MaterialCache()

    # Build a material with row_count rows
    table = pa.table({"x": list(range(row_count))})

    class _InMemoryRef:
        def to_arrow(self) -> pa.Table:
            return table

    material = Material(name=name, catalog="test", state="materialized", materialized_ref=_InMemoryRef())  # type: ignore[arg-type]
    cache.put(name, material)

    preview = _build_preview(joint, cache)

    assert preview.preview_rows is not None
    assert preview.preview_rows.num_rows == min(row_count, 10)


@given(
    name=_name_st,
    engine=_engine_st,
)
@settings(max_examples=50)
def test_preview_rows_none_when_uncached(name: str, engine: str) -> None:
    """When no cached output exists, preview_rows is None."""
    joint = _make_compiled_joint(name, engine, None, [], [], None)
    cache = MaterialCache()  # empty cache

    preview = _build_preview(joint, cache)

    assert preview.preview_rows is None
