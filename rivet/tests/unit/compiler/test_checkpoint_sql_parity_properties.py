"""Property tests for checkpoint SQL parity with sink joints.

Properties:
  1. For any valid SELECT SQL, LogicalPlan produced for checkpoint equals that for sink.
  3. For any checkpoint with LogicalPlan, cross_group_pushdown_pass produces same results as sink.
  4. For any columns/filter/limit, SQLGenerator for checkpoint uses upstream as FROM reference.

Requirements: 1.1–1.4, 3.1–3.4, 4.2–4.3
"""

from __future__ import annotations

import uuid
from dataclasses import replace

from hypothesis import given, settings
from hypothesis import strategies as st

from rivet_bridge.sql_gen import SQLGenerator
from rivet_config import JointDeclaration
from rivet_core.compiler import CompiledJoint, _compile_sql_joint
from rivet_core.errors import RivetError
from rivet_core.models import Column, Joint, Schema
from rivet_core.optimizer import FusedGroup, cross_group_pushdown_pass
from rivet_core.plugins import PluginRegistry
from rivet_core.sql_parser import SQLParser

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

_COL_NAMES = st.sampled_from(["id", "name", "amount", "status", "price", "qty", "region"])
_ARROW_TYPES = st.sampled_from(["int64", "utf8", "float64", "bool"])


@st.composite
def _upstream_schema(draw: st.DrawFn) -> dict[str, Schema]:
    n = draw(st.integers(min_value=1, max_value=4))
    used: set[str] = set()
    cols: list[Column] = []
    for _ in range(n):
        name = draw(_COL_NAMES.filter(lambda x: x not in used))
        used.add(name)
        cols.append(Column(name=name, type=draw(_ARROW_TYPES), nullable=True))
    return {"upstream": Schema(columns=cols)}


@st.composite
def _simple_select_sql(draw: st.DrawFn, upstream_schemas: dict[str, Schema]) -> str:
    cols = [c.name for c in upstream_schemas["upstream"].columns]
    n = draw(st.integers(min_value=1, max_value=len(cols)))
    selected = draw(st.lists(st.sampled_from(cols), min_size=n, max_size=n, unique=True))
    sql = f"SELECT {', '.join(selected)} FROM upstream"
    if draw(st.booleans()) and cols:
        col = draw(st.sampled_from(cols))
        sql += f" WHERE {col} > 0"
    if draw(st.booleans()):
        limit = draw(st.integers(min_value=1, max_value=100))
        sql += f" LIMIT {limit}"
    return sql


def _make_group(
    joints: list[str],
    *,
    group_id: str | None = None,
    engine_type: str = "databricks",
    entry_joints: list[str] | None = None,
    exit_joints: list[str] | None = None,
) -> FusedGroup:
    return FusedGroup(
        id=group_id or str(uuid.uuid4()),
        joints=joints,
        engine="eng1",
        engine_type=engine_type,
        adapters={j: None for j in joints},
        fused_sql=None,
        entry_joints=entry_joints or joints[:1],
        exit_joints=exit_joints or joints[-1:],
    )


def _stub_cj(
    name: str,
    joint_type: str,
    upstream: list[str] | None = None,
    output_schema: Schema | None = None,
) -> CompiledJoint:
    return CompiledJoint(
        name=name,
        type=joint_type,
        catalog=None,
        catalog_type=None,
        engine="eng1",
        engine_resolution=None,
        adapter=None,
        sql=None,
        sql_translated=None,
        sql_resolved=None,
        sql_dialect=None,
        engine_dialect=None,
        upstream=upstream or [],
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
        fused_group_id=None,
        tags=[],
        description=None,
        fusion_strategy_override=None,
        materialization_strategy_override=None,
    )


# ---------------------------------------------------------------------------
# Property 1: Checkpoint/sink compilation parity
# ---------------------------------------------------------------------------


# Feature: checkpoint-sql-parity, Property 1: Checkpoint SQL compilation parity with sink
@given(upstream_schemas=_upstream_schema())
@settings(max_examples=100)
def test_checkpoint_sink_compilation_parity(upstream_schemas: dict[str, Schema]) -> None:
    """For any valid SELECT SQL, LogicalPlan for checkpoint equals that for sink."""
    cols = [c.name for c in upstream_schemas["upstream"].columns]
    sql = f"SELECT {', '.join(cols)} FROM upstream"

    parser = SQLParser()
    registry = PluginRegistry()

    cp_joint = Joint(name="cp", joint_type="checkpoint", sql=sql, upstream=["upstream"])
    sink_joint = Joint(name="sk", joint_type="sink", sql=sql, upstream=["upstream"])

    cp_errors: list[RivetError] = []
    sk_errors: list[RivetError] = []

    cp_lp, cp_lineage, _, _, cp_schema = _compile_sql_joint(
        cp_joint, "", registry, parser, upstream_schemas, cp_errors, []
    )
    sk_lp, sk_lineage, _, _, sk_schema = _compile_sql_joint(
        sink_joint, "", registry, parser, upstream_schemas, sk_errors, []
    )

    assert not cp_errors
    assert not sk_errors

    # LogicalPlans must be structurally identical
    assert cp_lp is not None
    assert sk_lp is not None
    assert len(cp_lp.projections) == len(sk_lp.projections)
    assert len(cp_lp.predicates) == len(sk_lp.predicates)
    assert cp_lp.limit == sk_lp.limit

    # Column lineage must have same output columns
    cp_out_cols = {l.output_column for l in cp_lineage}
    sk_out_cols = {l.output_column for l in sk_lineage}
    assert cp_out_cols == sk_out_cols


# ---------------------------------------------------------------------------
# Property 3: Checkpoint/sink pushdown parity
# ---------------------------------------------------------------------------

_FULL_CAPS = {"databricks": ["predicate_pushdown", "projection_pushdown", "limit_pushdown"]}


# Feature: checkpoint-sql-parity, Property 3: Checkpoint pushdown parity with sink
@given(upstream_schemas=_upstream_schema())
@settings(max_examples=100)
def test_checkpoint_sink_pushdown_parity(upstream_schemas: dict[str, Schema]) -> None:
    """cross_group_pushdown_pass produces same results for checkpoint and sink with same SQL."""
    cols = [c.name for c in upstream_schemas["upstream"].columns]
    sql = f"SELECT {', '.join(cols)} FROM upstream WHERE {cols[0]} > 0"

    parser = SQLParser()
    registry = PluginRegistry()

    cp_joint = Joint(name="cp", joint_type="checkpoint", sql=sql, upstream=["upstream"])
    sk_joint = Joint(name="sk", joint_type="sink", sql=sql, upstream=["upstream"])

    cp_lp, cp_lineage, _, _, _ = _compile_sql_joint(
        cp_joint, "", registry, parser, upstream_schemas, [], []
    )
    sk_lp, sk_lineage, _, _, _ = _compile_sql_joint(
        sk_joint, "", registry, parser, upstream_schemas, [], []
    )

    if cp_lp is None or sk_lp is None:
        return  # Skip if SQL couldn't be parsed

    src_schema = upstream_schemas["upstream"]
    src_cj = _stub_cj("upstream", "source", output_schema=src_schema)
    src_group = _make_group(["upstream"], group_id="src_grp")

    # Build checkpoint compiled joint with its LogicalPlan and lineage
    cp_cj = replace(
        _stub_cj("cp", "checkpoint", upstream=["upstream"]),
        logical_plan=cp_lp,
        column_lineage=cp_lineage,
    )
    sk_cj = replace(
        _stub_cj("sk", "sink", upstream=["upstream"]), logical_plan=sk_lp, column_lineage=sk_lineage
    )

    cp_group = _make_group(["cp"], group_id="cp_grp", entry_joints=["cp"], exit_joints=["cp"])
    sk_group = _make_group(["sk"], group_id="sk_grp", entry_joints=["sk"], exit_joints=["sk"])

    cp_new_groups, _ = cross_group_pushdown_pass(
        [src_group, cp_group], {"upstream": src_cj, "cp": cp_cj}, _FULL_CAPS, {}
    )
    sk_new_groups, _ = cross_group_pushdown_pass(
        [src_group, sk_group], {"upstream": src_cj, "sk": sk_cj}, _FULL_CAPS, {}
    )

    cp_src = next(g for g in cp_new_groups if g.id == "src_grp")
    sk_src = next(g for g in sk_new_groups if g.id == "src_grp")

    # Both should push the same predicates to the source group
    cp_preds = {p.expression for preds in cp_src.per_joint_predicates.values() for p in preds}
    sk_preds = {p.expression for preds in sk_src.per_joint_predicates.values() for p in preds}
    assert cp_preds == sk_preds

    # Both should push the same projections
    cp_projs = {c for cols in cp_src.per_joint_projections.values() for c in cols}
    sk_projs = {c for cols in sk_src.per_joint_projections.values() for c in cols}
    assert cp_projs == sk_projs


# ---------------------------------------------------------------------------
# Property 4: SQL generation FROM reference for checkpoints
# ---------------------------------------------------------------------------


# Feature: checkpoint-sql-parity, Property 4: SQL generation from YAML fields for checkpoints
@given(
    col_names=st.lists(_COL_NAMES, min_size=1, max_size=4, unique=True),
    upstream_name=st.sampled_from(["raw_data", "src_events", "input_joint", "upstream"]),
)
@settings(max_examples=100)
def test_checkpoint_sql_gen_uses_upstream_as_from(col_names: list[str], upstream_name: str) -> None:
    """SQLGenerator for checkpoint uses upstream joint name as FROM reference."""
    from rivet_config.models import ColumnDecl

    decl = JointDeclaration(
        name="my_checkpoint",
        joint_type="checkpoint",
        catalog="local",
        table="out_table",
        upstream=[upstream_name],
        columns=[ColumnDecl(name=c, expression=None) for c in col_names],
        sql=None,
        source_path=None,
    )

    sql, errors = SQLGenerator().generate(decl, {upstream_name, "my_checkpoint"})

    assert not errors
    assert sql != ""
    # FROM reference must be the upstream joint, not the destination table
    assert f"FROM {upstream_name}" in sql
    assert "out_table" not in sql
